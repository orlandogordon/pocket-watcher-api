from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import List
from uuid import UUID

from src.db.core import get_db, NotFoundError
from src.crud import crud_financial_plan, crud_category
from src.models import financial_plan as financial_plan_models
from src.auth.dependencies import get_current_user_id

router = APIRouter(
    prefix="/financial_plans",
    tags=["financial_plans"],
    responses={404: {"description": "Not found"}},
)

def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")

@router.post("/", response_model=financial_plan_models.FinancialPlan, status_code=201)
def create_financial_plan(plan: financial_plan_models.FinancialPlanCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        return crud_financial_plan.create_financial_plan(db=db, user_id=user_id, plan=plan)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="A financial plan with this name already exists")

@router.get("/", response_model=List[financial_plan_models.FinancialPlan])
def read_financial_plans(skip: int = 0, limit: int = 100, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    plans = crud_financial_plan.get_financial_plans(db, user_id=user_id, skip=skip, limit=limit)
    return plans

@router.get("/{plan_uuid}", response_model=financial_plan_models.FinancialPlan)
def read_financial_plan(plan_uuid: str, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return db_plan

@router.put("/{plan_uuid}", response_model=financial_plan_models.FinancialPlan)
def update_financial_plan(plan_uuid: str, plan: financial_plan_models.FinancialPlanUpdate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    try:
        return crud_financial_plan.update_financial_plan(db=db, db_plan=db_plan, plan_in=plan)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="A financial plan with this name already exists")

@router.delete("/{plan_uuid}", status_code=204)
def delete_financial_plan(plan_uuid: str, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    crud_financial_plan.delete_financial_plan(db=db, db_plan=db_plan)
    return

@router.get("/{plan_uuid}/summary", response_model=financial_plan_models.FinancialPlanSummary)
def get_financial_plan_summary(plan_uuid: str, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.get_financial_plan_summary(db_plan)

@router.post("/{plan_uuid}/months", response_model=financial_plan_models.FinancialPlanMonth, status_code=201)
def create_financial_plan_month(plan_uuid: str, month_data: financial_plan_models.FinancialPlanMonthCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")

    # Resolve category UUIDs for expenses
    resolved_category_ids = {}
    for expense in month_data.expenses:
        cat = crud_category.read_db_category_by_uuid(db, expense.category_uuid)
        if not cat:
            raise HTTPException(status_code=404, detail=f"Category not found: {expense.category_uuid}")
        resolved_category_ids[str(expense.category_uuid)] = cat.id

    try:
        return crud_financial_plan.create_financial_plan_month(db=db, plan_id=db_plan.plan_id, month_data=month_data, resolved_category_ids=resolved_category_ids)
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="A month entry for this year/month already exists in the plan")

@router.post("/{plan_uuid}/months/bulk", response_model=List[financial_plan_models.FinancialPlanMonth], status_code=201)
def bulk_create_financial_plan_months(
    plan_uuid: str,
    bulk_data: financial_plan_models.FinancialPlanMonthBulkCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id),
):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")

    # Collect all unique category UUIDs across all months' expenses
    unique_cat_uuids = set()
    for month in bulk_data.months:
        for expense in month.expenses:
            unique_cat_uuids.add(expense.category_uuid)

    # Batch-resolve category UUIDs
    resolved_category_ids: dict[str, int] = {}
    if unique_cat_uuids:
        cats = crud_category.read_db_categories_by_uuids(db, list(unique_cat_uuids))
        resolved_category_ids = {str(c.uuid): c.id for c in cats}
        missing = unique_cat_uuids - {UUID(k) for k in resolved_category_ids}
        if missing:
            raise HTTPException(status_code=404, detail=f"Categories not found: {', '.join(str(u) for u in missing)}")

    try:
        return crud_financial_plan.bulk_create_financial_plan_months(
            db=db,
            plan_id=db_plan.plan_id,
            months=bulk_data.months,
            resolved_category_ids=resolved_category_ids,
        )
    except IntegrityError:
        db.rollback()
        raise HTTPException(status_code=409, detail="Duplicate year/month in plan")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/{plan_uuid}/months", response_model=List[financial_plan_models.FinancialPlanMonth])
def get_financial_plan_months(plan_uuid: str, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    parsed_uuid = _parse_uuid(plan_uuid)
    db_plan = crud_financial_plan.get_financial_plan_by_uuid(db, user_id=user_id, plan_uuid=parsed_uuid)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.get_financial_plan_months(db=db, plan_id=db_plan.plan_id)

@router.put("/months/{month_uuid}", response_model=financial_plan_models.FinancialPlanMonth)
def update_financial_plan_month(month_uuid: str, month_data: financial_plan_models.FinancialPlanMonthUpdate, db: Session = Depends(get_db)):
    parsed_uuid = _parse_uuid(month_uuid)
    db_month = crud_financial_plan.get_financial_plan_month_by_uuid(db, month_uuid=parsed_uuid)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    return crud_financial_plan.update_financial_plan_month(db=db, db_month=db_month, month_in=month_data)

@router.delete("/months/{month_uuid}", status_code=204)
def delete_financial_plan_month(month_uuid: str, db: Session = Depends(get_db)):
    parsed_uuid = _parse_uuid(month_uuid)
    db_month = crud_financial_plan.get_financial_plan_month_by_uuid(db, month_uuid=parsed_uuid)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    crud_financial_plan.delete_financial_plan_month(db=db, db_month=db_month)
    return

@router.post("/months/{month_uuid}/expenses", response_model=financial_plan_models.FinancialPlanExpense, status_code=201)
def create_financial_plan_expense(month_uuid: str, expense: financial_plan_models.FinancialPlanExpenseCreate, db: Session = Depends(get_db)):
    parsed_uuid = _parse_uuid(month_uuid)
    db_month = crud_financial_plan.get_financial_plan_month_by_uuid(db, month_uuid=parsed_uuid)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")

    # Resolve category UUID
    cat = crud_category.read_db_category_by_uuid(db, expense.category_uuid)
    if not cat:
        raise HTTPException(status_code=404, detail="Category not found")

    return crud_financial_plan.create_financial_plan_expense(db=db, month_id=db_month.month_id, expense=expense, category_id=cat.id)

@router.post("/months/{month_uuid}/expenses/bulk", response_model=List[financial_plan_models.FinancialPlanExpense], status_code=201)
def bulk_create_financial_plan_expenses(month_uuid: str, bulk_data: financial_plan_models.FinancialPlanExpenseBulkCreate, db: Session = Depends(get_db)):
    """
    Bulk create multiple expenses for a financial plan month in a single request.
    All expenses are created in a single database transaction.
    """
    parsed_uuid = _parse_uuid(month_uuid)
    db_month = crud_financial_plan.get_financial_plan_month_by_uuid(db, month_uuid=parsed_uuid)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")

    # Resolve category UUIDs
    category_ids = []
    for expense in bulk_data.expenses:
        cat = crud_category.read_db_category_by_uuid(db, expense.category_uuid)
        if not cat:
            raise HTTPException(status_code=404, detail=f"Category not found: {expense.category_uuid}")
        category_ids.append(cat.id)

    try:
        created_expenses = crud_financial_plan.bulk_create_financial_plan_expenses(
            db=db, month_id=db_month.month_id, expenses=bulk_data.expenses, category_ids=category_ids
        )
        return created_expenses
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/months/{month_uuid}/expenses", response_model=List[financial_plan_models.FinancialPlanExpense])
def get_financial_plan_expenses(month_uuid: str, db: Session = Depends(get_db)):
    parsed_uuid = _parse_uuid(month_uuid)
    db_month = crud_financial_plan.get_financial_plan_month_by_uuid(db, month_uuid=parsed_uuid)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    return crud_financial_plan.get_financial_plan_expenses(db=db, month_id=db_month.month_id)

@router.put("/expenses/{expense_uuid}", response_model=financial_plan_models.FinancialPlanExpense)
def update_financial_plan_expense(expense_uuid: str, expense: financial_plan_models.FinancialPlanExpenseUpdate, db: Session = Depends(get_db)):
    parsed_uuid = _parse_uuid(expense_uuid)
    db_expense = crud_financial_plan.get_financial_plan_expense_by_uuid(db, expense_uuid=parsed_uuid)
    if db_expense is None:
        raise HTTPException(status_code=404, detail="Financial plan expense not found")

    # Resolve category UUID if provided
    category_id = None
    update_data = expense.model_dump(exclude_unset=True)
    if 'category_uuid' in update_data and expense.category_uuid is not None:
        cat = crud_category.read_db_category_by_uuid(db, expense.category_uuid)
        if not cat:
            raise HTTPException(status_code=404, detail="Category not found")
        category_id = cat.id

    return crud_financial_plan.update_financial_plan_expense(db=db, db_expense=db_expense, expense_in=expense, category_id=category_id)

@router.delete("/expenses/{expense_uuid}", status_code=204)
def delete_financial_plan_expense(expense_uuid: str, db: Session = Depends(get_db)):
    parsed_uuid = _parse_uuid(expense_uuid)
    db_expense = crud_financial_plan.get_financial_plan_expense_by_uuid(db, expense_uuid=parsed_uuid)
    if db_expense is None:
        raise HTTPException(status_code=404, detail="Financial plan expense not found")
    crud_financial_plan.delete_financial_plan_expense(db=db, db_expense=db_expense)
    return
