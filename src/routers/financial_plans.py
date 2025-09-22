from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List

from src.db.core import get_db, NotFoundError
from src.crud import crud_financial_plan
from src.models import financial_plan as financial_plan_models

# This is a placeholder for your actual authentication logic
def get_current_user_id():
    return 1

router = APIRouter(
    prefix="/financial_plans",
    tags=["financial_plans"],
    responses={404: {"description": "Not found"}},
)

@router.post("/", response_model=financial_plan_models.FinancialPlan)
def create_financial_plan(plan: financial_plan_models.FinancialPlanCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    return crud_financial_plan.create_financial_plan(db=db, user_id=user_id, plan=plan)

@router.get("/", response_model=List[financial_plan_models.FinancialPlan])
def read_financial_plans(skip: int = 0, limit: int = 100, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    plans = crud_financial_plan.get_financial_plans(db, user_id=user_id, skip=skip, limit=limit)
    return plans

@router.get("/{plan_id}", response_model=financial_plan_models.FinancialPlan)
def read_financial_plan(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return db_plan

@router.put("/{plan_id}", response_model=financial_plan_models.FinancialPlan)
def update_financial_plan(plan_id: int, plan: financial_plan_models.FinancialPlanUpdate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.update_financial_plan(db=db, db_plan=db_plan, plan_in=plan)

@router.delete("/{plan_id}", status_code=204)
def delete_financial_plan(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    crud_financial_plan.delete_financial_plan(db=db, db_plan=db_plan)
    return

@router.get("/{plan_id}/summary", response_model=financial_plan_models.FinancialPlanSummary)
def get_financial_plan_summary(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.get_financial_plan_summary(db_plan)

@router.post("/{plan_id}/months", response_model=financial_plan_models.FinancialPlanMonth)
def create_financial_plan_month(plan_id: int, month_data: financial_plan_models.FinancialPlanMonthCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.create_financial_plan_month(db=db, plan_id=plan_id, month_data=month_data)

@router.get("/{plan_id}/months", response_model=List[financial_plan_models.FinancialPlanMonth])
def get_financial_plan_months(plan_id: int, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    db_plan = crud_financial_plan.get_financial_plan(db, user_id=user_id, plan_id=plan_id)
    if db_plan is None:
        raise HTTPException(status_code=404, detail="Financial plan not found")
    return crud_financial_plan.get_financial_plan_months(db=db, plan_id=plan_id)

@router.put("/months/{month_id}", response_model=financial_plan_models.FinancialPlanMonth)
def update_financial_plan_month(month_id: int, month_data: financial_plan_models.FinancialPlanMonthUpdate, db: Session = Depends(get_db)):
    db_month = crud_financial_plan.get_financial_plan_month(db, month_id=month_id)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    # TODO: Add user ownership check here
    return crud_financial_plan.update_financial_plan_month(db=db, db_month=db_month, month_in=month_data)

@router.delete("/months/{month_id}", status_code=204)
def delete_financial_plan_month(month_id: int, db: Session = Depends(get_db)):
    db_month = crud_financial_plan.get_financial_plan_month(db, month_id=month_id)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    # TODO: Add user ownership check here
    crud_financial_plan.delete_financial_plan_month(db=db, db_month=db_month)
    return

@router.post("/months/{month_id}/expenses", response_model=financial_plan_models.FinancialPlanExpense)
def create_financial_plan_expense(month_id: int, expense: financial_plan_models.FinancialPlanExpenseCreate, db: Session = Depends(get_db)):
    db_month = crud_financial_plan.get_financial_plan_month(db, month_id=month_id)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    # TODO: Add user ownership check here
    return crud_financial_plan.create_financial_plan_expense(db=db, month_id=month_id, expense=expense)

@router.get("/months/{month_id}/expenses", response_model=List[financial_plan_models.FinancialPlanExpense])
def get_financial_plan_expenses(month_id: int, db: Session = Depends(get_db)):
    db_month = crud_financial_plan.get_financial_plan_month(db, month_id=month_id)
    if db_month is None:
        raise HTTPException(status_code=404, detail="Financial plan month not found")
    # TODO: Add user ownership check here
    return crud_financial_plan.get_financial_plan_expenses(db=db, month_id=month_id)

@router.put("/expenses/{expense_id}", response_model=financial_plan_models.FinancialPlanExpense)
def update_financial_plan_expense(expense_id: int, expense: financial_plan_models.FinancialPlanExpenseUpdate, db: Session = Depends(get_db)):
    db_expense = crud_financial_plan.get_financial_plan_expense(db, expense_id=expense_id)
    if db_expense is None:
        raise HTTPException(status_code=404, detail="Financial plan expense not found")
    # TODO: Add user ownership check here
    return crud_financial_plan.update_financial_plan_expense(db=db, db_expense=db_expense, expense_in=expense)

@router.delete("/expenses/{expense_id}", status_code=204)
def delete_financial_plan_expense(expense_id: int, db: Session = Depends(get_db)):
    db_expense = crud_financial_plan.get_financial_plan_expense(db, expense_id=expense_id)
    if db_expense is None:
        raise HTTPException(status_code=404, detail="Financial plan expense not found")
    # TODO: Add user ownership check here
    crud_financial_plan.delete_financial_plan_expense(db=db, db_expense=db_expense)
    return
