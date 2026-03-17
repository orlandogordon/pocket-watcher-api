import calendar
from datetime import date

from sqlalchemy.orm import Session
from decimal import Decimal
from typing import List, Optional, Dict
from uuid import UUID, uuid4

from src.db.core import FinancialPlanDB, FinancialPlanMonthDB, FinancialPlanExpenseDB, NotFoundError
from src.models.financial_plan import (
    FinancialPlanCreate, FinancialPlanUpdate,
    FinancialPlanMonthCreate, FinancialPlanMonthUpdate,
    FinancialPlanExpenseCreate, FinancialPlanExpenseUpdate
)


def _sync_plan_dates(db: Session, plan_id: int):
    """Recompute plan start_date/end_date from its months. No-op if no months exist."""
    months = (
        db.query(FinancialPlanMonthDB.year, FinancialPlanMonthDB.month)
        .filter(FinancialPlanMonthDB.plan_id == plan_id)
        .all()
    )
    if not months:
        return
    earliest = min(months, key=lambda m: (m.year, m.month))
    latest = max(months, key=lambda m: (m.year, m.month))
    plan = db.query(FinancialPlanDB).filter(FinancialPlanDB.plan_id == plan_id).first()
    plan.start_date = date(earliest.year, earliest.month, 1)
    _, last_day = calendar.monthrange(latest.year, latest.month)
    plan.end_date = date(latest.year, latest.month, last_day)
    db.commit()

# Financial Plan CRUD

def create_financial_plan(db: Session, user_id: int, plan: FinancialPlanCreate) -> FinancialPlanDB:
    db_plan = FinancialPlanDB(id=uuid4(), **plan.model_dump(), user_id=user_id)
    db.add(db_plan)
    db.commit()
    db.refresh(db_plan)
    return db_plan

def get_financial_plan(db: Session, user_id: int, plan_id: int) -> Optional[FinancialPlanDB]:
    return db.query(FinancialPlanDB).filter(FinancialPlanDB.plan_id == plan_id, FinancialPlanDB.user_id == user_id).first()

def get_financial_plans(db: Session, user_id: int, skip: int = 0, limit: int = 100) -> List[FinancialPlanDB]:
    return db.query(FinancialPlanDB).filter(FinancialPlanDB.user_id == user_id).offset(skip).limit(limit).all()

def update_financial_plan(db: Session, db_plan: FinancialPlanDB, plan_in: FinancialPlanUpdate) -> FinancialPlanDB:
    update_data = plan_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_plan, key, value)
    db.commit()
    db.refresh(db_plan)
    return db_plan

def delete_financial_plan(db: Session, db_plan: FinancialPlanDB):
    db.delete(db_plan)
    db.commit()

# Financial Plan Month CRUD

def create_financial_plan_month(db: Session, plan_id: int, month_data: FinancialPlanMonthCreate, *, resolved_category_ids: Optional[Dict[str, int]] = None) -> FinancialPlanMonthDB:
    month_dict = month_data.model_dump(exclude={'expenses'})
    db_month = FinancialPlanMonthDB(id=uuid4(), **month_dict, plan_id=plan_id)
    db.add(db_month)
    db.commit()
    db.refresh(db_month)

    # Add expenses if provided
    for expense_data in month_data.expenses:
        cat_id = resolved_category_ids.get(str(expense_data.category_uuid)) if resolved_category_ids else None
        if cat_id is None:
            raise ValueError(f"Category UUID {expense_data.category_uuid} was not resolved")
        create_financial_plan_expense(db, db_month.month_id, expense_data, category_id=cat_id)

    # Refresh to get the expenses
    db.refresh(db_month)
    _sync_plan_dates(db, plan_id)
    return db_month

def get_financial_plan_month(db: Session, month_id: int) -> Optional[FinancialPlanMonthDB]:
    return db.query(FinancialPlanMonthDB).filter(FinancialPlanMonthDB.month_id == month_id).first()

def get_financial_plan_months(db: Session, plan_id: int) -> List[FinancialPlanMonthDB]:
    return db.query(FinancialPlanMonthDB).filter(FinancialPlanMonthDB.plan_id == plan_id).order_by(FinancialPlanMonthDB.year, FinancialPlanMonthDB.month).all()

def update_financial_plan_month(db: Session, db_month: FinancialPlanMonthDB, month_in: FinancialPlanMonthUpdate) -> FinancialPlanMonthDB:
    update_data = month_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_month, key, value)
    db.commit()
    db.refresh(db_month)
    return db_month

def delete_financial_plan_month(db: Session, db_month: FinancialPlanMonthDB):
    plan_id = db_month.plan_id
    db.delete(db_month)
    db.commit()
    _sync_plan_dates(db, plan_id)

def bulk_create_financial_plan_months(
    db: Session,
    plan_id: int,
    months: List[FinancialPlanMonthCreate],
    *,
    resolved_category_ids: Dict[str, int],
) -> List[FinancialPlanMonthDB]:
    """Bulk create multiple months (with expenses) in a single transaction."""
    db_months = []
    try:
        for month_data in months:
            month_dict = month_data.model_dump(exclude={'expenses'})
            db_month = FinancialPlanMonthDB(id=uuid4(), **month_dict, plan_id=plan_id)
            db.add(db_month)
            db.flush()  # get month_id for expense FK

            for expense_data in month_data.expenses:
                cat_id = resolved_category_ids.get(str(expense_data.category_uuid))
                if cat_id is None:
                    raise ValueError(f"Category UUID {expense_data.category_uuid} was not resolved")
                db_expense = FinancialPlanExpenseDB(
                    id=uuid4(),
                    month_id=db_month.month_id,
                    category_id=cat_id,
                    description=expense_data.description,
                    amount=expense_data.amount,
                    expense_type=expense_data.expense_type,
                )
                db.add(db_expense)

            db_months.append(db_month)

        db.commit()
        for db_month in db_months:
            db.refresh(db_month)
        _sync_plan_dates(db, plan_id)
        return db_months
    except Exception as e:
        db.rollback()
        raise


# Financial Plan Expense CRUD

def create_financial_plan_expense(db: Session, month_id: int, expense: FinancialPlanExpenseCreate, *, category_id: int) -> FinancialPlanExpenseDB:
    db_expense = FinancialPlanExpenseDB(
        id=uuid4(),
        month_id=month_id,
        category_id=category_id,
        description=expense.description,
        amount=expense.amount,
        expense_type=expense.expense_type,
    )
    db.add(db_expense)
    db.commit()
    db.refresh(db_expense)
    return db_expense

def bulk_create_financial_plan_expenses(db: Session, month_id: int, expenses: List[FinancialPlanExpenseCreate], *, category_ids: List[int]) -> List[FinancialPlanExpenseDB]:
    """
    Bulk create multiple financial plan expenses for a given month.
    All expenses are created in a single database transaction.
    """
    db_expenses = []

    try:
        for i, expense in enumerate(expenses):
            db_expense = FinancialPlanExpenseDB(
                id=uuid4(),
                month_id=month_id,
                category_id=category_ids[i],
                description=expense.description,
                amount=expense.amount,
                expense_type=expense.expense_type,
            )
            db.add(db_expense)
            db_expenses.append(db_expense)

        db.commit()

        # Refresh all created expenses
        for db_expense in db_expenses:
            db.refresh(db_expense)

        return db_expenses
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk expense creation failed: {str(e)}")

def get_financial_plan_expense(db: Session, expense_id: int) -> Optional[FinancialPlanExpenseDB]:
    return db.query(FinancialPlanExpenseDB).filter(FinancialPlanExpenseDB.expense_id == expense_id).first()

def get_financial_plan_expenses(db: Session, month_id: int) -> List[FinancialPlanExpenseDB]:
    return db.query(FinancialPlanExpenseDB).filter(FinancialPlanExpenseDB.month_id == month_id).all()

def update_financial_plan_expense(db: Session, db_expense: FinancialPlanExpenseDB, expense_in: FinancialPlanExpenseUpdate, *, category_id: Optional[int] = None) -> FinancialPlanExpenseDB:
    update_data = expense_in.model_dump(exclude_unset=True)
    # Remove UUID field, use resolved int ID instead
    update_data.pop('category_uuid', None)
    if category_id is not None:
        update_data['category_id'] = category_id
    for key, value in update_data.items():
        setattr(db_expense, key, value)
    db.commit()
    db.refresh(db_expense)
    return db_expense

def delete_financial_plan_expense(db: Session, db_expense: FinancialPlanExpenseDB):
    db.delete(db_expense)
    db.commit()

# Financial Plan Summary

def get_financial_plan_summary(db_plan: FinancialPlanDB):
    from src.models.financial_plan import FinancialPlanSummary, MonthlyPlanSummary

    monthly_summaries = []
    total_income = Decimal('0')
    total_expenses = Decimal('0')

    for month in db_plan.monthly_periods:
        month_expenses = sum(expense.amount for expense in month.expenses)
        net_surplus = month.planned_income - month_expenses

        monthly_summaries.append(MonthlyPlanSummary(
            year=month.year,
            month=month.month,
            planned_income=month.planned_income,
            total_expenses=month_expenses,
            net_surplus=net_surplus
        ))

        total_income += month.planned_income
        total_expenses += month_expenses

    return FinancialPlanSummary(
        id=db_plan.id,
        plan_name=db_plan.plan_name,
        start_date=db_plan.start_date,
        end_date=db_plan.end_date,
        total_months=len(db_plan.monthly_periods),
        total_planned_income=total_income,
        total_planned_expenses=total_expenses,
        total_net_surplus=total_income - total_expenses,
        monthly_summaries=monthly_summaries
    )


# ===== UUID-BASED OPERATIONS =====

def get_financial_plan_by_uuid(db: Session, user_id: int, plan_uuid: UUID) -> Optional[FinancialPlanDB]:
    return db.query(FinancialPlanDB).filter(
        FinancialPlanDB.id == plan_uuid,
        FinancialPlanDB.user_id == user_id
    ).first()

def get_financial_plan_month_by_uuid(db: Session, month_uuid: UUID) -> Optional[FinancialPlanMonthDB]:
    return db.query(FinancialPlanMonthDB).filter(
        FinancialPlanMonthDB.id == month_uuid
    ).first()

def get_financial_plan_expense_by_uuid(db: Session, expense_uuid: UUID) -> Optional[FinancialPlanExpenseDB]:
    return db.query(FinancialPlanExpenseDB).filter(
        FinancialPlanExpenseDB.id == expense_uuid
    ).first()
