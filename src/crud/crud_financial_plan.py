from sqlalchemy.orm import Session
from decimal import Decimal
from typing import List, Optional

from src.db.core import FinancialPlanDB, FinancialPlanMonthDB, FinancialPlanExpenseDB, NotFoundError
from src.models.financial_plan import (
    FinancialPlanCreate, FinancialPlanUpdate,
    FinancialPlanMonthCreate, FinancialPlanMonthUpdate,
    FinancialPlanExpenseCreate, FinancialPlanExpenseUpdate
)

# Financial Plan CRUD

def create_financial_plan(db: Session, user_id: int, plan: FinancialPlanCreate) -> FinancialPlanDB:
    db_plan = FinancialPlanDB(**plan.model_dump(), user_id=user_id)
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

def create_financial_plan_month(db: Session, plan_id: int, month_data: FinancialPlanMonthCreate) -> FinancialPlanMonthDB:
    month_dict = month_data.model_dump(exclude={'expenses'})
    db_month = FinancialPlanMonthDB(**month_dict, plan_id=plan_id)
    db.add(db_month)
    db.commit()
    db.refresh(db_month)

    # Add expenses if provided
    for expense_data in month_data.expenses:
        create_financial_plan_expense(db, db_month.month_id, expense_data)

    # Refresh to get the expenses
    db.refresh(db_month)
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
    db.delete(db_month)
    db.commit()

# Financial Plan Expense CRUD

def create_financial_plan_expense(db: Session, month_id: int, expense: FinancialPlanExpenseCreate) -> FinancialPlanExpenseDB:
    db_expense = FinancialPlanExpenseDB(**expense.model_dump(), month_id=month_id)
    db.add(db_expense)
    db.commit()
    db.refresh(db_expense)
    return db_expense

def get_financial_plan_expense(db: Session, expense_id: int) -> Optional[FinancialPlanExpenseDB]:
    return db.query(FinancialPlanExpenseDB).filter(FinancialPlanExpenseDB.expense_id == expense_id).first()

def get_financial_plan_expenses(db: Session, month_id: int) -> List[FinancialPlanExpenseDB]:
    return db.query(FinancialPlanExpenseDB).filter(FinancialPlanExpenseDB.month_id == month_id).all()

def update_financial_plan_expense(db: Session, db_expense: FinancialPlanExpenseDB, expense_in: FinancialPlanExpenseUpdate) -> FinancialPlanExpenseDB:
    update_data = expense_in.model_dump(exclude_unset=True)
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
        plan_id=db_plan.plan_id,
        plan_name=db_plan.plan_name,
        start_date=db_plan.start_date,
        end_date=db_plan.end_date,
        total_months=len(db_plan.monthly_periods),
        total_planned_income=total_income,
        total_planned_expenses=total_expenses,
        total_net_surplus=total_income - total_expenses,
        monthly_summaries=monthly_summaries
    )
