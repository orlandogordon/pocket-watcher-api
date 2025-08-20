from sqlalchemy.orm import Session
from decimal import Decimal
from typing import List, Optional

from src.db.core import FinancialPlanDB, FinancialPlanEntryDB, NotFoundError
from src.models.financial_plan import FinancialPlanCreate, FinancialPlanUpdate, FinancialPlanEntryCreate, FinancialPlanEntryUpdate, FinancialPlanEntryBulkCreate

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

# Financial Plan Entry CRUD

def create_financial_plan_entry(db: Session, plan_id: int, entry: FinancialPlanEntryCreate) -> FinancialPlanEntryDB:
    db_entry = FinancialPlanEntryDB(**entry.model_dump(), plan_id=plan_id)
    db.add(db_entry)
    db.commit()
    db.refresh(db_entry)
    return db_entry

def bulk_create_financial_plan_entries(db: Session, user_id: int, plan_id: int, bulk_data: FinancialPlanEntryBulkCreate) -> List[FinancialPlanEntryDB]:
    db_plan = get_financial_plan(db, user_id, plan_id)
    if not db_plan:
        raise NotFoundError(f"Financial plan with id {plan_id} not found")

    db_entries = [FinancialPlanEntryDB(**entry.model_dump(), plan_id=plan_id) for entry in bulk_data.entries]
    db.add_all(db_entries)
    db.commit()
    # No refresh for bulk operations, client can re-fetch if needed
    return db_entries

def get_financial_plan_entry(db: Session, entry_id: int) -> Optional[FinancialPlanEntryDB]:
    return db.query(FinancialPlanEntryDB).filter(FinancialPlanEntryDB.entry_id == entry_id).first()

def update_financial_plan_entry(db: Session, db_entry: FinancialPlanEntryDB, entry_in: FinancialPlanEntryUpdate) -> FinancialPlanEntryDB:
    update_data = entry_in.model_dump(exclude_unset=True)
    for key, value in update_data.items():
        setattr(db_entry, key, value)
    db.commit()
    db.refresh(db_entry)
    return db_entry

def delete_financial_plan_entry(db: Session, db_entry: FinancialPlanEntryDB):
    db.delete(db_entry)
    db.commit()

# Financial Plan Summary

def get_financial_plan_summary(db_plan: FinancialPlanDB):
    total_expenses = sum(entry.monthly_amount for entry in db_plan.entries)
    net_surplus = db_plan.monthly_income - total_expenses
    return {
        "total_income": db_plan.monthly_income,
        "total_expenses": total_expenses,
        "net_monthly_surplus": net_surplus
    }
