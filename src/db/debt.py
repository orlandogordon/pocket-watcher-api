from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID

from .core import (
    AccountDB,
    UserDB,
    DebtRepaymentPlanDB,
    DebtPlanAccountLinkDB,
    DebtRepaymentScheduleDB,
    DebtStrategy,
    NotFoundError
)
from pydantic import BaseModel, Field
from enum import Enum

# ===== ENUMS =====

class DebtStrategyEnum(str, Enum):
    AVALANCHE = "AVALANCHE"
    SNOWBALL = "SNOWBALL"
    CUSTOM = "CUSTOM"

# ===== DEBT REPAYMENT PLAN MODELS =====

class DebtRepaymentPlanCreate(BaseModel):
    plan_name: str = Field(..., max_length=255)
    strategy: DebtStrategyEnum = Field(default=DebtStrategyEnum.CUSTOM)
    target_payoff_date: Optional[date] = None

class DebtRepaymentPlanUpdate(BaseModel):
    plan_name: Optional[str] = Field(None, max_length=255)
    strategy: Optional[DebtStrategyEnum] = None
    target_payoff_date: Optional[date] = None
    status: Optional[str] = Field(None, max_length=50)

class DebtRepaymentPlanResponse(BaseModel):
    plan_id: int
    user_id: int
    plan_name: str
    strategy: DebtStrategyEnum
    target_payoff_date: Optional[date]
    status: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

# ===== PLAN-ACCOUNT LINK MODELS =====

class DebtPlanAccountLinkCreate(BaseModel):
    plan_id: int
    account_id: int
    priority: int = 0

# ===== DEBT REPAYMENT SCHEDULE MODELS =====

class MonthlyPaymentSchedule(BaseModel):
    payment_month: date
    scheduled_payment_amount: Decimal

class DebtRepaymentScheduleBulkCreate(BaseModel):
    account_id: int
    schedules: List[MonthlyPaymentSchedule]

class DebtRepaymentScheduleResponse(BaseModel):
    schedule_id: int
    account_id: int
    payment_month: date
    scheduled_payment_amount: Decimal

    class Config:
        from_attributes = True

# ===== DATABASE OPERATIONS - PLANS =====

def create_debt_repayment_plan(db: Session, user_id: int, plan_data: DebtRepaymentPlanCreate) -> DebtRepaymentPlanDB:
    db_plan = DebtRepaymentPlanDB(
        **plan_data.model_dump(exclude={"strategy"}), 
        strategy=DebtStrategy(plan_data.strategy.value),
        user_id=user_id
    )
    db.add(db_plan)
    db.commit()
    db.refresh(db_plan)
    return db_plan

def read_debt_repayment_plan(db: Session, plan_id: int, user_id: int) -> Optional[DebtRepaymentPlanDB]:
    return db.query(DebtRepaymentPlanDB).filter(
        DebtRepaymentPlanDB.plan_id == plan_id, 
        DebtRepaymentPlanDB.user_id == user_id
    ).first()

def read_all_debt_repayment_plans_for_user(db: Session, user_id: int) -> List[DebtRepaymentPlanDB]:
    return db.query(DebtRepaymentPlanDB).filter(DebtRepaymentPlanDB.user_id == user_id).all()

def add_account_to_plan(db: Session, user_id: int, link_data: DebtPlanAccountLinkCreate) -> DebtPlanAccountLinkDB:
    plan = read_debt_repayment_plan(db, link_data.plan_id, user_id)
    if not plan:
        raise NotFoundError("Repayment plan not found.")

    account = db.query(AccountDB).filter(AccountDB.id == link_data.account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    db_link = DebtPlanAccountLinkDB(**link_data.model_dump())
    db.add(db_link)
    db.commit()
    db.refresh(db_link)
    return db_link

def remove_account_from_plan(db: Session, user_id: int, plan_id: int, account_id: int) -> bool:
    link = db.query(DebtPlanAccountLinkDB).join(DebtRepaymentPlanDB).filter(
        DebtRepaymentPlanDB.user_id == user_id,
        DebtPlanAccountLinkDB.plan_id == plan_id,
        DebtPlanAccountLinkDB.account_id == account_id
    ).first()

    if not link:
        raise NotFoundError("Account link to plan not found.")

    db.delete(link)
    db.commit()
    return True

# ===== DATABASE OPERATIONS - SCHEDULES =====

def bulk_create_or_update_schedule(db: Session, user_id: int, schedule_data: DebtRepaymentScheduleBulkCreate):
    account = db.query(AccountDB).filter(AccountDB.id == schedule_data.account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    # Delete existing schedule for this account to replace it
    db.query(DebtRepaymentScheduleDB).filter(
        DebtRepaymentScheduleDB.account_id == schedule_data.account_id,
        DebtRepaymentScheduleDB.user_id == user_id
    ).delete()

    new_schedules = []
    for schedule in schedule_data.schedules:
        new_schedules.append(
            DebtRepaymentScheduleDB(
                user_id=user_id,
                account_id=schedule_data.account_id,
                payment_month=schedule.payment_month,
                scheduled_payment_amount=schedule.scheduled_payment_amount
            )
        )
    
    db.bulk_save_objects(new_schedules)
    db.commit()
    return len(new_schedules)

def read_schedule_for_account(db: Session, user_id: int, account_id: int) -> List[DebtRepaymentScheduleDB]:
    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError("Account not found.")

    return db.query(DebtRepaymentScheduleDB).filter(
        DebtRepaymentScheduleDB.account_id == account_id
    ).order_by(DebtRepaymentScheduleDB.payment_month).all()
