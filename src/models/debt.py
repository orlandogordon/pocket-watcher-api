from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal
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

# ===== DEBT PAYMENT MODELS =====

class DebtPaymentCreate(BaseModel):
    loan_account_id: int
    payment_source_account_id: Optional[int] = None
    transaction_id: Optional[int] = None
    payment_amount: Decimal
    principal_amount: Optional[Decimal] = None
    interest_amount: Optional[Decimal] = None
    remaining_balance_after_payment: Optional[Decimal] = None
    payment_date: date
    description: Optional[str] = Field(None, max_length=500)

class DebtPaymentUpdate(BaseModel):
    payment_source_account_id: Optional[int] = None
    transaction_id: Optional[int] = None
    payment_amount: Optional[Decimal] = None
    principal_amount: Optional[Decimal] = None
    interest_amount: Optional[Decimal] = None
    remaining_balance_after_payment: Optional[Decimal] = None
    payment_date: Optional[date] = None
    description: Optional[str] = Field(None, max_length=500)

class DebtPaymentResponse(BaseModel):
    payment_id: int
    loan_account_id: int
    payment_source_account_id: Optional[int]
    transaction_id: Optional[int]
    payment_amount: Decimal
    principal_amount: Optional[Decimal]
    interest_amount: Optional[Decimal]
    remaining_balance_after_payment: Optional[Decimal]
    payment_date: date
    description: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True
