from pydantic import BaseModel, Field, model_validator
from typing import Optional, List
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from uuid import UUID

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
    id: UUID
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
    plan_uuid: UUID
    account_uuid: UUID
    priority: int = 0

class DebtPlanAccountLinkResponse(BaseModel):
    account_uuid: UUID

    class Config:
        from_attributes = True

# ===== DEBT REPAYMENT SCHEDULE MODELS =====

class MonthlyPaymentSchedule(BaseModel):
    payment_month: date
    scheduled_payment_amount: Decimal

class DebtRepaymentScheduleBulkCreate(BaseModel):
    account_uuid: UUID
    schedules: List[MonthlyPaymentSchedule]

class DebtRepaymentScheduleResponse(BaseModel):
    id: UUID
    account_uuid: UUID
    payment_month: date
    scheduled_payment_amount: Decimal

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'account') and data.account:
                data.__dict__['account_uuid'] = data.account.uuid
        return data

# ===== DEBT PAYMENT MODELS =====

class DebtPaymentCreate(BaseModel):
    loan_account_uuid: UUID
    payment_source_account_uuid: Optional[UUID] = None
    transaction_uuid: Optional[UUID] = None
    payment_amount: Decimal
    principal_amount: Optional[Decimal] = None
    interest_amount: Optional[Decimal] = None
    remaining_balance_after_payment: Optional[Decimal] = None
    payment_date: date
    description: Optional[str] = Field(None, max_length=500)

class DebtPaymentBulkCreate(BaseModel):
    payments: List[DebtPaymentCreate]

class DebtPaymentUpdate(BaseModel):
    payment_source_account_uuid: Optional[UUID] = None
    transaction_uuid: Optional[UUID] = None
    payment_amount: Optional[Decimal] = None
    principal_amount: Optional[Decimal] = None
    interest_amount: Optional[Decimal] = None
    remaining_balance_after_payment: Optional[Decimal] = None
    payment_date: Optional[date] = None
    description: Optional[str] = Field(None, max_length=500)

class DebtPaymentResponse(BaseModel):
    id: UUID
    loan_account_uuid: UUID
    payment_source_account_uuid: Optional[UUID] = None
    transaction_uuid: Optional[UUID] = None
    payment_amount: Decimal
    principal_amount: Optional[Decimal]
    interest_amount: Optional[Decimal]
    remaining_balance_after_payment: Optional[Decimal]
    payment_date: date
    description: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'loan_account') and data.loan_account:
                data.__dict__['loan_account_uuid'] = data.loan_account.uuid
            if hasattr(data, 'payment_source_account') and data.payment_source_account:
                data.__dict__['payment_source_account_uuid'] = data.payment_source_account.uuid
            if hasattr(data, 'transaction') and data.transaction:
                data.__dict__['transaction_uuid'] = data.transaction.id
        return data
