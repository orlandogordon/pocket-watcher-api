from pydantic import BaseModel
from typing import List, Optional
from decimal import Decimal
from datetime import datetime

# Financial Plan Entry Models

class FinancialPlanEntryBase(BaseModel):
    category_id: int
    monthly_amount: Decimal

class FinancialPlanEntryCreate(FinancialPlanEntryBase):
    pass

class FinancialPlanEntryBulkCreate(BaseModel):
    entries: List[FinancialPlanEntryCreate]

class FinancialPlanEntryUpdate(BaseModel):
    monthly_amount: Optional[Decimal] = None

class FinancialPlanEntry(FinancialPlanEntryBase):
    entry_id: int
    plan_id: int
    created_at: datetime

    class Config:
        from_attributes = True

# Financial Plan Models

class FinancialPlanBase(BaseModel):
    plan_name: str
    monthly_income: Decimal

class FinancialPlanCreate(FinancialPlanBase):
    pass

class FinancialPlanUpdate(BaseModel):
    plan_name: Optional[str] = None
    monthly_income: Optional[Decimal] = None

class FinancialPlan(FinancialPlanBase):
    plan_id: int
    user_id: int
    created_at: datetime
    updated_at: datetime
    entries: List[FinancialPlanEntry] = []

    class Config:
        from_attributes = True

# Summary Model

class FinancialPlanSummary(BaseModel):
    total_income: Decimal
    total_expenses: Decimal
    net_monthly_surplus: Decimal
