from pydantic import BaseModel, Field
from typing import List, Optional, Literal
from decimal import Decimal
from datetime import datetime, date

# Financial Plan Expense Models

class FinancialPlanExpenseBase(BaseModel):
    category_id: int
    description: str = Field(..., min_length=1, max_length=255)
    amount: Decimal
    expense_type: Literal["recurring", "one_time"]

class FinancialPlanExpenseCreate(FinancialPlanExpenseBase):
    pass

class FinancialPlanExpenseUpdate(BaseModel):
    category_id: Optional[int] = None
    description: Optional[str] = Field(None, min_length=1, max_length=255)
    amount: Optional[Decimal] = None
    expense_type: Optional[Literal["recurring", "one_time"]] = None

class FinancialPlanExpense(FinancialPlanExpenseBase):
    expense_id: int
    month_id: int
    created_at: datetime

    class Config:
        from_attributes = True

# Financial Plan Month Models

class FinancialPlanMonthBase(BaseModel):
    year: int = Field(..., ge=2000, le=3000)
    month: int = Field(..., ge=1, le=12)
    planned_income: Decimal

class FinancialPlanMonthCreate(FinancialPlanMonthBase):
    expenses: List[FinancialPlanExpenseCreate] = []

class FinancialPlanMonthUpdate(BaseModel):
    planned_income: Optional[Decimal] = None

class FinancialPlanMonth(FinancialPlanMonthBase):
    month_id: int
    plan_id: int
    created_at: datetime
    expenses: List[FinancialPlanExpense] = []

    class Config:
        from_attributes = True

# Financial Plan Models

class FinancialPlanBase(BaseModel):
    plan_name: str = Field(..., min_length=1, max_length=255)
    start_date: date
    end_date: date

class FinancialPlanCreate(FinancialPlanBase):
    pass

class FinancialPlanUpdate(BaseModel):
    plan_name: Optional[str] = Field(None, min_length=1, max_length=255)
    start_date: Optional[date] = None
    end_date: Optional[date] = None

class FinancialPlan(FinancialPlanBase):
    plan_id: int
    user_id: int
    created_at: datetime
    updated_at: datetime
    monthly_periods: List[FinancialPlanMonth] = []

    class Config:
        from_attributes = True

# Summary Models

class MonthlyPlanSummary(BaseModel):
    year: int
    month: int
    planned_income: Decimal
    total_expenses: Decimal
    net_surplus: Decimal

class FinancialPlanSummary(BaseModel):
    plan_id: int
    plan_name: str
    start_date: date
    end_date: date
    total_months: int
    total_planned_income: Decimal
    total_planned_expenses: Decimal
    total_net_surplus: Decimal
    monthly_summaries: List[MonthlyPlanSummary] = []
