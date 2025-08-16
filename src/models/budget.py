from pydantic import BaseModel, Field, field_validator, computed_field
from typing import Optional, List
from datetime import datetime, date
from decimal import Decimal

from src.models.category import CategoryResponse

# ===== BUDGET PYDANTIC MODELS =====

class BudgetCategoryCreate(BaseModel):
    category_id: int = Field(..., description="The ID of the category")
    allocated_amount: Decimal = Field(..., ge=0, description="Allocated budget amount")

    @field_validator('allocated_amount')
    @classmethod
    def validate_allocated_amount(cls, v: Decimal) -> Decimal:
        return round(v, 2)

class BudgetCategoryUpdate(BaseModel):
    allocated_amount: Decimal = Field(..., ge=0, description="Allocated budget amount")

    @field_validator('allocated_amount')
    @classmethod
    def validate_allocated_amount(cls, v: Decimal) -> Decimal:
        return round(v, 2)

class BudgetCategoryResponse(BaseModel):
    budget_category_id: int
    budget_id: int
    category_id: int
    allocated_amount: Decimal
    spent_amount: Optional[Decimal] = None
    remaining_amount: Optional[Decimal] = None
    percentage_used: Optional[float] = None
    created_at: datetime
    category: CategoryResponse

    class Config:
        from_attributes = True

class BudgetCreate(BaseModel):
    budget_name: str = Field(..., min_length=1, max_length=255, description="Budget name")
    start_date: date = Field(..., description="Budget start date")
    end_date: date = Field(..., description="Budget end date")
    categories: List[BudgetCategoryCreate] = Field(..., min_items=1, description="Budget categories")

    @field_validator('budget_name')
    @classmethod
    def validate_budget_name(cls, v: str) -> str:
        return v.strip()

    @field_validator('end_date')
    @classmethod
    def validate_end_date(cls, v: date, info) -> date:
        if 'start_date' in info.data and v <= info.data['start_date']:
            raise ValueError('end_date must be after start_date')
        return v

    @field_validator('categories')
    @classmethod
    def validate_categories(cls, v: List[BudgetCategoryCreate]) -> List[BudgetCategoryCreate]:
        # Check for duplicate category IDs
        category_ids = [cat.category_id for cat in v]
        if len(category_ids) != len(set(category_ids)):
            raise ValueError('Duplicate category IDs are not allowed')
        return v

class BudgetUpdate(BaseModel):
    budget_name: Optional[str] = Field(None, min_length=1, max_length=255)
    start_date: Optional[date] = None
    end_date: Optional[date] = None

    @field_validator('budget_name')
    @classmethod
    def validate_budget_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

class BudgetResponse(BaseModel):
    budget_id: int
    budget_name: str
    start_date: date
    end_date: date
    total_allocated: Optional[Decimal] = None
    total_spent: Optional[Decimal] = None
    total_remaining: Optional[Decimal] = None
    percentage_used: Optional[float] = None
    is_active: Optional[bool] = None  # Whether budget period is current
    created_at: datetime
    updated_at: datetime
    budget_categories: Optional[List[BudgetCategoryResponse]] = None

    class Config:
        from_attributes = True

class BudgetSummary(BaseModel):
    """Lightweight budget summary"""
    budget_id: int
    budget_name: str
    start_date: date
    end_date: date
    total_allocated: Decimal
    total_spent: Decimal
    percentage_used: float
    is_active: bool

    class Config:
        from_attributes = True

class BudgetStats(BaseModel):
    """Budget statistics and insights"""
    budget_id: int
    budget_name: str
    period_days: int
    days_remaining: int
    categories_count: int
    categories_over_budget: int
    categories_on_track: int
    categories_under_budget: int
    biggest_overspend_category: Optional[str] = None
    biggest_overspend_amount: Optional[Decimal] = None
    most_efficient_category: Optional[str] = None
    daily_burn_rate: Decimal
    projected_total_spend: Decimal

class BudgetPerformance(BaseModel):
    """Budget performance analysis"""
    budget_id: int
    category_id: int
    category_name: str
    allocated_amount: Decimal
    spent_amount: Decimal
    remaining_amount: Decimal
    percentage_used: float
    status: str  # "over_budget", "on_track", "under_budget"
    daily_average: Decimal
    projected_spend: Decimal
