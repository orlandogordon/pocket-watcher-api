from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from uuid import UUID

from src.models.category import CategoryResponse


# ===== BUDGET TEMPLATE PYDANTIC MODELS =====

class TemplateCategoryCreate(BaseModel):
    category_uuid: UUID = Field(..., description="The UUID of the parent category")
    subcategory_uuid: Optional[UUID] = Field(None, description="Optional UUID of the subcategory")
    allocated_amount: Decimal = Field(..., ge=0, description="Allocated budget amount")

    @field_validator('allocated_amount')
    @classmethod
    def validate_allocated_amount(cls, v: Decimal) -> Decimal:
        return round(v, 2)


class TemplateCategoryUpdate(BaseModel):
    allocated_amount: Decimal = Field(..., ge=0, description="Allocated budget amount")

    @field_validator('allocated_amount')
    @classmethod
    def validate_allocated_amount(cls, v: Decimal) -> Decimal:
        return round(v, 2)


class TemplateCategoryResponse(BaseModel):
    id: UUID
    category: CategoryResponse
    subcategory: Optional[CategoryResponse] = None
    allocated_amount: Decimal
    created_at: datetime

    class Config:
        from_attributes = True


class TemplateCreate(BaseModel):
    template_name: str = Field(..., min_length=1, max_length=255, description="Template name")
    is_default: bool = Field(False, description="Whether this is the default template")
    categories: List[TemplateCategoryCreate] = Field(default_factory=list, description="Template categories")

    @field_validator('template_name')
    @classmethod
    def validate_template_name(cls, v: str) -> str:
        return v.strip()


class TemplateUpdate(BaseModel):
    template_name: Optional[str] = Field(None, min_length=1, max_length=255)
    is_default: Optional[bool] = None

    @field_validator('template_name')
    @classmethod
    def validate_template_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v


class TemplateResponse(BaseModel):
    id: UUID
    template_name: str
    is_default: bool
    created_at: datetime
    updated_at: datetime
    categories: Optional[List[TemplateCategoryResponse]] = None

    class Config:
        from_attributes = True


# ===== BUDGET MONTH PYDANTIC MODELS =====

class BudgetMonthUpdate(BaseModel):
    template_uuid: Optional[UUID] = Field(None, description="UUID of the template to assign (null to unassign)")


class BudgetMonthCategorySpending(BaseModel):
    category: CategoryResponse
    subcategory: Optional[CategoryResponse] = None
    allocated_amount: Decimal
    spent_amount: Decimal
    remaining_amount: Decimal
    percentage_used: float


class BudgetMonthResponse(BaseModel):
    id: UUID
    year: int
    month: int
    template: Optional[TemplateResponse] = None
    categories: Optional[List[BudgetMonthCategorySpending]] = None
    total_allocated: Optional[Decimal] = None
    total_spent: Optional[Decimal] = None
    total_remaining: Optional[Decimal] = None
    percentage_used: Optional[float] = None
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ===== STATS / PERFORMANCE =====

class BudgetMonthStats(BaseModel):
    id: UUID
    year: int
    month: int
    template_name: Optional[str] = None
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


class BudgetMonthPerformance(BaseModel):
    category_uuid: UUID
    category_name: str
    subcategory_uuid: Optional[UUID] = None
    subcategory_name: Optional[str] = None
    allocated_amount: Decimal
    spent_amount: Decimal
    remaining_amount: Decimal
    percentage_used: float
    status: str
    daily_average: Decimal
    projected_spend: Decimal
