from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID
from enum import Enum

from src.models.category import CategoryResponse
from src.db.core import RelationshipType


class EmbeddedTagResponse(BaseModel):
    id: UUID
    tag_name: str
    color: Optional[str] = None
    is_system: bool = False
    model_config = ConfigDict(from_attributes=True)

# ===== TRANSACTION PYDANTIC MODELS =====

class TransactionTypeEnum(str, Enum):
    PURCHASE = "PURCHASE"
    CREDIT = "CREDIT"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    FEE = "FEE"
    INTEREST = "INTEREST"


class SourceTypeEnum(str, Enum):
    CSV = "CSV"
    PDF = "PDF"
    MANUAL = "MANUAL"
    API = "API"


class TransactionCreate(BaseModel):
    account_uuid: UUID = Field(..., description="Account UUID for this transaction")
    transaction_date: date = Field(..., description="Date of the transaction")
    amount: Decimal = Field(..., description="Transaction amount")
    transaction_type: TransactionTypeEnum = Field(..., description="Type of transaction")
    description: Optional[str] = Field(None, max_length=500, description="Transaction description")
    merchant_name: Optional[str] = Field(None, max_length=255, description="Merchant name")
    category_uuid: Optional[UUID] = Field(None, description="UUID of the transaction's category")
    subcategory_uuid: Optional[UUID] = Field(None, description="UUID of the transaction's sub-category")
    comments: Optional[str] = Field(None, description="User comments")
    source_type: SourceTypeEnum = Field(default=SourceTypeEnum.MANUAL, description="Source of transaction data")

    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: Decimal) -> Decimal:
        return round(v, 2)

    @field_validator('description')
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

    @field_validator('merchant_name')
    @classmethod
    def validate_merchant_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v


class TransactionUpdate(BaseModel):
    """Update transaction - all fields optional"""
    account_uuid: Optional[UUID] = Field(None, description="UUID of the account to move this transaction to")
    transaction_date: Optional[date] = None
    amount: Optional[Decimal] = None
    transaction_type: Optional[TransactionTypeEnum] = None
    description: Optional[str] = Field(None, max_length=500)
    merchant_name: Optional[str] = Field(None, max_length=255)
    category_uuid: Optional[UUID] = Field(None, description="UUID of the transaction's category")
    subcategory_uuid: Optional[UUID] = Field(None, description="UUID of the transaction's sub-category")
    comments: Optional[str] = None

    @field_validator('amount')
    @classmethod
    def validate_amount(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        return round(v, 2) if v is not None else v

    @field_validator('description')
    @classmethod
    def validate_description(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

    @field_validator('merchant_name')
    @classmethod
    def validate_merchant_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v


class TransactionBulkUpdate(BaseModel):
    """Model for bulk updating transactions."""
    transaction_uuids: List[UUID] = Field(..., description="A list of transaction UUIDs to update.")
    account_uuid: Optional[UUID] = Field(None, description="Set a new account for all specified transactions.")
    category_uuid: Optional[UUID] = Field(None, description="Set a new category for all specified transactions.")
    subcategory_uuid: Optional[UUID] = Field(None, description="Set a new sub-category for all specified transactions.")
    comments: Optional[str] = Field(None, description="Add or overwrite comments for all specified transactions.")

    @field_validator('transaction_uuids')
    @classmethod
    def validate_transaction_uuids(cls, v: List[UUID]) -> List[UUID]:
        if not v:
            raise ValueError("transaction_uuids list cannot be empty.")
        return v


class SplitAllocationCreate(BaseModel):
    category_uuid: UUID
    subcategory_uuid: Optional[UUID] = None
    amount: Decimal


class SplitAllocationResponse(BaseModel):
    id: UUID
    category_uuid: UUID
    category_name: str
    subcategory_uuid: Optional[UUID] = None
    subcategory_name: Optional[str] = None
    amount: Decimal

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'category') and data.category:
                data.__dict__['category_uuid'] = data.category.uuid
                data.__dict__['category_name'] = data.category.name
            if hasattr(data, 'subcategory') and data.subcategory:
                data.__dict__['subcategory_uuid'] = data.subcategory.uuid
                data.__dict__['subcategory_name'] = data.subcategory.name
        return data


class TransactionSplitRequest(BaseModel):
    allocations: List[SplitAllocationCreate]

    @model_validator(mode='after')
    def validate_allocations(self):
        if len(self.allocations) < 2:
            raise ValueError("A split must have at least 2 allocations")
        pairs = [(a.category_uuid, a.subcategory_uuid) for a in self.allocations]
        if len(pairs) != len(set(pairs)):
            raise ValueError("Duplicate category/subcategory pairs not allowed")
        if any(a.amount <= 0 for a in self.allocations):
            raise ValueError("All allocation amounts must be positive")
        return self


class TransactionResponse(BaseModel):
    """Transaction data returned to client"""
    id: UUID
    account_uuid: Optional[UUID] = None
    transaction_date: date
    amount: Decimal
    transaction_type: TransactionTypeEnum
    category: Optional[CategoryResponse] = None
    subcategory: Optional[CategoryResponse] = None
    description: Optional[str]
    parsed_description: Optional[str]
    merchant_name: Optional[str]
    comments: Optional[str]
    source_type: SourceTypeEnum
    tags: List[EmbeddedTagResponse] = []
    split_allocations: List[SplitAllocationResponse] = []
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'account') and data.account:
                data.__dict__['account_uuid'] = data.account.uuid
            if hasattr(data, 'transaction_tags') and data.transaction_tags:
                data.__dict__['tags'] = [tt.tag for tt in data.transaction_tags]
        return data


class TransactionSummary(BaseModel):
    """Lightweight transaction summary"""
    id: UUID
    transaction_date: date
    amount: Decimal
    transaction_type: TransactionTypeEnum
    description: Optional[str]
    merchant_name: Optional[str]
    category: Optional[CategoryResponse] = None
    subcategory: Optional[CategoryResponse] = None

    class Config:
        from_attributes = True


class TransactionImport(BaseModel):
    """Bulk transaction import"""
    account_uuid: UUID
    transactions: List[TransactionCreate]
    source_type: SourceTypeEnum = Field(default=SourceTypeEnum.CSV)


class TransactionFilter(BaseModel):
    """Filter parameters for transaction queries"""
    account_id: Optional[int] = None
    account_ids: Optional[List[int]] = None
    transaction_type: Optional[TransactionTypeEnum] = None
    category_ids: Optional[List[int]] = None
    subcategory_ids: Optional[List[int]] = None
    tag_ids: Optional[List[int]] = None
    merchant_name: Optional[str] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    amount_min: Optional[Decimal] = None
    amount_max: Optional[Decimal] = None
    description_search: Optional[str] = None


class TransactionStats(BaseModel):
    """Aggregate transaction statistics"""
    total_count: int
    total_income: Decimal
    total_expenses: Decimal
    net: Decimal

# ===== AMORTIZATION MODELS =====

class AmortizationAllocation(BaseModel):
    month: str  # "2026-01" format
    amount: Decimal

class AmortizationScheduleCreate(BaseModel):
    # Option A: explicit allocations
    allocations: Optional[List[AmortizationAllocation]] = None
    # Option B: equal split shorthand
    start_month: Optional[str] = None  # "2026-01"
    months: Optional[int] = None

    @model_validator(mode='after')
    def validate_options(self):
        has_allocations = self.allocations is not None
        has_shorthand = self.start_month is not None or self.months is not None
        if has_allocations and has_shorthand:
            raise ValueError("Provide either 'allocations' or 'start_month'+'months', not both")
        if not has_allocations and not has_shorthand:
            raise ValueError("Provide either 'allocations' or 'start_month'+'months'")
        if has_shorthand:
            if self.start_month is None or self.months is None:
                raise ValueError("Both 'start_month' and 'months' are required for equal split")
            if self.months < 2:
                raise ValueError("Must amortize across at least 2 months")
        if has_allocations and len(self.allocations) < 2:
            raise ValueError("Must have at least 2 allocation entries")
        return self

class AmortizationScheduleEntry(BaseModel):
    id: UUID
    month: str  # "2026-01"
    amount: Decimal
    category_uuid: Optional[UUID] = None
    category_name: Optional[str] = None
    subcategory_uuid: Optional[UUID] = None
    subcategory_name: Optional[str] = None

class AmortizationScheduleResponse(BaseModel):
    transaction_uuid: UUID
    total_amount: Decimal
    num_months: int
    allocations: List[AmortizationScheduleEntry]


# ===== MONTHLY AVERAGE ANALYTICS MODELS =====

class MonthlyAverageTotals(BaseModel):
    avg_monthly_income: Decimal
    avg_monthly_expenses: Decimal
    avg_monthly_net: Decimal
    total_income: Decimal
    total_expenses: Decimal
    total_net: Decimal


class MonthlyAverageSubcategoryBreakdown(BaseModel):
    subcategory_uuid: UUID
    subcategory_name: str
    total: Decimal
    monthly_average: Decimal


class MonthlyAverageCategoryBreakdown(BaseModel):
    category_uuid: UUID
    category_name: str
    total: Decimal
    monthly_average: Decimal
    subcategories: List[MonthlyAverageSubcategoryBreakdown] = []


class MonthlyAverageMonthBreakdown(BaseModel):
    month: str  # "2025-01" format
    income: Decimal
    expenses: Decimal
    net: Decimal


class MonthlyAverageResponse(BaseModel):
    year: int
    months_with_data: int
    totals: MonthlyAverageTotals
    by_category: List[MonthlyAverageCategoryBreakdown]
    by_month: List[MonthlyAverageMonthBreakdown]


class TransactionRelationshipCreateByUUID(BaseModel):
    to_transaction_uuid: UUID
    relationship_type: RelationshipType
    amount_allocated: Optional[Decimal] = None
    notes: Optional[str] = None

class TransactionRelationshipUpdate(BaseModel):
    """Update transaction relationship - all fields optional"""
    to_transaction_uuid: Optional[UUID] = None
    relationship_type: Optional[RelationshipType] = None
    amount_allocated: Optional[Decimal] = None
    notes: Optional[str] = None

class TransactionRelationship(BaseModel):
    id: UUID
    from_transaction_uuid: UUID
    to_transaction_uuid: UUID
    relationship_type: RelationshipType
    amount_allocated: Optional[Decimal] = None
    notes: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True

    @model_validator(mode='before')
    @classmethod
    def resolve_uuids(cls, data):
        if hasattr(data, '__dict__'):
            if hasattr(data, 'from_transaction') and data.from_transaction:
                data.__dict__['from_transaction_uuid'] = data.from_transaction.id
            if hasattr(data, 'to_transaction') and data.to_transaction:
                data.__dict__['to_transaction_uuid'] = data.to_transaction.id
        return data
