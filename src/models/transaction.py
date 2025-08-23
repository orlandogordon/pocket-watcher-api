from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID
from enum import Enum

from src.models.category import CategoryResponse
from src.db.core import RelationshipType

# ===== TRANSACTION PYDANTIC MODELS =====

class TransactionTypeEnum(str, Enum):
    DEBIT = "DEBIT"
    CREDIT = "CREDIT"
    TRANSFER = "TRANSFER"
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
    account_id: int = Field(..., description="Account ID for this transaction")
    transaction_date: date = Field(..., description="Date of the transaction")
    posted_date: Optional[date] = Field(None, description="Date transaction was posted")
    amount: Decimal = Field(..., description="Transaction amount")
    transaction_type: TransactionTypeEnum = Field(..., description="Type of transaction")
    description: Optional[str] = Field(None, max_length=500, description="Transaction description")
    merchant_name: Optional[str] = Field(None, max_length=255, description="Merchant name")
    category_id: Optional[int] = Field(None, description="The ID of the transaction's category")
    subcategory_id: Optional[int] = Field(None, description="The ID of the transaction's sub-category")
    comments: Optional[str] = Field(None, description="User comments")
    external_transaction_id: Optional[str] = Field(None, max_length=255, description="External transaction ID")
    source_type: SourceTypeEnum = Field(default=SourceTypeEnum.MANUAL, description="Source of transaction data")
    raw_data: Optional[Dict[str, Any]] = Field(None, description="Raw transaction data from source")

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
    transaction_date: Optional[date] = None
    posted_date: Optional[date] = None
    amount: Optional[Decimal] = None
    transaction_type: Optional[TransactionTypeEnum] = None
    description: Optional[str] = Field(None, max_length=500)
    merchant_name: Optional[str] = Field(None, max_length=255)
    category_id: Optional[int] = Field(None, description="The ID of the transaction's category")
    subcategory_id: Optional[int] = Field(None, description="The ID of the transaction's sub-category")
    comments: Optional[str] = None
    needs_review: Optional[bool] = None

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
    """
    Model for bulk updating transactions.
    """
    transaction_ids: List[int] = Field(..., description="A list of transaction database IDs to update.")
    account_id: Optional[int] = Field(None, description="Set a new account for all specified transactions.")
    category_id: Optional[int] = Field(None, description="Set a new category for all specified transactions.")
    subcategory_id: Optional[int] = Field(None, description="Set a new sub-category for all specified transactions.")
    needs_review: Optional[bool] = Field(None, description="Mark all specified transactions as needing review.")
    comments: Optional[str] = Field(None, description="Add or overwrite comments for all specified transactions.")

    @field_validator('transaction_ids')
    @classmethod
    def validate_transaction_ids(cls, v: List[int]) -> List[int]:
        if not v:
            raise ValueError("transaction_ids list cannot be empty.")
        return v


class TransactionResponse(BaseModel):
    """Transaction data returned to client"""
    id: UUID
    db_id: int
    external_transaction_id: Optional[str]
    account_id: int
    transaction_date: date
    posted_date: Optional[date]
    amount: Decimal
    transaction_type: TransactionTypeEnum
    category: Optional[CategoryResponse] = None
    subcategory: Optional[CategoryResponse] = None
    description: Optional[str]
    parsed_description: Optional[str]
    merchant_name: Optional[str]
    comments: Optional[str]
    institution_name: Optional[str]
    account_number_last4: Optional[str]
    source_type: SourceTypeEnum
    needs_review: bool
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class TransactionSummary(BaseModel):
    """Lightweight transaction summary"""
    id: UUID
    db_id: int
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
    account_id: int
    transactions: List[TransactionCreate]
    source_type: SourceTypeEnum = Field(default=SourceTypeEnum.CSV)


class TransactionFilter(BaseModel):
    """Filter parameters for transaction queries"""
    account_id: Optional[int] = None
    account_ids: Optional[List[int]] = None
    transaction_type: Optional[TransactionTypeEnum] = None
    category_id: Optional[int] = None
    subcategory_id: Optional[int] = None
    merchant_name: Optional[str] = None
    date_from: Optional[date] = None
    date_to: Optional[date] = None
    amount_min: Optional[Decimal] = None
    amount_max: Optional[Decimal] = None
    needs_review: Optional[bool] = None
    description_search: Optional[str] = None


class TransactionStats(BaseModel):
    """Transaction statistics"""
    total_transactions: int
    total_income: Decimal
    total_expenses: Decimal
    net_amount: Decimal
    transactions_by_type: Dict[str, int]
    transactions_by_category: Dict[str, Decimal]

class TransactionRelationshipBase(BaseModel):
    to_transaction_id: int
    relationship_type: RelationshipType
    amount_allocated: Optional[Decimal] = None
    notes: Optional[str] = None

class TransactionRelationshipCreate(TransactionRelationshipBase):
    pass

class TransactionRelationship(TransactionRelationshipBase):
    relationship_id: int
    from_transaction_id: int
    created_at: datetime

    class Config:
        from_attributes = True

