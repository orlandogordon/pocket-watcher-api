from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime, date
from decimal import Decimal
from enum import Enum


# ===== ENUMS =====

class InvestmentTransactionTypeEnum(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    INTEREST = "INTEREST"
    SPLIT = "SPLIT"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"
    REINVESTMENT = "REINVESTMENT"


# ===== INVESTMENT HOLDING PYDANTIC MODELS =====

class InvestmentHoldingBase(BaseModel):
    symbol: str = Field(..., max_length=20, description="Ticker symbol for the holding")
    quantity: Decimal = Field(..., description="Number of shares/units owned")
    average_cost_basis: Optional[Decimal] = Field(None, description="Average price paid per share")

    @field_validator('quantity', 'average_cost_basis')
    @classmethod
    def round_decimal_fields(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is not None:
            return round(v, 6)
        return v

class InvestmentHoldingCreate(InvestmentHoldingBase):
    account_id: int = Field(..., description="The account this holding belongs to")

class InvestmentHoldingUpdate(BaseModel):
    quantity: Optional[Decimal] = None
    average_cost_basis: Optional[Decimal] = None

    @field_validator('quantity', 'average_cost_basis')
    @classmethod
    def round_decimal_fields(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is not None:
            return round(v, 6)
        return v

class InvestmentHoldingResponse(InvestmentHoldingBase):
    holding_id: int
    account_id: int
    current_price: Optional[Decimal]
    last_price_update: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ===== INVESTMENT TRANSACTION PYDANTIC MODELS =====

class InvestmentTransactionBase(BaseModel):
    transaction_type: InvestmentTransactionTypeEnum
    symbol: str = Field(..., max_length=20)
    quantity: Optional[Decimal] = Field(None, description="Number of shares/units")
    price_per_share: Optional[Decimal] = Field(None, description="Price per share/unit")
    total_amount: Decimal = Field(..., description="Total transaction value")
    fees: Optional[Decimal] = Field(default=0.00)
    transaction_date: date
    description: Optional[str] = Field(None, max_length=500)

class InvestmentTransactionCreate(InvestmentTransactionBase):
    account_id: int

class InvestmentTransactionUpdate(BaseModel):
    transaction_type: Optional[InvestmentTransactionTypeEnum] = None
    quantity: Optional[Decimal] = None
    price_per_share: Optional[Decimal] = None
    total_amount: Optional[Decimal] = None
    fees: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = Field(None, max_length=500)

class InvestmentTransactionResponse(InvestmentTransactionBase):
    investment_transaction_id: int
    account_id: int
    holding_id: Optional[int]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True
