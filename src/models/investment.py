from pydantic import BaseModel, Field, field_validator, model_validator
from typing import Optional, List
from datetime import datetime, date
from decimal import Decimal
from enum import Enum
from uuid import UUID


# ===== ENUMS =====

class SecurityTypeEnum(str, Enum):
    STOCK = "STOCK"
    ETF = "ETF"
    MUTUAL_FUND = "MUTUAL_FUND"
    OPTION = "OPTION"
    FUTURE = "FUTURE"
    BOND = "BOND"
    CRYPTO = "CRYPTO"

class InvestmentTransactionTypeEnum(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    INTEREST = "INTEREST"
    FEE = "FEE"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    SPLIT = "SPLIT"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"
    REINVESTMENT = "REINVESTMENT"
    EXPIRATION = "EXPIRATION"
    OTHER = "OTHER"


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

class InvestmentHoldingUpdate(BaseModel):
    security_type: Optional[SecurityTypeEnum] = None
    underlying_symbol: Optional[str] = Field(None, max_length=10)
    option_type: Optional[str] = Field(None, max_length=4)
    strike_price: Optional[Decimal] = None
    expiration_date: Optional[date] = None

class InvestmentHoldingResponse(InvestmentHoldingBase):
    id: UUID
    account_uuid: UUID
    current_price: Optional[Decimal]
    last_price_update: Optional[datetime]
    security_type: Optional[SecurityTypeEnum] = None
    underlying_symbol: Optional[str] = None
    option_type: Optional[str] = None
    strike_price: Optional[Decimal] = None
    expiration_date: Optional[date] = None
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
        return data


# ===== INVESTMENT ACCOUNT SUMMARY =====

class InvestmentAccountSummary(BaseModel):
    cash_balance: Decimal
    securities_value: Decimal
    total_value: Decimal


# ===== INVESTMENT TRANSACTION PYDANTIC MODELS =====

class InvestmentTransactionBase(BaseModel):
    transaction_type: InvestmentTransactionTypeEnum
    symbol: Optional[str] = Field(None, max_length=20)
    quantity: Optional[Decimal] = Field(None, description="Number of shares/units")
    price_per_share: Optional[Decimal] = Field(None, description="Price per share/unit")
    total_amount: Decimal = Field(..., description="Total transaction value")
    fees: Optional[Decimal] = Field(default=0.00)
    security_type: Optional[SecurityTypeEnum] = None
    transaction_date: date
    description: Optional[str] = Field(None, max_length=500)
    api_symbol: Optional[str] = Field(None, max_length=50, description="Symbol for yfinance API (OCC format for options)")

class InvestmentTransactionCreate(InvestmentTransactionBase):
    account_uuid: UUID

class InvestmentTransactionBulkCreate(BaseModel):
    transactions: List[InvestmentTransactionCreate]

class InvestmentTransactionUpdate(BaseModel):
    transaction_type: Optional[InvestmentTransactionTypeEnum] = None
    quantity: Optional[Decimal] = None
    price_per_share: Optional[Decimal] = None
    total_amount: Optional[Decimal] = None
    fees: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = Field(None, max_length=500)


class InvestmentTransactionResponse(InvestmentTransactionBase):
    id: UUID
    account_uuid: UUID
    holding_uuid: Optional[UUID] = None
    cost_basis_at_sale: Optional[Decimal] = None
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
            if hasattr(data, 'holding') and data.holding:
                data.__dict__['holding_uuid'] = data.holding.id
        return data
