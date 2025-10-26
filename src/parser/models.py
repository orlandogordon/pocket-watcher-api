from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from decimal import Decimal
from enum import Enum

class SecurityType(str, Enum):
    """Type of security for investment transactions"""
    STOCK = "STOCK"
    OPTION = "OPTION"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    INTEREST = "INTEREST"
    DIVIDEND = "DIVIDEND"
    FEE = "FEE"
    ADJUSTMENT = "ADJUSTMENT"
    OTHER = "OTHER"

class ParsedTransaction(BaseModel):
    transaction_date: date
    description: str
    amount: Decimal
    transaction_type: str
    is_duplicate: bool = False

class ParsedInvestmentTransaction(BaseModel):
    transaction_date: date
    transaction_type: str
    symbol: Optional[str]
    api_symbol: Optional[str] = None  # Symbol for API calls (yfinance format)
    description: str
    quantity: Optional[Decimal]
    price_per_share: Optional[Decimal]
    total_amount: Decimal
    is_duplicate: bool = False
    security_type: Optional[SecurityType] = None

class ParsedAccountInfo(BaseModel):
    account_number_last4: str

class ParsedData(BaseModel):
    account_info: Optional[ParsedAccountInfo] = None
    transactions: List[ParsedTransaction] = Field(default_factory=list)
    investment_transactions: List[ParsedInvestmentTransaction] = Field(default_factory=list)
