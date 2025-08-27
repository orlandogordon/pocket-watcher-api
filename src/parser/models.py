from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from decimal import Decimal

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
    description: str
    quantity: Optional[Decimal]
    price_per_share: Optional[Decimal]
    total_amount: Decimal

class ParsedAccountInfo(BaseModel):
    account_number_last4: str

class ParsedData(BaseModel):
    account_info: Optional[ParsedAccountInfo] = None
    transactions: List[ParsedTransaction] = Field(default_factory=list)
    investment_transactions: List[ParsedInvestmentTransaction] = Field(default_factory=list)
