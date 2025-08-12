from pydantic import BaseModel, Field, field_validator
from typing import Optional, List
from datetime import datetime
from decimal import Decimal
from enum import Enum


# ===== ACCOUNT PYDANTIC MODELS =====

class AccountTypeEnum(str, Enum):
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    CREDIT_CARD = "CREDIT_CARD"
    INVESTMENT = "INVESTMENT"
    LOAN = "LOAN"
    OTHER = "OTHER"


class InterestRateTypeEnum(str, Enum):
    FIXED = "FIXED"
    VARIABLE = "VARIABLE"


class AccountCreate(BaseModel):
    account_name: str = Field(..., min_length=1, max_length=255, description="Account name")
    account_type: AccountTypeEnum = Field(..., description="Type of account")
    institution_name: str = Field(..., min_length=1, max_length=255, description="Financial institution name")
    account_number_last4: Optional[str] = Field(None, min_length=4, max_length=4, description="Last 4 digits of account number")
    balance: Decimal = Field(default=Decimal('0.00'), description="Initial account balance")
    
    # Loan-specific fields
    interest_rate: Optional[Decimal] = Field(None, ge=0, le=1, description="Interest rate (0.0525 for 5.25%)")
    interest_rate_type: Optional[InterestRateTypeEnum] = Field(None, description="Fixed or variable interest rate")
    
    comments: Optional[str] = Field(None, max_length=1000, description="Optional comments about the account")

    @field_validator('account_name')
    @classmethod
    def validate_account_name(cls, v: str) -> str:
        return v.strip()

    @field_validator('institution_name')
    @classmethod
    def validate_institution_name(cls, v: str) -> str:
        return v.strip()

    @field_validator('account_number_last4')
    @classmethod
    def validate_account_number_last4(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not v.isdigit():
            raise ValueError('Account number last 4 digits must be numeric')
        return v

    @field_validator('balance')
    @classmethod
    def validate_balance(cls, v: Decimal) -> Decimal:
        # Round to 2 decimal places
        return round(v, 2)


class AccountUpdate(BaseModel):
    """Update account - all fields optional"""
    account_name: Optional[str] = Field(None, min_length=1, max_length=255)
    account_type: Optional[AccountTypeEnum] = None
    institution_name: Optional[str] = Field(None, min_length=1, max_length=255)
    account_number_last4: Optional[str] = Field(None, min_length=4, max_length=4)
    balance: Optional[Decimal] = Field(None, description="Updated account balance")
    interest_rate: Optional[Decimal] = Field(None, ge=0, le=1)
    interest_rate_type: Optional[InterestRateTypeEnum] = None
    comments: Optional[str] = Field(None, max_length=1000)

    @field_validator('account_name')
    @classmethod
    def validate_account_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

    @field_validator('institution_name')
    @classmethod
    def validate_institution_name(cls, v: Optional[str]) -> Optional[str]:
        return v.strip() if v else v

    @field_validator('account_number_last4')
    @classmethod
    def validate_account_number_last4(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if not v.isdigit():
            raise ValueError('Account number last 4 digits must be numeric')
        return v

    @field_validator('balance')
    @classmethod
    def validate_balance(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        return round(v, 2) if v is not None else v


class AccountResponse(BaseModel):
    """Account data returned to client"""
    id: int
    user_id: int
    account_name: str
    account_type: AccountTypeEnum
    institution_name: str
    account_number_last4: Optional[str]
    balance: Decimal
    balance_last_updated: Optional[datetime]
    interest_rate: Optional[Decimal]
    interest_rate_type: Optional[str]
    comments: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class AccountSummary(BaseModel):
    """Lightweight account summary for dropdowns/lists"""
    id: int
    account_name: str
    account_type: AccountTypeEnum
    institution_name: str
    balance: Decimal
    account_number_last4: Optional[str]

    class Config:
        from_attributes = True


class AccountBalance(BaseModel):
    """Account balance information"""
    account_id: int
    balance: Decimal
    balance_last_updated: Optional[datetime]


class AccountStats(BaseModel):
    """Account statistics"""
    total_accounts: int
    accounts_by_type: dict
    total_assets: Decimal
    total_liabilities: Decimal
    net_worth: Decimal
