from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import Optional, List
from datetime import datetime
from decimal import Decimal

# Import your database models
from .core import AccountDB, UserDB, NotFoundError, AccountType
from pydantic import BaseModel, Field, field_validator
from typing import Optional
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


# ===== DATABASE OPERATIONS =====

def create_db_account(db: Session, user_id: int, account_data: AccountCreate) -> AccountDB:
    """Create a new account for a user"""
    
    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Check if account name already exists for this user
    existing_account = db.query(AccountDB).filter(
        AccountDB.user_id == user_id,
        AccountDB.account_name == account_data.account_name
    ).first()
    if existing_account:
        raise ValueError(f"Account name '{account_data.account_name}' already exists")
    
    # Create new account
    db_account = AccountDB(
        user_id=user_id,
        account_name=account_data.account_name,
        account_type=AccountType(account_data.account_type.value),
        institution_name=account_data.institution_name,
        account_number_last4=account_data.account_number_last4,
        balance=account_data.balance,
        balance_last_updated=datetime.utcnow() if account_data.balance != 0 else None,
        interest_rate=account_data.interest_rate,
        interest_rate_type=account_data.interest_rate_type.value if account_data.interest_rate_type else None,
        comments=account_data.comments,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    try:
        db.add(db_account)
        db.commit()
        db.refresh(db_account)
        return db_account
    except IntegrityError:
        db.rollback()
        raise ValueError("Account creation failed due to database constraint")


def read_db_account(db: Session, account_id: int, user_id: Optional[int] = None) -> Optional[AccountDB]:
    """Read an account by ID, optionally filtering by user"""
    
    query = db.query(AccountDB).filter(AccountDB.id == account_id)
    
    if user_id:
        query = query.filter(AccountDB.user_id == user_id)
    
    return query.first()


def read_db_accounts(db: Session, user_id: int, account_type: Optional[AccountTypeEnum] = None, 
                     skip: int = 0, limit: int = 100) -> List[AccountDB]:
    """Read accounts for a user, optionally filtered by account type"""
    
    query = db.query(AccountDB).filter(AccountDB.user_id == user_id)
    
    if account_type:
        query = query.filter(AccountDB.account_type == AccountType(account_type.value))
    
    return query.offset(skip).limit(limit).all()


def read_db_accounts_summary(db: Session, user_id: int) -> List[AccountDB]:
    """Get all accounts for a user (for dropdowns, summaries)"""
    return db.query(AccountDB).filter(AccountDB.user_id == user_id).all()


def update_db_account(db: Session, account_id: int, user_id: int, account_updates: AccountUpdate) -> AccountDB:
    """Update an existing account"""
    
    # Get the existing account
    db_account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()
    
    if not db_account:
        raise NotFoundError(f"Account with id {account_id} not found")
    
    # Check for account name uniqueness if name is being updated
    if account_updates.account_name and account_updates.account_name != db_account.account_name:
        existing_name = db.query(AccountDB).filter(
            AccountDB.user_id == user_id,
            AccountDB.account_name == account_updates.account_name,
            AccountDB.id != account_id
        ).first()
        if existing_name:
            raise ValueError(f"Account name '{account_updates.account_name}' already exists")
    
    # Update only the fields that are provided
    update_data = account_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        if field == 'account_type' and value:
            setattr(db_account, field, AccountType(value.value))
        elif field == 'balance' and value is not None:
            setattr(db_account, field, value)
            db_account.balance_last_updated = datetime.utcnow()
        elif field == 'interest_rate_type' and value:
            setattr(db_account, field, value.value)
        else:
            setattr(db_account, field, value)
    
    # Always update the updated_at timestamp
    db_account.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_account)
        return db_account
    except IntegrityError:
        db.rollback()
        raise ValueError("Account update failed due to database constraint")


def delete_db_account(db: Session, account_id: int, user_id: int) -> bool:
    """Delete an account (only if it has no transactions)"""
    
    db_account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()
    
    if not db_account:
        raise NotFoundError(f"Account with id {account_id} not found")
    
    # Check if account has any transactions
    if db_account.transactions:
        raise ValueError("Cannot delete account with existing transactions")
    
    # Check if account has any investment holdings
    if hasattr(db_account, 'investment_holdings') and db_account.investment_holdings:
        raise ValueError("Cannot delete account with existing investment holdings")
    
    try:
        db.delete(db_account)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete account: {str(e)}")


def update_account_balance(db: Session, account_id: int, new_balance: Decimal) -> AccountDB:
    """Update an account's balance (used by transaction processing)"""
    
    db_account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if not db_account:
        raise NotFoundError(f"Account with id {account_id} not found")
    
    db_account.balance = round(new_balance, 2)
    db_account.balance_last_updated = datetime.utcnow()
    db_account.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_account)
        return db_account
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to update account balance: {str(e)}")


def get_account_stats(db: Session, user_id: int) -> AccountStats:
    """Get account statistics for a user"""
    
    accounts = db.query(AccountDB).filter(AccountDB.user_id == user_id).all()
    
    total_accounts = len(accounts)
    accounts_by_type = {}
    total_assets = Decimal('0.00')
    total_liabilities = Decimal('0.00')
    
    for account in accounts:
        # Count by type
        account_type = account.account_type.value
        accounts_by_type[account_type] = accounts_by_type.get(account_type, 0) + 1
        
        # Calculate assets vs liabilities
        if account.account_type in [AccountType.CHECKING, AccountType.SAVINGS, AccountType.INVESTMENT, AccountType.OTHER]:
            total_assets += account.balance
        elif account.account_type in [AccountType.CREDIT_CARD, AccountType.LOAN]:
            # For credit cards and loans, balance is typically negative (what you owe)
            total_liabilities += abs(account.balance)
    
    net_worth = total_assets - total_liabilities
    
    return AccountStats(
        total_accounts=total_accounts,
        accounts_by_type=accounts_by_type,
        total_assets=total_assets,
        total_liabilities=total_liabilities,
        net_worth=net_worth
    )


def get_accounts_count(db: Session, user_id: int, account_type: Optional[AccountTypeEnum] = None) -> int:
    """Get count of accounts for pagination"""
    
    query = db.query(AccountDB).filter(AccountDB.user_id == user_id)
    
    if account_type:
        query = query.filter(AccountDB.account_type == AccountType(account_type.value))
    
    return query.count()


def get_account_by_name(db: Session, user_id: int, account_name: str) -> Optional[AccountDB]:
    """Get account by name for a specific user"""
    return db.query(AccountDB).filter(
        AccountDB.user_id == user_id,
        AccountDB.account_name == account_name
    ).first()