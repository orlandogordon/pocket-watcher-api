from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, desc, asc
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import uuid4
import hashlib
import json

# Import your database models
from .core import TransactionDB, AccountDB, UserDB, NotFoundError, TransactionType, SourceType
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import UUID
from enum import Enum


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
    category: Optional[str] = Field(None, max_length=100, description="Transaction category")
    subcategory: Optional[str] = Field(None, max_length=100, description="Transaction subcategory")
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
    """Update transaction - all fields optional except IDs"""
    transaction_date: Optional[date] = None
    posted_date: Optional[date] = None
    amount: Optional[Decimal] = None
    transaction_type: Optional[TransactionTypeEnum] = None
    description: Optional[str] = Field(None, max_length=500)
    merchant_name: Optional[str] = Field(None, max_length=255)
    category: Optional[str] = Field(None, max_length=100)
    subcategory: Optional[str] = Field(None, max_length=100)
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
    category: Optional[str]
    subcategory: Optional[str]
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
    category: Optional[str]

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
    category: Optional[str] = None
    subcategory: Optional[str] = None
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


# ===== UTILITY FUNCTIONS =====

def generate_transaction_hash(transaction_data: TransactionCreate, user_id: int) -> str:
    """Generate a hash for transaction deduplication"""
    hash_string = (
        f"{user_id}|"
        f"{transaction_data.account_id}|"
        f"{transaction_data.transaction_date}|"
        f"{transaction_data.amount}|"
        f"{transaction_data.description or ''}|"
        f"{transaction_data.external_transaction_id or ''}"
    )
    return hashlib.sha256(hash_string.encode()).hexdigest()


def parse_transaction_description(description: str) -> str:
    """Parse and clean transaction description"""
    if not description:
        return ""
    
    # Basic cleanup - remove extra spaces, standardize format
    cleaned = " ".join(description.split())
    
    # You can add more sophisticated parsing logic here
    # For example: extract merchant names, remove transaction codes, etc.
    
    return cleaned


# ===== DATABASE OPERATIONS =====

def create_db_transaction(db: Session, user_id: int, transaction_data: TransactionCreate) -> TransactionDB:
    """Create a new transaction"""
    
    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Verify account exists and belongs to user
    account = db.query(AccountDB).filter(
        AccountDB.id == transaction_data.account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {transaction_data.account_id} not found")
    
    # Generate transaction hash for deduplication
    transaction_hash = generate_transaction_hash(transaction_data, user_id)
    
    # Check for duplicate transaction
    existing_transaction = db.query(TransactionDB).filter(
        TransactionDB.user_id == user_id,
        TransactionDB.transaction_hash == transaction_hash
    ).first()
    if existing_transaction:
        raise ValueError("Duplicate transaction detected")
    
    # Create new transaction
    db_transaction = TransactionDB(
        id=uuid4(),
        external_transaction_id=transaction_data.external_transaction_id,
        user_id=user_id,
        account_id=transaction_data.account_id,
        transaction_hash=transaction_hash,
        source_type=SourceType(transaction_data.source_type.value),
        raw_data_json=transaction_data.raw_data,
        transaction_date=transaction_data.transaction_date,
        posted_date=transaction_data.posted_date,
        amount=transaction_data.amount,
        transaction_type=TransactionType(transaction_data.transaction_type.value),
        category=transaction_data.category,
        subcategory=transaction_data.subcategory,
        description=transaction_data.description,
        parsed_description=parse_transaction_description(transaction_data.description or ""),
        merchant_name=transaction_data.merchant_name,
        comments=transaction_data.comments,
        institution_name=account.institution_name,
        account_number_last4=account.account_number_last4,
        needs_review=False,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    try:
        db.add(db_transaction)
        db.commit()
        db.refresh(db_transaction)
        
        # Update account balance (you might want to do this in a separate service)
        update_account_balance_from_transaction(db, account, db_transaction)
        
        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Transaction creation failed due to database constraint")


def read_db_transaction(db: Session, transaction_id: int, user_id: Optional[int] = None) -> Optional[TransactionDB]:
    """Read a transaction by ID"""
    
    query = db.query(TransactionDB).filter(TransactionDB.db_id == transaction_id)
    
    if user_id:
        query = query.filter(TransactionDB.user_id == user_id)
    
    return query.first()


def read_db_transaction_by_uuid(db: Session, transaction_uuid: UUID, user_id: Optional[int] = None) -> Optional[TransactionDB]:
    """Read a transaction by UUID"""
    
    query = db.query(TransactionDB).filter(TransactionDB.id == transaction_uuid)
    
    if user_id:
        query = query.filter(TransactionDB.user_id == user_id)
    
    return query.first()


def read_db_transactions(db: Session, user_id: int, filters: Optional[TransactionFilter] = None, 
                        skip: int = 0, limit: int = 100, order_by: str = "transaction_date", 
                        order_desc: bool = True) -> List[TransactionDB]:
    """Read transactions with filtering and pagination"""
    
    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)
    
    # Apply filters
    if filters:
        if filters.account_id:
            query = query.filter(TransactionDB.account_id == filters.account_id)
        
        if filters.account_ids:
            query = query.filter(TransactionDB.account_id.in_(filters.account_ids))
        
        if filters.transaction_type:
            query = query.filter(TransactionDB.transaction_type == TransactionType(filters.transaction_type.value))
        
        if filters.category:
            query = query.filter(TransactionDB.category == filters.category)
        
        if filters.subcategory:
            query = query.filter(TransactionDB.subcategory == filters.subcategory)
        
        if filters.merchant_name:
            query = query.filter(TransactionDB.merchant_name.ilike(f"%{filters.merchant_name}%"))
        
        if filters.date_from:
            query = query.filter(TransactionDB.transaction_date >= filters.date_from)
        
        if filters.date_to:
            query = query.filter(TransactionDB.transaction_date <= filters.date_to)
        
        if filters.amount_min is not None:
            query = query.filter(TransactionDB.amount >= filters.amount_min)
        
        if filters.amount_max is not None:
            query = query.filter(TransactionDB.amount <= filters.amount_max)
        
        if filters.needs_review is not None:
            query = query.filter(TransactionDB.needs_review == filters.needs_review)
        
        if filters.description_search:
            query = query.filter(
                or_(
                    TransactionDB.description.ilike(f"%{filters.description_search}%"),
                    TransactionDB.parsed_description.ilike(f"%{filters.description_search}%")
                )
            )
    
    # Apply ordering
    if hasattr(TransactionDB, order_by):
        order_column = getattr(TransactionDB, order_by)
        if order_desc:
            query = query.order_by(desc(order_column))
        else:
            query = query.order_by(asc(order_column))
    else:
        # Default ordering
        query = query.order_by(desc(TransactionDB.transaction_date))
    
    return query.offset(skip).limit(limit).all()


def update_db_transaction(db: Session, transaction_id: int, user_id: int, 
                         transaction_updates: TransactionUpdate) -> TransactionDB:
    """Update an existing transaction"""
    
    # Get the existing transaction
    db_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")
    
    # Store old amount for balance adjustment
    old_amount = db_transaction.amount
    
    # Update only the fields that are provided
    update_data = transaction_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        if field == 'transaction_type' and value:
            setattr(db_transaction, field, TransactionType(value.value))
        elif field == 'parsed_description' and field == 'description' and value:
            setattr(db_transaction, 'description', value)
            setattr(db_transaction, 'parsed_description', parse_transaction_description(value))
        else:
            setattr(db_transaction, field, value)
    
    # Always update the updated_at timestamp
    db_transaction.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_transaction)
        
        # Update account balance if amount changed
        if 'amount' in update_data and update_data['amount'] != old_amount:
            account = db.query(AccountDB).filter(AccountDB.id == db_transaction.account_id).first()
            if account:
                # Reverse old amount and apply new amount
                balance_adjustment = db_transaction.amount - old_amount
                new_balance = account.balance + balance_adjustment
                from .accounts import update_account_balance
                update_account_balance(db, account.id, new_balance)
        
        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Transaction update failed due to database constraint")


def delete_db_transaction(db: Session, transaction_id: int, user_id: int) -> bool:
    """Delete a transaction"""
    
    db_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")
    
    # Get account for balance adjustment
    account = db.query(AccountDB).filter(AccountDB.id == db_transaction.account_id).first()
    
    try:
        # Store amount for balance adjustment
        transaction_amount = db_transaction.amount
        
        db.delete(db_transaction)
        db.commit()
        
        # Update account balance (reverse the transaction)
        if account:
            new_balance = account.balance - transaction_amount
            from .accounts import update_account_balance
            update_account_balance(db, account.id, new_balance)
        
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete transaction: {str(e)}")


def bulk_create_transactions(db: Session, user_id: int, transaction_import: TransactionImport) -> List[TransactionDB]:
    """Bulk import transactions with deduplication"""
    
    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")
    
    # Verify account exists and belongs to user
    account = db.query(AccountDB).filter(
        AccountDB.id == transaction_import.account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {transaction_import.account_id} not found")
    
    created_transactions = []
    skipped_duplicates = []
    errors = []
    
    for i, transaction_data in enumerate(transaction_import.transactions):
        try:
            # Override account_id to match the import request
            transaction_data.account_id = transaction_import.account_id
            transaction_data.source_type = transaction_import.source_type
            
            # Generate hash for deduplication
            transaction_hash = generate_transaction_hash(transaction_data, user_id)
            
            # Check for duplicate
            existing = db.query(TransactionDB).filter(
                TransactionDB.user_id == user_id,
                TransactionDB.transaction_hash == transaction_hash
            ).first()
            
            if existing:
                skipped_duplicates.append({
                    'index': i,
                    'transaction': transaction_data.model_dump(),
                    'reason': 'Duplicate transaction'
                })
                continue
            
            # Create transaction
            db_transaction = TransactionDB(
                id=uuid4(),
                external_transaction_id=transaction_data.external_transaction_id,
                user_id=user_id,
                account_id=transaction_data.account_id,
                transaction_hash=transaction_hash,
                source_type=SourceType(transaction_data.source_type.value),
                raw_data_json=transaction_data.raw_data,
                transaction_date=transaction_data.transaction_date,
                posted_date=transaction_data.posted_date,
                amount=transaction_data.amount,
                transaction_type=TransactionType(transaction_data.transaction_type.value),
                category=transaction_data.category,
                subcategory=transaction_data.subcategory,
                description=transaction_data.description,
                parsed_description=parse_transaction_description(transaction_data.description or ""),
                merchant_name=transaction_data.merchant_name,
                comments=transaction_data.comments,
                institution_name=account.institution_name,
                account_number_last4=account.account_number_last4,
                needs_review=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            db.add(db_transaction)
            created_transactions.append(db_transaction)
            
        except Exception as e:
            errors.append({
                'index': i,
                'transaction': transaction_data.model_dump(),
                'error': str(e)
            })
    
    try:
        if created_transactions:
            db.commit()
            
            # Refresh all created transactions
            for transaction in created_transactions:
                db.refresh(transaction)
            
            # Update account balance based on all imported transactions
            balance_change = sum(t.amount for t in created_transactions)
            new_balance = account.balance + balance_change
            from .accounts import update_account_balance
            update_account_balance(db, account.id, new_balance)
        
        # Log import results (you might want to return this info)
        import_results = {
            'created': len(created_transactions),
            'skipped': len(skipped_duplicates),
            'errors': len(errors),
            'skipped_details': skipped_duplicates,
            'error_details': errors
        }
        
        return created_transactions
        
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk transaction import failed: {str(e)}")


def update_account_balance_from_transaction(db: Session, account: AccountDB, transaction: TransactionDB):
    """Update account balance based on transaction type"""
    
    # This is a simplified version - you might want more complex logic
    # depending on your account types and transaction types
    
    if transaction.transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT]:
        new_balance = account.balance + transaction.amount
    elif transaction.transaction_type in [TransactionType.DEBIT, TransactionType.WITHDRAWAL, TransactionType.FEE]:
        new_balance = account.balance - transaction.amount
    else:
        # For TRANSFER and INTEREST, you might need special handling
        new_balance = account.balance + transaction.amount
    
    from .accounts import update_account_balance
    update_account_balance(db, account.id, new_balance)


def get_transaction_stats(db: Session, user_id: int, filters: Optional[TransactionFilter] = None) -> TransactionStats:
    """Get transaction statistics for a user"""
    
    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)
    
    # Apply the same filters as read_db_transactions
    if filters:
        if filters.account_id:
            query = query.filter(TransactionDB.account_id == filters.account_id)
        if filters.account_ids:
            query = query.filter(TransactionDB.account_id.in_(filters.account_ids))
        if filters.transaction_type:
            query = query.filter(TransactionDB.transaction_type == TransactionType(filters.transaction_type.value))
        if filters.category:
            query = query.filter(TransactionDB.category == filters.category)
        if filters.date_from:
            query = query.filter(TransactionDB.transaction_date >= filters.date_from)
        if filters.date_to:
            query = query.filter(TransactionDB.transaction_date <= filters.date_to)
    
    transactions = query.all()
    
    total_transactions = len(transactions)
    total_income = Decimal('0.00')
    total_expenses = Decimal('0.00')
    transactions_by_type = {}
    transactions_by_category = {}
    
    for transaction in transactions:
        # Count by type
        tx_type = transaction.transaction_type.value
        transactions_by_type[tx_type] = transactions_by_type.get(tx_type, 0) + 1
        
        # Sum by category
        category = transaction.category or 'Uncategorized'
        if category not in transactions_by_category:
            transactions_by_category[category] = Decimal('0.00')
        transactions_by_category[category] += transaction.amount
        
        # Calculate income vs expenses
        if transaction.transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.INTEREST]:
            total_income += transaction.amount
        elif transaction.transaction_type in [TransactionType.DEBIT, TransactionType.WITHDRAWAL, TransactionType.FEE]:
            total_expenses += abs(transaction.amount)
    
    net_amount = total_income - total_expenses
    
    return TransactionStats(
        total_transactions=total_transactions,
        total_income=total_income,
        total_expenses=total_expenses,
        net_amount=net_amount,
        transactions_by_type=transactions_by_type,
        transactions_by_category=transactions_by_category
    )


def get_transactions_count(db: Session, user_id: int, filters: Optional[TransactionFilter] = None) -> int:
    """Get count of transactions for pagination"""
    
    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)
    
    # Apply filters (same as read_db_transactions)
    if filters:
        if filters.account_id:
            query = query.filter(TransactionDB.account_id == filters.account_id)
        if filters.account_ids:
            query = query.filter(TransactionDB.account_id.in_(filters.account_ids))
        if filters.transaction_type:
            query = query.filter(TransactionDB.transaction_type == TransactionType(filters.transaction_type.value))
        if filters.category:
            query = query.filter(TransactionDB.category == filters.category)
        if filters.subcategory:
            query = query.filter(TransactionDB.subcategory == filters.subcategory)
        if filters.merchant_name:
            query = query.filter(TransactionDB.merchant_name.ilike(f"%{filters.merchant_name}%"))
        if filters.date_from:
            query = query.filter(TransactionDB.transaction_date >= filters.date_from)
        if filters.date_to:
            query = query.filter(TransactionDB.transaction_date <= filters.date_to)
        if filters.amount_min is not None:
            query = query.filter(TransactionDB.amount >= filters.amount_min)
        if filters.amount_max is not None:
            query = query.filter(TransactionDB.amount <= filters.amount_max)
        if filters.needs_review is not None:
            query = query.filter(TransactionDB.needs_review == filters.needs_review)
        if filters.description_search:
            query = query.filter(
                or_(
                    TransactionDB.description.ilike(f"%{filters.description_search}%"),
                    TransactionDB.parsed_description.ilike(f"%{filters.description_search}%")
                )
            )
    
    return query.count()


def get_transactions_by_category(db: Session, user_id: int, date_from: Optional[date] = None, 
                                date_to: Optional[date] = None) -> Dict[str, Decimal]:
    """Get transaction totals grouped by category"""
    
    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)
    
    if date_from:
        query = query.filter(TransactionDB.transaction_date >= date_from)
    if date_to:
        query = query.filter(TransactionDB.transaction_date <= date_to)
    
    transactions = query.all()
    
    category_totals = {}
    for transaction in transactions:
        category = transaction.category or 'Uncategorized'
        if category not in category_totals:
            category_totals[category] = Decimal('0.00')
        category_totals[category] += transaction.amount
    
    return category_totals


def mark_transaction_for_review(db: Session, transaction_id: int, user_id: int, needs_review: bool = True) -> TransactionDB:
    """Mark a transaction for review"""
    
    db_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")
    
    db_transaction.needs_review = needs_review
    db_transaction.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_transaction)
        return db_transaction
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to update transaction review status: {str(e)}")


def get_transactions_needing_review(db: Session, user_id: int, skip: int = 0, limit: int = 100) -> List[TransactionDB]:
    """Get transactions that need review"""
    
    return db.query(TransactionDB).filter(
        TransactionDB.user_id == user_id,
        TransactionDB.needs_review == True
    ).offset(skip).limit(limit).all()