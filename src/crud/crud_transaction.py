from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, desc, asc
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import uuid4, UUID
import hashlib

# Import your database models
from src.db.core import TransactionDB, AccountDB, UserDB, NotFoundError, TransactionType, SourceType, CategoryDB, TransactionRelationshipDB, TagDB, TransactionTagDB, AccountType
from src.models.transaction import TransactionCreate, TransactionUpdate, TransactionFilter, TransactionStats, TransactionImport, TransactionRelationshipCreate
from src.parser.models import ParsedTransaction
from src.crud.crud_account import update_account_balance
from src.logging_config import get_logger

logger = get_logger(__name__)


# ===== UTILITY FUNCTIONS =====

def generate_transaction_hash(transaction_data: TransactionCreate, user_id: int, institution_name: str) -> str:
    """Generate a hash for transaction deduplication based on stable data."""
    hash_string = (
        f"{user_id}|"
        f"{institution_name.lower()}|"
        f"{transaction_data.transaction_date}|"
        f"{transaction_data.transaction_type.value}|"
        f"{transaction_data.amount}|"
        f"{transaction_data.description or ''}"
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
    
    account = None
    if transaction_data.account_id:
        # Verify account exists and belongs to user
        account = db.query(AccountDB).filter(
            AccountDB.id == transaction_data.account_id,
            AccountDB.user_id == user_id
        ).first()
        if not account:
            raise NotFoundError(f"Account with id {transaction_data.account_id} not found")

    # Verify category and subcategory exist and are valid
    if transaction_data.category_id:
        category = db.query(CategoryDB).filter(CategoryDB.id == transaction_data.category_id).first()
        if not category:
            raise NotFoundError(f"Category with id {transaction_data.category_id} not found")
        if category.parent_category_id is not None:
            raise ValueError(f"Category with id {transaction_data.category_id} is a sub-category and cannot be a primary category.")

    if transaction_data.subcategory_id:
        subcategory = db.query(CategoryDB).filter(CategoryDB.id == transaction_data.subcategory_id).first()
        if not subcategory:
            raise NotFoundError(f"Sub-category with id {transaction_data.subcategory_id} not found")
        if subcategory.parent_category_id != transaction_data.category_id:
            raise ValueError(f"Sub-category '{subcategory.name}' does not belong to category ID {transaction_data.category_id}")

    # Generate transaction hash for deduplication
    institution_name = account.institution_name if account else ""
    transaction_hash = generate_transaction_hash(transaction_data, user_id, institution_name)
    
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
        user_id=user_id,
        account_id=transaction_data.account_id,
        category_id=transaction_data.category_id,
        subcategory_id=transaction_data.subcategory_id,
        transaction_hash=transaction_hash,
        source_type=SourceType(transaction_data.source_type.value),
        transaction_date=transaction_data.transaction_date,
        amount=transaction_data.amount,
        transaction_type=TransactionType(transaction_data.transaction_type.value),
        description=transaction_data.description,
        parsed_description=parse_transaction_description(transaction_data.description or ""),
        merchant_name=transaction_data.merchant_name,
        comments=transaction_data.comments,
        institution_name=institution_name,
        account_number_last4=account.account_number_last4 if account else None,
        needs_review=False if account else True,
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow()
    )
    
    try:
        db.add(db_transaction)
        db.commit()
        db.refresh(db_transaction)
        
        if account:
            # Update account balance (you might want to do this in a separate service)
            update_account_balance_from_transaction(db, account, db_transaction)
        
        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Transaction creation failed due to database constraint")

def create_transaction_relationship(db: Session, user_id: int, from_transaction_id: int, relationship_data: TransactionRelationshipCreate) -> TransactionRelationshipDB:
    """Create a relationship between two transactions"""
    # Verify from_transaction exists and belongs to user
    from_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == from_transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    if not from_transaction:
        raise NotFoundError(f"Transaction with id {from_transaction_id} not found")

    # Verify to_transaction exists and belongs to user
    to_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == relationship_data.to_transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    if not to_transaction:
        raise NotFoundError(f"Transaction with id {relationship_data.to_transaction_id} not found")

    # Create new relationship
    db_relationship = TransactionRelationshipDB(
        from_transaction_id=from_transaction_id,
        to_transaction_id=relationship_data.to_transaction_id,
        relationship_type=relationship_data.relationship_type,
        amount_allocated=relationship_data.amount_allocated,
        notes=relationship_data.notes
    )

    try:
        db.add(db_relationship)
        db.commit()
        db.refresh(db_relationship)
        return db_relationship
    except IntegrityError:
        db.rollback()
        raise ValueError("Relationship creation failed due to database constraint")


def update_transaction_relationship(db: Session, user_id: int, relationship_id: int, relationship_updates: Dict[str, Any]) -> TransactionRelationshipDB:
    """Update an existing transaction relationship"""

    # Get the existing relationship
    db_relationship = db.query(TransactionRelationshipDB).filter(
        TransactionRelationshipDB.relationship_id == relationship_id
    ).first()

    if not db_relationship:
        raise NotFoundError(f"Relationship with id {relationship_id} not found")

    # Verify user owns both transactions in the relationship
    from_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == db_relationship.from_transaction_id,
        TransactionDB.user_id == user_id
    ).first()

    if not from_transaction:
        raise NotFoundError(f"Transaction relationship not found or doesn't belong to user")

    # If updating to_transaction_id, verify new transaction exists and belongs to user
    if 'to_transaction_id' in relationship_updates:
        to_transaction = db.query(TransactionDB).filter(
            TransactionDB.db_id == relationship_updates['to_transaction_id'],
            TransactionDB.user_id == user_id
        ).first()
        if not to_transaction:
            raise NotFoundError(f"Transaction with id {relationship_updates['to_transaction_id']} not found")

    # Update the relationship fields
    for field, value in relationship_updates.items():
        if hasattr(db_relationship, field):
            setattr(db_relationship, field, value)

    try:
        db.commit()
        db.refresh(db_relationship)
        return db_relationship
    except IntegrityError:
        db.rollback()
        raise ValueError("Relationship update failed due to database constraint")


def delete_transaction_relationship(db: Session, user_id: int, relationship_id: int) -> bool:
    """Delete a transaction relationship"""

    # Get the relationship
    db_relationship = db.query(TransactionRelationshipDB).filter(
        TransactionRelationshipDB.relationship_id == relationship_id
    ).first()

    if not db_relationship:
        raise NotFoundError(f"Relationship with id {relationship_id} not found")

    # Verify user owns the from_transaction
    from_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == db_relationship.from_transaction_id,
        TransactionDB.user_id == user_id
    ).first()

    if not from_transaction:
        raise NotFoundError(f"Transaction relationship not found or doesn't belong to user")

    try:
        db.delete(db_relationship)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete transaction relationship: {str(e)}")


def read_db_transaction(db: Session, transaction_id: int, user_id: Optional[int] = None) -> Optional[TransactionDB]:
    """Read a transaction by ID"""
    
    query = db.query(TransactionDB).filter(TransactionDB.db_id == transaction_id)
    
    if user_id:
        query = query.filter(TransactionDB.user_id == user_id)
    
    return query.options(joinedload(TransactionDB.category), joinedload(TransactionDB.subcategory)).first()


def read_db_transaction_by_uuid(db: Session, transaction_uuid: UUID, user_id: Optional[int] = None) -> Optional[TransactionDB]:
    """Read a transaction by UUID"""
    
    query = db.query(TransactionDB).filter(TransactionDB.id == transaction_uuid)
    
    if user_id:
        query = query.filter(TransactionDB.user_id == user_id)
    
    return query.options(joinedload(TransactionDB.category), joinedload(TransactionDB.subcategory)).first()


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
        
        if filters.category_id:
            query = query.filter(TransactionDB.category_id == filters.category_id)

        if filters.subcategory_id:
            query = query.filter(TransactionDB.subcategory_id == filters.subcategory_id)
        
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
            
        if filters.tag_id:
            query = query.join(TransactionDB.tags).filter(TagDB.id == filters.tag_id)
    
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
    
    return query.options(joinedload(TransactionDB.category), joinedload(TransactionDB.subcategory)).offset(skip).limit(limit).all()


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

    # Verify category and subcategory exist and are valid if updated
    if 'category_id' in update_data or 'subcategory_id' in update_data:
        category_id = update_data.get('category_id', db_transaction.category_id)
        subcategory_id = update_data.get('subcategory_id', db_transaction.subcategory_id)

        if category_id:
            category = db.query(CategoryDB).filter(CategoryDB.id == category_id).first()
            if not category:
                raise NotFoundError(f"Category with id {category_id} not found")
            if category.parent_category_id is not None:
                raise ValueError(f"Category with id {category_id} is a sub-category and cannot be a primary category.")

        if subcategory_id:
            if not category_id:
                raise ValueError("Cannot assign a sub-category without a primary category.")
            subcategory = db.query(CategoryDB).filter(CategoryDB.id == subcategory_id).first()
            if not subcategory:
                raise NotFoundError(f"Sub-category with id {subcategory_id} not found")
            if subcategory.parent_category_id != category_id:
                raise ValueError(f"Sub-category ID {subcategory_id} does not belong to category ID {category_id}")

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
        if 'amount' in update_data and update_data['amount'] != old_amount and db_transaction.account_id is not None:
            account = db.query(AccountDB).filter(AccountDB.id == db_transaction.account_id).first()
            if account:
                # Reverse old amount and apply new amount
                balance_adjustment = db_transaction.amount - old_amount
                new_balance = account.balance + balance_adjustment
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
            transaction_hash = generate_transaction_hash(transaction_data, user_id, account.institution_name)
            
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
                user_id=user_id,
                account_id=transaction_data.account_id,
                category_id=transaction_data.category_id,
                subcategory_id=transaction_data.subcategory_id,
                transaction_hash=transaction_hash,
                source_type=SourceType(transaction_data.source_type.value),
                transaction_date=transaction_data.transaction_date,
                amount=transaction_data.amount,
                transaction_type=TransactionType(transaction_data.transaction_type.value),
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
    
    if transaction.transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT]:
        new_balance = account.balance + transaction.amount
    elif transaction.transaction_type in [TransactionType.WITHDRAWAL, TransactionType.FEE, TransactionType.PURCHASE]:
        new_balance = account.balance - transaction.amount
    elif transaction.transaction_type == TransactionType.INTEREST:
        if account.account_type == AccountType.CREDIT_CARD:
            new_balance = account.balance - transaction.amount
        else:
            new_balance = account.balance + transaction.amount
    else:
        # For TRANSFER, you might need special handling
        logger.debug(f"Transaction type {transaction.transaction_type} requires special handling for balance update")
        breakpoint()
        new_balance = account.balance + transaction.amount
    
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
        if filters.category_id:
            query = query.filter(TransactionDB.category_id == filters.category_id)
        if filters.subcategory_id:
            query = query.filter(TransactionDB.subcategory_id == filters.subcategory_id)
        if filters.date_from:
            query = query.filter(TransactionDB.transaction_date >= filters.date_from)
        if filters.date_to:
            query = query.filter(TransactionDB.transaction_date <= filters.date_to)
    
    transactions = query.options(joinedload(TransactionDB.category), joinedload(TransactionDB.subcategory)).all()
    
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
        category_name = transaction.category.name if transaction.category else 'Uncategorized'
        if category_name not in transactions_by_category:
            transactions_by_category[category_name] = Decimal('0.00')
        transactions_by_category[category_name] += transaction.amount
        
        # Calculate income vs expenses
        if transaction.transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.INTEREST]:
            total_income += transaction.amount
        elif transaction.transaction_type in [TransactionType.WITHDRAWAL, TransactionType.FEE, TransactionType.PURCHASE]:
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
        if filters.category_id:
            query = query.filter(TransactionDB.category_id == filters.category_id)
        if filters.subcategory_id:
            query = query.filter(TransactionDB.subcategory_id == filters.subcategory_id)
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
    
    transactions = query.options(joinedload(TransactionDB.category)).all()
    
    category_totals = {}
    for transaction in transactions:
        category_name = transaction.category.name if transaction.category else 'Uncategorized'
        if category_name not in category_totals:
            category_totals[category_name] = Decimal('0.00')
        category_totals[category_name] += transaction.amount
    
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
    ).options(joinedload(TransactionDB.category), joinedload(TransactionDB.subcategory)).offset(skip).limit(limit).all()

def bulk_create_transactions_from_parsed_data(
    db: Session,
    user_id: int,
    transactions: List[ParsedTransaction],
    institution_name: str,
    account_id: Optional[int],
) -> List[TransactionDB]:
    """Bulk import transactions from a parsed file, with an optional account_id."""
    account = None
    if account_id:
        account = (
            db.query(AccountDB)
            .filter(AccountDB.id == account_id, AccountDB.user_id == user_id)
            .first()
        )
        if not account:
            raise NotFoundError(f"Account with id {account_id} not found for this user.")

    created_transactions = []
    processed_hashes = set()
    duplicate_tag = None
    duplicate_count = 0

    for t_data in transactions:
        try:
            # Assumes parser provides the name of the enum member (case-insensitive)
            transaction_type_enum = TransactionType[t_data.transaction_type.upper()]
        except KeyError:
            logger.warning(f"Skipping transaction with unknown type: {t_data.transaction_type}")
            continue

        # Create a TransactionCreate object to ensure data consistency and for hashing
        transaction_to_create = TransactionCreate(
            transaction_date=t_data.transaction_date,
            amount=t_data.amount,
            description=t_data.description,
            transaction_type=transaction_type_enum,
            account_id=account_id,
            source_type=SourceType.PDF,
        )

        transaction_hash = generate_transaction_hash(
            transaction_to_create, user_id, institution_name
        )

        if transaction_hash in processed_hashes:
            continue

        processed_hashes.add(transaction_hash)

        # Check if transaction already exists in database
        existing = (
            db.query(TransactionDB)
            .filter(
                TransactionDB.user_id == user_id,
                TransactionDB.transaction_hash == transaction_hash,
            )
            .first()
        )

        # Flag for review if: no account specified OR duplicate found in database
        needs_review_flag = True if not account_id else False
        is_duplicate = False
        if existing:
            needs_review_flag = True
            is_duplicate = True
            duplicate_count += 1
            logger.debug(f"Found duplicate transaction in database (will flag for review): {t_data.transaction_date} - {t_data.description}")

        db_transaction = TransactionDB(
            id=uuid4(),
            user_id=user_id,
            account_id=transaction_to_create.account_id,
            transaction_hash=transaction_hash,
            transaction_date=transaction_to_create.transaction_date,
            amount=transaction_to_create.amount,
            transaction_type=transaction_to_create.transaction_type,
            description=transaction_to_create.description,
            parsed_description=parse_transaction_description(transaction_to_create.description or ""),
            institution_name=institution_name,
            account_number_last4=account.account_number_last4 if account else None,
            source_type=transaction_to_create.source_type,
            needs_review=needs_review_flag,
        )

        # Apply duplicate tag if duplicate found in database
        if is_duplicate:
            if duplicate_tag is None:
                duplicate_tag = db.query(TagDB).filter(TagDB.user_id == user_id, TagDB.tag_name == "duplicate").first()
                if duplicate_tag is None:
                    duplicate_tag = TagDB(user_id=user_id, tag_name="duplicate", created_at=datetime.utcnow())
                    db.add(duplicate_tag)
            db_transaction.transaction_tags.append(TransactionTagDB(tag=duplicate_tag))

        db.add(db_transaction)
        created_transactions.append(db_transaction)

    if duplicate_count > 0:
        logger.info(f"Flagged {duplicate_count} duplicate transactions for review")

    if not created_transactions:
        return []

    try:
        db.commit()
        for t in created_transactions:
            db.refresh(t)
        if account:
            for t in created_transactions:
                update_account_balance_from_transaction(db, account, t)
        return created_transactions
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk transaction import failed: {str(e)}")

def bulk_update_db_transactions(db: Session, user_id: int, transaction_ids: List[int], updates: Dict[str, Any]) -> int:
    """Bulk update transactions for a user."""
    
    if not transaction_ids:
        return 0
        
    # Fetch transactions to ensure they belong to the user and exist
    transactions_to_update = db.query(TransactionDB).filter(
        TransactionDB.user_id == user_id,
        TransactionDB.id.in_(transaction_ids)
    ).all()
    
    if len(transactions_to_update) != len(transaction_ids):
        # This indicates that some transaction IDs were not found or didn't belong to the user
        found_ids = {t.id for t in transactions_to_update}
        missing_ids = set(transaction_ids) - found_ids
        raise NotFoundError(f"Transactions with IDs {missing_ids} not found or not owned by user.")

    # Perform the update
    # Note: This performs a bulk update, which is more efficient than updating one by one.
    # However, it does not trigger individual object lifecycle events (e.g., before_update).
    # For this use case, it's acceptable.
    update_query = db.query(TransactionDB).filter(
        TransactionDB.user_id == user_id,
        TransactionDB.id.in_(transaction_ids)
    )
    
    # Add updated_at timestamp
    updates_with_timestamp = {**updates, "updated_at": datetime.utcnow()}
    
    updated_count = update_query.update(updates_with_timestamp, synchronize_session=False)
    
    try:
        db.commit()
        return updated_count
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk transaction update failed: {str(e)}")
