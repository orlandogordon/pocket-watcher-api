from collections import defaultdict

from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, desc, asc, exists, select, func
from typing import Optional, List, Dict, Any, Set, Tuple
from datetime import datetime, date
from decimal import Decimal
from uuid import uuid4, UUID
import hashlib

# Import your database models
from src.db.core import TransactionDB, AccountDB, UserDB, NotFoundError, TransactionType, SourceType, CategoryDB, TransactionRelationshipDB, RelationshipType, TagDB, TransactionTagDB, AccountType, TransactionSplitAllocationDB, TransactionAmortizationScheduleDB
from src.models.transaction import (
    TransactionCreate, TransactionUpdate, TransactionFilter, TransactionStats,
    TransactionImport, TransactionSplitRequest, AmortizationScheduleCreate,
    AmortizationScheduleResponse, AmortizationScheduleEntry,
    MonthlyAverageResponse, MonthlyAverageTotals, MonthlyAverageCategoryBreakdown,
    MonthlyAverageSubcategoryBreakdown, MonthlyAverageMonthBreakdown,
)
from src.parser.models import ParsedTransaction
from src.crud.crud_account import update_account_balance
from src.crud.crud_category import read_db_categories_by_uuids
from src.logging_config import get_logger

logger = get_logger(__name__)

# Relationship types that reduce the original transaction's effective amount
ABSORBING_RELATIONSHIP_TYPES = {
    RelationshipType.REFUNDS,
    RelationshipType.OFFSETS,
    RelationshipType.REVERSES,
}


# ===== REFUND ATTRIBUTION =====

def get_refund_adjustments(
    db: Session,
    user_id: int,
    transaction_ids: List[int],
) -> Tuple[Dict[int, Decimal], Set[int]]:
    """For a set of transaction IDs, returns:
      1. adjustments: {original_txn_id: total_amount_to_subtract}
      2. absorbed_ids: set of refund/offset/reversal txn IDs whose impact is
         fully captured in the adjustment (should be excluded from totals)

    Only considers relationships where amount_allocated is not None.
    """
    if not transaction_ids:
        return {}, set()

    relationships = (
        db.query(TransactionRelationshipDB)
        .filter(
            TransactionRelationshipDB.relationship_type.in_(ABSORBING_RELATIONSHIP_TYPES),
            TransactionRelationshipDB.amount_allocated.isnot(None),
            or_(
                TransactionRelationshipDB.to_transaction_id.in_(transaction_ids),
                TransactionRelationshipDB.from_transaction_id.in_(transaction_ids),
            ),
        )
        .all()
    )

    adjustments: Dict[int, Decimal] = defaultdict(Decimal)
    absorbed_ids: Set[int] = set()

    for rel in relationships:
        adjustments[rel.to_transaction_id] += rel.amount_allocated
        absorbed_ids.add(rel.from_transaction_id)

    return dict(adjustments), absorbed_ids


def validate_refund_allocation(
    db: Session,
    to_transaction_id: int,
    new_amount_allocated: Decimal,
    exclude_relationship_id: Optional[int] = None,
) -> None:
    """Validate that total refund allocations don't exceed the original transaction amount.

    Args:
        to_transaction_id: The original transaction being refunded.
        new_amount_allocated: The amount being allocated in the new/updated relationship.
        exclude_relationship_id: If updating, exclude this relationship from the sum.

    Raises:
        ValueError if the total would exceed the original transaction amount.
    """
    original_txn = db.query(TransactionDB).filter(
        TransactionDB.db_id == to_transaction_id
    ).first()
    if not original_txn:
        return  # Will be caught by NotFoundError elsewhere

    # Sum existing absorbing allocations on this transaction
    existing_query = db.query(
        func.coalesce(func.sum(TransactionRelationshipDB.amount_allocated), 0)
    ).filter(
        TransactionRelationshipDB.to_transaction_id == to_transaction_id,
        TransactionRelationshipDB.relationship_type.in_(ABSORBING_RELATIONSHIP_TYPES),
        TransactionRelationshipDB.amount_allocated.isnot(None),
    )
    if exclude_relationship_id is not None:
        existing_query = existing_query.filter(
            TransactionRelationshipDB.relationship_id != exclude_relationship_id
        )
    existing_total = Decimal(str(existing_query.scalar()))

    proposed_total = existing_total + new_amount_allocated
    if proposed_total > original_txn.amount:
        raise ValueError(
            f"Total refund/offset amount (${proposed_total}) would exceed "
            f"original transaction amount (${original_txn.amount})"
        )


# ===== UTILITY FUNCTIONS =====

def generate_transaction_hash(
    user_id: int,
    institution_name: str,
    transaction_date,
    transaction_type_value: str,
    amount,
    description: str | None = None,
    make_unique: bool = False,
) -> str:
    """Generate a hash for transaction deduplication based on stable data.

    Args:
        transaction_type_value: The string value of the transaction type enum (e.g. "PURCHASE").
        make_unique: If True, append a UUID to guarantee a unique hash.
                     Used for approved duplicates in the preview flow.
    """
    hash_string = (
        f"{user_id}|"
        f"{institution_name.lower()}|"
        f"{transaction_date}|"
        f"{transaction_type_value}|"
        f"{amount}|"
        f"{description or ''}"
    )
    if make_unique:
        hash_string += f"|{uuid4()}"
    return hashlib.sha256(hash_string.encode()).hexdigest()


def get_original_transaction_for_duplicate(
    db: Session,
    user_id: int,
    transaction_hash: str
) -> Optional[TransactionDB]:
    """
    Find the original transaction that matches this hash.
    Used when a duplicate is detected to show user which transaction it duplicates.
    Returns the oldest transaction with this hash.
    """
    return db.query(TransactionDB).filter(
        TransactionDB.user_id == user_id,
        TransactionDB.transaction_hash == transaction_hash
    ).order_by(TransactionDB.created_at.asc()).first()


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

def create_db_transaction(db: Session, user_id: int, transaction_data: TransactionCreate, *,
                          account_id: Optional[int] = None, category_id: Optional[int] = None,
                          subcategory_id: Optional[int] = None) -> TransactionDB:
    """Create a new transaction"""

    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")

    account = None
    if account_id:
        # Verify account exists and belongs to user
        account = db.query(AccountDB).filter(
            AccountDB.id == account_id,
            AccountDB.user_id == user_id
        ).first()
        if not account:
            raise NotFoundError(f"Account with id {account_id} not found")

    # Verify category and subcategory exist and are valid
    if category_id:
        category = db.query(CategoryDB).filter(CategoryDB.id == category_id).first()
        if not category:
            raise NotFoundError(f"Category with id {category_id} not found")
        if category.parent_category_id is not None:
            raise ValueError(f"Category with id {category_id} is a sub-category and cannot be a primary category.")

    if subcategory_id:
        subcategory = db.query(CategoryDB).filter(CategoryDB.id == subcategory_id).first()
        if not subcategory:
            raise NotFoundError(f"Sub-category with id {subcategory_id} not found")
        if subcategory.parent_category_id != category_id:
            raise ValueError(f"Sub-category '{subcategory.name}' does not belong to category ID {category_id}")

    # Generate transaction hash for deduplication
    institution_name = account.institution_name if account else ""
    transaction_hash = generate_transaction_hash(
        user_id=user_id,
        institution_name=institution_name,
        transaction_date=transaction_data.transaction_date,
        transaction_type_value=transaction_data.transaction_type.value,
        amount=transaction_data.amount,
        description=transaction_data.description,
    )

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
        account_id=account_id,
        category_id=category_id,
        subcategory_id=subcategory_id,
        transaction_hash=transaction_hash,
        source_type=SourceType(transaction_data.source_type.value),
        transaction_date=transaction_data.transaction_date,
        amount=abs(transaction_data.amount),
        transaction_type=TransactionType(transaction_data.transaction_type.value),
        description=transaction_data.description,
        parsed_description=parse_transaction_description(transaction_data.description or ""),
        merchant_name=transaction_data.merchant_name,
        comments=transaction_data.comments,
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

def create_transaction_relationship(db: Session, user_id: int, from_transaction_id: int, relationship_data: Any) -> TransactionRelationshipDB:
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

    # Validate refund allocation doesn't exceed original transaction amount
    if (relationship_data.relationship_type in ABSORBING_RELATIONSHIP_TYPES
            and relationship_data.amount_allocated is not None):
        validate_refund_allocation(db, relationship_data.to_transaction_id, relationship_data.amount_allocated)

    # Create new relationship
    db_relationship = TransactionRelationshipDB(
        id=uuid4(),
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
    
    return query.options(joinedload(TransactionDB.category), joinedload(TransactionDB.subcategory), joinedload(TransactionDB.account)).first()


def read_db_transaction_by_uuid(db: Session, transaction_uuid: UUID, user_id: Optional[int] = None) -> Optional[TransactionDB]:
    """Read a transaction by UUID"""

    query = db.query(TransactionDB).filter(TransactionDB.id == transaction_uuid)

    if user_id:
        query = query.filter(TransactionDB.user_id == user_id)

    return query.options(
        joinedload(TransactionDB.category),
        joinedload(TransactionDB.subcategory),
        joinedload(TransactionDB.account),
        joinedload(TransactionDB.transaction_tags).joinedload(TransactionTagDB.tag),
        joinedload(TransactionDB.split_allocations).joinedload(TransactionSplitAllocationDB.category),
        joinedload(TransactionDB.split_allocations).joinedload(TransactionSplitAllocationDB.subcategory),
    ).first()


def _apply_transaction_filters(query, filters: TransactionFilter):
    """Apply TransactionFilter conditions to an existing query.

    Shared by read_db_transactions, get_transaction_stats, and get_transactions_count
    so the filtering logic is defined in exactly one place.
    """
    if filters.account_id:
        query = query.filter(TransactionDB.account_id == filters.account_id)
    if filters.account_ids:
        query = query.filter(TransactionDB.account_id.in_(filters.account_ids))
    if filters.transaction_type:
        query = query.filter(TransactionDB.transaction_type == TransactionType(filters.transaction_type.value))
    if filters.category_ids:
        split_cat_exists = exists(
            select(TransactionSplitAllocationDB.allocation_id).where(
                TransactionSplitAllocationDB.transaction_id == TransactionDB.db_id,
                TransactionSplitAllocationDB.category_id.in_(filters.category_ids),
            )
        )
        query = query.filter(
            or_(
                TransactionDB.category_id.in_(filters.category_ids),
                split_cat_exists,
            )
        )
    if filters.subcategory_ids:
        split_sub_exists = exists(
            select(TransactionSplitAllocationDB.allocation_id).where(
                TransactionSplitAllocationDB.transaction_id == TransactionDB.db_id,
                TransactionSplitAllocationDB.subcategory_id.in_(filters.subcategory_ids),
            )
        )
        query = query.filter(
            or_(
                TransactionDB.subcategory_id.in_(filters.subcategory_ids),
                split_sub_exists,
            )
        )
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
    if filters.description_search:
        query = query.filter(
            or_(
                TransactionDB.description.ilike(f"%{filters.description_search}%"),
                TransactionDB.parsed_description.ilike(f"%{filters.description_search}%")
            )
        )
    if filters.tag_ids:
        # Use EXISTS subquery to avoid duplicate rows from JOIN
        tag_subquery = (
            select(TransactionTagDB.transaction_id)
            .where(
                TransactionTagDB.tag_id.in_(filters.tag_ids),
                TransactionTagDB.transaction_id == TransactionDB.db_id,
            )
            .exists()
        )
        query = query.filter(tag_subquery)
    return query


def read_db_transactions(db: Session, user_id: int, filters: Optional[TransactionFilter] = None,
                        skip: int = 0, limit: int = 100, order_by: str = "transaction_date",
                        order_desc: bool = True) -> List[TransactionDB]:
    """Read transactions with filtering and pagination"""

    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)

    if filters:
        query = _apply_transaction_filters(query, filters)

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
    
    return query.options(
        joinedload(TransactionDB.category),
        joinedload(TransactionDB.subcategory),
        joinedload(TransactionDB.account),
        joinedload(TransactionDB.transaction_tags).joinedload(TransactionTagDB.tag),
        joinedload(TransactionDB.split_allocations).joinedload(TransactionSplitAllocationDB.category),
        joinedload(TransactionDB.split_allocations).joinedload(TransactionSplitAllocationDB.subcategory),
    ).offset(skip).limit(limit).all()


def update_db_transaction(db: Session, transaction_id: int, user_id: int,
                         transaction_updates: TransactionUpdate, *,
                         account_id: Optional[int] = None,
                         clear_account: bool = False,
                         category_id: Optional[int] = None,
                         subcategory_id: Optional[int] = None,
                         clear_category: bool = False,
                         clear_subcategory: bool = False) -> TransactionDB:
    """Update an existing transaction"""

    # Get the existing transaction
    db_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == transaction_id,
        TransactionDB.user_id == user_id
    ).first()

    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found")

    # Store old state for balance adjustment
    old_amount = db_transaction.amount
    old_account_id = db_transaction.account_id
    old_account = db.query(AccountDB).filter(AccountDB.id == old_account_id).first() if old_account_id else None
    old_txn_type = db_transaction.transaction_type

    # Update only the fields that are provided
    update_data = transaction_updates.model_dump(exclude_unset=True)

    # Apply account changes and remove account_uuid from update_data
    if clear_account:
        db_transaction.account_id = None
    elif account_id is not None:
        db_transaction.account_id = account_id
    update_data.pop('account_uuid', None)

    # Verify category and subcategory exist and are valid if updated
    # Determine effective IDs after applying clears
    if clear_category:
        resolved_category_id = None
    elif category_id is not None:
        resolved_category_id = category_id
    else:
        resolved_category_id = db_transaction.category_id

    if clear_subcategory:
        resolved_subcategory_id = None
    elif subcategory_id is not None:
        resolved_subcategory_id = subcategory_id
    else:
        resolved_subcategory_id = db_transaction.subcategory_id

    if category_id is not None or subcategory_id is not None:
        if resolved_category_id:
            category = db.query(CategoryDB).filter(CategoryDB.id == resolved_category_id).first()
            if not category:
                raise NotFoundError(f"Category with id {resolved_category_id} not found")
            if category.parent_category_id is not None:
                raise ValueError(f"Category with id {resolved_category_id} is a sub-category and cannot be a primary category.")

        if resolved_subcategory_id:
            if not resolved_category_id:
                raise ValueError("Cannot assign a sub-category without a primary category.")
            subcategory = db.query(CategoryDB).filter(CategoryDB.id == resolved_subcategory_id).first()
            if not subcategory:
                raise NotFoundError(f"Sub-category with id {resolved_subcategory_id} not found")
            if subcategory.parent_category_id != resolved_category_id:
                raise ValueError(f"Sub-category ID {resolved_subcategory_id} does not belong to category ID {resolved_category_id}")

    # Apply category/subcategory int IDs directly and remove UUID fields from update_data
    if clear_category:
        db_transaction.category_id = None
    elif category_id is not None:
        db_transaction.category_id = category_id
    update_data.pop('category_uuid', None)
    if clear_subcategory:
        db_transaction.subcategory_id = None
    elif subcategory_id is not None:
        db_transaction.subcategory_id = subcategory_id
    update_data.pop('subcategory_uuid', None)

    if 'amount' in update_data and update_data['amount'] is not None:
        update_data['amount'] = abs(update_data['amount'])

    for field, value in update_data.items():
        if field == 'transaction_type' and value:
            setattr(db_transaction, field, TransactionType(value.value))
        elif field == 'parsed_description' and field == 'description' and value:
            setattr(db_transaction, 'description', value)
            setattr(db_transaction, 'parsed_description', parse_transaction_description(value))
        else:
            setattr(db_transaction, field, value)
    
    # If amount changed and splits exist, clear splits if they no longer match
    if 'amount' in update_data and db_transaction.split_allocations:
        alloc_sum = sum(a.amount for a in db_transaction.split_allocations)
        if alloc_sum != db_transaction.amount:
            db.query(TransactionSplitAllocationDB).filter(
                TransactionSplitAllocationDB.transaction_id == db_transaction.db_id
            ).delete()
            logger.info(f"Cleared split allocations for transaction {db_transaction.id} — amount changed")

    # Always update the updated_at timestamp
    db_transaction.updated_at = datetime.utcnow()

    try:
        db.commit()
        db.refresh(db_transaction)

        # Update account balances if amount or account changed
        amount_changed = 'amount' in update_data and update_data['amount'] != old_amount
        account_changed = old_account_id != db_transaction.account_id

        if amount_changed or account_changed:
            # Reverse old effect from OLD account
            if old_account:
                reversed_balance = _reverse_balance_effect(old_account, old_txn_type, old_amount)
                update_account_balance(db, old_account.id, reversed_balance)
            # Apply new effect to NEW account
            if db_transaction.account_id:
                new_account = db.query(AccountDB).filter(AccountDB.id == db_transaction.account_id).first()
                if new_account:
                    update_account_balance_from_transaction(db, new_account, db_transaction)
        
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
        # Store transaction info for balance adjustment and snapshot recalculation
        transaction_amount = db_transaction.amount
        transaction_type = db_transaction.transaction_type
        transaction_date = db_transaction.transaction_date
        account_id = db_transaction.account_id

        db.delete(db_transaction)
        db.commit()

        # Update account balance (reverse the transaction)
        if account:
            new_balance = _reverse_balance_effect(account, transaction_type, transaction_amount)
            update_account_balance(db, account.id, new_balance)

        # Trigger snapshot recalculation from deleted transaction's date
        if account_id:
            from src.services.account_snapshot import trigger_backfill_if_needed
            trigger_backfill_if_needed(db, user_id, account_id, transaction_date)

        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete transaction: {str(e)}")


def bulk_create_transactions(db: Session, user_id: int, transaction_import: TransactionImport, *,
                             account_id: int) -> List[TransactionDB]:
    """Bulk import transactions with deduplication"""

    # Verify user exists
    user = db.query(UserDB).filter(UserDB.db_id == user_id).first()
    if not user:
        raise NotFoundError(f"User with id {user_id} not found")

    # Verify account exists and belongs to user
    account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {account_id} not found")

    created_transactions = []
    skipped_duplicates = []
    errors = []

    for i, transaction_data in enumerate(transaction_import.transactions):
        try:
            # Override source_type to match the import request
            transaction_data.source_type = transaction_import.source_type

            # Generate hash for deduplication
            transaction_hash = generate_transaction_hash(
                user_id=user_id,
                institution_name=account.institution_name,
                transaction_date=transaction_data.transaction_date,
                transaction_type_value=transaction_data.transaction_type.value,
                amount=transaction_data.amount,
                description=transaction_data.description,
            )

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

            db_transaction = TransactionDB(
                id=uuid4(),
                user_id=user_id,
                account_id=account_id,
                category_id=None,
                subcategory_id=None,
                transaction_hash=transaction_hash,
                source_type=SourceType(transaction_data.source_type.value),
                transaction_date=transaction_data.transaction_date,
                amount=abs(transaction_data.amount),
                transaction_type=TransactionType(transaction_data.transaction_type.value),
                description=transaction_data.description,
                parsed_description=parse_transaction_description(transaction_data.description or ""),
                merchant_name=transaction_data.merchant_name,
                comments=transaction_data.comments,
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
            for t in created_transactions:
                update_account_balance_from_transaction(db, account, t)
        
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


def _reverse_balance_effect(account: AccountDB, transaction_type: TransactionType, amount: 'Decimal') -> 'Decimal':
    """Calculate what the account balance would be if we reversed a transaction's effect.

    This is the inverse of update_account_balance_from_transaction.
    Uses abs(amount) so transaction_type alone determines direction,
    regardless of whether the stored amount is positive or negative.
    """
    abs_amount = abs(amount)
    if account.account_type == AccountType.CREDIT_CARD:
        if transaction_type in [TransactionType.PURCHASE, TransactionType.FEE]:
            return account.balance - abs_amount
        elif transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT]:
            return account.balance + abs_amount
        elif transaction_type == TransactionType.INTEREST:
            return account.balance - abs_amount
        else:
            return account.balance - abs_amount
    else:
        if transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT]:
            return account.balance - abs_amount
        elif transaction_type in [TransactionType.WITHDRAWAL, TransactionType.FEE, TransactionType.PURCHASE]:
            return account.balance + abs_amount
        elif transaction_type == TransactionType.INTEREST:
            return account.balance - abs_amount
        else:
            return account.balance - abs_amount


def update_account_balance_from_transaction(db: Session, account: AccountDB, transaction: TransactionDB):
    """Update account balance based on transaction type.

    Uses abs(amount) so transaction_type alone determines direction,
    regardless of whether the stored amount is positive or negative.

    For credit cards, balance represents debt (positive = you owe more),
    so the sign logic is inverted vs. checking/savings accounts.
    """
    abs_amount = abs(transaction.amount)
    if account.account_type == AccountType.CREDIT_CARD:
        # Credit card: positive balance = debt owed
        if transaction.transaction_type in [TransactionType.PURCHASE, TransactionType.FEE, TransactionType.INTEREST]:
            new_balance = account.balance + abs_amount  # increases debt
        elif transaction.transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.TRANSFER_IN]:
            new_balance = account.balance - abs_amount  # reduces debt (payment received)
        elif transaction.transaction_type == TransactionType.WITHDRAWAL:
            new_balance = account.balance - abs_amount  # reduces debt
        elif transaction.transaction_type == TransactionType.TRANSFER_OUT:
            new_balance = account.balance + abs_amount  # increases debt (refund sent back — rare)
        else:
            raise ValueError(f"Unhandled transaction type: {transaction.transaction_type}")
    else:
        # All other account types (checking, savings, loan, investment)
        if transaction.transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.TRANSFER_IN]:
            new_balance = account.balance + abs_amount  # money received
        elif transaction.transaction_type in [TransactionType.WITHDRAWAL, TransactionType.FEE, TransactionType.PURCHASE, TransactionType.TRANSFER_OUT]:
            new_balance = account.balance - abs_amount  # money sent
        elif transaction.transaction_type == TransactionType.INTEREST:
            new_balance = account.balance + abs_amount
        else:
            raise ValueError(f"Unhandled transaction type: {transaction.transaction_type}")

    update_account_balance(db, account.id, new_balance)


def get_transaction_stats(db: Session, user_id: int, filters: Optional[TransactionFilter] = None) -> TransactionStats:
    """Get aggregate transaction statistics for a user, using the same filter logic as read_db_transactions.
    Accounts for refund/offset/reversal relationships by adjusting effective amounts."""

    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)

    if filters:
        query = _apply_transaction_filters(query, filters)

    transactions = query.all()

    txn_ids = [t.db_id for t in transactions]
    adjustments, absorbed_ids = get_refund_adjustments(db, user_id, txn_ids)

    total_count = 0
    total_income = Decimal('0.00')
    total_expenses = Decimal('0.00')

    for transaction in transactions:
        if transaction.db_id in absorbed_ids:
            continue
        total_count += 1
        adj = adjustments.get(transaction.db_id, Decimal('0.00'))
        effective = max(transaction.amount - adj, Decimal('0.00'))

        if transaction.transaction_type in (TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.INTEREST):
            total_income += effective
        elif transaction.transaction_type in (TransactionType.PURCHASE, TransactionType.WITHDRAWAL, TransactionType.FEE):
            total_expenses += effective
        # TRANSFER_IN, TRANSFER_OUT: not counted as income or expense

    # If category filter is active, adjust split transaction amounts
    if filters and filters.category_ids:
        split_txn_ids = [t.db_id for t in transactions if t.category_id is None and t.db_id not in absorbed_ids]
        if split_txn_ids:
            alloc_amounts = (
                db.query(
                    TransactionSplitAllocationDB.transaction_id,
                    func.sum(TransactionSplitAllocationDB.amount),
                )
                .filter(
                    TransactionSplitAllocationDB.transaction_id.in_(split_txn_ids),
                    TransactionSplitAllocationDB.category_id.in_(filters.category_ids),
                )
                .group_by(TransactionSplitAllocationDB.transaction_id)
                .all()
            )
            alloc_map = {txn_id: amount for txn_id, amount in alloc_amounts}

            for txn in transactions:
                if txn.db_id in alloc_map and txn.db_id not in absorbed_ids:
                    adj = adjustments.get(txn.db_id, Decimal('0.00'))
                    full_amount = max(txn.amount - adj, Decimal('0.00'))
                    alloc_amount = alloc_map[txn.db_id]
                    if txn.db_id in adjustments and txn.amount:
                        ratio = 1 - adjustments[txn.db_id] / txn.amount
                        alloc_amount = max(alloc_amount * ratio, Decimal('0.00'))

                    if txn.transaction_type in (TransactionType.PURCHASE, TransactionType.WITHDRAWAL, TransactionType.FEE):
                        total_expenses -= full_amount
                        total_expenses += alloc_amount
                    elif txn.transaction_type in (TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.INTEREST):
                        total_income -= full_amount
                        total_income += alloc_amount

    return TransactionStats(
        total_count=total_count,
        total_income=total_income,
        total_expenses=total_expenses,
        net=total_income - total_expenses,
    )


INCOME_TYPES = (TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.INTEREST)
EXPENSE_TYPES = (TransactionType.PURCHASE, TransactionType.WITHDRAWAL, TransactionType.FEE)


def get_monthly_averages(db: Session, user_id: int, year: int, month: Optional[int] = None, account_ids: Optional[List[int]] = None) -> MonthlyAverageResponse:
    """Compute monthly average income/expenses broken down by category for a calendar year or a single month."""
    from calendar import monthrange

    if month:
        last_day = monthrange(year, month)[1]
        filters = TransactionFilter(
            date_from=date(year, month, 1),
            date_to=date(year, month, last_day),
        )
    else:
        filters = TransactionFilter(
            date_from=date(year, 1, 1),
            date_to=date(year, 12, 31),
        )
    if account_ids:
        filters.account_ids = account_ids

    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)
    query = _apply_transaction_filters(query, filters)
    transactions = query.all()

    txn_ids = [t.db_id for t in transactions]
    adjustments, absorbed_ids = get_refund_adjustments(db, user_id, txn_ids) if txn_ids else ({}, set())

    # Collect all category/subcategory IDs we need to look up
    cat_ids_needed: Set[int] = set()
    for txn in transactions:
        if txn.category_id:
            cat_ids_needed.add(txn.category_id)
        if txn.subcategory_id:
            cat_ids_needed.add(txn.subcategory_id)

    # Also gather from split allocations
    split_txn_ids = [t.db_id for t in transactions if t.category_id is None and t.db_id not in absorbed_ids]
    split_allocs = []
    if split_txn_ids:
        split_allocs = (
            db.query(TransactionSplitAllocationDB)
            .filter(TransactionSplitAllocationDB.transaction_id.in_(split_txn_ids))
            .all()
        )
        for alloc in split_allocs:
            cat_ids_needed.add(alloc.category_id)
            if alloc.subcategory_id:
                cat_ids_needed.add(alloc.subcategory_id)

    # Batch-load all categories
    cat_map: Dict[int, CategoryDB] = {}
    if cat_ids_needed:
        cats = db.query(CategoryDB).filter(CategoryDB.id.in_(cat_ids_needed)).all()
        cat_map = {c.id: c for c in cats}

    # Build per-split-txn lookup: {txn_db_id: [alloc, ...]}
    split_map: Dict[int, list] = defaultdict(list)
    for alloc in split_allocs:
        split_map[alloc.transaction_id].append(alloc)

    # Aggregate by month and by (parent_cat_id, subcat_id)
    months_seen: Set[str] = set()
    month_totals: Dict[str, Dict[str, Decimal]] = defaultdict(lambda: {"income": Decimal("0"), "expenses": Decimal("0")})
    # cat_totals: {parent_cat_id: {"total": Decimal, "subs": {subcat_id: Decimal}}}
    cat_totals: Dict[int, Dict[str, Any]] = defaultdict(lambda: {"total": Decimal("0"), "subs": defaultdict(Decimal)})

    for txn in transactions:
        if txn.db_id in absorbed_ids:
            continue

        adj = adjustments.get(txn.db_id, Decimal("0"))
        effective = max(txn.amount - adj, Decimal("0"))
        month_key = txn.transaction_date.strftime("%Y-%m")
        months_seen.add(month_key)

        # Accumulate month-level income/expenses
        if txn.transaction_type in INCOME_TYPES:
            month_totals[month_key]["income"] += effective
        elif txn.transaction_type in EXPENSE_TYPES:
            month_totals[month_key]["expenses"] += effective

        # Category breakdown (expenses only — income categories are less meaningful)
        if txn.transaction_type not in EXPENSE_TYPES:
            continue

        if txn.db_id in split_map:
            # Split transaction: attribute to each allocation's category
            alloc_total = sum(a.amount for a in split_map[txn.db_id])
            for alloc in split_map[txn.db_id]:
                ratio = alloc.amount / alloc_total if alloc_total else Decimal("0")
                alloc_effective = effective * ratio

                parent_cat_id = alloc.category_id
                cat_totals[parent_cat_id]["total"] += alloc_effective
                if alloc.subcategory_id:
                    cat_totals[parent_cat_id]["subs"][alloc.subcategory_id] += alloc_effective
        elif txn.category_id:
            cat_obj = cat_map.get(txn.category_id)
            if cat_obj and cat_obj.parent_category_id:
                # This is a subcategory used as primary — roll up to parent
                parent_id = cat_obj.parent_category_id
                cat_totals[parent_id]["total"] += effective
                cat_totals[parent_id]["subs"][txn.category_id] += effective
            else:
                cat_totals[txn.category_id]["total"] += effective

            if txn.subcategory_id:
                # Subcategory on the transaction itself
                cat_totals[txn.category_id]["subs"][txn.subcategory_id] += effective

    months_with_data = len(months_seen)
    divisor = Decimal(months_with_data) if months_with_data > 0 else Decimal("1")

    # Build totals
    total_income = sum((m["income"] for m in month_totals.values()), Decimal("0"))
    total_expenses = sum((m["expenses"] for m in month_totals.values()), Decimal("0"))

    totals = MonthlyAverageTotals(
        avg_monthly_income=(total_income / divisor).quantize(Decimal("0.01")),
        avg_monthly_expenses=(total_expenses / divisor).quantize(Decimal("0.01")),
        avg_monthly_net=((total_income - total_expenses) / divisor).quantize(Decimal("0.01")),
        total_income=total_income.quantize(Decimal("0.01")),
        total_expenses=total_expenses.quantize(Decimal("0.01")),
        total_net=(total_income - total_expenses).quantize(Decimal("0.01")),
    )

    # Build by_category
    by_category = []
    for parent_cat_id, data in sorted(cat_totals.items(), key=lambda x: x[1]["total"], reverse=True):
        cat_obj = cat_map.get(parent_cat_id)
        if not cat_obj:
            continue

        subcategories = []
        for sub_id, sub_total in sorted(data["subs"].items(), key=lambda x: x[1], reverse=True):
            sub_obj = cat_map.get(sub_id)
            if not sub_obj:
                continue
            subcategories.append(MonthlyAverageSubcategoryBreakdown(
                subcategory_uuid=sub_obj.uuid,
                subcategory_name=sub_obj.name,
                total=sub_total.quantize(Decimal("0.01")),
                monthly_average=(sub_total / divisor).quantize(Decimal("0.01")),
            ))

        by_category.append(MonthlyAverageCategoryBreakdown(
            category_uuid=cat_obj.uuid,
            category_name=cat_obj.name,
            total=data["total"].quantize(Decimal("0.01")),
            monthly_average=(data["total"] / divisor).quantize(Decimal("0.01")),
            subcategories=subcategories,
        ))

    # Build by_month (all 12 months, even if no data)
    by_month = []
    for m in range(1, 13):
        mk = f"{year}-{m:02d}"
        mt = month_totals.get(mk, {"income": Decimal("0"), "expenses": Decimal("0")})
        by_month.append(MonthlyAverageMonthBreakdown(
            month=mk,
            income=mt["income"].quantize(Decimal("0.01")),
            expenses=mt["expenses"].quantize(Decimal("0.01")),
            net=(mt["income"] - mt["expenses"]).quantize(Decimal("0.01")),
        ))

    return MonthlyAverageResponse(
        year=year,
        months_with_data=months_with_data,
        totals=totals,
        by_category=by_category,
        by_month=by_month,
    )


def get_transactions_count(db: Session, user_id: int, filters: Optional[TransactionFilter] = None) -> int:
    """Get count of transactions for pagination"""

    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)

    if filters:
        query = _apply_transaction_filters(query, filters)

    return query.count()


def get_transactions_by_category(db: Session, user_id: int, date_from: Optional[date] = None,
                                date_to: Optional[date] = None) -> Dict[str, Decimal]:
    """Get transaction totals grouped by category.
    Accounts for refund/offset/reversal relationships by adjusting effective amounts."""

    query = db.query(TransactionDB).filter(TransactionDB.user_id == user_id)

    if date_from:
        query = query.filter(TransactionDB.transaction_date >= date_from)
    if date_to:
        query = query.filter(TransactionDB.transaction_date <= date_to)

    transactions = query.options(joinedload(TransactionDB.category)).all()

    txn_ids = [t.db_id for t in transactions]
    adjustments, absorbed_ids = get_refund_adjustments(db, user_id, txn_ids)

    category_totals: Dict[str, Decimal] = {}
    for transaction in transactions:
        if transaction.db_id in absorbed_ids:
            continue
        category_name = transaction.category.name if transaction.category else 'Uncategorized'
        adj = adjustments.get(transaction.db_id, Decimal('0.00'))
        effective = max(transaction.amount - adj, Decimal('0.00'))
        category_totals[category_name] = category_totals.get(category_name, Decimal('0.00')) + effective

    # Split allocations: distribute amounts across categories
    split_allocs = (
        db.query(TransactionSplitAllocationDB, TransactionDB.db_id, TransactionDB.amount.label("txn_amount"))
        .join(TransactionDB, TransactionSplitAllocationDB.transaction_id == TransactionDB.db_id)
        .filter(TransactionDB.user_id == user_id)
        .options(joinedload(TransactionSplitAllocationDB.category))
    )
    if date_from:
        split_allocs = split_allocs.filter(TransactionDB.transaction_date >= date_from)
    if date_to:
        split_allocs = split_allocs.filter(TransactionDB.transaction_date <= date_to)
    split_allocs = split_allocs.all()

    if split_allocs:
        split_txn_ids = list(set(sa[1] for sa in split_allocs))
        split_adjustments, split_absorbed = get_refund_adjustments(db, user_id, split_txn_ids)

        for alloc, txn_db_id, txn_amount in split_allocs:
            if txn_db_id in split_absorbed:
                continue
            cat_name = alloc.category.name if alloc.category else 'Uncategorized'
            adj = split_adjustments.get(txn_db_id, Decimal('0.00'))
            if adj and txn_amount:
                ratio = 1 - adj / txn_amount
                effective = max(alloc.amount * ratio, Decimal('0.00'))
            else:
                effective = alloc.amount
            category_totals[cat_name] = category_totals.get(cat_name, Decimal('0.00')) + effective

    return category_totals


# ===== SPLIT CATEGORY ALLOCATION =====

def set_transaction_splits(db: Session, user_id: int, transaction_uuid: UUID,
                           split_request: TransactionSplitRequest) -> TransactionDB:
    txn = read_db_transaction_by_uuid(db, transaction_uuid, user_id)
    if not txn:
        raise NotFoundError("Transaction not found")

    # Mutual exclusion: splits and amortization cannot coexist
    has_amortization = db.query(TransactionAmortizationScheduleDB).filter(
        TransactionAmortizationScheduleDB.transaction_id == txn.db_id
    ).first() is not None
    if has_amortization:
        raise ValueError("Cannot split a transaction that has an amortization schedule. Remove amortization first.")

    # Validate sum equals transaction amount
    total = sum(a.amount for a in split_request.allocations)
    if total != txn.amount:
        raise ValueError(
            f"Allocation sum ({total}) must equal transaction amount ({txn.amount})"
        )

    # Resolve category UUIDs -> IDs
    all_uuids = set()
    for alloc in split_request.allocations:
        all_uuids.add(alloc.category_uuid)
        if alloc.subcategory_uuid:
            all_uuids.add(alloc.subcategory_uuid)

    categories = read_db_categories_by_uuids(db, list(all_uuids))
    uuid_to_id = {c.uuid: c.id for c in categories}

    missing = all_uuids - set(uuid_to_id.keys())
    if missing:
        raise NotFoundError(f"Categories not found: {missing}")

    # Delete existing allocations
    db.query(TransactionSplitAllocationDB).filter(
        TransactionSplitAllocationDB.transaction_id == txn.db_id
    ).delete()

    # Create new allocations
    for alloc in split_request.allocations:
        db_alloc = TransactionSplitAllocationDB(
            id=uuid4(),
            transaction_id=txn.db_id,
            category_id=uuid_to_id[alloc.category_uuid],
            subcategory_id=uuid_to_id.get(alloc.subcategory_uuid),
            amount=alloc.amount,
        )
        db.add(db_alloc)

    # Clear single-category assignment
    txn.category_id = None
    txn.subcategory_id = None

    db.commit()
    return read_db_transaction_by_uuid(db, transaction_uuid, user_id)


def delete_transaction_splits(db: Session, user_id: int,
                              transaction_uuid: UUID) -> bool:
    txn = read_db_transaction_by_uuid(db, transaction_uuid, user_id)
    if not txn:
        raise NotFoundError("Transaction not found")
    db.query(TransactionSplitAllocationDB).filter(
        TransactionSplitAllocationDB.transaction_id == txn.db_id
    ).delete()
    db.commit()
    return True


def get_transaction_splits(db: Session, user_id: int,
                           transaction_uuid: UUID) -> list:
    txn = read_db_transaction_by_uuid(db, transaction_uuid, user_id)
    if not txn:
        raise NotFoundError("Transaction not found")
    return (
        db.query(TransactionSplitAllocationDB)
        .filter(TransactionSplitAllocationDB.transaction_id == txn.db_id)
        .options(
            joinedload(TransactionSplitAllocationDB.category),
            joinedload(TransactionSplitAllocationDB.subcategory),
        )
        .all()
    )


def bulk_create_transactions_from_parsed_data(
    db: Session,
    user_id: int,
    transactions: List[ParsedTransaction],
    institution_name: str,
    account_id: Optional[int],
    skip_duplicates: bool = True,
) -> Tuple[List[TransactionDB], List[Dict]]:
    """
    Bulk import transactions from a parsed file, with an optional account_id.

    Args:
        db: Database session
        user_id: User ID
        transactions: List of parsed transactions
        institution_name: Institution name for hashing
        account_id: Optional account ID to associate transactions with
        skip_duplicates: If True, skip duplicate transactions (default: True)

    Returns:
        Tuple of (created_transactions, skipped_duplicates)
        skipped_duplicates is a list of dicts containing:
            - parsed_transaction: ParsedTransaction object
            - existing_transaction: TransactionDB object (the duplicate in DB)
            - transaction_hash: str
    """
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
    skipped_duplicates = []
    duplicate_count = 0

    # Pre-fetch all existing transaction hashes for this user to avoid flagging within-statement duplicates
    existing_hashes_dict = {
        t.transaction_hash: t for t in
        db.query(TransactionDB)
        .filter(TransactionDB.user_id == user_id)
        .all()
    }

    for t_data in transactions:
        try:
            # Assumes parser provides the name of the enum member (case-insensitive)
            transaction_type_enum = TransactionType[t_data.transaction_type.upper()]
        except KeyError:
            logger.warning(f"Skipping transaction with unknown type: {t_data.transaction_type}")
            continue

        # Build hash directly (avoids constructing TransactionCreate with UUID requirement)
        txn_type_value = transaction_type_enum.value
        hash_string = (
            f"{user_id}|"
            f"{institution_name.lower()}|"
            f"{t_data.transaction_date}|"
            f"{txn_type_value}|"
            f"{t_data.amount}|"
            f"{t_data.description or ''}"
        )
        transaction_hash = hashlib.sha256(hash_string.encode()).hexdigest()

        # Check if transaction hash existed in database BEFORE this upload
        is_duplicate = transaction_hash in existing_hashes_dict

        # If duplicate and skip_duplicates=True, add to skipped list instead of creating
        if is_duplicate and skip_duplicates:
            existing_transaction = existing_hashes_dict[transaction_hash]
            skipped_duplicates.append({
                'parsed_transaction': t_data,
                'existing_transaction': existing_transaction,
                'transaction_hash': transaction_hash
            })
            duplicate_count += 1
            logger.debug(f"Skipping duplicate transaction: {t_data.transaction_date} - {t_data.description}")
            continue  # Skip creation

        if is_duplicate and not skip_duplicates:
            duplicate_count += 1
            logger.debug(f"Found duplicate transaction in database (will create anyway): {t_data.transaction_date} - {t_data.description}")

        db_transaction = TransactionDB(
            id=uuid4(),
            user_id=user_id,
            account_id=account_id,
            transaction_hash=transaction_hash,
            transaction_date=t_data.transaction_date,
            amount=abs(t_data.amount),
            transaction_type=transaction_type_enum,
            description=t_data.description,
            parsed_description=parse_transaction_description(t_data.description or ""),
            source_type=SourceType.PDF,
        )

        db.add(db_transaction)
        created_transactions.append(db_transaction)

    if duplicate_count > 0:
        if skip_duplicates:
            logger.info(f"Skipped {duplicate_count} duplicate transactions")
        else:
            logger.info(f"Flagged {duplicate_count} duplicate transactions for review")

    if not created_transactions:
        return [], skipped_duplicates

    try:
        db.commit()
        for t in created_transactions:
            db.refresh(t)
        if account:
            for t in created_transactions:
                update_account_balance_from_transaction(db, account, t)
        return created_transactions, skipped_duplicates
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


# ===== UUID-BASED OPERATIONS FOR API ENDPOINTS =====

def update_db_transaction_by_uuid(db: Session, transaction_uuid: UUID, user_id: int,
                                  transaction_updates: TransactionUpdate, *,
                                  account_id: Optional[int] = None,
                                  clear_account: bool = False,
                                  category_id: Optional[int] = None,
                                  subcategory_id: Optional[int] = None,
                                  clear_category: bool = False,
                                  clear_subcategory: bool = False) -> TransactionDB:
    """Update an existing transaction by UUID (for API endpoints)"""

    # Get the existing transaction by UUID
    db_transaction = db.query(TransactionDB).filter(
        TransactionDB.id == transaction_uuid,
        TransactionDB.user_id == user_id
    ).first()

    if not db_transaction:
        raise NotFoundError(f"Transaction with UUID {transaction_uuid} not found")

    # Use the existing update logic by calling update_db_transaction with db_id
    return update_db_transaction(db, db_transaction.db_id, user_id, transaction_updates,
                                account_id=account_id, clear_account=clear_account,
                                category_id=category_id, subcategory_id=subcategory_id,
                                clear_category=clear_category, clear_subcategory=clear_subcategory)


def delete_db_transaction_by_uuid(db: Session, transaction_uuid: UUID, user_id: int) -> bool:
    """Delete a transaction by UUID (for API endpoints)"""

    db_transaction = db.query(TransactionDB).filter(
        TransactionDB.id == transaction_uuid,
        TransactionDB.user_id == user_id
    ).first()

    if not db_transaction:
        raise NotFoundError(f"Transaction with UUID {transaction_uuid} not found")

    # Use the existing delete logic by calling delete_db_transaction with db_id
    return delete_db_transaction(db, db_transaction.db_id, user_id)


def read_transaction_relationships_by_uuid(
    db: Session, user_id: int, transaction_uuid: UUID
) -> List[TransactionRelationshipDB]:
    """Read all relationships for a transaction by UUID"""
    transaction = db.query(TransactionDB).filter(
        TransactionDB.id == transaction_uuid,
        TransactionDB.user_id == user_id
    ).first()
    if not transaction:
        raise NotFoundError(f"Transaction with UUID {transaction_uuid} not found")

    return db.query(TransactionRelationshipDB).filter(
        or_(
            TransactionRelationshipDB.from_transaction_id == transaction.db_id,
            TransactionRelationshipDB.to_transaction_id == transaction.db_id,
        )
    ).options(
        joinedload(TransactionRelationshipDB.from_transaction),
        joinedload(TransactionRelationshipDB.to_transaction),
    ).all()


# ===== UUID-BASED RELATIONSHIP OPERATIONS =====

def create_transaction_relationship_by_uuid(db: Session, user_id: int, from_transaction_uuid: UUID,
                                            relationship_data: 'TransactionRelationshipCreateByUUID') -> TransactionRelationshipDB:
    """Create a relationship between two transactions using UUIDs"""
    from_transaction = db.query(TransactionDB).filter(
        TransactionDB.id == from_transaction_uuid,
        TransactionDB.user_id == user_id
    ).first()
    if not from_transaction:
        raise NotFoundError(f"Transaction not found")

    to_transaction = db.query(TransactionDB).filter(
        TransactionDB.id == relationship_data.to_transaction_uuid,
        TransactionDB.user_id == user_id
    ).first()
    if not to_transaction:
        raise NotFoundError(f"Target transaction not found")

    # Validate refund allocation doesn't exceed original transaction amount
    if (relationship_data.relationship_type in ABSORBING_RELATIONSHIP_TYPES
            and relationship_data.amount_allocated is not None):
        validate_refund_allocation(db, to_transaction.db_id, relationship_data.amount_allocated)

    db_relationship = TransactionRelationshipDB(
        id=uuid4(),
        from_transaction_id=from_transaction.db_id,
        to_transaction_id=to_transaction.db_id,
        relationship_type=relationship_data.relationship_type,
        amount_allocated=relationship_data.amount_allocated,
        notes=relationship_data.notes
    )

    try:
        db.add(db_relationship)
        db.commit()
        # Re-query with joinedloads for response serialization
        db_relationship = db.query(TransactionRelationshipDB).options(
            joinedload(TransactionRelationshipDB.from_transaction),
            joinedload(TransactionRelationshipDB.to_transaction),
        ).filter(TransactionRelationshipDB.id == db_relationship.id).first()
        return db_relationship
    except IntegrityError:
        db.rollback()
        raise ValueError("Relationship creation failed due to database constraint")


def update_transaction_relationship_by_uuid(db: Session, user_id: int, relationship_uuid: UUID,
                                            relationship_updates: dict) -> TransactionRelationshipDB:
    """Update a transaction relationship by UUID"""
    db_relationship = db.query(TransactionRelationshipDB).filter(
        TransactionRelationshipDB.id == relationship_uuid
    ).first()

    if not db_relationship:
        raise NotFoundError(f"Relationship not found")

    # Verify user owns the from_transaction
    from_transaction = db.query(TransactionDB).filter(
        TransactionDB.db_id == db_relationship.from_transaction_id,
        TransactionDB.user_id == user_id
    ).first()
    if not from_transaction:
        raise NotFoundError(f"Transaction relationship not found or doesn't belong to user")

    # Validate refund allocation on update
    new_amount = relationship_updates.get('amount_allocated', db_relationship.amount_allocated)
    new_type = relationship_updates.get('relationship_type', db_relationship.relationship_type)
    if new_type in ABSORBING_RELATIONSHIP_TYPES and new_amount is not None:
        validate_refund_allocation(
            db, db_relationship.to_transaction_id, new_amount,
            exclude_relationship_id=db_relationship.relationship_id,
        )

    for field, value in relationship_updates.items():
        if hasattr(db_relationship, field):
            setattr(db_relationship, field, value)

    try:
        db.commit()
        # Re-query with joinedloads for response serialization
        db_relationship = db.query(TransactionRelationshipDB).options(
            joinedload(TransactionRelationshipDB.from_transaction),
            joinedload(TransactionRelationshipDB.to_transaction),
        ).filter(TransactionRelationshipDB.id == db_relationship.id).first()
        return db_relationship
    except IntegrityError:
        db.rollback()
        raise ValueError("Relationship update failed due to database constraint")


def delete_transaction_relationship_by_uuid(db: Session, user_id: int, relationship_uuid: UUID) -> bool:
    """Delete a transaction relationship by UUID"""
    db_relationship = db.query(TransactionRelationshipDB).filter(
        TransactionRelationshipDB.id == relationship_uuid
    ).first()

    if not db_relationship:
        raise NotFoundError(f"Relationship not found")

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


# ===== AMORTIZATION SCHEDULE =====

def create_or_replace_amortization_schedule(
    db: Session, user_id: int, transaction_uuid: UUID,
    schedule_data: AmortizationScheduleCreate,
) -> AmortizationScheduleResponse:
    txn = read_db_transaction_by_uuid(db, transaction_uuid, user_id)
    if not txn:
        raise NotFoundError("Transaction not found")

    # Mutual exclusion: splits and amortization cannot coexist
    has_splits = db.query(TransactionSplitAllocationDB).filter(
        TransactionSplitAllocationDB.transaction_id == txn.db_id
    ).first() is not None
    if has_splits:
        raise ValueError("Cannot amortize a transaction that has split allocations. Remove splits first.")

    txn_amount = abs(txn.amount)

    # Build allocations list: [(month_date, amount)]
    alloc_pairs = []
    if schedule_data.allocations:
        for a in schedule_data.allocations:
            try:
                y, m = a.month.split("-")
                month_date = date(int(y), int(m), 1)
            except (ValueError, AttributeError):
                raise ValueError(f"Invalid month format: {a.month}. Expected YYYY-MM")
            alloc_pairs.append((month_date, a.amount))

        total = sum(a[1] for a in alloc_pairs)
        if total != txn_amount:
            raise ValueError(f"Allocation sum ({total}) must equal transaction amount ({txn_amount})")
    else:
        # Equal split
        y, m = schedule_data.start_month.split("-")
        start = date(int(y), int(m), 1)
        n = schedule_data.months
        per_month = (txn_amount * 100 // n) / 100  # floor to 2 decimals
        remainder = txn_amount - per_month * n

        for i in range(n):
            month_num = start.month + i
            year = start.year + (month_num - 1) // 12
            month = ((month_num - 1) % 12) + 1
            amt = per_month + (remainder if i == n - 1 else Decimal('0.00'))
            alloc_pairs.append((date(year, month, 1), amt))

    # Delete existing schedule rows
    db.query(TransactionAmortizationScheduleDB).filter(
        TransactionAmortizationScheduleDB.transaction_id == txn.db_id
    ).delete()

    # Insert new rows
    for month_date, amount in alloc_pairs:
        db.add(TransactionAmortizationScheduleDB(
            id=uuid4(),
            transaction_id=txn.db_id,
            month_date=month_date,
            amount=amount,
        ))

    db.commit()
    return _build_amortization_response(db, txn)


def read_amortization_schedule(
    db: Session, user_id: int, transaction_uuid: UUID,
) -> Optional[AmortizationScheduleResponse]:
    txn = read_db_transaction_by_uuid(db, transaction_uuid, user_id)
    if not txn:
        raise NotFoundError("Transaction not found")

    rows = (
        db.query(TransactionAmortizationScheduleDB)
        .filter(TransactionAmortizationScheduleDB.transaction_id == txn.db_id)
        .order_by(TransactionAmortizationScheduleDB.month_date)
        .all()
    )
    if not rows:
        return None
    return _build_amortization_response_from_rows(txn, rows)


def delete_amortization_schedule(
    db: Session, user_id: int, transaction_uuid: UUID,
) -> bool:
    txn = read_db_transaction_by_uuid(db, transaction_uuid, user_id)
    if not txn:
        raise NotFoundError("Transaction not found")
    db.query(TransactionAmortizationScheduleDB).filter(
        TransactionAmortizationScheduleDB.transaction_id == txn.db_id
    ).delete()
    db.commit()
    return True


def _build_amortization_response(db: Session, txn: TransactionDB) -> AmortizationScheduleResponse:
    rows = (
        db.query(TransactionAmortizationScheduleDB)
        .filter(TransactionAmortizationScheduleDB.transaction_id == txn.db_id)
        .order_by(TransactionAmortizationScheduleDB.month_date)
        .all()
    )
    return _build_amortization_response_from_rows(txn, rows)


def _build_amortization_response_from_rows(
    txn: TransactionDB, rows: list,
) -> AmortizationScheduleResponse:
    # Resolve category from parent transaction
    cat_uuid = txn.category.uuid if txn.category else None
    cat_name = txn.category.name if txn.category else None
    subcat_uuid = txn.subcategory.uuid if txn.subcategory else None
    subcat_name = txn.subcategory.name if txn.subcategory else None

    entries = []
    for r in rows:
        entries.append(AmortizationScheduleEntry(
            id=r.id,
            month=r.month_date.strftime("%Y-%m"),
            amount=r.amount,
            category_uuid=cat_uuid,
            category_name=cat_name,
            subcategory_uuid=subcat_uuid,
            subcategory_name=subcat_name,
        ))
    return AmortizationScheduleResponse(
        transaction_uuid=txn.id,
        total_amount=abs(txn.amount),
        num_months=len(entries),
        allocations=entries,
    )
