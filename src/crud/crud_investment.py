from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import desc
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal
import hashlib

# Import your database models
from src.db.core import (
    InvestmentHoldingDB, 
    InvestmentTransactionDB, 
    AccountDB, 
    UserDB, 
    NotFoundError, 
    InvestmentTransactionType
)
from src.models.investment import (
    InvestmentHoldingCreate,
    InvestmentHoldingUpdate,
    InvestmentTransactionCreate,
    InvestmentTransactionUpdate,
    InvestmentTransactionTypeEnum,
    InvestmentTransactionBulkCreate
)
from src.parser.models import ParsedInvestmentTransaction


# ===== DATABASE OPERATIONS - INVESTMENT HOLDINGS =====

def create_db_investment_holding(db: Session, user_id: int, holding_data: InvestmentHoldingCreate) -> InvestmentHoldingDB:
    account = db.query(AccountDB).filter(
        AccountDB.id == holding_data.account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {holding_data.account_id} not found for this user.")

    existing_holding = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == holding_data.account_id,
        InvestmentHoldingDB.symbol == holding_data.symbol
    ).first()
    if existing_holding:
        raise ValueError(f"Holding with symbol {holding_data.symbol} already exists in this account.")

    db_holding = InvestmentHoldingDB(
        **holding_data.model_dump()
    )
    
    try:
        db.add(db_holding)
        db.commit()
        db.refresh(db_holding)
        return db_holding
    except IntegrityError:
        db.rollback()
        raise ValueError("Holding creation failed due to database constraint.")

def read_db_investment_holding(db: Session, holding_id: int, user_id: int) -> Optional[InvestmentHoldingDB]:
    return db.query(InvestmentHoldingDB).join(AccountDB).filter(
        InvestmentHoldingDB.holding_id == holding_id,
        AccountDB.user_id == user_id
    ).first()

def read_db_investment_holdings_by_account(db: Session, account_id: int, user_id: int) -> List[InvestmentHoldingDB]:
    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError(f"Account with id {account_id} not found.")
    return db.query(InvestmentHoldingDB).filter(InvestmentHoldingDB.account_id == account_id).all()

def update_db_investment_holding(db: Session, holding_id: int, user_id: int, holding_updates: InvestmentHoldingUpdate) -> InvestmentHoldingDB:
    db_holding = read_db_investment_holding(db, holding_id, user_id)
    if not db_holding:
        raise NotFoundError(f"Holding with id {holding_id} not found.")

    update_data = holding_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_holding, field, value)
    
    db_holding.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_holding)
        return db_holding
    except IntegrityError:
        db.rollback()
        raise ValueError("Holding update failed.")

def delete_db_investment_holding(db: Session, holding_id: int, user_id: int) -> bool:
    db_holding = read_db_investment_holding(db, holding_id, user_id)
    if not db_holding:
        raise NotFoundError(f"Holding with id {holding_id} not found.")
    
    # Optional: Check if there are associated transactions before deleting
    
    try:
        db.delete(db_holding)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete holding: {str(e)}")


# ===== DATABASE OPERATIONS - INVESTMENT TRANSACTIONS =====

def create_db_investment_transaction(db: Session, user_id: int, transaction_data: InvestmentTransactionCreate) -> InvestmentTransactionDB:
    account = db.query(AccountDB).filter(
        AccountDB.id == transaction_data.account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {transaction_data.account_id} not found.")

    # Find or create the corresponding holding
    holding = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == transaction_data.account_id,
        InvestmentHoldingDB.symbol == transaction_data.symbol
    ).first()

    if not holding and transaction_data.transaction_type in [InvestmentTransactionTypeEnum.BUY, InvestmentTransactionTypeEnum.REINVESTMENT]:
        holding_create = InvestmentHoldingCreate(
            account_id=transaction_data.account_id,
            symbol=transaction_data.symbol,
            quantity=Decimal('0'), # Will be updated by the transaction
            average_cost_basis=Decimal('0')
        )
        holding = create_db_investment_holding(db, user_id, holding_create)

    db_transaction = InvestmentTransactionDB(
        **transaction_data.model_dump(exclude={'transaction_type'}),
        transaction_type=InvestmentTransactionType(transaction_data.transaction_type.value),
        holding_id=holding.holding_id if holding else None
    )

    try:
        db.add(db_transaction)
        db.commit()
        db.refresh(db_transaction)
        
        # Update holding based on transaction
        if holding:
            update_holding_from_transaction(db, holding, db_transaction)

        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Investment transaction creation failed.")

def generate_investment_transaction_hash(transaction_data: ParsedInvestmentTransaction, user_id: int, institution_name: str) -> str:
    """Generate a hash for investment transaction deduplication."""
    hash_string = (
        f"{user_id}|"
        f"{institution_name.lower()}|"
        f"{transaction_data.transaction_date}|"
        f"{transaction_data.transaction_type}|"
        f"{transaction_data.symbol}|"
        f"{transaction_data.quantity}|"
        f"{transaction_data.price_per_share}|"
        f"{transaction_data.total_amount}"
    )
    return hashlib.sha256(hash_string.encode()).hexdigest()


def bulk_create_investment_transactions_from_parsed_data(
    db: Session,
    user_id: int,
    transactions: List[ParsedInvestmentTransaction],
    institution_name: str,
    account_id: Optional[int]
) -> List[InvestmentTransactionDB]:
    """Bulk import investment transactions from a parsed file, with an optional account_id."""
    account = None
    if account_id:
        account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
        if not account:
            raise NotFoundError(f"Account with id {account_id} not found for this user.")

    created_transactions = []
    for t_data in transactions:
        transaction_hash = generate_investment_transaction_hash(t_data, user_id, institution_name)

        existing = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.user_id == user_id,
            InvestmentTransactionDB.transaction_hash == transaction_hash
        ).first()
        if existing:
            continue

        # Find or create the corresponding holding
        holding = None
        if account_id:
            holding = db.query(InvestmentHoldingDB).filter(
                InvestmentHoldingDB.account_id == account_id,
                InvestmentHoldingDB.symbol == t_data.symbol
            ).first()

            if not holding and t_data.transaction_type in [InvestmentTransactionTypeEnum.BUY, InvestmentTransactionTypeEnum.REINVESTMENT]:
                holding_create = InvestmentHoldingCreate(
                    account_id=account_id,
                    symbol=t_data.symbol,
                    quantity=Decimal('0'),
                    average_cost_basis=Decimal('0')
                )
                holding = create_db_investment_holding(db, user_id, holding_create)

        db_transaction = InvestmentTransactionDB(
            user_id=user_id,
            account_id=account_id,
            holding_id=holding.holding_id if holding else None,
            transaction_hash=transaction_hash,
            transaction_date=t_data.transaction_date,
            transaction_type=InvestmentTransactionType(t_data.transaction_type.value),
            symbol=t_data.symbol,
            quantity=t_data.quantity,
            price_per_share=t_data.price_per_share,
            amount=t_data.amount,
            fees=t_data.fees,
            description=t_data.description,
            needs_review=True if not account_id else False,
        )
        db.add(db_transaction)
        created_transactions.append(db_transaction)

    if not created_transactions:
        return []

    try:
        db.commit()
        for t in created_transactions:
            db.refresh(t)
            if t.holding_id:
                holding = db.query(InvestmentHoldingDB).get(t.holding_id)
                if holding:
                    update_holding_from_transaction(db, holding, t)
        return created_transactions
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk investment transaction import failed: {str(e)}")


def bulk_create_investment_transactions(db: Session, user_id: int, bulk_data: InvestmentTransactionBulkCreate) -> List[InvestmentTransactionDB]:
    db_transactions = []
    for transaction_data in bulk_data.transactions:
        db_transactions.append(create_db_investment_transaction(db, user_id, transaction_data))
    return db_transactions

def read_db_investment_transaction(db: Session, transaction_id: int, user_id: int) -> Optional[InvestmentTransactionDB]:
    return db.query(InvestmentTransactionDB).join(AccountDB).filter(
        InvestmentTransactionDB.investment_transaction_id == transaction_id,
        AccountDB.user_id == user_id
    ).first()

def read_db_investment_transactions(db: Session, user_id: int, account_id: Optional[int] = None, skip: int = 0, limit: int = 100) -> List[InvestmentTransactionDB]:
    query = db.query(InvestmentTransactionDB).join(AccountDB).filter(AccountDB.user_id == user_id)
    if account_id:
        query = query.filter(InvestmentTransactionDB.account_id == account_id)
    
    return query.order_by(desc(InvestmentTransactionDB.transaction_date)).offset(skip).limit(limit).all()

def update_db_investment_transaction(db: Session, transaction_id: int, user_id: int, transaction_updates: InvestmentTransactionUpdate) -> InvestmentTransactionDB:
    db_transaction = read_db_investment_transaction(db, transaction_id, user_id)
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")

    update_data = transaction_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        if field == 'transaction_type' and value:
            setattr(db_transaction, field, InvestmentTransactionType(value.value))
        else:
            setattr(db_transaction, field, value)
            
    db_transaction.updated_at = datetime.utcnow()

    try:
        db.commit()
        db.refresh(db_transaction)
        # Note: Re-calculating holding state on update is complex and omitted here for simplicity.
        # A full implementation would require reversing the old transaction and applying the new one.
        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Transaction update failed.")

def bulk_update_db_investment_transactions(db: Session, user_id: int, transaction_ids: List[int], updates: Dict[str, Any]) -> int:
    """
    Bulk update multiple investment transactions for a user.
    """
    if not transaction_ids:
        return 0

    # First, get the account_ids for the given transaction_ids to verify ownership
    transactions_query = db.query(InvestmentTransactionDB.investment_transaction_id, AccountDB.user_id).join(AccountDB).filter(
        InvestmentTransactionDB.investment_transaction_id.in_(transaction_ids)
    ).all()

    if len(transactions_query) != len(set(transaction_ids)):
        found_ids = {t[0] for t in transactions_query}
        missing_ids = set(transaction_ids) - found_ids
        raise NotFoundError(f"Investment transactions with the following IDs not found: {missing_ids}")

    for t in transactions_query:
        if t.user_id != user_id:
            raise NotFoundError(f"Transaction with ID {t.investment_transaction_id} does not belong to the user.")

    # Perform the bulk update
    update_data = {**updates, "updated_at": datetime.utcnow()}
    
    try:
        updated_rows = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.investment_transaction_id.in_(transaction_ids)
        ).update(update_data, synchronize_session=False)
        
        db.commit()
        return updated_rows
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to bulk update investment transactions: {str(e)}")

def delete_db_investment_transaction(db: Session, transaction_id: int, user_id: int) -> bool:
    db_transaction = read_db_investment_transaction(db, transaction_id, user_id)
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")
    
    try:
        db.delete(db_transaction)
        db.commit()
        # Note: Re-calculating holding state on delete is also complex.
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete transaction: {str(e)}")


# ===== UTILITY FUNCTIONS =====

def update_holding_from_transaction(db: Session, holding: InvestmentHoldingDB, transaction: InvestmentTransactionDB):
    """Updates a holding's quantity and cost basis after a transaction."""
    if transaction.transaction_type in [InvestmentTransactionType.BUY, InvestmentTransactionType.REINVESTMENT]:
        if transaction.quantity is not None and transaction.price_per_share is not None:
            new_quantity = holding.quantity + transaction.quantity
            if new_quantity > 0:
                old_total_cost = holding.quantity * (holding.average_cost_basis or 0)
                new_total_cost = transaction.quantity * transaction.price_per_share
                holding.average_cost_basis = (old_total_cost + new_total_cost) / new_quantity
            holding.quantity = new_quantity

    elif transaction.transaction_type == InvestmentTransactionType.SELL:
        if transaction.quantity is not None:
            holding.quantity -= transaction.quantity
            # Cost basis does not change on sell
    
    # Other transaction types like SPLIT would require more specific logic
    
    holding.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(holding)
    except Exception:
        db.rollback()
        raise
