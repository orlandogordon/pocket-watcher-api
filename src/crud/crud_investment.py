from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import desc
from typing import Optional, List, Dict, Any
from datetime import datetime
from decimal import Decimal
import hashlib

from src.logging_config import get_logger

logger = get_logger(__name__)

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

        # NOTE: Account balance will be updated by end-of-day snapshot job
        # which fetches live market prices and calculates true portfolio value

        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Investment transaction creation failed.")

def map_transaction_type_to_enum(transaction_type_str: str) -> Optional[InvestmentTransactionType]:
    """
    Map various transaction type strings from different institutions to the InvestmentTransactionType enum.
    Returns None if no mapping is found.
    """
    # Normalize the string
    normalized = transaction_type_str.upper().strip().replace(" ", "_")

    # Direct mapping attempts
    mapping = {
        # Direct matches
        "BUY": InvestmentTransactionType.BUY,
        "SELL": InvestmentTransactionType.SELL,
        "DIVIDEND": InvestmentTransactionType.DIVIDEND,
        "INTEREST": InvestmentTransactionType.INTEREST,
        "SPLIT": InvestmentTransactionType.SPLIT,
        "MERGER": InvestmentTransactionType.MERGER,
        "SPINOFF": InvestmentTransactionType.SPINOFF,
        "REINVESTMENT": InvestmentTransactionType.REINVESTMENT,

        # Schwab-specific mappings
        "BUY_TO_OPEN": InvestmentTransactionType.BUY,
        "BUY_TO_CLOSE": InvestmentTransactionType.BUY,
        "SELL_TO_OPEN": InvestmentTransactionType.SELL,
        "SELL_TO_CLOSE": InvestmentTransactionType.SELL,
        "CREDIT_INTEREST": InvestmentTransactionType.INTEREST,
        "BOND_INTEREST": InvestmentTransactionType.INTEREST,

        # TD Ameritrade mappings
        "BOUGHT_TO_OPEN": InvestmentTransactionType.BUY,
        "SOLD_TO_CLOSE": InvestmentTransactionType.SELL,

        # Ameriprise mappings
        "PURCHASE": InvestmentTransactionType.BUY,
        "SALE": InvestmentTransactionType.SELL,
    }

    # Try direct lookup
    if normalized in mapping:
        return mapping[normalized]

    # Try partial matches
    if "BUY" in normalized or "PURCHASE" in normalized:
        return InvestmentTransactionType.BUY
    if "SELL" in normalized or "SALE" in normalized:
        return InvestmentTransactionType.SELL
    if "INTEREST" in normalized:
        return InvestmentTransactionType.INTEREST
    if "DIVIDEND" in normalized or "DIV" in normalized:
        return InvestmentTransactionType.DIVIDEND
    if "REINVEST" in normalized:
        return InvestmentTransactionType.REINVESTMENT

    # No mapping found
    return None

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
    duplicate_count = 0

    for t_data in transactions:
        # Generate transaction hash for deduplication
        transaction_hash = generate_investment_transaction_hash(t_data, user_id, institution_name)

        # Check if transaction already exists in database
        existing_transaction = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.user_id == user_id,
            InvestmentTransactionDB.transaction_hash == transaction_hash
        ).first()

        # Flag as needing review if duplicate found in database
        needs_review = existing_transaction is not None
        if needs_review:
            logger.debug(f"Found duplicate investment transaction in database (will flag for review): {t_data.transaction_date} - {t_data.description}")
            duplicate_count += 1

        # Map the transaction type string to the enum FIRST
        transaction_type_enum = map_transaction_type_to_enum(t_data.transaction_type)
        if not transaction_type_enum:
            # Log and skip transactions we can't map
            logger.warning(f"Could not map transaction type '{t_data.transaction_type}' to enum. Skipping transaction.")
            continue

        # Find or create the corresponding holding (only for transactions with symbols)
        holding = None
        if account_id and t_data.symbol:
            holding = db.query(InvestmentHoldingDB).filter(
                InvestmentHoldingDB.account_id == account_id,
                InvestmentHoldingDB.symbol == t_data.symbol
            ).first()

            # Create holding for BUY/REINVESTMENT transactions if it doesn't exist
            if not holding and transaction_type_enum in [InvestmentTransactionType.BUY, InvestmentTransactionType.REINVESTMENT]:
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
            transaction_date=t_data.transaction_date,
            transaction_type=transaction_type_enum,
            symbol=t_data.symbol if t_data.symbol else "UNKNOWN",  # symbol is required, use placeholder if missing
            quantity=t_data.quantity,
            price_per_share=t_data.price_per_share,
            total_amount=t_data.total_amount,
            fees=None,  # Not currently parsed
            description=t_data.description,
            transaction_hash=transaction_hash,
            needs_review=needs_review  # Flag if duplicate found in database
        )
        db.add(db_transaction)
        created_transactions.append(db_transaction)

    if duplicate_count > 0:
        logger.info(f"Flagged {duplicate_count} duplicate investment transactions for review")

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

        # NOTE: Account balance should be updated separately by a snapshot job
        # that fetches current market prices, not from historical statement imports

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

def calculate_account_total_value(db: Session, account_id: int) -> Decimal:
    """Calculate the total market value of all holdings in an investment account."""
    holdings = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == account_id
    ).all()

    total_value = Decimal('0.00')
    for holding in holdings:
        if holding.quantity and holding.current_price:
            total_value += holding.quantity * holding.current_price
        elif holding.quantity and holding.average_cost_basis:
            # Fallback to cost basis if current price not available
            total_value += holding.quantity * holding.average_cost_basis

    return total_value

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
