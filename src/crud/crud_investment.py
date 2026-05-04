from sqlalchemy.orm import Session, joinedload
from sqlalchemy.exc import IntegrityError
from sqlalchemy import case, desc
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from uuid import uuid4
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
    InvestmentTransactionType,
    SnapshotBackfillJobDB,
    AccountType
)
from src.services.job_runner import get_job_runner
from src.models.investment import (
    InvestmentTransactionCreate,
    InvestmentTransactionUpdate,
    InvestmentTransactionTypeEnum,
    InvestmentTransactionBulkCreate
)
from src.parser.models import ParsedInvestmentTransaction
from src.services.account_snapshot import parse_split_ratio
from src.services.price_fetcher import is_option_symbol, parse_option_symbol


# ===== DATABASE OPERATIONS - INVESTMENT HOLDINGS (READ-ONLY) =====

def read_db_investment_holding(db: Session, holding_id: int, user_id: int) -> Optional[InvestmentHoldingDB]:
    return db.query(InvestmentHoldingDB).join(AccountDB).options(
        joinedload(InvestmentHoldingDB.account),
    ).filter(
        InvestmentHoldingDB.holding_id == holding_id,
        AccountDB.user_id == user_id
    ).first()

def read_db_investment_holdings_by_account(db: Session, account_id: int, user_id: int) -> List[InvestmentHoldingDB]:
    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError(f"Account with id {account_id} not found.")
    return db.query(InvestmentHoldingDB).options(
        joinedload(InvestmentHoldingDB.account),
    ).filter(InvestmentHoldingDB.account_id == account_id).all()


# ===== UUID-BASED OPERATIONS - HOLDINGS (READ-ONLY) =====

def read_db_investment_holding_by_uuid(db: Session, holding_uuid: 'UUID', user_id: int) -> Optional[InvestmentHoldingDB]:
    return db.query(InvestmentHoldingDB).join(AccountDB).options(
        joinedload(InvestmentHoldingDB.account),
    ).filter(
        InvestmentHoldingDB.id == holding_uuid,
        AccountDB.user_id == user_id
    ).first()


# ===== UPDATE HOLDINGS =====

def update_db_investment_holding_by_uuid(db: Session, holding_uuid: 'UUID', user_id: int, updates) -> InvestmentHoldingDB:
    holding = read_db_investment_holding_by_uuid(db, holding_uuid, user_id)
    if not holding:
        raise NotFoundError(f"Holding with uuid {holding_uuid} not found.")
    update_data = updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(holding, field, value)
    holding.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(holding)
    return holding


# ===== REBUILD HOLDINGS FROM TRANSACTIONS =====

def _holding_key(txn) -> Optional[str]:
    """
    Derive the key used to group transactions into holdings.
    Options use api_symbol (OCC format) so they don't merge with stock holdings
    that share the same underlying ticker (e.g. QQQ ETF vs QQQ put option).
    Everything else uses the plain ticker symbol.
    """
    if txn.api_symbol and is_option_symbol(txn.api_symbol):
        return txn.api_symbol
    if not txn.api_symbol and txn.security_type == "OPTION":
        logger.warning(
            f"Option transaction {txn.id} has no OCC api_symbol, "
            f"will merge with stock holding for {txn.symbol}"
        )
    return txn.symbol


def rebuild_holdings_from_transactions(db: Session, account_id: int) -> List[InvestmentHoldingDB]:
    """
    Rebuild all holdings for an account by replaying investment transactions.
    Holdings are a materialized cache derived from transactions.
    Caller is responsible for committing.
    """
    # 1. Cache existing price data (fetched from Yahoo Finance, not derived from transactions)
    existing_holdings = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == account_id
    ).all()
    price_cache: Dict[str, Dict] = {}
    for h in existing_holdings:
        price_cache[h.symbol] = {
            'current_price': h.current_price,
            'last_price_update': h.last_price_update,
        }

    # 2. Null out holding_id on all transactions for this account
    db.query(InvestmentTransactionDB).filter(
        InvestmentTransactionDB.account_id == account_id
    ).update({InvestmentTransactionDB.holding_id: None}, synchronize_session='fetch')

    # 3. Delete all existing holdings for the account
    db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == account_id
    ).delete(synchronize_session='fetch')

    # 4. Replay transactions in chronological order
    # Within the same date, process BUY/REINVESTMENT before SELL/EXPIRATION
    # so that holdings exist before being reduced.
    type_priority = case(
        (InvestmentTransactionDB.transaction_type.in_([
            InvestmentTransactionType.BUY,
            InvestmentTransactionType.REINVESTMENT,
            InvestmentTransactionType.TRANSFER_IN,
            InvestmentTransactionType.TRANSFER_OUT,
        ]), 0),
        (InvestmentTransactionDB.transaction_type.in_([
            InvestmentTransactionType.SELL,
            InvestmentTransactionType.EXPIRATION,
        ]), 2),
        else_=1,
    )
    transactions = db.query(InvestmentTransactionDB).filter(
        InvestmentTransactionDB.account_id == account_id
    ).order_by(
        InvestmentTransactionDB.transaction_date.asc(),
        type_priority,
        InvestmentTransactionDB.investment_transaction_id.asc()
    ).all()

    # holding_key -> InvestmentHoldingDB
    # Key is OCC symbol for options (e.g. "QQQ240524P00454000"), plain ticker otherwise ("QQQ")
    holdings_map: Dict[str, InvestmentHoldingDB] = {}

    for txn in transactions:
        key = _holding_key(txn)
        txn_type = txn.transaction_type

        if txn_type in (InvestmentTransactionType.BUY, InvestmentTransactionType.REINVESTMENT):
            if not key:
                continue
            if key not in holdings_map:
                holding = InvestmentHoldingDB(
                    id=uuid4(),
                    account_id=account_id,
                    symbol=key,
                    quantity=Decimal('0'),
                    average_cost_basis=Decimal('0'),
                )
                db.add(holding)
                db.flush()
                holdings_map[key] = holding

            holding = holdings_map[key]
            qty = txn.quantity or Decimal('0')
            pps = txn.price_per_share or Decimal('0')
            new_quantity = holding.quantity + qty
            if new_quantity > 0:
                old_total_cost = holding.quantity * (holding.average_cost_basis or Decimal('0'))
                new_total_cost = qty * pps
                holding.average_cost_basis = (old_total_cost + new_total_cost) / new_quantity
            holding.quantity = new_quantity
            txn.holding_id = holding.holding_id

        elif txn_type == InvestmentTransactionType.SELL:
            if not key or key not in holdings_map:
                continue
            holding = holdings_map[key]
            qty = txn.quantity or Decimal('0')
            txn.cost_basis_at_sale = holding.average_cost_basis
            holding.quantity -= qty
            txn.holding_id = holding.holding_id

        elif txn_type == InvestmentTransactionType.EXPIRATION:
            if not key or key not in holdings_map:
                continue
            holding = holdings_map[key]
            txn.cost_basis_at_sale = holding.average_cost_basis
            holding.quantity = Decimal('0')
            txn.holding_id = holding.holding_id

        elif txn_type == InvestmentTransactionType.SPLIT:
            if not key or key not in holdings_map:
                continue
            holding = holdings_map[key]
            ratio = parse_split_ratio(txn.description or '')
            if ratio and ratio != Decimal('0'):
                holding.quantity = holding.quantity * ratio
                if holding.average_cost_basis and holding.average_cost_basis > 0:
                    holding.average_cost_basis = holding.average_cost_basis / ratio
            txn.holding_id = holding.holding_id

        else:
            # DIVIDEND, INTEREST, FEE, TRANSFER — link to holding if key matches
            if key and key in holdings_map:
                txn.holding_id = holdings_map[key].holding_id

    # 5. Derive option fields and security_type
    for key, holding in holdings_map.items():
        # For option holdings the key IS the OCC api_symbol; for stocks it's the ticker
        if is_option_symbol(key):
            holding.security_type = "OPTION"
            parsed = parse_option_symbol(key)
            if parsed:
                holding.underlying_symbol = parsed['underlying']
                holding.option_type = parsed['option_type']
                holding.strike_price = Decimal(str(parsed['strike']))
                exp = parsed['expiration']
                holding.expiration_date = date.fromisoformat(exp) if isinstance(exp, str) else exp
        else:
            # Use security_type from the most recent transaction for this holding
            typed_txn = next(
                (t for t in reversed(transactions) if _holding_key(t) == key and t.security_type),
                None
            )
            if typed_txn:
                holding.security_type = typed_txn.security_type
            else:
                holding.security_type = "STOCK"

    # 6. Restore price cache
    for symbol, holding in holdings_map.items():
        if symbol in price_cache:
            holding.current_price = price_cache[symbol]['current_price']
            holding.last_price_update = price_cache[symbol]['last_price_update']

    # 7. Remove holdings with quantity <= 0
    to_remove = [sym for sym, h in holdings_map.items() if h.quantity <= 0]
    for sym in to_remove:
        holding = holdings_map.pop(sym)
        # Null out holding_id on transactions that reference this holding
        for txn in transactions:
            if txn.holding_id == holding.holding_id:
                txn.holding_id = None
        db.delete(holding)

    # 8. Flush — caller commits
    db.flush()

    logger.info(f"Rebuilt {len(holdings_map)} holdings for account {account_id} from {len(transactions)} transactions")
    return list(holdings_map.values())


# ===== DATABASE OPERATIONS - INVESTMENT TRANSACTIONS =====

def _create_investment_transaction_no_rebuild(db: Session, user_id: int, transaction_data: InvestmentTransactionCreate, *,
                                               account_id: int) -> InvestmentTransactionDB:
    """Create an investment transaction without triggering a holdings rebuild.
    Used internally by both single-create and bulk-create paths."""
    account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {account_id} not found.")

    # Generate transaction hash for deduplication
    hash_string = (
        f"{user_id}|"
        f"{account_id}|"
        f"{transaction_data.transaction_date}|"
        f"{transaction_data.transaction_type.value}|"
        f"{transaction_data.symbol}|"
        f"{transaction_data.quantity}|"
        f"{transaction_data.price_per_share}|"
        f"{transaction_data.total_amount}"
    )
    transaction_hash = hashlib.sha256(hash_string.encode()).hexdigest()

    db_transaction = InvestmentTransactionDB(
        id=uuid4(),
        user_id=user_id,
        account_id=account_id,
        transaction_type=InvestmentTransactionType(transaction_data.transaction_type.value),
        symbol=transaction_data.symbol,
        quantity=abs(transaction_data.quantity) if transaction_data.quantity is not None else None,
        price_per_share=transaction_data.price_per_share,
        total_amount=abs(transaction_data.total_amount),
        fees=transaction_data.fees,
        transaction_date=transaction_data.transaction_date,
        description=transaction_data.description,
        api_symbol=transaction_data.api_symbol,
        security_type=transaction_data.security_type.value if hasattr(transaction_data, 'security_type') and transaction_data.security_type else None,
        holding_id=None,
        transaction_hash=transaction_hash,
    )

    db.add(db_transaction)
    return db_transaction


def _update_investment_account_balance(db: Session, account_id: int) -> None:
    """Recalculate and update the balance for an investment account after transaction changes.

    Balance = holdings market value + cash balance.
    Cash balance is derived from initial_cash_balance + replaying all investment transactions.
    initial_cash_balance must be set correctly on the account (via account creation or update).
    """
    from src.services.account_snapshot import get_account_state_on_date
    account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if account and account.account_type == AccountType.INVESTMENT:
        holdings_value = calculate_account_total_value(db, account_id)
        state = get_account_state_on_date(db, account_id, date.today())
        account.balance = holdings_value + state['cash_balance']


def create_db_investment_transaction(db: Session, user_id: int, transaction_data: InvestmentTransactionCreate, *,
                                      account_id: int) -> InvestmentTransactionDB:
    db_transaction = _create_investment_transaction_no_rebuild(db, user_id, transaction_data, account_id=account_id)

    try:
        db.commit()
        db.refresh(db_transaction)
        rebuild_holdings_from_transactions(db, account_id)
        _update_investment_account_balance(db, account_id)
        db.commit()
        return db_transaction
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Investment transaction creation failed: {e}")
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

        # Fee and transfer mappings
        "FEE": InvestmentTransactionType.FEE,
        "TRANSFER_IN": InvestmentTransactionType.TRANSFER_IN,
        "TRANSFER_OUT": InvestmentTransactionType.TRANSFER_OUT,
        "DEPOSIT": InvestmentTransactionType.TRANSFER_IN,
        "WITHDRAWAL": InvestmentTransactionType.TRANSFER_OUT,
        "WIRE": InvestmentTransactionType.TRANSFER_IN,
        "ACH": InvestmentTransactionType.TRANSFER_IN,
        "JOURNAL": InvestmentTransactionType.TRANSFER_IN,
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
    if "FEE" in normalized or "COMMISSION" in normalized:
        return InvestmentTransactionType.FEE
    if "WITHDRAWAL" in normalized:
        return InvestmentTransactionType.TRANSFER_OUT
    if any(w in normalized for w in ["TRANSFER_IN", "TRANSFER_OUT", "TRANSFER", "DEPOSIT", "WIRE", "ACH", "JOURNAL"]):
        return InvestmentTransactionType.TRANSFER_IN

    # No mapping found
    return None

def generate_investment_transaction_hash(transaction_data: ParsedInvestmentTransaction, user_id: int, institution_name: str, make_unique: bool = False) -> str:
    """Generate a hash for investment transaction deduplication.

    Args:
        make_unique: If True, append a UUID to guarantee a unique hash.
                     Used for approved duplicates in the preview flow.
    """
    hash_string = (
        f"{user_id}|"
        f"{institution_name.lower()}|"
        f"{transaction_data.transaction_date}|"
        f"{transaction_data.transaction_type}|"
        f"{transaction_data.symbol}|"
        f"{transaction_data.quantity}|"
        f"{transaction_data.price_per_share}|"
        f"{transaction_data.total_amount}|"
        f"{transaction_data.description}"
    )
    if make_unique:
        from uuid import uuid4
        hash_string += f"|{uuid4()}"
    return hashlib.sha256(hash_string.encode()).hexdigest()


def get_original_investment_transaction_for_duplicate(
    db: Session,
    user_id: int,
    transaction_hash: str
) -> Optional[InvestmentTransactionDB]:
    """
    Find the original investment transaction that matches this hash.
    Used when a duplicate is detected to show user which transaction it duplicates.
    Returns the oldest transaction with this hash.
    """
    return db.query(InvestmentTransactionDB).filter(
        InvestmentTransactionDB.user_id == user_id,
        InvestmentTransactionDB.transaction_hash == transaction_hash
    ).order_by(InvestmentTransactionDB.created_at.asc()).first()


def bulk_create_investment_transactions_from_parsed_data(
    db: Session,
    user_id: int,
    transactions: List[ParsedInvestmentTransaction],
    institution_name: str,
    account_id: Optional[int],
    skip_duplicates: bool = True,
) -> Tuple[List[InvestmentTransactionDB], List[Dict], Optional[int]]:
    """
    Bulk import investment transactions from a parsed file, with an optional account_id.

    Args:
        db: Database session
        user_id: User ID
        transactions: List of parsed investment transactions
        institution_name: Institution name for hashing
        account_id: Optional account ID to associate transactions with
        skip_duplicates: If True, skip duplicate transactions (default: True)

    Returns:
        Tuple of (created_transactions, skipped_duplicates, backfill_job_id or None)
        skipped_duplicates is a list of dicts containing:
            - parsed_transaction: ParsedInvestmentTransaction object
            - existing_transaction: InvestmentTransactionDB object (the duplicate in DB)
            - transaction_hash: str
    """
    account = None
    if account_id:
        account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
        if not account:
            raise NotFoundError(f"Account with id {account_id} not found for this user.")

    created_transactions = []
    skipped_duplicates = []
    duplicate_count = 0

    # Pre-fetch all existing investment transaction hashes for this user to avoid flagging within-statement duplicates
    existing_hashes_dict = {
        t.transaction_hash: t for t in
        db.query(InvestmentTransactionDB)
        .filter(InvestmentTransactionDB.user_id == user_id)
        .all()
    }

    for t_data in transactions:
        # Generate transaction hash for deduplication
        transaction_hash = generate_investment_transaction_hash(t_data, user_id, institution_name)

        # Check if transaction hash existed in database BEFORE this upload
        existing_transaction = existing_hashes_dict.get(transaction_hash)

        # If duplicate and skip_duplicates=True, add to skipped list instead of creating
        if existing_transaction and skip_duplicates:
            skipped_duplicates.append({
                'parsed_transaction': t_data,
                'existing_transaction': existing_transaction,
                'transaction_hash': transaction_hash
            })
            duplicate_count += 1
            logger.debug(f"Skipping duplicate investment transaction: {t_data.transaction_date} - {t_data.description}")
            continue  # Skip creation

        if existing_transaction and not skip_duplicates:
            logger.debug(f"Found duplicate investment transaction in database (will create anyway): {t_data.transaction_date} - {t_data.description}")
            duplicate_count += 1

        # Map the transaction type string to the enum FIRST
        transaction_type_enum = map_transaction_type_to_enum(t_data.transaction_type)
        if not transaction_type_enum:
            logger.error(
                f"Unmapped investment transaction type '{t_data.transaction_type}' — "
                f"transaction skipped (date={t_data.transaction_date}, "
                f"amount={t_data.total_amount}, desc={t_data.description})"
            )
            continue

        # Guard: strip share-based fields from non-share transaction types
        NON_SHARE_TYPES = {InvestmentTransactionType.INTEREST, InvestmentTransactionType.FEE,
                           InvestmentTransactionType.TRANSFER_IN, InvestmentTransactionType.TRANSFER_OUT}
        if transaction_type_enum in NON_SHARE_TYPES:
            t_data_quantity = None
            t_data_price = None
            t_data_symbol = None
            t_data_api_symbol = None
        else:
            t_data_quantity = t_data.quantity
            t_data_price = t_data.price_per_share
            t_data_symbol = t_data.symbol
            t_data_api_symbol = t_data.api_symbol

        db_transaction = InvestmentTransactionDB(
            id=uuid4(),  # Generate UUID for new transaction
            user_id=user_id,
            account_id=account_id,
            holding_id=None,
            transaction_date=t_data.transaction_date,
            transaction_type=transaction_type_enum,
            symbol=t_data_symbol,
            quantity=abs(t_data_quantity) if t_data_quantity is not None else None,
            price_per_share=t_data_price,
            total_amount=abs(t_data.total_amount),
            fees=None,  # Not currently parsed
            description=t_data.description,
            api_symbol=t_data_api_symbol,
            security_type=t_data.security_type.value if t_data.security_type else None,
            transaction_hash=transaction_hash,
        )
        db.add(db_transaction)
        created_transactions.append(db_transaction)

    if duplicate_count > 0:
        if skip_duplicates:
            logger.info(f"Skipped {duplicate_count} duplicate investment transactions")
        else:
            logger.info(f"Flagged {duplicate_count} duplicate investment transactions for review")

    if not created_transactions:
        return [], skipped_duplicates, None

    try:
        db.commit()
        if account_id:
            rebuild_holdings_from_transactions(db, account_id)
            _update_investment_account_balance(db, account_id)
            db.commit()
    except Exception as e:
        db.rollback()
        raise ValueError(f"Bulk investment transaction import failed: {str(e)}")

    # NEW: Trigger backfill if historical transactions
    backfill_job_id = None

    if created_transactions and account_id:
        # Verify this is an investment account
        if account and account.account_type == AccountType.INVESTMENT:
            earliest_date = min(t.transaction_date for t in created_transactions)
            latest_date = max(t.transaction_date for t in created_transactions)

            # Only backfill if transactions are in the past
            if earliest_date < date.today():
                # Check for existing running job for this account
                existing_job = db.query(SnapshotBackfillJobDB).filter(
                    SnapshotBackfillJobDB.account_id == account_id,
                    SnapshotBackfillJobDB.status.in_(['PENDING', 'IN_PROGRESS'])
                ).first()

                if existing_job:
                    logger.info(f"Backfill job {existing_job.id} already running for account {account_id}")
                    return created_transactions, skipped_duplicates, existing_job.id

                # Limit backfill to last 10 years
                max_backfill_date = date.today() - timedelta(days=365 * 10)
                if earliest_date < max_backfill_date:
                    earliest_date = max_backfill_date
                    logger.warning(f"Limiting backfill to last 10 years (from {max_backfill_date})")

                # Create backfill job
                job = SnapshotBackfillJobDB(
                    user_id=user_id,
                    account_id=account_id,
                    start_date=earliest_date,
                    end_date=date.today(),
                    status='PENDING'
                )
                db.add(job)
                db.commit()
                db.refresh(job)

                backfill_job_id = job.id

                logger.info(f"Created backfill job {job.id} for account {account_id} ({earliest_date} to {date.today()})")

                # Submit job to runner (async)
                try:
                    job_runner = get_job_runner()
                    job_runner.submit_job(job.id, account_id, earliest_date, date.today())
                except Exception as e:
                    logger.error(f"Failed to submit backfill job {job.id}: {str(e)}")
                    # Don't fail the transaction upload if job submission fails
                    # Job will remain in PENDING status and can be retried

    return created_transactions, skipped_duplicates, backfill_job_id


def bulk_create_investment_transactions(db: Session, user_id: int, bulk_data: InvestmentTransactionBulkCreate,
                                         account_id_map: Optional[Dict[str, int]] = None) -> List[InvestmentTransactionDB]:
    """Bulk create investment transactions.

    Args:
        account_id_map: Dict mapping account_uuid (str) -> account int ID.
                        Must be provided by the router after resolving UUIDs.
    """
    if account_id_map is None:
        account_id_map = {}
    db_transactions = []
    affected_account_ids = set()
    for transaction_data in bulk_data.transactions:
        acct_uuid_str = str(transaction_data.account_uuid)
        if acct_uuid_str not in account_id_map:
            account = db.query(AccountDB).filter(
                AccountDB.uuid == transaction_data.account_uuid,
                AccountDB.user_id == user_id
            ).first()
            if not account:
                raise NotFoundError(f"Account not found for UUID {transaction_data.account_uuid}")
            account_id_map[acct_uuid_str] = account.id
        acct_id = account_id_map[acct_uuid_str]
        db_txn = _create_investment_transaction_no_rebuild(db, user_id, transaction_data, account_id=acct_id)
        db_transactions.append(db_txn)
        affected_account_ids.add(acct_id)

    try:
        db.commit()
        for t in db_transactions:
            db.refresh(t)
        for acct_id in affected_account_ids:
            rebuild_holdings_from_transactions(db, acct_id)
            _update_investment_account_balance(db, acct_id)
        db.commit()
    except IntegrityError as e:
        db.rollback()
        logger.error(f"Bulk investment transaction creation failed: {e}")
        raise ValueError("Bulk investment transaction creation failed.")

    return db_transactions

def read_db_investment_transaction(db: Session, transaction_id: int, user_id: int) -> Optional[InvestmentTransactionDB]:
    return db.query(InvestmentTransactionDB).join(AccountDB).filter(
        InvestmentTransactionDB.investment_transaction_id == transaction_id,
        AccountDB.user_id == user_id
    ).first()

def read_db_investment_transactions(db: Session, user_id: int, account_id: Optional[int] = None, skip: int = 0, limit: int = 100) -> List[InvestmentTransactionDB]:
    query = db.query(InvestmentTransactionDB).join(AccountDB).options(
        joinedload(InvestmentTransactionDB.account),
        joinedload(InvestmentTransactionDB.holding),
    ).filter(AccountDB.user_id == user_id)
    if account_id:
        query = query.filter(InvestmentTransactionDB.account_id == account_id)

    return query.order_by(desc(InvestmentTransactionDB.transaction_date)).offset(skip).limit(limit).all()

def update_db_investment_transaction(db: Session, transaction_id: int, user_id: int, transaction_updates: InvestmentTransactionUpdate) -> InvestmentTransactionDB:
    db_transaction = read_db_investment_transaction(db, transaction_id, user_id)
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")

    account_id = db_transaction.account_id

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
        if account_id:
            rebuild_holdings_from_transactions(db, account_id)
            _update_investment_account_balance(db, account_id)
            db.commit()
        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Transaction update failed.")


def delete_db_investment_transaction(db: Session, transaction_id: int, user_id: int) -> bool:
    db_transaction = read_db_investment_transaction(db, transaction_id, user_id)
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")

    account_id = db_transaction.account_id
    transaction_date = db_transaction.transaction_date

    try:
        db.delete(db_transaction)
        db.commit()
        if account_id:
            rebuild_holdings_from_transactions(db, account_id)
            _update_investment_account_balance(db, account_id)
            db.commit()

        # Trigger snapshot recalculation from deleted transaction's date
        if account_id:
            from src.services.account_snapshot import trigger_backfill_if_needed
            trigger_backfill_if_needed(db, user_id, account_id, transaction_date)

        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete transaction: {str(e)}")


# ===== UUID-BASED OPERATIONS - INVESTMENT TRANSACTIONS =====

def read_db_investment_transaction_by_uuid(db: Session, transaction_uuid: 'UUID', user_id: int) -> Optional[InvestmentTransactionDB]:
    return db.query(InvestmentTransactionDB).options(
        joinedload(InvestmentTransactionDB.account),
        joinedload(InvestmentTransactionDB.holding),
    ).filter(
        InvestmentTransactionDB.id == transaction_uuid,
        InvestmentTransactionDB.user_id == user_id
    ).first()

def update_db_investment_transaction_by_uuid(db: Session, transaction_uuid: 'UUID', user_id: int,
                                              transaction_updates: InvestmentTransactionUpdate) -> InvestmentTransactionDB:
    db_transaction = read_db_investment_transaction_by_uuid(db, transaction_uuid, user_id)
    if not db_transaction:
        raise NotFoundError(f"Investment transaction not found.")
    return update_db_investment_transaction(db, db_transaction.investment_transaction_id, user_id, transaction_updates)

def delete_db_investment_transaction_by_uuid(db: Session, transaction_uuid: 'UUID', user_id: int) -> bool:
    db_transaction = read_db_investment_transaction_by_uuid(db, transaction_uuid, user_id)
    if not db_transaction:
        raise NotFoundError(f"Investment transaction not found.")
    return delete_db_investment_transaction(db, db_transaction.investment_transaction_id, user_id)


# ===== UTILITY FUNCTIONS =====

def calculate_account_total_value(db: Session, account_id: int) -> Decimal:
    """Calculate the total market value of all holdings in an investment account."""
    TWO_PLACES = Decimal('0.01')
    holdings = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == account_id
    ).all()

    total_value = Decimal('0.00')
    for holding in holdings:
        if holding.quantity and holding.current_price:
            total_value += (holding.quantity * holding.current_price).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
        elif holding.quantity and holding.average_cost_basis:
            # Fallback to cost basis if current price not available
            total_value += (holding.quantity * holding.average_cost_basis).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)

    return total_value

