"""
Account Value Snapshot Service

Handles capturing daily snapshots of account values for historical tracking
and net worth calculations across all account types.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func, case
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Dict
from uuid import uuid4

from src.db.core import (
    AccountDB,
    AccountValueHistoryDB,
    AccountType,
    InvestmentHoldingDB,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    TransactionDB,
    TransactionType,
    DebtPaymentDB
)
from src.services import price_fetcher
from src.services.price_fetcher import fetch_bulk_historical_prices, is_option_symbol
from src.logging_config import get_logger

logger = get_logger(__name__)


def trigger_backfill_if_needed(
    db: Session,
    user_id: int,
    account_id: int,
    transaction_date: date,
) -> None:
    """
    Trigger historical snapshot backfill from transaction_date through today.

    Shared helper used by:
    - Upload confirm flow (after creating transactions)
    - Transaction delete (after removing a transaction)
    - Investment transaction delete (after removing a transaction)
    """
    from src.db.core import SnapshotBackfillJobDB
    from src.services.job_runner import get_job_runner

    account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if not account:
        return

    today = date.today()
    if transaction_date >= today:
        return

    earliest_date = transaction_date

    # Limit to last 10 years
    max_backfill_date = today - timedelta(days=365 * 10)
    if earliest_date < max_backfill_date:
        earliest_date = max_backfill_date

    # Check for existing running job
    existing_job = db.query(SnapshotBackfillJobDB).filter(
        SnapshotBackfillJobDB.account_id == account_id,
        SnapshotBackfillJobDB.status.in_(["PENDING", "IN_PROGRESS"]),
    ).first()
    if existing_job:
        logger.info(f"Backfill job already running for account {account_id}")
        return

    job = SnapshotBackfillJobDB(
        user_id=user_id,
        account_id=account_id,
        start_date=earliest_date,
        end_date=today,
        status="PENDING",
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    try:
        runner = get_job_runner()
        runner.submit_job(job.id, account_id, earliest_date, today)
        logger.info(f"Submitted backfill job {job.id} for account {account_id}")
    except Exception as e:
        logger.error(f"Failed to submit backfill job: {e}")


def parse_split_ratio(description: str) -> Decimal:
    """
    Parse stock split ratio from description string.

    Examples:
        "2:1 Stock Split" -> 2.0
        "3 for 1 split" -> 3.0
        "1:2 reverse split" -> 0.5

    Returns:
        Split multiplier (e.g., 2.0 for 2:1 split)
    """
    import re

    # Try to match "X:Y" pattern
    match = re.search(r'(\d+\.?\d*)\s*:\s*(\d+\.?\d*)', description)
    if match:
        numerator = Decimal(match.group(1))
        denominator = Decimal(match.group(2))
        return numerator / denominator

    # Try to match "X for Y" pattern
    match = re.search(r'(\d+\.?\d*)\s+for\s+(\d+\.?\d*)', description, re.IGNORECASE)
    if match:
        numerator = Decimal(match.group(1))
        denominator = Decimal(match.group(2))
        return numerator / denominator

    # Default: 1.0 (no split)
    logger.warning(f"Could not parse split ratio from: {description}")
    return Decimal('1.0')


def get_account_state_on_date(
    db: Session,
    account_id: int,
    target_date: date
) -> Dict[str, any]:
    """
    Determine account state on a specific historical date by replaying transactions.

    Returns both holdings AND cash balance for complete account picture.

    Args:
        db: Database session
        account_id: Investment account ID
        target_date: Date to calculate state for

    Returns:
        {
            'holdings': {
                'AAPL': {
                    'quantity': Decimal('100'),
                    'average_cost_basis': Decimal('150.25'),
                    'api_symbol': 'AAPL'
                },
                'TSLA': { ... }
            },
            'cash_balance': Decimal('5000.00')
        }

    Algorithm:
        1. Get initial cash balance from account.initial_cash_balance
        2. Query all investment transactions up to and including target_date
        3. Order by transaction_date ASC
        4. Replay transactions to build holdings and cash state:
           - BUY: Add to holdings, decrease cash
           - SELL: Reduce holdings, increase cash
           - DIVIDEND: Increase cash (no holdings change)
           - INTEREST: Increase cash
           - FEE: Decrease cash
           - TRANSFER: Increase/decrease cash (based on amount sign)
           - SPLIT: Adjust quantity and cost basis
           - EXPIRATION: Zero out expired option holdings
        6. Filter out holdings with quantity <= 0
        7. Return holdings snapshot and cash balance
    """
    # Get account to retrieve initial cash balance
    account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    # Query transactions up to target_date
    # Within the same date, process BUY/REINVESTMENT/TRANSFER before SELL/EXPIRATION
    # so that holdings exist before being reduced (matches rebuild_holdings_from_transactions).
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
        InvestmentTransactionDB.account_id == account_id,
        InvestmentTransactionDB.transaction_date <= target_date,
    ).order_by(
        InvestmentTransactionDB.transaction_date.asc(),
        type_priority,
        InvestmentTransactionDB.investment_transaction_id.asc(),
    ).all()

    # Initialize state
    holdings = {}  # {symbol: {'quantity': Decimal, 'average_cost_basis': Decimal, 'api_symbol': str}}
    cash_balance = account.initial_cash_balance or Decimal('0')

    # Replay transactions
    for txn in transactions:
        # Use OCC api_symbol for options so they don't merge with stock holdings
        # sharing the same underlying ticker (matches _holding_key in crud_investment).
        if txn.api_symbol and is_option_symbol(txn.api_symbol):
            symbol = txn.api_symbol
        else:
            symbol = txn.symbol

        if txn.transaction_type == InvestmentTransactionType.BUY:
            # Update holdings
            if symbol not in holdings:
                holdings[symbol] = {
                    'quantity': Decimal('0'),
                    'average_cost_basis': Decimal('0'),
                    'api_symbol': txn.api_symbol or symbol
                }

            # Weighted average cost basis
            old_qty = holdings[symbol]['quantity']
            old_basis = holdings[symbol]['average_cost_basis']
            new_qty = old_qty + txn.quantity

            holdings[symbol]['average_cost_basis'] = (
                (old_qty * old_basis + txn.quantity * txn.price_per_share) / new_qty
            )
            holdings[symbol]['quantity'] = new_qty

            # Decrease cash (total cost of purchase)
            # total_amount may be negative (e.g. -1714.65 for a buy) — use abs
            cash_balance -= abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.SELL:
            if symbol in holdings:
                holdings[symbol]['quantity'] -= txn.quantity
                # Cost basis stays same (weighted average)

            # Increase cash (proceeds from sale)
            cash_balance += abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.DIVIDEND:
            # Increase cash (dividends paid in cash)
            cash_balance += abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.INTEREST:
            # Increase cash
            cash_balance += abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.FEE:
            # Decrease cash
            cash_balance -= abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.TRANSFER_IN:
            cash_balance += abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.TRANSFER_OUT:
            cash_balance -= abs(txn.total_amount)

        elif txn.transaction_type == InvestmentTransactionType.REINVESTMENT:
            # REINVESTMENT: Dividend automatically used to buy more shares
            # Similar to BUY but cash doesn't change (dividend already included in cash before being reinvested)
            if symbol not in holdings:
                holdings[symbol] = {
                    'quantity': Decimal('0'),
                    'average_cost_basis': Decimal('0'),
                    'api_symbol': txn.api_symbol or symbol
                }

            # Weighted average cost basis
            old_qty = holdings[symbol]['quantity']
            old_basis = holdings[symbol]['average_cost_basis']
            new_qty = old_qty + txn.quantity

            if new_qty > 0:
                holdings[symbol]['average_cost_basis'] = (
                    (old_qty * old_basis + txn.quantity * txn.price_per_share) / new_qty
                )
            holdings[symbol]['quantity'] = new_qty

        elif txn.transaction_type == InvestmentTransactionType.SPLIT:
            # Adjust quantity and cost basis
            if symbol in holdings and txn.description:
                ratio = parse_split_ratio(txn.description)  # e.g., "2:1" → 2.0
                holdings[symbol]['quantity'] *= ratio
                holdings[symbol]['average_cost_basis'] /= ratio

        elif txn.transaction_type == InvestmentTransactionType.EXPIRATION:
            # Option expired worthless — zero out the holding
            if symbol in holdings:
                holdings[symbol]['quantity'] = Decimal('0')

    # Filter out zero/negative holdings
    active_holdings = {s: h for s, h in holdings.items() if h['quantity'] > 0}

    return {
        'holdings': active_holdings,
        'cash_balance': cash_balance
    }


def dismiss_snapshot_reviews(
    db: Session,
    account_id: int,
    snapshot_uuids: list,
    dismiss_reason: str = "Dismissed by user"
) -> int:
    """
    Dismiss needs_review flags on specified snapshots.

    Args:
        db: Database session
        account_id: Account the snapshots belong to
        snapshot_uuids: List of snapshot UUIDs to dismiss
        dismiss_reason: Reason for dismissal (appended to review_reason)

    Returns:
        Count of snapshots updated
    """
    snapshots = db.query(AccountValueHistoryDB).filter(
        AccountValueHistoryDB.account_id == account_id,
        AccountValueHistoryDB.uuid.in_(snapshot_uuids),
        AccountValueHistoryDB.needs_review == True
    ).all()

    for snapshot in snapshots:
        snapshot.needs_review = False
        existing_reason = snapshot.review_reason or ""
        snapshot.review_reason = f"{existing_reason} | Dismissed: {dismiss_reason}".strip(" |")

    db.commit()
    return len(snapshots)


def update_snapshot(
    db: Session,
    account_id: int,
    snapshot_uuid,
    updates: dict,
) -> AccountValueHistoryDB:
    """
    Apply partial updates to a single snapshot.

    Args:
        updates: Dict of fields to update (balance, securities_value, etc.)
                 May include 'dismiss_review' bool to clear the review flag.
    """
    snapshot = db.query(AccountValueHistoryDB).filter(
        AccountValueHistoryDB.account_id == account_id,
        AccountValueHistoryDB.uuid == snapshot_uuid,
    ).first()

    if not snapshot:
        raise ValueError("Snapshot not found")

    editable_fields = {
        "balance", "securities_value", "cash_balance",
        "total_cost_basis", "unrealized_gain_loss", "realized_gain_loss",
    }
    for field in editable_fields:
        if field in updates and updates[field] is not None:
            setattr(snapshot, field, updates[field])

    snapshot.snapshot_source = "MANUAL_EDIT"

    if updates.get("dismiss_review"):
        snapshot.needs_review = False
        existing_reason = snapshot.review_reason or ""
        snapshot.review_reason = f"{existing_reason} | Manually edited & dismissed".strip(" |")

    db.commit()
    db.refresh(snapshot)
    return snapshot


def _reverse_balance_for_type(
    account_type: AccountType,
    transaction_type: TransactionType,
    amount: Decimal,
    current_balance: Decimal
) -> Decimal:
    """
    Pure function to reverse a transaction's balance effect.

    Given the current balance, returns what the balance was BEFORE this transaction.
    Mirrors the logic in crud_transaction._reverse_balance_effect but takes explicit args.
    """
    abs_amount = abs(amount)
    if account_type == AccountType.CREDIT_CARD:
        if transaction_type in [TransactionType.PURCHASE, TransactionType.FEE, TransactionType.INTEREST]:
            return current_balance - abs_amount  # undo debt increase
        elif transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.TRANSFER_IN]:
            return current_balance + abs_amount  # undo debt reduction
        elif transaction_type == TransactionType.WITHDRAWAL:
            return current_balance + abs_amount  # undo debt reduction
        elif transaction_type == TransactionType.TRANSFER_OUT:
            return current_balance - abs_amount  # undo debt increase
        else:
            raise ValueError(f"Unhandled transaction type: {transaction_type}")
    else:
        if transaction_type in [TransactionType.CREDIT, TransactionType.DEPOSIT, TransactionType.TRANSFER_IN]:
            return current_balance - abs_amount  # undo incoming
        elif transaction_type in [TransactionType.WITHDRAWAL, TransactionType.FEE, TransactionType.PURCHASE, TransactionType.TRANSFER_OUT]:
            return current_balance + abs_amount  # undo outgoing
        elif transaction_type == TransactionType.INTEREST:
            return current_balance - abs_amount  # undo interest earned
        else:
            raise ValueError(f"Unhandled transaction type: {transaction_type}")


def get_non_investment_balance_on_date(
    db: Session,
    account: AccountDB,
    target_date: date
) -> Decimal:
    """
    Derive historical balance for a non-investment account on a given date
    by walking backwards from the current balance.

    Algorithm:
        1. Start from account.balance (current)
        2. Query all transactions AFTER target_date
        3. Reverse each transaction's effect to walk the balance backwards
        4. Return the derived historical balance
    """
    # Get all transactions after the target date, ordered newest first
    future_transactions = db.query(TransactionDB).filter(
        TransactionDB.account_id == account.id,
        TransactionDB.transaction_date > target_date
    ).order_by(TransactionDB.transaction_date.desc()).all()

    balance = account.balance
    for txn in future_transactions:
        balance = _reverse_balance_for_type(
            account_type=account.account_type,
            transaction_type=txn.transaction_type,
            amount=txn.amount,
            current_balance=balance
        )

    return balance


def recalculate_non_investment_snapshots(
    db: Session,
    account_id: int,
    start_date: date,
    end_date: date,
    reason: str = "Transaction replay backfill"
) -> Dict[str, int]:
    """
    Create or recalculate snapshots for non-investment accounts using transaction replay.

    For each day in [start_date, end_date]:
    1. Derive historical balance via get_non_investment_balance_on_date()
    2. For LOAN accounts, also calculate YTD principal/interest data
    3. Create/update snapshot with snapshot_source="BACKFILL"
    4. Flag needs_review=True for dates before earliest transaction (uncertain data)
    """
    account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    results = {'created': 0, 'updated': 0, 'failed': 0, 'skipped': 0}

    # Find earliest transaction date to flag uncertain data
    earliest_txn = db.query(func.min(TransactionDB.transaction_date)).filter(
        TransactionDB.account_id == account_id
    ).scalar()

    current_date = start_date
    while current_date <= end_date:
        try:
            # 1. Derive historical balance
            historical_balance = get_non_investment_balance_on_date(db, account, current_date)

            # 2. For LOAN accounts, also get YTD data
            loan_data = {}
            if account.account_type == AccountType.LOAN:
                loan_data = calculate_loan_account_snapshot(db, account, current_date)

            # 3. Check if before earliest transaction
            needs_review = earliest_txn is not None and current_date < earliest_txn
            review_reason = "Date is before earliest transaction — balance may be inaccurate" if needs_review else None

            # 4. Check if snapshot already exists
            existing_snapshot = db.query(AccountValueHistoryDB).filter(
                AccountValueHistoryDB.account_id == account_id,
                AccountValueHistoryDB.value_date == current_date
            ).first()

            if existing_snapshot:
                existing_snapshot.balance = historical_balance
                existing_snapshot.principal_paid_ytd = loan_data.get('principal_paid_ytd')
                existing_snapshot.interest_paid_ytd = loan_data.get('interest_paid_ytd')
                existing_snapshot.last_recalculated_at = datetime.utcnow()
                existing_snapshot.recalculation_count += 1
                existing_snapshot.recalculation_reason = reason
                if needs_review:
                    existing_snapshot.needs_review = True
                    existing_snapshot.review_reason = review_reason
                results['updated'] += 1
            else:
                new_snapshot = AccountValueHistoryDB(
                    uuid=uuid4(),
                    account_id=account_id,
                    value_date=current_date,
                    balance=historical_balance,
                    principal_paid_ytd=loan_data.get('principal_paid_ytd'),
                    interest_paid_ytd=loan_data.get('interest_paid_ytd'),
                    snapshot_source="BACKFILL",
                    recalculation_count=0,
                    needs_review=needs_review,
                    review_reason=review_reason
                )
                db.add(new_snapshot)
                results['created'] += 1

            db.commit()

        except Exception as e:
            logger.error(f"Failed to create/update non-investment snapshot for {current_date}: {str(e)}", exc_info=True)
            results['failed'] += 1
            db.rollback()

        current_date += timedelta(days=1)

    logger.info(f"Non-investment snapshot recalculation complete for account {account_id}: {results}")
    return results


def recalculate_account_snapshots(
    db: Session,
    account_id: int,
    start_date: date,
    end_date: date,
    reason: str = "Historical transactions added",
    delay_between_prices: float = 0.5
) -> Dict[str, int]:
    """
    Create or recalculate account snapshots for a date range.

    For each day in the range:
    1. Determine account state (holdings + cash) on that date
    2. Fetch historical prices for holdings (bulk optimization)
    3. Calculate securities value, cash balance, and total
    4. Update existing snapshot OR create new one
    5. Update audit trail

    Args:
        db: Database session
        account_id: Account to create/update snapshots for
        start_date: First date to process (inclusive)
        end_date: Last date to process (inclusive)
        reason: Human-readable reason for recalculation
        delay_between_prices: Rate limiting delay (seconds) between price API calls

    Returns:
        {
            'created': Number of new snapshots created,
            'updated': Number of existing snapshots recalculated,
            'failed': Number of dates that failed processing,
            'skipped': Number of dates skipped (no holdings/cash on that date)
        }

    Performance:
        - Uses bulk price fetching to minimize API calls
        - 90 days × 10 holdings = 10 API calls (vs 900 without bulk)

    Error Handling:
        - If price unavailable, uses cost basis as fallback
        - Marks snapshot with needs_review=True if prices missing
        - Continues processing other dates even if one fails
    """
    # Verify account exists
    account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    # Dispatch to appropriate handler based on account type
    if account.account_type != AccountType.INVESTMENT:
        return recalculate_non_investment_snapshots(
            db=db,
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
            reason=reason
        )

    results = {'created': 0, 'updated': 0, 'failed': 0, 'skipped': 0}

    # Get all unique symbols that appear in date range (for bulk price fetching)
    logger.info(f"Scanning date range to collect symbols for bulk price fetch")
    all_symbols = set()
    current_date = start_date
    while current_date <= end_date:
        state = get_account_state_on_date(db, account_id, current_date)
        all_symbols.update(state['holdings'].keys())
        current_date += timedelta(days=1)

    # Bulk fetch historical prices for all symbols
    logger.info(f"Bulk fetching historical prices for {len(all_symbols)} symbols from {start_date} to {end_date}")
    bulk_prices = fetch_bulk_historical_prices(list(all_symbols), start_date, end_date)

    # Iterate through each day
    current_date = start_date
    while current_date <= end_date:
        try:
            # 1. Get account state on this date
            state = get_account_state_on_date(db, account_id, current_date)
            holdings = state['holdings']
            cash_balance = state['cash_balance']

            # Skip if no holdings and no cash
            if not holdings and cash_balance == 0:
                results['skipped'] += 1
                current_date += timedelta(days=1)
                continue

            # 2. Calculate securities value using bulk-fetched prices
            securities_value = Decimal('0')
            total_cost_basis = Decimal('0')
            missing_prices = []
            TWO_PLACES = Decimal('0.01')

            for symbol, holding_data in holdings.items():
                quantity = holding_data['quantity']
                cost_basis = holding_data['average_cost_basis']  # Note: key is 'average_cost_basis'
                api_symbol = holding_data['api_symbol']

                # Get price from bulk-fetched data
                historical_price = None
                if api_symbol in bulk_prices and current_date in bulk_prices[api_symbol]:
                    historical_price = bulk_prices[api_symbol][current_date]
                elif api_symbol in bulk_prices:
                    # Find nearest previous date
                    available_dates = sorted([d for d in bulk_prices[api_symbol].keys() if d <= current_date])
                    if available_dates:
                        nearest_date = available_dates[-1]
                        historical_price = bulk_prices[api_symbol][nearest_date]
                        logger.debug(f"Using {nearest_date} price for {symbol} on {current_date}")

                if historical_price:
                    securities_value += (quantity * historical_price).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                    total_cost_basis += (quantity * cost_basis).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                else:
                    # Fallback to cost basis
                    logger.warning(f"No historical price for {symbol} on {current_date}, using cost basis")
                    securities_value += (quantity * cost_basis).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                    total_cost_basis += (quantity * cost_basis).quantize(TWO_PLACES, rounding=ROUND_HALF_UP)
                    missing_prices.append(symbol)

            # 3. Calculate totals
            total_balance = securities_value + cash_balance
            unrealized_gain_loss = securities_value - total_cost_basis

            # 4. Check if snapshot already exists
            existing_snapshot = db.query(AccountValueHistoryDB).filter(
                AccountValueHistoryDB.account_id == account_id,
                AccountValueHistoryDB.value_date == current_date
            ).first()

            if existing_snapshot:
                # Update existing snapshot
                existing_snapshot.balance = total_balance
                existing_snapshot.securities_value = securities_value
                existing_snapshot.cash_balance = cash_balance
                existing_snapshot.total_cost_basis = total_cost_basis
                existing_snapshot.unrealized_gain_loss = unrealized_gain_loss
                existing_snapshot.last_recalculated_at = datetime.utcnow()
                existing_snapshot.recalculation_count += 1
                existing_snapshot.recalculation_reason = reason

                if missing_prices:
                    existing_snapshot.needs_review = True
                    existing_snapshot.review_reason = f"Missing price data for: {', '.join(missing_prices)}"

                results['updated'] += 1
            else:
                # Create new snapshot
                new_snapshot = AccountValueHistoryDB(
                    uuid=uuid4(),
                    account_id=account_id,
                    value_date=current_date,
                    balance=total_balance,
                    securities_value=securities_value,
                    cash_balance=cash_balance,
                    total_cost_basis=total_cost_basis,
                    unrealized_gain_loss=unrealized_gain_loss,
                    snapshot_source="BACKFILL",
                    recalculation_count=0,
                    needs_review=bool(missing_prices),
                    review_reason=f"Missing price data for: {', '.join(missing_prices)}" if missing_prices else None
                )
                db.add(new_snapshot)
                results['created'] += 1

            db.commit()

        except Exception as e:
            logger.error(f"Failed to create/update snapshot for {current_date}: {str(e)}", exc_info=True)
            results['failed'] += 1
            db.rollback()

        current_date += timedelta(days=1)

    logger.info(f"Snapshot recalculation complete: {results}")
    return results


def calculate_investment_account_snapshot(
    db: Session,
    account: AccountDB,
    snapshot_date: date
) -> Dict[str, Decimal]:
    """
    Calculate snapshot data for an investment account.
    Returns: balance, total_cost_basis, unrealized_gain_loss
    """
    holdings = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == account.id
    ).all()

    total_value = Decimal('0.00')
    total_cost_basis = Decimal('0.00')

    for holding in holdings:
        if holding.quantity and holding.quantity > 0:
            # Use current_price if available, otherwise use cost basis
            price = holding.current_price if holding.current_price else holding.average_cost_basis
            if price:
                total_value += holding.quantity * price

            # Calculate cost basis
            if holding.average_cost_basis:
                total_cost_basis += holding.quantity * holding.average_cost_basis

    unrealized_gain_loss = total_value - total_cost_basis if total_cost_basis > 0 else None

    return {
        'balance': total_value,
        'total_cost_basis': total_cost_basis if total_cost_basis > 0 else None,
        'unrealized_gain_loss': unrealized_gain_loss
    }


def calculate_loan_account_snapshot(
    db: Session,
    account: AccountDB,
    snapshot_date: date
) -> Dict[str, Optional[Decimal]]:
    """
    Calculate snapshot data for a loan account.
    Returns: balance, principal_paid_ytd, interest_paid_ytd
    """
    # Calculate YTD principal and interest from debt payments
    year_start = date(snapshot_date.year, 1, 1)

    ytd_payments = db.query(
        func.sum(DebtPaymentDB.principal_amount).label('principal_ytd'),
        func.sum(DebtPaymentDB.interest_amount).label('interest_ytd')
    ).filter(
        DebtPaymentDB.loan_account_id == account.id,
        DebtPaymentDB.payment_date >= year_start,
        DebtPaymentDB.payment_date <= snapshot_date
    ).first()

    return {
        'balance': account.balance,
        'principal_paid_ytd': ytd_payments.principal_ytd if ytd_payments.principal_ytd else None,
        'interest_paid_ytd': ytd_payments.interest_ytd if ytd_payments.interest_ytd else None
    }


def create_account_snapshot(
    db: Session,
    account_id: int,
    snapshot_date: date,
    snapshot_source: str = "MANUAL"
) -> AccountValueHistoryDB:
    """
    Create a daily snapshot of an account's value.
    Handles all account types: checking, savings, credit cards, loans, and investments.
    """
    account = db.query(AccountDB).filter(AccountDB.id == account_id).first()
    if not account:
        raise ValueError(f"Account {account_id} not found")

    # Check if snapshot already exists for this date
    existing = db.query(AccountValueHistoryDB).filter(
        AccountValueHistoryDB.account_id == account_id,
        AccountValueHistoryDB.value_date == snapshot_date
    ).first()

    if existing:
        # Update existing snapshot instead of creating new one
        snapshot = existing
    else:
        snapshot = AccountValueHistoryDB(
            uuid=uuid4(),
            account_id=account_id,
            value_date=snapshot_date,
            snapshot_source=snapshot_source
        )

    # Calculate account-type specific data
    if account.account_type == AccountType.INVESTMENT:
        data = calculate_investment_account_snapshot(db, account, snapshot_date)
        snapshot.balance = data['balance']
        snapshot.total_cost_basis = data['total_cost_basis']
        snapshot.unrealized_gain_loss = data['unrealized_gain_loss']
    elif account.account_type == AccountType.LOAN:
        data = calculate_loan_account_snapshot(db, account, snapshot_date)
        snapshot.balance = data['balance']
        snapshot.principal_paid_ytd = data['principal_paid_ytd']
        snapshot.interest_paid_ytd = data['interest_paid_ytd']
    else:
        # For checking, savings, credit cards, and other account types
        if snapshot_date != date.today():
            # Derive historical balance via transaction replay
            snapshot.balance = get_non_investment_balance_on_date(db, account, snapshot_date)
        else:
            snapshot.balance = account.balance

    if not existing:
        db.add(snapshot)

    db.commit()
    db.refresh(snapshot)
    return snapshot


def update_investment_prices(
    db: Session,
    user_id: int,
    delay: float = 0.5
) -> Dict[str, int]:
    """
    Update current prices for all investment holdings for a user.
    Fetches live prices from Yahoo Finance.

    Returns: {
        'updated': count of holdings updated,
        'failed': count of holdings that failed to update
    }
    """
    # Get all investment accounts for user
    investment_accounts = db.query(AccountDB).filter(
        AccountDB.user_id == user_id,
        AccountDB.account_type == AccountType.INVESTMENT
    ).all()

    if not investment_accounts:
        return {'updated': 0, 'failed': 0}

    # Get all holdings across all investment accounts
    account_ids = [acc.id for acc in investment_accounts]
    holdings = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id.in_(account_ids)
    ).all()

    if not holdings:
        return {'updated': 0, 'failed': 0}

    # Extract unique symbols
    symbols = [h.symbol for h in holdings]

    logger.info(f"Fetching prices for {len(symbols)} holdings...")

    # Fetch all prices
    prices = price_fetcher.fetch_bulk_prices(symbols, delay=delay)

    # Update holdings with new prices
    updated = 0
    failed = 0

    for holding in holdings:
        price = prices.get(holding.symbol)

        if price:
            holding.current_price = price
            holding.last_price_update = datetime.utcnow()
            updated += 1
        else:
            logger.warning(f"Failed to fetch price for {holding.symbol}")
            failed += 1

    db.commit()

    # Recalculate account balances now that prices are updated
    if updated > 0:
        from src.crud.crud_investment import _update_investment_account_balance
        for acc in investment_accounts:
            _update_investment_account_balance(db, acc.id)
        db.commit()

    logger.info(f"Updated {updated} holdings, {failed} failed")

    return {'updated': updated, 'failed': failed}


def create_all_account_snapshots(
    db: Session,
    user_id: int,
    snapshot_date: date,
    snapshot_source: str = "SCHEDULED",
    update_prices: bool = True
) -> List[AccountValueHistoryDB]:
    """
    Create snapshots for all accounts belonging to a user.
    Typically called by end-of-day job.

    Args:
        update_prices: If True, fetches latest market prices before creating snapshots
    """
    # Update investment prices first if requested
    if update_prices:
        logger.info(f"Updating investment prices for user {user_id}...")
        result = update_investment_prices(db, user_id)
        logger.info(f"Price update complete: {result}")

    accounts = db.query(AccountDB).filter(AccountDB.user_id == user_id).all()

    snapshots = []
    for account in accounts:
        try:
            snapshot = create_account_snapshot(
                db=db,
                account_id=account.id,
                snapshot_date=snapshot_date,
                snapshot_source=snapshot_source
            )
            snapshots.append(snapshot)
        except Exception as e:
            logger.error(f"Error creating snapshot for account {account.id}: {str(e)}", exc_info=True)
            # Continue with other accounts even if one fails
            continue

    return snapshots


def get_net_worth_history(
    db: Session,
    user_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[Dict]:
    """
    Get historical net worth data for a user.
    Returns daily totals aggregated across all accounts.
    """
    # Loans and credit cards store balance as positive (amount owed),
    # so negate them for net worth calculation.
    signed_balance = case(
        (AccountDB.account_type.in_([AccountType.LOAN, AccountType.CREDIT_CARD]),
         -AccountValueHistoryDB.balance),
        else_=AccountValueHistoryDB.balance,
    )

    query = db.query(
        AccountValueHistoryDB.value_date,
        func.sum(signed_balance).label('total_balance'),
        func.sum(AccountValueHistoryDB.unrealized_gain_loss).label('total_unrealized_gains')
    ).join(AccountDB).filter(
        AccountDB.user_id == user_id
    )

    if start_date:
        query = query.filter(AccountValueHistoryDB.value_date >= start_date)
    if end_date:
        query = query.filter(AccountValueHistoryDB.value_date <= end_date)

    results = query.group_by(
        AccountValueHistoryDB.value_date
    ).order_by(
        AccountValueHistoryDB.value_date
    ).all()

    return [
        {
            'date': row.value_date.isoformat(),
            'net_worth': float(row.total_balance) if row.total_balance else 0.0,
            'total_unrealized_gains': float(row.total_unrealized_gains) if row.total_unrealized_gains else None
        }
        for row in results
    ]


def get_account_value_history(
    db: Session,
    account_id: int,
    user_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None
) -> List[AccountValueHistoryDB]:
    """
    Get historical value data for a specific account.
    """
    # Verify account ownership
    account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()

    if not account:
        raise ValueError(f"Account {account_id} not found or access denied")

    query = db.query(AccountValueHistoryDB).filter(
        AccountValueHistoryDB.account_id == account_id
    )

    if start_date:
        query = query.filter(AccountValueHistoryDB.value_date >= start_date)
    if end_date:
        query = query.filter(AccountValueHistoryDB.value_date <= end_date)

    return query.order_by(AccountValueHistoryDB.value_date).all()
