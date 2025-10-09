"""
Account Value Snapshot Service

Handles capturing daily snapshots of account values for historical tracking
and net worth calculations across all account types.
"""
from sqlalchemy.orm import Session
from sqlalchemy import func
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict

from src.db.core import (
    AccountDB,
    AccountValueHistoryDB,
    AccountType,
    InvestmentHoldingDB,
    DebtPaymentDB
)
from src.services import price_fetcher


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
    snapshot_source: str = "SYSTEM"
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
        # Just use the current balance
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

    print(f"Fetching prices for {len(symbols)} holdings...")

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
            print(f"Failed to fetch price for {holding.symbol}")
            failed += 1

    db.commit()

    print(f"Updated {updated} holdings, {failed} failed")

    return {'updated': updated, 'failed': failed}


def create_all_account_snapshots(
    db: Session,
    user_id: int,
    snapshot_date: date,
    snapshot_source: str = "EOD_JOB",
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
        print(f"Updating investment prices for user {user_id}...")
        result = update_investment_prices(db, user_id)
        print(f"Price update complete: {result}")

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
            print(f"Error creating snapshot for account {account.id}: {str(e)}")
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
    query = db.query(
        AccountValueHistoryDB.value_date,
        func.sum(AccountValueHistoryDB.balance).label('total_balance'),
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
