from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from typing import Optional, List, Dict
from datetime import datetime
from decimal import Decimal
from uuid import UUID, uuid4

# Import your database models
from src.db.core import (
    AccountDB, UserDB, NotFoundError, AccountType,
    TransactionDB, TransactionRelationshipDB, TransactionAmortizationScheduleDB,
    TransactionSplitAllocationDB, TransactionTagDB,
    InvestmentTransactionDB, InvestmentHoldingDB,
    DebtPaymentDB, DebtPlanAccountLinkDB, DebtRepaymentScheduleDB,
    AccountValueHistoryDB, SnapshotBackfillJobDB,
    UploadJobDB, SkippedTransactionDB,
)
from src.models.account import AccountCreate, AccountUpdate, AccountStats, AccountTypeEnum
from src.logging_config import get_logger

logger = get_logger(__name__)


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
    # For investment accounts, seed initial_cash_balance from the starting balance
    # so that transaction replay correctly includes it.
    account_type_enum = AccountType(account_data.account_type.value)
    initial_cash = account_data.balance if account_type_enum == AccountType.INVESTMENT else Decimal('0')

    db_account = AccountDB(
        uuid=uuid4(),
        user_id=user_id,
        account_name=account_data.account_name,
        account_type=account_type_enum,
        institution_name=account_data.institution_name,
        account_number_last4=account_data.account_number_last4,
        balance=account_data.balance,
        initial_cash_balance=initial_cash,
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
    if hasattr(db_account, 'investment_transactions') and db_account.investment_transactions:
        raise ValueError("Cannot delete account with existing investment transactions")
    
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

def get_db_account_by_last_four(db: Session, user_id: int, last_four: str) -> AccountDB:
    """Get account by the last four digits of its number for a specific user."""
    account = db.query(AccountDB).filter(
        AccountDB.user_id == user_id,
        AccountDB.account_number_last4 == last_four
    ).first()
    if not account:
        raise NotFoundError(f"Account with last four digits {last_four} not found.")
    return account


# ===== UUID-BASED OPERATIONS =====

def read_db_account_by_uuid(db: Session, account_uuid: UUID, user_id: int) -> Optional[AccountDB]:
    """Read an account by UUID, filtered by user ownership."""
    return db.query(AccountDB).filter(
        AccountDB.uuid == account_uuid,
        AccountDB.user_id == user_id
    ).first()


def update_db_account_by_uuid(db: Session, account_uuid: UUID, user_id: int, account_updates: AccountUpdate) -> AccountDB:
    """Update an existing account by UUID."""
    db_account = db.query(AccountDB).filter(
        AccountDB.uuid == account_uuid,
        AccountDB.user_id == user_id
    ).first()

    if not db_account:
        raise NotFoundError(f"Account not found")

    # Check for account name uniqueness if name is being updated
    if account_updates.account_name and account_updates.account_name != db_account.account_name:
        existing_name = db.query(AccountDB).filter(
            AccountDB.user_id == user_id,
            AccountDB.account_name == account_updates.account_name,
            AccountDB.id != db_account.id
        ).first()
        if existing_name:
            raise ValueError(f"Account name '{account_updates.account_name}' already exists")

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

    db_account.updated_at = datetime.utcnow()

    try:
        db.commit()
        db.refresh(db_account)
        return db_account
    except IntegrityError:
        db.rollback()
        raise ValueError("Account update failed due to database constraint")


def delete_db_account_by_uuid(db: Session, account_uuid: UUID, user_id: int, force: bool = False) -> Dict:
    """Delete an account by UUID.

    If force=False (default), raises ValueError if the account has associated data.
    If force=True, cascade-deletes all associated records and returns deletion counts.
    """
    db_account = db.query(AccountDB).filter(
        AccountDB.uuid == account_uuid,
        AccountDB.user_id == user_id
    ).first()

    if not db_account:
        raise NotFoundError("Account not found")

    if not force:
        # Check all possible related records, not just transactions
        conflicts = []
        if db_account.transactions:
            conflicts.append("transactions")
        if db_account.investment_transactions:
            conflicts.append("investment transactions")
        if db_account.investment_holdings:
            conflicts.append("investment holdings")
        if db_account.debt_payments:
            conflicts.append("debt payments")
        if db_account.debt_payments_from:
            conflicts.append("debt payment sources")
        if db_account.debt_repayment_plans_link:
            conflicts.append("debt repayment plans")
        if db_account.debt_repayment_schedules:
            conflicts.append("debt repayment schedules")
        if db_account.value_history:
            conflicts.append("account history snapshots")

        if conflicts:
            raise ValueError(
                f"Cannot delete account with existing {', '.join(conflicts)}. "
                f"Use ?force=true to cascade-delete all associated data."
            )

        try:
            db.delete(db_account)
            db.commit()
            return {}
        except Exception as e:
            db.rollback()
            raise ValueError(f"Failed to delete account: {str(e)}")

    # Force delete: cascade-delete all associated records
    account_id = db_account.id
    deleted = {}

    try:
        # 1. Get transaction IDs for this account (needed for child record cleanup)
        transaction_ids = [
            t.db_id for t in
            db.query(TransactionDB.db_id).filter(TransactionDB.account_id == account_id).all()
        ]

        if transaction_ids:
            # 2. Transaction relationships (either side references this account's transactions)
            deleted["transaction_relationships"] = db.query(TransactionRelationshipDB).filter(
                (TransactionRelationshipDB.from_transaction_id.in_(transaction_ids)) |
                (TransactionRelationshipDB.to_transaction_id.in_(transaction_ids))
            ).delete(synchronize_session="fetch")

            # 3. Amortization schedules
            deleted["amortization_schedules"] = db.query(TransactionAmortizationScheduleDB).filter(
                TransactionAmortizationScheduleDB.transaction_id.in_(transaction_ids)
            ).delete(synchronize_session="fetch")

            # 4. Split allocations
            deleted["split_allocations"] = db.query(TransactionSplitAllocationDB).filter(
                TransactionSplitAllocationDB.transaction_id.in_(transaction_ids)
            ).delete(synchronize_session="fetch")

            # 5. Transaction tags
            deleted["transaction_tags"] = db.query(TransactionTagDB).filter(
                TransactionTagDB.transaction_id.in_(transaction_ids)
            ).delete(synchronize_session="fetch")

            # 6. Transactions
            deleted["transactions"] = db.query(TransactionDB).filter(
                TransactionDB.account_id == account_id
            ).delete(synchronize_session="fetch")

        # 7. Investment transactions
        deleted["investment_transactions"] = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.account_id == account_id
        ).delete(synchronize_session="fetch")

        # 8. Investment holdings
        deleted["investment_holdings"] = db.query(InvestmentHoldingDB).filter(
            InvestmentHoldingDB.account_id == account_id
        ).delete(synchronize_session="fetch")

        # 9. Debt plan account links
        deleted["debt_plan_links"] = db.query(DebtPlanAccountLinkDB).filter(
            DebtPlanAccountLinkDB.account_id == account_id
        ).delete(synchronize_session="fetch")

        # 10. Debt payments — delete where this is the loan account, null out where it's the payment source
        deleted["debt_payments"] = db.query(DebtPaymentDB).filter(
            DebtPaymentDB.loan_account_id == account_id
        ).delete(synchronize_session="fetch")

        nulled_source_payments = db.query(DebtPaymentDB).filter(
            DebtPaymentDB.payment_source_account_id == account_id
        ).update({"payment_source_account_id": None}, synchronize_session="fetch")
        deleted["debt_payments_source_nulled"] = nulled_source_payments

        # 11. Debt repayment schedules
        deleted["debt_repayment_schedules"] = db.query(DebtRepaymentScheduleDB).filter(
            DebtRepaymentScheduleDB.account_id == account_id
        ).delete(synchronize_session="fetch")

        # 12. Account value history
        deleted["account_snapshots"] = db.query(AccountValueHistoryDB).filter(
            AccountValueHistoryDB.account_id == account_id
        ).delete(synchronize_session="fetch")

        # 13. Snapshot backfill jobs
        deleted["backfill_jobs"] = db.query(SnapshotBackfillJobDB).filter(
            SnapshotBackfillJobDB.account_id == account_id
        ).delete(synchronize_session="fetch")

        # 14. Upload jobs — null out account_id (preserves audit trail)
        nulled_upload_jobs = db.query(UploadJobDB).filter(
            UploadJobDB.account_id == account_id
        ).update({"account_id": None}, synchronize_session="fetch")
        deleted["upload_jobs_nulled"] = nulled_upload_jobs

        # 15. Delete the account
        db.delete(db_account)
        db.commit()

        # Strip zero counts
        deleted = {k: v for k, v in deleted.items() if v > 0}

        logger.info(f"Force-deleted account {account_uuid}: {deleted}")
        return deleted

    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to force-delete account: {str(e)}")
