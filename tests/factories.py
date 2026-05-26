"""Test data factories.

Plain functions (no factory_boy) that build and flush ORM rows with sane
Faker-backed defaults. Pass `db` (a Session) plus any field overrides as
kwargs. FK parents are required positionally so callers can't forget them
under SQLite's live FK enforcement.
"""
from datetime import date
from decimal import Decimal
from itertools import count
from uuid import uuid4

from faker import Faker

from src.db.core import (
    AccountDB,
    AccountType,
    CategoryDB,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    SourceType,
    TransactionDB,
    TransactionType,
    UserDB,
)

fake = Faker()

# Category.name carries a UNIQUE constraint — keep generated names distinct.
_category_seq = count(1)


def make_user(db, **kw):
    defaults = dict(
        id=uuid4(),
        email=fake.unique.email(),
        username=fake.unique.user_name(),
        password_hash="x",
    )
    defaults.update(kw)
    user = UserDB(**defaults)
    db.add(user)
    db.flush()
    return user


def make_account(db, user, **kw):
    defaults = dict(
        uuid=uuid4(),
        user_id=user.db_id,
        account_name="Test Checking",
        account_type=AccountType.CHECKING,
        institution_name="Test Bank",
        balance=Decimal("0.00"),
    )
    defaults.update(kw)
    account = AccountDB(**defaults)
    db.add(account)
    db.flush()
    return account


def make_category(db, **kw):
    defaults = dict(
        uuid=uuid4(),
        name=f"Category {next(_category_seq)}",
    )
    defaults.update(kw)
    category = CategoryDB(**defaults)
    db.add(category)
    db.flush()
    return category


def make_transaction(db, user, account, **kw):
    defaults = dict(
        id=uuid4(),
        user_id=user.db_id,
        account_id=account.id,
        transaction_hash=uuid4().hex,
        source_type=SourceType.MANUAL,
        transaction_date=date(2026, 1, 1),
        amount=Decimal("10.00"),
        transaction_type=TransactionType.PURCHASE,
        description="Test transaction",
    )
    defaults.update(kw)
    txn = TransactionDB(**defaults)
    db.add(txn)
    db.flush()
    return txn


def make_investment_txn(db, user, account, **kw):
    defaults = dict(
        id=uuid4(),
        user_id=user.db_id,
        account_id=account.id,
        transaction_hash=uuid4().hex,
        transaction_type=InvestmentTransactionType.BUY,
        total_amount=Decimal("100.00"),
        transaction_date=date(2026, 1, 1),
    )
    defaults.update(kw)
    txn = InvestmentTransactionDB(**defaults)
    db.add(txn)
    db.flush()
    return txn
