"""Income/expense aggregation in get_transaction_stats.

Two behaviours covered here:

- **Transfers**: a transfer is real money crossing a single account's boundary,
  so when exactly one account is filtered it counts toward income/expense.
  Across all or multiple accounts a transfer is internal movement that nets to
  zero and stays excluded.
- **Liability interest (#69)**: INTEREST on a credit card / loan is a finance
  charge (expense); on an asset account it's interest earned (income).
"""
from decimal import Decimal

import pytest

from src.crud.crud_transaction import get_transaction_stats
from src.db.core import AccountType, TransactionType
from src.models.transaction import TransactionFilter
from tests.factories import make_account, make_transaction, make_user

pytestmark = pytest.mark.integration


@pytest.fixture
def user(db):
    return make_user(db)


@pytest.fixture
def account(db, user):
    return make_account(db, user)


def _txn(db, user, account, ttype, amount):
    return make_transaction(db, user, account, amount=Decimal(str(amount)),
                            transaction_type=ttype)


def _sample(db, user, account):
    _txn(db, user, account, TransactionType.CREDIT, 100)
    _txn(db, user, account, TransactionType.PURCHASE, 40)
    _txn(db, user, account, TransactionType.TRANSFER_IN, 500)
    _txn(db, user, account, TransactionType.TRANSFER_OUT, 200)


def test_single_account_folds_transfers_into_income_and_expense(db, user, account):
    _sample(db, user, account)
    stats = get_transaction_stats(db, user.db_id, TransactionFilter(account_ids=[account.db_id]))
    assert stats.total_income == Decimal("600")    # 100 credit + 500 transfer in
    assert stats.total_expenses == Decimal("240")  # 40 purchase + 200 transfer out
    assert stats.net == Decimal("360")
    assert stats.total_count == 4


def test_all_accounts_no_filter_excludes_transfers(db, user, account):
    _sample(db, user, account)
    stats = get_transaction_stats(db, user.db_id, None)
    assert stats.total_income == Decimal("100")
    assert stats.total_expenses == Decimal("40")
    assert stats.net == Decimal("60")
    assert stats.total_count == 4


def test_empty_account_filter_excludes_transfers(db, user, account):
    _sample(db, user, account)
    stats = get_transaction_stats(db, user.db_id, TransactionFilter())
    assert stats.total_income == Decimal("100")
    assert stats.total_expenses == Decimal("40")


def test_multiple_accounts_exclude_transfers(db, user, account):
    second = make_account(db, user, account_name="Second")
    _sample(db, user, account)
    stats = get_transaction_stats(db, user.db_id, TransactionFilter(account_ids=[account.db_id, second.db_id]))
    assert stats.total_income == Decimal("100")
    assert stats.total_expenses == Decimal("40")
    assert stats.net == Decimal("60")


def test_interest_is_expense_on_liability_account(db, user):
    card = make_account(db, user, account_name="Card", account_type=AccountType.CREDIT_CARD)
    _txn(db, user, card, TransactionType.PURCHASE, 40)
    _txn(db, user, card, TransactionType.INTEREST, 12.50)
    stats = get_transaction_stats(db, user.db_id, None)
    assert stats.total_expenses == Decimal("52.50")  # purchase + finance charge
    assert stats.total_income == Decimal("0.00")


def test_interest_is_income_on_asset_account(db, user, account):
    _txn(db, user, account, TransactionType.INTEREST, 3)
    stats = get_transaction_stats(db, user.db_id, None)
    assert stats.total_income == Decimal("3")
    assert stats.total_expenses == Decimal("0.00")
