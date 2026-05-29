"""Investment-transaction duplicate analysis (services/duplicate_analyzer).

The regular-transaction half is covered by test_upload_dedup_roundtrip.py; this
pins the investment half: unmapped types are rejected, DB-hash matches are
rejected as 'database' duplicates, within-statement repeats flow to
ready_to_import flagged (so the user decides per-row), and the account_id guard
holds (it is part of the dedup hash).
"""
from datetime import date
from decimal import Decimal

import pytest

from src.crud.crud_investment import generate_investment_transaction_hash
from src.db.core import InvestmentTransactionType
from src.parser.models import ParsedInvestmentTransaction
from src.services.duplicate_analyzer import analyze_investment_transactions
from tests.factories import make_account, make_investment_txn, make_user

pytestmark = pytest.mark.integration


@pytest.fixture
def user(db):
    return make_user(db)


@pytest.fixture
def account(db, user):
    return make_account(db, user)


def _parsed(txn_type="BUY", symbol="AAPL", qty="10", price="150", total="-1500", when=date(2026, 1, 1)):
    return ParsedInvestmentTransaction(
        transaction_date=when,
        transaction_type=txn_type,
        symbol=symbol,
        api_symbol=None,
        description=f"{txn_type} {symbol}",
        quantity=Decimal(qty) if qty is not None else None,
        price_per_share=Decimal(price) if price is not None else None,
        total_amount=Decimal(total),
    )


def test_empty_returns_empty(db, user, account):
    assert analyze_investment_transactions([], user.db_id, account.db_id, db) == ([], [])


def test_none_account_raises(db, user):
    with pytest.raises(ValueError):
        analyze_investment_transactions([_parsed()], user.db_id, None, db)


def test_unique_goes_to_ready(db, user, account):
    rejected, ready = analyze_investment_transactions([_parsed()], user.db_id, account.db_id, db)
    assert rejected == []
    assert len(ready) == 1
    assert ready[0]["is_duplicate"] is False
    assert ready[0]["transaction_kind"] == "investment"


def test_unmapped_type_is_rejected(db, user, account):
    rejected, ready = analyze_investment_transactions(
        [_parsed(txn_type="MYSTERY")], user.db_id, account.db_id, db
    )
    assert ready == []
    assert len(rejected) == 1
    assert rejected[0]["duplicate_type"] == "unmapped_type"
    assert rejected[0]["is_duplicate"] is False


def test_db_duplicate_is_rejected(db, user, account):
    parsed = _parsed()
    existing_hash = generate_investment_transaction_hash(parsed, user.db_id, account.db_id)
    make_investment_txn(
        db, user, account,
        transaction_hash=existing_hash,
        transaction_type=InvestmentTransactionType.BUY,
        symbol="AAPL", quantity=Decimal("10"), price_per_share=Decimal("150"),
        total_amount=Decimal("-1500"), transaction_date=date(2026, 1, 1),
    )

    rejected, ready = analyze_investment_transactions([parsed], user.db_id, account.db_id, db)
    assert ready == []
    assert len(rejected) == 1
    assert rejected[0]["is_duplicate"] is True
    assert rejected[0]["duplicate_type"] == "database"


def test_within_statement_duplicate_flows_to_ready_flagged(db, user, account):
    parsed = _parsed()
    rejected, ready = analyze_investment_transactions([parsed, parsed], user.db_id, account.db_id, db)
    # Policy: only DB matches auto-reject; an in-statement repeat ships to
    # ready_to_import flagged so the user decides per-row.
    assert rejected == []
    assert len(ready) == 2
    assert ready[0]["is_duplicate"] is False
    assert ready[1]["is_duplicate"] is True
    assert ready[1]["duplicate_type"] == "within_statement"


def test_different_account_same_data_not_flagged(db, user):
    a1 = make_account(db, user, account_name="Brokerage 1")
    a2 = make_account(db, user, account_name="Brokerage 2")
    parsed = _parsed()
    existing_hash = generate_investment_transaction_hash(parsed, user.db_id, a1.db_id)
    make_investment_txn(
        db, user, a1, transaction_hash=existing_hash,
        transaction_type=InvestmentTransactionType.BUY, total_amount=Decimal("-1500"),
        transaction_date=date(2026, 1, 1),
    )
    # Same parsed row analyzed against the OTHER account must not collide.
    rejected, ready = analyze_investment_transactions([parsed], user.db_id, a2.db_id, db)
    assert rejected == []
    assert len(ready) == 1
