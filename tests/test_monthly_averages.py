"""Monthly-average aggregation (crud_transaction.get_monthly_averages).

Covers the averaging divisor (distinct months with data, not 12), the income/
expense split, refund netting, the expenses-only category breakdown sorted by
total, split-allocation distribution into that breakdown, and the always-12
by_month series.
"""
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from src.crud.crud_transaction import get_monthly_averages
from src.db.core import (
    AccountType,
    RelationshipType,
    TransactionRelationshipDB,
    TransactionSplitAllocationDB,
    TransactionType,
)
from tests.factories import make_account, make_category, make_transaction, make_user

pytestmark = pytest.mark.integration


@pytest.fixture
def user(db):
    return make_user(db)


@pytest.fixture
def account(db, user):
    return make_account(db, user)


def _txn(db, user, account, amount, ttype, when, **kw):
    return make_transaction(db, user, account, amount=Decimal(amount),
                            transaction_type=ttype, transaction_date=when, **kw)


def test_average_divides_by_distinct_months_with_data(db, user, account):
    _txn(db, user, account, "100", TransactionType.PURCHASE, date(2026, 1, 5))
    _txn(db, user, account, "200", TransactionType.PURCHASE, date(2026, 2, 5))
    _txn(db, user, account, "300", TransactionType.PURCHASE, date(2026, 3, 5))

    res = get_monthly_averages(db, user.db_id, 2026)
    assert res.months_with_data == 3
    assert res.totals.total_expenses == Decimal("600.00")
    assert res.totals.avg_monthly_expenses == Decimal("200.00")  # 600 / 3, not / 12


def test_income_and_expense_net(db, user, account):
    _txn(db, user, account, "1000", TransactionType.CREDIT, date(2026, 1, 5))
    _txn(db, user, account, "400", TransactionType.PURCHASE, date(2026, 1, 6))

    res = get_monthly_averages(db, user.db_id, 2026)
    assert res.months_with_data == 1
    assert res.totals.total_income == Decimal("1000.00")
    assert res.totals.total_expenses == Decimal("400.00")
    assert res.totals.total_net == Decimal("600.00")
    assert res.totals.avg_monthly_net == Decimal("600.00")


def test_by_month_always_twelve_entries(db, user, account):
    _txn(db, user, account, "100", TransactionType.PURCHASE, date(2026, 1, 5))
    res = get_monthly_averages(db, user.db_id, 2026)
    assert len(res.by_month) == 12
    jan = next(m for m in res.by_month if m.month == "2026-01")
    assert jan.expenses == Decimal("100.00")
    dec = next(m for m in res.by_month if m.month == "2026-12")
    assert dec.expenses == Decimal("0.00")


def test_category_breakdown_sorted_by_total_desc(db, user, account):
    cat_a = make_category(db)
    cat_b = make_category(db)
    _txn(db, user, account, "100", TransactionType.PURCHASE, date(2026, 1, 5), category_id=cat_a.db_id)
    _txn(db, user, account, "250", TransactionType.PURCHASE, date(2026, 1, 6), category_id=cat_b.db_id)

    res = get_monthly_averages(db, user.db_id, 2026)
    assert [c.total for c in res.by_category] == [Decimal("250.00"), Decimal("100.00")]
    assert res.by_category[0].category_uuid == cat_b.uuid
    # months_with_data == 1, so monthly_average equals total.
    assert res.by_category[0].monthly_average == Decimal("250.00")


def test_refund_reduces_monthly_expense_and_absorbs_refund(db, user, account):
    parent = _txn(db, user, account, "100", TransactionType.PURCHASE, date(2026, 1, 5))
    refund = _txn(db, user, account, "40", TransactionType.CREDIT, date(2026, 1, 6))
    db.add(TransactionRelationshipDB(
        uuid=uuid4(), from_transaction_id=refund.db_id, to_transaction_id=parent.db_id,
        relationship_type=RelationshipType.REFUNDS, amount_allocated=Decimal("40"),
    ))
    db.flush()

    res = get_monthly_averages(db, user.db_id, 2026)
    assert res.totals.total_expenses == Decimal("60.00")  # 100 - 40
    assert res.totals.total_income == Decimal("0.00")      # refund row absorbed, not income


def test_split_distributed_across_category_breakdown(db, user, account):
    cat_a = make_category(db)
    cat_b = make_category(db)
    parent = _txn(db, user, account, "100", TransactionType.PURCHASE, date(2026, 1, 5), category_id=None)
    for cat, amt in ((cat_a, "60"), (cat_b, "40")):
        db.add(TransactionSplitAllocationDB(
            uuid=uuid4(), transaction_id=parent.db_id, category_id=cat.db_id, amount=Decimal(amt)
        ))
    db.flush()

    res = get_monthly_averages(db, user.db_id, 2026)
    totals = {c.category_uuid: c.total for c in res.by_category}
    assert totals[cat_a.uuid] == Decimal("60.00")
    assert totals[cat_b.uuid] == Decimal("40.00")


def test_liability_interest_is_expense_asset_interest_is_income(db, user, account):
    card = make_account(db, user, account_name="Card", account_type=AccountType.CREDIT_CARD)
    _txn(db, user, card, "12.50", TransactionType.INTEREST, date(2026, 1, 5))   # finance charge
    _txn(db, user, account, "3.00", TransactionType.INTEREST, date(2026, 1, 6))  # interest earned

    res = get_monthly_averages(db, user.db_id, 2026)
    assert res.totals.total_expenses == Decimal("12.50")
    assert res.totals.total_income == Decimal("3.00")


def test_single_month_query_scopes_to_that_month(db, user, account):
    _txn(db, user, account, "100", TransactionType.PURCHASE, date(2026, 1, 5))
    _txn(db, user, account, "999", TransactionType.PURCHASE, date(2026, 2, 5))

    res = get_monthly_averages(db, user.db_id, 2026, month=1)
    assert res.months_with_data == 1
    assert res.totals.total_expenses == Decimal("100.00")
