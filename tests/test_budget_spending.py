"""Budget spending calc (crud_budget.calculate_category_spending).

Sums a category's in-month expense spending, scoped optionally to a subcategory,
netting refund/offset/reversal adjustments and adding split allocations to that
category (themselves refund-scaled). Income and out-of-month rows are excluded.
"""
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from src.crud.crud_budget import calculate_category_spending
from src.db.core import (
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


@pytest.fixture
def cat(db):
    return make_category(db)


def _purchase(db, user, account, amount, when, **kw):
    return make_transaction(db, user, account, amount=Decimal(amount),
                            transaction_type=TransactionType.PURCHASE, transaction_date=when, **kw)


def _refund(db, frm, to, amount):
    db.add(TransactionRelationshipDB(
        id=uuid4(), from_transaction_id=frm.db_id, to_transaction_id=to.db_id,
        relationship_type=RelationshipType.REFUNDS, amount_allocated=Decimal(amount),
    ))
    db.flush()


def test_sums_in_month_category_expenses(db, user, account, cat):
    _purchase(db, user, account, "40", date(2026, 1, 10), category_id=cat.id)
    _purchase(db, user, account, "60", date(2026, 1, 20), category_id=cat.id)
    # Excluded: income type, and an out-of-month expense.
    make_transaction(db, user, account, amount=Decimal("999"), transaction_type=TransactionType.CREDIT,
                     transaction_date=date(2026, 1, 15), category_id=cat.id)
    _purchase(db, user, account, "50", date(2026, 2, 1), category_id=cat.id)

    assert calculate_category_spending(db, user.db_id, 2026, 1, cat.id) == Decimal("100.00")


def test_subcategory_scopes_spending(db, user, account, cat):
    sub_x = make_category(db)
    sub_y = make_category(db)
    _purchase(db, user, account, "30", date(2026, 1, 10), category_id=cat.id, subcategory_id=sub_x.id)
    _purchase(db, user, account, "20", date(2026, 1, 11), category_id=cat.id, subcategory_id=sub_y.id)

    assert calculate_category_spending(db, user.db_id, 2026, 1, cat.id, sub_x.id) == Decimal("30")
    # No subcategory filter rolls both up under the parent.
    assert calculate_category_spending(db, user.db_id, 2026, 1, cat.id) == Decimal("50")


def test_refund_reduces_spending(db, user, account, cat):
    p = _purchase(db, user, account, "100", date(2026, 1, 10), category_id=cat.id)
    r = make_transaction(db, user, account, amount=Decimal("30"), transaction_type=TransactionType.CREDIT,
                         transaction_date=date(2026, 1, 12))
    _refund(db, r, p, "30")
    assert calculate_category_spending(db, user.db_id, 2026, 1, cat.id) == Decimal("70.00")


def test_split_allocation_added_to_direct_spending(db, user, account, cat):
    _purchase(db, user, account, "50", date(2026, 1, 10), category_id=cat.id)
    split_parent = _purchase(db, user, account, "100", date(2026, 1, 11), category_id=None)
    db.add(TransactionSplitAllocationDB(
        id=uuid4(), transaction_id=split_parent.db_id, category_id=cat.id, amount=Decimal("60")
    ))
    db.flush()
    # 50 direct + 60 split allocation to this category.
    assert calculate_category_spending(db, user.db_id, 2026, 1, cat.id) == Decimal("110")


def test_split_allocation_is_refund_scaled(db, user, account, cat):
    _purchase(db, user, account, "50", date(2026, 1, 10), category_id=cat.id)
    split_parent = _purchase(db, user, account, "100", date(2026, 1, 11), category_id=None)
    db.add(TransactionSplitAllocationDB(
        id=uuid4(), transaction_id=split_parent.db_id, category_id=cat.id, amount=Decimal("60")
    ))
    db.flush()
    r = make_transaction(db, user, account, amount=Decimal("50"), transaction_type=TransactionType.CREDIT,
                         transaction_date=date(2026, 1, 12))
    _refund(db, r, split_parent, "50")  # 50% of the split parent refunded
    # 50 direct + (60 * 0.5) scaled split = 80.
    assert calculate_category_spending(db, user.db_id, 2026, 1, cat.id) == Decimal("80.00")
