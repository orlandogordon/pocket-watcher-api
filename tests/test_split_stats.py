"""Category-filtered split-allocation stats (crud_transaction.get_transaction_stats).

When a category filter is active, a split parent (category_id is None) must
contribute only its allocation *to the filtered category* — not its full
amount. And when that parent also carries a refund/offset, the allocation is
scaled by the same proportion the refund reduced the parent. This is the
get_transaction_stats branch at lines 931-963.
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from src.crud.crud_transaction import get_transaction_stats
from src.db.core import (
    RelationshipType,
    TransactionRelationshipDB,
    TransactionSplitAllocationDB,
    TransactionType,
)
from src.models.transaction import TransactionFilter
from tests.factories import make_account, make_category, make_transaction, make_user

pytestmark = pytest.mark.integration


@pytest.fixture
def user(db):
    return make_user(db)


@pytest.fixture
def account(db, user):
    return make_account(db, user)


@pytest.fixture
def cat_a(db):
    return make_category(db)


@pytest.fixture
def cat_b(db):
    return make_category(db)


def _alloc(db, txn, category, amount):
    a = TransactionSplitAllocationDB(
        uuid=uuid4(), transaction_id=txn.db_id, category_id=category.db_id, amount=Decimal(amount)
    )
    db.add(a)
    db.flush()
    return a


def _refund(db, frm, to, amount):
    r = TransactionRelationshipDB(
        uuid=uuid4(), from_transaction_id=frm.db_id, to_transaction_id=to.db_id,
        relationship_type=RelationshipType.REFUNDS, amount_allocated=Decimal(amount),
    )
    db.add(r)
    db.flush()
    return r


def test_split_parent_contributes_only_filtered_allocation(db, user, account, cat_a, cat_b):
    parent = make_transaction(db, user, account, amount=Decimal("100.00"),
                              transaction_type=TransactionType.PURCHASE, category_id=None)
    _alloc(db, parent, cat_a, "60.00")
    _alloc(db, parent, cat_b, "40.00")

    stats_a = get_transaction_stats(db, user.db_id, TransactionFilter(category_ids=[cat_a.db_id]))
    assert stats_a.total_count == 1
    assert stats_a.total_expenses == Decimal("60.00")

    stats_b = get_transaction_stats(db, user.db_id, TransactionFilter(category_ids=[cat_b.db_id]))
    assert stats_b.total_expenses == Decimal("40.00")


def test_refund_scales_filtered_split_allocation(db, user, account, cat_a, cat_b):
    parent = make_transaction(db, user, account, amount=Decimal("100.00"),
                              transaction_type=TransactionType.PURCHASE, category_id=None)
    _alloc(db, parent, cat_a, "60.00")
    _alloc(db, parent, cat_b, "40.00")
    refund = make_transaction(db, user, account, amount=Decimal("50.00"),
                              transaction_type=TransactionType.CREDIT)
    _refund(db, refund, parent, "50.00")  # 50% of the parent refunded

    stats = get_transaction_stats(db, user.db_id, TransactionFilter(category_ids=[cat_a.db_id]))
    # cat_a allocation 60 scaled by (1 - 50/100) = 30.
    assert stats.total_expenses == Decimal("30.00")


def test_income_split_uses_income_branch(db, user, account, cat_a, cat_b):
    parent = make_transaction(db, user, account, amount=Decimal("200.00"),
                              transaction_type=TransactionType.CREDIT, category_id=None)
    _alloc(db, parent, cat_a, "120.00")
    _alloc(db, parent, cat_b, "80.00")

    stats = get_transaction_stats(db, user.db_id, TransactionFilter(category_ids=[cat_a.db_id]))
    assert stats.total_income == Decimal("120.00")
    assert stats.total_expenses == Decimal("0.00")


def test_normally_categorized_txn_is_not_split_adjusted(db, user, account, cat_a):
    # A txn with a real category_id (not a split) must contribute its full amount
    # under the same category filter — the split block only touches category_id IS NULL rows.
    make_transaction(db, user, account, amount=Decimal("25.00"),
                     transaction_type=TransactionType.PURCHASE, category_id=cat_a.db_id)

    stats = get_transaction_stats(db, user.db_id, TransactionFilter(category_ids=[cat_a.db_id]))
    assert stats.total_expenses == Decimal("25.00")
