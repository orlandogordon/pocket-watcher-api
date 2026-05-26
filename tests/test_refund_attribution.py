"""Refund/offset/reversal attribution math (crud_transaction).

These are the relationship-aware adjustments where bugs hide: an absorbing
relationship (REFUNDS/OFFSETS/REVERSES) reduces the *original* transaction's
effective amount and removes the refund row itself from the totals. FEES_FOR is
the non-absorbing control. Covers the two pure helpers plus their integration
into get_transaction_stats (the part that nets refunds out of expenses/income).
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from src.crud.crud_transaction import (
    get_refund_adjustments,
    get_transaction_stats,
    validate_refund_allocation,
)
from src.db.core import RelationshipType, TransactionRelationshipDB, TransactionType
from src.models.transaction import TransactionFilter
from tests.factories import make_account, make_transaction, make_user

pytestmark = pytest.mark.integration


@pytest.fixture
def user(db):
    return make_user(db)


@pytest.fixture
def account(db, user):
    return make_account(db, user)


def _rel(db, from_txn, to_txn, rtype, amount):
    rel = TransactionRelationshipDB(
        id=uuid4(),
        from_transaction_id=from_txn.db_id,
        to_transaction_id=to_txn.db_id,
        relationship_type=rtype,
        amount_allocated=(Decimal(amount) if amount is not None else None),
    )
    db.add(rel)
    db.flush()
    return rel


# ===== get_refund_adjustments =====

def test_empty_ids_short_circuit(db, user):
    assert get_refund_adjustments(db, user.db_id, []) == ({}, set())


def test_single_refund_adjusts_original_and_absorbs_refund(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    refund = make_transaction(db, user, account, amount=Decimal("30.00"), transaction_type=TransactionType.CREDIT)
    _rel(db, refund, original, RelationshipType.REFUNDS, "30.00")

    adjustments, absorbed = get_refund_adjustments(db, user.db_id, [original.db_id, refund.db_id])
    assert adjustments == {original.db_id: Decimal("30.00")}
    assert absorbed == {refund.db_id}


def test_multiple_refunds_to_same_original_sum(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    r1 = make_transaction(db, user, account, amount=Decimal("30.00"), transaction_type=TransactionType.CREDIT)
    r2 = make_transaction(db, user, account, amount=Decimal("20.00"), transaction_type=TransactionType.CREDIT)
    _rel(db, r1, original, RelationshipType.REFUNDS, "30.00")
    _rel(db, r2, original, RelationshipType.OFFSETS, "20.00")

    adjustments, absorbed = get_refund_adjustments(db, user.db_id, [original.db_id])
    assert adjustments == {original.db_id: Decimal("50.00")}
    assert absorbed == {r1.db_id, r2.db_id}


def test_null_allocation_is_ignored(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    refund = make_transaction(db, user, account, amount=Decimal("30.00"), transaction_type=TransactionType.CREDIT)
    _rel(db, refund, original, RelationshipType.REFUNDS, None)

    adjustments, absorbed = get_refund_adjustments(db, user.db_id, [original.db_id, refund.db_id])
    assert adjustments == {}
    assert absorbed == set()


def test_non_absorbing_type_is_ignored(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    fee = make_transaction(db, user, account, amount=Decimal("5.00"), transaction_type=TransactionType.FEE)
    _rel(db, fee, original, RelationshipType.FEES_FOR, "5.00")

    adjustments, absorbed = get_refund_adjustments(db, user.db_id, [original.db_id, fee.db_id])
    assert adjustments == {}
    assert absorbed == set()


# ===== validate_refund_allocation =====

def test_validate_within_bounds_ok(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"))
    refund = make_transaction(db, user, account, amount=Decimal("30.00"))
    _rel(db, refund, original, RelationshipType.REFUNDS, "30.00")
    # 30 existing + 20 new = 50 <= 100 — no raise.
    validate_refund_allocation(db, original.db_id, Decimal("20.00"))


def test_validate_exceeding_raises(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"))
    refund = make_transaction(db, user, account, amount=Decimal("80.00"))
    _rel(db, refund, original, RelationshipType.REFUNDS, "80.00")
    with pytest.raises(ValueError):
        validate_refund_allocation(db, original.db_id, Decimal("30.00"))  # 110 > 100


def test_validate_excludes_relationship_being_updated(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"))
    refund = make_transaction(db, user, account, amount=Decimal("80.00"))
    rel = _rel(db, refund, original, RelationshipType.REFUNDS, "80.00")
    # Updating that same rel to 90 must not double-count its old 80.
    validate_refund_allocation(db, original.db_id, Decimal("90.00"), exclude_relationship_id=rel.relationship_id)


def test_validate_missing_original_is_silent(db):
    validate_refund_allocation(db, 999999, Decimal("10.00"))  # no raise


# ===== get_transaction_stats refund integration =====

def test_stats_partial_refund_reduces_expense(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    refund = make_transaction(db, user, account, amount=Decimal("30.00"), transaction_type=TransactionType.CREDIT)
    _rel(db, refund, original, RelationshipType.REFUNDS, "30.00")

    stats = get_transaction_stats(db, user.db_id)
    assert stats.total_count == 1  # refund row is absorbed, not counted
    assert stats.total_expenses == Decimal("70.00")
    assert stats.total_income == Decimal("0.00")
    assert stats.net == Decimal("-70.00")


def test_stats_full_refund_zeroes_expense(db, user, account):
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    refund = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.CREDIT)
    _rel(db, refund, original, RelationshipType.REFUNDS, "100.00")

    stats = get_transaction_stats(db, user.db_id)
    assert stats.total_expenses == Decimal("0.00")


def test_stats_over_allocation_clamps_to_zero(db, user, account):
    # validate_refund_allocation would block this, but stats must still clamp
    # defensively if a >original allocation ever lands in the DB.
    original = make_transaction(db, user, account, amount=Decimal("100.00"), transaction_type=TransactionType.PURCHASE)
    refund = make_transaction(db, user, account, amount=Decimal("120.00"), transaction_type=TransactionType.CREDIT)
    _rel(db, refund, original, RelationshipType.REFUNDS, "120.00")

    stats = get_transaction_stats(db, user.db_id)
    assert stats.total_expenses == Decimal("0.00")  # max(100 - 120, 0)
