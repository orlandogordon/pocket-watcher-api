"""Over-HTTP tests for the /transfers/* router.

Migrated from direct handler calls to the `client` fixture (Phase 2): every
case now exercises real routing + auth + serialization. The cascade regressions
delete a transaction through the actual DELETE /transactions/{uuid} endpoint
(rather than session.delete) and assert the dismissal / OFFSETS rows are gone —
a faithful over-HTTP version of the original DB-level checks.
"""
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.core import (
    DismissedTransferPairDB,
    InvestmentTransactionType,
    RelationshipType,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionTagDB,
    TransactionType,
    AccountType,
)
from src.services.system_tags import ensure_system_tags, get_system_tag
from tests.factories import make_account, make_investment_txn, make_transaction

pytestmark = pytest.mark.integration


def _accounts(db, user):
    checking = make_account(db, user, account_name="TD Checking", account_type=AccountType.CHECKING)
    amex = make_account(db, user, account_name="Amex Gold", account_type=AccountType.CREDIT_CARD,
                        institution_name="American Express")
    return checking, amex


def _make_pair(db, user, checking, amex, amount="100", out_date=date(2026, 2, 5), in_date=date(2026, 2, 4)):
    out = make_transaction(db, user, checking, amount=Decimal(amount),
                           transaction_type=TransactionType.TRANSFER_OUT,
                           transaction_date=out_date, description="ELECTRONICPMT AMEXEPAYMENT")
    in_ = make_transaction(db, user, amex, amount=Decimal(amount),
                           transaction_type=TransactionType.TRANSFER_IN,
                           transaction_date=in_date, description="AUTOPAY PAYMENT")
    return out, in_


def _offsets_count(db):
    return (
        db.query(TransactionRelationshipDB)
        .filter(TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS)
        .count()
    )


# ===== SUGGESTIONS =====

def test_suggestions_lists_pair(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    resp = client.get("/transfers/suggestions")
    assert resp.status_code == 200
    body = resp.json()
    assert len(body) == 1
    assert body[0]["out_side"]["id"] == str(out.id)
    assert body[0]["in_side"]["id"] == str(in_.id)


def test_suggestions_empty(client, db, test_user):
    _accounts(db, test_user)
    assert client.get("/transfers/suggestions").json() == []


def test_suggestions_unauthenticated_401(unauth_client):
    assert unauth_client.get("/transfers/suggestions").status_code == 401


# ===== CONFIRM =====

def test_confirm_creates_offsets(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    resp = client.post("/transfers/suggestions/confirm",
                       json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)})
    assert resp.status_code == 201
    assert "relationship_id" in resp.json()
    assert _offsets_count(db) == 1


def test_confirm_reclassify_updates_type_and_hash(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    # OUT side starts as PURCHASE; reclassify_from flips it to TRANSFER_OUT.
    out = make_transaction(db, test_user, checking, amount=Decimal("100"),
                           transaction_type=TransactionType.PURCHASE,
                           transaction_date=date(2026, 2, 5), description="AMEXEPAYMENT")
    in_ = make_transaction(db, test_user, amex, amount=Decimal("100"),
                           transaction_type=TransactionType.TRANSFER_IN,
                           transaction_date=date(2026, 2, 4), description="AUTOPAY")
    old_hash = out.transaction_hash

    resp = client.post("/transfers/suggestions/confirm", json={
        "from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id), "reclassify_from": True,
    })
    assert resp.status_code == 201
    db.refresh(out)
    assert out.transaction_type == TransactionType.TRANSFER_OUT
    assert out.transaction_hash != old_hash


def test_confirm_unknown_uuid_404(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, _ = _make_pair(db, test_user, checking, amex)
    resp = client.post("/transfers/suggestions/confirm",
                       json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(uuid4())})
    assert resp.status_code == 404


def test_confirm_409_already_linked(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    body = {"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)}
    assert client.post("/transfers/suggestions/confirm", json=body).status_code == 201
    assert client.post("/transfers/suggestions/confirm", json=body).status_code == 409


def test_reclassify_strips_needs_review_tag(client, db, test_user):
    ensure_system_tags(test_user.db_id, db)
    nr_tag = get_system_tag(test_user.db_id, db, "Needs Review")
    checking, amex = _accounts(db, test_user)
    out = make_transaction(db, test_user, checking, amount=Decimal("100"),
                           transaction_type=TransactionType.PURCHASE,
                           transaction_date=date(2026, 2, 5), description="AMEXEPAYMENT")
    in_ = make_transaction(db, test_user, amex, amount=Decimal("100"),
                           transaction_type=TransactionType.TRANSFER_IN,
                           transaction_date=date(2026, 2, 4), description="AUTOPAY")
    db.add(TransactionTagDB(transaction_id=out.db_id, tag_id=nr_tag.tag_id))
    db.flush()

    client.post("/transfers/suggestions/confirm", json={
        "from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id), "reclassify_from": True,
    })
    remaining = db.query(TransactionTagDB).filter(
        TransactionTagDB.transaction_id == out.db_id, TransactionTagDB.tag_id == nr_tag.tag_id,
    ).count()
    assert remaining == 0


def test_confirm_without_reclassify_preserves_tag(client, db, test_user):
    ensure_system_tags(test_user.db_id, db)
    nr_tag = get_system_tag(test_user.db_id, db, "Needs Review")
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    db.add(TransactionTagDB(transaction_id=out.db_id, tag_id=nr_tag.tag_id))
    db.flush()

    client.post("/transfers/suggestions/confirm",
                json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)})
    remaining = db.query(TransactionTagDB).filter(
        TransactionTagDB.transaction_id == out.db_id, TransactionTagDB.tag_id == nr_tag.tag_id,
    ).count()
    assert remaining == 1


def test_investment_side_confirm_does_not_crash(client, db, test_user):
    checking, _ = _accounts(db, test_user)
    schwab = make_account(db, test_user, account_name="Schwab", account_type=AccountType.INVESTMENT,
                          institution_name="Charles Schwab")
    out = make_transaction(db, test_user, checking, amount=Decimal("500"),
                           transaction_type=TransactionType.TRANSFER_OUT,
                           transaction_date=date(2026, 3, 10), description="SCHWAB MONEYLINK")
    inv_in = make_investment_txn(db, test_user, schwab, total_amount=Decimal("500"),
                                 transaction_type=InvestmentTransactionType.TRANSFER_IN,
                                 transaction_date=date(2026, 3, 9), description="MoneyLink deposit")
    resp = client.post("/transfers/suggestions/confirm",
                       json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(inv_in.id)})
    assert resp.status_code == 201
    assert "relationship_id" in resp.json()


# ===== DISMISS =====

def test_dismiss_persists_and_excludes(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    resp = client.post("/transfers/suggestions/dismiss",
                       json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)})
    assert resp.status_code == 201
    assert db.query(DismissedTransferPairDB).count() == 1
    assert client.get("/transfers/suggestions").json() == []


# ===== CASCADE ON DELETE (via DELETE /transactions/{uuid}) =====

def test_dismissal_cascades_on_transaction_delete(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    client.post("/transfers/suggestions/dismiss",
                json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)})
    assert db.query(DismissedTransferPairDB).count() == 1

    assert client.delete(f"/transactions/{out.id}").status_code == 204
    assert db.query(DismissedTransferPairDB).count() == 0


def test_offsets_cascades_on_transaction_delete(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    client.post("/transfers/suggestions/confirm",
                json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)})
    assert _offsets_count(db) == 1

    assert client.delete(f"/transactions/{out.id}").status_code == 204
    assert _offsets_count(db) == 0


def test_dismiss_delete_reimport_resurfaces(client, db, test_user):
    checking, amex = _accounts(db, test_user)
    out, in_ = _make_pair(db, test_user, checking, amex)
    client.post("/transfers/suggestions/dismiss",
                json={"from_transaction_uuid": str(out.id), "to_transaction_uuid": str(in_.id)})
    assert client.get("/transfers/suggestions").json() == []

    out_date, out_amount, out_desc = out.transaction_date, out.amount, out.description
    assert client.delete(f"/transactions/{out.id}").status_code == 204

    new_out = make_transaction(db, test_user, checking, amount=out_amount,
                               transaction_type=TransactionType.TRANSFER_OUT,
                               transaction_date=out_date, description=out_desc)
    suggestions = client.get("/transfers/suggestions").json()
    assert len(suggestions) == 1
    assert suggestions[0]["out_side"]["id"] == str(new_out.id)


# ===== ORPHANS =====

def test_orphan_with_no_partner(client, db, test_user):
    checking, _ = _accounts(db, test_user)
    out = make_transaction(db, test_user, checking, amount=Decimal("250"),
                           transaction_type=TransactionType.TRANSFER_OUT,
                           transaction_date=date(2026, 1, 1), description="LONELY TRANSFER")
    resp = client.get("/transfers/orphans")
    assert resp.status_code == 200
    assert [o["id"] for o in resp.json()] == [str(out.id)]
