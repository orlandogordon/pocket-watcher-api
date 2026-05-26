"""Over-HTTP tests for the /account-history router.

Note: despite an older API-gap note, this router uses `account_uuid` (string
UUID) path params, not integer ids. Tests use a CHECKING account so snapshot
creation stays in-memory (investment snapshots would reach the price fetcher).
A snapshot's value for a cash account is just its balance.
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.core import AccountType
from tests.factories import make_account

pytestmark = pytest.mark.integration


def _cash_account(db, user, balance="1000.00", name="Checking"):
    return make_account(db, user, account_name=name, account_type=AccountType.CHECKING,
                        balance=Decimal(balance))


# ===== SNAPSHOT CREATION =====

def test_create_account_snapshot_201(client, db, test_user):
    acct = _cash_account(db, test_user)
    resp = client.post(f"/account-history/snapshots/account/{acct.uuid}", params={"snapshot_date": "2026-01-15"})
    assert resp.status_code == 201
    body = resp.json()
    assert body["account_uuid"] == str(acct.uuid)
    assert Decimal(str(body["balance"])) == Decimal("1000.00")
    assert body["snapshot_uuid"]


def test_create_snapshot_unknown_account_404(client):
    assert client.post(f"/account-history/snapshots/account/{uuid4()}").status_code == 404


def test_create_snapshot_malformed_uuid_400(client):
    assert client.post("/account-history/snapshots/account/not-a-uuid").status_code == 400


def test_create_all_snapshots(client, db, test_user):
    _cash_account(db, test_user, name="A")
    _cash_account(db, test_user, name="B")
    resp = client.post("/account-history/snapshots/all", params={"snapshot_date": "2026-01-15"})
    assert resp.status_code == 201
    assert resp.json()["count"] >= 2


def test_unauthenticated_401(unauth_client):
    assert unauth_client.get("/account-history/net-worth").status_code == 401


# ===== HISTORY READS =====

def test_net_worth_history(client, db, test_user):
    acct = _cash_account(db, test_user)
    client.post(f"/account-history/snapshots/account/{acct.uuid}", params={"snapshot_date": "2026-01-15"})
    resp = client.get("/account-history/net-worth")
    assert resp.status_code == 200
    assert resp.json()["total_points"] >= 1


def test_account_value_history_200(client, db, test_user):
    acct = _cash_account(db, test_user)
    client.post(f"/account-history/snapshots/account/{acct.uuid}", params={"snapshot_date": "2026-01-15"})
    resp = client.get(f"/account-history/accounts/{acct.uuid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["account_uuid"] == str(acct.uuid)
    assert body["account_name"] == "Checking"


def test_account_value_history_unknown_404(client):
    assert client.get(f"/account-history/accounts/{uuid4()}").status_code == 404


def test_account_value_history_malformed_400(client):
    assert client.get("/account-history/accounts/not-a-uuid").status_code == 400


# ===== SNAPSHOT EDIT / DISMISS =====

def test_update_snapshot_200(client, db, test_user):
    acct = _cash_account(db, test_user)
    snap = client.post(f"/account-history/snapshots/account/{acct.uuid}", params={"snapshot_date": "2026-01-15"}).json()
    resp = client.put(
        f"/account-history/accounts/{acct.uuid}/snapshots/{snap['snapshot_uuid']}",
        json={"balance": "1234.00"},
    )
    assert resp.status_code == 200
    assert Decimal(str(resp.json()["balance"])) == Decimal("1234.00")


def test_update_snapshot_unknown_404(client, db, test_user):
    acct = _cash_account(db, test_user)
    resp = client.put(
        f"/account-history/accounts/{acct.uuid}/snapshots/{uuid4()}",
        json={"balance": "1.00"},
    )
    assert resp.status_code == 404


def test_update_snapshot_no_fields_400(client, db, test_user):
    acct = _cash_account(db, test_user)
    snap = client.post(f"/account-history/snapshots/account/{acct.uuid}", params={"snapshot_date": "2026-01-15"}).json()
    resp = client.put(f"/account-history/accounts/{acct.uuid}/snapshots/{snap['snapshot_uuid']}", json={})
    assert resp.status_code == 400


def test_dismiss_snapshot_reviews(client, db, test_user):
    acct = _cash_account(db, test_user)
    snap = client.post(f"/account-history/snapshots/account/{acct.uuid}", params={"snapshot_date": "2026-01-15"}).json()
    resp = client.post(
        f"/account-history/accounts/{acct.uuid}/snapshots/dismiss-review",
        json={"snapshot_uuids": [snap["snapshot_uuid"]], "reason": "looks fine"},
    )
    assert resp.status_code == 200
    assert "dismissed_count" in resp.json()
