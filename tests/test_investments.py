"""Over-HTTP tests for the /investments router.

Covers investment transactions (CRUD + bulk), the derived holdings (rebuilt
from transactions on write), and the account summary. `refresh-prices` is
intentionally not exercised here — it reaches yfinance and belongs to the
mocked-services phase. Investment transactions live on INVESTMENT-type accounts.
"""
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.core import AccountType
from tests.factories import make_account, make_user

pytestmark = pytest.mark.integration


def _inv_account(db, user):
    return make_account(db, user, account_name="Brokerage", account_type=AccountType.INVESTMENT)


def _buy(account_uuid, **overrides):
    payload = {
        "account_uuid": str(account_uuid),
        "transaction_type": "BUY",
        "symbol": "AAPL",
        "quantity": "10",
        "price_per_share": "150.00",
        "total_amount": "1500.00",
        "transaction_date": "2026-01-15",
    }
    payload.update(overrides)
    return payload


# ===== TRANSACTIONS =====

def test_create_investment_transaction_201(client, db, test_user):
    acct = _inv_account(db, test_user)
    resp = client.post("/investments/transactions/", json=_buy(acct.uuid))
    assert resp.status_code == 201
    body = resp.json()
    assert body["symbol"] == "AAPL"
    assert body["account_uuid"] == str(acct.uuid)
    assert Decimal(str(body["total_amount"])) == Decimal("1500.00")


def test_create_unknown_account_404(client):
    assert client.post("/investments/transactions/", json=_buy(uuid4())).status_code == 404


def test_create_missing_total_amount_422(client, db, test_user):
    acct = _inv_account(db, test_user)
    payload = _buy(acct.uuid)
    del payload["total_amount"]
    assert client.post("/investments/transactions/", json=payload).status_code == 422


def test_read_transaction_200_404_400(client, db, test_user):
    acct = _inv_account(db, test_user)
    created = client.post("/investments/transactions/", json=_buy(acct.uuid)).json()
    assert client.get(f"/investments/transactions/{created['id']}").status_code == 200
    assert client.get(f"/investments/transactions/{uuid4()}").status_code == 404
    assert client.get("/investments/transactions/not-a-uuid").status_code == 400


def test_update_transaction_200(client, db, test_user):
    acct = _inv_account(db, test_user)
    created = client.post("/investments/transactions/", json=_buy(acct.uuid)).json()
    resp = client.put(f"/investments/transactions/{created['id']}", json={"quantity": "5"})
    assert resp.status_code == 200
    assert Decimal(str(resp.json()["quantity"])) == Decimal("5")


def test_update_unknown_404(client):
    assert client.put(f"/investments/transactions/{uuid4()}", json={"quantity": "1"}).status_code == 404


def test_delete_transaction_204(client, db, test_user):
    acct = _inv_account(db, test_user)
    created = client.post("/investments/transactions/", json=_buy(acct.uuid)).json()
    assert client.delete(f"/investments/transactions/{created['id']}").status_code == 204
    assert client.get(f"/investments/transactions/{created['id']}").status_code == 404


def test_list_transactions_for_account(client, db, test_user):
    acct = _inv_account(db, test_user)
    client.post("/investments/transactions/", json=_buy(acct.uuid))
    client.post("/investments/transactions/", json=_buy(acct.uuid, symbol="MSFT", transaction_date="2026-02-01"))
    txns = client.get(f"/investments/accounts/{acct.uuid}/transactions/").json()
    assert {t["symbol"] for t in txns} == {"AAPL", "MSFT"}


def test_list_transactions_unknown_account_404(client):
    assert client.get(f"/investments/accounts/{uuid4()}/transactions/").status_code == 404


def test_bulk_upload_transactions(client, db, test_user):
    acct = _inv_account(db, test_user)
    body = {"transactions": [_buy(acct.uuid), _buy(acct.uuid, symbol="MSFT", transaction_date="2026-02-01")]}
    resp = client.post("/investments/transactions/bulk-upload", json=body)
    assert resp.status_code == 201
    assert len(resp.json()) == 2


def test_unauthenticated_401(unauth_client, db, test_user):
    acct = _inv_account(db, test_user)
    assert unauth_client.get(f"/investments/accounts/{acct.uuid}/transactions/").status_code == 401


# ===== HOLDINGS (derived) =====

def test_buy_creates_holding(client, db, test_user):
    acct = _inv_account(db, test_user)
    client.post("/investments/transactions/", json=_buy(acct.uuid))
    holdings = client.get(f"/investments/accounts/{acct.uuid}/holdings/").json()
    assert len(holdings) == 1
    assert holdings[0]["symbol"] == "AAPL"
    assert holdings[0]["account_uuid"] == str(acct.uuid)


def test_read_holding_200_and_404(client, db, test_user):
    acct = _inv_account(db, test_user)
    client.post("/investments/transactions/", json=_buy(acct.uuid))
    holding = client.get(f"/investments/accounts/{acct.uuid}/holdings/").json()[0]
    assert client.get(f"/investments/holdings/{holding['id']}").status_code == 200
    assert client.get(f"/investments/holdings/{uuid4()}").status_code == 404


def test_rebuild_holdings(client, db, test_user):
    acct = _inv_account(db, test_user)
    client.post("/investments/transactions/", json=_buy(acct.uuid))
    resp = client.post(f"/investments/accounts/{acct.uuid}/holdings/rebuild")
    assert resp.status_code == 200
    assert any(h["symbol"] == "AAPL" for h in resp.json())


def test_holdings_unknown_account_404(client):
    assert client.get(f"/investments/accounts/{uuid4()}/holdings/").status_code == 404


# ===== ACCOUNT SUMMARY =====

def test_account_summary_empty(client, db, test_user):
    acct = _inv_account(db, test_user)
    body = client.get(f"/investments/accounts/{acct.uuid}/summary").json()
    assert Decimal(str(body["securities_value"])) == Decimal("0")
    assert "total_value" in body


def test_account_summary_unknown_404(client):
    assert client.get(f"/investments/accounts/{uuid4()}/summary").status_code == 404
