"""Over-HTTP tests for the /debt router.

Covers repayment plans (full CRUD — the PUT/DELETE that earlier API-gap notes
flagged as missing now exist), plan↔account links, monthly schedules, and debt
payments. Schedules require a LOAN account; payments require LOAN or
CREDIT_CARD. Plan responses expose their UUID under "id"; payment responses
under "uuid" (renamed from id for API consistency).
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.core import AccountType
from tests.factories import make_account

pytestmark = pytest.mark.integration


def _loan(db, user, name="Car Loan"):
    return make_account(db, user, account_name=name, account_type=AccountType.LOAN)


def _make_plan(client, name="Payoff", strategy="AVALANCHE"):
    resp = client.post("/debt/plans/", json={"plan_name": name, "strategy": strategy})
    assert resp.status_code == 201, resp.text
    return resp.json()


# ===== PLANS =====

def test_create_plan_201(client):
    plan = _make_plan(client, name="Debt Freedom", strategy="SNOWBALL")
    assert plan["plan_name"] == "Debt Freedom"
    assert plan["strategy"] == "SNOWBALL"
    assert plan["id"]


def test_create_duplicate_plan_409(client):
    _make_plan(client, name="OnlyOne")
    assert client.post("/debt/plans/", json={"plan_name": "OnlyOne"}).status_code == 409


def test_list_plans(client):
    _make_plan(client, name="P1")
    _make_plan(client, name="P2")
    names = {p["plan_name"] for p in client.get("/debt/plans/").json()}
    assert {"P1", "P2"} <= names


def test_get_plan_200_404_400(client):
    plan = _make_plan(client, name="Gettable")
    assert client.get(f"/debt/plans/{plan['id']}").status_code == 200
    assert client.get(f"/debt/plans/{uuid4()}").status_code == 404
    assert client.get("/debt/plans/not-a-uuid").status_code == 400


def test_update_plan_200(client):
    plan = _make_plan(client, name="Before")
    resp = client.put(f"/debt/plans/{plan['id']}", json={"plan_name": "After", "status": "PAUSED"})
    assert resp.status_code == 200
    assert resp.json()["plan_name"] == "After"
    assert resp.json()["status"] == "PAUSED"


def test_update_plan_unknown_404(client):
    assert client.put(f"/debt/plans/{uuid4()}", json={"plan_name": "X"}).status_code == 404


def test_delete_plan_204(client):
    plan = _make_plan(client, name="Doomed")
    assert client.delete(f"/debt/plans/{plan['id']}").status_code == 204
    assert client.get(f"/debt/plans/{plan['id']}").status_code == 404


def test_delete_plan_unknown_404(client):
    assert client.delete(f"/debt/plans/{uuid4()}").status_code == 404


def test_unauthenticated_401(unauth_client):
    assert unauth_client.get("/debt/plans/").status_code == 401


# ===== PLAN <-> ACCOUNT LINKS =====

def test_link_account_to_plan(client, db, test_user):
    plan = _make_plan(client)
    loan = _loan(db, test_user)
    resp = client.post("/debt/plans/accounts/", json={"plan_uuid": plan["id"], "account_uuid": str(loan.uuid)})
    assert resp.status_code == 201

    linked = client.get(f"/debt/plans/{plan['id']}/accounts/").json()
    assert [a["account_uuid"] for a in linked] == [str(loan.uuid)]


def test_link_unknown_plan_404(client, db, test_user):
    loan = _loan(db, test_user)
    resp = client.post("/debt/plans/accounts/", json={"plan_uuid": str(uuid4()), "account_uuid": str(loan.uuid)})
    assert resp.status_code == 404


def test_link_unknown_account_404(client):
    plan = _make_plan(client)
    resp = client.post("/debt/plans/accounts/", json={"plan_uuid": plan["id"], "account_uuid": str(uuid4())})
    assert resp.status_code == 404


def test_remove_account_from_plan(client, db, test_user):
    plan = _make_plan(client)
    loan = _loan(db, test_user)
    client.post("/debt/plans/accounts/", json={"plan_uuid": plan["id"], "account_uuid": str(loan.uuid)})
    resp = client.delete(f"/debt/plans/{plan['id']}/accounts/{loan.uuid}")
    assert resp.status_code == 204
    assert client.get(f"/debt/plans/{plan['id']}/accounts/").json() == []


def test_remove_unlinked_account_404(client, db, test_user):
    plan = _make_plan(client)
    loan = _loan(db, test_user)
    assert client.delete(f"/debt/plans/{plan['id']}/accounts/{loan.uuid}").status_code == 404


# ===== SCHEDULES =====

def test_create_and_read_schedule(client, db, test_user):
    loan = _loan(db, test_user)
    body = {
        "account_uuid": str(loan.uuid),
        "schedules": [
            {"payment_month": "2026-01-01", "scheduled_payment_amount": "300.00"},
            {"payment_month": "2026-02-01", "scheduled_payment_amount": "300.00"},
        ],
    }
    resp = client.post("/debt/schedules/", json=body)
    assert resp.status_code == 201

    rows = client.get(f"/debt/schedules/{loan.uuid}").json()
    assert len(rows) == 2
    assert all(Decimal(str(r["scheduled_payment_amount"])) == Decimal("300.00") for r in rows)


def test_create_schedule_unknown_account_404(client):
    body = {"account_uuid": str(uuid4()), "schedules": []}
    assert client.post("/debt/schedules/", json=body).status_code == 404


# ===== PAYMENTS =====

def _payment_body(loan_uuid, **overrides):
    body = {
        "loan_account_uuid": str(loan_uuid),
        "payment_amount": "250.00",
        "principal_amount": "200.00",
        "interest_amount": "50.00",
        "payment_date": "2026-01-15",
    }
    body.update(overrides)
    return body


def test_create_payment_201(client, db, test_user):
    loan = _loan(db, test_user)
    resp = client.post("/debt/payments/", json=_payment_body(loan.uuid))
    assert resp.status_code == 201
    body = resp.json()
    assert body["uuid"]
    assert Decimal(str(body["payment_amount"])) == Decimal("250.00")


def test_create_payment_unknown_loan_404(client):
    assert client.post("/debt/payments/", json=_payment_body(uuid4())).status_code == 404


def test_create_payment_wrong_account_type_400(client, db, test_user):
    checking = make_account(db, test_user, account_name="Chk", account_type=AccountType.CHECKING)
    resp = client.post("/debt/payments/", json=_payment_body(checking.uuid))
    assert resp.status_code == 400


def test_list_get_update_delete_payment(client, db, test_user):
    loan = _loan(db, test_user)
    created = client.post("/debt/payments/", json=_payment_body(loan.uuid)).json()

    listed = client.get(f"/debt/accounts/{loan.uuid}/payments/").json()
    assert len(listed) == 1

    assert client.get(f"/debt/payments/{created['uuid']}/").status_code == 200

    upd = client.put(f"/debt/payments/{created['uuid']}/", json={"payment_amount": "275.00"})
    assert upd.status_code == 200
    assert Decimal(str(upd.json()["payment_amount"])) == Decimal("275.00")

    assert client.delete(f"/debt/payments/{created['uuid']}/").status_code == 204
    assert client.get(f"/debt/payments/{created['uuid']}/").status_code == 404


def test_payment_get_unknown_404(client):
    assert client.get(f"/debt/payments/{uuid4()}/").status_code == 404


def test_bulk_upload_payments(client, db, test_user):
    loan = _loan(db, test_user)
    body = {"payments": [_payment_body(loan.uuid), _payment_body(loan.uuid, payment_date="2026-02-15")]}
    resp = client.post("/debt/payments/bulk-upload", json=body)
    assert resp.status_code == 201
    assert len(resp.json()) == 2
