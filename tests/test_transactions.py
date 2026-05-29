"""Over-HTTP tests for the /transactions router.

Notable here vs. accounts: these handlers authenticate via the bare
`current_user_id()` contextvar (not `Depends`), so they exercise the async
get_db contextvar bridge in the `client` fixture. Domain rules pinned below:
the single-account transfer stats rule, split allocation sum/validation, and
refund relationships.
"""
from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.core import AccountType, TransactionType
from tests.factories import make_account, make_category, make_transaction, make_user

pytestmark = pytest.mark.integration


def _payload(account_uuid, **overrides):
    payload = {
        "account_uuid": str(account_uuid),
        "transaction_date": "2026-02-01",
        "amount": "25.00",
        "transaction_type": "PURCHASE",
        "description": "Coffee",
    }
    payload.update(overrides)
    return payload


# ===== CREATE =====

def test_create_transaction_201(client, db, test_user):
    acct = make_account(db, test_user)
    resp = client.post("/transactions/", json=_payload(acct.uuid))
    assert resp.status_code == 201
    body = resp.json()
    assert body["account_uuid"] == str(acct.uuid)
    assert Decimal(str(body["amount"])) == Decimal("25.00")
    assert body["transaction_type"] == "PURCHASE"


def test_create_resolves_category(client, db, test_user):
    acct = make_account(db, test_user)
    cat = make_category(db, name="Groceries")
    resp = client.post("/transactions/", json=_payload(acct.uuid, category_uuid=str(cat.uuid)))
    assert resp.status_code == 201
    # CategoryResponse serializes its UUID under "id" (validation_alias="uuid").
    assert resp.json()["category"]["id"] == str(cat.uuid)


def test_create_unknown_account_404(client):
    assert client.post("/transactions/", json=_payload(uuid4())).status_code == 404


def test_create_unknown_category_404(client, db, test_user):
    acct = make_account(db, test_user)
    resp = client.post("/transactions/", json=_payload(acct.uuid, category_uuid=str(uuid4())))
    assert resp.status_code == 404


def test_create_missing_amount_422(client, db, test_user):
    acct = make_account(db, test_user)
    payload = _payload(acct.uuid)
    del payload["amount"]
    assert client.post("/transactions/", json=payload).status_code == 422


def test_create_unauthenticated_401(unauth_client, db, test_user):
    acct = make_account(db, test_user)
    assert unauth_client.post("/transactions/", json=_payload(acct.uuid)).status_code == 401


# ===== LIST / FILTER =====

def test_list_empty(client):
    resp = client.get("/transactions/")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_only_current_user(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, description="Mine")
    other = make_user(db, email="o@x.com", username="o")
    other_acct = make_account(db, other, account_name="Theirs")
    make_transaction(db, other, other_acct, description="Theirs")

    descs = [t["description"] for t in client.get("/transactions/").json()]
    assert descs == ["Mine"]


def test_list_filter_by_account(client, db, test_user):
    a1 = make_account(db, test_user, account_name="A1")
    a2 = make_account(db, test_user, account_name="A2")
    make_transaction(db, test_user, a1, description="on a1")
    make_transaction(db, test_user, a2, description="on a2")

    resp = client.get("/transactions/", params={"account_uuid": str(a1.uuid)})
    assert [t["description"] for t in resp.json()] == ["on a1"]


def test_list_filter_by_type(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE, description="buy")
    make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT, description="refund")

    resp = client.get("/transactions/", params={"transaction_type": "CREDIT"})
    assert [t["description"] for t in resp.json()] == ["refund"]


def test_list_filter_by_date_range(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, transaction_date=date(2026, 1, 1), description="jan")
    make_transaction(db, test_user, acct, transaction_date=date(2026, 3, 1), description="mar")

    resp = client.get("/transactions/", params={"date_from": "2026-02-01", "date_to": "2026-12-31"})
    assert [t["description"] for t in resp.json()] == ["mar"]


def test_list_filter_by_amount_range(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, amount=Decimal("5.00"), description="small")
    make_transaction(db, test_user, acct, amount=Decimal("500.00"), description="big")

    resp = client.get("/transactions/", params={"amount_min": "100"})
    assert [t["description"] for t in resp.json()] == ["big"]


def test_list_filter_by_description_search(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, description="WHOLEFOODS MARKET")
    make_transaction(db, test_user, acct, description="SHELL GAS")

    resp = client.get("/transactions/", params={"description_search": "wholefoods"})
    assert [t["description"] for t in resp.json()] == ["WHOLEFOODS MARKET"]


def test_list_pagination(client, db, test_user):
    acct = make_account(db, test_user)
    for i in range(3):
        make_transaction(db, test_user, acct, transaction_date=date(2026, 1, i + 1))
    assert len(client.get("/transactions/", params={"limit": 2}).json()) == 2
    assert len(client.get("/transactions/", params={"skip": 2}).json()) == 1


def test_list_filter_unknown_account_404(client):
    assert client.get("/transactions/", params={"account_uuid": str(uuid4())}).status_code == 404


def test_list_unauthenticated_401(unauth_client):
    assert unauth_client.get("/transactions/").status_code == 401


# ===== GET / UPDATE / DELETE =====

def test_get_transaction_200(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct, description="Findable")
    resp = client.get(f"/transactions/{txn.uuid}")
    assert resp.status_code == 200
    assert resp.json()["description"] == "Findable"


def test_get_unknown_404(client):
    assert client.get(f"/transactions/{uuid4()}").status_code == 404


def test_get_malformed_uuid_400(client):
    assert client.get("/transactions/not-a-uuid").status_code == 400


def test_get_cross_user_404(client, db):
    other = make_user(db, email="cu@x.com", username="cu")
    acct = make_account(db, other, account_name="Theirs")
    txn = make_transaction(db, other, acct)
    assert client.get(f"/transactions/{txn.uuid}").status_code == 404


def test_update_transaction_200(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct, amount=Decimal("10.00"), description="old")
    resp = client.put(f"/transactions/{txn.uuid}", json={"amount": "99.99", "description": "new"})
    assert resp.status_code == 200
    body = resp.json()
    assert Decimal(str(body["amount"])) == Decimal("99.99")
    assert body["description"] == "new"


def test_update_clear_category(client, db, test_user):
    acct = make_account(db, test_user)
    cat = make_category(db, name="ToClear")
    txn = make_transaction(db, test_user, acct, category_id=cat.db_id)
    # Sending null explicitly clears the category (model_fields_set distinguishes it).
    resp = client.put(f"/transactions/{txn.uuid}", json={"category_uuid": None})
    assert resp.status_code == 200
    assert resp.json()["category"] is None


def test_update_unknown_404(client):
    assert client.put(f"/transactions/{uuid4()}", json={"description": "x"}).status_code == 404


def test_delete_transaction_204(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct)
    assert client.delete(f"/transactions/{txn.uuid}").status_code == 204
    assert client.get(f"/transactions/{txn.uuid}").status_code == 404


def test_delete_unknown_404(client):
    assert client.delete(f"/transactions/{uuid4()}").status_code == 404


# ===== STATS (incl. the single-account transfer rule) =====

def test_stats_income_expense_net(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT, amount=Decimal("100.00"))
    make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE, amount=Decimal("30.00"))

    body = client.get("/transactions/stats").json()
    assert body["total_count"] == 2
    assert Decimal(str(body["total_income"])) == Decimal("100.00")
    assert Decimal(str(body["total_expenses"])) == Decimal("30.00")
    assert Decimal(str(body["net"])) == Decimal("70.00")


def test_stats_transfer_excluded_across_all_accounts(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE, amount=Decimal("30.00"))
    make_transaction(db, test_user, acct, transaction_type=TransactionType.TRANSFER_OUT, amount=Decimal("100.00"))

    # No account filter → transfer is internal movement, excluded from expenses.
    body = client.get("/transactions/stats").json()
    assert Decimal(str(body["total_expenses"])) == Decimal("30.00")


def test_stats_transfer_counted_for_single_account(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE, amount=Decimal("30.00"))
    make_transaction(db, test_user, acct, transaction_type=TransactionType.TRANSFER_OUT, amount=Decimal("100.00"))

    # Single-account scope → transfer crosses the boundary, counts as expense.
    body = client.get("/transactions/stats", params={"account_uuid": str(acct.uuid)}).json()
    assert Decimal(str(body["total_expenses"])) == Decimal("130.00")


def test_stats_unauthenticated_401(unauth_client):
    assert unauth_client.get("/transactions/stats").status_code == 401


# ===== SPLITS =====

def _split_body(cat1, cat2, a1="60.00", a2="40.00"):
    return {
        "allocations": [
            {"category_uuid": str(cat1.uuid), "amount": a1},
            {"category_uuid": str(cat2.uuid), "amount": a2},
        ]
    }


def test_set_and_get_splits(client, db, test_user):
    acct = make_account(db, test_user)
    cat1, cat2 = make_category(db, name="Food"), make_category(db, name="Tip")
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))

    resp = client.put(f"/transactions/{txn.uuid}/splits", json=_split_body(cat1, cat2))
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["split_allocations"]) == 2
    assert body["category"] is None  # single-category assignment cleared

    got = client.get(f"/transactions/{txn.uuid}/splits").json()
    assert {a["category_name"] for a in got} == {"Food", "Tip"}


def test_set_splits_sum_mismatch_400(client, db, test_user):
    acct = make_account(db, test_user)
    cat1, cat2 = make_category(db, name="A"), make_category(db, name="B")
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))
    resp = client.put(f"/transactions/{txn.uuid}/splits", json=_split_body(cat1, cat2, "60.00", "30.00"))
    assert resp.status_code == 400
    assert "must equal" in resp.json()["detail"]


def test_set_splits_single_allocation_422(client, db, test_user):
    acct = make_account(db, test_user)
    cat = make_category(db, name="Solo")
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))
    body = {"allocations": [{"category_uuid": str(cat.uuid), "amount": "100.00"}]}
    assert client.put(f"/transactions/{txn.uuid}/splits", json=body).status_code == 422


def test_get_splits_unknown_txn_404(client):
    assert client.get(f"/transactions/{uuid4()}/splits").status_code == 404


def test_delete_splits_204(client, db, test_user):
    acct = make_account(db, test_user)
    cat1, cat2 = make_category(db, name="X"), make_category(db, name="Y")
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))
    client.put(f"/transactions/{txn.uuid}/splits", json=_split_body(cat1, cat2))
    assert client.delete(f"/transactions/{txn.uuid}/splits").status_code == 204
    assert client.get(f"/transactions/{txn.uuid}/splits").json() == []


# ===== RELATIONSHIPS =====

def test_create_and_list_refund_relationship(client, db, test_user):
    acct = make_account(db, test_user)
    purchase = make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE, amount=Decimal("50.00"))
    refund = make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT, amount=Decimal("50.00"))

    resp = client.post(
        f"/transactions/{refund.uuid}/relationships",
        json={"to_transaction_uuid": str(purchase.uuid), "relationship_type": "REFUNDS"},
    )
    assert resp.status_code == 201
    rel = resp.json()
    assert rel["relationship_type"] == "REFUNDS"

    listed = client.get(f"/transactions/{refund.uuid}/relationships").json()
    assert len(listed) == 1
    assert listed[0]["id"] == rel["id"]


def test_create_relationship_unknown_target_404(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct)
    resp = client.post(
        f"/transactions/{txn.uuid}/relationships",
        json={"to_transaction_uuid": str(uuid4()), "relationship_type": "REFUNDS"},
    )
    assert resp.status_code == 404


def test_delete_relationship_204(client, db, test_user):
    acct = make_account(db, test_user)
    a = make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE)
    b = make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT)
    rel = client.post(
        f"/transactions/{b.uuid}/relationships",
        json={"to_transaction_uuid": str(a.uuid), "relationship_type": "REFUNDS"},
    ).json()
    assert client.delete(f"/transactions/relationships/{rel['id']}").status_code == 204


# ===== BULK UPDATE =====

def test_bulk_update_category(client, db, test_user):
    acct = make_account(db, test_user)
    cat = make_category(db, name="Reassigned")
    t1 = make_transaction(db, test_user, acct)
    t2 = make_transaction(db, test_user, acct)

    resp = client.patch(
        "/transactions/bulk-update",
        json={"transaction_uuids": [str(t1.uuid), str(t2.uuid)], "category_uuid": str(cat.uuid)},
    )
    assert resp.status_code == 200
    assert client.get(f"/transactions/{t1.uuid}").json()["category"]["id"] == str(cat.uuid)


def test_bulk_update_no_fields_400(client, db, test_user):
    acct = make_account(db, test_user)
    t1 = make_transaction(db, test_user, acct)
    resp = client.patch("/transactions/bulk-update", json={"transaction_uuids": [str(t1.uuid)]})
    assert resp.status_code == 400


def test_update_reassigns_account(client, db, test_user):
    a1 = make_account(db, test_user, account_name="From")
    a2 = make_account(db, test_user, account_name="To")
    txn = make_transaction(db, test_user, a1)
    resp = client.put(f"/transactions/{txn.uuid}", json={"account_uuid": str(a2.uuid)})
    assert resp.status_code == 200
    assert resp.json()["account_uuid"] == str(a2.uuid)


# ===== BULK UPLOAD =====

def test_bulk_upload_creates_transactions(client, db, test_user):
    acct = make_account(db, test_user)
    body = {
        "account_uuid": str(acct.uuid),
        "transactions": [
            _payload(acct.uuid, amount="10.00", description="one"),
            _payload(acct.uuid, amount="20.00", description="two", transaction_date="2026-02-02"),
        ],
    }
    resp = client.post("/transactions/bulk-upload/", json=body)
    assert resp.status_code == 201
    assert len(resp.json()) == 2


def test_bulk_upload_unknown_account_404(client):
    body = {"account_uuid": str(uuid4()), "transactions": []}
    assert client.post("/transactions/bulk-upload/", json=body).status_code == 404


# ===== MONTHLY AVERAGES =====

def test_monthly_averages_single_month(client, db, test_user):
    acct = make_account(db, test_user)
    make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT,
                     amount=Decimal("1200.00"), transaction_date=date(2026, 2, 5))
    make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE,
                     amount=Decimal("600.00"), transaction_date=date(2026, 2, 10))

    body = client.get("/transactions/stats/monthly-averages", params={"year": 2026}).json()
    assert body["months_with_data"] == 1
    assert Decimal(str(body["totals"]["total_income"])) == Decimal("1200.00")
    assert Decimal(str(body["totals"]["total_expenses"])) == Decimal("600.00")


# ===== AMORTIZATION =====

def test_set_and_get_amortization_equal_split(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))

    resp = client.put(
        f"/transactions/{txn.uuid}/amortization",
        json={"start_month": "2026-01", "months": 4},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["num_months"] == 4
    assert Decimal(str(body["total_amount"])) == Decimal("100.00")
    assert {Decimal(str(a["amount"])) for a in body["allocations"]} == {Decimal("25.00")}

    got = client.get(f"/transactions/{txn.uuid}/amortization").json()
    assert len(got["allocations"]) == 4


def test_get_amortization_none_404(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct)
    assert client.get(f"/transactions/{txn.uuid}/amortization").status_code == 404


def test_amortization_conflicts_with_splits_400(client, db, test_user):
    acct = make_account(db, test_user)
    cat1, cat2 = make_category(db, name="S1"), make_category(db, name="S2")
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))
    client.put(f"/transactions/{txn.uuid}/splits", json=_split_body(cat1, cat2))

    resp = client.put(f"/transactions/{txn.uuid}/amortization", json={"start_month": "2026-01", "months": 2})
    assert resp.status_code == 400
    assert "split" in resp.json()["detail"].lower()


def test_delete_amortization_204(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct, amount=Decimal("100.00"))
    client.put(f"/transactions/{txn.uuid}/amortization", json={"start_month": "2026-01", "months": 2})
    assert client.delete(f"/transactions/{txn.uuid}/amortization").status_code == 204
    assert client.get(f"/transactions/{txn.uuid}/amortization").status_code == 404


# ===== RELATIONSHIP UPDATE =====

def test_update_relationship_notes(client, db, test_user):
    acct = make_account(db, test_user)
    a = make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE)
    b = make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT)
    rel = client.post(
        f"/transactions/{b.uuid}/relationships",
        json={"to_transaction_uuid": str(a.uuid), "relationship_type": "REFUNDS"},
    ).json()

    resp = client.put(f"/transactions/relationships/{rel['id']}", json={"notes": "partial refund"})
    assert resp.status_code == 200
    assert resp.json()["notes"] == "partial refund"


def test_update_relationship_no_fields_400(client, db, test_user):
    acct = make_account(db, test_user)
    a = make_transaction(db, test_user, acct, transaction_type=TransactionType.PURCHASE)
    b = make_transaction(db, test_user, acct, transaction_type=TransactionType.CREDIT)
    rel = client.post(
        f"/transactions/{b.uuid}/relationships",
        json={"to_transaction_uuid": str(a.uuid), "relationship_type": "REFUNDS"},
    ).json()
    assert client.put(f"/transactions/relationships/{rel['id']}", json={}).status_code == 400


# ===== INVESTMENT-ACCOUNT WRITE GUARD =====
# Cross-cutting invariant: regular transactions may not be created on,
# bulk-uploaded to, or reassigned onto an INVESTMENT account (those belong on
# /investment-transactions). The guard fires on create / bulk-upload / account
# reassignment, never on a field-only edit, and must not partially write a bulk
# update. (Migrated from the former test_investment_account_write_guard.py.)

def _guard_accounts(db, user):
    checking = make_account(db, user, account_name="Guard Checking", account_type=AccountType.CHECKING)
    invest = make_account(db, user, account_name="Guard Brokerage", account_type=AccountType.INVESTMENT)
    return checking, invest


def test_guard_create_rejects_investment_account(client, db, test_user):
    _, invest = _guard_accounts(db, test_user)
    resp = client.post("/transactions/", json=_payload(invest.uuid))
    assert resp.status_code == 400
    assert "investment" in resp.json()["detail"].lower()


def test_guard_create_allows_non_investment_account(client, db, test_user):
    checking, _ = _guard_accounts(db, test_user)
    assert client.post("/transactions/", json=_payload(checking.uuid)).status_code == 201


def test_guard_update_rejects_reassignment_to_investment(client, db, test_user):
    checking, invest = _guard_accounts(db, test_user)
    txn = make_transaction(db, test_user, checking)
    assert client.put(f"/transactions/{txn.uuid}", json={"account_uuid": str(invest.uuid)}).status_code == 400


def test_guard_update_allows_reassignment_to_non_investment(client, db, test_user):
    checking, _ = _guard_accounts(db, test_user)
    savings = make_account(db, test_user, account_name="Guard Savings", account_type=AccountType.SAVINGS)
    txn = make_transaction(db, test_user, checking)
    resp = client.put(f"/transactions/{txn.uuid}", json={"account_uuid": str(savings.uuid)})
    assert resp.status_code == 200
    assert resp.json()["account_uuid"] == str(savings.uuid)


def test_guard_update_allows_field_edit_without_account_change(client, db, test_user):
    checking, _ = _guard_accounts(db, test_user)
    txn = make_transaction(db, test_user, checking)
    resp = client.put(f"/transactions/{txn.uuid}", json={"description": "edited"})
    assert resp.status_code == 200
    assert resp.json()["description"] == "edited"


def test_guard_bulk_update_rejects_investment_target_no_partial_write(client, db, test_user):
    checking, invest = _guard_accounts(db, test_user)
    t1 = make_transaction(db, test_user, checking, description="t1")
    t2 = make_transaction(db, test_user, checking, description="t2")
    resp = client.patch("/transactions/bulk-update", json={
        "transaction_uuids": [str(t1.uuid), str(t2.uuid)], "account_uuid": str(invest.uuid),
    })
    assert resp.status_code == 400
    db.refresh(t1); db.refresh(t2)
    assert t1.account_id == checking.db_id
    assert t2.account_id == checking.db_id


def test_guard_bulk_upload_rejects_investment_target(client, db, test_user):
    _, invest = _guard_accounts(db, test_user)
    body = {"account_uuid": str(invest.uuid), "transactions": [_payload(invest.uuid)], "source_type": "CSV"}
    assert client.post("/transactions/bulk-upload/", json=body).status_code == 400
