"""Over-HTTP tests for the /tags router.

User tags are created through the API itself (no factory). System tags are
seeded with `ensure_system_tags` so the 403 "system tags cannot be
modified/deleted" guards can be exercised — the harness skips the startup hook
that would normally seed them. TagResponse exposes its UUID under "id".
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from src.services.system_tags import ensure_system_tags, get_system_tag
from tests.factories import make_account, make_transaction

pytestmark = pytest.mark.integration


def _make_tag(client, name="Vacation", color=None):
    body = {"tag_name": name}
    if color:
        body["color"] = color
    resp = client.post("/tags/", json=body)
    assert resp.status_code == 201, resp.text
    return resp.json()


# ===== CREATE =====

def test_create_tag_201(client):
    tag = _make_tag(client, name="Business", color="#1A2B3C")
    assert tag["tag_name"] == "Business"
    assert tag["color"] == "#1A2B3C"
    assert tag["is_system"] is False


def test_create_duplicate_name_400(client):
    _make_tag(client, name="Dupe")
    resp = client.post("/tags/", json={"tag_name": "Dupe"})
    assert resp.status_code == 400
    assert "already exists" in resp.json()["detail"]


def test_create_invalid_color_422(client):
    assert client.post("/tags/", json={"tag_name": "Bad", "color": "zzzzzz"}).status_code == 422


def test_create_blank_name_422(client):
    assert client.post("/tags/", json={"tag_name": ""}).status_code == 422


def test_create_unauthenticated_401(unauth_client):
    assert unauth_client.post("/tags/", json={"tag_name": "X"}).status_code == 401


# ===== READ =====

def test_list_tags(client):
    _make_tag(client, name="A")
    _make_tag(client, name="B")
    names = {t["tag_name"] for t in client.get("/tags/").json()}
    assert {"A", "B"} <= names


def test_get_tag_200(client):
    tag = _make_tag(client, name="Findable")
    resp = client.get(f"/tags/{tag['id']}")
    assert resp.status_code == 200
    assert resp.json()["tag_name"] == "Findable"


def test_get_unknown_404(client):
    assert client.get(f"/tags/{uuid4()}").status_code == 404


def test_get_malformed_uuid_400(client):
    assert client.get("/tags/not-a-uuid").status_code == 400


def test_search_tags(client):
    _make_tag(client, name="Groceries")
    _make_tag(client, name="Gas")
    results = client.get("/tags/search/", params={"search_term": "gro"}).json()
    assert [t["tag_name"] for t in results] == ["Groceries"]


# ===== UPDATE / DELETE (+ system-tag guards) =====

def test_update_tag_200(client):
    tag = _make_tag(client, name="Old")
    resp = client.put(f"/tags/{tag['id']}", json={"tag_name": "New"})
    assert resp.status_code == 200
    assert resp.json()["tag_name"] == "New"


def test_update_unknown_404(client):
    assert client.put(f"/tags/{uuid4()}", json={"tag_name": "X"}).status_code == 404


def test_delete_tag_204(client):
    tag = _make_tag(client, name="Disposable")
    assert client.delete(f"/tags/{tag['id']}").status_code == 204
    assert client.get(f"/tags/{tag['id']}").status_code == 404


def test_update_system_tag_403(client, db, test_user):
    ensure_system_tags(test_user.db_id, db)
    sys_tag = get_system_tag(test_user.db_id, db, "Needs Review")
    resp = client.put(f"/tags/{sys_tag.id}", json={"tag_name": "Renamed"})
    assert resp.status_code == 403


def test_delete_system_tag_403(client, db, test_user):
    ensure_system_tags(test_user.db_id, db)
    sys_tag = get_system_tag(test_user.db_id, db, "Needs Review")
    assert client.delete(f"/tags/{sys_tag.id}").status_code == 403


# ===== TAG <-> TRANSACTION ASSOCIATION =====

def test_add_and_list_tag_on_transaction(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct, description="Tagged txn")
    tag = _make_tag(client, name="Reimbursable")

    add = client.post("/tags/transactions/", params={"transaction_uuid": str(txn.id), "tag_uuid": tag["id"]})
    assert add.status_code == 201

    txns = client.get(f"/tags/{tag['id']}/transactions").json()
    assert [t["description"] for t in txns] == ["Tagged txn"]


def test_add_tag_duplicate_400(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct)
    tag = _make_tag(client, name="Once")
    params = {"transaction_uuid": str(txn.id), "tag_uuid": tag["id"]}
    assert client.post("/tags/transactions/", params=params).status_code == 201
    assert client.post("/tags/transactions/", params=params).status_code == 400


def test_remove_tag_from_transaction_204(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct)
    tag = _make_tag(client, name="Temp")
    client.post("/tags/transactions/", params={"transaction_uuid": str(txn.id), "tag_uuid": tag["id"]})

    resp = client.delete(f"/tags/transactions/{txn.id}/tags/{tag['id']}")
    assert resp.status_code == 204


def test_remove_missing_association_404(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct)
    tag = _make_tag(client, name="Unlinked")
    assert client.delete(f"/tags/transactions/{txn.id}/tags/{tag['id']}").status_code == 404


def test_bulk_tag_transactions(client, db, test_user):
    acct = make_account(db, test_user)
    t1 = make_transaction(db, test_user, acct)
    t2 = make_transaction(db, test_user, acct)
    tag = _make_tag(client, name="Bulk")

    resp = client.post(
        "/tags/transactions/bulk-tag",
        json={"transaction_uuids": [str(t1.id), str(t2.id)], "tag_uuid": tag["id"]},
    )
    assert resp.status_code == 201
    assert resp.json()["tagged_count"] == 2


def test_bulk_tag_unknown_tag_404(client, db, test_user):
    acct = make_account(db, test_user)
    t1 = make_transaction(db, test_user, acct)
    resp = client.post(
        "/tags/transactions/bulk-tag",
        json={"transaction_uuids": [str(t1.id)], "tag_uuid": str(uuid4())},
    )
    assert resp.status_code == 404


# ===== STATS =====

def test_tag_stats(client, db, test_user):
    acct = make_account(db, test_user)
    txn = make_transaction(db, test_user, acct, amount=Decimal("40.00"))
    tag = _make_tag(client, name="Counted")
    client.post("/tags/transactions/", params={"transaction_uuid": str(txn.id), "tag_uuid": tag["id"]})

    stats = client.get(f"/tags/{tag['id']}/stats").json()
    assert stats["transaction_count"] == 1

    all_stats = client.get("/tags/stats").json()
    assert any(s["id"] == tag["id"] for s in all_stats)
