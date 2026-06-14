"""Over-HTTP tests for the /accounts router.

Canonical Phase 2 pattern: drive the real request stack through the `client`
fixture (URL routing, auth dependency, Pydantic request validation, and
response serialization), not the handler functions directly. `unauth_client`
leaves auth real so the 401 path is exercised.

Cross-user access returns 404, not 403, by design — every account query is
scoped by `user_id`, so another user's row is simply not found (no existence
leak). Malformed UUIDs are rejected by `_parse_account_uuid` as 400 before the
DB is touched.
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from src.db.core import AccountType
from tests.factories import make_account, make_transaction, make_user

pytestmark = pytest.mark.integration


def _valid_payload(**overrides):
    payload = {
        "account_name": "My Checking",
        "account_type": "CHECKING",
        "institution_name": "Test Bank",
        "balance": "100.00",
    }
    payload.update(overrides)
    return payload


# ===== CREATE =====

def test_create_account_201_and_persists(client, db, test_user):
    resp = client.post("/accounts/", json=_valid_payload())
    assert resp.status_code == 201
    body = resp.json()
    assert body["account_name"] == "My Checking"
    assert body["account_type"] == "CHECKING"
    assert Decimal(str(body["balance"])) == Decimal("100.00")
    assert body["id"]

    # Round-trips through the list endpoint (same session via get_db override).
    listed = client.get("/accounts/").json()
    assert [a["id"] for a in listed] == [body["id"]]


def test_create_rounds_balance_to_two_places(client):
    resp = client.post("/accounts/", json=_valid_payload(balance="100.126"))
    assert resp.status_code == 201
    assert Decimal(str(resp.json()["balance"])) == Decimal("100.13")


def test_create_investment_seeds_initial_cash_balance(client):
    resp = client.post(
        "/accounts/",
        json=_valid_payload(account_name="Brokerage", account_type="INVESTMENT", balance="500.00"),
    )
    assert resp.status_code == 201
    assert Decimal(str(resp.json()["initial_cash_balance"])) == Decimal("500.00")


def test_create_duplicate_name_400(client):
    assert client.post("/accounts/", json=_valid_payload()).status_code == 201
    dup = client.post("/accounts/", json=_valid_payload(institution_name="Other Bank"))
    assert dup.status_code == 400
    assert "already exists" in dup.json()["detail"]


def test_create_missing_required_field_422(client):
    payload = _valid_payload()
    del payload["account_type"]
    assert client.post("/accounts/", json=payload).status_code == 422


def test_create_blank_name_422(client):
    assert client.post("/accounts/", json=_valid_payload(account_name="")).status_code == 422


@pytest.mark.parametrize("bad_last4", ["abcd", "12", "12345"])
def test_create_invalid_last4_422(client, bad_last4):
    resp = client.post("/accounts/", json=_valid_payload(account_number_last4=bad_last4))
    assert resp.status_code == 422


def test_create_unauthenticated_401(unauth_client):
    assert unauth_client.post("/accounts/", json=_valid_payload()).status_code == 401


# ===== LIST =====

def test_list_empty(client):
    resp = client.get("/accounts/")
    assert resp.status_code == 200
    assert resp.json() == []


def test_list_returns_only_current_user_accounts(client, db, test_user):
    make_account(db, test_user, account_name="Mine A")
    make_account(db, test_user, account_name="Mine B")
    other = make_user(db, email="other@example.com", username="other")
    make_account(db, other, account_name="Theirs")

    names = {a["account_name"] for a in client.get("/accounts/").json()}
    assert names == {"Mine A", "Mine B"}


def test_list_filter_by_account_type(client, db, test_user):
    make_account(db, test_user, account_name="Chk", account_type=AccountType.CHECKING)
    make_account(db, test_user, account_name="CC", account_type=AccountType.CREDIT_CARD)

    resp = client.get("/accounts/", params={"account_type": "CREDIT_CARD"})
    assert resp.status_code == 200
    assert [a["account_name"] for a in resp.json()] == ["CC"]


def test_list_pagination(client, db, test_user):
    for i in range(3):
        make_account(db, test_user, account_name=f"Acct {i}")
    assert len(client.get("/accounts/", params={"limit": 2}).json()) == 2
    assert len(client.get("/accounts/", params={"skip": 2}).json()) == 1


def test_list_unauthenticated_401(unauth_client):
    assert unauth_client.get("/accounts/").status_code == 401


# ===== GET ONE =====

def test_get_account_200(client, db, test_user):
    acct = make_account(db, test_user, account_name="Readable")
    resp = client.get(f"/accounts/{acct.uuid}")
    assert resp.status_code == 200
    assert resp.json()["account_name"] == "Readable"


def test_get_unknown_uuid_404(client):
    assert client.get(f"/accounts/{uuid4()}").status_code == 404


def test_get_malformed_uuid_422(client):
    resp = client.get("/accounts/not-a-uuid")
    assert resp.status_code == 422


def test_get_cross_user_404(client, db):
    other = make_user(db, email="x@example.com", username="x")
    acct = make_account(db, other, account_name="Theirs")
    assert client.get(f"/accounts/{acct.uuid}").status_code == 404


def test_get_unauthenticated_401(unauth_client, db, test_user):
    acct = make_account(db, test_user)
    assert unauth_client.get(f"/accounts/{acct.uuid}").status_code == 401


# ===== UPDATE =====

def test_update_account_200(client, db, test_user):
    # NB: AccountUpdate has no `balance` field — balance is transaction-derived,
    # not directly settable via PUT. Update the fields the model does expose.
    acct = make_account(db, test_user, account_name="Before", institution_name="Old Bank")
    resp = client.put(
        f"/accounts/{acct.uuid}",
        json={"account_name": "After", "institution_name": "New Bank"},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["account_name"] == "After"
    assert body["institution_name"] == "New Bank"


def test_update_unknown_uuid_404(client):
    assert client.put(f"/accounts/{uuid4()}", json={"account_name": "X"}).status_code == 404


def test_update_duplicate_name_400(client, db, test_user):
    make_account(db, test_user, account_name="Taken")
    acct = make_account(db, test_user, account_name="Original")
    resp = client.put(f"/accounts/{acct.uuid}", json={"account_name": "Taken"})
    assert resp.status_code == 400
    assert "already exists" in resp.json()["detail"]


def test_update_malformed_uuid_422(client):
    assert client.put("/accounts/nope", json={"account_name": "X"}).status_code == 422


def test_update_cross_user_404(client, db):
    other = make_user(db, email="y@example.com", username="y")
    acct = make_account(db, other, account_name="Theirs")
    assert client.put(f"/accounts/{acct.uuid}", json={"account_name": "Hijack"}).status_code == 404


# ===== DELETE =====

def test_delete_clean_account_204(client, db, test_user):
    acct = make_account(db, test_user, account_name="Disposable")
    assert client.delete(f"/accounts/{acct.uuid}").status_code == 204
    # Gone afterwards.
    assert client.get(f"/accounts/{acct.uuid}").status_code == 404


def test_delete_with_associated_data_409(client, db, test_user):
    acct = make_account(db, test_user, account_name="HasTxns")
    make_transaction(db, test_user, acct)
    resp = client.delete(f"/accounts/{acct.uuid}")
    assert resp.status_code == 409
    assert "force=true" in resp.json()["detail"]


def test_delete_force_cascades_200(client, db, test_user):
    acct = make_account(db, test_user, account_name="ForceMe")
    make_transaction(db, test_user, acct)
    resp = client.delete(f"/accounts/{acct.uuid}", params={"force": "true"})
    assert resp.status_code == 200
    assert "deleted" in resp.json()
    assert client.get(f"/accounts/{acct.uuid}").status_code == 404


def _make_statement_doc(db, user, account, *, institution="tdbank"):
    """Persist an upload-job document with a real file in isolated storage,
    linked to ``account``. Returns (job, storage_key)."""
    from src.db.core import UploadJobDB
    from src.services import file_storage

    key = file_storage.build_key(user.db_id, uuid4(), "stmt.pdf")
    file_storage.get_storage().save(b"%PDF-1.4 fake", key)
    job = UploadJobDB(
        uuid=uuid4(), user_id=user.db_id, account_id=account.db_id,
        institution=institution, storage_key=key, status="COMPLETED",
    )
    db.add(job)
    db.commit()
    return job, key


def test_delete_force_unlinks_statements_by_default(client, db, test_user):
    """Force-delete keeps statements as user docs: account_id nulled, file kept."""
    from src.services import file_storage

    acct = make_account(db, test_user, account_name="HasStmt")
    job, key = _make_statement_doc(db, test_user, acct)

    resp = client.delete(f"/accounts/{acct.uuid}", params={"force": "true"})
    assert resp.status_code == 200
    assert resp.json()["deleted"].get("upload_jobs_nulled") == 1
    # Document survives, unlinked, and its file is still on disk.
    db.refresh(job)
    assert job.account_id is None
    assert file_storage.get_storage().exists(key)


def test_delete_force_purge_statements_removes_files(client, db, test_user):
    """?purge_statements=true deletes the linked docs and their stored files."""
    from src.db.core import UploadJobDB
    from src.services import file_storage

    acct = make_account(db, test_user, account_name="PurgeStmt")
    job, key = _make_statement_doc(db, test_user, acct)
    job_id = job.db_id

    resp = client.delete(
        f"/accounts/{acct.uuid}", params={"force": "true", "purge_statements": "true"}
    )
    assert resp.status_code == 200
    assert resp.json()["deleted"].get("upload_jobs_deleted") == 1
    # Row gone and file reclaimed.
    assert db.query(UploadJobDB).filter(UploadJobDB.db_id == job_id).first() is None
    assert not file_storage.get_storage().exists(key)


def test_delete_unknown_uuid_404(client):
    assert client.delete(f"/accounts/{uuid4()}").status_code == 404


def test_delete_malformed_uuid_422(client):
    assert client.delete("/accounts/bad").status_code == 422


def test_delete_cross_user_404(client, db):
    other = make_user(db, email="z@example.com", username="z")
    acct = make_account(db, other, account_name="Theirs")
    assert client.delete(f"/accounts/{acct.uuid}").status_code == 404


# ===== STATS / SUMMARY =====

def test_stats_net_worth_math(client, db, test_user):
    make_account(db, test_user, account_name="Chk", account_type=AccountType.CHECKING, balance=Decimal("1000.00"))
    make_account(db, test_user, account_name="CC", account_type=AccountType.CREDIT_CARD, balance=Decimal("500.00"))
    make_account(db, test_user, account_name="Loan", account_type=AccountType.LOAN, balance=Decimal("200.00"))

    body = client.get("/accounts/stats").json()
    assert body["total_accounts"] == 3
    assert Decimal(str(body["total_assets"])) == Decimal("1000.00")
    assert Decimal(str(body["total_liabilities"])) == Decimal("700.00")
    assert Decimal(str(body["net_worth"])) == Decimal("300.00")
    assert body["accounts_by_type"] == {"CHECKING": 1, "CREDIT_CARD": 1, "LOAN": 1}


def test_stats_empty(client):
    body = client.get("/accounts/stats").json()
    assert body["total_accounts"] == 0
    assert Decimal(str(body["net_worth"])) == Decimal("0.00")


def test_summary_lists_accounts(client, db, test_user):
    make_account(db, test_user, account_name="Summarized")
    body = client.get("/accounts/summary").json()
    assert len(body) == 1
    assert body[0]["account_name"] == "Summarized"
    assert "id" in body[0] and "balance" in body[0]


def test_stats_unauthenticated_401(unauth_client):
    assert unauth_client.get("/accounts/stats").status_code == 401
