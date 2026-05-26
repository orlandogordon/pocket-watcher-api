"""End-to-end preview->confirm upload flow over HTTP, driving a real parser.

Unlike test_uploads.py (which seeds Redis sessions directly to exercise the
session-management endpoints), this file posts an actual statement file to
`POST /uploads/statement/preview`, so the real parse -> duplicate-analysis ->
LLM-processing -> Redis-store path runs, then edits/rejects rows and finalizes
via `POST /uploads/statement/confirm`. The LLM is faked (`fake_llm`) and the
post-confirm snapshot-backfill trigger is stubbed (it would otherwise spawn a
ThreadJobRunner against the real SessionLocal — job-runner mocking is Phase 5).

The fixture is a small synthetic Amex activity CSV (no real cardholder data);
it doubles as the first parser fixture and exercises `amex.parse(is_csv=True)`.
"""
from pathlib import Path
from uuid import uuid4

import pytest

from src.db.core import (
    AccountType,
    TagDB,
    TransactionDB,
    TransactionTagDB,
)
from src.services.system_tags import ensure_system_tags
from tests.factories import make_account

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).parent / "parsers" / "fixtures" / "amex_sample.csv"

# Descriptions as the Amex CSV parser emits them (single-spaced, prefixes
# stripped). The fixture has 3 purchases (positive amounts) + 1 credit
# (the negative AUTOPAY row).
PURCHASE_DESCS = [
    "STARBUCKS SEATTLE WA",
    "WHOLEFOODS MARKET AUSTIN TX",
    "UNITED AIRLINES 800-555-0001",
]
CREDIT_DESC = "AUTOPAY PAYMENT THANK YOU"


@pytest.fixture(autouse=True)
def _no_backfill(monkeypatch):
    """Stub the snapshot-backfill trigger so confirm doesn't spawn a background
    job that opens the real (non-test) database session."""
    monkeypatch.setattr(
        "src.services.account_snapshot.trigger_backfill_if_needed",
        lambda *a, **k: None,
    )


@pytest.fixture
def cc_account(db, test_user):
    return make_account(
        db, test_user,
        account_name="Amex Platinum",
        account_type=AccountType.CREDIT_CARD,
        institution_name="Amex",
    )


def _csv_bytes() -> bytes:
    return _FIXTURE.read_bytes()


def _preview(client, account_uuid, *, institution="amex"):
    return client.post(
        "/uploads/statement/preview",
        files={"file": ("amex_sample.csv", _csv_bytes(), "text/csv")},
        data={"institution": institution, "account_uuid": str(account_uuid)},
    )


# ===== PREVIEW (parse + duplicate analysis) =====

def test_preview_parses_all_rows(client, fake_llm, cc_account):
    resp = _preview(client, cc_account.uuid)
    assert resp.status_code == 201, resp.text
    body = resp.json()

    assert body["summary"] == {
        "total_parsed": 4,
        "rejected": 0,
        "ready_to_import": 4,
        "can_confirm": True,
    }
    ready = body["ready_to_import"]["transactions"]
    assert {item["parsed_data"]["description"] for item in ready} == {
        *PURCHASE_DESCS, CREDIT_DESC,
    }
    # No investment rows from a card statement.
    assert body["ready_to_import"]["investment_transactions"] == []
    assert body["preview_session_id"]


def test_preview_unsupported_content_type_415(client, fake_llm, cc_account):
    resp = client.post(
        "/uploads/statement/preview",
        files={"file": ("note.txt", b"hello", "text/plain")},
        data={"institution": "amex", "account_uuid": str(cc_account.uuid)},
    )
    assert resp.status_code == 415


def test_preview_unknown_institution_400(client, fake_llm, cc_account):
    resp = _preview(client, cc_account.uuid, institution="not-a-bank")
    assert resp.status_code == 400


def test_preview_unresolvable_account_400(client, fake_llm, test_user):
    # CSV carries no account info and no account_uuid is sent -> cannot build
    # the account-scoped dedup hash, so preview must fail with an actionable 400.
    resp = client.post(
        "/uploads/statement/preview",
        files={"file": ("amex_sample.csv", _csv_bytes(), "text/csv")},
        data={"institution": "amex"},
    )
    assert resp.status_code == 400


def test_preview_unauthenticated_401(unauth_client, cc_account):
    resp = unauth_client.post(
        "/uploads/statement/preview",
        files={"file": ("amex_sample.csv", _csv_bytes(), "text/csv")},
        data={"institution": "amex", "account_uuid": str(cc_account.uuid)},
    )
    assert resp.status_code == 401


# ===== EDIT + REJECT -> CONFIRM =====

def _temp_id_for(ready_items, description):
    for item in ready_items:
        if item["parsed_data"]["description"] == description:
            return item["temp_id"]
    raise AssertionError(f"no ready row for {description!r}")


def test_edit_reject_then_confirm(client, fake_llm, cc_account, db, test_user):
    preview = _preview(client, cc_account.uuid).json()
    sid = preview["preview_session_id"]
    ready = preview["ready_to_import"]["transactions"]

    edit_id = _temp_id_for(ready, "STARBUCKS SEATTLE WA")
    reject_id = _temp_id_for(ready, CREDIT_DESC)

    edit = client.post(
        f"/uploads/preview/{sid}/edit-transaction",
        json={"temp_id": edit_id, "edited_data": {
            "merchant_name": "Starbucks",
            "description": "Starbucks Coffee",
        }},
    )
    assert edit.status_code == 200, edit.text

    rej = client.post(f"/uploads/preview/{sid}/reject-item", json={"temp_id": reject_id})
    assert rej.status_code == 200
    assert rej.json()["summary"] == {
        "total_parsed": 4, "rejected": 1, "ready_to_import": 3, "can_confirm": True,
    }

    confirm = client.post("/uploads/statement/confirm", json={"preview_session_id": sid})
    assert confirm.status_code == 201, confirm.text
    cbody = confirm.json()
    assert cbody["transactions_created"] == 3
    assert cbody["investment_transactions_created"] == 0

    created = db.query(TransactionDB).filter(
        TransactionDB.user_id == test_user.db_id
    ).all()
    by_desc = {t.description: t for t in created}
    # Edited row persisted both fields; rejected credit row was not created.
    assert "Starbucks Coffee" in by_desc
    assert by_desc["Starbucks Coffee"].merchant_name == "Starbucks"
    assert CREDIT_DESC not in by_desc
    assert len(created) == 3

    # The Redis session is consumed on confirm.
    assert client.get(f"/uploads/preview/{sid}").status_code == 404


def test_confirm_rejects_unknown_category_uuid_400(client, fake_llm, cc_account):
    preview = _preview(client, cc_account.uuid).json()
    sid = preview["preview_session_id"]
    edit_id = _temp_id_for(preview["ready_to_import"]["transactions"], "STARBUCKS SEATTLE WA")

    # A category_uuid not in the predefined set must be rejected at confirm time
    # (it would otherwise bypass the locked category vocabulary).
    bogus = str(uuid4())
    client.post(
        f"/uploads/preview/{sid}/edit-transaction",
        json={"temp_id": edit_id, "edited_data": {"category_uuid": bogus}},
    )
    confirm = client.post("/uploads/statement/confirm", json={"preview_session_id": sid})
    assert confirm.status_code == 400


# ===== DUPLICATE DETECTION ON RE-UPLOAD =====

def test_reupload_flags_all_rows_as_duplicates(client, fake_llm, cc_account):
    first = _preview(client, cc_account.uuid).json()
    confirm = client.post(
        "/uploads/statement/confirm", json={"preview_session_id": first["preview_session_id"]}
    )
    assert confirm.json()["transactions_created"] == 4

    second = _preview(client, cc_account.uuid)
    assert second.status_code == 201
    body = second.json()
    assert body["summary"]["rejected"] == 4
    assert body["summary"]["ready_to_import"] == 0
    rejected = body["rejected"]["transactions"]
    assert len(rejected) == 4
    assert all(item["is_duplicate"] for item in rejected)
    assert all(item["duplicate_type"] == "database" for item in rejected)


# ===== NEEDS-REVIEW AUTO-TAG (null LLM suggestion -> no category/merchant) =====

def test_confirm_auto_flags_uncategorized_rows(client, fake_llm, cc_account, db, test_user):
    ensure_system_tags(test_user.db_id, db)

    preview = _preview(client, cc_account.uuid).json()
    confirm = client.post(
        "/uploads/statement/confirm", json={"preview_session_id": preview["preview_session_id"]}
    )
    assert confirm.status_code == 201

    needs_review = db.query(TagDB).filter(
        TagDB.user_id == test_user.db_id,
        TagDB.tag_name == "Needs Review",
    ).one()

    created = db.query(TransactionDB).filter(
        TransactionDB.user_id == test_user.db_id
    ).all()
    # The fake LLM returns a null category for every row, and none of these
    # rows is a transfer, so all four are flagged for review with an
    # explanatory comment and the system tag.
    assert len(created) == 4
    for txn in created:
        assert txn.category_id is None
        assert "Auto-flagged for review" in (txn.comments or "")
        link = db.query(TransactionTagDB).filter(
            TransactionTagDB.transaction_id == txn.db_id,
            TransactionTagDB.tag_id == needs_review.tag_id,
        ).first()
        assert link is not None, f"{txn.description} not tagged Needs Review"


def test_llm_unavailable_degrades_gracefully(client, fake_llm, cc_account):
    # When the LLM backend is down the preview still succeeds; rows just carry
    # no suggestion and the summary flags the degraded state.
    fake_llm.unavailable = True
    resp = _preview(client, cc_account.uuid)
    assert resp.status_code == 201
    body = resp.json()
    assert body["summary"]["ready_to_import"] == 4
    assert body["llm_summary"]["degraded"] is True
