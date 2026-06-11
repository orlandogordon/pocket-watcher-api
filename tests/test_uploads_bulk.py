"""Bulk statement import + document storage over HTTP (#59, Phases 4-5).

Uploads the synthetic Amex CSV via POST /uploads/files, kicks off a batch, drives
the worker synchronously (the real submit spawns a thread on its own SessionLocal,
so it's stubbed), then asserts progress, the document listing, and owner-scoped
content streaming.
"""
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from src.db.core import AccountType, BulkImportBatchDB, UploadJobDB
from src.services import file_storage
from src.services.bulk_import_runner import process_batch
from tests.factories import make_account, make_user

pytestmark = pytest.mark.integration

_FIXTURE = Path(__file__).parent / "parsers" / "fixtures" / "amex_sample.csv"
_CSV_BYTES = _FIXTURE.read_bytes()


@pytest.fixture(autouse=True)
def _no_async(monkeypatch):
    """Don't spawn the real worker thread / snapshot backfill from the endpoint."""
    monkeypatch.setattr("src.routers.uploads.submit_bulk_import", lambda batch_id: None)
    monkeypatch.setattr(
        "src.services.bulk_import_runner.trigger_backfill_if_needed",
        lambda *a, **k: None,
    )
    # crud delete paths import this lazily from account_snapshot — stub the source
    # so the cascade-delete test doesn't spawn a real-DB backfill thread.
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


def _upload(client, account_uuid, institution="amex", filename="amex_sample.csv"):
    return client.post(
        "/uploads/files",
        files={"file": (filename, _CSV_BYTES, "text/csv")},
        data={"account_uuid": str(account_uuid), "institution": institution},
    )


# ----- POST /uploads/files -----

def test_upload_file_persists_document(client, db, cc_account, fake_llm):
    resp = _upload(client, cc_account.uuid)
    assert resp.status_code == 201
    body = resp.json()
    doc_uuid = body["document_uuid"]
    assert body["size"] == len(_CSV_BYTES)

    job = db.query(UploadJobDB).filter(UploadJobDB.uuid == UUID(doc_uuid)).first()
    assert job is not None
    assert job.status == "UPLOADED"
    assert job.storage_key and job.account_id == cc_account.db_id
    assert file_storage.get_storage().exists(job.storage_key)


def test_upload_file_unknown_institution_400(client, cc_account):
    resp = _upload(client, cc_account.uuid, institution="not-a-bank")
    assert resp.status_code == 400


@pytest.mark.parametrize("institution", ["venmo", "cashapp"])
def test_upload_file_rejects_p2p_institutions_400(client, cc_account, institution):
    """#77: Venmo/Cash App are pass-throughs, not accounts — they were removed
    from PARSER_MAPPING, so the upload flow must reject them as unknown
    institutions. Guards against silently re-adding them."""
    resp = _upload(client, cc_account.uuid, institution=institution)
    assert resp.status_code == 400


def test_upload_file_bad_account_uuid_400(client):
    resp = _upload(client, "not-a-uuid")
    assert resp.status_code == 400


def test_upload_file_unowned_account_404(client, db):
    other = make_user(db, email="o@x", username="o")
    other_acct = make_account(db, other, account_type=AccountType.CREDIT_CARD)
    resp = _upload(client, other_acct.uuid)
    assert resp.status_code == 404


def test_upload_file_requires_auth(unauth_client, cc_account):
    resp = _upload(unauth_client, cc_account.uuid)
    assert resp.status_code == 401


# ----- POST /uploads/bulk + worker -----

def _start_batch(client, doc_uuids):
    return client.post("/uploads/bulk", json={"document_uuids": [str(u) for u in doc_uuids]})


def test_bulk_import_happy_path(client, db, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]

    start = _start_batch(client, [doc_uuid])
    assert start.status_code == 202
    batch_uuid = start.json()["batch_uuid"]

    batch = db.query(BulkImportBatchDB).filter(BulkImportBatchDB.uuid == UUID(batch_uuid)).first()
    assert batch.total_files == 1
    job = db.query(UploadJobDB).filter(UploadJobDB.uuid == UUID(doc_uuid)).first()
    assert job.batch_id == batch.db_id and job.status == "PENDING"

    # Drive the worker on the test session (endpoint's submit was stubbed).
    process_batch(db, batch.db_id)

    db.refresh(job)
    assert job.status == "COMPLETED"
    assert job.transactions_created == 4  # 3 purchases + 1 AUTOPAY credit

    prog = client.get(f"/uploads/bulk/{batch_uuid}").json()
    assert prog["status"] == "COMPLETED"
    assert prog["processed"] == 1
    assert prog["created"] == 4
    # fake_llm returns null categories, so all 4 rows are auto-flagged Needs Review.
    assert prog["needs_review"] == 4
    assert prog["per_file"][0]["status"] == "COMPLETED"
    # LLM was reachable (fake_llm), so nothing degraded (#60).
    assert prog["llm_degraded"] is False
    assert prog["per_file"][0]["llm_degraded"] is False


def test_bulk_import_unknown_document_404(client, cc_account):
    resp = _start_batch(client, [uuid4()])
    assert resp.status_code == 404


def test_bulk_import_rejects_already_imported(client, db, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]
    first = _start_batch(client, [doc_uuid])
    assert first.status_code == 202
    # Same document can't be queued into a second batch.
    second = _start_batch(client, [doc_uuid])
    assert second.status_code == 400


def test_bulk_import_allows_retry_of_failed_document(client, db, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]
    assert _start_batch(client, [doc_uuid]).status_code == 202

    # Simulate the file failing during processing: status FAILED, but batch_id
    # stays set (the rollback doesn't clear it). It must remain retryable (#65) —
    # a failed file imported nothing, so it's not "already imported".
    job = db.query(UploadJobDB).filter(UploadJobDB.uuid == UUID(doc_uuid)).first()
    job.status = "FAILED"
    db.commit()

    retry = _start_batch(client, [doc_uuid])
    assert retry.status_code == 202
    db.refresh(job)
    assert job.status == "PENDING"  # requeued into the new batch


def test_bulk_import_requires_auth(unauth_client, cc_account):
    resp = _start_batch(unauth_client, [uuid4()])
    assert resp.status_code == 401


def test_cancel_bulk_import(client, db, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]
    batch_uuid = _start_batch(client, [doc_uuid]).json()["batch_uuid"]

    resp = client.delete(f"/uploads/bulk/{batch_uuid}")
    assert resp.status_code == 200
    assert resp.json()["status"] == "CANCELLED"

    # Un-processed files are cancelled too, not left stuck "PENDING" (#4).
    job = db.query(UploadJobDB).filter(UploadJobDB.uuid == UUID(doc_uuid)).first()
    assert job.status == "CANCELLED"


# ----- Document browsing / viewing -----

def test_list_documents_for_account(client, db, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]

    resp = client.get(f"/uploads/documents?account_uuid={cc_account.uuid}")
    assert resp.status_code == 200
    docs = resp.json()["documents"]
    assert [d["document_uuid"] for d in docs] == [doc_uuid]
    assert docs[0]["account_uuid"] == str(cc_account.uuid)


def test_document_content_streams_original_bytes(client, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]

    resp = client.get(f"/uploads/documents/{doc_uuid}/content")
    assert resp.status_code == 200
    assert resp.content == _CSV_BYTES
    assert "inline" in resp.headers["content-disposition"]


def test_document_cross_user_content_404(client, db, cc_account):
    # A document owned by a different user must not be fetchable as test_user.
    other = make_user(db, email="o2@x", username="o2")
    other_acct = make_account(db, other, account_type=AccountType.CREDIT_CARD)
    foreign = UploadJobDB(
        uuid=uuid4(), user_id=other.db_id, account_id=other_acct.db_id,
        institution="amex", status="UPLOADED", storage_key=f"{other.db_id}/x.csv",
    )
    db.add(foreign)
    db.commit()

    resp = client.get(f"/uploads/documents/{foreign.uuid}/content")
    assert resp.status_code == 404


def test_get_document_bad_uuid_422(client):
    assert client.get("/uploads/documents/not-a-uuid").status_code == 422


def test_delete_document_cascades_transactions(client, db, cc_account, fake_llm):
    doc_uuid = _upload(client, cc_account.uuid).json()["document_uuid"]
    batch_uuid = _start_batch(client, [doc_uuid]).json()["batch_uuid"]
    batch = db.query(BulkImportBatchDB).filter(BulkImportBatchDB.uuid == UUID(batch_uuid)).first()
    process_batch(db, batch.db_id)

    from src.db.core import TransactionDB
    job = db.query(UploadJobDB).filter(UploadJobDB.uuid == UUID(doc_uuid)).first()
    # The imported rows are linked to this document.
    assert db.query(TransactionDB).filter(TransactionDB.upload_job_id == job.db_id).count() == 4

    resp = client.delete(f"/uploads/documents/{doc_uuid}")
    assert resp.status_code == 204

    # File gone, document gone, and the transactions it imported are gone.
    assert not file_storage.get_storage().exists(job.storage_key)
    assert client.get(f"/uploads/documents/{doc_uuid}").status_code == 404
    assert db.query(TransactionDB).filter(TransactionDB.account_id == cc_account.db_id).count() == 0
