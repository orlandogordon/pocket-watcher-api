"""Over-HTTP tests for the /uploads router — jobs + preview-session management.

The preview→confirm flow that parses an actual statement (PDF/CSV) and the LLM
processing are deferred to the parser-fixtures / mocked-services phases. Here we
seed preview sessions directly into fakeredis via `create_preview_session`,
which exercises the whole session-management half of the router over HTTP
(list, get, reject/restore item, extend, cancel) plus the DB-backed job
endpoints — all without a real file or external calls.
"""
import pytest

from src.services.preview_session import create_preview_session

pytestmark = pytest.mark.integration


def _seed_session(fake_redis, user_id, *, temp_ids=("t1",), institution="TD Bank"):
    ready = [{"temp_id": tid, "review_status": "ready"} for tid in temp_ids]
    summary = {"total_parsed": len(ready), "rejected": 0, "ready_to_import": len(ready), "can_confirm": True}
    session_id, _ = create_preview_session(
        fake_redis,
        user_id=user_id,
        institution=institution,
        account_id=None,
        filename="statement.pdf",
        source_type="PDF",
        rejected={"transactions": [], "investment_transactions": []},
        ready_to_import={"transactions": ready, "investment_transactions": []},
        summary=summary,
    )
    return session_id


# ===== JOBS (DB-backed) =====

def test_list_jobs_empty(client):
    resp = client.get("/uploads/jobs")
    assert resp.status_code == 200
    assert resp.json()["jobs"] == []


def test_get_job_unknown_404(client):
    assert client.get("/uploads/jobs/99999").status_code == 404


def test_get_skipped_unknown_job_404(client):
    assert client.get("/uploads/jobs/99999/skipped").status_code == 404


def test_jobs_unauthenticated_401(unauth_client):
    assert unauth_client.get("/uploads/jobs").status_code == 401


# ===== PREVIEW SESSION LISTING / RETRIEVAL =====

def test_list_sessions_empty(client):
    assert client.get("/uploads/preview/sessions").json() == []


def test_list_and_get_session(client, fake_redis, test_user):
    sid = _seed_session(fake_redis, test_user.db_id)
    listed = client.get("/uploads/preview/sessions").json()
    assert [s["preview_session_id"] for s in listed] == [sid]

    resp = client.get(f"/uploads/preview/{sid}")
    assert resp.status_code == 200
    assert resp.json()["preview_session_id"] == sid
    assert resp.json()["summary"]["ready_to_import"] == 1


def test_get_unknown_session_404(client):
    assert client.get("/uploads/preview/nope").status_code == 404


def test_session_owned_by_other_user_404(client, fake_redis, test_user):
    # Seed under a different user id → current user must not see it.
    sid = _seed_session(fake_redis, test_user.db_id + 999)
    assert client.get(f"/uploads/preview/{sid}").status_code == 404
    assert client.get("/uploads/preview/sessions").json() == []


# ===== REJECT / RESTORE =====

def test_reject_then_restore_item(client, fake_redis, test_user):
    sid = _seed_session(fake_redis, test_user.db_id, temp_ids=("t1", "t2"))

    rej = client.post(f"/uploads/preview/{sid}/reject-item", json={"temp_id": "t1"})
    assert rej.status_code == 200
    assert rej.json()["summary"]["rejected"] == 1
    assert rej.json()["summary"]["ready_to_import"] == 1

    res = client.post(f"/uploads/preview/{sid}/restore-item", json={"temp_id": "t1"})
    assert res.status_code == 200
    assert res.json()["summary"]["rejected"] == 0
    assert res.json()["summary"]["ready_to_import"] == 2


def test_reject_unknown_item_404(client, fake_redis, test_user):
    sid = _seed_session(fake_redis, test_user.db_id)
    assert client.post(f"/uploads/preview/{sid}/reject-item", json={"temp_id": "nope"}).status_code == 404


def test_reject_unknown_session_404(client):
    assert client.post("/uploads/preview/nope/reject-item", json={"temp_id": "t1"}).status_code == 404


# ===== EXTEND / CANCEL =====

def test_extend_session(client, fake_redis, test_user):
    sid = _seed_session(fake_redis, test_user.db_id)
    resp = client.get(f"/uploads/preview/{sid}/extend", params={"hours": 6})
    assert resp.status_code == 200
    assert resp.json()["extended_by_hours"] == 6


def test_extend_unknown_session_404(client):
    assert client.get("/uploads/preview/nope/extend").status_code == 404


def test_cancel_session_204(client, fake_redis, test_user):
    sid = _seed_session(fake_redis, test_user.db_id)
    assert client.delete(f"/uploads/preview/{sid}").status_code == 204
    assert client.get(f"/uploads/preview/{sid}").status_code == 404


def test_cancel_unknown_session_404(client):
    assert client.delete("/uploads/preview/nope").status_code == 404
