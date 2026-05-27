"""job_runner service — SessionLocal, the snapshot recalc, and threading mocked.

The worker opens its own SessionLocal (not the request session) and the
ThreadJobRunner spawns a daemon thread, so tests patch SessionLocal to an
isolated in-memory engine, stub recalculate_account_snapshots, and replace
threading.Thread. Covers the runner factory, the Celery stub, thread
submission, worker success/not-found/failure status transitions, and
interrupted-job recovery on startup.
"""
from datetime import date, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from src.db.core import Base, SnapshotBackfillJobDB
from src.services import job_runner as jr
from tests.factories import make_account, make_user


@pytest.fixture
def job_env(monkeypatch):
    # Isolated in-memory engine so the worker's own SessionLocal() and close()
    # never touch the request-scoped `db` fixture. FK enforcement is on for all
    # sqlite engines here, so seed a real user+account for the job's FKs.
    eng = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(eng)
    Session = sessionmaker(bind=eng)
    s = Session()
    user = make_user(s)
    account = make_account(s, user)
    s.commit()
    uid, aid = user.db_id, account.id
    s.close()
    monkeypatch.setattr(jr, "SessionLocal", Session)
    yield Session, uid, aid
    eng.dispose()


def _seed_job(Session, uid, aid, status="PENDING"):
    s = Session()
    job = SnapshotBackfillJobDB(
        user_id=uid, account_id=aid, start_date=date(2026, 1, 1), end_date=date(2026, 1, 31), status=status
    )
    s.add(job)
    s.commit()
    job_id = job.id
    s.close()
    return job_id


def _get_job(Session, job_id):
    s = Session()
    try:
        return s.query(SnapshotBackfillJobDB).filter(SnapshotBackfillJobDB.id == job_id).first()
    finally:
        s.close()


# ===== factory / stubs =====

def test_get_job_runner_defaults_to_thread():
    assert isinstance(jr.get_job_runner(), jr.ThreadJobRunner)


def test_celery_runner_not_implemented():
    with pytest.raises(NotImplementedError):
        jr.CeleryJobRunner().submit_job(1, 1, date(2026, 1, 1), date(2026, 1, 2))


def test_thread_runner_spawns_worker_thread(monkeypatch):
    fake_thread_cls = MagicMock()
    monkeypatch.setattr(jr.threading, "Thread", fake_thread_cls)
    jr.ThreadJobRunner().submit_job(7, 3, date(2026, 1, 1), date(2026, 1, 31))

    fake_thread_cls.assert_called_once()
    assert fake_thread_cls.call_args.kwargs["target"] is jr.run_backfill_worker
    assert fake_thread_cls.call_args.kwargs["args"] == (7, 3, date(2026, 1, 1), date(2026, 1, 31))
    fake_thread_cls.return_value.start.assert_called_once()


# ===== run_backfill_worker =====

def test_worker_completes_and_records_results(job_env, monkeypatch):
    Session, uid, aid = job_env
    monkeypatch.setattr(jr, "recalculate_account_snapshots",
                        lambda **k: {"created": 2, "updated": 1, "failed": 0, "skipped": 3})
    job_id = _seed_job(Session, uid, aid)

    jr.run_backfill_worker(job_id, aid, date(2026, 1, 1), date(2026, 1, 31))

    job = _get_job(Session, job_id)
    assert job.status == "COMPLETED"
    assert job.snapshots_created == 2
    assert job.snapshots_skipped == 3
    assert job.completed_at is not None


def test_worker_missing_job_is_noop(job_env, monkeypatch):
    Session, uid, aid = job_env
    recalc = MagicMock()
    monkeypatch.setattr(jr, "recalculate_account_snapshots", recalc)
    # No job seeded — worker should log and return without calling recalc.
    jr.run_backfill_worker(999, aid, date(2026, 1, 1), date(2026, 1, 31))
    recalc.assert_not_called()


def test_worker_failure_marks_job_failed(job_env, monkeypatch):
    Session, uid, aid = job_env

    def _boom(**kwargs):
        raise RuntimeError("snapshot blew up")
    monkeypatch.setattr(jr, "recalculate_account_snapshots", _boom)
    job_id = _seed_job(Session, uid, aid)

    jr.run_backfill_worker(job_id, aid, date(2026, 1, 1), date(2026, 1, 31))

    job = _get_job(Session, job_id)
    assert job.status == "FAILED"
    assert "snapshot blew up" in (job.error_message or "")


def test_recover_interrupted_jobs(job_env):
    Session, uid, aid = job_env
    pending = _seed_job(Session, uid, aid, status="PENDING")
    in_progress = _seed_job(Session, uid, aid, status="IN_PROGRESS")
    completed = _seed_job(Session, uid, aid, status="COMPLETED")

    jr.recover_interrupted_jobs()

    assert _get_job(Session, pending).status == "FAILED"
    assert _get_job(Session, in_progress).status == "FAILED"
    assert _get_job(Session, completed).status == "COMPLETED"
