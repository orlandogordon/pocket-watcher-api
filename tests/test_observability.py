"""Tests for the logging/observability layer (#C3): request_id propagation,
the access-log middleware, slow-query logging, and the global error handlers.

The `pocket_watcher` logger has propagate=False, so pytest's `caplog` (which
hangs off the root logger) can't see its records. `pw_logs` attaches a handler
directly to `pocket_watcher` and collects the LogRecords instead.
"""
import logging

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import text

import src.db.core as db_core
from src.main import app


@pytest.fixture
def pw_logs():
    logger = logging.getLogger("pocket_watcher")
    records: list[logging.LogRecord] = []

    class _Capture(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = _Capture()
    logger.addHandler(handler)
    try:
        yield records
    finally:
        logger.removeHandler(handler)


def _by_message(records, message):
    return [r for r in records if r.getMessage() == message]


def test_response_carries_request_id_header(client):
    resp = client.get("/accounts/")
    assert resp.status_code == 200
    assert resp.headers.get("X-Request-ID")


def test_inbound_request_id_is_honored(client):
    resp = client.get("/accounts/", headers={"X-Request-ID": "trace-me-123"})
    assert resp.headers.get("X-Request-ID") == "trace-me-123"


def test_completion_log_has_request_id_and_status(client, pw_logs):
    client.get("/accounts/", headers={"X-Request-ID": "corr-42"})
    completes = _by_message(pw_logs, "request.complete")
    assert completes, "expected a request.complete log"
    rec = completes[-1]
    assert rec.request_id == "corr-42"
    assert rec.status_code == 200
    assert isinstance(rec.duration_ms, float)


def test_slow_query_logged_when_over_threshold(db, pw_logs, monkeypatch):
    # Force every statement to count as slow.
    monkeypatch.setattr(db_core, "SLOW_QUERY_MS", 0.0)
    db.execute(text("SELECT 1"))
    assert _by_message(pw_logs, "db.slow_query"), "expected a db.slow_query log"


def test_fast_query_not_logged(db, pw_logs, monkeypatch):
    monkeypatch.setattr(db_core, "SLOW_QUERY_MS", 10_000.0)
    db.execute(text("SELECT 1"))
    assert not _by_message(pw_logs, "db.slow_query")


def test_validation_error_is_logged(client, pw_logs):
    resp = client.get("/accounts/not-a-valid-uuid")
    assert resp.status_code == 422
    assert _by_message(pw_logs, "validation_error")


def test_unhandled_exception_returns_500_and_logs_traceback(pw_logs):
    def _boom():
        raise RuntimeError("boom-for-test")

    app.add_api_route("/_boom_test", _boom, methods=["GET"])
    try:
        unsafe_client = TestClient(app, raise_server_exceptions=False)
        resp = unsafe_client.get("/_boom_test", headers={"X-Request-ID": "boom-rid"})
        assert resp.status_code == 500
        assert resp.json() == {"detail": "Internal Server Error"}

        handled = _by_message(pw_logs, "unhandled_exception")
        assert handled, "expected an unhandled_exception log"
        rec = handled[-1]
        assert rec.exc_info is not None  # full traceback captured
        # request_id is bridged from request.state since the 500 handler runs
        # above the request-context middleware.
        assert rec.request_id == "boom-rid"
    finally:
        app.router.routes = [
            r for r in app.router.routes if getattr(r, "path", None) != "/_boom_test"
        ]
