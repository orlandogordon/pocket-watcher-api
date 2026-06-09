"""#67 — every API datetime serializes as UTC with a `Z` suffix.

Columns store naive UTC; the offset is stamped at the Pydantic edge via
`src.utils.time.UTCDateTime`. These guard that the wire format carries the
timezone so the frontend can format to local time without guessing.
"""
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel

from src.utils.time import UTCDateTime, to_utc_iso, utcnow


def test_utcnow_is_naive_utc():
    now = utcnow()
    assert now.tzinfo is None  # stored naive (no Postgres session-tz shift)


def test_to_utc_iso_assumes_utc_for_naive():
    assert to_utc_iso(datetime(2026, 6, 8, 22, 15, 0)) == "2026-06-08T22:15:00Z"


def test_to_utc_iso_converts_aware_to_utc():
    edt = timezone(timedelta(hours=-4))
    assert to_utc_iso(datetime(2026, 6, 8, 18, 15, 0, tzinfo=edt)) == "2026-06-08T22:15:00Z"


def test_utcdatetime_field_serializes_with_z():
    class M(BaseModel):
        ts: UTCDateTime

    assert M(ts=datetime(2026, 6, 8, 22, 15, 0)).model_dump_json() == '{"ts":"2026-06-08T22:15:00Z"}'


def test_tag_response_created_at_has_z(client):
    resp = client.post("/tags/", json={"tag_name": "TZCheck"})
    assert resp.status_code == 201
    created_at = resp.json()["created_at"]
    assert created_at.endswith("Z")
    # Parses back to a real instant (no off-by-offset shift).
    datetime.fromisoformat(created_at.replace("Z", "+00:00"))


def test_account_response_timestamps_have_z(client):
    body = {
        "account_name": "TZ Account",
        "account_type": "CHECKING",
        "institution_name": "TZ Bank",
        "balance": "100.00",
    }
    resp = client.post("/accounts/", json=body)
    assert resp.status_code == 201
    data = resp.json()
    assert data["created_at"].endswith("Z")
    assert data["updated_at"].endswith("Z")
