"""demo_guard.enforce_demo_upload_allowlist (#82).

The DEMO_MODE upload allowlist is the one demo-gated backend control: off by
default (home server), and in demo mode it 403s any upload that isn't a known
synthetic sample. The committed sample fixtures must hash into the allowlist.
"""
import pytest
from fastapi import HTTPException

from src.services.demo_guard import (
    DEMO_SAMPLE_SHA256,
    demo_mode_enabled,
    enforce_demo_upload_allowlist,
)

SAMPLE_BYTES = open("tests/parsers/fixtures/amex_sample.csv", "rb").read()


def test_no_op_when_demo_mode_unset(monkeypatch):
    monkeypatch.delenv("DEMO_MODE", raising=False)
    # Arbitrary bytes pass when the flag is off — guard is inert on home server.
    enforce_demo_upload_allowlist(b"anything at all")


@pytest.mark.parametrize("val,expected", [
    ("1", True), ("true", True), ("TRUE", True), ("yes", True), ("on", True),
    ("0", False), ("false", False), ("", False), ("off", False),
])
def test_demo_mode_enabled_parsing(monkeypatch, val, expected):
    monkeypatch.setenv("DEMO_MODE", val)
    assert demo_mode_enabled() is expected


def test_non_sample_upload_rejected_in_demo_mode(monkeypatch):
    monkeypatch.setenv("DEMO_MODE", "1")
    with pytest.raises(HTTPException) as exc:
        enforce_demo_upload_allowlist(b"a totally different file")
    assert exc.value.status_code == 403


def test_committed_sample_passes_in_demo_mode(monkeypatch):
    monkeypatch.setenv("DEMO_MODE", "1")
    # The real fixture bytes the FE bundles must be accepted.
    enforce_demo_upload_allowlist(SAMPLE_BYTES)


def test_allowlist_covers_the_three_samples():
    import hashlib
    import glob

    on_disk = {
        hashlib.sha256(open(f, "rb").read()).hexdigest()
        for f in glob.glob("tests/parsers/fixtures/*_sample.csv")
    }
    # Every allowlisted hash corresponds to a committed sample fixture.
    assert DEMO_SAMPLE_SHA256 <= on_disk
