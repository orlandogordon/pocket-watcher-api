"""Guard: no committed PDF fixture may contain PII, plus unit tests for the gate.

The parametrized test runs the sanitization gate (scripts/verify_pdf_sanitized.py)
over every `tests/parsers/fixtures/*.pdf` with an empty denylist, so a fixture
with leftover PII metadata or an SSN-shaped value can never pass CI. (Denylist
checking is runtime-only — your real values are never committed.) The remaining
tests exercise the gate's detection logic directly with synthetic strings, so it
is verified without needing a real PDF on hand.
"""
import importlib.util
from pathlib import Path

import pytest

pytestmark = pytest.mark.parser

_GATE_PATH = Path(__file__).parent.parent / "scripts" / "verify_pdf_sanitized.py"
_spec = importlib.util.spec_from_file_location("verify_pdf_sanitized", _GATE_PATH)
gate = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gate)

_FIXTURE_PDFS = sorted((Path(__file__).parent / "parsers" / "fixtures").glob("*.pdf"))


@pytest.mark.parametrize("pdf", _FIXTURE_PDFS, ids=[p.name for p in _FIXTURE_PDFS])
def test_committed_pdf_fixture_has_no_pii(pdf):
    fails = [str(f) for f in gate.verify(pdf, deny=[]) if f.level == "FAIL"]
    assert not fails, "PII gate failed:\n" + "\n".join(fails)


# ----- gate detection logic (no PDF needed) -----

def _fails(findings):
    return [f.detail for f in findings if f.level == "FAIL"]


def test_scan_text_flags_denied_value():
    findings = gate.scan_text("t", "Statement for John Smith", deny=["John Smith"])
    assert any("denied value" in d for d in _fails(findings))


def test_scan_text_matches_digits_across_grouping():
    # Account printed as "4938 9145" must match a denylist value of "49389145".
    findings = gate.scan_text("t", "Acct 4938 9145 ending", deny=["49389145"])
    assert any("grouped" in d for d in _fails(findings))


def test_scan_text_flags_ssn_without_denylist():
    findings = gate.scan_text("t", "SSN 123-45-6789 on file", deny=[])
    assert any("SSN" in d for d in _fails(findings))


def test_clean_text_passes():
    findings = gate.scan_text("t", "VANGUARD S&P 500 ETF dividend $12.34", deny=["John Smith"])
    assert _fails(findings) == []


def test_scan_metadata_flags_author_and_title():
    findings = gate.scan_metadata({"Author": "John Smith", "Title": "Statement_4938"}, deny=[])
    fails = _fails(findings)
    assert len(fails) == 2


def test_scan_metadata_software_fields_are_warnings_not_fails():
    findings = gate.scan_metadata({"Producer": "pdfTeX-1.40", "Creator": "LaTeX"}, deny=[])
    assert _fails(findings) == []
    assert all(f.level == "WARN" for f in findings)
