"""Structural integration tests for the investment PDF parsers.

These run against REAL statements under the gitignored
``tests/parsers/fixtures/local/<institution>/`` directory and skip when none
are present, so the suite stays green on a clean checkout / CI. That folder is a
curated, intentional local corpus (distinct from the app's ``input/`` working
dir) — see tests/parsers/fixtures/README.md.

Because the PDFs are real and these test files ARE committed, the assertions are
deliberately STRUCTURAL — transaction count, normalized type, OCC option-symbol
shape, value types, date sanity, account-tail format — and never pin specific
amounts, dates, or symbols, so no personal financial data lands in git.

Marked ``slow`` (pdfplumber): exclude with ``-m "not slow"``.
"""
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from src.parser import ameriprise, schwab, tdameritrade
from src.parser.models import SecurityType
from src.services.price_fetcher import is_option_symbol

pytestmark = [pytest.mark.parser, pytest.mark.integration, pytest.mark.slow]

_LOCAL = Path(__file__).parent / "parsers" / "fixtures" / "local"

_PARSERS = {
    "schwab": schwab,
    "tdameritrade": tdameritrade,
    "ameriprise": ameriprise,
}

# Union of normalized types the investment parsers emit.
_KNOWN_TYPES = {
    "BUY", "SELL", "DIVIDEND", "INTEREST", "FEE", "REINVESTMENT",
    "TRANSFER_IN", "TRANSFER_OUT", "EXPIRATION", "SPLIT", "OTHER",
}


def _pdfs(institution: str):
    return sorted(_LOCAL.joinpath(institution).glob("*.pdf"))


def _parse_all(institution: str):
    """Parse every local PDF once; skip the test if the institution has none."""
    pdfs = _pdfs(institution)
    if not pdfs:
        pytest.skip(f"no {institution} PDFs under tests/parsers/fixtures/local/{institution}/")
    return [(pdf, _PARSERS[institution].parse(pdf, is_csv=False)) for pdf in pdfs]


@pytest.mark.parametrize("institution", list(_PARSERS))
def test_real_pdfs_parse_with_valid_structure(institution):
    total = 0
    for pdf, data in _parse_all(institution):
        for t in data.investment_transactions:
            total += 1
            assert t.transaction_type in _KNOWN_TYPES, f"{pdf.name}: bad type {t.transaction_type!r}"
            assert isinstance(t.total_amount, Decimal)
            assert isinstance(t.transaction_date, date)
            assert 2000 <= t.transaction_date.year <= 2100, f"{pdf.name}: implausible year"
            # quantity may be negative (closing/short positions show in parens);
            # only the type matters here — sign is normalized later at confirm.
            assert t.quantity is None or isinstance(t.quantity, Decimal)
            assert t.price_per_share is None or isinstance(t.price_per_share, Decimal)
            # Options must carry a well-formed OCC api_symbol when one is set.
            if t.security_type == SecurityType.OPTION and t.api_symbol:
                assert is_option_symbol(t.api_symbol), f"{pdf.name}: bad OCC {t.api_symbol!r}"

    assert total > 0, f"{institution}: parsed zero transactions from local PDF(s)"


@pytest.mark.parametrize("institution", list(_PARSERS))
def test_real_pdfs_stock_qty_price_reconciles(institution):
    """#72: for non-option equity trades, quantity*price must reconcile with the
    amount. A column-boundary spill (wide price losing its leading digit to the
    quantity cell) breaks this — structural, no values pinned."""
    checked = 0
    for pdf, data in _parse_all(institution):
        for t in data.investment_transactions:
            if (t.transaction_type in ("BUY", "SELL", "REINVESTMENT")
                    and t.security_type != SecurityType.OPTION
                    and t.quantity and t.price_per_share and t.total_amount):
                checked += 1
                target = abs(t.total_amount)
                tol = max(target * Decimal("0.01"), Decimal("1"))
                assert abs(t.quantity * t.price_per_share - target) <= tol, (
                    f"{pdf.name}: {t.transaction_type} {t.symbol} "
                    f"qty*price ({t.quantity}*{t.price_per_share}) != amount ({target})"
                )
    assert checked > 0, f"{institution}: no non-option equity trades to reconcile"


@pytest.mark.parametrize("institution", list(_PARSERS))
def test_real_pdfs_account_last4(institution):
    last4s = [
        data.account_info.account_number_last4
        for _, data in _parse_all(institution)
        if data.account_info is not None
    ]
    if last4s:
        assert all(s.isdigit() and len(s) == 4 for s in last4s)
