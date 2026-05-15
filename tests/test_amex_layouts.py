"""Regression tests for src/parser/amex.py against known-good PDF layouts.

Each test asserts the parser's per-transaction-type sums match the values
read from the statement's body subtotals. This catches silent banner-detection
bugs that drop entire categories of rows.

Skips gracefully if input/personal_seed/amex-gold-1005/ PDFs aren't present.
"""
from collections import defaultdict
from decimal import Decimal
from pathlib import Path
import unittest

AMEX_PDF_DIR = Path(__file__).parent.parent / "input" / "personal_seed" / "amex-gold-1005"


def _sums_by_type(pdf_path: Path) -> dict:
    from src.parser.amex import parse_statement
    parsed = parse_statement(str(pdf_path))
    sums: dict = defaultdict(lambda: Decimal("0"))
    for t in parsed.transactions:
        sums[t.transaction_type] += Decimal(t.amount)
    return dict(sums)


class TestAmexLayouts(unittest.TestCase):
    """One test per distinct layout variant we've observed in the wild."""

    def _check(self, fname: str, expected: dict):
        pdf = AMEX_PDF_DIR / fname
        if not pdf.exists():
            self.skipTest(f"{pdf} not present")
        sums = _sums_by_type(pdf)
        for ttype, expected_amt in expected.items():
            actual = sums.get(ttype, Decimal("0"))
            self.assertEqual(
                actual, expected_amt,
                f"{fname}: {ttype} sum mismatch — expected {expected_amt}, got {actual}",
            )

    def test_pre_2025_layout(self):
        # "Payments t Amount" / "Credits Amount" banners (the original layout).
        self._check("2023-03-15.pdf", {
            "TRANSFER_IN": Decimal("2046.79"),
            "Credit":      Decimal("512.20"),
            "Purchase":    Decimal("1919.96"),
            "Fee":         Decimal("0"),
            "Interest":    Decimal("0"),
        })

    def test_post_2024_layout(self):
        # "Payments Details" / "Credits Details" banners. Pre-fix the parser
        # silently dropped every payment and credit row on this layout.
        self._check("2025-03-14.pdf", {
            "TRANSFER_IN": Decimal("2500.00"),
            "Credit":      Decimal("658.10"),
            "Purchase":    Decimal("2906.88"),
            "Fee":         Decimal("0"),
            "Interest":    Decimal("0"),
        })

    def test_annual_membership_fee_layout(self):
        # "Fees ⧫ - Pay Over Time and/or Cash Advance activity" banner. Pre-fix
        # the $250 annual membership fee row was mis-categorized as Purchase.
        self._check("2024-07-15.pdf", {
            "TRANSFER_IN": Decimal("2921.90"),
            "Credit":      Decimal("598.19"),
            "Purchase":    Decimal("4706.46"),
            "Fee":         Decimal("250.00"),
            "Interest":    Decimal("0"),
        })

    def test_page_break_and_cash_advance_layout(self):
        # 2025-02-12 exercises three independent fixes at once:
        #   (1) page-joining: "Fees" was the last line of a page; the parser
        #       was concatenating it onto the next page's "Date Description..."
        #       producing "FeesDate Description Type Amount", which defeated
        #       the exact-match banner trigger.
        #   (2) Cash Advances: the section banner "Cash Advances" was unrecognized,
        #       so the cash-advance row inherited tracking_purchases from the
        #       previous section and was mis-categorized.
        #   (3) Post-2024 banner names (Payments/Credits Details).
        # Purchase = Total New Charges ($2,716.39) + Total Cash Advances ($20.00).
        self._check("2025-02-12.pdf", {
            "TRANSFER_IN": Decimal("2776.48"),
            "Credit":      Decimal("127.78"),
            "Purchase":    Decimal("2736.39"),
            "Fee":         Decimal("10.00"),
            "Interest":    Decimal("0.39"),
        })


if __name__ == "__main__":
    unittest.main()
