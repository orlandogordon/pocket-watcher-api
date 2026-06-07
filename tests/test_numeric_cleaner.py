"""Unit tests for the shared statement numeric cleaner (#71).

The bug: a ``'$ (1,234.56)'`` cell dropped/crashed because a bare
``.replace('$','')`` left the space between the sign and the digits, so
``Decimal('- 1234.56')`` raised — and on the amount path that throw silently
dropped the whole transaction row. ``clean_decimal`` is the robust replacement.

All values below are synthetic.
"""
from decimal import Decimal

import pytest

from src.parser.models import clean_decimal

pytestmark = pytest.mark.parser


@pytest.mark.parametrize("raw, expected", [
    # The exact #71 regression: spaced-$ parenthesized negative w/ separator.
    ("$ (1,234.56)", Decimal("-1234.56")),
    ("$ 7,777.00", Decimal("7777.00")),
    ("($960.00)", Decimal("-960.00")),
    ("-$960.00", Decimal("-960.00")),
    ("- 1234.56", Decimal("-1234.56")),
    ("$0.09", Decimal("0.09")),
    ("1,234.56", Decimal("1234.56")),
    ("3", Decimal("3")),
])
def test_parses_currency_variants(raw, expected):
    assert clean_decimal(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "   ", "-", "None", "N/A", "--"])
def test_returns_none_for_non_numbers(raw):
    assert clean_decimal(raw) is None


def test_accepts_already_decimal_like_input():
    assert clean_decimal(Decimal("12.50")) == Decimal("12.50")
    assert clean_decimal(42) == Decimal("42")
