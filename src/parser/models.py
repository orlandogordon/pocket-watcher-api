import re
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum


class StatementParseError(Exception):
    """A real transaction row could not be parsed.

    Raised by the parsers to fail the *whole* statement instead of silently
    dropping the offending row. Silently skipping a financial transaction yields
    a wrong-but-plausible account (off holdings/balance, no signal), the worst
    outcome for financial data — and imports are atomic + dedup-safe, so
    fix-parser → re-run is cheap (#71).
    """


_DECIMAL_STRIP_RE = re.compile(r"[^0-9.]")


def clean_decimal(raw) -> Optional[Decimal]:
    """Parse a currency/numeric statement cell into a ``Decimal``.

    Handles ``$``, thousands commas, and — unlike a bare ``.replace('$','')`` —
    whitespace that lands *between* a sign and the digits (e.g. ``'$ (10,431.00)'``
    or ``'- 10431.00'``), plus parenthesized negatives. Returns ``None`` when the
    cell carries no number (empty, ``'-'``, ``'None'``, or no digits); callers
    decide whether that null is allowed (cash-type quantity) or a hard parse
    error (#71).
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if not s or s == "-":
        return None
    negative = "(" in s or s.lstrip().startswith("-")
    digits = _DECIMAL_STRIP_RE.sub("", s)
    if not digits or digits == ".":
        return None
    try:
        value = Decimal(digits)
    except InvalidOperation:
        return None
    return -value if negative else value

class SecurityType(str, Enum):
    """Type of security for investment transactions"""
    STOCK = "STOCK"
    ETF = "ETF"
    MUTUAL_FUND = "MUTUAL_FUND"
    OPTION = "OPTION"
    FUTURE = "FUTURE"
    BOND = "BOND"
    CRYPTO = "CRYPTO"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    INTEREST = "INTEREST"
    DIVIDEND = "DIVIDEND"
    FEE = "FEE"
    ADJUSTMENT = "ADJUSTMENT"
    OTHER = "OTHER"


# Common ETF tickers — extend as needed
KNOWN_ETFS = frozenset({
    # Broad market
    "SPY", "VOO", "VTI", "IVV", "QQQ", "QQQM", "DIA", "IWM", "VT", "VXUS",
    # Sector
    "XLF", "XLK", "XLE", "XLV", "XLI", "XLU", "XLP", "XLY", "XLC", "XLRE", "XLB", "SOXX",
    # Bond
    "BND", "AGG", "TLT", "IEF", "SHY", "TIP", "LQD", "HYG", "JNK", "VCIT", "VCSH",
    # International
    "EFA", "EEM", "VWO", "IEMG", "VEA", "IXUS", "FNDE",
    # Real estate
    "VNQ", "VNQI", "IYR", "SCHH",
    # Commodity
    "GLD", "SLV", "IAU", "USO", "GDX", "GDXJ",
    # Dividend
    "VYM", "SCHD", "HDV", "DVY", "SDY", "DGRO",
    # Growth/Value
    "VUG", "VTV", "IWF", "IWD", "VOOG", "VOOV",
    # Small/Mid cap
    "IJR", "IJH", "VB", "VO", "MDY",
    # Thematic
    "ARKK", "ARKW", "ARKG", "ARKF", "ARKQ",
    # Leveraged/Inverse (common ones)
    "TQQQ", "SQQQ", "SPXL", "SPXS", "UVXY", "SOXL", "SOXS",
})

# Common mutual fund patterns — tickers ending in X with 5 chars
KNOWN_MUTUAL_FUNDS = frozenset({
    "VTSAX", "VTIAX", "VBTLX", "VFIAX", "VSMPX", "VEXAX",
    "FXAIX", "FSKAX", "FTIHX", "FBNDX",
    "SWTSX", "SWPPX", "SWISX",
})


def classify_security_type(symbol: str, is_option: bool = False) -> SecurityType:
    """Classify a symbol as STOCK, ETF, MUTUAL_FUND, or OPTION."""
    if is_option:
        return SecurityType.OPTION
    upper = symbol.upper()
    if upper in KNOWN_ETFS:
        return SecurityType.ETF
    if upper in KNOWN_MUTUAL_FUNDS or (len(upper) == 5 and upper.endswith("X") and upper.isalpha()):
        return SecurityType.MUTUAL_FUND
    return SecurityType.STOCK


class ParsedTransaction(BaseModel):
    transaction_date: date
    description: str
    amount: Decimal
    transaction_type: str
    is_duplicate: bool = False
    # Set by a parser when the source truncated the merchant name mid-word (the
    # Amex activity-CSV fixed-width export does this). The real brand is
    # unrecoverable, so downstream merchant extraction is skipped and the
    # merchant is left blank → Needs Review. Default False for every other path.
    merchant_truncated: bool = False

class ParsedInvestmentTransaction(BaseModel):
    transaction_date: date
    transaction_type: str
    symbol: Optional[str]
    api_symbol: Optional[str] = None  # Symbol for API calls (yfinance format)
    description: str
    quantity: Optional[Decimal]
    price_per_share: Optional[Decimal]
    total_amount: Decimal
    is_duplicate: bool = False
    security_type: Optional[SecurityType] = None

class ParsedAccountInfo(BaseModel):
    account_number_last4: str

class ParsedData(BaseModel):
    account_info: Optional[ParsedAccountInfo] = None
    transactions: List[ParsedTransaction] = Field(default_factory=list)
    investment_transactions: List[ParsedInvestmentTransaction] = Field(default_factory=list)
