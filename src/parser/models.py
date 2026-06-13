import re
from pydantic import BaseModel, Field
from typing import List, Optional, Tuple
from datetime import date
from decimal import Decimal, InvalidOperation
from enum import Enum

from src.logging_config import get_logger

logger = get_logger(__name__)


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


def recover_misaligned_qty_price(
    quantity: Decimal, price: Decimal, target: Decimal, tol: Decimal
) -> Optional[Tuple[Decimal, Decimal]]:
    """Recover a quantity/price pair split across the wrong column boundary (#72).

    The table parsers map each row's Quantity/Price/Amount to fixed pixel columns.
    A wide price (e.g. '3,283.0201') can have its leading integer digit fall just
    left of the Quantity|Price boundary, so '3 | 3283.0201' is extracted as
    '33 | 283.0201'. The digits are intact, only the split moved. Using the
    trustworthy amount as ground truth, walk the misplaced leading digit(s) back
    from the quantity onto the front of the price's integer part and return the
    first split whose |quantity*price| reconciles with the amount. The quantity's
    sign (negative for sells in some statements, e.g. Schwab) is preserved.
    """
    sign = Decimal(-1) if quantity < 0 else Decimal(1)
    qty_digits = str(abs(int(quantity)))
    price_int, _, price_frac = format(abs(price), 'f').partition('.')
    for k in range(1, len(qty_digits)):
        cand_qty = Decimal(qty_digits[:-k])
        moved = qty_digits[-k:]
        cand_price_str = f"{moved}{price_int}.{price_frac}" if price_frac else f"{moved}{price_int}"
        cand_price = Decimal(cand_price_str)
        if abs(cand_qty * cand_price - target) <= tol:
            return sign * cand_qty, cand_price
    return None


def reconcile_equity_qty_price(
    quantity: Decimal, price: Decimal, amount: Decimal, context: str = ""
) -> Tuple[Decimal, Decimal]:
    """Validate a non-option equity trade's quantity*price against its amount,
    repairing a column-boundary digit-spill when possible (#72).

    The amount column is trustworthy, so |quantity*price| must reconcile with
    |amount|. When it doesn't, attempt to recover a misaligned split (see
    recover_misaligned_qty_price); if that fails, raise StatementParseError so the
    statement fails loudly rather than persisting a corrupt holding.

    The caller restricts this to non-option equity: only stocks reach the 4-digit
    prices that trigger the spill, and option contracts display a 2-decimal-
    rounded price that won't reconcile tightly with the x100 contract amount.
    Returns the (possibly corrected) (quantity, price).
    """
    target = abs(amount)
    tol = max(target * Decimal('0.01'), Decimal('1'))
    if abs(abs(quantity * price) - target) <= tol:
        return quantity, price
    recovered = None
    if quantity == quantity.to_integral_value():
        recovered = recover_misaligned_qty_price(quantity, price, target, tol)
    if recovered is None:
        raise StatementParseError(
            f"{context}: quantity*price does not reconcile with amount "
            f"(quantity={quantity}, price={price}, amount={amount})"
        )
    return recovered


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

class ReconciliationResult(BaseModel):
    """Outcome of checking parsed rows against a statement's own control totals.

    ``reconciled=False`` is a *non-fatal* warning the import surfaces (a yellow
    badge), not a block — the rows still import. Carries the numbers so the UI
    can show "off by $X". An unclassified transaction type never reaches here;
    that raises ``StatementParseError`` instead (a parser bug, not statement drift).
    """
    reconciled: bool
    expected_net_change: Decimal
    parsed_net: Decimal
    delta: Decimal
    detail: str = ""


class ParsedData(BaseModel):
    account_info: Optional[ParsedAccountInfo] = None
    transactions: List[ParsedTransaction] = Field(default_factory=list)
    investment_transactions: List[ParsedInvestmentTransaction] = Field(default_factory=list)
    # Set by parsers that have statement control totals (PDF begin/end balance);
    # None when not checked (e.g. CSVs). reconciled=False → import-with-warning.
    reconciliation: Optional[ReconciliationResult] = None


def reconcile_statement_balance(
    transactions: List[ParsedTransaction],
    *,
    expected_net_change: Decimal,
    credit_types: frozenset,
    debit_types: frozenset,
    context: str = "",
    tolerance: Decimal = Decimal("0.01"),
) -> ReconciliationResult:
    """Check parsed rows against a statement's own control totals.

    The statement prints a trustworthy net balance move (e.g. a checking
    statement's ``EndingBalance - BeginningBalance``); the parsed rows must sum to
    it. ``credit_types`` move the balance up by ``abs(amount)``, ``debit_types``
    move it down, both in the statement's own balance convention (for a credit
    card that's debt: charges are credits, payments are debits).

    Two distinct outcomes, deliberately handled differently (todo #78):

    - **Numeric mismatch** (rows don't sum to the statement's net move): returned
      as ``reconciled=False`` — a *non-fatal* warning. Most likely a dropped or
      duplicated row; the user can re-check, so we import-and-flag rather than
      block a possibly-benign edge case.
    - **Unclassified transaction type** (a type in neither set): raises
      ``StatementParseError``. That's a parser/coverage bug, not statement drift —
      it can't be reconciled at all, and silently skipping it is exactly how a
      dropped row hides. Fail loud so it gets fixed.

    Callers invoke this only when control totals were found (skip otherwise, e.g.
    CSVs with no balances).
    """
    net = Decimal("0")
    for t in transactions:
        tt = t.transaction_type.upper()
        amt = abs(t.amount)
        if tt in credit_types:
            net += amt
        elif tt in debit_types:
            net -= amt
        else:
            raise StatementParseError(
                f"{context}: transaction type {t.transaction_type!r} is not "
                f"classified for balance reconciliation (credit or debit)"
            )
    delta = net - expected_net_change
    reconciled = abs(delta) <= tolerance
    detail = ""
    if not reconciled:
        detail = (
            f"{context}: parsed transactions net to {net:+.2f} but the statement "
            f"balance moved {expected_net_change:+.2f} (off by {delta:+.2f}) — a "
            f"transaction was likely dropped or duplicated during parsing"
        )
        logger.warning(detail)
    return ReconciliationResult(
        reconciled=reconciled,
        expected_net_change=expected_net_change,
        parsed_net=net,
        delta=delta,
        detail=detail,
    )
