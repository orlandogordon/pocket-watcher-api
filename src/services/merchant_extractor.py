"""
Per-institution merchant extraction.

Returns either a substring of the raw description or a value from a hand-curated
alias table — never an invented brand. When the row's shape is ambiguous,
unrecognized, or contains no real brand (bare addresses, generic descriptors,
parser-corrupted tokens), returns None and the caller falls through to the LLM.

The safety property: ``extract_merchant``'s output space is closed:
``{substrings of input} ∪ {alias values}``. Hallucination is structurally
impossible. See backend todo #35 for the full rationale.
"""

from __future__ import annotations

import re
from typing import Callable, Optional


# ---------- alias table (exact-match on normalized input) ----------
#
# Recurring institutional payees whose raw description is a fixed string.
# Match is exact after normalization (whitespace-collapse, uppercase). Add
# entries here when a new fixed-string payee shows up across institutions.

_MERCHANT_ALIASES: dict[str, str] = {
    # TD Bank ACH/electronic shapes
    "HESAA PAYMENT": "HESAA",
    "AMEX EPAYMENT ACH PMT": "American Express",
    "AMZ_STORECRD_PMT PAYMENT": "Amazon",
    "DEPT EDUCATION STUDENT LN": "Dept of Education",
    "CRUNCH FIT CLUB FEES": "Crunch Fitness",
    "SCHWAB BROKERAGE MONEYLINK": "Schwab",
    "PNC BANK NA PAYROLL": "PNC Bank",
    "PAYPAL TRANSFER": "PayPal",
    "STATE OF N.J. NJSTTAXRFD": "State of NJ",
    "IRS TREAS 310 TAX REF": "IRS",
    "NYC FINANCE PARKING TK": "NYC Finance",
    # Amazon Synchrony fixed payment shapes
    "ONLINE PYMT-THANK YOU ATLANTA GA": None,  # generic — explicit None
    "MOBILE PAYMENT - THANK YOU": None,
    "ONLINE PAYMENT - THANK YOU": None,
}


# ---------- generic descriptors that are NEVER a merchant ----------
#
# Substring match (uppercase). Anything in this set returns None — these are
# transaction-type labels, not brands.

_GENERIC_DESCRIPTORS: tuple[str, ...] = (
    "ANNUAL MEMBERSHIP FEE",
    "CHARGE ON PURCHASES",
    "INTEREST CHARGE",
    "ASSET-BASED BILL",
    "FDIC INSURED",
    "ELECTRONIC FUNDING",
    "ANNUAL FEE",
)


# ---------- wrapped-merchant patterns ----------
#
# Payment processors that prefix the real merchant. Capture the wrapped name.

_WRAPPED_PATTERNS: tuple[re.Pattern, ...] = (
    re.compile(r"^PAYPAL\s*\*\s*(?P<merchant>.+?)\s*(?:\d{10}|$)"),
    re.compile(r"^SQ\s*\*\s*(?P<merchant>.+?)\s*(?:[A-Z]{2}\s*$|$)"),
    re.compile(r"^TST\s*\*\s*(?P<merchant>.+?)\s*(?:[A-Z]{2}\s*$|$)"),
    re.compile(r"^STRIPE\s*\*\s*(?P<merchant>.+?)\s*(?:[A-Z]{2}\s*$|$)"),
)


# ---------- shape rejectors ----------

# A captured merchant slot that begins with "<digits> <UPPERCASE>" is almost
# certainly a street address ("1120 TILTON RD"), not a merchant. Reject.
_ADDRESS_LIKE = re.compile(r"^\d+\s+[A-Z]")

# Standalone all-caps single token that's purely a state abbreviation suffix
_BARE_STATE = re.compile(r"^[A-Z]{2}$")


# ---------- prefix-stripped aliases ----------
#
# Aliases that match a PREFIX of the raw description rather than the whole
# string. Used for shapes like "SCHWAB1INT08/29-09/26" where the suffix is a
# date range that varies row-to-row but the prefix uniquely identifies the
# payee.

_PREFIX_ALIASES: tuple[tuple[re.Pattern, str], ...] = (
    (re.compile(r"^SCHWAB1INT"), "Schwab"),
    (re.compile(r"^AMEX\s+EPAYMENT\b"), "American Express"),
    (re.compile(r"^AMZ_STORECRD_PMT\b"), "Amazon"),
    (re.compile(r"^TD\s+ZELLE\s+(?:SENT|RECEIVED)\b"), "Zelle"),
    (re.compile(r"^TfrTDBank", re.IGNORECASE), "TD Bank"),
    (re.compile(r"^WHOLEFOODS#?\b"), "Whole Foods"),
    (re.compile(r"^AMAZON\s+PRIME\b"), "Amazon Prime"),
    (re.compile(r"^AMAZON\s+(?:RETAIL|MARKETPLACE|DIGITAL)\b"), "Amazon"),
)


# ---------- TD Bank purchase pattern ----------
#
# Captures the merchant slot in card-purchase rows. Anchored to purchase-only
# prefixes — withdrawal / cash-deposit rows fall through to None even though
# they have similar shape (those rows have a street address, not a merchant).

_TDBANK_PURCHASE_PREFIX = re.compile(
    r"^(?:VISA\s+DDA\s+(?:PUR|REF)|DDA\s+PURCHASE)\s+(?:AP\s+)?\d+\s+(?P<rest>.+)$"
)


def _tdbank_purchase(raw: str, norm: str) -> Optional[str]:
    m = _TDBANK_PURCHASE_PREFIX.match(raw)
    if not m:
        return None
    rest = m.group("rest")
    # The rest is "<MERCHANT>  <CITY>  * <ST>" with 2+ spaces between fields.
    parts = re.split(r"\s{2,}", rest)
    if len(parts) < 2:
        return None
    merchant_raw = parts[0].strip()
    if not merchant_raw or _ADDRESS_LIKE.match(merchant_raw):
        return None
    # Drop trailing store-number-like digits (e.g. "WALGREENS 6321" -> "WALGREENS").
    merchant_raw = re.sub(r"\s+\d{3,}$", "", merchant_raw)
    if len(merchant_raw) < 2:
        return None
    return _titlecase(merchant_raw)


# ---------- brokerage / investment ticker rows ----------
#
# Captures leading uppercase tokens before a corporate suffix. Truncated /
# corrupted parser output (e.g. "ILLOW GROUPINCCLASS LASSC" — note INCCLASS
# has no word boundary before INC) won't match and falls through to None.

_BROKERAGE_PAT = re.compile(
    r"^(?P<merchant>[A-Z][A-Z\s&.']{2,}?)"
    r"\s+(?:INC|CORP|CO|LLC|LTD|ETF|TRUST|HOLDINGS)\b"
)


def _brokerage(raw: str, norm: str) -> Optional[str]:
    if not norm or not norm[0].isalpha():
        return None
    m = _BROKERAGE_PAT.match(norm)
    if not m:
        return None
    merchant = m.group("merchant").strip()
    if len(merchant) < 3:
        return None
    return _titlecase(merchant)


# ---------- AmEx / general POS pattern ----------
#
# AmEx statement rows have shape "<MERCHANT> <CITY> <ST>" with the state as a
# trailing 2-letter token. The merchant capture is everything before the city
# tokens. Single-space separators (unlike TD Bank's column-padded layout).
#
# The capture is conservative: it rejects rows that start with digits or whose
# captured merchant is empty / all-uppercase-state-like. Long-tail one-off
# merchants (Cava Grill, Pianos NY) match cleanly; ambiguous shapes fall
# through to LLM.

_AMEX_POS_PAT = re.compile(
    r"^(?P<merchant>[A-Z][A-Z0-9'&./# -]+?)"
    r"\s+(?P<city>[A-Z][A-Z\s.]+?)"
    r"\s+(?P<state>[A-Z]{2})\s*$"
)


def _amex_pos(raw: str, norm: str) -> Optional[str]:
    if not norm or not norm[0].isalpha():
        return None
    m = _AMEX_POS_PAT.match(norm)
    if not m:
        return None
    merchant = m.group("merchant").strip()
    # Strip trailing # store numbers if attached
    merchant = re.sub(r"#\s*\d+\s*$", "", merchant).strip()
    if not merchant or len(merchant) < 2:
        return None
    if _ADDRESS_LIKE.match(merchant) or _BARE_STATE.match(merchant):
        return None
    return _titlecase(merchant)


# ---------- per-institution dispatch ----------

_InstitutionHandler = Callable[[str, str], Optional[str]]

_INSTITUTION_HANDLERS: dict[str, tuple[_InstitutionHandler, ...]] = {
    "tdbank": (_tdbank_purchase,),
    "amex": (_amex_pos,),
    "amzn-synchrony": (_amex_pos,),  # similar "<MERCHANT> <CITY> <ST>" shape
    "schwab": (_brokerage,),
    "tdameritrade": (_brokerage,),
    "ameriprise": (_brokerage,),
}


# ---------- public API ----------


def extract_merchant(institution: Optional[str], raw_description: Optional[str]) -> Optional[str]:
    """Return a canonical merchant name, or None if the row's shape is
    ambiguous, unrecognized, or contains no real brand.

    Output is always either a substring of ``raw_description`` or a value from
    the hand-curated alias table — never an invented string. When None is
    returned, callers should fall back to LLM merchant inference.
    """
    if not raw_description or not raw_description.strip():
        return None
    raw = raw_description
    norm = _normalize_for_match(raw)

    # 1. Generic descriptors are never merchants
    for descriptor in _GENERIC_DESCRIPTORS:
        if descriptor in norm:
            return None

    # 2. Exact-match alias table (None values explicitly suppress)
    if norm in _MERCHANT_ALIASES:
        return _MERCHANT_ALIASES[norm]

    # 3. Prefix-match aliases
    for pattern, merchant in _PREFIX_ALIASES:
        if pattern.match(norm):
            return merchant

    # 4. Wrapped-merchant patterns (PAYPAL *X, SQ *X, TST *X, STRIPE *X)
    for pattern in _WRAPPED_PATTERNS:
        m = pattern.match(norm)
        if m:
            inner = m.group("merchant").strip()
            if inner and not _ADDRESS_LIKE.match(inner):
                return _titlecase(inner)

    # 5. Per-institution patterns
    inst_key = (institution or "").lower()
    for handler in _INSTITUTION_HANDLERS.get(inst_key, ()):
        result = handler(raw, norm)
        if result is not None:
            return result

    return None


# ---------- helpers ----------


def _normalize_for_match(raw: str) -> str:
    """Collapse internal whitespace and uppercase. Used for alias / generic
    matching. Original raw stays available for substring capture."""
    return re.sub(r"\s+", " ", raw.strip()).upper()


_SHORT_ACRONYMS = {
    "NYC", "USA", "ATM", "POS", "ETF", "LLC", "INC", "FBO", "IRS",
    "IHOP", "CVS", "DSW", "KFC", "BBQ", "AMC", "NBA", "MTA", "SA",
    "NJ", "NY", "CA", "TX", "FL", "PA", "MA", "VA", "GA", "OH", "IL",
    "MI", "WA", "OR", "CO", "AZ", "NV", "NM", "ID", "UT", "MN", "WI",
    "AL", "AK", "AR", "CT", "DE", "DC", "HI", "IA", "IN", "KS", "KY",
    "LA", "MD", "ME", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "OK",
    "RI", "SC", "SD", "TN", "VT", "WV", "WY", "PR",
}


def _titlecase(s: str) -> str:
    """Convert ALL CAPS merchant text to display case, preserving known
    acronyms (NYC, IHOP) and US state codes. Best-effort — don't rely on
    this matching the LLM's casing exactly; the merchant column is for
    grouping, not display.
    """
    words = s.split()
    out: list[str] = []
    for w in words:
        if w.upper() in _SHORT_ACRONYMS:
            out.append(w.upper())
        elif len(w) == 1:
            # Single-letter tokens (K, M in "K M Tire") -> uppercase
            out.append(w.upper())
        else:
            t = w.lower().capitalize()
            # Fix apostrophe casing: Joe'S -> Joe's
            t = re.sub(r"'(\w)", lambda m: "'" + m.group(1).lower(), t)
            out.append(t)
    return " ".join(out)
