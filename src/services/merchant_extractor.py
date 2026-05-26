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
    # TD Bank ACH/electronic shapes (pre-#50 spaced forms, kept for any
    # historical descriptions still in spaced shape).
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
    # TD Bank ACH squashed-token shapes (post-#50). The parser's
    # _ACH_ELECTRONIC_RE strips the leading `ACHDEBIT,` / `ELECTRONICPMT-WEB,`
    # prefix, leaving a squashed payee token plus a trailing reference id.
    # The reference id is stripped by _strip_trailing_ref() before lookup,
    # so the keys here are the bare payee token.
    "CRUNCHFITCLUBFEES":        "Crunch Fitness",
    "ROBINHOODDEBITS":          "Robinhood",
    "DEPTEDUCATIONSTUDENTLN":   "Dept of Education",
    "AMEXEPAYMENTACHPMT":       "American Express",
    "AMZSTORECRDPMT":           "Amazon",
    "AMZ_STORECRD_PMTPAYMENT":  "Amazon",
    "AMAZONCORPSYFPAYMNT":      "Amazon",
    "PNCBANKNAREGSALARY":       "PNC Bank",
    "ACTALENT,INC.DIRDEP":      "Actalent",
    # NJCLASS (older shape) and HESAAPAYMENTP (post-2024 shape) refer to
    # the same NJ state student-loan obligation — unify under "HESAA".
    "STATEOFNJNJCLASSLN":       "HESAA",
    "HESAAPAYMENTP":            "HESAA",
    # Willis Towers Watson payroll, two payment-method codes.
    "WILLIS NORTHAMEPAYROLL*BM": "Willis Towers Watson",
    "WILLIS AMERICASPAYROLL*BG": "Willis Towers Watson",
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

# A captured merchant slot that begins with digits followed by uppercase letters
# is almost certainly a street address. Two shapes show up:
#   - Spaced: "1120 TILTON RD" (Amex / pre-strip TD)
#   - Squashed: "849FISCHERBLVD" (post-#50 TD strip preserves no whitespace
#     between the leading number and the street name)
# The `\s*` allows zero-or-more spaces so both forms are rejected.
_ADDRESS_LIKE = re.compile(r"^\d+\s*[A-Z]")

# Standalone all-caps single token that's purely a state abbreviation suffix
_BARE_STATE = re.compile(r"^[A-Z]{2}$")


# Generic descriptor tokens that should never form a merchant name on their
# own. Post-#48 AmEx strips the leading "AplPay " from rows like
# "AplPay STORE TOMS RIVER NJ", leaving captures like merchant="Store",
# "Max", "The Club" — useless for grouping and unrelated businesses get
# collapsed under the same name. A capture composed entirely of these
# tokens is rejected. Multi-word captures that *contain* a generic token
# (e.g. "5TH STREET DELI", "DOWNTOWN CAFE LLC") still pass.
_NOT_A_MERCHANT: frozenset[str] = frozenset({
    "THE", "STORE", "STORES", "SHOP", "SHOPS",
    "MAX", "MIN", "CLUB", "MARKET",
    "CAFE", "BAR", "GRILL", "DELI", "RESTAURANT",
    "CO", "INC", "LLC", "LTD",
})


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
    # Post-#50 the TD parser rewrites Zelle rows to `Zelle: <counterparty>`.
    # The counterparty name is preserved in the description; this alias
    # keeps the merchant column as a stable "Zelle" so all Zelle activity
    # groups together in analytics (the row's transfer classification
    # carries the counterparty-specific info separately).
    (re.compile(r"^ZELLE:\s+"), "Zelle"),
    (re.compile(r"^TfrTDBank", re.IGNORECASE), "TD Bank"),
    (re.compile(r"^WHOLEFOODS#?\b"), "Whole Foods"),
    (re.compile(r"^AMAZON\s+PRIME\b"), "Amazon Prime"),
    (re.compile(r"^AMAZON\s+(?:RETAIL|MARKETPLACE|DIGITAL)\b"), "Amazon"),
    # Recurring Apple billing descriptor. iCloud, App Store, Apple Music,
    # AppleCare, etc. all surface as `APPLE.COM/BILL ...` (often trailed by a
    # support phone + state). The specific product is unknowable from the row,
    # so group them under a single stable "apple.com" merchant. (Category stays
    # null for the standalone shape — see _CATEGORY_RULES in llm_client.)
    (re.compile(r"^APPLE\.COM"), "apple.com"),
    # CVS rows arrive as `CVS/PHARMACY #NNNN NNNNN ...`, `CVS PHARMACY ...`, or
    # bare `CVS #NNNN ...`. Normalize the brand to "CVS Pharmacy" rather than
    # letting the title-caser emit "Cvs/pharmacy" with the slash intact.
    (re.compile(r"^CVS\b"), "CVS Pharmacy"),
)


# ---------- TD Bank purchase pattern ----------
#
# Captures the merchant slot in card-purchase rows after the parser's
# _clean_description has stripped the AUT compound prefix. Post-#50 shape:
#   MICROSOFTXBOX MSBILLINFO *WA
#   WAWA FUEL/CONVENIENCE TOMS RIVER *NJ
#   849FISCHERBLVD TOMSRIVER *NJ            (address — rejected)
# Anchored on the trailing `*<STATE>` suffix that #50's strip leaves intact.
# Merchant is everything before the city; city is the last whitespace-delimited
# uppercase run before the `*ST`. The single-space layout of post-strip rows
# means the lazy/greedy boundary between merchant and city is heuristic, but
# the trailing store-number strip cleans up the common case where the parser
# leaves a numeric store id attached.

_TDBANK_PURCHASE_POST_CLEAN = re.compile(
    r"^(?P<merchant>.+?)\s+(?P<city>[A-Z][A-Z\s.]+?)\s+\*(?P<state>[A-Z]{2})\s*$"
)


def _tdbank_purchase(raw: str, norm: str) -> Optional[str]:
    m = _TDBANK_PURCHASE_POST_CLEAN.match(raw)
    if not m:
        return None
    merchant_raw = m.group("merchant").strip()
    if not merchant_raw or _ADDRESS_LIKE.match(merchant_raw):
        return None
    # Reject bare-number captures. The lazy merchant pattern stops at the
    # first whitespace, so `1120 TILTON RD NORTHFIELD *NJ` captures
    # merchant=`1120` (which then bypasses _ADDRESS_LIKE since that rejector
    # requires letters after the digits).
    if not any(ch.isalpha() for ch in merchant_raw):
        return None
    merchant_raw = _strip_store_suffix(merchant_raw)
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
    merchant = _strip_store_suffix(m.group("merchant").strip())
    if not merchant or len(merchant) < 2:
        return None
    if _ADDRESS_LIKE.match(merchant) or _BARE_STATE.match(merchant):
        return None
    # Reject captures composed entirely of generic descriptor tokens.
    tokens = merchant.split()
    if tokens and all(tok in _NOT_A_MERCHANT for tok in tokens):
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

    # 2. Exact-match alias table (None values explicitly suppress). Try the
    # normalized form first, then a version with the trailing reference-id
    # suffix stripped — covers post-#50 TD ACH rows like
    # `CRUNCHFITCLUBFEES****300238869` whose stable key is the bare token.
    if norm in _MERCHANT_ALIASES:
        return _MERCHANT_ALIASES[norm]
    norm_stripped = _strip_trailing_ref(norm)
    if norm_stripped != norm and norm_stripped in _MERCHANT_ALIASES:
        return _MERCHANT_ALIASES[norm_stripped]

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


# Trailing reference-ID suffix shapes found in TD post-#50 ACH rows:
#   STATEOFNJNJCLASSLN****41203    (masked stars + digits)
#   DEPTEDUCATIONSTUDENTLN0000     (bare trailing digits, no stars)
#   HESAAPAYMENTP19515308          (alpha-then-digits)
# The optional `*+` covers the masked-account variant; the digits at end
# are the recurring-billing reference number that changes month-to-month
# and would otherwise prevent an exact-match alias hit.
_TRAILING_REF = re.compile(r"(?:\*+)?\d+$")


def _strip_trailing_ref(norm: str) -> str:
    """Strip a trailing reference-id suffix from a normalized description so
    rows like `CRUNCHFITCLUBFEES****300238869` match the bare alias key
    `CRUNCHFITCLUBFEES`. Idempotent when no suffix matches."""
    stripped = _TRAILING_REF.sub("", norm).rstrip()
    return stripped or norm


def _is_store_token(tok: str) -> bool:
    """A trailing token that's a store id / reference number, not part of the
    brand name. Two shapes:
      - a pure number or `#`-prefixed store number (`8368`, `#05675`, `000005675`)
      - a numeric-dominant reference code (`P40905479D`) — 4+ digits overall.
        The 4-digit floor avoids eating brand tokens that legitimately carry a
        digit or two (`3M`, `7UP`, `H&M2`), which are also rarely trailing.
    """
    if re.fullmatch(r"#?\d+", tok):
        return True
    return sum(ch.isdigit() for ch in tok) >= 4


def _strip_store_suffix(merchant: str) -> str:
    """Drop trailing store-id / reference-number tokens from a captured POS
    merchant slot. POS rows routinely append one or two of these between the
    brand and the city: `WAWA 8368 8368`, `CVS/PHARMACY #05675 000005675`,
    `SPOTIFY P40905479D 685603`. Pop junk tokens from the right, stopping at
    the first token that looks like part of the name; never strip the capture
    down to nothing (keep at least one token). Trailing separator punctuation
    left behind (`JOEYS PIZZA-`) is then trimmed."""
    tokens = merchant.split()
    while len(tokens) > 1 and _is_store_token(tokens[-1]):
        tokens.pop()
    return " ".join(tokens).rstrip(" -#/.,")


_SHORT_ACRONYMS = {
    "NYC", "USA", "ATM", "POS", "ETF", "LLC", "INC", "FBO", "IRS",
    "IHOP", "CVS", "DSW", "KFC", "BBQ", "AMC", "NBA", "MTA", "SA",
    "NJ", "NY", "CA", "TX", "FL", "PA", "MA", "VA", "GA", "OH", "IL",
    "MI", "WA", "OR", "CO", "AZ", "NV", "NM", "ID", "UT", "MN", "WI",
    "AL", "AK", "AR", "CT", "DE", "DC", "HI", "IA", "IN", "KS", "KY",
    "LA", "MD", "ME", "MO", "MS", "MT", "NC", "ND", "NE", "NH", "OK",
    "RI", "SC", "SD", "TN", "VT", "WV", "WY", "PR",
}


def _case_token(tok: str) -> str:
    """Case a single slash-free, whitespace-free token: preserve known
    acronyms / state codes and single letters; otherwise capitalize."""
    if tok.upper() in _SHORT_ACRONYMS:
        return tok.upper()
    if len(tok) == 1:
        # Single-letter tokens (K, M in "K M Tire") -> uppercase
        return tok.upper()
    t = tok.lower().capitalize()
    # Fix apostrophe casing: Joe'S -> Joe's
    return re.sub(r"'(\w)", lambda m: "'" + m.group(1).lower(), t)


def _titlecase(s: str) -> str:
    """Convert ALL CAPS merchant text to display case, preserving known
    acronyms (NYC, IHOP) and US state codes. Tokens glued by '/' are cased on
    each side and rejoined (CVS/PHARMACY -> CVS/Pharmacy, FUEL/CONVENIENCE ->
    Fuel/Convenience) so a slash doesn't defeat the acronym lookup or leave a
    lowercased tail. Best-effort — don't rely on this matching the LLM's casing
    exactly; the merchant column is for grouping, not display.
    """
    out: list[str] = []
    for word in s.split():
        if "/" in word:
            out.append("/".join(_case_token(p) for p in word.split("/")))
        else:
            out.append(_case_token(word))
    return " ".join(out)
