"""Tier A transfer classification.

Pure functions — no DB writes. Used at preview-time (uploads.py) and by the
bulk-upload path to flag checking outflows that look like payments to
another user-owned account (CC, INVESTMENT, LOAN, OTHER) and propose a
TRANSFER_OUT reclassification with a suggested partner account.

Matching signals (in priority order):
1. Per-account `match_aliases` — user-supplied alternative match strings
   (e.g. 'AMZ_STORECRD' for an Amazon Store Card account, since TD
   statements use that abbreviation rather than the full name).
2. Account name / institution as a single normalized string
   (e.g. 'AMERICANEXPRESS', 'SCHWABBROKERAGE').
3. Per-word tokens from name + institution (e.g. 'AMEX', 'SCHWAB'),
   with English-common stopwords stripped to avoid false positives on
   generic merchant words like 'STORE' or 'EXPRESS'.
4. `account_number_last4`, but only when surrounded by non-digit
   characters so it can't accidentally match a substring of a longer
   ID/reference number.

Card-mask digits (e.g. `*****30089881312`) are stripped from the
description before matching, since they carry no transfer signal and
would otherwise produce digit-substring false positives.

Originally Venmo / Cash App were on a hard denylist because no such
accounts existed in the system — once added (phase 2 step 1+2 of #49),
they were removed from the denylist so VENMO* / CASHAPP* descriptions
can pair to the new accounts.
"""
import re
from dataclasses import dataclass
from typing import Iterable, Optional

from src.db.core import AccountDB, AccountType, TransactionType


# Card-mask: any run of '*' followed by digits (e.g. '*****30089881312',
# '****03991459200'). Carries no transfer signal — purely a card identifier
# emitted by some statement formats. Strip before normalization to avoid
# accidental digit-substring matches against account last4s.
_CARD_MASK_RE = re.compile(r"\*+\d+")


# Words that, on their own, are too generic to be a useful match signal.
# Dropped from the per-word token set; the full-string normalized name
# (e.g. 'AMERICANEXPRESS', 'AMAZONSTORECARD') still includes them.
#
# Most additions here are observed false-positive sources from live data:
# 'STORE' matched DERMSTORECOM/WALGREENSSTORE/YCCSTORE; 'EXPRESS' matched
# EXPRESSCOM/HOLIDAYINNEXPRESS; 'AMAZON' matched AMAZONCOM/AMAZONPHOTOD;
# 'AMERICAN' / 'CHARLES' / 'BANK' are defensive — too common.
_STOPWORDS: frozenset[str] = frozenset({
    # Account-shape words.
    "BANK", "CARD", "ACCOUNT", "ACCT", "CHECKING", "SAVINGS",
    "BROKERAGE", "INVESTMENT", "INVESTMENTS", "LOAN", "CREDIT",
    # Color / trim adjectives.
    "GOLD", "PLATINUM", "BLUE", "GREEN", "SILVER",
    # English connectors.
    "THE", "AND", "FOR", "WITH",
    # Position / role words.
    "MAIN", "PRIMARY", "JOINT",
    # Generic merchant-name overlap (sourced from observed false
    # positives — extend as new ones surface).
    "STORE", "EXPRESS", "AMAZON", "AMERICAN", "CHARLES",
})


# P2P rails that don't have a corresponding user-owned account in the
# system. Descriptions containing these terms get a hard skip — they
# can't be transfers to a user account because the user has no such
# account. Venmo and Cash App were originally on this list; they came
# off once Phase 2 #49 added account onboarding for both.
P2P_DENYLIST: frozenset[str] = frozenset({
    "PAYPAL",
})


_MIN_TOKEN_LEN = 3


@dataclass(frozen=True)
class ClassificationResult:
    transaction_type: TransactionType
    suggested_partner_account_id: Optional[int]
    matched_token: Optional[str]


def _strip_card_mask(text: str) -> str:
    return _CARD_MASK_RE.sub("", text)


def _normalize(text: str) -> str:
    """Strip card-mask noise, uppercase, and remove whitespace for
    substring matching."""
    return "".join(_strip_card_mask(text).upper().split())


def build_account_tokens(account: AccountDB) -> set[str]:
    """Return the matchable token set for an account.

    Tokens are uppercased and whitespace-stripped (so a token "AMEX GOLD"
    becomes "AMEXGOLD" to match descriptions like "AMEXEPAYMENT" that have
    no internal spaces). Single words shorter than `_MIN_TOKEN_LEN` or in
    `_STOPWORDS` are dropped; multi-word phrases are kept regardless so
    name like "Amex Gold" still contributes "AMEXGOLD" even though "GOLD"
    alone would be filtered.

    User-supplied `match_aliases` (Phase 2 of #49) bypass the stopword
    filter — they're the user's explicit answer to "what does the bank
    call this account on outgoing payment descriptions?" so they should
    match exactly as given.
    """
    tokens: set[str] = set()

    for raw in (account.account_name or "", account.institution_name or ""):
        full = _normalize(raw)
        if full:
            tokens.add(full)
        for word in raw.upper().split():
            if len(word) >= _MIN_TOKEN_LEN and word not in _STOPWORDS:
                tokens.add(word)

    if account.account_number_last4:
        last4 = account.account_number_last4.strip()
        if last4:
            tokens.add(last4)

    for alias in (getattr(account, "match_aliases", None) or []):
        normalized_alias = _normalize(alias)
        if normalized_alias:
            tokens.add(normalized_alias)

    return tokens


def _description_matches_denylist(normalized_description: str) -> bool:
    for term in P2P_DENYLIST:
        if _normalize(term) in normalized_description:
            return True
    return False


def _token_matches(token: str, normalized_description: str) -> bool:
    """Substring match, except for tokens that are exactly 4 digits — those
    are treated as account last4 and require digit-boundary on both sides
    so a card-mask collision (e.g. '...91459200' matching '9145') can't
    flip the classification."""
    if token.isdigit() and len(token) == 4:
        return bool(re.search(rf"(?<!\d){re.escape(token)}(?!\d)", normalized_description))
    return token in normalized_description


def classify_outflow(
    description: str,
    source_account_id: int,
    user_accounts: Iterable[AccountDB],
) -> ClassificationResult:
    """Classify a single checking-account outflow.

    Returns TRANSFER_OUT + suggested_partner_account_id when the description
    names another user-owned account, unless the description matches the
    P2P denylist. Otherwise returns PURCHASE.

    If multiple candidate accounts match, the candidate whose longest-matching
    token is longest wins (most-specific match, e.g. "SCHWABBROKERAGE" beats
    "SCHWAB").
    """
    norm = _normalize(description or "")

    if not norm or _description_matches_denylist(norm):
        return ClassificationResult(TransactionType.PURCHASE, None, None)

    best_token: Optional[str] = None
    best_account_id: Optional[int] = None

    for account in user_accounts:
        if account.id == source_account_id:
            continue
        if account.account_type not in (
            AccountType.CREDIT_CARD,
            AccountType.INVESTMENT,
            AccountType.LOAN,
            AccountType.OTHER,
        ):
            continue

        longest_match: Optional[str] = None
        for token in build_account_tokens(account):
            if _token_matches(token, norm) and (longest_match is None or len(token) > len(longest_match)):
                longest_match = token

        if longest_match is not None and (
            best_token is None or len(longest_match) > len(best_token)
        ):
            best_token = longest_match
            best_account_id = account.id

    if best_account_id is None:
        return ClassificationResult(TransactionType.PURCHASE, None, None)

    return ClassificationResult(
        TransactionType.TRANSFER_OUT,
        best_account_id,
        best_token,
    )


# Source account types where outflows might be transfers to other user-owned
# accounts. (Outflows from a CC are just normal purchases on the card; no
# point running Tier A there.)
_TIER_A_SOURCE_ACCOUNT_TYPES = frozenset({AccountType.CHECKING, AccountType.SAVINGS})

# Parsed types we'll consider reclassifying.
_TIER_A_CANDIDATE_PARSED_TYPES = frozenset({"PURCHASE", "WITHDRAWAL"})


def classify_parsed_transactions(
    parsed_transactions: list,
    source_account: Optional[AccountDB],
    user_accounts: Iterable[AccountDB],
) -> dict[int, ClassificationResult]:
    """Run Tier A across a batch of ParsedTransaction objects, mutating
    `transaction_type` to "TRANSFER_OUT" where a confident match is found
    and returning a dict of {parsed_position: ClassificationResult} for
    the surfaced matches.

    No-ops when `source_account` is None or not a checking/savings account.
    """
    suggestions: dict[int, ClassificationResult] = {}
    if source_account is None or source_account.account_type not in _TIER_A_SOURCE_ACCOUNT_TYPES:
        return suggestions

    user_accounts_list = list(user_accounts)
    for i, parsed in enumerate(parsed_transactions):
        if (parsed.transaction_type or "").upper() not in _TIER_A_CANDIDATE_PARSED_TYPES:
            continue
        result = classify_outflow(
            description=parsed.description,
            source_account_id=source_account.id,
            user_accounts=user_accounts_list,
        )
        if result.transaction_type == TransactionType.TRANSFER_OUT:
            parsed.transaction_type = TransactionType.TRANSFER_OUT.value
            suggestions[i] = result
    return suggestions
