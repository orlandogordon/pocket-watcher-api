"""Tier A transfer classification.

Pure functions — no DB writes. Used at preview-time (uploads.py) and by the
bulk-upload path to flag checking outflows that look like payments to
another user-owned account (CC, INVESTMENT, LOAN, OTHER) and propose a
TRANSFER_OUT reclassification with a suggested partner account.

The matching signal is institution/account tokens appearing in the
description string (e.g. "AMEXEPAYMENT" -> Amex Gold account). A P2P
denylist short-circuits descriptions that name a P2P rail (VENMO, ZELLE,
CASHAPP, PAYPAL), since those are real expense even when the user keeps
the rail as an account.
"""
from dataclasses import dataclass
from typing import Iterable, Optional

from src.db.core import AccountDB, AccountType, TransactionType


P2P_DENYLIST: frozenset[str] = frozenset({
    "VENMO",
    "ZELLE",
    "CASHAPP",
    "CASH APP",
    "PAYPAL",
})


# Words that, on their own, are too generic to be a useful match signal.
# These get dropped from the per-word token set; full-string and
# space-stripped tokens still include them.
_STOPWORDS: frozenset[str] = frozenset({
    "BANK", "CARD", "ACCOUNT", "ACCT", "CHECKING", "SAVINGS",
    "BROKERAGE", "INVESTMENT", "INVESTMENTS", "LOAN", "CREDIT",
    "GOLD", "PLATINUM", "BLUE", "GREEN", "SILVER",
    "THE", "AND", "FOR", "WITH",
    "MAIN", "PRIMARY", "JOINT",
})

_MIN_TOKEN_LEN = 3


@dataclass(frozen=True)
class ClassificationResult:
    transaction_type: TransactionType
    suggested_partner_account_id: Optional[int]
    matched_token: Optional[str]


def _normalize(text: str) -> str:
    """Uppercase + strip whitespace for substring matching."""
    return "".join(text.upper().split())


def build_account_tokens(account: AccountDB) -> set[str]:
    """Return the matchable token set for an account.

    Tokens are uppercased and whitespace-stripped (so a token "AMEX GOLD"
    becomes "AMEXGOLD" to match descriptions like "AMEXEPAYMENT" that have
    no internal spaces). Single words shorter than `_MIN_TOKEN_LEN` or in
    `_STOPWORDS` are dropped; multi-word phrases are kept regardless so
    name like "Amex Gold" still contributes "AMEXGOLD" even though "GOLD"
    alone would be filtered.
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

    return tokens


def _description_matches_denylist(normalized_description: str) -> bool:
    for term in P2P_DENYLIST:
        if _normalize(term) in normalized_description:
            return True
    return False


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
            if token in norm and (longest_match is None or len(token) > len(longest_match)):
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
