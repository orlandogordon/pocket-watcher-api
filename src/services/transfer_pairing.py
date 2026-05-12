"""Tier B transfer pairing.

Find unpaired TRANSFER_OUT / TRANSFER_IN pairs across both regular and
investment transactions, score them, and either auto-create an OFFSETS
relationship (when unique-closest-date AND Tier-A token confirms) or
surface as a suggestion for the inbox.

A pair is "unpaired" when no `OFFSETS` row already references it on either
side, and the user hasn't dismissed it.
"""
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Iterable, Optional
from uuid import uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.orm import Session

from src.db.core import (
    AccountDB,
    DismissedTransferPairDB,
    InvestmentTransactionDB,
    InvestmentTransactionType,
    RelationshipType,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionType,
)
from src.services.transfer_classifier import build_account_tokens, _normalize


# CC TRANSFER_IN typically posts 1–4 days before the checking TRANSFER_OUT
# (card network vs. ACH clearing). Asymmetric window reflects that with a
# 1-day grace on the opposite side.
DATE_WINDOW_DAYS_BEFORE_OUT = 5
DATE_WINDOW_DAYS_AFTER_OUT = 1


class PairConfidence(str, Enum):
    HIGH = "HIGH"      # Tier-A token confirms the partner account identity
    MEDIUM = "MEDIUM"  # amount + date window only


@dataclass(frozen=True)
class TxnSide:
    """Either a regular TransactionDB or an InvestmentTransactionDB row,
    flattened into the fields needed for pairing."""
    is_investment: bool
    txn_id: int
    user_id: int
    account_id: int
    transaction_date: date
    amount: Decimal
    description: Optional[str]


@dataclass(frozen=True)
class PairCandidate:
    out_side: TxnSide
    in_side: TxnSide
    confidence: PairConfidence
    date_offset_days: int  # in_side.date - out_side.date


def _within_window(out_date: date, in_date: date) -> bool:
    earliest = out_date - timedelta(days=DATE_WINDOW_DAYS_BEFORE_OUT)
    latest = out_date + timedelta(days=DATE_WINDOW_DAYS_AFTER_OUT)
    return earliest <= in_date <= latest


def _load_unpaired_regular(
    db: Session, user_id: int, txn_type: TransactionType
) -> list[TxnSide]:
    rel_subq = select(TransactionRelationshipDB.from_transaction_id).where(
        and_(
            TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS,
            TransactionRelationshipDB.from_transaction_id.is_not(None),
        )
    ).union(
        select(TransactionRelationshipDB.to_transaction_id).where(
            and_(
                TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS,
                TransactionRelationshipDB.to_transaction_id.is_not(None),
            )
        )
    )

    rows = db.query(TransactionDB).filter(
        TransactionDB.user_id == user_id,
        TransactionDB.transaction_type == txn_type,
        TransactionDB.account_id.is_not(None),
        ~TransactionDB.db_id.in_(rel_subq),
    ).all()

    return [
        TxnSide(
            is_investment=False,
            txn_id=r.db_id,
            user_id=r.user_id,
            account_id=r.account_id,
            transaction_date=r.transaction_date,
            amount=r.amount,
            description=r.description,
        )
        for r in rows
    ]


def _load_unpaired_investment(
    db: Session, user_id: int, txn_type: InvestmentTransactionType
) -> list[TxnSide]:
    rel_subq = select(TransactionRelationshipDB.from_investment_transaction_id).where(
        and_(
            TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS,
            TransactionRelationshipDB.from_investment_transaction_id.is_not(None),
        )
    ).union(
        select(TransactionRelationshipDB.to_investment_transaction_id).where(
            and_(
                TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS,
                TransactionRelationshipDB.to_investment_transaction_id.is_not(None),
            )
        )
    )

    rows = db.query(InvestmentTransactionDB).filter(
        InvestmentTransactionDB.user_id == user_id,
        InvestmentTransactionDB.transaction_type == txn_type,
        InvestmentTransactionDB.account_id.is_not(None),
        ~InvestmentTransactionDB.investment_transaction_id.in_(rel_subq),
    ).all()

    return [
        TxnSide(
            is_investment=True,
            txn_id=r.investment_transaction_id,
            user_id=r.user_id,
            account_id=r.account_id,
            transaction_date=r.transaction_date,
            amount=r.total_amount,
            description=r.description,
        )
        for r in rows
    ]


def _load_dismissed_pairs(db: Session, user_id: int) -> set[tuple]:
    """Return set of frozen pair-keys for fast membership check."""
    rows = db.query(DismissedTransferPairDB).filter(
        DismissedTransferPairDB.user_id == user_id
    ).all()
    return {_dismissal_key(
        from_is_investment=r.from_investment_transaction_id is not None,
        from_id=r.from_investment_transaction_id or r.from_transaction_id,
        to_is_investment=r.to_investment_transaction_id is not None,
        to_id=r.to_investment_transaction_id or r.to_transaction_id,
    ) for r in rows}


def _dismissal_key(
    from_is_investment: bool, from_id: int,
    to_is_investment: bool, to_id: int,
) -> tuple:
    return (from_is_investment, from_id, to_is_investment, to_id)


def _is_dismissed(out_side: TxnSide, in_side: TxnSide, dismissed: set[tuple]) -> bool:
    key = _dismissal_key(
        out_side.is_investment, out_side.txn_id,
        in_side.is_investment, in_side.txn_id,
    )
    return key in dismissed


def _tier_a_confirms(
    out_side: TxnSide,
    in_side: TxnSide,
    accounts_by_id: dict[int, AccountDB],
) -> bool:
    """A pair is Tier-A-confirmed when a token from the IN-side account's
    identity appears in the OUT-side description."""
    partner_account = accounts_by_id.get(in_side.account_id)
    if partner_account is None or not out_side.description:
        return False
    norm = _normalize(out_side.description)
    if not norm:
        return False
    return any(token in norm for token in build_account_tokens(partner_account))


def find_pair_suggestions(db: Session, user_id: int) -> list[PairCandidate]:
    """Return all candidate pairs for the suggestion inbox.

    Excludes pairs the user has dismissed and pairs already linked by an
    OFFSETS relationship.
    """
    out_sides = (
        _load_unpaired_regular(db, user_id, TransactionType.TRANSFER_OUT)
        + _load_unpaired_investment(db, user_id, InvestmentTransactionType.TRANSFER_OUT)
    )
    in_sides = (
        _load_unpaired_regular(db, user_id, TransactionType.TRANSFER_IN)
        + _load_unpaired_investment(db, user_id, InvestmentTransactionType.TRANSFER_IN)
    )
    dismissed = _load_dismissed_pairs(db, user_id)
    accounts_by_id = {a.id: a for a in db.query(AccountDB).filter(AccountDB.user_id == user_id).all()}

    candidates: list[PairCandidate] = []
    for out_side in out_sides:
        for in_side in in_sides:
            if out_side.account_id == in_side.account_id:
                continue
            if out_side.amount != in_side.amount:
                continue
            if not _within_window(out_side.transaction_date, in_side.transaction_date):
                continue
            if _is_dismissed(out_side, in_side, dismissed):
                continue
            confidence = (
                PairConfidence.HIGH
                if _tier_a_confirms(out_side, in_side, accounts_by_id)
                else PairConfidence.MEDIUM
            )
            candidates.append(PairCandidate(
                out_side=out_side,
                in_side=in_side,
                confidence=confidence,
                date_offset_days=(in_side.transaction_date - out_side.transaction_date).days,
            ))

    return candidates


def find_auto_pair_for_outflow(
    db: Session,
    out_side: TxnSide,
    suggested_partner_account_id: int,
) -> Optional[TxnSide]:
    """Return the unique closest-date TRANSFER_IN on the suggested partner
    account, or None if there is no candidate or the closest-date tie is
    ambiguous.

    Caller is responsible for verifying Tier A flagged this outflow. We
    enforce the partner-account constraint here (TRANSFER_IN must be on
    the exact suggested account).
    """
    candidates: list[TxnSide] = []

    candidates.extend([
        s for s in _load_unpaired_regular(db, out_side.user_id, TransactionType.TRANSFER_IN)
        if s.account_id == suggested_partner_account_id
        and s.amount == out_side.amount
        and _within_window(out_side.transaction_date, s.transaction_date)
    ])
    candidates.extend([
        s for s in _load_unpaired_investment(db, out_side.user_id, InvestmentTransactionType.TRANSFER_IN)
        if s.account_id == suggested_partner_account_id
        and s.amount == out_side.amount
        and _within_window(out_side.transaction_date, s.transaction_date)
    ])

    if not candidates:
        return None

    def offset(s: TxnSide) -> int:
        return abs((s.transaction_date - out_side.transaction_date).days)

    candidates.sort(key=offset)
    if len(candidates) >= 2 and offset(candidates[0]) == offset(candidates[1]):
        return None  # tied closest-date — surface as suggestion instead
    return candidates[0]


def create_offsets_relationship(
    db: Session,
    out_side: TxnSide,
    in_side: TxnSide,
) -> TransactionRelationshipDB:
    """Create a TransactionRelationshipDB row pairing the two sides.

    Caller commits.
    """
    rel = TransactionRelationshipDB(
        id=uuid4(),
        relationship_type=RelationshipType.OFFSETS,
        from_transaction_id=None if out_side.is_investment else out_side.txn_id,
        from_investment_transaction_id=out_side.txn_id if out_side.is_investment else None,
        to_transaction_id=None if in_side.is_investment else in_side.txn_id,
        to_investment_transaction_id=in_side.txn_id if in_side.is_investment else None,
    )
    db.add(rel)
    return rel


def find_orphans(db: Session, user_id: int) -> list[TxnSide]:
    """Return TRANSFER_OUT and TRANSFER_IN rows with no OFFSETS partner
    and no matching candidate in the suggestion query — i.e. transfers
    that look like they're missing the other half of the statement."""
    suggestions = find_pair_suggestions(db, user_id)
    paired_in_suggestions: set[tuple[bool, int]] = set()
    for c in suggestions:
        paired_in_suggestions.add((c.out_side.is_investment, c.out_side.txn_id))
        paired_in_suggestions.add((c.in_side.is_investment, c.in_side.txn_id))

    all_sides = (
        _load_unpaired_regular(db, user_id, TransactionType.TRANSFER_OUT)
        + _load_unpaired_regular(db, user_id, TransactionType.TRANSFER_IN)
        + _load_unpaired_investment(db, user_id, InvestmentTransactionType.TRANSFER_OUT)
        + _load_unpaired_investment(db, user_id, InvestmentTransactionType.TRANSFER_IN)
    )
    return [s for s in all_sides if (s.is_investment, s.txn_id) not in paired_in_suggestions]
