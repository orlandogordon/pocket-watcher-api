"""Projection helpers for the data-health (attention inbox) endpoints.

Each helper reads one existing signal source and returns a list of
:class:`AttentionItem`. Pure functions over a SQLAlchemy session — no
HTTP, no commits.

See `Backend Todos/completed/43-data-health-unification.md`.
"""
from typing import Optional
from uuid import UUID

from sqlalchemy.orm import Session

from src.db.core import (
    AccountDB,
    AccountValueHistoryDB,
    InvestmentTransactionDB,
    TagDB,
    TransactionDB,
    TransactionTagDB,
)
from src.models.data_health import AttentionAction, AttentionItem, AttentionSubject
from src.services.system_tags import get_system_tag
from src.services.transfer_pairing import TxnSide, find_orphans, find_pair_suggestions


def project_needs_review(db: Session, user_id: int) -> list[AttentionItem]:
    """One AttentionItem per transaction tagged with the system
    'Needs Review' tag for this user."""
    tag = get_system_tag(user_id, db, "Needs Review")
    if tag is None:
        return []

    # outerjoin on AccountDB: TransactionDB.account_id is nullable.
    rows = (
        db.query(TransactionDB, TransactionTagDB, AccountDB)
        .join(TransactionTagDB, TransactionTagDB.transaction_id == TransactionDB.db_id)
        .outerjoin(AccountDB, AccountDB.id == TransactionDB.account_id)
        .filter(
            TransactionDB.user_id == user_id,
            TransactionTagDB.tag_id == tag.tag_id,
        )
        .all()
    )

    items: list[AttentionItem] = []
    for txn, link, account in rows:
        items.append(AttentionItem(
            id=f"needs_review:{txn.id}",
            kind="needs_review",
            severity="action_required",
            subject=AttentionSubject(type="transaction", primary_uuid=txn.id),
            summary=f"Categorize {txn.amount} on {txn.transaction_date.isoformat()}",
            details={
                "transaction_uuid": str(txn.id),
                "transaction_date": txn.transaction_date.isoformat(),
                "amount": str(txn.amount),
                "description": txn.description,
                "merchant_name": txn.merchant_name,
                "transaction_type": txn.transaction_type.value,
                "account_uuid": str(account.uuid) if account else None,
                "account_name": account.account_name if account else None,
            },
            confidence=None,
            created_at=link.created_at,
            actions=[
                AttentionAction(
                    label="Mark reviewed",
                    method="DELETE",
                    href=f"/tags/transactions/{txn.id}/tags/{tag.id}",
                ),
            ],
        ))
    return items


def _uuid_for_side(db: Session, side: TxnSide) -> Optional[UUID]:
    if side.is_investment:
        row = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.investment_transaction_id == side.txn_id
        ).first()
        return row.id if row else None
    row = db.query(TransactionDB).filter(TransactionDB.db_id == side.txn_id).first()
    return row.id if row else None


def _side_created_at(db: Session, side: TxnSide):
    if side.is_investment:
        row = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.investment_transaction_id == side.txn_id
        ).first()
    else:
        row = db.query(TransactionDB).filter(TransactionDB.db_id == side.txn_id).first()
    return row.created_at if row else None


def _account_for_side(db: Session, side: TxnSide) -> Optional[AccountDB]:
    """Look up the AccountDB row backing a TxnSide. Returns None if the
    side has no account (shouldn't happen in practice — both regular and
    investment transactions require an account — but we degrade gracefully)."""
    if side.account_id is None:
        return None
    return db.query(AccountDB).filter(AccountDB.id == side.account_id).first()


def project_transfer_pairs(db: Session, user_id: int) -> list[AttentionItem]:
    """Wrap find_pair_suggestions into AttentionItem shape. Confidence
    HIGH/MEDIUM maps straight from PairCandidate."""
    candidates = find_pair_suggestions(db, user_id)
    items: list[AttentionItem] = []
    for c in candidates:
        out_uuid = _uuid_for_side(db, c.out_side)
        in_uuid = _uuid_for_side(db, c.in_side)
        if out_uuid is None or in_uuid is None:
            continue
        created_at = _side_created_at(db, c.out_side) or _side_created_at(db, c.in_side)
        if created_at is None:
            continue
        out_account = _account_for_side(db, c.out_side)
        in_account = _account_for_side(db, c.in_side)
        items.append(AttentionItem(
            id=f"transfer_pair:{out_uuid}:{in_uuid}",
            kind="transfer_pair",
            severity="suggested",
            subject=AttentionSubject(
                type="transfer_pair",
                primary_uuid=out_uuid,
                partner_uuid=in_uuid,
            ),
            summary=(
                f"Pair {c.out_side.amount} TRANSFER_OUT "
                f"({c.out_side.transaction_date.isoformat()}) with TRANSFER_IN "
                f"({c.in_side.transaction_date.isoformat()})"
            ),
            details={
                "out_uuid": str(out_uuid),
                "in_uuid": str(in_uuid),
                "amount": str(c.out_side.amount),
                "out_date": c.out_side.transaction_date.isoformat(),
                "in_date": c.in_side.transaction_date.isoformat(),
                "date_offset_days": c.date_offset_days,
                "out_is_investment": c.out_side.is_investment,
                "in_is_investment": c.in_side.is_investment,
                "out_description": c.out_side.description,
                "in_description": c.in_side.description,
                "out_account_uuid": str(out_account.uuid) if out_account else None,
                "out_account_name": out_account.account_name if out_account else None,
                "in_account_uuid": str(in_account.uuid) if in_account else None,
                "in_account_name": in_account.account_name if in_account else None,
            },
            confidence=c.confidence.value,  # "HIGH" | "MEDIUM"
            created_at=created_at,
            actions=[
                AttentionAction(
                    label="Confirm pair",
                    method="POST",
                    href="/transfers/suggestions/confirm",
                    body={
                        "from_transaction_uuid": str(out_uuid),
                        "to_transaction_uuid": str(in_uuid),
                        # Bake the reclassify decision into the body so the
                        # inbox can fire the action verbatim. If a side is
                        # already the expected transfer type, this is a
                        # no-op flag — confirm becomes a pure OFFSETS link.
                        "reclassify_from": c.out_side.transaction_type != "TRANSFER_OUT",
                        "reclassify_to": c.in_side.transaction_type != "TRANSFER_IN",
                    },
                ),
                AttentionAction(
                    label="Dismiss",
                    method="POST",
                    href="/transfers/suggestions/dismiss",
                    body={
                        "from_transaction_uuid": str(out_uuid),
                        "to_transaction_uuid": str(in_uuid),
                    },
                ),
            ],
        ))
    return items


def project_transfer_orphans(db: Session, user_id: int) -> list[AttentionItem]:
    """Wrap find_orphans into AttentionItem shape. No dismissal endpoint
    exists yet (deferred follow-up), so actions is empty."""
    orphans = find_orphans(db, user_id)
    items: list[AttentionItem] = []
    for side in orphans:
        uuid_ = _uuid_for_side(db, side)
        if uuid_ is None:
            continue
        created_at = _side_created_at(db, side)
        if created_at is None:
            continue
        subject_type = "investment_transaction" if side.is_investment else "transaction"
        account = _account_for_side(db, side)
        items.append(AttentionItem(
            id=f"transfer_orphan:{uuid_}",
            kind="transfer_orphan",
            severity="informational",
            subject=AttentionSubject(type=subject_type, primary_uuid=uuid_),
            summary=(
                f"Transfer of {side.amount} on "
                f"{side.transaction_date.isoformat()} has no matching partner"
            ),
            details={
                "transaction_uuid": str(uuid_),
                "transaction_date": side.transaction_date.isoformat(),
                "amount": str(side.amount),
                "description": side.description,
                "is_investment": side.is_investment,
                "transaction_type": side.transaction_type,
                "account_uuid": str(account.uuid) if account else None,
                "account_name": account.account_name if account else None,
            },
            confidence=None,
            created_at=created_at,
            actions=[],
        ))
    return items


def project_snapshot_review(db: Session, user_id: int) -> list[AttentionItem]:
    """One AttentionItem per account_value_history row with
    needs_review=True for accounts owned by this user."""
    rows = (
        db.query(AccountValueHistoryDB, AccountDB)
        .join(AccountDB, AccountDB.id == AccountValueHistoryDB.account_id)
        .filter(
            AccountDB.user_id == user_id,
            AccountValueHistoryDB.needs_review == True,
        )
        .all()
    )

    items: list[AttentionItem] = []
    for snap, account in rows:
        items.append(AttentionItem(
            id=f"snapshot_review:{snap.uuid}",
            kind="snapshot_review",
            severity="informational",
            subject=AttentionSubject(type="snapshot", primary_uuid=snap.uuid),
            summary=(
                f"Snapshot for {account.account_name} on "
                f"{snap.value_date.isoformat()} needs review"
            ),
            details={
                "snapshot_uuid": str(snap.uuid),
                "account_uuid": str(account.uuid),
                "account_name": account.account_name,
                "value_date": snap.value_date.isoformat(),
                "balance": str(snap.balance),
                "review_reason": snap.review_reason,
            },
            confidence=None,
            created_at=snap.created_at,
            actions=[
                AttentionAction(
                    label="Dismiss",
                    method="POST",
                    href=f"/accounts/{account.uuid}/snapshots/dismiss-review",
                    body={"snapshot_uuids": [str(snap.uuid)]},
                ),
            ],
        ))
    return items
