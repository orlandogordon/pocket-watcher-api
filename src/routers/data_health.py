"""Data-health (attention inbox) API.

Aggregates four existing signal sources into a single normalized stream
of :class:`AttentionItem` for the frontend inbox. Thin projection only —
no signal generation or persistence lives here.

See `Backend Todos/completed/43-data-health-unification.md`.
"""
from fastapi import APIRouter, Depends
from sqlalchemy import and_
from sqlalchemy.orm import Session

from src.auth.dependencies import get_current_user_id
from src.db.core import (
    AccountDB,
    AccountValueHistoryDB,
    TransactionDB,
    TransactionTagDB,
    get_db,
)
from src.logging_config import get_logger
from src.models.data_health import AttentionItem, DataHealthCountResponse
from src.services.data_health import (
    project_needs_review,
    project_snapshot_review,
    project_transfer_orphans,
    project_transfer_pairs,
)
from src.services.system_tags import get_system_tag
from src.services.transfer_pairing import find_orphans, find_pair_suggestions

logger = get_logger(__name__)

router = APIRouter(prefix="/data-health", tags=["data-health"])


@router.get("/items", response_model=list[AttentionItem])
def list_attention_items(
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Unified attention-inbox feed. Items sorted by `created_at` desc."""
    items = [
        *project_needs_review(db, user_id),
        *project_transfer_pairs(db, user_id),
        *project_transfer_orphans(db, user_id),
        *project_snapshot_review(db, user_id),
    ]
    items.sort(key=lambda x: x.created_at, reverse=True)
    return items


@router.get("/count", response_model=DataHealthCountResponse)
def count_attention_items(
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Sidebar-badge count. Avoids building Pydantic models — straight
    SQL counts for `needs_review` and `snapshot_review`; live pairing
    pass for the two transfer kinds (small input sets in practice)."""
    tag = get_system_tag(user_id, db, "Needs Review")
    needs_review_count = 0
    if tag is not None:
        needs_review_count = (
            db.query(TransactionTagDB)
            .join(TransactionDB, TransactionDB.db_id == TransactionTagDB.transaction_id)
            .filter(
                TransactionDB.user_id == user_id,
                TransactionTagDB.tag_id == tag.tag_id,
            )
            .count()
        )

    snapshot_review_count = (
        db.query(AccountValueHistoryDB)
        .join(AccountDB, AccountDB.id == AccountValueHistoryDB.account_id)
        .filter(
            AccountDB.user_id == user_id,
            AccountValueHistoryDB.needs_review == True,
        )
        .count()
    )

    transfer_pair_count = len(find_pair_suggestions(db, user_id))
    transfer_orphan_count = len(find_orphans(db, user_id))

    by_kind = {
        "needs_review": needs_review_count,
        "transfer_pair": transfer_pair_count,
        "transfer_orphan": transfer_orphan_count,
        "snapshot_review": snapshot_review_count,
    }
    return DataHealthCountResponse(total=sum(by_kind.values()), by_kind=by_kind)
