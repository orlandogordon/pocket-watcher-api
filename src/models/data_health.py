"""Response shapes for the data-health (attention inbox) endpoints.

A thin projection over existing source tables — these models are not
persisted. See `src/services/data_health.py` for the projection logic
and `Backend Todos/completed/43-data-health-unification.md` for the
contract.
"""
from datetime import datetime
from typing import Any, Literal, Optional
from uuid import UUID

from src.utils.time import UTCDateTime

from pydantic import BaseModel


AttentionKind = Literal[
    "needs_review",
    "transfer_pair",
    "transfer_orphan",
    "snapshot_review",
]
AttentionSeverity = Literal["action_required", "suggested", "informational"]
AttentionConfidence = Literal["HIGH", "MEDIUM"]
AttentionSubjectType = Literal[
    "transaction",
    "investment_transaction",
    "snapshot",
    "transfer_pair",
]


class AttentionSubject(BaseModel):
    """The entity an attention item is *about*. `partner_uuid` is set only
    for transfer-pair items (links the two sides)."""
    type: AttentionSubjectType
    primary_uuid: UUID
    partner_uuid: Optional[UUID] = None


class AttentionAction(BaseModel):
    """A pre-resolved action the frontend can invoke without knowing the
    item's kind internals. `href` points at an existing endpoint."""
    label: str
    method: Literal["POST", "DELETE"]
    href: str
    body: Optional[dict[str, Any]] = None


class AttentionItem(BaseModel):
    """One row in the unified attention inbox.

    `id` is a deterministic string derived from the source row's UUID
    (e.g. ``"needs_review:<uuid>"``) — stable across requests, suitable
    as a React key. Not a database primary key.
    """
    id: str
    kind: AttentionKind
    severity: AttentionSeverity
    subject: AttentionSubject
    summary: str
    details: dict[str, Any]
    confidence: Optional[AttentionConfidence] = None
    created_at: UTCDateTime
    actions: list[AttentionAction]


class DataHealthCountResponse(BaseModel):
    """Lightweight count for the sidebar badge. `by_kind` keys are the
    same values as :data:`AttentionKind`."""
    total: int
    by_kind: dict[str, int]
