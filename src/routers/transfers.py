"""Transfer suggestion inbox API.

Surfaces unpaired TRANSFER_OUT/TRANSFER_IN candidate pairs (regular or
investment) for the user to confirm, dismiss, or leave alone. See
`src/services/transfer_pairing.py` for the pairing logic.
"""
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict
from sqlalchemy.orm import Session

from src.auth.dependencies import get_current_user_id
from src.crud.crud_transaction import update_transaction_type_with_hash
from src.db.core import (
    AccountDB,
    DismissedTransferPairDB,
    InvestmentTransactionDB,
    RelationshipType,
    TransactionDB,
    TransactionRelationshipDB,
    TransactionType,
    get_db,
)
from src.logging_config import get_logger
from src.services.system_tags import remove_system_tag
from src.services.transfer_pairing import (
    PairConfidence,
    TxnSide,
    create_offsets_relationship,
    find_orphans,
    find_pair_suggestions,
)

logger = get_logger(__name__)

router = APIRouter(prefix="/transfers", tags=["transfers"])


class TransferTxnRef(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    is_investment: bool
    transaction_date: str
    amount: str
    description: Optional[str] = None
    account_id: Optional[UUID] = None
    account_name: Optional[str] = None
    transaction_type: str


class PairSuggestionResponse(BaseModel):
    out_side: TransferTxnRef
    in_side: TransferTxnRef
    confidence: PairConfidence
    date_offset_days: int


class ConfirmRequest(BaseModel):
    from_transaction_uuid: UUID
    to_transaction_uuid: UUID
    reclassify_from: bool = False
    reclassify_to: bool = False


class DismissRequest(BaseModel):
    from_transaction_uuid: UUID
    to_transaction_uuid: UUID


def _resolve_uuid(
    db: Session,
    user_id: int,
    txn_uuid: UUID,
) -> tuple[Optional[TransactionDB], Optional[InvestmentTransactionDB]]:
    """Look up a UUID in both regular and investment transactions. Returns
    (regular, investment) where exactly one is non-None on success."""
    reg = db.query(TransactionDB).filter(
        TransactionDB.id == txn_uuid, TransactionDB.user_id == user_id
    ).first()
    if reg is not None:
        return reg, None
    inv = db.query(InvestmentTransactionDB).filter(
        InvestmentTransactionDB.id == txn_uuid,
        InvestmentTransactionDB.user_id == user_id,
    ).first()
    return None, inv


def _serialize_side(
    side: TxnSide,
    accounts_by_id: dict[int, AccountDB],
    txn_uuid: UUID,
    txn_type_value: str,
) -> TransferTxnRef:
    account = accounts_by_id.get(side.account_id)
    return TransferTxnRef(
        id=txn_uuid,
        is_investment=side.is_investment,
        transaction_date=side.transaction_date.isoformat(),
        amount=str(side.amount),
        description=side.description,
        account_id=account.uuid if account else None,
        account_name=account.account_name if account else None,
        transaction_type=txn_type_value,
    )


def _uuid_for_side(db: Session, side: TxnSide) -> Optional[UUID]:
    if side.is_investment:
        row = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.investment_transaction_id == side.txn_id
        ).first()
        return row.id if row else None
    row = db.query(TransactionDB).filter(TransactionDB.db_id == side.txn_id).first()
    return row.id if row else None


def _type_value_for_side(db: Session, side: TxnSide) -> str:
    if side.is_investment:
        row = db.query(InvestmentTransactionDB).filter(
            InvestmentTransactionDB.investment_transaction_id == side.txn_id
        ).first()
        return row.transaction_type.value if row else ""
    row = db.query(TransactionDB).filter(TransactionDB.db_id == side.txn_id).first()
    return row.transaction_type.value if row else ""


@router.get("/suggestions", response_model=list[PairSuggestionResponse])
def get_suggestions(
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """List pending transfer-pair suggestions for the current user."""
    candidates = find_pair_suggestions(db, user_id)
    accounts_by_id = {
        a.id: a for a in db.query(AccountDB).filter(AccountDB.user_id == user_id).all()
    }
    out: list[PairSuggestionResponse] = []
    for c in candidates:
        out_uuid = _uuid_for_side(db, c.out_side)
        in_uuid = _uuid_for_side(db, c.in_side)
        if out_uuid is None or in_uuid is None:
            continue
        out.append(PairSuggestionResponse(
            out_side=_serialize_side(
                c.out_side, accounts_by_id, out_uuid,
                _type_value_for_side(db, c.out_side),
            ),
            in_side=_serialize_side(
                c.in_side, accounts_by_id, in_uuid,
                _type_value_for_side(db, c.in_side),
            ),
            confidence=c.confidence,
            date_offset_days=c.date_offset_days,
        ))
    return out


@router.get("/orphans", response_model=list[TransferTxnRef])
def get_orphans(
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """List TRANSFER_OUT / TRANSFER_IN rows that have neither an OFFSETS
    partner nor a matching unpaired candidate — likely a missing statement
    upload on the partner side."""
    orphans = find_orphans(db, user_id)
    accounts_by_id = {
        a.id: a for a in db.query(AccountDB).filter(AccountDB.user_id == user_id).all()
    }
    out: list[TransferTxnRef] = []
    for s in orphans:
        uuid_ = _uuid_for_side(db, s)
        if uuid_ is None:
            continue
        out.append(_serialize_side(s, accounts_by_id, uuid_, _type_value_for_side(db, s)))
    return out


def _side_from_resolved(
    reg: Optional[TransactionDB], inv: Optional[InvestmentTransactionDB]
) -> TxnSide:
    if reg is not None:
        return TxnSide(
            is_investment=False,
            txn_id=reg.db_id,
            user_id=reg.user_id,
            account_id=reg.account_id,
            transaction_date=reg.transaction_date,
            amount=reg.amount,
            description=reg.description,
        )
    assert inv is not None
    return TxnSide(
        is_investment=True,
        txn_id=inv.investment_transaction_id,
        user_id=inv.user_id,
        account_id=inv.account_id,
        transaction_date=inv.transaction_date,
        amount=inv.total_amount,
        description=inv.description,
    )


@router.post("/suggestions/confirm", status_code=201)
def confirm_suggestion(
    body: ConfirmRequest,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Confirm a transfer pair. Optionally reclassify either side's type
    (PURCHASE/WITHDRAWAL → TRANSFER_OUT, or CREDIT/DEPOSIT → TRANSFER_IN)
    with hash recomputation, then create an OFFSETS relationship in a
    single DB transaction.
    """
    from_reg, from_inv = _resolve_uuid(db, user_id, body.from_transaction_uuid)
    to_reg, to_inv = _resolve_uuid(db, user_id, body.to_transaction_uuid)

    if (from_reg is None and from_inv is None) or (to_reg is None and to_inv is None):
        raise HTTPException(status_code=404, detail="One or both transactions not found")

    if body.reclassify_from and from_reg is not None:
        update_transaction_type_with_hash(db, from_reg, TransactionType.TRANSFER_OUT)
        # A confirmed transfer's intent is captured by the type; the
        # category-driven Needs Review tag no longer applies.
        remove_system_tag(db, user_id, from_reg.db_id, "Needs Review")
    if body.reclassify_to and to_reg is not None:
        update_transaction_type_with_hash(db, to_reg, TransactionType.TRANSFER_IN)
        remove_system_tag(db, user_id, to_reg.db_id, "Needs Review")

    from_side = _side_from_resolved(from_reg, from_inv)
    to_side = _side_from_resolved(to_reg, to_inv)

    existing = db.query(TransactionRelationshipDB).filter(
        TransactionRelationshipDB.relationship_type == RelationshipType.OFFSETS,
        TransactionRelationshipDB.from_transaction_id == (None if from_side.is_investment else from_side.txn_id),
        TransactionRelationshipDB.from_investment_transaction_id == (from_side.txn_id if from_side.is_investment else None),
        TransactionRelationshipDB.to_transaction_id == (None if to_side.is_investment else to_side.txn_id),
        TransactionRelationshipDB.to_investment_transaction_id == (to_side.txn_id if to_side.is_investment else None),
    ).first()
    if existing is not None:
        raise HTTPException(status_code=409, detail="Pair is already linked")

    rel = create_offsets_relationship(db, from_side, to_side)
    db.commit()
    db.refresh(rel)
    return {"relationship_id": str(rel.id)}


@router.post("/suggestions/dismiss", status_code=201)
def dismiss_suggestion(
    body: DismissRequest,
    user_id: int = Depends(get_current_user_id),
    db: Session = Depends(get_db),
):
    """Record a dismissal so this pair stops surfacing in the suggestions
    endpoint."""
    from_reg, from_inv = _resolve_uuid(db, user_id, body.from_transaction_uuid)
    to_reg, to_inv = _resolve_uuid(db, user_id, body.to_transaction_uuid)

    if (from_reg is None and from_inv is None) or (to_reg is None and to_inv is None):
        raise HTTPException(status_code=404, detail="One or both transactions not found")

    dismissal = DismissedTransferPairDB(
        user_id=user_id,
        from_transaction_id=from_reg.db_id if from_reg else None,
        from_investment_transaction_id=from_inv.investment_transaction_id if from_inv else None,
        to_transaction_id=to_reg.db_id if to_reg else None,
        to_investment_transaction_id=to_inv.investment_transaction_id if to_inv else None,
        created_at=datetime.utcnow(),
    )
    db.add(dismissal)
    db.commit()
    return {"dismissed": True}
