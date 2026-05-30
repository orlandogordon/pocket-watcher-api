from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date, datetime
from uuid import UUID

from src.db.core import get_db, AccountDB
from src.crud import crud_account
from src.services import account_snapshot
from src.models.account_history import (
    AccountSnapshotResponse,
    DismissReviewRequest,
    SnapshotUpdateRequest,
    NetWorthHistoryResponse,
    NetWorthDataPoint,
    AccountValueHistoryResponse,
    AccountValueHistoryPoint,
)
from src.auth.dependencies import get_current_user_id

router = APIRouter(
    prefix="/account-history",
    tags=["account-history"],
)


@router.post("/snapshots/all", status_code=201)
def create_all_snapshots(
    snapshot_date: Optional[date] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create snapshots for all user accounts for a specific date.
    If no date is provided, uses today's date.
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    snapshots = account_snapshot.create_all_account_snapshots(
        db=db,
        user_id=user_id,
        snapshot_date=snapshot_date,
        snapshot_source="MANUAL"
    )

    return {
        "message": f"Created {len(snapshots)} snapshots for {snapshot_date}",
        "count": len(snapshots),
        "date": snapshot_date
    }


@router.post("/snapshots/account/{account_uuid}", response_model=AccountSnapshotResponse, status_code=201)
def create_account_snapshot(
    account_uuid: UUID,
    snapshot_date: Optional[date] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a snapshot of an account's value for a specific date.
    If no date is provided, uses today's date.
    """
    account = crud_account.read_db_account_by_uuid(db, account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    if snapshot_date is None:
        snapshot_date = date.today()

    try:
        snapshot = account_snapshot.create_account_snapshot(
            db=db,
            account_id=account.db_id,
            snapshot_date=snapshot_date,
            snapshot_source="MANUAL"
        )
        return snapshot
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/net-worth", response_model=NetWorthHistoryResponse)
def get_net_worth_history(
    start_date: Optional[date] = Query(None, description="Start date for history (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for history (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get historical net worth data across all accounts.
    Returns daily net worth aggregated from all account snapshots.
    """
    data_points = account_snapshot.get_net_worth_history(
        db=db,
        user_id=user_id,
        start_date=start_date,
        end_date=end_date
    )

    return NetWorthHistoryResponse(
        data=[NetWorthDataPoint(**dp) for dp in data_points],
        start_date=start_date,
        end_date=end_date,
        total_points=len(data_points)
    )


@router.get("/accounts/{account_uuid}", response_model=AccountValueHistoryResponse)
def get_account_value_history(
    account_uuid: UUID,
    start_date: Optional[date] = Query(None, description="Start date for history (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for history (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get historical value data for a specific account.
    """
    account = crud_account.read_db_account_by_uuid(db, account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    try:
        points = account_snapshot.get_account_value_history(
            db=db,
            account_id=account.db_id,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date
        )

        return AccountValueHistoryResponse(
            account_uuid=account.uuid,
            account_name=account.account_name,
            account_type=account.account_type.value,
            data=[AccountValueHistoryPoint(**p) for p in points]
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/accounts/{account_uuid}/snapshots/{snapshot_uuid}", response_model=AccountSnapshotResponse)
def update_snapshot(
    account_uuid: UUID,
    snapshot_uuid: UUID,
    request: SnapshotUpdateRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Edit a single snapshot's values. Supports partial updates.
    Optionally dismiss the review flag in the same call.
    """
    account = crud_account.read_db_account_by_uuid(db, account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    updates = request.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No update fields provided")

    try:
        snapshot = account_snapshot.update_snapshot(
            db=db,
            account_id=account.db_id,
            snapshot_uuid=snapshot_uuid,
            updates=updates,
        )
        return snapshot
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/accounts/{account_uuid}/snapshots/dismiss-review")
def dismiss_snapshot_reviews(
    account_uuid: UUID,
    request: DismissReviewRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Dismiss needs_review flags on specified snapshots for an account.
    """
    account = crud_account.read_db_account_by_uuid(db, account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    dismissed_count = account_snapshot.dismiss_snapshot_reviews(
        db=db,
        account_id=account.db_id,
        snapshot_uuids=request.snapshot_uuids,
        dismiss_reason=request.reason
    )

    return {"dismissed_count": dismissed_count}
