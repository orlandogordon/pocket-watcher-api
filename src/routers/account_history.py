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
    AccountValueHistoryResponse
)

router = APIRouter(
    prefix="/account-history",
    tags=["account-history"],
)

# Placeholder for user authentication
def get_current_user_id():
    return 1


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
    account_uuid: str,
    snapshot_date: Optional[date] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a snapshot of an account's value for a specific date.
    If no date is provided, uses today's date.
    """
    try:
        parsed_uuid = UUID(account_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")

    account = crud_account.read_db_account_by_uuid(db, parsed_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    if snapshot_date is None:
        snapshot_date = date.today()

    try:
        snapshot = account_snapshot.create_account_snapshot(
            db=db,
            account_id=account.id,
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
        total_days=len(data_points)
    )


@router.get("/accounts/{account_uuid}", response_model=AccountValueHistoryResponse)
def get_account_value_history(
    account_uuid: str,
    start_date: Optional[date] = Query(None, description="Start date for history (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for history (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get historical value data for a specific account.
    """
    try:
        parsed_uuid = UUID(account_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")

    account = crud_account.read_db_account_by_uuid(db, parsed_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    try:
        snapshots = account_snapshot.get_account_value_history(
            db=db,
            account_id=account.id,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date
        )

        return AccountValueHistoryResponse(
            account_uuid=account.uuid,
            account_name=account.account_name,
            account_type=account.account_type.value,
            data=snapshots
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/accounts/{account_uuid}/snapshots/{snapshot_uuid}", response_model=AccountSnapshotResponse)
def update_snapshot(
    account_uuid: str,
    snapshot_uuid: str,
    request: SnapshotUpdateRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Edit a single snapshot's values. Supports partial updates.
    Optionally dismiss the review flag in the same call.
    """
    try:
        parsed_account_uuid = UUID(account_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid account UUID format")

    try:
        parsed_snapshot_uuid = UUID(snapshot_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid snapshot UUID format")

    account = crud_account.read_db_account_by_uuid(db, parsed_account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    updates = request.model_dump(exclude_unset=True)
    if not updates:
        raise HTTPException(status_code=400, detail="No update fields provided")

    try:
        snapshot = account_snapshot.update_snapshot(
            db=db,
            account_id=account.id,
            snapshot_uuid=parsed_snapshot_uuid,
            updates=updates,
        )
        return snapshot
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.post("/accounts/{account_uuid}/snapshots/dismiss-review")
def dismiss_snapshot_reviews(
    account_uuid: str,
    request: DismissReviewRequest,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Dismiss needs_review flags on specified snapshots for an account.
    """
    try:
        parsed_uuid = UUID(account_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")

    account = crud_account.read_db_account_by_uuid(db, parsed_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    dismissed_count = account_snapshot.dismiss_snapshot_reviews(
        db=db,
        account_id=account.id,
        snapshot_uuids=request.snapshot_uuids,
        dismiss_reason=request.reason
    )

    return {"dismissed_count": dismissed_count}
