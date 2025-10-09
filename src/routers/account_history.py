from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date, datetime

from src.db.core import get_db, AccountDB
from src.services import account_snapshot
from src.models.account_history import (
    AccountSnapshotResponse,
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


@router.post("/snapshots/account/{account_id}", response_model=AccountSnapshotResponse, status_code=201)
def create_account_snapshot(
    account_id: int,
    snapshot_date: Optional[date] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a snapshot of an account's value for a specific date.
    If no date is provided, uses today's date.
    """
    # Verify account ownership
    account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    if snapshot_date is None:
        snapshot_date = date.today()

    try:
        snapshot = account_snapshot.create_account_snapshot(
            db=db,
            account_id=account_id,
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


@router.get("/accounts/{account_id}", response_model=AccountValueHistoryResponse)
def get_account_value_history(
    account_id: int,
    start_date: Optional[date] = Query(None, description="Start date for history (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="End date for history (YYYY-MM-DD)"),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get historical value data for a specific account.
    """
    # Get account info
    account = db.query(AccountDB).filter(
        AccountDB.id == account_id,
        AccountDB.user_id == user_id
    ).first()

    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    try:
        snapshots = account_snapshot.get_account_value_history(
            db=db,
            account_id=account_id,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date
        )

        return AccountValueHistoryResponse(
            account_id=account.id,
            account_name=account.account_name,
            account_type=account.account_type.value,
            data=snapshots
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
