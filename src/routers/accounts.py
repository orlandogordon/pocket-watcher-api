from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from typing import List, Optional, Dict, Any
from datetime import date
from uuid import UUID

from src.crud import crud_account
from src.models import account as account_models
from src.models.account_history import SnapshotBackfillJobResponse, AccountSnapshotResponse
from src.db.core import (
    get_db,
    NotFoundError,
    SnapshotBackfillJobDB,
    AccountValueHistoryDB,
)
from src.services.job_runner import get_job_runner

router = APIRouter(
    prefix="/accounts",
    tags=["accounts"],
)

# This is a placeholder for a proper authentication dependency.
# In a real app, this would decode a JWT token to get the current user.
def get_current_user_id() -> int:
    return 1


def _parse_account_uuid(account_uuid: str) -> UUID:
    """Validate and parse a UUID string, raising 400 on invalid format."""
    try:
        return UUID(account_uuid)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")


@router.post("/", response_model=account_models.AccountResponse, status_code=status.HTTP_201_CREATED)
def create_account(
    account: account_models.AccountCreate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Create a new account for the current user.
    """
    try:
        return crud_account.create_db_account(db=db, user_id=user_id, account_data=account)
    except (ValueError, NotFoundError) as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.get("/", response_model=List[account_models.AccountResponse])
def read_accounts(
    account_type: Optional[account_models.AccountTypeEnum] = None,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve all accounts for the current user, with optional filtering by account type.
    """
    return crud_account.read_db_accounts(
        db=db, user_id=user_id, account_type=account_type, skip=skip, limit=limit
    )

@router.get("/summary", response_model=List[account_models.AccountSummary])
def read_accounts_summary(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a lightweight summary of all accounts for the current user.
    """
    return crud_account.read_db_accounts_summary(db=db, user_id=user_id)

@router.get("/stats", response_model=account_models.AccountStats)
def get_account_statistics(
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get statistics for the current user's accounts (net worth, totals, etc.).
    """
    return crud_account.get_account_stats(db=db, user_id=user_id)

@router.get("/{account_uuid}", response_model=account_models.AccountResponse)
def read_account(
    account_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific account by its UUID.
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    db_account = crud_account.read_db_account_by_uuid(db=db, account_uuid=uuid_obj, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    return db_account

@router.put("/{account_uuid}", response_model=account_models.AccountResponse)
def update_account(
    account_uuid: str,
    account: account_models.AccountUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update an account.
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    try:
        return crud_account.update_db_account_by_uuid(
            db=db, account_uuid=uuid_obj, user_id=user_id, account_updates=account
        )
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{account_uuid}")
def delete_account(
    account_uuid: str,
    force: bool = Query(False),
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete an account.

    Without ?force=true, returns 409 if the account has associated data.
    With ?force=true, cascade-deletes all associated records and returns deletion counts.
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    db_account = crud_account.read_db_account_by_uuid(db=db, account_uuid=uuid_obj, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")

    try:
        deleted = crud_account.delete_db_account_by_uuid(
            db=db, account_uuid=uuid_obj, user_id=user_id, force=force
        )
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))

    if force:
        return {"deleted": deleted}

    return db_account


# ===== SNAPSHOT BACKFILL ENDPOINTS =====

@router.get("/{account_uuid}/snapshot-jobs", response_model=List[SnapshotBackfillJobResponse])
def list_backfill_jobs(
    account_uuid: str,
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    List all backfill jobs for an account.

    Returns jobs ordered by created_at DESC (newest first).
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=uuid_obj, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    jobs = db.query(SnapshotBackfillJobDB).filter(
        SnapshotBackfillJobDB.account_id == account.id
    ).order_by(SnapshotBackfillJobDB.created_at.desc()).offset(skip).limit(limit).all()

    return jobs


@router.get("/{account_uuid}/snapshot-jobs/{job_id}", response_model=SnapshotBackfillJobResponse)
def get_backfill_job(
    account_uuid: str,
    job_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get detailed status of a specific backfill job.
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=uuid_obj, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    job = db.query(SnapshotBackfillJobDB).filter(
        SnapshotBackfillJobDB.id == job_id,
        SnapshotBackfillJobDB.account_id == account.id
    ).first()

    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    return job


@router.post("/{account_uuid}/snapshots/recalculate")
def manually_recalculate_snapshots(
    account_uuid: str,
    start_date: date,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
) -> Dict[str, Any]:
    """
    Manually trigger snapshot recalculation for an account.

    Use Cases:
        - Fix incorrect historical snapshots
        - Backfill after manual transaction edits
        - Re-fetch prices if historical data was incorrect
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=uuid_obj, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    if not end_date:
        end_date = date.today()

    # Check for existing running job
    existing_job = db.query(SnapshotBackfillJobDB).filter(
        SnapshotBackfillJobDB.account_id == account.id,
        SnapshotBackfillJobDB.status.in_(['PENDING', 'IN_PROGRESS'])
    ).first()

    if existing_job:
        raise HTTPException(
            status_code=409,
            detail=f"Backfill job {existing_job.id} already running for this account"
        )

    # Create job
    job = SnapshotBackfillJobDB(
        user_id=user_id,
        account_id=account.id,
        start_date=start_date,
        end_date=end_date,
        status='PENDING'
    )
    db.add(job)
    db.commit()
    db.refresh(job)

    # Submit to runner
    job_runner = get_job_runner()
    job_runner.submit_job(job.id, account.id, start_date, end_date)

    return {
        "message": "Snapshot recalculation started",
        "job_id": job.id,
        "account_uuid": str(account.uuid),
        "start_date": start_date.isoformat(),
        "end_date": end_date.isoformat(),
        "status": "PENDING"
    }


@router.get("/{account_uuid}/snapshots/needs-review", response_model=List[AccountSnapshotResponse])
def get_snapshots_needing_review(
    account_uuid: str,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Get all snapshots that need review (missing price data, etc.)
    """
    uuid_obj = _parse_account_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=uuid_obj, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    snapshots = db.query(AccountValueHistoryDB).filter(
        AccountValueHistoryDB.account_id == account.id,
        AccountValueHistoryDB.needs_review == True
    ).order_by(AccountValueHistoryDB.value_date.desc()).all()

    return snapshots
