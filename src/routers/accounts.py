from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from typing import List, Optional

from src.crud import crud_account
from src.models import account as account_models
from src.db.core import get_db, NotFoundError

router = APIRouter(
    prefix="/accounts",
    tags=["accounts"],
)

# This is a placeholder for a proper authentication dependency.
# In a real app, this would decode a JWT token to get the current user.
def get_current_user_id() -> int:
    return 1

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

@router.get("/{account_id}", response_model=account_models.AccountResponse)
def read_account(
    account_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Retrieve a specific account by its ID.
    """
    db_account = crud_account.read_db_account(db=db, account_id=account_id, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    return db_account

@router.put("/{account_id}", response_model=account_models.AccountResponse)
def update_account(
    account_id: int,
    account: account_models.AccountUpdate,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Update an account.
    """
    try:
        return crud_account.update_db_account(
            db=db, account_id=account_id, user_id=user_id, account_updates=account
        )
    except NotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))

@router.delete("/{account_id}", response_model=account_models.AccountResponse)
def delete_account(
    account_id: int,
    db: Session = Depends(get_db),
    user_id: int = Depends(get_current_user_id)
):
    """
    Delete an account. It can only be deleted if it has no associated transactions or holdings.
    """
    db_account = crud_account.read_db_account(db, account_id=account_id, user_id=user_id)
    if db_account is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Account not found")
    
    try:
        crud_account.delete_db_account(db=db, account_id=account_id, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
    
    return db_account
