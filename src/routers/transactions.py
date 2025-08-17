from fastapi import APIRouter, HTTPException, Request
from typing import List
from fastapi.params import Depends
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from src.db.core import NotFoundError, get_db
from src.models.transaction import TransactionCreate, TransactionUpdate, TransactionResponse, TransactionImport
from src.crud.crud_transaction import (
    create_db_transaction,
    read_db_transaction,
    update_db_transaction,
    delete_db_transaction,
    bulk_create_transactions,
)

router = APIRouter(
    prefix="/transactions",
    tags=["transactions"],
)

# A placeholder for user authentication
def get_current_user_id():
    return 1

@router.post("/")
def create_transaction(request: Request, transaction: TransactionCreate, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    try:
        db_transaction = create_db_transaction(db, user_id, transaction)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Database integrity error.") from e
    return TransactionResponse.model_validate(db_transaction)

@router.post("/bulk-upload/")
def create_transactions(request: Request, transaction_import: TransactionImport, db: Session = Depends(get_db)) -> List[TransactionResponse]:
    user_id = get_current_user_id()
    try:
        created_transactions = bulk_create_transactions(db, user_id, transaction_import)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail="Database integrity error.") from e
    return [TransactionResponse.model_validate(t) for t in created_transactions]

@router.get("/{transaction_id}")
def read_transaction(request: Request, transaction_id: str, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    db_transaction = read_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id)
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return TransactionResponse.model_validate(db_transaction)

@router.put("/{transaction_id}")
def update_transaction(request: Request, transaction_id: str, transaction: TransactionUpdate, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    try:
        db_transaction = update_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id, transaction_updates=transaction)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Transaction not found") from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionResponse.model_validate(db_transaction)

@router.delete("/{transaction_id}")
def delete_transaction(request: Request, transaction_id: str, db: Session = Depends(get_db)) -> TransactionResponse:
    user_id = get_current_user_id()
    db_transaction = read_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id)
    if not db_transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    try:
        delete_db_transaction(db, transaction_id=int(transaction_id), user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return TransactionResponse.model_validate(db_transaction)
