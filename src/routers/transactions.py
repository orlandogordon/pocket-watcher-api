from fastapi import APIRouter, HTTPException, Request
from fastapi.params import Depends
from sqlalchemy.orm import Session
from ..db.core import NotFoundError, get_db
from ..db.transactions import (
    Transaction,
    TransactionCreate,
    TransactionUpdate,
    read_db_transaction,
    create_db_transaction,
    update_db_transaction,
    delete_db_transaction,
)
# from .limiter import limiter


router = APIRouter(
    prefix="/transactions",
)


# @limiter.limit("1/second")
@router.post("/")
def create_transaction(request: Request, transaction: TransactionCreate, db: Session = Depends(get_db)) -> Transaction:
    db_transaction = create_db_transaction(transaction, db)
    return Transaction(**db_transaction.__dict__)


@router.get("/{transaction_id}")
def read_transaction(request: Request, transaction_id: int, db: Session = Depends(get_db)) -> Transaction:
    try:
        db_transaction = read_db_transaction(transaction_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Transaction(**db_transaction.__dict__)


@router.get("/{transaction_id}/automations")
def read_transaction_automations(
    request: Request, transaction_id: int, db: Session = Depends(get_db)
) -> list[Transaction]:
    # try:
    #     transactions = read_db_transactions_for_transaction(transaction_id, db)
    # except NotFoundError as e:
    #     raise HTTPException(status_code=404) from e
    # return [Automation(**automation.__dict__) for automation in automations]
    return []


@router.put("/{transaction_id}")
def update_transaction(request: Request, transaction_id: int, transaction: TransactionUpdate, db: Session = Depends(get_db)) -> Transaction:
    try:
        db_transaction = update_db_transaction(transaction_id, transaction, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Transaction(**db_transaction.__dict__)


@router.delete("/{transaction_id}")
def delete_transaction(request: Request, transaction_id: int, db: Session = Depends(get_db)) -> Transaction:
    try:
        db_transaction = delete_db_transaction(transaction_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Transaction(**db_transaction.__dict__)