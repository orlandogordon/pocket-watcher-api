from fastapi import APIRouter, HTTPException, Request
from fastapi.params import Depends
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from ..db.core import NotFoundError, get_db
from ..db.transactions import (
    Transaction,
    TransactionCreate,
    TransactionUpdate,
    get_db_transaction,
    get_db_transaction_by_identifier,
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
    try:
        db_transaction = create_db_transaction(transaction, db)
    except ValueError as e:
        try:
            existing = get_db_transaction_by_identifier(transaction.transaction_identifier, db)
            proceed = input(f"Duplicate transaction identifier found: {existing.transaction_identifier}. Would you like to proceed with insertion anyway? (y/n)")
            if proceed.lower() == 'y' or proceed.lower() == 'yes' or proceed.lower() == '1':
                print("Adjusting identifier and proceeding with transaction creation despite duplicate identifier detected.")
                transaction.transaction_identifier = f"{transaction.transaction_identifier}_duplicate"
                db_transaction = create_db_transaction(transaction, db)
            else:
                print("Transaction creation aborted due to duplicate identifier.")
                raise HTTPException(status_code=400, detail=str(e)) from e
        except NotFoundError:
            print("MAJOR ERROR: Transaction with identifier not found here. Even though it was previously found in the create_db_transaction function.")
            raise HTTPException(status_code=404, detail=str(e)) from e
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail=str("SQL Alchemy Integrity Error: Please ensure the payload matches the expected input format")) from e
    return Transaction(**db_transaction.__dict__)


@router.get("/{transaction_id}")
def read_transaction(request: Request, transaction_id: str, db: Session = Depends(get_db)) -> Transaction:
    try:
        db_transaction = get_db_transaction(transaction_id, db)
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
def update_transaction(request: Request, transaction_id: str, transaction: TransactionUpdate, db: Session = Depends(get_db)) -> Transaction:
    try:
        db_transaction = update_db_transaction(transaction_id, transaction, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Transaction(**db_transaction.__dict__)


@router.delete("/{transaction_id}")
def delete_transaction(request: Request, transaction_id: str, db: Session = Depends(get_db)) -> Transaction:
    try:
        db_transaction = delete_db_transaction(transaction_id, db)
    except NotFoundError as e:
        raise HTTPException(status_code=404) from e
    return Transaction(**db_transaction.__dict__)