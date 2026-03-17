from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from uuid import UUID

from src.crud import crud_investment, crud_account
from src.db.core import get_db, NotFoundError
from src.models.investment import (
    InvestmentHoldingResponse, InvestmentHoldingUpdate,
    InvestmentTransactionCreate, InvestmentTransactionResponse, InvestmentTransactionUpdate, InvestmentTransactionBulkCreate,
    InvestmentTransactionBulkUpdate
)

router = APIRouter(
    prefix="/investments",
    tags=["investments"],
)

# A placeholder for user authentication
def get_current_user_id():
    return 1

def _parse_uuid(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid UUID format")

# --- Investment Holdings (read-only, derived from transactions) ---

@router.get("/accounts/{account_uuid}/holdings/", response_model=List[InvestmentHoldingResponse])
def read_holdings_for_account(account_uuid: str, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=parsed_uuid, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    try:
        return crud_investment.read_db_investment_holdings_by_account(db=db, account_id=account.id, user_id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Account not found") from e

@router.get("/holdings/{holding_uuid}", response_model=InvestmentHoldingResponse)
def read_holding(holding_uuid: str, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(holding_uuid)
    db_holding = crud_investment.read_db_investment_holding_by_uuid(db=db, holding_uuid=parsed_uuid, user_id=user_id)
    if db_holding is None:
        raise HTTPException(status_code=404, detail="Holding not found")
    return db_holding

@router.put("/holdings/{holding_uuid}", response_model=InvestmentHoldingResponse)
def update_holding(holding_uuid: str, updates: InvestmentHoldingUpdate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(holding_uuid)
    try:
        return crud_investment.update_db_investment_holding_by_uuid(db=db, holding_uuid=parsed_uuid, user_id=user_id, updates=updates)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.post("/accounts/{account_uuid}/holdings/rebuild", response_model=List[InvestmentHoldingResponse])
def rebuild_holdings(account_uuid: str, db: Session = Depends(get_db)):
    """Manual rebuild of holdings from transactions. For admin/debugging."""
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=parsed_uuid, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    try:
        holdings = crud_investment.rebuild_holdings_from_transactions(db, account.id)
        db.commit()
        return holdings
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"Rebuild failed: {str(e)}")

# --- Investment Transactions ---

@router.post("/transactions/", response_model=InvestmentTransactionResponse, status_code=201)
def create_transaction(transaction: InvestmentTransactionCreate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    # Resolve account UUID
    account = crud_account.read_db_account_by_uuid(db, transaction.account_uuid, user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    try:
        return crud_investment.create_db_investment_transaction(db=db, user_id=user_id, transaction_data=transaction, account_id=account.id)
    except (ValueError, NotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/transactions/bulk-upload", response_model=List[InvestmentTransactionResponse], status_code=201)
def create_bulk_transactions(bulk_data: InvestmentTransactionBulkCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        return crud_investment.bulk_create_investment_transactions(db=db, user_id=user_id, bulk_data=bulk_data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.patch("/transactions/bulk-update")
def bulk_update_transactions(bulk_update_data: InvestmentTransactionBulkUpdate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)) -> Dict[str, Any]:
    # Resolve transaction UUIDs to int IDs
    transaction_ids = []
    for t_uuid in bulk_update_data.transaction_uuids:
        txn = crud_investment.read_db_investment_transaction_by_uuid(db, t_uuid, user_id)
        if not txn:
            raise HTTPException(status_code=404, detail=f"Investment transaction {t_uuid} not found")
        transaction_ids.append(txn.investment_transaction_id)

    # Build update payload
    update_payload = {}
    if bulk_update_data.description is not None:
        update_payload["description"] = bulk_update_data.description
    if bulk_update_data.account_uuid is not None:
        account = crud_account.read_db_account_by_uuid(db, bulk_update_data.account_uuid, user_id)
        if not account:
            raise HTTPException(status_code=404, detail="Account not found")
        update_payload["account_id"] = account.id

    if not update_payload:
        raise HTTPException(status_code=400, detail="No update fields provided.")

    try:
        updated_count = crud_investment.bulk_update_db_investment_transactions(
            db=db,
            user_id=user_id,
            transaction_ids=transaction_ids,
            updates=update_payload
        )
        return {"message": f"Successfully updated {updated_count} transactions."}
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/accounts/{account_uuid}/transactions/", response_model=List[InvestmentTransactionResponse])
def read_transactions_for_account(account_uuid: str, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(account_uuid)
    account = crud_account.read_db_account_by_uuid(db=db, account_uuid=parsed_uuid, user_id=user_id)
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")
    return crud_investment.read_db_investment_transactions(db=db, user_id=user_id, account_id=account.id, skip=skip, limit=limit)

@router.get("/transactions/{transaction_uuid}", response_model=InvestmentTransactionResponse)
def read_transaction(transaction_uuid: str, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    db_transaction = crud_investment.read_db_investment_transaction_by_uuid(db=db, transaction_uuid=parsed_uuid, user_id=user_id)
    if db_transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return db_transaction

@router.put("/transactions/{transaction_uuid}", response_model=InvestmentTransactionResponse)
def update_transaction(transaction_uuid: str, transaction: InvestmentTransactionUpdate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    try:
        return crud_investment.update_db_investment_transaction_by_uuid(db=db, transaction_uuid=parsed_uuid, user_id=user_id, transaction_updates=transaction)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Transaction not found") from e

@router.delete("/transactions/{transaction_uuid}", status_code=204)
def delete_transaction(transaction_uuid: str, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    parsed_uuid = _parse_uuid(transaction_uuid)
    db_transaction = crud_investment.read_db_investment_transaction_by_uuid(db, transaction_uuid=parsed_uuid, user_id=user_id)
    if db_transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    try:
        crud_investment.delete_db_investment_transaction_by_uuid(db, transaction_uuid=parsed_uuid, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return None
