from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List, Dict, Any

from src.crud import crud_investment
from src.db.core import get_db, NotFoundError
from src.models.investment import (
    InvestmentHoldingCreate, InvestmentHoldingResponse, InvestmentHoldingUpdate,
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

# --- Investment Holdings ---

@router.post("/holdings/", response_model=InvestmentHoldingResponse)
def create_holding(holding: InvestmentHoldingCreate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return crud_investment.create_db_investment_holding(db=db, user_id=user_id, holding_data=holding)
    except (ValueError, NotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/accounts/{account_id}/holdings/", response_model=List[InvestmentHoldingResponse])
def read_holdings_for_account(account_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return crud_investment.read_db_investment_holdings_by_account(db=db, account_id=account_id, user_id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Account not found") from e

@router.get("/holdings/{holding_id}", response_model=InvestmentHoldingResponse)
def read_holding(holding_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_holding = crud_investment.read_db_investment_holding(db=db, holding_id=holding_id, user_id=user_id)
    if db_holding is None:
        raise HTTPException(status_code=404, detail="Holding not found")
    return db_holding

@router.put("/holdings/{holding_id}", response_model=InvestmentHoldingResponse)
def update_holding(holding_id: int, holding: InvestmentHoldingUpdate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return crud_investment.update_db_investment_holding(db=db, holding_id=holding_id, user_id=user_id, holding_updates=holding)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Holding not found") from e

@router.delete("/holdings/{holding_id}", response_model=InvestmentHoldingResponse)
def delete_holding(holding_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_holding = crud_investment.read_db_investment_holding(db, holding_id=holding_id, user_id=user_id)
    if db_holding is None:
        raise HTTPException(status_code=404, detail="Holding not found")
    try:
        crud_investment.delete_db_investment_holding(db, holding_id=holding_id, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return db_holding

# --- Investment Transactions ---

@router.post("/transactions/", response_model=InvestmentTransactionResponse)
def create_transaction(transaction: InvestmentTransactionCreate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return crud_investment.create_db_investment_transaction(db=db, user_id=user_id, transaction_data=transaction)
    except (ValueError, NotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/transactions/bulk-upload", response_model=List[InvestmentTransactionResponse])
def create_bulk_transactions(bulk_data: InvestmentTransactionBulkCreate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)):
    try:
        return crud_investment.bulk_create_investment_transactions(db=db, user_id=user_id, bulk_data=bulk_data)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

@router.patch("/transactions/bulk-update")
def bulk_update_transactions(bulk_update_data: InvestmentTransactionBulkUpdate, db: Session = Depends(get_db), user_id: int = Depends(get_current_user_id)) -> Dict[str, Any]:
    update_payload = bulk_update_data.model_dump(exclude_unset=True, exclude={"transaction_ids"})
    if not update_payload:
        raise HTTPException(status_code=400, detail="No update fields provided.")
    try:
        updated_count = crud_investment.bulk_update_db_investment_transactions(
            db=db, 
            user_id=user_id, 
            transaction_ids=bulk_update_data.transaction_ids, 
            updates=update_payload
        )
        return {"message": f"Successfully updated {updated_count} transactions."}
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/accounts/{account_id}/transactions/", response_model=List[InvestmentTransactionResponse])
def read_transactions_for_account(account_id: int, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    return crud_investment.read_db_investment_transactions(db=db, user_id=user_id, account_id=account_id, skip=skip, limit=limit)

@router.get("/transactions/{transaction_id}", response_model=InvestmentTransactionResponse)
def read_transaction(transaction_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_transaction = crud_investment.read_db_investment_transaction(db=db, transaction_id=transaction_id, user_id=user_id)
    if db_transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return db_transaction

@router.put("/transactions/{transaction_id}", response_model=InvestmentTransactionResponse)
def update_transaction(transaction_id: int, transaction: InvestmentTransactionUpdate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return crud_investment.update_db_investment_transaction(db=db, transaction_id=transaction_id, user_id=user_id, transaction_updates=transaction)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Transaction not found") from e

@router.delete("/transactions/{transaction_id}", response_model=InvestmentTransactionResponse)
def delete_transaction(transaction_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_transaction = crud_investment.read_db_investment_transaction(db, transaction_id=transaction_id, user_id=user_id)
    if db_transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    try:
        crud_investment.delete_db_investment_transaction(db, transaction_id=transaction_id, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return db_transaction
