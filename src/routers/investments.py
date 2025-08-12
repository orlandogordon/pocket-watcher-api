from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from typing import List

from src.crud.crud_investment import (
    create_db_investment_holding, read_db_investment_holdings_by_account, read_db_investment_holding, update_db_investment_holding, delete_db_investment_holding,
    create_db_investment_transaction, read_db_investment_transactions, read_db_investment_transaction, update_db_investment_transaction, delete_db_investment_transaction
)
from src.db.core import get_db, NotFoundError
from src.models.investment import (
    InvestmentHoldingCreate, InvestmentHoldingResponse, InvestmentHoldingUpdate,
    InvestmentTransactionCreate, InvestmentTransactionResponse, InvestmentTransactionUpdate
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
        return create_db_investment_holding(db=db, user_id=user_id, holding_data=holding)
    except (ValueError, NotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/accounts/{account_id}/holdings/", response_model=List[InvestmentHoldingResponse])
def read_holdings_for_account(account_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return read_db_investment_holdings_by_account(db=db, account_id=account_id, user_id=user_id)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Account not found") from e

@router.get("/holdings/{holding_id}", response_model=InvestmentHoldingResponse)
def read_holding(holding_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_holding = read_db_investment_holding(db=db, holding_id=holding_id, user_id=user_id)
    if db_holding is None:
        raise HTTPException(status_code=404, detail="Holding not found")
    return db_holding

@router.put("/holdings/{holding_id}", response_model=InvestmentHoldingResponse)
def update_holding(holding_id: int, holding: InvestmentHoldingUpdate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return update_db_investment_holding(db=db, holding_id=holding_id, user_id=user_id, holding_updates=holding)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Holding not found") from e

@router.delete("/holdings/{holding_id}", response_model=InvestmentHoldingResponse)
def delete_holding(holding_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_holding = read_db_investment_holding(db, holding_id=holding_id, user_id=user_id)
    if db_holding is None:
        raise HTTPException(status_code=404, detail="Holding not found")
    try:
        delete_db_investment_holding(db, holding_id=holding_id, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return db_holding

# --- Investment Transactions ---

@router.post("/transactions/", response_model=InvestmentTransactionResponse)
def create_transaction(transaction: InvestmentTransactionCreate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return create_db_investment_transaction(db=db, user_id=user_id, transaction_data=transaction)
    except (ValueError, NotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/accounts/{account_id}/transactions/", response_model=List[InvestmentTransactionResponse])
def read_transactions_for_account(account_id: int, skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    return read_db_investment_transactions(db=db, user_id=user_id, account_id=account_id, skip=skip, limit=limit)

@router.get("/transactions/{transaction_id}", response_model=InvestmentTransactionResponse)
def read_transaction(transaction_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_transaction = read_db_investment_transaction(db=db, transaction_id=transaction_id, user_id=user_id)
    if db_transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    return db_transaction

@router.put("/transactions/{transaction_id}", response_model=InvestmentTransactionResponse)
def update_transaction(transaction_id: int, transaction: InvestmentTransactionUpdate, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    try:
        return update_db_investment_transaction(db=db, transaction_id=transaction_id, user_id=user_id, transaction_updates=transaction)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail="Transaction not found") from e

@router.delete("/transactions/{transaction_id}", response_model=InvestmentTransactionResponse)
def delete_transaction(transaction_id: int, db: Session = Depends(get_db)):
    user_id = get_current_user_id()
    db_transaction = read_db_investment_transaction(db, transaction_id=transaction_id, user_id=user_id)
    if db_transaction is None:
        raise HTTPException(status_code=404, detail="Transaction not found")
    try:
        delete_db_investment_transaction(db, transaction_id=transaction_id, user_id=user_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return db_transaction
