from typing import Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .core import TransactionDB, NotFoundError
from datetime import datetime

class Transaction(BaseModel):
    id: int
    user_id: int
    date: datetime
    description: str
    category: Optional[str] = None
    amount: float
    transaction_type: str  # e.g., "income", "expense"
    bank_name: str
    account_holder: str
    account_number: int

class TransactionCreate(BaseModel):
    user_id: int
    date: datetime
    description: str
    category: Optional[str] = None
    amount: float
    transaction_type: str  # e.g., "income", "expense"
    bank_name: str
    account_holder: str
    account_number: int

class TransactionUpdate(BaseModel):
    user_id: int
    date: datetime
    description: str
    category: Optional[str] = None
    amount: float
    transaction_type: str  # e.g., "income", "expense"
    bank_name: str
    account_holder: str
    account_number: int


def read_db_transaction(transaction_id: int, session: Session) -> TransactionDB:
    db_transaction = session.query(TransactionDB).filter(TransactionDB.id == transaction_id).first()
    if db_transaction is None:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")
    return db_transaction


def create_db_transaction(transaction: TransactionCreate, session: Session) -> TransactionDB:
    db_transaction = TransactionDB(**transaction.model_dump(exclude_none=True))
    session.add(db_transaction)
    session.commit()
    session.refresh(db_transaction)

    return db_transaction


def update_db_transaction(transaction_id: int, transaction: TransactionUpdate, session: Session) -> TransactionDB:
    db_transaction = read_db_transaction(transaction_id, session)
    for key, value in transaction.model_dump(exclude_none=True).items():
        setattr(db_transaction, key, value)
    session.commit()
    session.refresh(db_transaction)

    return db_transaction


def delete_db_transaction(transaction_id: int, session: Session) -> TransactionDB:
    db_transaction = read_db_transaction(transaction_id, session)
    session.delete(db_transaction)
    session.commit()
    return db_transaction