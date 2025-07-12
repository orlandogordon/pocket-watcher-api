from typing import Optional, List
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from .core import TransactionDB, NotFoundError
from datetime import datetime, date
from uuid import uuid4, UUID

class Transaction(BaseModel):
    id: int
    user_id: int
    transaction_date: date
    description: str
    category: Optional[str] = None
    amount: float
    transaction_type: str  # e.g., "income", "expense"
    bank_name: str
    account_holder: str
    account_number: int

class TransactionInput(BaseModel):
    user_id: int
    transaction_date: date
    parsed_description: str
    category: Optional[str] = None
    amount: float
    transaction_identifier: Optional[str] = None 
    transaction_type: str  # e.g., "income", "expense"
    bank_name: Optional[str] = None
    account_holder: Optional[str] = None
    account_number: Optional[int] = None

class TransactionCreate(BaseModel):
    public_id: UUID = Field(default_factory=uuid4)
    user_id: int
    transaction_date: date
    description: Optional[str] = None
    parsed_description: str
    category: Optional[str] = None
    amount: float
    tags: Optional[str] = None  # Comma-separated tags
    transaction_identifier: Optional[str] = None 
    transaction_type: str  # e.g., "income", "expense"
    bank_name: Optional[str] = None
    account_holder: Optional[str] = None
    account_number: Optional[int] = None

    def model_post_init(self, __context):
        if self.transaction_identifier is None:
            self.transaction_identifier = f"{self.user_id}_{self.date.strftime('%Y%m%d')}_{self.description[:10]}_{self.amount}"
        if self.description is None:
            self.description = self.parsed_description

class TransactionUpdate(BaseModel):
    user_id: int
    transaction_date: date
    description: str
    category: Optional[str] = None
    # amount: float
    tags: Optional[str] = None  # Comma-separated tags
    transaction_identifier: Optional[str] = None
    transaction_type: str  # e.g., "income", "expense"
    bank_name: Optional[str] = None
    account_holder: Optional[str] = None
    account_number: Optional[int] = None

    def model_post_init(self, __context):
        if self.transaction_identifier is None:
            self.transaction_identifier = f"{self.user_id}_{self.date.strftime('%Y%m%d')}_{self.description[:10]}_{self.amount}"



def get_db_transaction(transaction_id: str, session: Session) -> TransactionDB:
    db_transaction = session.query(TransactionDB).filter(TransactionDB.public_id == transaction_id).first()
    if db_transaction is None:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")
    return db_transaction


def get_db_transaction_by_identifier(transaction_identifier: str, session: Session) -> TransactionDB:
    db_transaction = session.query(TransactionDB).filter(TransactionDB.transaction_identifier == transaction_identifier).first()
    if db_transaction is None:
        raise NotFoundError(f"Transaction with id {transaction_identifier} not found.")
    return db_transaction


def create_db_transaction(transaction: TransactionCreate, session: Session) -> TransactionDB:
    db_transaction = TransactionDB(**transaction.model_dump(exclude_none=True))

    existing = session.query(TransactionDB).filter(
        TransactionDB.user_id == db_transaction.user_id,
        TransactionDB.transaction_identifier == db_transaction.transaction_identifier
    ).first()

    if existing:
        # TODO: Handle the case where a transaction with the same identifier already exists
        raise ValueError(f"Transaction with identifier {db_transaction.transaction_identifier} already exists for user {db_transaction.user_id}.")
        # print(f"Transaction with identifier {db_transaction.transaction_identifier} already exists for user {db_transaction.user_id}. Returning exisiting transaction.")
        # return existing

    session.add(db_transaction)
    session.commit()
    session.refresh(db_transaction)

    return db_transaction

def create_db_transactions(transactions: List[TransactionCreate], session: Session) -> TransactionDB:
    db_transactions = [TransactionDB(**transaction.model_dump(exclude_none=True)) for transaction in transactions]  
    session.bulk_save_objects(db_transactions)
    session.commit()
    # TODO: Refresh the objects to get the updated state from the database
    # for transaction in db_transactions:
    #     session.refresh(transaction)
    return db_transactions

def update_db_transaction(transaction_id: str, transaction: TransactionUpdate, session: Session) -> TransactionDB:
    db_transaction = get_db_transaction(transaction_id, session)
    for key, value in transaction.model_dump(exclude_none=True).items():
        setattr(db_transaction, key, value)
    session.commit()
    session.refresh(db_transaction)

    return db_transaction


def delete_db_transaction(transaction_id: str, session: Session) -> TransactionDB:
    db_transaction = get_db_transaction(transaction_id, session)
    session.delete(db_transaction)
    session.commit()
    return db_transaction