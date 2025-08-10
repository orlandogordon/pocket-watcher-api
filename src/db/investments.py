from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import and_, or_, desc, asc
from typing import Optional, List, Dict, Any
from datetime import datetime, date
from decimal import Decimal
from uuid import uuid4, UUID
import hashlib
import json

# Import your database models
from .core import (
    InvestmentHoldingDB, 
    InvestmentTransactionDB, 
    AccountDB, 
    UserDB, 
    NotFoundError, 
    InvestmentTransactionType
)
from pydantic import BaseModel, Field, field_validator
from enum import Enum


# ===== ENUMS =====

class InvestmentTransactionTypeEnum(str, Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    INTEREST = "INTEREST"
    SPLIT = "SPLIT"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"
    REINVESTMENT = "REINVESTMENT"


# ===== INVESTMENT HOLDING PYDANTIC MODELS =====

class InvestmentHoldingBase(BaseModel):
    symbol: str = Field(..., max_length=20, description="Ticker symbol for the holding")
    quantity: Decimal = Field(..., description="Number of shares/units owned")
    average_cost_basis: Optional[Decimal] = Field(None, description="Average price paid per share")

    @field_validator('quantity', 'average_cost_basis')
    @classmethod
    def round_decimal_fields(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is not None:
            return round(v, 6)
        return v

class InvestmentHoldingCreate(InvestmentHoldingBase):
    account_id: int = Field(..., description="The account this holding belongs to")

class InvestmentHoldingUpdate(BaseModel):
    quantity: Optional[Decimal] = None
    average_cost_basis: Optional[Decimal] = None

    @field_validator('quantity', 'average_cost_basis')
    @classmethod
    def round_decimal_fields(cls, v: Optional[Decimal]) -> Optional[Decimal]:
        if v is not None:
            return round(v, 6)
        return v

class InvestmentHoldingResponse(InvestmentHoldingBase):
    holding_id: int
    account_id: int
    current_price: Optional[Decimal]
    last_price_update: Optional[datetime]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ===== INVESTMENT TRANSACTION PYDANTIC MODELS =====

class InvestmentTransactionBase(BaseModel):
    transaction_type: InvestmentTransactionTypeEnum
    symbol: str = Field(..., max_length=20)
    quantity: Optional[Decimal] = Field(None, description="Number of shares/units")
    price_per_share: Optional[Decimal] = Field(None, description="Price per share/unit")
    total_amount: Decimal = Field(..., description="Total transaction value")
    fees: Optional[Decimal] = Field(None, default=0.00)
    transaction_date: date
    description: Optional[str] = Field(None, max_length=500)

class InvestmentTransactionCreate(InvestmentTransactionBase):
    account_id: int

class InvestmentTransactionUpdate(BaseModel):
    transaction_type: Optional[InvestmentTransactionTypeEnum] = None
    quantity: Optional[Decimal] = None
    price_per_share: Optional[Decimal] = None
    total_amount: Optional[Decimal] = None
    fees: Optional[Decimal] = None
    transaction_date: Optional[date] = None
    description: Optional[str] = Field(None, max_length=500)

class InvestmentTransactionResponse(InvestmentTransactionBase):
    investment_transaction_id: int
    account_id: int
    holding_id: Optional[int]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


# ===== DATABASE OPERATIONS - INVESTMENT HOLDINGS =====

def create_db_investment_holding(db: Session, user_id: int, holding_data: InvestmentHoldingCreate) -> InvestmentHoldingDB:
    account = db.query(AccountDB).filter(
        AccountDB.id == holding_data.account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {holding_data.account_id} not found for this user.")

    existing_holding = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == holding_data.account_id,
        InvestmentHoldingDB.symbol == holding_data.symbol
    ).first()
    if existing_holding:
        raise ValueError(f"Holding with symbol {holding_data.symbol} already exists in this account.")

    db_holding = InvestmentHoldingDB(
        **holding_data.model_dump()
    )
    
    try:
        db.add(db_holding)
        db.commit()
        db.refresh(db_holding)
        return db_holding
    except IntegrityError:
        db.rollback()
        raise ValueError("Holding creation failed due to database constraint.")

def read_db_investment_holding(db: Session, holding_id: int, user_id: int) -> Optional[InvestmentHoldingDB]:
    return db.query(InvestmentHoldingDB).join(AccountDB).filter(
        InvestmentHoldingDB.holding_id == holding_id,
        AccountDB.user_id == user_id
    ).first()

def read_db_investment_holdings_by_account(db: Session, account_id: int, user_id: int) -> List[InvestmentHoldingDB]:
    account = db.query(AccountDB).filter(AccountDB.id == account_id, AccountDB.user_id == user_id).first()
    if not account:
        raise NotFoundError(f"Account with id {account_id} not found.")
    return db.query(InvestmentHoldingDB).filter(InvestmentHoldingDB.account_id == account_id).all()

def update_db_investment_holding(db: Session, holding_id: int, user_id: int, holding_updates: InvestmentHoldingUpdate) -> InvestmentHoldingDB:
    db_holding = read_db_investment_holding(db, holding_id, user_id)
    if not db_holding:
        raise NotFoundError(f"Holding with id {holding_id} not found.")

    update_data = holding_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(db_holding, field, value)
    
    db_holding.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(db_holding)
        return db_holding
    except IntegrityError:
        db.rollback()
        raise ValueError("Holding update failed.")

def delete_db_investment_holding(db: Session, holding_id: int, user_id: int) -> bool:
    db_holding = read_db_investment_holding(db, holding_id, user_id)
    if not db_holding:
        raise NotFoundError(f"Holding with id {holding_id} not found.")
    
    # Optional: Check if there are associated transactions before deleting
    
    try:
        db.delete(db_holding)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete holding: {str(e)}")


# ===== DATABASE OPERATIONS - INVESTMENT TRANSACTIONS =====

def create_db_investment_transaction(db: Session, user_id: int, transaction_data: InvestmentTransactionCreate) -> InvestmentTransactionDB:
    account = db.query(AccountDB).filter(
        AccountDB.id == transaction_data.account_id,
        AccountDB.user_id == user_id
    ).first()
    if not account:
        raise NotFoundError(f"Account with id {transaction_data.account_id} not found.")

    # Find or create the corresponding holding
    holding = db.query(InvestmentHoldingDB).filter(
        InvestmentHoldingDB.account_id == transaction_data.account_id,
        InvestmentHoldingDB.symbol == transaction_data.symbol
    ).first()

    if not holding and transaction_data.transaction_type in [InvestmentTransactionTypeEnum.BUY, InvestmentTransactionTypeEnum.REINVESTMENT]:
        holding_create = InvestmentHoldingCreate(
            account_id=transaction_data.account_id,
            symbol=transaction_data.symbol,
            quantity=Decimal('0'), # Will be updated by the transaction
            average_cost_basis=Decimal('0')
        )
        holding = create_db_investment_holding(db, user_id, holding_create)

    db_transaction = InvestmentTransactionDB(
        **transaction_data.model_dump(exclude={'transaction_type'}),
        transaction_type=InvestmentTransactionType(transaction_data.transaction_type.value),
        holding_id=holding.holding_id if holding else None
    )

    try:
        db.add(db_transaction)
        db.commit()
        db.refresh(db_transaction)
        
        # Update holding based on transaction
        if holding:
            update_holding_from_transaction(db, holding, db_transaction)

        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Investment transaction creation failed.")

def read_db_investment_transaction(db: Session, transaction_id: int, user_id: int) -> Optional[InvestmentTransactionDB]:
    return db.query(InvestmentTransactionDB).join(AccountDB).filter(
        InvestmentTransactionDB.investment_transaction_id == transaction_id,
        AccountDB.user_id == user_id
    ).first()

def read_db_investment_transactions(db: Session, user_id: int, account_id: Optional[int] = None, skip: int = 0, limit: int = 100) -> List[InvestmentTransactionDB]:
    query = db.query(InvestmentTransactionDB).join(AccountDB).filter(AccountDB.user_id == user_id)
    if account_id:
        query = query.filter(InvestmentTransactionDB.account_id == account_id)
    
    return query.order_by(desc(InvestmentTransactionDB.transaction_date)).offset(skip).limit(limit).all()

def update_db_investment_transaction(db: Session, transaction_id: int, user_id: int, transaction_updates: InvestmentTransactionUpdate) -> InvestmentTransactionDB:
    db_transaction = read_db_investment_transaction(db, transaction_id, user_id)
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")

    update_data = transaction_updates.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        if field == 'transaction_type' and value:
            setattr(db_transaction, field, InvestmentTransactionType(value.value))
        else:
            setattr(db_transaction, field, value)
            
    db_transaction.updated_at = datetime.utcnow()

    try:
        db.commit()
        db.refresh(db_transaction)
        # Note: Re-calculating holding state on update is complex and omitted here for simplicity.
        # A full implementation would require reversing the old transaction and applying the new one.
        return db_transaction
    except IntegrityError:
        db.rollback()
        raise ValueError("Transaction update failed.")

def delete_db_investment_transaction(db: Session, transaction_id: int, user_id: int) -> bool:
    db_transaction = read_db_investment_transaction(db, transaction_id, user_id)
    if not db_transaction:
        raise NotFoundError(f"Transaction with id {transaction_id} not found.")
    
    try:
        db.delete(db_transaction)
        db.commit()
        # Note: Re-calculating holding state on delete is also complex.
        return True
    except Exception as e:
        db.rollback()
        raise ValueError(f"Failed to delete transaction: {str(e)}")


# ===== UTILITY FUNCTIONS =====

def update_holding_from_transaction(db: Session, holding: InvestmentHoldingDB, transaction: InvestmentTransactionDB):
    """Updates a holding's quantity and cost basis after a transaction."""
    if transaction.transaction_type in [InvestmentTransactionType.BUY, InvestmentTransactionType.REINVESTMENT]:
        if transaction.quantity is not None and transaction.price_per_share is not None:
            new_quantity = holding.quantity + transaction.quantity
            if new_quantity > 0:
                old_total_cost = holding.quantity * (holding.average_cost_basis or 0)
                new_total_cost = transaction.quantity * transaction.price_per_share
                holding.average_cost_basis = (old_total_cost + new_total_cost) / new_quantity
            holding.quantity = new_quantity

    elif transaction.transaction_type == InvestmentTransactionType.SELL:
        if transaction.quantity is not None:
            holding.quantity -= transaction.quantity
            # Cost basis does not change on sell
    
    # Other transaction types like SPLIT would require more specific logic
    
    holding.updated_at = datetime.utcnow()
    
    try:
        db.commit()
        db.refresh(holding)
    except Exception:
        db.rollback()
        raise
