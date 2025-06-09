from typing import Optional
from pydantic import BaseModel
from sqlalchemy.orm import Session
from .core import InvestmentDB, NotFoundError
from datetime import datetime


class Investment(BaseModel):
    id: int
    user_id: int
    date: datetime
    transaction_type: str  # e.g., "income", "expense"
    symbol: str
    description: str
    quantity: float
    price_per_unit: float
    total_value: float
    brokerage_name: str
    account_holder: Optional[str] = None
    account_number: int

class InvestmentCreate(BaseModel):
    user_id: int
    date: datetime
    transaction_type: str  # e.g., "income", "expense"
    symbol: str
    description: str
    quantity: float
    price_per_unit: float
    total_value: float
    brokerage_name: str
    account_holder: Optional[str] = None
    account_number: int

class InvestmentUpdate(BaseModel):
    user_id: int
    date: datetime
    transaction_type: str  # e.g., "income", "expense"
    symbol: str
    description: str
    quantity: float
    price_per_unit: float
    total_value: float
    brokerage_name: str
    account_holder: Optional[str] = None
    account_number: int


def read_db_investment(investment_id: int, session: Session) -> InvestmentDB:
    db_investment = session.query(InvestmentDB).filter(InvestmentDB.id == investment_id).first()
    if db_investment is None:
        raise NotFoundError(f"Investment with id {investment_id} not found.")
    return db_investment


def create_db_investment(investment: InvestmentCreate, session: Session) -> InvestmentDB:
    db_investment = InvestmentDB(**investment.model_dump(exclude_none=True))
    session.add(db_investment)
    session.commit()
    session.refresh(db_investment)

    return db_investment


def update_db_investment(investment_id: int, investment: InvestmentUpdate, session: Session) -> InvestmentDB:
    db_investment = read_db_investment(investment_id, session)
    for key, value in investment.model_dump(exclude_none=True).items():
        setattr(db_investment, key, value)
    session.commit()
    session.refresh(db_investment)

    return db_investment


def delete_db_investment(investment_id: int, session: Session) -> InvestmentDB:
    db_investment = read_db_investment(investment_id, session)
    session.delete(db_investment)
    session.commit()
    return db_investment