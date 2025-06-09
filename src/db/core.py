from typing import Optional
from sqlalchemy import create_engine, ForeignKey, Boolean, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from datetime import datetime

DATABASE_URL = "sqlite:///test.db"
# DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/pocket_watcher_db"


class NotFoundError(Exception):
    pass


class Base(DeclarativeBase):
    pass


class UserDB(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str]
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now())


class TransactionDB(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    date: Mapped[datetime]
    description: Mapped[str]
    category: Mapped[str]
    amount: Mapped[float]
    transaction_type: Mapped[str]  # e.g., "income", "expense"
    bank_name: Mapped[str]
    account_holder: Mapped[str]
    account_number: Mapped[int]

    created_at: Mapped[datetime] = mapped_column(default=datetime.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None)

class InvestmentDB(Base):
    __tablename__ = "investments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    date: Mapped[datetime]
    transaction_type: Mapped[str]  # e.g., "buy", "sell"
    symbol: Mapped[str]
    description: Mapped[str]
    quantity: Mapped[float]
    price_per_unit: Mapped[float]
    total_value: Mapped[float]
    brokerage_name: Mapped[str]
    account_holder: Mapped[Optional[str]] = mapped_column(default=None)
    account_number: Mapped[int]

    created_at: Mapped[datetime] = mapped_column(default=datetime.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None)

engine = create_engine(DATABASE_URL, echo=True)
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


# Dependency to get the database session
def get_db():
    database = session_local()
    try:
        yield database
    finally:
        database.close()