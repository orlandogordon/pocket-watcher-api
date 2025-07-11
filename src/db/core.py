from typing import Optional
from sqlalchemy import create_engine, ForeignKey, Index, UniqueConstraint, Boolean, Column, Integer, String, DateTime, Date
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, mapped_column
from datetime import datetime, date
from uuid import UUID

# DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/pocket_watcher_db"
DATABASE_URL = "sqlite:///test.db"


class NotFoundError(Exception):
    pass


class Base(DeclarativeBase):
    pass


class UserDB(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    first_name: Mapped[str]
    last_name: Mapped[str]
    email: Mapped[str] = mapped_column(unique=True)
    password: Mapped[str]
    date_of_birth: Mapped[date]
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None)
    created_at: Mapped[datetime] = mapped_column(default=datetime.now())


class TransactionDB(Base):
    __tablename__ = "transactions"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    public_id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))
    
    transaction_date: Mapped[date]
    description: Mapped[str]
    parsed_description: Mapped[str]
    category: Mapped[str] = mapped_column(nullable=True)
    amount: Mapped[float]
    tags: Mapped[str] = mapped_column(default='', nullable=True)  # Comma-separated tags
    transaction_identifier: Mapped[str]
    transaction_type: Mapped[str]  # e.g., "income", "expense"
    bank_name: Mapped[str] = mapped_column(nullable=True)
    account_holder: Mapped[str] = mapped_column(nullable=True)
    account_number: Mapped[int] = mapped_column(nullable=True)

    created_at: Mapped[datetime] = mapped_column(default=datetime.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(default=None)

    __table_args__ = (
        Index("idx_transactions_userid", "user_id"),
        Index("idx_transactions_userid_date", "user_id", "transaction_date"),
        Index('idx_transactions_date', 'transaction_date'),
        UniqueConstraint('user_id', 'transaction_identifier', name='unique_transaction')
    )


class InvestmentDB(Base):
    __tablename__ = "investments"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    public_id: Mapped[str] = mapped_column(unique=True, nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"))

    transaction_date: Mapped[datetime]
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

    __table_args__ = (
        Index("idx_investments_userid", "user_id"),
        Index("idx_investments_userid_date", "user_id", "transaction_date"),
        Index('idx_investments_date', 'transaction_date'),
    )


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