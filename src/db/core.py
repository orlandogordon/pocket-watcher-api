import os
from typing import Optional
from sqlalchemy import create_engine, ForeignKey, Index, UniqueConstraint, Boolean, Column, Integer, String, Text, JSON, DECIMAL, DateTime, Date
from sqlalchemy.types import Enum
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Mapped, relationship, mapped_column
from datetime import datetime, date
from uuid import UUID
from decimal import Decimal
import enum


DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///test.db")


class NotFoundError(Exception):
    pass


class Base(DeclarativeBase):
    pass


class UserDB(Base):
    __tablename__ = "users"
    
    __table_args__ = (
        # Unique constraints
        UniqueConstraint("email", name="uq_user_email"),
        UniqueConstraint("username", name="uq_user_username"),
        
        # Query indexes
        Index("idx_users_email", "email"),
    )

    # Core User Identification
    db_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    
    # Authentication
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    username: Mapped[str] = mapped_column(String(100), nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    
    # Personal Information
    first_name: Mapped[Optional[str]] = mapped_column(String(100))
    last_name: Mapped[Optional[str]] = mapped_column(String(100))
    date_of_birth: Mapped[Optional[date]] = mapped_column(Date)
    
    # Activity Tracking
    last_login_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    accounts = relationship("AccountDB", back_populates="user")
    transactions = relationship("TransactionDB", back_populates="user")
    tags = relationship("TagDB", back_populates="user")
    budgets = relationship("BudgetDB", back_populates="user")
    debt_repayment_plans = relationship("DebtRepaymentPlanDB", back_populates="user")
    financial_plans = relationship("FinancialPlanDB", back_populates="user")


class CategoryDB(Base):
    __tablename__ = "categories"
    
    __table_args__ = (
        UniqueConstraint("name", name="uq_category_name"),
        Index("idx_category_name", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    parent_category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    
    # Relationship to self for subcategories
    parent = relationship("CategoryDB", remote_side=[id], back_populates="children")
    children = relationship("CategoryDB", back_populates="parent")

    # Relationships to budget categories
    budget_allocations = relationship("BudgetCategoryDB", back_populates="category")

    # Relationships to transactions (as primary and sub-category)
    primary_transactions = relationship("TransactionDB", foreign_keys="TransactionDB.category_id", back_populates="category")
    sub_transactions = relationship("TransactionDB", foreign_keys="TransactionDB.subcategory_id", back_populates="subcategory")


class TransactionType(enum.Enum):
    PURCHASE = "PURCHASE"
    CREDIT = "CREDIT"
    TRANSFER = "TRANSFER"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    FEE = "FEE"
    INTEREST = "INTEREST"


class InvestmentTransactionType(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    INTEREST = "INTEREST"
    SPLIT = "SPLIT"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"
    REINVESTMENT = "REINVESTMENT"


class SourceType(enum.Enum):
    CSV = "CSV"
    PDF = "PDF"
    MANUAL = "MANUAL"
    API = "API"


class RelationshipType(enum.Enum):
    OFFSETS = "OFFSETS"
    REFUNDS = "REFUNDS"
    SPLITS = "SPLITS"
    FEES_FOR = "FEES_FOR"
    REVERSES = "REVERSES"


class AccountType(enum.Enum):
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    CREDIT_CARD = "CREDIT_CARD"
    INVESTMENT = "INVESTMENT"
    LOAN = "LOAN"
    OTHER = "OTHER"


class BudgetDB(Base):
    __tablename__ = "budgets"
    
    __table_args__ = (
        # Prevent duplicate budget names per user
        UniqueConstraint("user_id", "budget_name", name="uq_user_budget_name"),
    )

    # Primary Key
    budget_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Foreign Key
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    
    # Budget Data
    budget_name: Mapped[str] = mapped_column(String(255), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB", back_populates="budgets")
    budget_categories = relationship("BudgetCategoryDB", back_populates="budget")


class BudgetCategoryDB(Base):
    __tablename__ = "budget_categories"
    
    __table_args__ = (
        # Prevent duplicate category allocations per budget
        UniqueConstraint("budget_id", "category_id", name="uq_budget_category"),
    )

    # Primary Key
    budget_category_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Foreign Keys
    budget_id: Mapped[int] = mapped_column(ForeignKey("budgets.budget_id"))
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    
    # Budget Allocation
    allocated_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    budget = relationship("BudgetDB", back_populates="budget_categories")
    category = relationship("CategoryDB", back_populates="budget_allocations")


class InvestmentHoldingDB(Base):
    __tablename__ = "investment_holdings"

    __table_args__ = (
        # Prevent duplicate holdings per account/symbol
        UniqueConstraint("account_id", "symbol", name="uq_account_symbol"),

        # Query indexes
        Index("idx_holdings_account", "account_id"),
        Index("idx_holdings_symbol", "symbol"),
    )

    # Primary Key
    holding_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))

    # Holding Data
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)  # e.g., "AAPL", "VTSAX", "AAPL250117C00150000"
    quantity: Mapped[Decimal] = mapped_column(DECIMAL(15, 6), nullable=False)  # shares/units/contracts owned
    average_cost_basis: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # average price paid per share
    current_price: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # latest market price
    last_price_update: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Options-specific fields (nullable for stocks)
    underlying_symbol: Mapped[Optional[str]] = mapped_column(String(10))  # e.g., "AAPL" for option
    option_type: Mapped[Optional[str]] = mapped_column(String(4))  # "CALL" or "PUT"
    strike_price: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # e.g., 150.00
    expiration_date: Mapped[Optional[date]] = mapped_column(Date)  # e.g., 2025-01-17

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    account = relationship("AccountDB", back_populates="investment_holdings")
    investment_transactions = relationship("InvestmentTransactionDB", back_populates="holding")


class InvestmentTransactionDB(Base):
    __tablename__ = "investment_transactions"

    __table_args__ = (
        # Query indexes
        Index("idx_investment_transactions_user_date", "user_id", "transaction_date"),
        Index("idx_investment_transactions_account_date", "account_id", "transaction_date"),
        Index("idx_investment_transactions_holding", "holding_id"),
        Index("idx_investment_transactions_date", "transaction_date"),
        Index("idx_investment_transactions_type", "transaction_type"),

        # Duplicate prevention
        UniqueConstraint("user_id", "transaction_hash", name="uq_user_investment_transaction_hash"),
    )

    # Primary Key
    investment_transaction_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Keys
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"), nullable=False)
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))
    holding_id: Mapped[Optional[int]] = mapped_column(ForeignKey("investment_holdings.holding_id"))  # Optional for dividends, etc.

    # Deduplication & Source Tracking
    transaction_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Transaction Data
    transaction_type: Mapped[InvestmentTransactionType] = mapped_column(Enum(InvestmentTransactionType))
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)  # Underlying ticker (e.g., "AAPL", "SPY")
    api_symbol: Mapped[Optional[str]] = mapped_column(String(50))  # yfinance API format (OCC for options)
    quantity: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 6))  # shares/units (null for dividends)
    price_per_share: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # price per share/unit
    total_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)  # total transaction value
    fees: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # transaction fees
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Description & Details
    description: Mapped[Optional[str]] = mapped_column(String(500))

    # Processing
    needs_review: Mapped[bool] = mapped_column(Boolean, default=False)

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB")
    account = relationship("AccountDB", back_populates="investment_transactions")
    holding = relationship("InvestmentHoldingDB", back_populates="investment_transactions")


class DebtPaymentDB(Base):
    __tablename__ = "debt_payments"
    
    __table_args__ = (
        # Query indexes
        Index("idx_debt_payments_loan_account", "loan_account_id"),
        Index("idx_debt_payments_source_account", "payment_source_account_id"),
        Index("idx_debt_payments_date", "payment_date"),
        Index("idx_debt_payments_transaction", "transaction_id"),
    )

    # Primary Key
    payment_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Foreign Keys
    loan_account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))  # The loan account
    payment_source_account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))  # Checking account used for payment
    transaction_id: Mapped[Optional[int]] = mapped_column(ForeignKey("transactions.db_id"))  # Links to bank statement transaction
    
    # Payment Data
    payment_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    principal_amount: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))
    interest_amount: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))
    remaining_balance_after_payment: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))
    payment_date: Mapped[date] = mapped_column(Date, nullable=False)
    
    # Description & Details
    description: Mapped[Optional[str]] = mapped_column(String(500))
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    loan_account = relationship("AccountDB", foreign_keys=[loan_account_id], back_populates="debt_payments")
    payment_source_account = relationship("AccountDB", foreign_keys=[payment_source_account_id], back_populates="debt_payments_from")
    transaction = relationship("TransactionDB")


class DebtStrategy(enum.Enum):
    AVALANCHE = "AVALANCHE"
    SNOWBALL = "SNOWBALL"
    CUSTOM = "CUSTOM"


class DebtRepaymentPlanDB(Base):
    __tablename__ = "debt_repayment_plans"

    __table_args__ = (
        UniqueConstraint("user_id", "plan_name", name="uq_user_debt_plan_name"),
    )

    plan_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    plan_name: Mapped[str] = mapped_column(String(255), nullable=False)
    strategy: Mapped[DebtStrategy] = mapped_column(Enum(DebtStrategy), default=DebtStrategy.CUSTOM)
    target_payoff_date: Mapped[Optional[date]] = mapped_column(Date)
    status: Mapped[str] = mapped_column(String(50), default="ACTIVE")

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("UserDB", back_populates="debt_repayment_plans")
    linked_accounts = relationship("DebtPlanAccountLinkDB", back_populates="plan", cascade="all, delete-orphan")


class DebtPlanAccountLinkDB(Base):
    __tablename__ = "debt_plan_account_links"

    plan_id: Mapped[int] = mapped_column(ForeignKey("debt_repayment_plans.plan_id"), primary_key=True)
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"), primary_key=True)
    priority: Mapped[int] = mapped_column(Integer, default=0)

    plan = relationship("DebtRepaymentPlanDB", back_populates="linked_accounts")
    account = relationship("AccountDB", back_populates="debt_repayment_plans_link")


class DebtRepaymentScheduleDB(Base):
    __tablename__ = "debt_repayment_schedules"
    
    __table_args__ = (
        UniqueConstraint("user_id", "account_id", "payment_month", name="uq_user_account_month_payment"),
    )

    schedule_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))
    
    payment_month: Mapped[date] = mapped_column(Date, nullable=False)
    scheduled_payment_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    
    user = relationship("UserDB")
    account = relationship("AccountDB", back_populates="debt_repayment_schedules")


class TransactionDB(Base):
    __tablename__ = "transactions"
    
    __table_args__ = (
        # Performance indexes for common queries
        Index("idx_transactions_user_date", "user_id", "transaction_date"),
        Index("idx_transactions_user_account", "user_id", "account_id"), 
        Index("idx_transactions_date", "transaction_date"),
        
        # Duplicate prevention
        UniqueConstraint("user_id", "transaction_hash", name="uq_user_transaction_hash"),
        
    )

    # Core Transaction Identification
    db_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))
    category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    subcategory_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))

    # Deduplication & Source Tracking
    transaction_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    source_type: Mapped[SourceType] = mapped_column(Enum(SourceType))

    # Basic Transaction Data
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    transaction_type: Mapped[TransactionType] = mapped_column(Enum(TransactionType))

    # Description & Details
    description: Mapped[Optional[str]] = mapped_column(String(500))
    parsed_description: Mapped[Optional[str]] = mapped_column(Text)
    merchant_name: Mapped[Optional[str]] = mapped_column(String(255))
    comments: Mapped[Optional[str]] = mapped_column(Text)

    # Financial Institution Data
    institution_name: Mapped[Optional[str]] = mapped_column(String(255))
    account_number_last4: Mapped[Optional[str]] = mapped_column(String(4))

    # Processing
    needs_review: Mapped[bool] = mapped_column(Boolean, default=False)

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB", back_populates="transactions")
    account = relationship("AccountDB", back_populates="transactions")
    category = relationship("CategoryDB", foreign_keys=[category_id], back_populates="primary_transactions")
    subcategory = relationship("CategoryDB", foreign_keys=[subcategory_id], back_populates="sub_transactions")
    
    # Relationship tables
    relationship_from = relationship("TransactionRelationshipDB", foreign_keys="TransactionRelationshipDB.from_transaction_id", back_populates="from_transaction")
    relationship_to = relationship("TransactionRelationshipDB", foreign_keys="TransactionRelationshipDB.to_transaction_id", back_populates="to_transaction")
    
    # Tags relationship
    transaction_tags = relationship("TransactionTagDB", back_populates="transaction")


class TransactionRelationshipDB(Base):
    __tablename__ = "transaction_relationships"
    
    __table_args__ = (
        # Query relationships from either direction
        Index("idx_rel_from_transaction", "from_transaction_id"),
        Index("idx_rel_to_transaction", "to_transaction_id"),
        Index("idx_rel_type", "relationship_type"),
        
        # Prevent duplicate relationships
        UniqueConstraint("from_transaction_id", "to_transaction_id", "relationship_type", 
                        name="uq_transaction_relationship"),
    )

    # Primary Key
    relationship_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Foreign Keys
    from_transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.db_id"))
    to_transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.db_id"))
    
    # Relationship Data
    relationship_type: Mapped[RelationshipType] = mapped_column(Enum(RelationshipType))
    amount_allocated: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))
    notes: Mapped[Optional[str]] = mapped_column(Text)
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    from_transaction = relationship("TransactionDB", foreign_keys=[from_transaction_id], back_populates="relationship_from")
    to_transaction = relationship("TransactionDB", foreign_keys=[to_transaction_id], back_populates="relationship_to")


class TagDB(Base):
    __tablename__ = "tags"
    
    __table_args__ = (
        # Prevent duplicate tag names per user
        UniqueConstraint("user_id", "tag_name", name="uq_user_tag_name"),
    )

    # Primary Key
    tag_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    
    # Foreign Key
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    
    # Tag Data
    tag_name: Mapped[str] = mapped_column(String(100), nullable=False)
    color: Mapped[Optional[str]] = mapped_column(String(7))  # Hex color code
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    user = relationship("UserDB", back_populates="tags")
    transaction_tags = relationship("TransactionTagDB", back_populates="tag")


class TransactionTagDB(Base):
    __tablename__ = "transaction_tags"

    # Composite Primary Key
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.db_id"), primary_key=True)
    tag_id: Mapped[int] = mapped_column(ForeignKey("tags.tag_id"), primary_key=True)
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    transaction = relationship("TransactionDB", back_populates="transaction_tags")
    tag = relationship("TagDB", back_populates="transaction_tags")


class AccountDB(Base):
    __tablename__ = "accounts"

    __table_args__ = (
        # Prevent duplicate account names per user
        UniqueConstraint("user_id", "account_name", name="uq_user_account_name"),
    )

    # Core Account Identification
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))

    # Account Details
    account_name: Mapped[str] = mapped_column(String(255), nullable=False)  # "Chase Checking", "Amex Gold Card"
    account_type: Mapped[AccountType] = mapped_column(Enum(AccountType))
    institution_name: Mapped[str] = mapped_column(String(255), nullable=False)
    account_number_last4: Mapped[Optional[str]] = mapped_column(String(4))

    # Loan-specific fields (only used for LOAN account types)
    original_principal: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))
    minimum_payment: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))
    interest_rate: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(5, 4))  # e.g., 0.0525 for 5.25%
    interest_rate_type: Mapped[Optional[str]] = mapped_column(String(20))  # "FIXED" or "VARIABLE"

    # Balance Tracking
    balance: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), default=0.00)
    balance_last_updated: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Optional Metadata
    comments: Mapped[Optional[str]] = mapped_column(Text)

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB", back_populates="accounts")
    transactions = relationship("TransactionDB", back_populates="account")
    investment_holdings = relationship("InvestmentHoldingDB", back_populates="account")
    investment_transactions = relationship("InvestmentTransactionDB", back_populates="account")
    debt_payments = relationship("DebtPaymentDB", foreign_keys="DebtPaymentDB.loan_account_id", back_populates="loan_account")
    debt_payments_from = relationship("DebtPaymentDB", foreign_keys="DebtPaymentDB.payment_source_account_id", back_populates="payment_source_account")
    debt_repayment_plans_link = relationship("DebtPlanAccountLinkDB", back_populates="account", cascade="all, delete-orphan")
    debt_repayment_schedules = relationship("DebtRepaymentScheduleDB", back_populates="account", cascade="all, delete-orphan")
    value_history = relationship("AccountValueHistoryDB", back_populates="account", cascade="all, delete-orphan")


class AccountValueHistoryDB(Base):
    """
    Daily snapshots of account values for historical tracking and net worth calculations.
    Supports all account types: checking, savings, credit cards, loans, and investments.
    """
    __tablename__ = "account_value_history"

    __table_args__ = (
        # Ensure only one snapshot per account per day
        UniqueConstraint("account_id", "value_date", name="uq_account_value_date"),

        # Query indexes for performance
        Index("idx_account_value_account", "account_id"),
        Index("idx_account_value_date", "value_date"),
        Index("idx_account_value_account_date", "account_id", "value_date"),
    )

    # Primary Key
    snapshot_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Key
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))

    # Snapshot Data
    value_date: Mapped[date] = mapped_column(Date, nullable=False)  # The date of this snapshot
    balance: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)  # Account value on this date

    # Investment-specific fields (nullable for non-investment accounts)
    total_cost_basis: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Total amount invested
    unrealized_gain_loss: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Current value - cost basis
    realized_gain_loss: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Gain/loss from sells

    # Loan-specific fields (nullable for non-loan accounts)
    principal_paid_ytd: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Principal paid this year
    interest_paid_ytd: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Interest paid this year

    # Metadata
    snapshot_source: Mapped[str] = mapped_column(String(50), default="SYSTEM")  # SYSTEM, MANUAL, EOD_JOB, etc.

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    account = relationship("AccountDB", back_populates="value_history")


class FinancialPlanDB(Base):
    __tablename__ = "financial_plans"

    __table_args__ = (
        UniqueConstraint("user_id", "plan_name", name="uq_user_financial_plan_name"),
    )

    plan_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    plan_name: Mapped[str] = mapped_column(String(255), nullable=False)
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    user = relationship("UserDB", back_populates="financial_plans")
    monthly_periods = relationship("FinancialPlanMonthDB", back_populates="plan", cascade="all, delete-orphan")


class FinancialPlanMonthDB(Base):
    __tablename__ = "financial_plan_months"
    __table_args__ = (
        UniqueConstraint("plan_id", "year", "month", name="uq_plan_year_month"),
    )
    month_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    plan_id: Mapped[int] = mapped_column(ForeignKey("financial_plans.plan_id"))
    year: Mapped[int] = mapped_column(nullable=False)
    month: Mapped[int] = mapped_column(nullable=False)
    planned_income: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    plan = relationship("FinancialPlanDB", back_populates="monthly_periods")
    expenses = relationship("FinancialPlanExpenseDB", back_populates="month", cascade="all, delete-orphan")


class FinancialPlanExpenseDB(Base):
    __tablename__ = "financial_plan_expenses"

    expense_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    month_id: Mapped[int] = mapped_column(ForeignKey("financial_plan_months.month_id"))
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    expense_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'recurring' or 'one_time'
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    month = relationship("FinancialPlanMonthDB", back_populates="expenses")
    category = relationship("CategoryDB")


engine = create_engine(DATABASE_URL, echo=os.getenv("SQL_ECHO", "false").lower() == "true")
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Dependency to get the database session
def get_db():
    database = session_local()
    try:
        yield database
    finally:
        database.close()
