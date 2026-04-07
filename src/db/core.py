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

    # Auth — reject JWTs whose `iat` is earlier than this timestamp.
    # Bumping this value (to now()) is the "log out everywhere" mechanism for a single user.
    jwt_valid_after: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    
    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    accounts = relationship("AccountDB", back_populates="user")
    transactions = relationship("TransactionDB", back_populates="user")
    tags = relationship("TagDB", back_populates="user")
    budget_templates = relationship("BudgetTemplateDB", back_populates="user")
    debt_repayment_plans = relationship("DebtRepaymentPlanDB", back_populates="user")
    financial_plans = relationship("FinancialPlanDB", back_populates="user")


class CategoryDB(Base):
    __tablename__ = "categories"

    __table_args__ = (
        UniqueConstraint("name", name="uq_category_name"),
        Index("idx_category_name", "name"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    uuid: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    parent_category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    
    # Relationship to self for subcategories
    parent = relationship("CategoryDB", remote_side=[id], back_populates="children")
    children = relationship("CategoryDB", back_populates="parent")

    # Relationships to budget categories
    budget_allocations = relationship("BudgetTemplateCategoryDB", foreign_keys="BudgetTemplateCategoryDB.category_id", back_populates="category")

    # Relationships to transactions (as primary and sub-category)
    primary_transactions = relationship("TransactionDB", foreign_keys="TransactionDB.category_id", back_populates="category")
    sub_transactions = relationship("TransactionDB", foreign_keys="TransactionDB.subcategory_id", back_populates="subcategory")


class TransactionType(enum.Enum):
    PURCHASE = "PURCHASE"
    CREDIT = "CREDIT"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    DEPOSIT = "DEPOSIT"
    WITHDRAWAL = "WITHDRAWAL"
    FEE = "FEE"
    INTEREST = "INTEREST"


class InvestmentTransactionType(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    INTEREST = "INTEREST"
    FEE = "FEE"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    SPLIT = "SPLIT"
    MERGER = "MERGER"
    SPINOFF = "SPINOFF"
    REINVESTMENT = "REINVESTMENT"
    EXPIRATION = "EXPIRATION"


class SourceType(enum.Enum):
    CSV = "CSV"
    PDF = "PDF"
    MANUAL = "MANUAL"
    API = "API"


class RelationshipType(enum.Enum):
    OFFSETS = "OFFSETS"
    REFUNDS = "REFUNDS"
    FEES_FOR = "FEES_FOR"
    REVERSES = "REVERSES"


class AccountType(enum.Enum):
    CHECKING = "CHECKING"
    SAVINGS = "SAVINGS"
    CREDIT_CARD = "CREDIT_CARD"
    INVESTMENT = "INVESTMENT"
    LOAN = "LOAN"
    OTHER = "OTHER"


class BudgetTemplateDB(Base):
    __tablename__ = "budget_templates"

    __table_args__ = (
        UniqueConstraint("user_id", "template_name", name="uq_user_template_name"),
    )

    # Primary Key
    template_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Key
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))

    # Template Data
    template_name: Mapped[str] = mapped_column(String(255), nullable=False)
    is_default: Mapped[bool] = mapped_column(Boolean, default=False)

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB", back_populates="budget_templates")
    categories = relationship("BudgetTemplateCategoryDB", back_populates="template", cascade="all, delete-orphan")
    month_assignments = relationship("BudgetMonthDB", back_populates="template")


class BudgetTemplateCategoryDB(Base):
    __tablename__ = "budget_template_categories"

    __table_args__ = (
        UniqueConstraint("template_id", "category_id", "subcategory_id",
                         name="uq_template_category_sub"),
    )

    # Primary Key
    allocation_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Keys
    template_id: Mapped[int] = mapped_column(ForeignKey("budget_templates.template_id"))
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    subcategory_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))

    # Budget Allocation
    allocated_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    template = relationship("BudgetTemplateDB", back_populates="categories")
    category = relationship("CategoryDB", foreign_keys=[category_id], back_populates="budget_allocations")
    subcategory = relationship("CategoryDB", foreign_keys=[subcategory_id])


class BudgetMonthDB(Base):
    __tablename__ = "budget_months"

    __table_args__ = (
        UniqueConstraint("user_id", "year", "month", name="uq_user_year_month"),
        Index("idx_budget_months_user", "user_id"),
        Index("idx_budget_months_template", "template_id"),
    )

    # Primary Key
    month_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Keys
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    template_id: Mapped[Optional[int]] = mapped_column(ForeignKey("budget_templates.template_id"))

    # Month Data
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB")
    template = relationship("BudgetTemplateDB", back_populates="month_assignments")


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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Key
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))

    # Holding Data
    symbol: Mapped[str] = mapped_column(String(20), nullable=False)  # e.g., "AAPL", "VTSAX", "AAPL250117C00150000"
    quantity: Mapped[Decimal] = mapped_column(DECIMAL(15, 6), nullable=False)  # shares/units/contracts owned
    average_cost_basis: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # average price paid per share
    current_price: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # latest market price
    last_price_update: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Security classification
    security_type: Mapped[Optional[str]] = mapped_column(String(20))  # STOCK, ETF, MUTUAL_FUND, OPTION, FUTURE, BOND, CRYPTO

    # Options-specific fields (nullable for non-options)
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
    )

    # Primary Key (internal)
    investment_transaction_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # External-facing UUID (for API consistency with regular transactions)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Keys
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"), nullable=False)
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))
    holding_id: Mapped[Optional[int]] = mapped_column(ForeignKey("investment_holdings.holding_id"))  # Optional for dividends, etc.

    # Deduplication & Source Tracking
    transaction_hash: Mapped[str] = mapped_column(String(64), nullable=False)

    # Transaction Data
    transaction_type: Mapped[InvestmentTransactionType] = mapped_column(Enum(InvestmentTransactionType))
    symbol: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)  # Underlying ticker (e.g., "AAPL", "SPY"); NULL for non-share types
    api_symbol: Mapped[Optional[str]] = mapped_column(String(50))  # yfinance API format (OCC for options)
    quantity: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 6))  # shares/units (null for dividends)
    price_per_share: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # price per share/unit
    total_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)  # total transaction value
    fees: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # transaction fees
    transaction_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Description & Details
    description: Mapped[Optional[str]] = mapped_column(String(500))

    # Security classification
    security_type: Mapped[Optional[str]] = mapped_column(String(20))  # STOCK, ETF, MUTUAL_FUND, OPTION, FUTURE, BOND, CRYPTO

    # Sale-specific fields
    cost_basis_at_sale: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 4))  # Snapshot of avg cost basis at time of SELL

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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
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

    # Audit Trail
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    user = relationship("UserDB", back_populates="transactions")
    account = relationship("AccountDB", back_populates="transactions")
    category = relationship("CategoryDB", foreign_keys=[category_id], back_populates="primary_transactions")
    subcategory = relationship("CategoryDB", foreign_keys=[subcategory_id], back_populates="sub_transactions")
    
    # Relationship tables
    relationship_from = relationship("TransactionRelationshipDB", foreign_keys="TransactionRelationshipDB.from_transaction_id", back_populates="from_transaction", cascade="all, delete-orphan")
    relationship_to = relationship("TransactionRelationshipDB", foreign_keys="TransactionRelationshipDB.to_transaction_id", back_populates="to_transaction", cascade="all, delete-orphan")

    # Tags relationship
    transaction_tags = relationship("TransactionTagDB", back_populates="transaction", cascade="all, delete-orphan")

    # Split allocations
    split_allocations = relationship("TransactionSplitAllocationDB", back_populates="transaction",
                                      cascade="all, delete-orphan")

    # Amortization schedule
    amortization_schedule = relationship("TransactionAmortizationScheduleDB", back_populates="transaction",
                                          cascade="all, delete-orphan")


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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

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


class TransactionSplitAllocationDB(Base):
    __tablename__ = "transaction_split_allocations"

    __table_args__ = (
        UniqueConstraint(
            "transaction_id", "category_id", "subcategory_id",
            name="uq_split_allocation_txn_cat_sub"
        ),
        Index("idx_split_alloc_transaction", "transaction_id"),
        Index("idx_split_alloc_category", "category_id"),
    )

    allocation_id: Mapped[int]           = mapped_column(primary_key=True, autoincrement=True)
    id:            Mapped[UUID]          = mapped_column(unique=True, nullable=False)
    transaction_id: Mapped[int]          = mapped_column(ForeignKey("transactions.db_id"))
    category_id:   Mapped[int]           = mapped_column(ForeignKey("categories.id"))
    subcategory_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    amount:        Mapped[Decimal]       = mapped_column(DECIMAL(15, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    transaction = relationship("TransactionDB", back_populates="split_allocations")
    category    = relationship("CategoryDB", foreign_keys=[category_id])
    subcategory = relationship("CategoryDB", foreign_keys=[subcategory_id])


class TransactionAmortizationScheduleDB(Base):
    __tablename__ = "transaction_amortization_schedules"

    __table_args__ = (
        UniqueConstraint("transaction_id", "month_date", name="uq_amortization_txn_month"),
        Index("idx_amortization_transaction", "transaction_id"),
        Index("idx_amortization_month", "month_date"),
    )

    schedule_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    transaction_id: Mapped[int] = mapped_column(ForeignKey("transactions.db_id"))
    month_date: Mapped[date] = mapped_column(Date, nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    transaction = relationship("TransactionDB", back_populates="amortization_schedule")


class TagDB(Base):
    __tablename__ = "tags"
    
    __table_args__ = (
        # Prevent duplicate tag names per user
        UniqueConstraint("user_id", "tag_name", name="uq_user_tag_name"),
    )

    # Primary Key
    tag_id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Key
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))

    # Tag Data
    tag_name: Mapped[str] = mapped_column(String(100), nullable=False)
    color: Mapped[Optional[str]] = mapped_column(String(7))  # Hex color code
    is_system: Mapped[bool] = mapped_column(Boolean, default=False)

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
    uuid: Mapped[UUID] = mapped_column(unique=True, nullable=False)
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

    # Investment-specific fields (only used for INVESTMENT account types)
    initial_cash_balance: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), default=0.00)  # Starting cash for transaction replay

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
    uuid: Mapped[UUID] = mapped_column(unique=True, nullable=False)

    # Foreign Key
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))

    # Snapshot Data
    value_date: Mapped[date] = mapped_column(Date, nullable=False)  # The date of this snapshot
    balance: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)  # Account value on this date

    # Investment-specific fields (nullable for non-investment accounts)
    securities_value: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Market value of holdings
    cash_balance: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Cash in account
    total_cost_basis: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Total amount invested
    unrealized_gain_loss: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Current value - cost basis
    realized_gain_loss: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Gain/loss from sells

    # Loan-specific fields (nullable for non-loan accounts)
    principal_paid_ytd: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Principal paid this year
    interest_paid_ytd: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 2))  # Interest paid this year

    # Metadata
    snapshot_source: Mapped[str] = mapped_column(String(50), default="MANUAL")  # MANUAL, SCHEDULED, BACKFILL

    # Backfill audit trail
    last_recalculated_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    recalculation_count: Mapped[int] = mapped_column(Integer, default=0)
    recalculation_reason: Mapped[Optional[str]] = mapped_column(String(255))
    needs_review: Mapped[bool] = mapped_column(Boolean, default=False)
    review_reason: Mapped[Optional[str]] = mapped_column(String(255))

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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
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
    id: Mapped[UUID] = mapped_column(unique=True, nullable=False)
    month_id: Mapped[int] = mapped_column(ForeignKey("financial_plan_months.month_id"))
    category_id: Mapped[int] = mapped_column(ForeignKey("categories.id"))
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    expense_type: Mapped[str] = mapped_column(String(20), nullable=False)  # 'recurring' or 'one_time'
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    month = relationship("FinancialPlanMonthDB", back_populates="expenses")
    category = relationship("CategoryDB")

    @property
    def category_uuid(self):
        return self.category.uuid if self.category else None


class SnapshotBackfillJobDB(Base):
    """
    Tracks async backfill jobs that recalculate historical account snapshots.
    Created when historical investment transactions are uploaded.
    """
    __tablename__ = "snapshot_backfill_jobs"

    __table_args__ = (
        Index("idx_backfill_jobs_account", "account_id"),
        Index("idx_backfill_jobs_status", "status"),
        Index("idx_backfill_jobs_created", "created_at"),
    )

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Keys
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"))
    account_id: Mapped[int] = mapped_column(ForeignKey("accounts.id"))

    # Job Parameters
    start_date: Mapped[date] = mapped_column(Date, nullable=False)
    end_date: Mapped[date] = mapped_column(Date, nullable=False)

    # Job Status
    status: Mapped[str] = mapped_column(String(20), nullable=False)  # PENDING, IN_PROGRESS, COMPLETED, FAILED, QUEUED
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Results
    snapshots_created: Mapped[int] = mapped_column(Integer, default=0)
    snapshots_updated: Mapped[int] = mapped_column(Integer, default=0)
    snapshots_failed: Mapped[int] = mapped_column(Integer, default=0)
    snapshots_skipped: Mapped[int] = mapped_column(Integer, default=0)

    # Relationships
    user = relationship("UserDB")
    account = relationship("AccountDB")


class UploadJobDB(Base):
    """
    Tracks statement upload jobs and their processing status.
    Enables async processing with result tracking.
    """
    __tablename__ = "upload_jobs"

    __table_args__ = (
        Index("idx_upload_jobs_user", "user_id"),
        Index("idx_upload_jobs_account", "account_id"),
        Index("idx_upload_jobs_status", "status"),
        Index("idx_upload_jobs_created", "created_at"),
    )

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Keys
    user_id: Mapped[int] = mapped_column(ForeignKey("users.db_id"), nullable=False)
    account_id: Mapped[Optional[int]] = mapped_column(ForeignKey("accounts.id"))

    # Upload Details
    file_path: Mapped[Optional[str]] = mapped_column(String(500))  # S3 key, local path, or null if deleted
    institution: Mapped[str] = mapped_column(String(100), nullable=False)
    skip_duplicates: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Job Status
    status: Mapped[str] = mapped_column(String(20), default="PENDING", nullable=False)  # PENDING, PROCESSING, COMPLETED, FAILED
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Result Metrics
    transactions_created: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    transactions_skipped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    investment_transactions_created: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    investment_transactions_skipped: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    completed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)

    # Relationships
    user = relationship("UserDB")
    account = relationship("AccountDB")
    skipped_transactions = relationship("SkippedTransactionDB", back_populates="upload_job")


class SkippedTransactionDB(Base):
    """
    Stores details of transactions that were skipped during upload due to being duplicates.
    Preserves audit trail even if original transaction is deleted.
    """
    __tablename__ = "skipped_transactions"

    __table_args__ = (
        Index("idx_skipped_transactions_job", "upload_job_id"),
        Index("idx_skipped_transactions_date", "parsed_date"),
    )

    # Primary Key
    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    # Foreign Keys
    upload_job_id: Mapped[int] = mapped_column(ForeignKey("upload_jobs.id"), nullable=False)

    # Transaction Type
    transaction_type: Mapped[str] = mapped_column(String(20), nullable=False)  # "REGULAR" or "INVESTMENT"

    # Parsed Transaction Data (preserved for audit trail)
    parsed_date: Mapped[date] = mapped_column(Date, nullable=False)
    parsed_amount: Mapped[Decimal] = mapped_column(DECIMAL(15, 2), nullable=False)
    parsed_description: Mapped[str] = mapped_column(Text, nullable=False)
    parsed_transaction_type: Mapped[str] = mapped_column(String(50), nullable=False)
    parsed_symbol: Mapped[Optional[str]] = mapped_column(String(20))  # For investment transactions
    parsed_quantity: Mapped[Optional[Decimal]] = mapped_column(DECIMAL(15, 6))  # For investment transactions
    parsed_data_json: Mapped[Optional[str]] = mapped_column(JSON)  # Full parsed data as backup

    # References to Existing Transactions (SET NULL on delete for audit preservation)
    existing_transaction_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("transactions.id", ondelete="SET NULL")
    )
    existing_investment_transaction_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("investment_transactions.id", ondelete="SET NULL")
    )

    # Timestamp
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)

    # Relationships
    upload_job = relationship("UploadJobDB", back_populates="skipped_transactions")
    existing_transaction = relationship("TransactionDB", foreign_keys=[existing_transaction_id])
    existing_investment_transaction = relationship("InvestmentTransactionDB", foreign_keys=[existing_investment_transaction_id])


engine = create_engine(DATABASE_URL, echo=os.getenv("SQL_ECHO", "false").lower() == "true")
session_local = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Dependency to get the database session
def get_db():
    database = session_local()
    try:
        yield database
    finally:
        database.close()
