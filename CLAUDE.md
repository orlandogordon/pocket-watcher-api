# Pocket Watcher API - Technical Documentation

## Overview

Pocket Watcher API is a personal finance management backend service built with FastAPI that provides comprehensive financial tracking capabilities. The application enables users to track transactions across multiple accounts, manage budgets, monitor investments, and create debt repayment strategies.

## Architecture

### Core Technology Stack

- **Framework**: FastAPI (Python 3.13)
- **Database ORM**: SQLAlchemy 2.0
- **Database**: SQLite (development) / PostgreSQL (production via DATABASE_URL)
- **Data Validation**: Pydantic
- **Database Migrations**: Alembic
- **PDF Processing**: pdfplumber
- **ASGI Server**: uvicorn

### Application Structure

```
pocket-watcher-api/
├── src/
│   ├── main.py                  # FastAPI application entry point
│   ├── crud/                    # Database CRUD operations
│   │   ├── __init__.py
│   │   ├── crud_account.py      # Account operations
│   │   ├── crud_budget.py       # Budget operations
│   │   ├── crud_category.py     # Category operations
│   │   ├── crud_debt.py         # Debt management operations
│   │   ├── crud_financial_plan.py # Financial planning operations
│   │   ├── crud_investment.py   # Investment operations
│   │   ├── crud_tag.py          # Tag operations
│   │   ├── crud_transaction.py  # Transaction operations
│   │   └── crud_user.py         # User operations
│   ├── db/
│   │   └── core.py              # Database models and configuration
│   ├── models/                  # Pydantic models for API
│   │   ├── __init__.py
│   │   ├── account.py
│   │   ├── budget.py
│   │   ├── category.py
│   │   ├── debt.py
│   │   ├── financial_plan.py
│   │   ├── investment.py
│   │   ├── tag.py
│   │   ├── transaction.py
│   │   └── user.py
│   ├── parser/                  # Statement parsing modules
│   │   ├── models.py            # Parsed data models
│   │   ├── tdbank.py            # TD Bank parser (table-based)
│   │   ├── amex.py              # Amex parser (line-based)
│   │   └── amzn_synchrony.py   # Amazon Synchrony parser
│   ├── routers/                 # API route definitions
│   │   ├── __init__.py
│   │   ├── accounts.py
│   │   ├── budgets.py
│   │   ├── categories.py
│   │   ├── debts.py
│   │   ├── financial_plans.py
│   │   ├── investments.py
│   │   ├── tags.py
│   │   ├── transactions.py
│   │   ├── uploads.py           # File upload endpoints
│   │   └── users.py
│   └── services/                # Business logic services
│       ├── importer.py          # Statement import service
│       └── s3.py                # S3 storage service
├── alembic/                     # Database migrations
│   ├── versions/
│   ├── env.py
│   └── script.py.mako
├── input/                       # Input file processing
│   ├── statements/              # PDF/CSV statements
│   │   ├── amex/
│   │   ├── amzn-synchrony/
│   │   └── tdbank/
│   └── transaction_csv/         # CSV transaction files
├── scripts/                     # Utility scripts
│   ├── bulk_upload.py
│   ├── seed.py
│   ├── seed_via_api.py
│   └── test_*.py               # Test upload scripts
└── requirements.txt
```

## Database Schema

### Core Entities

1. **Users** (`users`)
   - Manages user authentication and profile information
   - Tracks login activity and audit trail
   - Central entity linking to all user-owned resources

2. **Accounts** (`accounts`)
   - Represents financial accounts (checking, savings, credit cards, loans, investments)
   - Stores institution information and account metadata
   - Tracks current balance and last update time
   - Includes loan-specific fields (interest rate, minimum payment, original principal)

3. **Transactions** (`transactions`)
   - Core financial transaction records
   - Includes deduplication via transaction hashing
   - Supports categorization (primary and sub-categories)
   - Tracks source type (CSV, PDF, Manual, API)
   - Includes review flags for transactions needing attention

4. **Categories** (`categories`)
   - Hierarchical category system (parent/child relationships)
   - Used for transaction classification and budget allocation

5. **Investment Holdings** (`investment_holdings`)
   - Tracks current investment positions
   - Maintains cost basis and current price information

6. **Debt Management**
   - `debt_payments`: Individual loan payment records
   - `debt_repayment_plans`: Strategic debt payoff plans (Avalanche, Snowball, Custom)
   - `debt_repayment_schedules`: Monthly payment schedules

### Key Features

#### Transaction Processing
- **Deduplication**: SHA-256 hash-based duplicate detection using user ID, institution, date, type, amount, and description
- **Bulk Import**: Supports batch transaction imports from PDF/CSV with automatic deduplication
- **Relationship Tracking**: Links related transactions (refunds, offsets, splits, reversals)
- **Tagging System**: Custom user-defined tags for flexible organization

#### Account Management
- Multiple account types with type-specific fields
- Automatic balance updates based on transaction activity
- Support for investment and loan accounts with specialized tracking

#### Budget & Planning
- Custom budget creation with date ranges
- Category-based budget allocation
- Financial planning tools with income/expense projections

## Statement Parsing

The application includes sophisticated PDF and CSV parsing capabilities for various bank statements, with two distinct parsing approaches:

### Parsing Approaches

1. **Table-Based Parsing (TD Bank)**
   - Uses pdfplumber to identify and extract table structures
   - Dynamically detects column boundaries and headers
   - Processes transactions as structured table data
   - Handles complex multi-page tables with proper cell mapping

2. **Line-Based Parsing (Amex, Amazon Synchrony)**
   - Extracts text line by line from PDFs
   - Uses regex patterns to identify transaction data
   - More flexible for statements without clear table structures

### TD Bank Parser Workflow
1. Extract text and identify table boundaries using visual lines
2. Detect section headers ("Deposits", "Payments") to categorize transactions
3. Build table cells from horizontal and vertical line positions
4. Extract structured data from table cells
5. Complete partial dates using statement period context
6. Return `ParsedData` objects with deduplication hashes

### Parser Output
All parsers return standardized `ParsedTransaction` objects containing:
- Transaction date
- Amount
- Description
- Transaction type (mapped to system enums)
- Duplicate detection flags

## API Endpoints

The API provides comprehensive endpoints for all financial management features:

### Core Resources
- **Users** (`/users/`): User management, authentication, password changes
- **Accounts** (`/accounts/`): Financial account CRUD, balance tracking, statistics
- **Transactions** (`/transactions/`): Transaction CRUD, bulk operations, relationships
- **Categories** (`/categories/`): Hierarchical category management
- **Tags** (`/tags/`): Custom tagging system, bulk tagging operations
- **Budgets** (`/budgets/`): Budget creation, category allocation, performance tracking

### Specialized Features
- **Investments** (`/investments/`): Holdings, investment transactions, portfolio tracking
- **Debt Management** (`/debt/`): Repayment plans, payment tracking, schedules
- **Financial Plans** (`/financial_plans/`): Long-term financial goal planning
- **Uploads** (`/uploads/statement`): Async processing of bank statements (PDF/CSV)

### Key API Operations

#### Transaction Management
- Single and bulk creation with deduplication
- Bulk updates for categorization and review status  
- Transaction relationship linking (refunds, splits, offsets)
- UUID-based identification for external integrations

#### Account Operations
- Multi-type account support (checking, savings, credit, loan, investment)
- Automatic balance updates from transactions
- Account statistics and net worth calculations
- Loan-specific fields (interest rates, minimum payments)

#### Budget Features
- Date-range based budgets with category allocations
- Performance tracking against actual spending
- Budget copying for recurring monthly budgets
- Real-time statistics and variance analysis

#### File Upload Processing
- Supports multiple institutions (TD Bank, Amex, Amazon Synchrony)
- Automatic parser selection based on institution
- Async processing with transaction deduplication
- Optional account linking during import

## Development Guidelines

1. **Simplicity First**: Start with simple implementations, avoid over-engineering
2. **Performance Priority**: Optimize database queries and use appropriate indexes
3. **Security Focus**: Validate user ownership on all operations
4. **Clear Communication**: Ask for clarification rather than making assumptions

## Next Steps / To-Do

### Logging Implementation

Replace all print statements with proper logging infrastructure:

1. **Configure Logging System**
   - Set up Python's logging module with appropriate handlers
   - Create separate loggers for different modules
   - Implement log rotation and retention policies

2. **Environment-Based Configuration**
   ```python
   # Environment variables for log levels
   APP_LOG_LEVEL = os.getenv("APP_LOG_LEVEL", "INFO")  # Application logs
   THIRD_PARTY_LOG_LEVEL = os.getenv("THIRD_PARTY_LOG_LEVEL", "WARNING")  # SQLAlchemy, pdfplumber, etc.
   ```

3. **Logger Setup**
   ```python
   import logging
   
   # Application logger
   app_logger = logging.getLogger("loanchy")
   app_logger.setLevel(APP_LOG_LEVEL)
   
   # Third-party loggers
   logging.getLogger("sqlalchemy").setLevel(THIRD_PARTY_LOG_LEVEL)
   logging.getLogger("pdfplumber").setLevel(THIRD_PARTY_LOG_LEVEL)
   ```

4. **Replace Print Statements**
   - Parser debug output → `logger.debug()`
   - Error messages → `logger.error()` with exception info
   - Info messages → `logger.info()`
   - Transaction processing details → `logger.debug()`

5. **Structured Logging**
   - Add correlation IDs for request tracking
   - Include user context in log messages
   - Log performance metrics for slow queries

6. **Audit Logging**
   - Consider database table for audit logs (financial compliance)
   - Track critical operations (account modifications, bulk imports)
   - Maintain user action history

### Additional Improvements

- Implement comprehensive error handling middleware
- Add request/response validation logging
- Create health check endpoints with logging
- Set up monitoring and alerting based on log patterns
- Document logging standards and best practices