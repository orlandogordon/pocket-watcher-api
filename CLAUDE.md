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
- **Uploads** (`/uploads/statement/preview` + `/confirm`): Two-step preview/confirm flow for bank statement imports (PDF/CSV) with LLM description cleanup and category/merchant suggestions

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

## Auth Error Contract (#58)

Consistent, intentional status codes for authorization/identity errors — relied
on by the frontend, don't "fix" them into uniformity:

- **Cross-user access → 404** (don't-leak-existence) everywhere a resource is
  owned by a user: accounts, transactions, investments, budgets, debts,
  financial_plans, tags, account-history, uploads. Asking for someone else's
  resource is indistinguishable from "not found".
- **Users routes → 403** (not 404) for a non-self / non-admin caller, via
  `require_self_or_admin`. The lookup resolves the user first, so an *unknown*
  user UUID is **404** but an *existing other* user is **403**. `change-password`
  is **403** for anyone but self (admins included).
- **System-tag edits → 403**: modifying/deleting a tag with `is_system=True`
  returns 403 (the tag exists and is visible, but is read-only).
- **Malformed UUID → 422** on every path/query UUID (FastAPI-native validation).
  Sole exception: the multipart Form `account_uuid` on `POST /uploads/files` and
  `POST /uploads/statement/preview` is **400** (keeps an empty-string="no
  account" guard).

## Database & Migrations (C1)

- **Targets both SQLite and PostgreSQL.** SQLite (`sqlite:///test.db`) is the
  default for local dev; Postgres is production. Set `DATABASE_URL` to switch
  (`postgresql+psycopg2://user:pass@host:5432/db`). `psycopg2` is in
  `requirements.txt`; `docker-compose.yml` provides a `postgres:17` + `redis:7`
  for local PG parity (`docker compose up -d`).
- **Squashed migration history (#C1).** The prior 18-migration chain was
  collapsed into one baseline (`819291009bbf`) + a category-seed data migration
  (`b1c2d3e4f5a6`). The baseline was autogenerated against and verified on real
  Postgres **and** SQLite (clean `upgrade head`, empty `alembic check`, full
  `downgrade base`→`upgrade head` round-trip). Fresh installs start from these
  two; don't resurrect the old versions.
- **All future migrations target Postgres** — write them PG-first and test on a
  real instance (the compose service) before merging.
- **SQLite/PG portability gotchas to respect when adding schema:**
  - `CHECK` constraints must not do arithmetic on booleans (`(x IS NOT NULL) +
    ...` is SQLite-only). Use `CASE WHEN x IS NOT NULL THEN 1 ELSE 0 END + ...`
    — that's why the `ck_rel_*` / `ck_dismissed_*` constraints are written that
    way.
  - Postgres keeps named `ENUM` types after their table is dropped; downgrades
    that drop enum-bearing tables must `DROP TYPE IF EXISTS` them (the baseline
    downgrade does this, guarded to `postgresql`).
  - Use portable abstractions: `sa.Uuid` (not native UUID), generic `JSON`,
    SQLAlchemy `.ilike()`, explicit `String(n)` / `DECIMAL(p,s)`.

## Deployment & CI/CD (C5)

- **Production** runs the `docker-compose.prod.yml` stack (api + `postgres:17` +
  `redis:7` + loki/promtail/grafana) on a self-hosted home server, VPN-only. It's
  distinct from the dev `docker-compose.yml` (postgres+redis only).
- **Auto-deploy** via `.github/workflows/deploy.yml`: on push to `main`, a `test`
  job runs the suite on a GitHub-hosted runner; on pass, a `deploy` job runs on a
  **self-hosted runner** on the server — `git pull` + `docker compose -f
  docker-compose.prod.yml up -d --build` (rebuilds only `api`; data
  services/volumes untouched), then curls `GET /health` to verify. So merging to
  `main` auto-deploys.
- The `api` image (`Dockerfile`) runs `alembic upgrade head` (incl. the category
  seed) on start, then uvicorn. First admin is minted out-of-band via
  `python -m src.jobs.bootstrap_admin` (admin creation has no API path).
- Detailed server topology, secrets, and ops runbook live in **private notes**,
  not this repo.

## Testing

`pytest`-based suite that runs the app in-process via `TestClient` — **no
server, Postgres, or Redis needed** (in-memory SQLite + `fakeredis`). Run:

```
./venv/Scripts/python.exe -m pytest --cov=src --cov-report=term-missing -q
```

- Fixtures/factories: `tests/conftest.py`, `tests/factories.py`. Authed
  `client` vs `unauth_client`; `admin_client` for admin-gated routes;
  `fake_redis`, `fake_llm`.
- Markers (`pyproject.toml`): `parser`, `integration`, `slow`.
- Parser regression runs off a **gitignored** real-statement corpus at
  `tests/parsers/fixtures/local/<institution>/` (PII — never committed); those
  tests skip when it is absent. Committed synthetic `*.csv` fixtures cover the
  CSV paths. The PDF `parse_statement`/`parse_pdf` bodies are excluded from
  coverage (corpus-only). Coverage is ~76% fresh-clone / ~78% local; no enforced
  floor.
- **Never commit real PII** in fixtures or code (repo is public). Statement PDFs
  are never committed — only synthetic CSV fixtures; real statements live in the
  gitignored `local/` corpus.
- **`scripts/` is local-only** (gitignored, #59) — not in the repo. Scheduled
  jobs live in `src/jobs/` (`python -m src.jobs.eod_snapshot`,
  `python -m src.jobs.option_expiration_sweep`,
  `python -m src.jobs.sweep_preview_orphans`).

## Logging & Observability (C3)

Structured logging is implemented — build on it, don't reinvent it:

- **JSON everywhere.** `src/logging_config.py` configures `python-json-logger`
  on every handler (`timestamp`, `level`, `logger`, `message`, exc info). Get a
  logger with `from src.logging_config import get_logger; logger =
  get_logger(__name__)`. Levels via `APP_LOG_LEVEL` / `THIRD_PARTY_LOG_LEVEL`;
  optional rotating file via `LOG_FILE`.
- **Request correlation.** A `ContextFilter` stamps `request_id` and `user_id`
  on every record from contextvars (`src/request_context.py`,
  `src/auth/context.py`) — so route and CRUD logs carry them with no plumbing.
  The filter won't clobber a value already set via `extra`.
- **Access log.** `src/middleware/request_logging.py` (registered innermost in
  `main.py`) generates/propagates `request_id` (honors inbound `X-Request-ID`,
  echoes it on the response), logs `request.start` and `request.complete`
  (status_code + duration_ms; 4xx WARNING / 5xx ERROR). Request bodies are not
  read/logged (avoids consuming upload streams + leaking secrets).
- **Slow queries.** Engine event listeners in `src/db/core.py` log statements
  over `SLOW_QUERY_MS` (default 500) at WARNING with duration_ms; params
  omitted. Full SQL stays gated behind `SQL_ECHO`.
- **Error handlers** (`main.py`): `HTTPException` (4xx WARNING / 5xx ERROR),
  `RequestValidationError` (WARNING + offending input), and a catch-all
  `Exception` (ERROR + full traceback, generic 500 JSON). The catch-all runs
  above the logging middleware, so `request_id` is bridged via `request.state`.
- **CRUD mutations** log at INFO with the resource id (create/update/delete);
  reads stay quiet. Failures with existing try/except log at ERROR.
- **Health:** `GET /health` (public) → `{status, db, redis}`, 200 / 503;
  `GET /health/llm` (authed) is the LLM reachability probe.

### Remaining / deferred

- **Audit logging** — a DB table for critical operations (financial
  compliance) is still future work, distinct from app logs.
- **Log aggregation + monitoring/alerting** (Loki+Grafana on the home server)
  is **close-out C5**, not C3.