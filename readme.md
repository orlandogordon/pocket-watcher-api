# Pocket Watcher API

A personal-finance management backend built with **FastAPI**. It tracks
transactions across multiple accounts, imports and parses bank/brokerage
statements, and supports budgets, investment holdings, debt-repayment plans, and
financial planning.

> This README is the quick-start and orientation guide. The authoritative
> technical detail (schema, parser internals, auth contract, observability,
> migration policy) lives in [`CLAUDE.md`](CLAUDE.md).

## Features

- **Accounts** — checking, savings, credit, loan, and investment accounts with
  balance tracking and net-worth statistics.
- **Transactions** — CRUD plus bulk import, SHA-256 deduplication, hierarchical
  categories, custom tags, and relationship linking (refunds, splits, offsets).
- **Statement import** — PDF/CSV parsing for several institutions (TD Bank, Amex,
  Amazon Synchrony, Schwab, TD Ameritrade, Ameriprise) via a two-step
  preview/confirm flow with optional LLM description cleanup and
  category/merchant suggestions.
- **Budgets** — reusable budget templates with month instances and a
  subcategory envelope model.
- **Investments** — holdings, investment transactions, and portfolio valuation.
- **Debt** — repayment plans (Avalanche / Snowball / Custom), payment tracking,
  and schedules.
- **Financial plans** — long-term goal projections.

## Tech Stack

- **Framework**: FastAPI (Python 3.13)
- **ORM / migrations**: SQLAlchemy 2.0 + Alembic
- **Database**: SQLite (local dev) / PostgreSQL (production, via `DATABASE_URL`)
- **Validation**: Pydantic
- **Cache / sessions**: Redis (upload preview sessions)
- **PDF parsing**: pdfplumber
- **Server**: uvicorn

## Local Development Setup

Local dev defaults to **SQLite** (`sqlite:///test.db`), so the app runs with no
external services. **PostgreSQL** is the production target and can be run locally
via Docker for parity. The Alembic history is a single squashed baseline + a
category-seed migration; fresh databases start from these.

Install dependencies into a virtualenv first:

```powershell
python -m venv venv
./venv/Scripts/python.exe -m pip install -r requirements.txt
```

### Quick start (SQLite)

```powershell
python -m alembic upgrade head          # create schema + seed categories
$env:ADMIN_EMAIL="you@example.com"; $env:ADMIN_PASSWORD="<a-strong-password>"
python -m src.jobs.bootstrap_admin      # mint the first admin (idempotent)
uvicorn src.main:app --reload
```

The interactive API docs are then served at `http://127.0.0.1:8000/docs`.

### Running against Postgres (Docker)

`docker-compose.yml` provides `postgres:17` + `redis:7` with persistent volumes
and `restart: unless-stopped` (they survive reboots; data lives in named volumes,
not the containers).

```powershell
# 1. Start Postgres + Redis
docker compose up -d

# 2. Point the app at the container (or uncomment DATABASE_URL in .env)
$env:DATABASE_URL = "postgresql+psycopg2://pocketwatcher:pocketwatcher@localhost:5432/pocketwatcher"

# 3. Create schema + seed categories
python -m alembic upgrade head

# 4. Mint the first admin (idempotent — safe to re-run)
$env:ADMIN_EMAIL="you@example.com"; $env:ADMIN_PASSWORD="<a-strong-password>"; python -m src.jobs.bootstrap_admin

# 5. Run the app
uvicorn src.main:app --reload
```

Notes:
- **GUI access:** connect any Postgres client (pgAdmin, DBeaver, psql) to
  `localhost:5432`, db/user/pass `pocketwatcher`. The container must be running.
- **Stop vs wipe:** `docker compose down` stops the stack but keeps the data;
  `docker compose down -v` deletes the volumes (empties the DB).

## Creating Users

There is **no public registration**. User creation is admin-gated end to end:

1. **First admin** — minted out-of-band by `python -m src.jobs.bootstrap_admin`,
   driven by `ADMIN_EMAIL` / `ADMIN_PASSWORD` (optional `ADMIN_USERNAME`). This is
   the *only* way to grant admin, and `is_admin` has no API path. Re-running with
   an existing email is a no-op.
2. **Authenticate** — `POST /auth/login` with email + password returns a JWT;
   send it as `Authorization: Bearer <token>` on subsequent requests.
3. **Additional users** — an authenticated admin creates them via
   `POST /users/` (also admin-only).

## Loading Data

- **Statement import (primary path)** — upload a PDF/CSV via the two-step flow:
  `POST /uploads/statement/preview` returns parsed, deduplicated, optionally
  LLM-cleaned transactions held in a Redis session; `POST /uploads/.../confirm`
  commits the ones you keep. Bulk/multi-file import and persistent per-account
  document storage are also available under `/uploads/`.
- **Manual entry** — create accounts, transactions, budgets, etc. directly
  through their REST endpoints (browse them all at `/docs`).

## Scheduled Jobs

These run **alongside** the API server (cron/systemd in production) — they are
not started by the web process. Each is a standalone module under `src/jobs/`:

| Job | Command | Cadence | Purpose |
|-----|---------|---------|---------|
| End-of-day snapshot | `python -m src.jobs.eod_snapshot [--date YYYY-MM-DD] [--user-id ID] [--skip-weekends]` | Daily after market close | Fetch latest prices, write daily account snapshots, update net-worth history. |
| Option-expiration sweep | `python -m src.jobs.option_expiration_sweep --dry-run` / `--apply` | Daily / as needed | Close out option contracts past their OCC expiration that still show open positions (synthesizes `$0` EXPIRATION rows for OTM; flags ITM for manual review). |
| Preview-orphan sweep | `python -m src.jobs.sweep_preview_orphans --dry-run` / `--apply` | Daily | Reclaim uploaded preview files left on disk by previews that were never confirmed or cancelled (only deletes unreferenced files older than the 12h session TTL + margin). |

`src.jobs.bootstrap_admin` (above) also lives here but is a one-off provisioning
step, not a scheduled job. Always `--dry-run` the sweeps before `--apply`, and
back up the production DB (see deployment) before applying in production.

## Testing

The test suite is `pytest`-based and runs the app in-process via `TestClient` —
**no running server, database, or Redis is required** (SQLite is in-memory and
Redis is faked).

Run everything with coverage:

```
./venv/Scripts/python.exe -m pytest --cov=src --cov-report=term-missing -q
```

(`python -m pytest ...` on any platform; the path above is the Windows venv.)

- **Coverage** lands around **76% on a fresh clone** and **~78% locally**. There
  is no enforced `--cov-fail-under` floor — coverage is a guardrail, not the
  target. The PDF `parse_statement`/`parse_pdf` parser bodies are excluded from
  measurement (see `pyproject.toml`) because they can only be exercised by real
  statement PDFs, which are never committed.
- **Markers** (`pyproject.toml`): `parser`, `integration`, `slow`. Deselect with
  e.g. `-m "not parser"`.
- **Parser regression corpus**: real statements live, gitignored, under
  `tests/parsers/fixtures/local/<institution>/` and contain PII, so they are
  never committed. Those tests **skip when the corpus is absent**. The committed
  synthetic CSV fixtures (`tests/parsers/fixtures/*.csv`) cover the CSV paths.
- **No PII in committed fixtures** (repo is public). Statement PDFs are never
  committed — only synthetic CSVs. Real statements stay in the gitignored
  `local/` corpus.

## Deployment & CI/CD

Pushing to `main` auto-deploys via `.github/workflows/deploy.yml`: a `test` job
runs the suite on a GitHub-hosted runner, and on pass a `deploy` job runs on a
self-hosted runner — `git pull` + `docker compose -f docker-compose.prod.yml up
-d --build` (rebuilds the `api` image only; data services and volumes are left
untouched), then verifies `GET /health`. The `api` image runs `alembic upgrade
head` on start, then uvicorn.

The production stack (`docker-compose.prod.yml`) adds Loki/Promtail/Grafana log
aggregation on top of Postgres + Redis. The detailed server topology, secrets,
and ops runbook are kept in private notes, not this repo. See the
"Deployment & CI/CD" section of [`CLAUDE.md`](CLAUDE.md) for more.

## Architecture

The codebase follows a layered FastAPI structure:

```
src/
├── main.py        # app entry point, middleware, error handlers
├── routers/       # API route definitions per resource
├── crud/          # database operations
├── models/        # Pydantic request/response models
├── db/core.py     # SQLAlchemy models + engine config
├── parser/        # per-institution statement parsers
├── services/      # business logic (importer, storage, preview sessions, …)
└── jobs/          # standalone scheduled / provisioning jobs
```

Conventions worth knowing up front: internal integer PKs are `db_id` and external
identifiers are `uuid` (exposed as `id` in responses); monetary values are always
`Decimal`; logging is structured JSON with per-request `request_id` correlation.
See [`CLAUDE.md`](CLAUDE.md) for the schema, parser workflows, auth error
contract, migration policy, and observability details.

## Implementation Standards

- **Simplicity first** — start with the simplest implementation; don't
  over-engineer.
- **Performance & security first** — validate user ownership on every operation;
  use appropriate indexes and efficient queries.
- **Ask, don't guess** — clarify ambiguous requirements rather than assuming.
