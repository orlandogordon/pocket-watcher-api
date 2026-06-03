FROM python:3.13-slim

WORKDIR /app

# Build/runtime deps:
#  - gcc + libc6-dev: compile psycopg2 (source build, not psycopg2-binary).
#    libc6-dev is required explicitly — with --no-install-recommends, gcc does
#    NOT pull in the standard C headers (assert.h etc.) on its own.
#  - libpq-dev: psycopg2 build (pg_config) + libpq at runtime.
#  - curl: the container HEALTHCHECK below.
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc libc6-dev libpq-dev curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://localhost:8000/health || exit 1

# Apply migrations (idempotent; includes the category seed) then serve.
CMD ["sh", "-c", "alembic upgrade head && uvicorn src.main:app --host 0.0.0.0 --port 8000"]
