"""Shared pytest fixtures: in-memory DB, TestClient, auth/redis overrides.

Isolation model: one session-scoped in-memory SQLite engine (StaticPool so
every connection shares the same in-memory database), and a function-scoped
`db` session joined into an outer transaction that is rolled back after each
test. The CRUD layer commits frequently, so we hold the per-test boundary with
the canonical SQLAlchemy "join a Session into an external transaction" recipe:
an outer transaction plus a SAVEPOINT, with an `after_transaction_end` listener
that restarts the SAVEPOINT after every `commit()`. The outer transaction is
never touched, so the teardown rollback fully cleans up. (Plain
`join_transaction_mode="create_savepoint"` was not enough against commit-heavy
CRUD — committed rows leaked across tests.)

The `client` fixture wires FastAPI's three injectable dependencies to the test
session, a fixed user, and a fake Redis. `unauth_client` deliberately leaves
auth un-overridden so the real dependency raises 401.

Auth has two shapes in this codebase: routers either inject
`Depends(get_current_user_id)` (e.g. accounts) or call the bare
`current_user_id()` contextvar helper inside the handler (e.g. transactions,
investments, budgets…). Overriding the dependency only covers the first. To
cover both, the authed `client`'s `get_db` override ALSO populates the
contextvar — and it MUST be an `async` generator. Sync `def` dependencies and
sync endpoints each run in their own threadpool context copy, so a contextvar
set in a sync dependency does not reach the handler; an async dependency runs
in the event-loop task whose context is copied into the endpoint's threadpool
dispatch, so the value propagates. (The real app relies on the same mechanism:
the async middleware sets the contextvar before the sync endpoint is
dispatched.) `unauth_client` does NOT set it, so both auth shapes still 401.

`TestClient` is constructed without the `with` context manager on purpose: the
app's `startup` hook (`recover_interrupted_jobs` + `ensure_system_tags`) runs
against the real database via the unpatched `get_db`, which we don't want in
tests. Skipping lifespan avoids it; middleware still runs per request.
"""
import os

# Must be set before importing src.main — src.auth.config reads JWT_SECRET at
# import time and refuses to load without a >=32 char value.
os.environ.setdefault("JWT_SECRET", "test-jwt-secret-value-0123456789abcdef")

import pytest  # noqa: E402
from fakeredis import FakeRedis  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from sqlalchemy import create_engine, event  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

from src.auth.context import set_current_user_id  # noqa: E402
from src.auth.dependencies import get_current_user_id  # noqa: E402
from src.db.core import Base, get_db  # noqa: E402
from src.main import app  # noqa: E402
from src.services.redis_client import get_redis_dependency  # noqa: E402
from tests.factories import make_user  # noqa: E402


@pytest.fixture(scope="session")
def engine():
    eng = create_engine(
        "sqlite://",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(eng)
    yield eng
    eng.dispose()


@pytest.fixture
def db(engine):
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    nested = connection.begin_nested()

    @event.listens_for(session, "after_transaction_end")
    def _restart_savepoint(sess, trans):
        nonlocal nested
        if not nested.is_active:
            nested = connection.begin_nested()

    try:
        yield session
    finally:
        session.close()
        if transaction.is_active:
            transaction.rollback()
        connection.close()


@pytest.fixture
def fake_redis():
    client = FakeRedis(decode_responses=True)
    yield client
    client.flushall()


@pytest.fixture
def test_user(db):
    return make_user(db, email="tester@example.com", username="tester")


@pytest.fixture
def admin_user(db):
    return make_user(db, email="admin@example.com", username="admin", is_admin=True)


@pytest.fixture
def client(db, fake_redis, test_user):
    # Async so the contextvar set here propagates into the sync endpoint's
    # threadpool dispatch (see module docstring). Covers both auth shapes.
    async def _override_get_db():
        set_current_user_id(test_user.db_id)
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_current_user_id] = lambda: test_user.db_id
    app.dependency_overrides[get_redis_dependency] = lambda: fake_redis
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def admin_client(db, fake_redis, admin_user):
    """Like `client` but authenticated as an admin user — for admin-gated
    routes (e.g. user create/list) behind `get_current_admin_user_id`."""
    async def _override_get_db():
        set_current_user_id(admin_user.db_id)
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_current_user_id] = lambda: admin_user.db_id
    app.dependency_overrides[get_redis_dependency] = lambda: fake_redis
    yield TestClient(app)
    app.dependency_overrides.clear()


@pytest.fixture
def unauth_client(db, fake_redis):
    """TestClient with no auth override — routes hit the real dependency and
    return 401 since no Bearer token is sent."""
    def _override_get_db():
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_redis_dependency] = lambda: fake_redis
    yield TestClient(app)
    app.dependency_overrides.clear()
