"""Shared pytest fixtures: in-memory DB, TestClient, auth/redis overrides.

Isolation model: one session-scoped in-memory SQLite engine (StaticPool so
every connection shares the same in-memory database), and a function-scoped
`db` session joined into an outer transaction that is rolled back after each
test. `join_transaction_mode="create_savepoint"` means the CRUD layer's
frequent `db.commit()` calls land on a savepoint, so the outer rollback still
fully cleans up between tests.

The `client` fixture wires FastAPI's three injectable dependencies to the test
session, a fixed user, and a fake Redis. `unauth_client` deliberately leaves
auth un-overridden so the real dependency raises 401.

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
from sqlalchemy import create_engine  # noqa: E402
from sqlalchemy.orm import Session  # noqa: E402
from sqlalchemy.pool import StaticPool  # noqa: E402

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
    session = Session(bind=connection, join_transaction_mode="create_savepoint")
    try:
        yield session
    finally:
        session.close()
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
def client(db, fake_redis, test_user):
    def _override_get_db():
        yield db

    app.dependency_overrides[get_db] = _override_get_db
    app.dependency_overrides[get_current_user_id] = lambda: test_user.db_id
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
