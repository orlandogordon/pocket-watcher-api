"""Tests for the admin-bootstrap job (#61)."""
import pytest

from src.crud.crud_user import read_db_user
from src.db.core import TagDB
from src.jobs.bootstrap_admin import bootstrap_admin
from tests.factories import make_user

pytestmark = pytest.mark.integration

CREDS = dict(email="root@example.com", password="Password123", username="root")


def test_bootstrap_creates_admin(db):
    created = bootstrap_admin(db, **CREDS)

    assert created is True
    user = read_db_user(db, email="root@example.com")
    assert user is not None
    assert user.is_admin is True
    # create_db_user seeds system tags, so the new admin is immediately usable
    assert db.query(TagDB).filter(TagDB.user_id == user.db_id).count() > 0


def test_bootstrap_is_noop_when_email_exists(db):
    make_user(db, email="root@example.com", username="someoneelse", is_admin=False)

    created = bootstrap_admin(db, **CREDS)

    assert created is False
    # existing user's admin flag is left untouched (not clobbered to True)
    assert read_db_user(db, email="root@example.com").is_admin is False
