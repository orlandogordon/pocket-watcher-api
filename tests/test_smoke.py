"""Phase 1 harness smoke tests — one per layer, proving conftest works.

Deep coverage lands in later phases; these only verify that the DB session,
TestClient + dependency overrides, auth, and the parser layer all wire up.
"""
import pytest

from src.crud import crud_account
from tests.factories import make_account


def test_crud_db_roundtrip(db, test_user):
    """db fixture + a real CRUD query round-trip."""
    make_account(db, test_user, account_name="Smoke Checking")
    accounts = crud_account.read_db_accounts(db, user_id=test_user.db_id)
    assert len(accounts) == 1
    assert accounts[0].account_name == "Smoke Checking"


@pytest.mark.integration
def test_router_authenticated_returns_200(client, db, test_user):
    """Authed TestClient sees rows created in the shared test session."""
    make_account(db, test_user, account_name="Smoke Checking")
    resp = client.get("/accounts/")
    assert resp.status_code == 200
    assert "Smoke Checking" in [a["account_name"] for a in resp.json()]


@pytest.mark.integration
def test_router_unauthenticated_returns_401(unauth_client):
    """No Bearer token → real get_current_user_id dependency raises 401."""
    resp = unauth_client.get("/accounts/")
    assert resp.status_code == 401


def test_parser_layer_imports():
    """Parser registry imports cleanly (fixture-backed parser tests: Phase 3)."""
    from src.services.importer import PARSER_MAPPING

    assert PARSER_MAPPING
