"""Over-HTTP tests for the /users router.

Auth shape here is the full-row dependency: routes inject `get_current_user`
(a UserDB) and authorize via `require_self_or_admin` / `get_current_admin_user_id`.
`client` is authed as the non-admin `test_user`; `admin_client` as an admin.
User paths take the INTEGER user_id; UserResponse exposes the UUID under "id".
Passwords are bcrypt-hashed, so login/change-password tests seed a real hash.
"""
import pytest

from src.crud.crud_user import hash_password
from tests.factories import make_user

pytestmark = pytest.mark.integration


def _user_create_payload(**overrides):
    payload = {
        "email": "newuser@example.com",
        "username": "newuser",
        "password": "Password123",
        "confirm_password": "Password123",
        "first_name": "New",
    }
    payload.update(overrides)
    return payload


# ===== /me =====

def test_me_returns_current_user(client, test_user):
    resp = client.get("/users/me")
    assert resp.status_code == 200
    assert resp.json()["email"] == "tester@example.com"
    assert resp.json()["id"]  # UUID


def test_me_unauthenticated_401(unauth_client):
    assert unauth_client.get("/users/me").status_code == 401


# ===== ADMIN-GATED: create / list =====

def test_create_user_as_admin_201(admin_client):
    resp = admin_client.post("/users/", json=_user_create_payload())
    assert resp.status_code == 201
    assert resp.json()["email"] == "newuser@example.com"


def test_create_user_as_non_admin_403(client):
    assert client.post("/users/", json=_user_create_payload()).status_code == 403


def test_create_user_password_mismatch_422(admin_client):
    resp = admin_client.post("/users/", json=_user_create_payload(confirm_password="Different123"))
    assert resp.status_code == 422


def test_list_users_as_admin(admin_client, test_user):
    emails = {u["email"] for u in admin_client.get("/users/").json()}
    assert "tester@example.com" in emails


def test_list_users_as_non_admin_403(client):
    assert client.get("/users/").status_code == 403


# ===== SELF-OR-ADMIN READ / UPDATE / DELETE =====

def test_read_self_200(client, test_user):
    resp = client.get(f"/users/{test_user.db_id}")
    assert resp.status_code == 200
    assert resp.json()["email"] == "tester@example.com"


def test_read_other_user_as_non_admin_403(client, test_user):
    assert client.get(f"/users/{test_user.db_id + 999}").status_code == 403


def test_read_unknown_user_as_admin_404(admin_client):
    assert admin_client.get("/users/999999").status_code == 404


def test_read_user_by_uuid_self(client, test_user):
    resp = client.get(f"/users/uuid/{test_user.id}")
    assert resp.status_code == 200
    assert resp.json()["id"] == str(test_user.id)


def test_update_self_200(client, test_user):
    resp = client.put(f"/users/{test_user.db_id}", json={"first_name": "Renamed"})
    assert resp.status_code == 200
    assert resp.json()["first_name"] == "Renamed"


def test_update_other_user_403(client, test_user):
    assert client.put(f"/users/{test_user.db_id + 999}", json={"first_name": "X"}).status_code == 403


def test_delete_user_as_admin_200(admin_client, db):
    victim = make_user(db, email="victim@example.com", username="victim")
    resp = admin_client.delete(f"/users/{victim.db_id}")
    assert resp.status_code == 200


# ===== LOGIN =====

def test_login_success(client, db):
    make_user(db, email="login@example.com", username="loginuser",
              password_hash=hash_password("Secret123"))
    resp = client.post("/users/login", json={"email": "login@example.com", "password": "Secret123"})
    assert resp.status_code == 200
    assert resp.json()["token_type"] == "bearer"


def test_login_wrong_password_401(client, db):
    make_user(db, email="login2@example.com", username="loginuser2",
              password_hash=hash_password("Secret123"))
    resp = client.post("/users/login", json={"email": "login2@example.com", "password": "WrongPass"})
    assert resp.status_code == 401


# ===== CHANGE PASSWORD (self only) =====

def test_change_password_success(client, db, test_user):
    test_user.password_hash = hash_password("OldPass123")
    db.flush()
    resp = client.post(f"/users/{test_user.db_id}/change-password", json={
        "current_password": "OldPass123", "new_password": "NewPass123", "confirm_new_password": "NewPass123",
    })
    assert resp.status_code == 200


def test_change_password_wrong_current_400(client, db, test_user):
    test_user.password_hash = hash_password("OldPass123")
    db.flush()
    resp = client.post(f"/users/{test_user.db_id}/change-password", json={
        "current_password": "NotMyPassword1", "new_password": "NewPass123", "confirm_new_password": "NewPass123",
    })
    assert resp.status_code == 400


def test_change_password_other_user_403(client, test_user):
    resp = client.post(f"/users/{test_user.db_id + 999}/change-password", json={
        "current_password": "OldPass123", "new_password": "NewPass123", "confirm_new_password": "NewPass123",
    })
    assert resp.status_code == 403
