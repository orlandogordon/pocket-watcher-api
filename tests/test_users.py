"""Over-HTTP tests for the /users router.

Auth shape here is the full-row dependency: routes inject `get_current_user`
(a UserDB) and authorize via `require_self_or_admin` / `get_current_admin_user_id`.
`client` is authed as the non-admin `test_user`; `admin_client` as an admin.
User paths take the user UUID; UserResponse exposes the UUID under "id". A
lookup resolves the user first, so an unknown UUID is 404 and an existing other
user (to a non-admin) is 403.
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


def test_me_is_admin_false_for_normal_user(client):
    assert client.get("/users/me").json()["is_admin"] is False


def test_me_is_admin_true_for_admin(admin_client):
    assert admin_client.get("/users/me").json()["is_admin"] is True


def test_me_unauthenticated_401(unauth_client):
    assert unauth_client.get("/users/me").status_code == 401


# ===== ADMIN-GATED: create / list =====

def test_create_user_as_admin_201(admin_client):
    resp = admin_client.post("/users/", json=_user_create_payload())
    assert resp.status_code == 201
    assert resp.json()["email"] == "newuser@example.com"
    assert resp.json()["is_admin"] is False  # provisioned users are never admin


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
    resp = client.get(f"/users/{test_user.uuid}")
    assert resp.status_code == 200
    assert resp.json()["email"] == "tester@example.com"
    assert resp.json()["id"] == str(test_user.uuid)


def test_read_other_user_as_non_admin_403(client, db, test_user):
    other = make_user(db, email="other@example.com", username="other")
    assert client.get(f"/users/{other.uuid}").status_code == 403


def test_read_unknown_user_as_admin_404(admin_client):
    from uuid import uuid4
    assert admin_client.get(f"/users/{uuid4()}").status_code == 404


def test_update_self_200(client, test_user):
    resp = client.put(f"/users/{test_user.uuid}", json={"first_name": "Renamed"})
    assert resp.status_code == 200
    assert resp.json()["first_name"] == "Renamed"


def test_update_other_user_403(client, db, test_user):
    other = make_user(db, email="other2@example.com", username="other2")
    assert client.put(f"/users/{other.uuid}", json={"first_name": "X"}).status_code == 403


def test_delete_user_as_admin_204(admin_client, db):
    victim = make_user(db, email="victim@example.com", username="victim")
    resp = admin_client.delete(f"/users/{victim.uuid}")
    assert resp.status_code == 204


# ===== CHANGE PASSWORD (self only) =====

def test_change_password_success(client, db, test_user):
    test_user.password_hash = hash_password("OldPass123")
    db.flush()
    resp = client.post(f"/users/{test_user.uuid}/change-password", json={
        "current_password": "OldPass123", "new_password": "NewPass123", "confirm_new_password": "NewPass123",
    })
    assert resp.status_code == 200


def test_change_password_wrong_current_400(client, db, test_user):
    test_user.password_hash = hash_password("OldPass123")
    db.flush()
    resp = client.post(f"/users/{test_user.uuid}/change-password", json={
        "current_password": "NotMyPassword1", "new_password": "NewPass123", "confirm_new_password": "NewPass123",
    })
    assert resp.status_code == 400


def test_change_password_other_user_403(client, db, test_user):
    other = make_user(db, email="other3@example.com", username="other3")
    resp = client.post(f"/users/{other.uuid}/change-password", json={
        "current_password": "OldPass123", "new_password": "NewPass123", "confirm_new_password": "NewPass123",
    })
    assert resp.status_code == 403
