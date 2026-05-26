"""Over-HTTP tests for the /categories router.

Categories are locked to the code-defined tree (#29): create/update/delete all
return 405. Only the read endpoints are live. Categories are global (not
user-scoped), so the list reflects whatever the tree contains — here, just what
the test seeds, since startup seeding is skipped in the harness.
"""
from uuid import uuid4

import pytest

from tests.factories import make_category

pytestmark = pytest.mark.integration


def test_create_category_405(client):
    assert client.post("/categories/", json={"name": "Nope"}).status_code == 405


def test_update_category_405(client):
    assert client.put(f"/categories/{uuid4()}", json={"name": "Nope"}).status_code == 405


def test_delete_category_405(client):
    assert client.delete(f"/categories/{uuid4()}").status_code == 405


def test_list_categories_200(client, db):
    make_category(db, name="Food")
    make_category(db, name="Travel")
    body = client.get("/categories/").json()
    names = {c["name"] for c in body}
    assert {"Food", "Travel"} <= names
    # CategoryResponse exposes its UUID under "id".
    assert all("id" in c for c in body)


def test_list_pagination(client, db):
    for i in range(3):
        make_category(db, name=f"Cat-{i}")
    assert len(client.get("/categories/", params={"limit": 2}).json()) == 2


def test_get_category_200(client, db):
    cat = make_category(db, name="Findable")
    resp = client.get(f"/categories/{cat.uuid}")
    assert resp.status_code == 200
    assert resp.json()["name"] == "Findable"
    assert resp.json()["id"] == str(cat.uuid)


def test_get_unknown_404(client):
    assert client.get(f"/categories/{uuid4()}").status_code == 404


def test_get_malformed_uuid_400(client):
    assert client.get("/categories/not-a-uuid").status_code == 400


def test_list_unauthenticated_401(unauth_client):
    assert unauth_client.get("/categories/").status_code == 401
