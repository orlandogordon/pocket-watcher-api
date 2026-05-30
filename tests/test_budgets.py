"""Over-HTTP tests for the /budgets router.

Pins the template/month model: reusable templates, get-or-create budget months
(reading a month auto-creates it, assigning the default template if one
exists), and the subcategory envelope rule — subcategory allocations may not
exceed their parent category's allocation (the "ceiling"). TemplateResponse and
TemplateCategoryResponse both expose their UUID under "id".
"""
from decimal import Decimal
from uuid import uuid4

import pytest

from tests.factories import make_category

pytestmark = pytest.mark.integration


def _parent_and_sub(db):
    parent = make_category(db, name="Food")
    sub = make_category(db, name="Groceries", parent_category_id=parent.db_id)
    return parent, sub


# ===== TEMPLATES =====

def test_create_template_201(client):
    resp = client.post("/budgets/templates/", json={"template_name": "Monthly", "is_default": True})
    assert resp.status_code == 201
    body = resp.json()
    assert body["template_name"] == "Monthly"
    assert body["is_default"] is True


def test_create_template_with_categories(client, db):
    parent, sub = _parent_and_sub(db)
    resp = client.post("/budgets/templates/", json={
        "template_name": "WithCats",
        "categories": [
            {"category_uuid": str(parent.uuid), "allocated_amount": "500.00"},
            {"category_uuid": str(parent.uuid), "subcategory_uuid": str(sub.uuid), "allocated_amount": "300.00"},
        ],
    })
    assert resp.status_code == 201
    assert len(resp.json()["categories"]) == 2


def test_create_template_envelope_violation_400(client, db):
    parent, sub = _parent_and_sub(db)
    resp = client.post("/budgets/templates/", json={
        "template_name": "OverEnvelope",
        "categories": [
            {"category_uuid": str(parent.uuid), "allocated_amount": "100.00"},
            {"category_uuid": str(parent.uuid), "subcategory_uuid": str(sub.uuid), "allocated_amount": "150.00"},
        ],
    })
    assert resp.status_code == 400
    assert "envelope" in resp.json()["detail"].lower()


def test_create_template_duplicate_name_400(client):
    client.post("/budgets/templates/", json={"template_name": "Dup"})
    assert client.post("/budgets/templates/", json={"template_name": "Dup"}).status_code == 400


def test_create_template_unknown_category_404(client):
    resp = client.post("/budgets/templates/", json={
        "template_name": "BadCat",
        "categories": [{"category_uuid": str(uuid4()), "allocated_amount": "10.00"}],
    })
    assert resp.status_code == 404


def test_list_templates(client):
    client.post("/budgets/templates/", json={"template_name": "T1"})
    client.post("/budgets/templates/", json={"template_name": "T2"})
    names = {t["template_name"] for t in client.get("/budgets/templates/").json()}
    assert {"T1", "T2"} <= names


def test_get_template_200_404_422(client):
    created = client.post("/budgets/templates/", json={"template_name": "Gettable"}).json()
    assert client.get(f"/budgets/templates/{created['id']}").status_code == 200
    assert client.get(f"/budgets/templates/{uuid4()}").status_code == 404
    assert client.get("/budgets/templates/not-a-uuid").status_code == 422


def test_update_template(client):
    created = client.post("/budgets/templates/", json={"template_name": "Before"}).json()
    resp = client.put(f"/budgets/templates/{created['id']}", json={"template_name": "After", "is_default": True})
    assert resp.status_code == 200
    assert resp.json()["template_name"] == "After"
    assert resp.json()["is_default"] is True


def test_update_template_unknown_404(client):
    assert client.put(f"/budgets/templates/{uuid4()}", json={"template_name": "X"}).status_code == 404


def test_delete_template_204(client):
    created = client.post("/budgets/templates/", json={"template_name": "Doomed"}).json()
    assert client.delete(f"/budgets/templates/{created['id']}").status_code == 204
    assert client.get(f"/budgets/templates/{created['id']}").status_code == 404


def test_unauthenticated_401(unauth_client):
    assert unauth_client.get("/budgets/templates/").status_code == 401


# ===== TEMPLATE CATEGORIES =====

def test_add_template_category_201(client, db):
    parent, _ = _parent_and_sub(db)
    tmpl = client.post("/budgets/templates/", json={"template_name": "AddCat"}).json()
    resp = client.post(
        f"/budgets/templates/{tmpl['id']}/categories/",
        json={"category_uuid": str(parent.uuid), "allocated_amount": "200.00"},
    )
    assert resp.status_code == 201
    assert Decimal(str(resp.json()["allocated_amount"])) == Decimal("200.00")


def test_add_template_category_envelope_400(client, db):
    parent, sub = _parent_and_sub(db)
    tmpl = client.post("/budgets/templates/", json={
        "template_name": "EnvAdd",
        "categories": [{"category_uuid": str(parent.uuid), "allocated_amount": "100.00"}],
    }).json()
    resp = client.post(
        f"/budgets/templates/{tmpl['id']}/categories/",
        json={"category_uuid": str(parent.uuid), "subcategory_uuid": str(sub.uuid), "allocated_amount": "150.00"},
    )
    assert resp.status_code == 400


def test_add_template_category_unknown_category_404(client):
    tmpl = client.post("/budgets/templates/", json={"template_name": "BadAdd"}).json()
    resp = client.post(
        f"/budgets/templates/{tmpl['id']}/categories/",
        json={"category_uuid": str(uuid4()), "allocated_amount": "10.00"},
    )
    assert resp.status_code == 404


def test_update_and_delete_template_category(client, db):
    parent, _ = _parent_and_sub(db)
    tmpl = client.post("/budgets/templates/", json={
        "template_name": "EditCat",
        "categories": [{"category_uuid": str(parent.uuid), "allocated_amount": "100.00"}],
    }).json()
    alloc_id = tmpl["categories"][0]["id"]

    upd = client.put(f"/budgets/templates/categories/{alloc_id}", json={"allocated_amount": "250.00"})
    assert upd.status_code == 200
    assert Decimal(str(upd.json()["allocated_amount"])) == Decimal("250.00")

    assert client.delete(f"/budgets/templates/categories/{alloc_id}").status_code == 204


# ===== BUDGET MONTHS =====

def test_get_month_auto_creates(client):
    resp = client.get("/budgets/months/2026/3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["year"] == 2026 and body["month"] == 3
    assert body["id"]


def test_assign_template_to_month(client, db):
    parent, _ = _parent_and_sub(db)
    tmpl = client.post("/budgets/templates/", json={
        "template_name": "MonthTmpl",
        "categories": [{"category_uuid": str(parent.uuid), "allocated_amount": "400.00"}],
    }).json()

    resp = client.put("/budgets/months/2026/4", json={"template_uuid": tmpl["id"]})
    assert resp.status_code == 200
    body = resp.json()
    assert body["template"]["id"] == tmpl["id"]
    assert Decimal(str(body["total_allocated"])) == Decimal("400.00")


def test_assign_unknown_template_404(client):
    assert client.put("/budgets/months/2026/5", json={"template_uuid": str(uuid4())}).status_code == 404


def test_list_budget_months(client):
    client.get("/budgets/months/2026/6")  # auto-create one
    months = client.get("/budgets/months/").json()
    assert any(m["year"] == 2026 and m["month"] == 6 for m in months)


def test_month_stats_and_performance(client):
    client.get("/budgets/months/2026/3")  # ensure month exists
    assert client.get("/budgets/months/2026/3/stats").status_code == 200
    assert client.get("/budgets/months/2026/3/performance").status_code == 200
