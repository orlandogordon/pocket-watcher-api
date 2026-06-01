"""Tests for the LLM health probe endpoint (#60).

The endpoint never hits a real model — get_llm_client is monkeypatched to a
fake whose health_check() returns a canned (online, model) tuple.
"""
import pytest

from src.routers import health as health_mod


class _FakeLLM:
    def __init__(self, online, model="served-model"):
        self._online = online
        self._model = model
        self.calls = 0

    def health_check(self):
        self.calls += 1
        return (self._online, self._model if self._online else None)


@pytest.fixture(autouse=True)
def _reset_cache():
    health_mod.reset_llm_health_cache()
    yield
    health_mod.reset_llm_health_cache()


def test_llm_health_online(client, monkeypatch):
    fake = _FakeLLM(online=True)
    monkeypatch.setattr(health_mod, "get_llm_client", lambda: fake)
    resp = client.get("/health/llm")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["online"] is True
    assert body["model"] == "served-model"
    assert "checked_at" in body


def test_llm_health_offline_returns_200(client, monkeypatch):
    fake = _FakeLLM(online=False)
    monkeypatch.setattr(health_mod, "get_llm_client", lambda: fake)
    resp = client.get("/health/llm")
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["online"] is False
    assert body["model"] is None


def test_llm_health_cached_within_ttl_probes_once(client, monkeypatch):
    fake = _FakeLLM(online=True)
    monkeypatch.setattr(health_mod, "get_llm_client", lambda: fake)
    client.get("/health/llm")
    client.get("/health/llm")
    assert fake.calls == 1


def test_llm_health_requires_auth(unauth_client):
    resp = unauth_client.get("/health/llm")
    assert resp.status_code == 401
