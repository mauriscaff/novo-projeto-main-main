"""Testes de seguranca do frontend: sem exposicao de API key em HTML."""

from __future__ import annotations

import main as main_module
from app import dependencies


def test_dashboard_does_not_expose_api_key_in_html(client, monkeypatch):
    secret = "test-super-secret-key"
    monkeypatch.setattr(main_module.settings, "api_key", secret, raising=False)
    monkeypatch.setattr(dependencies.settings, "api_key", secret, raising=False)

    r = client.get("/")

    assert r.status_code == 200
    assert secret not in r.text
    set_cookie = r.headers.get("set-cookie", "")
    assert "zh_api_session=" in set_cookie
    assert "httponly" in set_cookie.lower()


def test_readiness_accepts_cookie_auth_without_api_header(client, monkeypatch):
    secret = "test-super-secret-key"
    monkeypatch.setattr(main_module.settings, "api_key", secret, raising=False)
    monkeypatch.setattr(dependencies.settings, "api_key", secret, raising=False)

    client.cookies.set("zh_api_session", secret)
    r = client.get("/health/readiness")

    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ("ok", "degraded")
