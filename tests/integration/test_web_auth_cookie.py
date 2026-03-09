"""Testes de autenticacao web por sessao JWT (cookie HttpOnly)."""

from __future__ import annotations


def test_dashboard_does_not_expose_api_key_in_html(client):
    r = client.get("/")
    assert r.status_code == 200
    assert "window.ZH_API_KEY = \"\";" in r.text
    assert "change-me-in-production" not in r.text
    assert "zh_api_session=" not in r.headers.get("set-cookie", "")


def test_session_me_requires_authentication(client):
    r = client.get("/api/v1/auth/session/me")
    assert r.status_code == 401


def test_session_login_sets_http_only_cookie_and_me_works(client):
    login = client.post(
        "/api/v1/auth/session/login",
        json={"username": "admin", "password": "P@ssw0rd"},
    )
    assert login.status_code == 200
    set_cookie = login.headers.get("set-cookie", "").lower()
    assert "zh_access_token=" in set_cookie
    assert "httponly" in set_cookie

    me = client.get("/api/v1/auth/session/me")
    assert me.status_code == 200
    payload = me.json()
    assert payload["sub"] == "admin"
    assert payload["method"] == "jwt_cookie"


def test_readiness_accepts_session_cookie_without_api_key_header(client):
    client.post("/api/v1/auth/session/login", json={"username": "admin", "password": "P@ssw0rd"})
    r = client.get("/health/readiness")
    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ("ok", "degraded")


def test_session_logout_revokes_cookie_for_next_requests(client):
    client.post("/api/v1/auth/session/login", json={"username": "admin", "password": "P@ssw0rd"})
    logout = client.post("/api/v1/auth/session/logout")
    assert logout.status_code == 200
    assert "zh_access_token=" in logout.headers.get("set-cookie", "")

    me = client.get("/api/v1/auth/session/me")
    assert me.status_code == 401
