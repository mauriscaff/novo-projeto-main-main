# Integration tests - FastAPI endpoints
"""Testes dos endpoints FastAPI com TestClient."""

from config import get_settings

settings = get_settings()


def test_health_returns_200(client):
    r = client.get("/health")
    assert r.status_code == 200


def test_health_returns_json_with_status(client):
    r = client.get("/health")
    data = r.json()
    assert "status" in data
    assert data["status"] == "ok"
    assert "version" in data
    assert "timestamp" in data
    assert "service" in data  # compatibilidade com clientes atuais
    assert "database" not in data
    assert "scheduler" not in data
    assert "vcenters" not in data


def test_readiness_requires_authentication(client):
    r = client.get("/health/readiness")
    assert r.status_code == 401


def test_readiness_with_api_key_returns_details(client):
    r = client.get("/health/readiness", headers={"X-API-Key": settings.api_key})
    assert r.status_code == 200
    data = r.json()
    assert data["status"] in ("ok", "degraded")
    assert "database" in data
    assert "scheduler" in data
    assert "vcenters" in data


def test_docs_returns_200(client):
    r = client.get("/docs")
    assert r.status_code == 200


def test_redoc_returns_200(client):
    r = client.get("/redoc")
    assert r.status_code == 200
