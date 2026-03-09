# Integration tests - FastAPI endpoints
"""Testes dos endpoints FastAPI com TestClient."""

import os
os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter.db"

from fastapi.testclient import TestClient
from main import app

client = TestClient(app)


def test_health_returns_200():
    r = client.get("/health")
    assert r.status_code == 200


def test_health_returns_json_with_status():
    r = client.get("/health")
    data = r.json()
    assert "status" in data
    assert data["status"] in ("ok", "degraded")


def test_docs_returns_200():
    r = client.get("/docs")
    assert r.status_code == 200


def test_redoc_returns_200():
    r = client.get("/redoc")
    assert r.status_code == 200
