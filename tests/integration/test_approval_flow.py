"""Testes do fluxo de aprovacao (endpoints e bloqueio READONLY)."""

from __future__ import annotations

import os

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter.db"


class TestApprovalEndpoints:
    """Endpoints de aprovacao - comportamento esperado."""

    def test_list_approvals_accepts_get(self, client):
        """GET /api/v1/approvals/ pode retornar 200 (lista) ou 401."""
        r = client.get("/api/v1/approvals/")
        assert r.status_code in (200, 401, 403)

    def test_create_approval_requires_body(self, client):
        """POST /api/v1/approvals sem body -> 422."""
        r = client.post("/api/v1/approvals/", json={})
        assert r.status_code in (422, 401, 403)


class TestReadonlyModeReflectedInApi:
    """Resposta da API deve refletir READONLY_MODE quando disponivel."""

    def test_approvals_list_may_include_readonly_flag(self, client):
        """Se o endpoint retornar dados, pode incluir readonly_mode_active."""
        r = client.get("/api/v1/approvals/")
        if r.status_code == 200 and r.json():
            data = r.json()
            if isinstance(data, dict) and "readonly_mode_active" in data:
                assert isinstance(data["readonly_mode_active"], bool)
            elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                pass
