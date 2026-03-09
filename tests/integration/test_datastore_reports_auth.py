from __future__ import annotations

import os

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter.db"

from fastapi.testclient import TestClient

import main as main_module

app = main_module.app


async def _noop_scheduler_start() -> None:
    return None


def _noop_scheduler_stop() -> None:
    return None


main_module.scheduler_start = _noop_scheduler_start
main_module.scheduler_stop = _noop_scheduler_stop


def test_datastore_reports_endpoints_require_authentication():
    app.dependency_overrides.clear()
    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/datastore-reports/snapshots",
            json={
                "phase": "pre_delete",
                "job_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "datastore": "DS1",
            },
        )
        assert create_resp.status_code == 401

        get_resp = client.get("/api/v1/datastore-reports/snapshots/1")
        assert get_resp.status_code == 401

        compare_resp = client.get(
            "/api/v1/datastore-reports/compare",
            params={"pre_report_id": 1, "post_report_id": 2},
        )
        assert compare_resp.status_code == 401

        datastore_deletion_verify_resp = client.get(
            "/api/v1/datastore-reports/datastore-deletion-verification",
            params={"datastore": "DS1"},
        )
        assert datastore_deletion_verify_resp.status_code == 401

        verify_resp = client.get("/api/v1/datastore-reports/verify-files/pair-demo")
        assert verify_resp.status_code == 401
        verify_alias_resp = client.get("/api/v1/datastore-reports/post-exclusion-file-verification/pair-demo")
        assert verify_alias_resp.status_code == 401

        export_resp = client.get(
            "/api/v1/datastore-reports/verify-files/pair-demo/export",
            params={"format": "json"},
        )
        assert export_resp.status_code == 401
        export_alias_resp = client.get(
            "/api/v1/datastore-reports/post-exclusion-file-verification/pair-demo/export",
            params={"format": "json"},
        )
        assert export_alias_resp.status_code == 401
