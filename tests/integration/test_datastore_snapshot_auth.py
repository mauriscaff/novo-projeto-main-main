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


def test_datastore_snapshot_endpoints_require_authentication():
    app.dependency_overrides.clear()
    with TestClient(app) as client:
        create_resp = client.post(
            "/api/v1/scan/datastore-snapshots",
            json={"vcenter_id": 1, "datastore_name": "DS1"},
        )
        assert create_resp.status_code == 401

        create_laudo_resp = client.post(
            "/api/v1/scan/datastore-laudos",
            json={"vcenter_id": 1, "datastore_name": "DS1"},
        )
        assert create_laudo_resp.status_code == 401

        get_resp = client.get("/api/v1/scan/datastore-snapshots/1")
        assert get_resp.status_code == 401

        get_laudo_resp = client.get("/api/v1/scan/datastore-laudos/1")
        assert get_laudo_resp.status_code == 401

        export_resp = client.get("/api/v1/scan/datastore-snapshots/1/export")
        assert export_resp.status_code == 401

        export_laudo_resp = client.get("/api/v1/scan/datastore-laudos/1/export")
        assert export_laudo_resp.status_code == 401

        report_resp = client.get(
            "/api/v1/scan/jobs/11111111-1111-1111-1111-111111111111/executive-report",
            params={"datastore_name": "DS1"},
        )
        assert report_resp.status_code == 401
