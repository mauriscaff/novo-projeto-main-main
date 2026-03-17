from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.dependencies import get_current_user
from app.models.base import AsyncSessionLocal, init_db
from app.models.vcenter import VCenter
from app.models.zombie_scan import ZombieVmdkRecord
import app.api.routes.scanner as scanner_module
import main as main_module

app = main_module.app


async def _noop_scheduler_start() -> None:
    return None


def _noop_scheduler_stop() -> None:
    return None


main_module.scheduler_start = _noop_scheduler_start
main_module.scheduler_stop = _noop_scheduler_stop


def _auth_override() -> dict:
    return {"sub": "scan-datastores-tester", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(VCenter))
        await db.execute(delete(ZombieVmdkRecord))
        await db.commit()


def _build_record(*, datastore: str, vcenter_name: str, vcenter_host: str, path_suffix: str) -> ZombieVmdkRecord:
    now = datetime.now(timezone.utc)
    return ZombieVmdkRecord(
        job_id="job-test-datastores",
        path=f"[{datastore}] vm/{path_suffix}.vmdk",
        datastore=datastore,
        folder="vm",
        datastore_type="VMFS",
        tamanho_gb=1.0,
        ultima_modificacao=now,
        tipo_zombie="ORPHANED",
        vcenter_host=vcenter_host,
        vcenter_name=vcenter_name,
        datacenter="DC1",
        detection_rules=["rule-test"],
        likely_causes=[],
    )


async def _seed_records() -> None:
    async with AsyncSessionLocal() as db:
        db.add_all(
            [
                _build_record(
                    datastore="ds-common",
                    vcenter_name="vc-a",
                    vcenter_host="vc-a.local",
                    path_suffix="disk-1",
                ),
                # Mesmo datastore + mesmo vCenter (deve continuar sendo 1 entrada distinta)
                _build_record(
                    datastore="ds-common",
                    vcenter_name="vc-a",
                    vcenter_host="vc-a.local",
                    path_suffix="disk-2",
                ),
                # Mesmo datastore em outro vCenter (NAO pode ser ocultado)
                _build_record(
                    datastore="ds-common",
                    vcenter_name="vc-b",
                    vcenter_host="vc-b.local",
                    path_suffix="disk-3",
                ),
                _build_record(
                    datastore="ds-only-a",
                    vcenter_name="vc-a",
                    vcenter_host="vc-a.local",
                    path_suffix="disk-4",
                ),
                _build_record(
                    datastore="ds-only-b",
                    vcenter_name="vc-b",
                    vcenter_host="vc-b.local",
                    path_suffix="disk-5",
                ),
            ]
        )
        await db.commit()


async def _seed_active_vcenter() -> None:
    async with AsyncSessionLocal() as db:
        db.add(
            VCenter(
                name="vc-live",
                host="vc-live.local",
                port=443,
                username="administrator@vsphere.local",
                password="dummy-encrypted",
                disable_ssl_verify=True,
                is_active=True,
            )
        )
        await db.commit()


def test_list_known_datastores_keeps_distinct_entries_per_vcenter():
    asyncio.run(_reset_db())
    asyncio.run(_seed_records())

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get("/api/v1/scan/datastores")
            assert resp.status_code == 200, resp.text

            payload = resp.json()
            assert isinstance(payload, list)

            unique_pairs = {
                (row["name"], row["vcenter_name"], row["vcenter_host"])
                for row in payload
            }
            assert len(unique_pairs) == len(payload)
            assert ("ds-common", "vc-a", "vc-a.local") in unique_pairs
            assert ("ds-common", "vc-b", "vc-b.local") in unique_pairs
            assert ("ds-only-a", "vc-a", "vc-a.local") in unique_pairs
            assert ("ds-only-b", "vc-b", "vc-b.local") in unique_pairs
            assert all("vcenter_id" in row for row in payload)
            assert all(row["vcenter_id"] is None for row in payload)
    finally:
        app.dependency_overrides.clear()


def test_list_datastores_live_includes_maintenance_flags_per_vcenter(monkeypatch):
    asyncio.run(_reset_db())
    asyncio.run(_seed_active_vcenter())

    async def _fake_list_datastores_async(_si):
        return [
            {
                "name": "STA_LUN_05",
                "accessible": True,
                "maintenance_mode": True,
                "maintenance_state": "inMaintenance",
            },
            {
                "name": "STA_LUN_06",
                "accessible": True,
                "maintenance_mode": False,
                "maintenance_state": "normal",
            },
        ]

    monkeypatch.setattr(scanner_module.connection_manager, "register", lambda _vc: None)
    monkeypatch.setattr(scanner_module.vcenter_pool, "get_service_instance", lambda _vc_id: object())
    monkeypatch.setattr(scanner_module, "list_datastores_async", _fake_list_datastores_async)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get("/api/v1/scan/datastores?source=live")
            assert resp.status_code == 200, resp.text
            payload = resp.json()
            assert isinstance(payload, list)
            assert all("vcenter_id" in row for row in payload)
            assert all(isinstance(row["vcenter_id"], int) for row in payload)
            assert any(
                row["name"] == "STA_LUN_05"
                and row["vcenter_host"] == "vc-live.local"
                and row["maintenance_mode"] is True
                and row["maintenance_state"] == "inMaintenance"
                and isinstance(row["vcenter_id"], int)
                for row in payload
            )
            assert any(
                row["name"] == "STA_LUN_06"
                and row["vcenter_host"] == "vc-live.local"
                and row["maintenance_mode"] is False
                for row in payload
            )
    finally:
        app.dependency_overrides.clear()

