from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime, timezone

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter.db"

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.core.scanner import scan_runner
from app.core.scanner.zombie_detector import DatastoreScanMetric, ZombieType, ZombieVmdkResult
from app.dependencies import get_current_user
from app.models.base import AsyncSessionLocal, init_db
from app.models.vcenter import VCenter
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
import main as main_module

app = main_module.app


async def _noop_scheduler_start() -> None:
    return None


def _noop_scheduler_stop() -> None:
    return None


main_module.scheduler_start = _noop_scheduler_start
main_module.scheduler_stop = _noop_scheduler_stop


def _auth_override() -> dict:
    return {"sub": "scan-datastore-tester", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(ZombieVmdkRecord))
        await db.execute(delete(ZombieScanJob))
        await db.execute(delete(VCenter).where(VCenter.name.like("zz_test_%")))
        await db.commit()


async def _seed_vcenter() -> int:
    async with AsyncSessionLocal() as db:
        vc = VCenter(
            name="zz_test_vc-prod",
            host="zz_test_vc-prod.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        db.add(vc)
        await db.commit()
        await db.refresh(vc)
        return vc.id


def _install_vmware_stubs(monkeypatch) -> None:
    monkeypatch.setattr(scan_runner.connection_manager, "register", lambda _vc: None)
    monkeypatch.setattr(scan_runner.vcenter_pool, "get_service_instance", lambda _vc_id: object())
    monkeypatch.setattr(scan_runner, "build_global_vmdk_inventory", lambda _sis: frozenset())

    async def _fake_list_datacenters(_si):
        return ["DC1"]

    monkeypatch.setattr(scan_runner, "list_datacenters_async", _fake_list_datacenters)

    available: dict[str, list[tuple[ZombieType, float]]] = {
        "ds-prod-01": [(ZombieType.ORPHANED, 10.0)],
        "ds-backup-02": [(ZombieType.BROKEN_CHAIN, 2.5)],
    }

    async def _fake_scan_datacenter(
        _si,
        datacenter_name,
        orphan_days,
        stale_snapshot_days,
        min_file_size_mb,
        progress_callback=None,
        global_vmdk_paths=None,
        target_datastores=None,
    ):
        _ = orphan_days, stale_snapshot_days, min_file_size_mb, progress_callback, global_vmdk_paths
        now = datetime.now(timezone.utc)
        selected = list(available.keys())
        if target_datastores:
            targets = {d.strip().lower() for d in target_datastores if str(d).strip()}
            selected = [ds for ds in available if ds.lower() in targets]

        results: list[ZombieVmdkResult] = []
        metrics: list[DatastoreScanMetric] = []
        for ds in selected:
            ds_rows = available[ds]
            for idx, (z_type, size_gb) in enumerate(ds_rows, start=1):
                results.append(
                    ZombieVmdkResult(
                        path=f"[{ds}] vm-{idx}/disk-{idx}.vmdk",
                        datastore=ds,
                        tamanho_gb=size_gb,
                        ultima_modificacao=now,
                        tipo_zombie=z_type,
                        vcenter_host="vc-prod.local",
                        datacenter=datacenter_name,
                        detection_rules=["rule-test"],
                        likely_causes=[],
                        folder=f"vm-{idx}",
                        datastore_type="VMFS",
                        confidence_score=90,
                    )
                )
            metrics.append(
                DatastoreScanMetric(
                    datastore_name=ds,
                    scan_start_time=now,
                    scan_duration_seconds=0.05,
                    files_found=max(1, len(ds_rows)),
                    zombies_found=len(ds_rows),
                )
            )

        return results, metrics

    monkeypatch.setattr(scan_runner, "scan_datacenter", _fake_scan_datacenter)


def _wait_job_status(client: TestClient, job_id: str, timeout_sec: float = 4.0) -> dict:
    deadline = time.time() + timeout_sec
    last_payload: dict = {}
    while time.time() < deadline:
        resp = client.get(f"/api/v1/scan/jobs/{job_id}")
        assert resp.status_code == 200, resp.text
        payload = resp.json()
        last_payload = payload
        if payload["status"] in ("completed", "failed"):
            return payload
        time.sleep(0.05)
    return last_payload


def test_scan_by_datastore_existing(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_vmware_stubs(monkeypatch)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            start_resp = client.post(
                "/api/v1/scan/start-by-datastore",
                json={
                    "vcenter_ids": [vc_id],
                    "datacenters": ["DC1"],
                    "datastores": ["ds-prod-01"],
                },
            )
            assert start_resp.status_code == 202, start_resp.text
            job_id = start_resp.json()["job_id"]

            status_payload = _wait_job_status(client, job_id)
            assert status_payload["status"] == "completed"
            assert status_payload["summary"]["total_vmdks_encontrados"] == 1
            assert status_payload["summary"]["total_size_gb"] == 10.0
            assert status_payload["summary"]["total_excluiveis"] == 1
            assert status_payload["summary"]["total_excluiveis_gb"] == 10.0
            assert len(status_payload["datastore_metrics"]) == 1
            assert status_payload["datastore_metrics"][0]["datastore_name"] == "ds-prod-01"
    finally:
        app.dependency_overrides.clear()


def test_scan_by_datastore_inexistent(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_vmware_stubs(monkeypatch)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            start_resp = client.post(
                "/api/v1/scan/start-by-datastore",
                json={
                    "vcenter_ids": [vc_id],
                    "datacenters": ["DC1"],
                    "datastores": ["ds-nao-existe"],
                },
            )
            assert start_resp.status_code == 202, start_resp.text
            job_id = start_resp.json()["job_id"]

            status_payload = _wait_job_status(client, job_id)
            assert status_payload["status"] == "completed"
            assert status_payload["summary"]["total_vmdks_encontrados"] == 0
            assert status_payload["summary"]["total_size_gb"] == 0.0
            assert status_payload["summary"]["total_excluiveis"] == 0
            assert status_payload["summary"]["total_excluiveis_gb"] == 0.0
            assert any(
                "Datastore(s) nao encontrado(s)" in msg
                for msg in (status_payload.get("error_messages") or [])
            )
    finally:
        app.dependency_overrides.clear()


def test_scan_by_datastore_multiple(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_vmware_stubs(monkeypatch)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            start_resp = client.post(
                "/api/v1/scan/start-by-datastore",
                json={
                    "vcenter_ids": [vc_id],
                    "datastores": ["ds-prod-01", "ds-backup-02"],
                },
            )
            assert start_resp.status_code == 202, start_resp.text
            job_id = start_resp.json()["job_id"]

            status_payload = _wait_job_status(client, job_id)
            assert status_payload["status"] == "completed"
            assert status_payload["summary"]["total_vmdks_encontrados"] == 2
            assert status_payload["summary"]["total_size_gb"] == 12.5
            assert status_payload["summary"]["total_excluiveis"] == 2
            assert status_payload["summary"]["total_excluiveis_gb"] == 12.5

            ds_metrics = status_payload["datastore_metrics"]
            assert len(ds_metrics) == 2
            assert {m["datastore_name"] for m in ds_metrics} == {"ds-prod-01", "ds-backup-02"}
    finally:
        app.dependency_overrides.clear()
