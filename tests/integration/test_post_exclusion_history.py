from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.dependencies import get_current_user
from app.models.audit_log import ApprovalToken, AuditLog
from app.models.base import AsyncSessionLocal, init_db
from app.models.datastore_deletion_run import DatastoreDeletionVerificationRun
from app.models.datastore_report_snapshot import DatastoreDecommissionReport
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
    return {"sub": "history-tester", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(AuditLog))
        await db.execute(delete(ApprovalToken))
        await db.execute(delete(DatastoreDecommissionReport))
        await db.execute(delete(DatastoreDeletionVerificationRun))
        await db.execute(delete(ZombieVmdkRecord))
        await db.execute(delete(ZombieScanJob))
        await db.execute(delete(VCenter).where(VCenter.name.like("zz_hist_%")))
        await db.commit()


async def _seed_history() -> tuple[int, int]:
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as db:
        vc = VCenter(
            name="zz_hist_vcenter",
            host="zz_hist_vcenter.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        db.add(vc)
        await db.flush()

        baseline_removed = "30000000-0000-0000-0000-000000000001"
        verification_removed = "30000000-0000-0000-0000-000000000002"
        baseline_same = "30000000-0000-0000-0000-000000000003"
        verification_same = "30000000-0000-0000-0000-000000000004"

        db.add_all(
            [
                ZombieScanJob(
                    job_id=baseline_removed,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS_HIST_REMOVED"],
                    status="completed",
                    started_at=now - timedelta(hours=4),
                    finished_at=now - timedelta(hours=4, minutes=-1),
                    total_vmdks=2,
                    total_size_gb=15.0,
                    datastore_metrics=[{"datastore_name": "DS_HIST_REMOVED", "files_found": 2, "zombies_found": 2}],
                ),
                ZombieScanJob(
                    job_id=verification_removed,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS_OTHER"],
                    status="completed",
                    started_at=now - timedelta(hours=3),
                    finished_at=now - timedelta(hours=3, minutes=-1),
                    total_vmdks=0,
                    total_size_gb=0.0,
                    datastore_metrics=[{"datastore_name": "DS_OTHER", "files_found": 1, "zombies_found": 0}],
                ),
                ZombieScanJob(
                    job_id=baseline_same,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS_HIST_KEEP"],
                    status="completed",
                    started_at=now - timedelta(hours=2),
                    finished_at=now - timedelta(hours=2, minutes=-1),
                    total_vmdks=1,
                    total_size_gb=3.0,
                    datastore_metrics=[{"datastore_name": "DS_HIST_KEEP", "files_found": 1, "zombies_found": 1}],
                ),
                ZombieScanJob(
                    job_id=verification_same,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS_HIST_KEEP"],
                    status="completed",
                    started_at=now - timedelta(hours=1),
                    finished_at=now - timedelta(hours=1, minutes=-1),
                    total_vmdks=1,
                    total_size_gb=3.0,
                    datastore_metrics=[{"datastore_name": "DS_HIST_KEEP", "files_found": 1, "zombies_found": 1}],
                ),
            ]
        )

        db.add_all(
            [
                ZombieVmdkRecord(
                    job_id=baseline_removed,
                    path="[DS_HIST_REMOVED] vm-a/a.vmdk",
                    datastore="DS_HIST_REMOVED",
                    tamanho_gb=10.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-a"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=baseline_removed,
                    path="[DS_HIST_REMOVED] vm-b/b.vmdk",
                    datastore="DS_HIST_REMOVED",
                    tamanho_gb=5.0,
                    tipo_zombie="BROKEN_CHAIN",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-b"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=baseline_same,
                    path="[DS_HIST_KEEP] vm-c/c.vmdk",
                    datastore="DS_HIST_KEEP",
                    tamanho_gb=3.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-c"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=verification_same,
                    path="[DS_HIST_KEEP] vm-c/c.vmdk",
                    datastore="DS_HIST_KEEP",
                    tamanho_gb=3.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-c"],
                    likely_causes=[],
                ),
            ]
        )
        await db.flush()

        removed_run = DatastoreDeletionVerificationRun(
            datastore="DS_HIST_REMOVED",
            vcenter_host_scope=vc.host,
            baseline_job_id=baseline_removed,
            verification_job_id=verification_removed,
            status="datastore_removed",
            deleted_vmdk_count=2,
            deleted_size_gb=15.0,
            remaining_vmdk_count=0,
            remaining_size_gb=0.0,
        )
        keep_run = DatastoreDeletionVerificationRun(
            datastore="DS_HIST_KEEP",
            vcenter_host_scope=vc.host,
            baseline_job_id=baseline_same,
            verification_job_id=verification_same,
            status="no_cleanup",
            deleted_vmdk_count=0,
            deleted_size_gb=0.0,
            remaining_vmdk_count=1,
            remaining_size_gb=3.0,
        )
        db.add_all([removed_run, keep_run])
        await db.commit()
        await db.refresh(removed_run)
        await db.refresh(keep_run)
        return removed_run.id, keep_run.id


def test_deletion_history_list_endpoint_returns_summary_and_items():
    asyncio.run(_reset_db())
    removed_run_id, keep_run_id = asyncio.run(_seed_history())
    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get("/api/v1/datastore-reports/datastore-deletion-verification/history")
        assert resp.status_code == 200
        data = resp.json()
        assert data["summary"]["total_verifications"] == 2
        assert data["summary"]["total_datastores_removed"] == 1
        assert data["summary"]["total_deleted_vmdks"] == 2
        ids = {item["run_id"] for item in data["items"]}
        assert removed_run_id in ids
        assert keep_run_id in ids
        removed_item = next(item for item in data["items"] if item["run_id"] == removed_run_id)
        assert removed_item["datastore"] == "DS_HIST_REMOVED"
        assert removed_item["evidence_consistent_with_stored_summary"] is True
        assert removed_item["deleted_vmdks"] == []
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_deletion_history_detail_endpoint_returns_reconstructed_evidence():
    asyncio.run(_reset_db())
    removed_run_id, _ = asyncio.run(_seed_history())
    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/datastore-reports/datastore-deletion-verification/history/{removed_run_id}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["run_id"] == removed_run_id
        assert data["deleted_vmdk_count"] == 2
        assert data["evidence_consistent_with_stored_summary"] is True
        assert sorted(item["path"] for item in data["deleted_vmdks"]) == [
            "[DS_HIST_REMOVED] vm-a/a.vmdk",
            "[DS_HIST_REMOVED] vm-b/b.vmdk",
        ]
    finally:
        app.dependency_overrides.pop(get_current_user, None)


def test_post_exclusion_history_page_loads(client):
    resp = client.get("/operations/post-exclusion-history")
    assert resp.status_code == 200
    assert 'id="zh-history-feedback"' in resp.text
    assert "/static/js/post_exclusion_history.js" in resp.text
