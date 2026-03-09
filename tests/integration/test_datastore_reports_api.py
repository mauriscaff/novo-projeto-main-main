from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter.db"

from app.api.routes import datastore_reports as datastore_reports_route
from app.dependencies import get_current_user
from app.models.audit_log import ApprovalToken, AuditLog
from app.models.base import AsyncSessionLocal, init_db
from app.models.datastore_deletion_run import DatastoreDeletionVerificationRun
from app.models.datastore_report_snapshot import DatastoreDecommissionReport
from app.models.datastore_snapshot import DatastoreDecomSnapshot
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
    return {"sub": "integration-reporter", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(AuditLog))
        await db.execute(delete(ApprovalToken))
        await db.execute(delete(DatastoreDecommissionReport))
        await db.execute(delete(DatastoreDeletionVerificationRun))
        await db.execute(delete(DatastoreDecomSnapshot))
        await db.execute(delete(ZombieVmdkRecord))
        await db.execute(delete(ZombieScanJob))
        await db.execute(delete(VCenter).where(VCenter.name.like("zz_test_%")))
        await db.commit()


async def _seed_data() -> tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as db:
        vc = VCenter(
            name="zz_test_vc-report",
            host="zz_test_vc-report.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        db.add(vc)
        await db.flush()

        pre_job_id = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        post_job_id = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        ds2_job_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"

        db.add_all(
            [
                ZombieScanJob(
                    job_id=pre_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS1"],
                    status="completed",
                    started_at=now - timedelta(minutes=30),
                    finished_at=now - timedelta(minutes=29),
                    total_vmdks=3,
                    total_size_gb=17.0,
                ),
                ZombieScanJob(
                    job_id=post_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS1"],
                    status="completed",
                    started_at=now - timedelta(minutes=20),
                    finished_at=now - timedelta(minutes=19),
                    total_vmdks=1,
                    total_size_gb=5.0,
                ),
                ZombieScanJob(
                    job_id=ds2_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS2"],
                    status="completed",
                    started_at=now - timedelta(minutes=10),
                    finished_at=now - timedelta(minutes=9),
                    total_vmdks=1,
                    total_size_gb=2.0,
                ),
            ]
        )

        db.add_all(
            [
                ZombieVmdkRecord(
                    job_id=pre_job_id,
                    path="[DS1] vm-a/a.vmdk",
                    datastore="DS1",
                    tamanho_gb=10.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-a"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=pre_job_id,
                    path="[DS1] vm-b/b.vmdk",
                    datastore="DS1",
                    tamanho_gb=5.0,
                    tipo_zombie="BROKEN_CHAIN",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-b"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=pre_job_id,
                    path="[DS1] vm-c/c.vmdk",
                    datastore="DS1",
                    tamanho_gb=2.0,
                    tipo_zombie="POSSIBLE_FALSE_POSITIVE",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-c"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=post_job_id,
                    path="[DS1] vm-b/b.vmdk",
                    datastore="DS1",
                    tamanho_gb=5.0,
                    tipo_zombie="BROKEN_CHAIN",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-b"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=ds2_job_id,
                    path="[DS2] vm-z/z.vmdk",
                    datastore="DS2",
                    tamanho_gb=2.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-z"],
                    likely_causes=[],
                ),
            ]
        )
        await db.commit()
        return pre_job_id, post_job_id, ds2_job_id


async def _seed_auto_datastore_deletion_case(
    *,
    status_case: str,
    datastore: str = "DS_AUTO",
) -> tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as db:
        vc = VCenter(
            name=f"zz_test_vc-auto-{status_case}",
            host=f"zz_test_vc-auto-{status_case}.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        db.add(vc)
        await db.flush()

        baseline_job_id = f"11111111-1111-1111-1111-1111111111{status_case[:1]}"
        verification_job_id = f"22222222-2222-2222-2222-2222222222{status_case[:1]}"

        baseline_metrics = [{"datastore_name": datastore, "files_found": 3, "zombies_found": 3}]
        if status_case == "datastore_removed":
            verification_metrics = [{"datastore_name": "DS_OTHER", "files_found": 1, "zombies_found": 1}]
        else:
            verification_metrics = [{"datastore_name": datastore, "files_found": 1, "zombies_found": 1}]

        db.add_all(
            [
                ZombieScanJob(
                    job_id=baseline_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=[datastore],
                    status="completed",
                    started_at=now - timedelta(minutes=20),
                    finished_at=now - timedelta(minutes=19),
                    total_vmdks=3,
                    total_size_gb=17.0,
                    datastore_metrics=baseline_metrics,
                ),
                ZombieScanJob(
                    job_id=verification_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=[datastore],
                    status="completed",
                    started_at=now - timedelta(minutes=10),
                    finished_at=now - timedelta(minutes=9),
                    total_vmdks=1,
                    total_size_gb=5.0,
                    datastore_metrics=verification_metrics,
                ),
            ]
        )

        baseline_records = [
            ZombieVmdkRecord(
                job_id=baseline_job_id,
                path=f"[{datastore}] vm-a/a.vmdk",
                datastore=datastore,
                tamanho_gb=10.0,
                tipo_zombie="ORPHANED",
                vcenter_host=vc.host,
                vcenter_name=vc.name,
                datacenter="DC1",
                detection_rules=["rule-a"],
                likely_causes=[],
            ),
            ZombieVmdkRecord(
                job_id=baseline_job_id,
                path=f"[{datastore}] vm-b/b.vmdk",
                datastore=datastore,
                tamanho_gb=5.0,
                tipo_zombie="BROKEN_CHAIN",
                vcenter_host=vc.host,
                vcenter_name=vc.name,
                datacenter="DC1",
                detection_rules=["rule-b"],
                likely_causes=[],
            ),
            ZombieVmdkRecord(
                job_id=baseline_job_id,
                path=f"[{datastore}] vm-c/c.vmdk",
                datastore=datastore,
                tamanho_gb=2.0,
                tipo_zombie="SNAPSHOT_ORPHAN",
                vcenter_host=vc.host,
                vcenter_name=vc.name,
                datacenter="DC1",
                detection_rules=["rule-c"],
                likely_causes=[],
            ),
        ]
        db.add_all(baseline_records)

        if status_case == "partial_cleanup":
            db.add(
                ZombieVmdkRecord(
                    job_id=verification_job_id,
                    path=f"[{datastore}] vm-b/b.vmdk",
                    datastore=datastore,
                    tamanho_gb=5.0,
                    tipo_zombie="BROKEN_CHAIN",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-b"],
                    likely_causes=[],
                )
            )
        elif status_case == "no_cleanup":
            db.add_all(
                [
                    ZombieVmdkRecord(
                        job_id=verification_job_id,
                        path=f"[{datastore}] vm-a/a.vmdk",
                        datastore=datastore,
                        tamanho_gb=10.0,
                        tipo_zombie="ORPHANED",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-a"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id=verification_job_id,
                        path=f"[{datastore}] vm-b/b.vmdk",
                        datastore=datastore,
                        tamanho_gb=5.0,
                        tipo_zombie="BROKEN_CHAIN",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-b"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id=verification_job_id,
                        path=f"[{datastore}] vm-c/c.vmdk",
                        datastore=datastore,
                        tamanho_gb=2.0,
                        tipo_zombie="SNAPSHOT_ORPHAN",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-c"],
                        likely_causes=[],
                    ),
                ]
            )
        else:
            db.add(
                ZombieVmdkRecord(
                    job_id=verification_job_id,
                    path="[DS_OTHER] vm-z/z.vmdk",
                    datastore="DS_OTHER",
                    tamanho_gb=1.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-z"],
                    likely_causes=[],
                )
            )

        await db.commit()
        return vc.host, baseline_job_id, verification_job_id


@pytest.fixture(scope="module")
def client() -> TestClient:
    app.dependency_overrides[get_current_user] = _auth_override
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_snapshot_and_compare_success(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    pre_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "pre_delete",
            "job_id": pre_job_id,
            "datastore": "DS1",
        },
    )
    assert pre_resp.status_code == 201, pre_resp.text
    pre = pre_resp.json()
    assert pre["phase"] == "pre_delete"
    assert pre["total_items"] == 3
    assert pre["total_size_gb"] == 17.0
    assert pre["deletable_items"] == 2
    assert pre["deletable_size_gb"] == 15.0

    post_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": pre["pair_id"],
        },
    )
    assert post_resp.status_code == 201, post_resp.text
    post = post_resp.json()
    assert post["phase"] == "post_delete"
    assert post["total_items"] == 1
    assert post["total_size_gb"] == 5.0
    assert post["deletable_items"] == 1
    assert post["deletable_size_gb"] == 5.0

    get_resp = client.get(f"/api/v1/datastore-reports/snapshots/{pre['report_id']}")
    assert get_resp.status_code == 200
    assert get_resp.json()["report_id"] == pre["report_id"]

    cmp_resp = client.get(
        "/api/v1/datastore-reports/compare",
        params={
            "pre_report_id": pre["report_id"],
            "post_report_id": post["report_id"],
        },
    )
    assert cmp_resp.status_code == 200, cmp_resp.text
    data = cmp_resp.json()
    assert data["datastore"] == "DS1"
    assert data["removed_items"] == 2
    assert data["removed_size_gb"] == 12.0
    assert data["removed_breakdown"]["ORPHANED"] == 1
    assert data["removed_breakdown"]["POSSIBLE_FALSE_POSITIVE"] == 1
    assert data["pre_totals"]["total_items"] == 3
    assert data["post_totals"]["total_items"] == 1


def test_snapshot_validation_errors_and_compare_rules(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, ds2_job_id = asyncio.run(_seed_data())

    missing_job = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "pre_delete",
            "job_id": "99999999-9999-9999-9999-999999999999",
            "datastore": "DS1",
        },
    )
    assert missing_job.status_code == 404

    ds_without_rows = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "pre_delete",
            "job_id": pre_job_id,
            "datastore": "DS_DOES_NOT_EXIST",
        },
    )
    assert ds_without_rows.status_code == 404

    pre_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    post_same_ds_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "post_delete", "job_id": post_job_id, "datastore": "DS1"},
    )
    post_other_ds_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "post_delete", "job_id": ds2_job_id, "datastore": "DS2"},
    )

    assert pre_resp.status_code == 201
    assert post_same_ds_resp.status_code == 201
    assert post_other_ds_resp.status_code == 201

    pre = pre_resp.json()
    post_other_ds = post_other_ds_resp.json()

    mismatch_ds_compare = client.get(
        "/api/v1/datastore-reports/compare",
        params={
            "pre_report_id": pre["report_id"],
            "post_report_id": post_other_ds["report_id"],
        },
    )
    assert mismatch_ds_compare.status_code == 422

    wrong_phase_compare = client.get(
        "/api/v1/datastore-reports/compare",
        params={
            "pre_report_id": pre["report_id"],
            "post_report_id": pre["report_id"],
        },
    )
    assert wrong_phase_compare.status_code == 422

    not_found_report = client.get("/api/v1/datastore-reports/snapshots/999999")
    assert not_found_report.status_code == 404


def test_snapshot_pair_integrity_and_strict_compare(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    pre_1_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    assert pre_1_resp.status_code == 201, pre_1_resp.text
    pre_1 = pre_1_resp.json()

    bad_pair_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": "pair-inexistente",
        },
    )
    assert bad_pair_resp.status_code == 422
    assert "pair_id informado" in bad_pair_resp.json()["detail"]

    pre_2_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    assert pre_2_resp.status_code == 201, pre_2_resp.text
    pre_2 = pre_2_resp.json()

    post_2_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": pre_2["pair_id"],
        },
    )
    assert post_2_resp.status_code == 201, post_2_resp.text
    post_2 = post_2_resp.json()

    non_strict_resp = client.get(
        "/api/v1/datastore-reports/compare",
        params={
            "pre_report_id": pre_1["report_id"],
            "post_report_id": post_2["report_id"],
        },
    )
    assert non_strict_resp.status_code == 200, non_strict_resp.text

    strict_resp = client.get(
        "/api/v1/datastore-reports/compare",
        params={
            "pre_report_id": pre_1["report_id"],
            "post_report_id": post_2["report_id"],
            "strict_pair": True,
        },
    )
    assert strict_resp.status_code == 422
    assert "strict_pair=true" in strict_resp.json()["detail"]


def test_verify_files_success_with_pagination_and_export(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    pre_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    assert pre_resp.status_code == 201, pre_resp.text
    pre = pre_resp.json()

    post_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": pre["pair_id"],
        },
    )
    assert post_resp.status_code == 201, post_resp.text

    pair_id = pre["pair_id"]
    verify_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={"page": 1, "page_size": 1},
    )
    assert verify_resp.status_code == 200, verify_resp.text
    data = verify_resp.json()
    assert data["pair_id"] == pair_id
    assert data["datastore_name"] == "DS1"
    assert data["datastore_found_in_pre"] is True
    assert data["datastore_found_in_post"] is True
    assert data["deleted_files_count"] == 2
    assert data["deleted_size_gb"] == 12.0
    assert data["removed_files_count"] == 2
    assert data["removed_size_gb"] == 12.0
    assert data["size_gain_gb"] == 12.0
    assert data["size_gain_percent"] == 70.59
    assert data["pre_total_size_gb"] == 17.0
    assert data["post_total_size_gb"] == 5.0
    assert data["remaining_size_gb"] == 5.0
    assert data["deleted_breakdown"]["ORPHANED"] == 1
    assert data["deleted_breakdown"]["POSSIBLE_FALSE_POSITIVE"] == 1
    assert data["deleted_size_breakdown_gb"]["ORPHANED"] == 10.0
    assert data["deleted_size_breakdown_gb"]["POSSIBLE_FALSE_POSITIVE"] == 2.0
    assert data["remaining_files_count"] == 1
    assert data["verification_status"] == "partially_removed"
    assert data["datastore_status"] == "still_present"
    assert data["total_evidence"] == 2
    assert data["has_more_evidence"] is True
    assert len(data["deleted_vmdks"]) == 1
    assert data["deleted_vmdks"][0]["path"] == "[DS1] vm-a/a.vmdk"
    assert data["deleted_vmdks"][0]["last_seen_job_id"] == pre_job_id

    verify_alias_resp = client.get(
        f"/api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}",
        params={"page": 1, "page_size": 1, "sort_by": "size_asc"},
    )
    assert verify_alias_resp.status_code == 200, verify_alias_resp.text
    alias_data = verify_alias_resp.json()
    assert alias_data["pair_id"] == pair_id
    assert alias_data["deleted_vmdks"][0]["path"] == "[DS1] vm-c/c.vmdk"
    assert alias_data["has_more_evidence"] is True

    verify_alias_capped_resp = client.get(
        f"/api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}",
        params={"page": 1, "page_size": 500, "include_deleted_limit": 1},
    )
    assert verify_alias_capped_resp.status_code == 200, verify_alias_capped_resp.text
    capped_data = verify_alias_capped_resp.json()
    assert len(capped_data["deleted_vmdks"]) == 1

    verify_page2_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={"page": 2, "page_size": 1},
    )
    assert verify_page2_resp.status_code == 200
    page2_data = verify_page2_resp.json()
    assert len(page2_data["deleted_vmdks"]) == 1
    assert page2_data["has_more_evidence"] is False
    assert page2_data["deleted_vmdks"][0]["path"] == "[DS1] vm-c/c.vmdk"

    verify_no_evidence_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={"include_deleted_vmdks": "false"},
    )
    assert verify_no_evidence_resp.status_code == 200
    no_evidence_data = verify_no_evidence_resp.json()
    assert no_evidence_data["deleted_files_count"] == 2
    assert no_evidence_data["deleted_vmdks"] == []
    assert no_evidence_data["has_more_evidence"] is False

    export_json_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}/export",
        params={"format": "json"},
    )
    assert export_json_resp.status_code == 200, export_json_resp.text
    assert "application/json" in export_json_resp.headers.get("content-type", "")
    assert "attachment; filename=" in export_json_resp.headers.get("content-disposition", "")
    export_json = export_json_resp.json()
    assert export_json["deleted_files_count"] == 2
    assert export_json["deleted_size_gb"] == 12.0
    assert export_json["size_gain_gb"] == 12.0
    assert export_json["size_gain_percent"] == 70.59
    assert len(export_json["deleted_vmdks"]) == 2

    export_csv_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}/export",
        params={"format": "csv"},
    )
    assert export_csv_resp.status_code == 200, export_csv_resp.text
    assert "text/csv" in export_csv_resp.headers.get("content-type", "")
    assert "attachment; filename=" in export_csv_resp.headers.get("content-disposition", "")
    csv_body = export_csv_resp.text
    assert (
        "pair_id,datastore,path,tamanho_gb,tipo_zombie,vcenter_host,datacenter,last_seen_job_id"
        in csv_body
    )
    assert "[DS1] vm-a/a.vmdk" in csv_body
    assert "[DS1] vm-c/c.vmdk" in csv_body

    export_alias_resp = client.get(
        f"/api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}/export",
        params={"format": "json"},
    )
    assert export_alias_resp.status_code == 200, export_alias_resp.text
    assert "application/json" in export_alias_resp.headers.get("content-type", "")
    assert "attachment; filename=" in export_alias_resp.headers.get("content-disposition", "")
    export_alias_json = export_alias_resp.json()
    assert export_alias_json["pair_id"] == pair_id
    assert len(export_alias_json["deleted_vmdks"]) == 2

    export_alias_csv_resp = client.get(
        f"/api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}/export",
        params={"format": "csv"},
    )
    assert export_alias_csv_resp.status_code == 200, export_alias_csv_resp.text
    assert "text/csv" in export_alias_csv_resp.headers.get("content-type", "")
    assert "attachment; filename=" in export_alias_csv_resp.headers.get("content-disposition", "")
    alias_csv_body = export_alias_csv_resp.text
    assert (
        "pair_id,datastore,path,tamanho_gb,tipo_zombie,vcenter_host,datacenter,last_seen_job_id"
        in alias_csv_body
    )
    assert "[DS1] vm-a/a.vmdk" in alias_csv_body

    invalid_sort_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={"sort_by": "invalid"},
    )
    assert invalid_sort_resp.status_code == 422


def test_verify_files_pair_errors(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    pair_not_found = client.get("/api/v1/datastore-reports/verify-files/pair-inexistente")
    assert pair_not_found.status_code == 404
    pair_not_found_export = client.get(
        "/api/v1/datastore-reports/verify-files/pair-inexistente/export",
        params={"format": "json"},
    )
    assert pair_not_found_export.status_code == 404
    invalid_export_format = client.get(
        "/api/v1/datastore-reports/verify-files/pair-inexistente/export",
        params={"format": "xml"},
    )
    assert invalid_export_format.status_code == 422
    invalid_export_format_alias = client.get(
        "/api/v1/datastore-reports/post-exclusion-file-verification/pair-inexistente/export",
        params={"format": "xml"},
    )
    assert invalid_export_format_alias.status_code == 422

    async def _seed_inconsistent_pair() -> None:
        async with AsyncSessionLocal() as db:
            db.add_all(
                [
                    DatastoreDecommissionReport(
                        pair_id="pair-inconsistente",
                        phase="pre_delete",
                        job_id=pre_job_id,
                        datastore="DS1",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=3,
                        total_size_gb=17.0,
                        deletable_items=2,
                        deletable_size_gb=15.0,
                        breakdown={"ORPHANED": 1},
                    ),
                    DatastoreDecommissionReport(
                        pair_id="pair-inconsistente",
                        phase="post_delete",
                        job_id=post_job_id,
                        datastore="DS2",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=1,
                        total_size_gb=5.0,
                        deletable_items=1,
                        deletable_size_gb=5.0,
                        breakdown={"ORPHANED": 1},
                    ),
                ]
            )
            await db.commit()

    asyncio.run(_seed_inconsistent_pair())

    inconsistent_pair = client.get("/api/v1/datastore-reports/verify-files/pair-inconsistente")
    assert inconsistent_pair.status_code == 422
    assert "inconsistente" in inconsistent_pair.json()["detail"]
    inconsistent_pair_export = client.get(
        "/api/v1/datastore-reports/verify-files/pair-inconsistente/export",
        params={"format": "csv"},
    )
    assert inconsistent_pair_export.status_code == 422

    async def _seed_unknown_scope_pair() -> None:
        async with AsyncSessionLocal() as db:
            post_scope_mismatch_job = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
            db.add(
                ZombieScanJob(
                    job_id=post_scope_mismatch_job,
                    vcenter_ids=[1],
                    datacenters=["DC1"],
                    datastores=["DS1"],
                    status="completed",
                    started_at=datetime.now(timezone.utc) - timedelta(minutes=3),
                    finished_at=datetime.now(timezone.utc) - timedelta(minutes=2),
                    total_vmdks=1,
                    total_size_gb=8.0,
                )
            )
            db.add(
                ZombieVmdkRecord(
                    job_id=post_scope_mismatch_job,
                    path="[DS1] vm-scope/scope.vmdk",
                    datastore="DS1",
                    tamanho_gb=8.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host="zz_test_vc-outra.local",
                    vcenter_name="zz_test_vc-report",
                    datacenter="DC1",
                    detection_rules=["rule-scope"],
                    likely_causes=[],
                )
            )
            db.add_all(
                [
                    DatastoreDecommissionReport(
                        pair_id="pair-unknown-scope",
                        phase="pre_delete",
                        job_id=pre_job_id,
                        datastore="DS1",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=3,
                        total_size_gb=17.0,
                        deletable_items=2,
                        deletable_size_gb=15.0,
                        breakdown={"ORPHANED": 1},
                    ),
                    DatastoreDecommissionReport(
                        pair_id="pair-unknown-scope",
                        phase="post_delete",
                        job_id=post_scope_mismatch_job,
                        datastore="DS1",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-outra.local",
                        total_items=1,
                        total_size_gb=8.0,
                        deletable_items=1,
                        deletable_size_gb=8.0,
                        breakdown={"ORPHANED": 1},
                    ),
                ]
            )
            await db.commit()

    asyncio.run(_seed_unknown_scope_pair())

    unknown_scope_pair = client.get("/api/v1/datastore-reports/verify-files/pair-unknown-scope")
    assert unknown_scope_pair.status_code == 200, unknown_scope_pair.text
    unknown_data = unknown_scope_pair.json()
    assert unknown_data["datastore_status"] == "unknown"
    assert unknown_data["datastore_found_in_pre"] is True
    assert unknown_data["datastore_found_in_post"] is True
    assert unknown_data["deleted_files_count"] == 0
    assert unknown_data["deleted_vmdks"] == []

    async def _seed_pre_missing_pair() -> None:
        async with AsyncSessionLocal() as db:
            db.add_all(
                [
                    DatastoreDecommissionReport(
                        pair_id="pair-pre-missing",
                        phase="pre_delete",
                        job_id=pre_job_id,
                        datastore="DS_PRE_MISSING",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=1,
                        total_size_gb=1.0,
                        deletable_items=1,
                        deletable_size_gb=1.0,
                        breakdown={"ORPHANED": 1},
                    ),
                    DatastoreDecommissionReport(
                        pair_id="pair-pre-missing",
                        phase="post_delete",
                        job_id=post_job_id,
                        datastore="DS_PRE_MISSING",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=0,
                        total_size_gb=0.0,
                        deletable_items=0,
                        deletable_size_gb=0.0,
                        breakdown={},
                    ),
                ]
            )
            await db.commit()

    asyncio.run(_seed_pre_missing_pair())

    pre_missing_pair = client.get("/api/v1/datastore-reports/verify-files/pair-pre-missing")
    assert pre_missing_pair.status_code == 404
    assert "nao encontrado no job pre_delete" in pre_missing_pair.json()["detail"]


def test_verify_files_deduplicates_by_vmdk_path(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    async def _seed_duplicate_pre_path() -> None:
        async with AsyncSessionLocal() as db:
            vc_stmt = select(VCenter).where(VCenter.name == "zz_test_vc-report")
            vc = (await db.execute(vc_stmt)).scalar_one()
            db.add(
                ZombieVmdkRecord(
                    job_id=pre_job_id,
                    path="[DS1] vm-a/a.vmdk",
                    datastore="DS1",
                    tamanho_gb=10.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-dup"],
                    likely_causes=[],
                )
            )
            await db.commit()

    asyncio.run(_seed_duplicate_pre_path())

    pre_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    assert pre_resp.status_code == 201, pre_resp.text
    pre = pre_resp.json()

    post_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": pre["pair_id"],
        },
    )
    assert post_resp.status_code == 201, post_resp.text

    verify_resp = client.get(f"/api/v1/datastore-reports/verify-files/{pre['pair_id']}")
    assert verify_resp.status_code == 200, verify_resp.text
    data = verify_resp.json()
    assert data["deleted_files_count"] == 2
    assert data["deleted_size_gb"] == 12.0
    assert sorted(v["path"] for v in data["deleted_vmdks"]) == [
        "[DS1] vm-a/a.vmdk",
        "[DS1] vm-c/c.vmdk",
    ]


def test_verify_files_filters_by_tipo_and_min_size(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    pre_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    assert pre_resp.status_code == 201, pre_resp.text
    pair_id = pre_resp.json()["pair_id"]

    post_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": pair_id,
        },
    )
    assert post_resp.status_code == 201, post_resp.text

    filtered_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={
            "tipo_zombie": ["ORPHANED", "POSSIBLE_FALSE_POSITIVE"],
            "min_size_gb": 3,
            "page": 1,
            "page_size": 50,
        },
    )
    assert filtered_resp.status_code == 200, filtered_resp.text
    data = filtered_resp.json()
    assert data["deleted_files_count"] == 1
    assert data["deleted_size_gb"] == 10.0
    assert data["deleted_breakdown"] == {"ORPHANED": 1}
    assert len(data["deleted_vmdks"]) == 1
    assert data["deleted_vmdks"][0]["path"] == "[DS1] vm-a/a.vmdk"

    invalid_tipo_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={"tipo_zombie": "INVALID_TYPE"},
    )
    assert invalid_tipo_resp.status_code == 422
    assert "valores aceitos" in invalid_tipo_resp.json()["detail"].lower()


def test_verify_files_page_out_of_range_and_export_limit(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    pre_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": "DS1"},
    )
    assert pre_resp.status_code == 201, pre_resp.text
    pair_id = pre_resp.json()["pair_id"]

    post_resp = client.post(
        "/api/v1/datastore-reports/snapshots",
        json={
            "phase": "post_delete",
            "job_id": post_job_id,
            "datastore": "DS1",
            "pair_id": pair_id,
        },
    )
    assert post_resp.status_code == 201, post_resp.text

    out_of_range_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}",
        params={"page": 999, "page_size": 100},
    )
    assert out_of_range_resp.status_code == 200, out_of_range_resp.text
    data = out_of_range_resp.json()
    assert data["total_evidence"] == 2
    assert data["deleted_vmdks"] == []
    assert data["has_more_evidence"] is False

    export_overflow_resp = client.get(
        f"/api/v1/datastore-reports/verify-files/{pair_id}/export",
        params={"format": "json", "max_rows": 1},
    )
    assert export_overflow_resp.status_code == 422
    assert "excede max_rows" in export_overflow_resp.json()["detail"]


def test_post_exclusion_verification_marks_removed_datastore(client: TestClient):
    asyncio.run(_reset_db())
    pre_job_id, post_job_id, _ = asyncio.run(_seed_data())

    async def _seed_removed_pair() -> str:
        async with AsyncSessionLocal() as db:
            db.add(
                ZombieScanJob(
                    job_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                    vcenter_ids=[1],
                    datacenters=["DC1"],
                    datastores=["DS_POST_ONLY"],
                    status="completed",
                    started_at=datetime.now(timezone.utc) - timedelta(minutes=5),
                    finished_at=datetime.now(timezone.utc) - timedelta(minutes=4),
                    total_vmdks=1,
                    total_size_gb=1.0,
                )
            )
            db.add(
                ZombieVmdkRecord(
                    job_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                    path="[DS_POST_ONLY] vm-y/y.vmdk",
                    datastore="DS_POST_ONLY",
                    tamanho_gb=1.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host="zz_test_vc-report.local",
                    vcenter_name="zz_test_vc-report",
                    datacenter="DC1",
                    detection_rules=["rule-y"],
                    likely_causes=[],
                )
            )

            pair_id = "pair-datastore-removed"
            db.add_all(
                [
                    DatastoreDecommissionReport(
                        pair_id=pair_id,
                        phase="pre_delete",
                        job_id=pre_job_id,
                        datastore="DS1",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=3,
                        total_size_gb=17.0,
                        deletable_items=2,
                        deletable_size_gb=15.0,
                        breakdown={"ORPHANED": 1, "BROKEN_CHAIN": 1, "POSSIBLE_FALSE_POSITIVE": 1},
                    ),
                    DatastoreDecommissionReport(
                        pair_id=pair_id,
                        phase="post_delete",
                        job_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
                        datastore="DS1",
                        vcenter_name="zz_test_vc-report",
                        vcenter_host="zz_test_vc-report.local",
                        total_items=0,
                        total_size_gb=0.0,
                        deletable_items=0,
                        deletable_size_gb=0.0,
                        breakdown={},
                    ),
                ]
            )
            await db.commit()
            return pair_id

    pair_id = asyncio.run(_seed_removed_pair())
    resp = client.get(f"/api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}")
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["datastore_status"] == "removed"
    assert data["datastore_name"] == "DS1"
    assert data["datastore_found_in_pre"] is True
    assert data["datastore_found_in_post"] is False
    assert data["removed_files_count"] == 3
    assert data["removed_size_gb"] == 17.0
    assert data["verification_status"] == "fully_removed"
    assert data["deleted_files_count"] == 3
    assert data["deleted_size_gb"] == 17.0
    assert data["remaining_files_count"] == 0
    assert data["remaining_size_gb"] == 0.0


def test_verify_files_timeout_returns_504(client: TestClient, monkeypatch: pytest.MonkeyPatch):
    async def _slow_payload(*args, **kwargs):
        await asyncio.sleep(1.2)
        return None

    monkeypatch.setattr(datastore_reports_route, "_build_file_verification_payload", _slow_payload)

    timeout_resp = client.get(
        "/api/v1/datastore-reports/verify-files/pair-timeout",
        params={"timeout_sec": 1},
    )
    assert timeout_resp.status_code == 504
    detail = timeout_resp.json()["detail"].lower()
    assert "tempo limite" in detail
    assert "page_size" in detail


def test_datastore_deletion_verification_partial_cleanup(client: TestClient):
    asyncio.run(_reset_db())
    host, baseline_job_id, verification_job_id = asyncio.run(
        _seed_auto_datastore_deletion_case(status_case="partial_cleanup")
    )

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={
            "datastore": "DS_AUTO",
            "vcenter_host": host,
            "evidence_limit": 1,
        },
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "partial_cleanup"
    assert data["baseline_job_id"] == baseline_job_id
    assert data["verification_job_id"] == verification_job_id
    assert data["deleted_vmdk_count"] == 2
    assert data["remaining_vmdk_count"] == 1
    assert data["deleted_size_gb"] == 12.0
    assert data["remaining_size_gb"] == 5.0
    assert data["size_gain_percent"] == 70.59
    assert data["deleted_breakdown"]["ORPHANED"] == 1
    assert data["deleted_breakdown"]["SNAPSHOT_ORPHAN"] == 1
    assert len(data["deleted_vmdks"]) == 1


def test_datastore_deletion_verification_datastore_removed(client: TestClient):
    asyncio.run(_reset_db())
    _, baseline_job_id, _ = asyncio.run(
        _seed_auto_datastore_deletion_case(status_case="datastore_removed")
    )

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_AUTO"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "datastore_removed"
    assert data["baseline_job_id"] == baseline_job_id
    assert data["deleted_vmdk_count"] == 3
    assert data["remaining_vmdk_count"] == 0
    assert data["deleted_size_gb"] == 17.0
    assert data["remaining_size_gb"] == 0.0


def test_datastore_deletion_verification_no_cleanup(client: TestClient):
    asyncio.run(_reset_db())
    asyncio.run(_seed_auto_datastore_deletion_case(status_case="no_cleanup"))

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_AUTO"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "no_cleanup"
    assert data["deleted_vmdk_count"] == 0
    assert data["remaining_vmdk_count"] == 3
    assert data["deleted_size_gb"] == 0.0
    assert data["remaining_size_gb"] == 17.0


def test_datastore_deletion_verification_totals_no_duplicate_for_same_pair(client: TestClient):
    asyncio.run(_reset_db())
    host, _, _ = asyncio.run(_seed_auto_datastore_deletion_case(status_case="partial_cleanup"))

    first_resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_AUTO", "vcenter_host": host},
    )
    assert first_resp.status_code == 200, first_resp.text

    second_resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_AUTO", "vcenter_host": host},
    )
    assert second_resp.status_code == 200, second_resp.text

    scoped_totals_resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification/totals",
        params={"datastore": "DS_AUTO", "vcenter_host": host},
    )
    assert scoped_totals_resp.status_code == 200, scoped_totals_resp.text
    scoped = scoped_totals_resp.json()
    assert scoped["total_verifications"] == 1
    assert scoped["total_partial_cleanup"] == 1
    assert scoped["total_datastores_removed"] == 0
    assert scoped["total_no_cleanup"] == 0
    assert scoped["total_deleted_vmdks"] == 2
    assert scoped["total_deleted_size_gb"] == 12.0
    assert scoped["last_verification_at"] is not None

    global_totals_resp = client.get("/api/v1/datastore-reports/datastore-deletion-verification/totals")
    assert global_totals_resp.status_code == 200, global_totals_resp.text
    global_totals = global_totals_resp.json()
    assert global_totals["total_verifications"] == 1
    assert global_totals["total_deleted_vmdks"] == 2


def test_datastore_deletion_verification_errors_404(client: TestClient):
    asyncio.run(_reset_db())

    no_verification_resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_AUTO"},
    )
    assert no_verification_resp.status_code == 404
    assert "scan de verificacao" in no_verification_resp.json()["detail"].lower()

    async def _seed_only_verification() -> None:
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            vc = VCenter(
                name="zz_test_vc-only-verification",
                host="zz_test_vc-only-verification.local",
                port=443,
                username="administrator@vsphere.local",
                password="dummy",
                disable_ssl_verify=True,
                is_active=True,
            )
            db.add(vc)
            await db.flush()

            db.add(
                ZombieScanJob(
                    job_id="33333333-3333-3333-3333-333333333333",
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS_AUTO"],
                    status="completed",
                    started_at=now - timedelta(minutes=5),
                    finished_at=now - timedelta(minutes=4),
                    total_vmdks=1,
                    total_size_gb=1.0,
                    datastore_metrics=[{"datastore_name": "DS_AUTO", "files_found": 1, "zombies_found": 1}],
                )
            )
            db.add(
                ZombieVmdkRecord(
                    job_id="33333333-3333-3333-3333-333333333333",
                    path="[DS_AUTO] vm-only/only.vmdk",
                    datastore="DS_AUTO",
                    tamanho_gb=1.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-only"],
                    likely_causes=[],
                )
            )
            await db.commit()

    asyncio.run(_seed_only_verification())

    no_baseline_resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_AUTO"},
    )
    assert no_baseline_resp.status_code == 404
    assert "baseline anterior" in no_baseline_resp.json()["detail"].lower()


def test_datastore_deletion_verification_required_full_removed(client: TestClient):
    asyncio.run(_reset_db())

    async def _seed_full_removed() -> None:
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            vc = VCenter(
                name="zz_test_vc-required-full-removed",
                host="zz_test_vc-required-full-removed.local",
                port=443,
                username="administrator@vsphere.local",
                password="dummy",
                disable_ssl_verify=True,
                is_active=True,
            )
            db.add(vc)
            await db.flush()

            db.add_all(
                [
                    ZombieScanJob(
                        job_id="44444444-4444-4444-4444-444444444444",
                        vcenter_ids=[vc.id],
                        datacenters=["DC1"],
                        datastores=["DS_REQ_FULL"],
                        status="completed",
                        started_at=now - timedelta(minutes=20),
                        finished_at=now - timedelta(minutes=19),
                        total_vmdks=2,
                        total_size_gb=12.5,
                        datastore_metrics=[{"datastore_name": "DS_REQ_FULL", "files_found": 2}],
                    ),
                    ZombieScanJob(
                        job_id="55555555-5555-5555-5555-555555555555",
                        vcenter_ids=[vc.id],
                        datacenters=["DC1"],
                        datastores=[],
                        status="completed",
                        started_at=now - timedelta(minutes=10),
                        finished_at=now - timedelta(minutes=9),
                        total_vmdks=0,
                        total_size_gb=0.0,
                        datastore_metrics=[],
                    ),
                ]
            )

            db.add_all(
                [
                    ZombieVmdkRecord(
                        job_id="44444444-4444-4444-4444-444444444444",
                        path="[DS_REQ_FULL] vm-a/a.vmdk",
                        datastore="DS_REQ_FULL",
                        tamanho_gb=10.0,
                        tipo_zombie="ORPHANED",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-a"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id="44444444-4444-4444-4444-444444444444",
                        path="[DS_REQ_FULL] vm-b/b.vmdk",
                        datastore="DS_REQ_FULL",
                        tamanho_gb=2.5,
                        tipo_zombie="BROKEN_CHAIN",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-b"],
                        likely_causes=[],
                    ),
                ]
            )
            await db.commit()

    asyncio.run(_seed_full_removed())

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_REQ_FULL"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["datastore_removed"] is True
    assert data["status"] == "datastore_removed"
    assert data["deleted_size_gb"] == 12.5
    assert data["deleted_vmdk_count"] == 2
    assert data["remaining_vmdk_count"] == 0


def test_datastore_deletion_verification_required_partial_cleanup(client: TestClient):
    asyncio.run(_reset_db())

    async def _seed_partial_required() -> None:
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            vc = VCenter(
                name="zz_test_vc-required-partial",
                host="zz_test_vc-required-partial.local",
                port=443,
                username="administrator@vsphere.local",
                password="dummy",
                disable_ssl_verify=True,
                is_active=True,
            )
            db.add(vc)
            await db.flush()

            db.add_all(
                [
                    ZombieScanJob(
                        job_id="66666666-6666-6666-6666-666666666666",
                        vcenter_ids=[vc.id],
                        datacenters=["DC1"],
                        datastores=["DS_REQ_PARTIAL"],
                        status="completed",
                        started_at=now - timedelta(minutes=20),
                        finished_at=now - timedelta(minutes=19),
                        total_vmdks=2,
                        total_size_gb=15.0,
                        datastore_metrics=[{"datastore_name": "DS_REQ_PARTIAL", "files_found": 2}],
                    ),
                    ZombieScanJob(
                        job_id="77777777-7777-7777-7777-777777777777",
                        vcenter_ids=[vc.id],
                        datacenters=["DC1"],
                        datastores=["DS_REQ_PARTIAL"],
                        status="completed",
                        started_at=now - timedelta(minutes=10),
                        finished_at=now - timedelta(minutes=9),
                        total_vmdks=1,
                        total_size_gb=5.0,
                        datastore_metrics=[{"datastore_name": "DS_REQ_PARTIAL", "files_found": 1}],
                    ),
                ]
            )

            db.add_all(
                [
                    ZombieVmdkRecord(
                        job_id="66666666-6666-6666-6666-666666666666",
                        path="[DS_REQ_PARTIAL] vm-a/a.vmdk",
                        datastore="DS_REQ_PARTIAL",
                        tamanho_gb=10.0,
                        tipo_zombie="ORPHANED",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-a"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id="66666666-6666-6666-6666-666666666666",
                        path="[DS_REQ_PARTIAL] vm-b/b.vmdk",
                        datastore="DS_REQ_PARTIAL",
                        tamanho_gb=5.0,
                        tipo_zombie="BROKEN_CHAIN",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-b"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id="77777777-7777-7777-7777-777777777777",
                        path="[DS_REQ_PARTIAL] vm-b/b.vmdk",
                        datastore="DS_REQ_PARTIAL",
                        tamanho_gb=5.0,
                        tipo_zombie="BROKEN_CHAIN",
                        vcenter_host=vc.host,
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-b"],
                        likely_causes=[],
                    ),
                ]
            )
            await db.commit()

    asyncio.run(_seed_partial_required())

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_REQ_PARTIAL"},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "partial_cleanup"
    assert data["deleted_vmdk_count"] == 1
    assert data["remaining_vmdk_count"] == 1


def test_datastore_deletion_verification_accepts_host_without_port(client: TestClient):
    asyncio.run(_reset_db())

    async def _seed_host_port_mismatch_case() -> str:
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            vc = VCenter(
                name="zz_test_vc-host-port",
                host="zz-test-host-port.local",
                port=443,
                username="administrator@vsphere.local",
                password="dummy",
                disable_ssl_verify=True,
                is_active=True,
            )
            db.add(vc)
            await db.flush()

            baseline_job_id = "77777777-7777-7777-7777-777777777777"
            verification_job_id = "88888888-8888-8888-8888-888888888888"

            db.add_all(
                [
                    ZombieScanJob(
                        job_id=baseline_job_id,
                        vcenter_ids=[vc.id],
                        datacenters=["DC1"],
                        datastores=["DS_HOST_PORT"],
                        status="completed",
                        started_at=now - timedelta(minutes=20),
                        finished_at=now - timedelta(minutes=19),
                        total_vmdks=2,
                        total_size_gb=12.0,
                        datastore_metrics=[{"datastore_name": "DS_HOST_PORT", "files_found": 2}],
                    ),
                    ZombieScanJob(
                        job_id=verification_job_id,
                        vcenter_ids=[vc.id],
                        datacenters=["DC1"],
                        datastores=["DS_HOST_PORT"],
                        status="completed",
                        started_at=now - timedelta(minutes=10),
                        finished_at=now - timedelta(minutes=9),
                        total_vmdks=1,
                        total_size_gb=5.0,
                        datastore_metrics=[{"datastore_name": "DS_HOST_PORT", "files_found": 1}],
                    ),
                ]
            )

            db.add_all(
                [
                    ZombieVmdkRecord(
                        job_id=baseline_job_id,
                        path="[DS_HOST_PORT] vm-a/a.vmdk",
                        datastore="DS_HOST_PORT",
                        tamanho_gb=7.0,
                        tipo_zombie="ORPHANED",
                        vcenter_host="zz-test-host-port.local:443",
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-a"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id=baseline_job_id,
                        path="[DS_HOST_PORT] vm-b/b.vmdk",
                        datastore="DS_HOST_PORT",
                        tamanho_gb=5.0,
                        tipo_zombie="BROKEN_CHAIN",
                        vcenter_host="zz-test-host-port.local:443",
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-b"],
                        likely_causes=[],
                    ),
                    ZombieVmdkRecord(
                        job_id=verification_job_id,
                        path="[DS_HOST_PORT] vm-b/b.vmdk",
                        datastore="DS_HOST_PORT",
                        tamanho_gb=5.0,
                        tipo_zombie="BROKEN_CHAIN",
                        vcenter_host="zz-test-host-port.local:443",
                        vcenter_name=vc.name,
                        datacenter="DC1",
                        detection_rules=["rule-b"],
                        likely_causes=[],
                    ),
                ]
            )
            await db.commit()
            return vc.host

    host = asyncio.run(_seed_host_port_mismatch_case())

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_HOST_PORT", "vcenter_host": host},
    )
    assert resp.status_code == 200, resp.text
    data = resp.json()
    assert data["status"] == "partial_cleanup"
    assert data["deleted_vmdk_count"] == 1
    assert data["deleted_size_gb"] == 7.0


def test_datastore_deletion_verification_required_no_baseline_404(client: TestClient):
    asyncio.run(_reset_db())

    async def _seed_only_verification_required() -> None:
        now = datetime.now(timezone.utc)
        async with AsyncSessionLocal() as db:
            vc = VCenter(
                name="zz_test_vc-required-no-baseline",
                host="zz_test_vc-required-no-baseline.local",
                port=443,
                username="administrator@vsphere.local",
                password="dummy",
                disable_ssl_verify=True,
                is_active=True,
            )
            db.add(vc)
            await db.flush()

            db.add(
                ZombieScanJob(
                    job_id="88888888-8888-8888-8888-888888888888",
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=["DS_REQ_NO_BASELINE"],
                    status="completed",
                    started_at=now - timedelta(minutes=5),
                    finished_at=now - timedelta(minutes=4),
                    total_vmdks=1,
                    total_size_gb=3.0,
                    datastore_metrics=[{"datastore_name": "DS_REQ_NO_BASELINE", "files_found": 1}],
                )
            )
            db.add(
                ZombieVmdkRecord(
                    job_id="88888888-8888-8888-8888-888888888888",
                    path="[DS_REQ_NO_BASELINE] vm-a/a.vmdk",
                    datastore="DS_REQ_NO_BASELINE",
                    tamanho_gb=3.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-a"],
                    likely_causes=[],
                )
            )
            await db.commit()

    asyncio.run(_seed_only_verification_required())

    resp = client.get(
        "/api/v1/datastore-reports/datastore-deletion-verification",
        params={"datastore": "DS_REQ_NO_BASELINE"},
    )
    assert resp.status_code == 404
