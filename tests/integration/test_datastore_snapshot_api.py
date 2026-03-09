from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.dependencies import get_current_user
from app.models.audit_log import ApprovalToken, AuditLog
from app.models.base import AsyncSessionLocal, init_db
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
    return {"sub": "integration-tester", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(AuditLog))
        await db.execute(delete(ApprovalToken))
        await db.execute(delete(DatastoreDecomSnapshot))
        await db.execute(delete(ZombieVmdkRecord))
        await db.execute(delete(ZombieScanJob))
        await db.execute(delete(VCenter).where(VCenter.name.like("zz_test_%")))
        await db.commit()


async def _seed_snapshot_data() -> int:
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as db:
        vc_prod = VCenter(
            name="zz_test_vc-prod",
            host="zz_test_vc-prod.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        vc_other = VCenter(
            name="zz_test_vc-other",
            host="zz_test_vc-other.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        db.add_all([vc_prod, vc_other])
        await db.flush()
        await db.refresh(vc_prod)
        await db.refresh(vc_other)

        job_old = ZombieScanJob(
            job_id="11111111-1111-1111-1111-111111111111",
            vcenter_ids=[vc_prod.id],
            datacenters=["DC1"],
            datastores=None,
            status="completed",
            started_at=now - timedelta(hours=3),
            finished_at=now - timedelta(hours=2),
            total_vmdks=1,
            total_size_gb=99.0,
        )
        job_latest = ZombieScanJob(
            job_id="22222222-2222-2222-2222-222222222222",
            vcenter_ids=[vc_prod.id],
            datacenters=["DC1", "DC2"],
            datastores=None,
            status="completed",
            started_at=now - timedelta(hours=1),
            finished_at=now - timedelta(minutes=30),
            total_vmdks=4,
            total_size_gb=19.5,
        )
        job_other = ZombieScanJob(
            job_id="33333333-3333-3333-3333-333333333333",
            vcenter_ids=[vc_other.id],
            datacenters=["DCX"],
            datastores=None,
            status="completed",
            started_at=now - timedelta(minutes=20),
            finished_at=now - timedelta(minutes=10),
            total_vmdks=1,
            total_size_gb=500.0,
        )
        db.add_all([job_old, job_latest, job_other])

        db.add(
            ZombieVmdkRecord(
                job_id=job_old.job_id,
                path="[DS1] old/legacy.vmdk",
                datastore="DS1",
                tamanho_gb=99.0,
                tipo_zombie="ORPHANED",
                vcenter_host=vc_prod.host,
                vcenter_name=vc_prod.name,
                datacenter="DC1",
                detection_rules=["legacy"],
                likely_causes=[],
            )
        )

        db.add_all(
            [
                ZombieVmdkRecord(
                    job_id=job_latest.job_id,
                    path="[DS1] vm-a/a.vmdk",
                    datastore="DS1",
                    tamanho_gb=10.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc_prod.host,
                    vcenter_name=vc_prod.name,
                    datacenter="DC1",
                    detection_rules=["rule-a"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=job_latest.job_id,
                    path="[DS1] vm-b/b.vmdk",
                    datastore="DS1",
                    tamanho_gb=5.5,
                    tipo_zombie="BROKEN_CHAIN",
                    vcenter_host=vc_prod.host,
                    vcenter_name=vc_prod.name,
                    datacenter="DC1",
                    detection_rules=["rule-b"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=job_latest.job_id,
                    path="[DS1] vm-c/c.vmdk",
                    datastore="DS1",
                    tamanho_gb=3.0,
                    tipo_zombie="SNAPSHOT_ORPHAN",
                    vcenter_host=vc_prod.host,
                    vcenter_name=vc_prod.name,
                    datacenter="DC2",
                    detection_rules=["rule-c"],
                    likely_causes=[],
                ),
                ZombieVmdkRecord(
                    job_id=job_latest.job_id,
                    path="[DS2] vm-d/d.vmdk",
                    datastore="DS2",
                    tamanho_gb=1.0,
                    tipo_zombie="ORPHANED",
                    vcenter_host=vc_prod.host,
                    vcenter_name=vc_prod.name,
                    datacenter="DC1",
                    detection_rules=["rule-d"],
                    likely_causes=[],
                ),
            ]
        )

        db.add(
            ZombieVmdkRecord(
                job_id=job_other.job_id,
                path="[DS1] foreign/x.vmdk",
                datastore="DS1",
                tamanho_gb=500.0,
                tipo_zombie="ORPHANED",
                vcenter_host=vc_other.host,
                vcenter_name=vc_other.name,
                datacenter="DCX",
                detection_rules=["rule-x"],
                likely_causes=[],
            )
        )

        await db.commit()
        return vc_prod.id


async def _seed_running_job() -> str:
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as db:
        job = ZombieScanJob(
            job_id="44444444-4444-4444-4444-444444444444",
            vcenter_ids=[],
            datacenters=[],
            datastores=[],
            status="running",
            started_at=now - timedelta(minutes=2),
            finished_at=None,
            total_vmdks=0,
            total_size_gb=0.0,
        )
        db.add(job)
        await db.commit()
        return job.job_id


def _create_snapshot(client: TestClient, vc_id: int, datacenter: str | None = None) -> dict:
    payload = {
        "vcenter_id": vc_id,
        "datastore_name": "DS1",
    }
    if datacenter is not None:
        payload["datacenter"] = datacenter
    response = client.post("/api/v1/scan/datastore-snapshots", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


def _create_laudo(client: TestClient, vc_id: int, datacenter: str | None = None) -> dict:
    payload = {
        "vcenter_id": vc_id,
        "datastore_name": "DS1",
    }
    if datacenter is not None:
        payload["datacenter"] = datacenter
    response = client.post("/api/v1/scan/datastore-laudos", json=payload)
    assert response.status_code == 201, response.text
    return response.json()


@pytest.fixture(scope="module")
def client() -> TestClient:
    app.dependency_overrides[get_current_user] = _auth_override
    with TestClient(app) as test_client:
        yield test_client
    app.dependency_overrides.clear()


def test_create_snapshot_with_optional_datacenter_filter(client: TestClient):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_snapshot_data())

    data_all_dc = _create_snapshot(client, vc_id)
    assert data_all_dc["total_itens"] == 3
    assert data_all_dc["total_size_gb"] == 18.5
    assert data_all_dc["breakdown"]["ORPHANED"] == 1
    assert data_all_dc["breakdown"]["BROKEN_CHAIN"] == 1
    assert data_all_dc["breakdown"]["SNAPSHOT_ORPHAN"] == 1
    assert data_all_dc["conclusao"] == "base para auditoria pós-descomissionamento"

    data_dc1 = _create_snapshot(client, vc_id, datacenter="DC1")
    assert data_dc1["total_itens"] == 2
    assert data_dc1["total_size_gb"] == 15.5
    assert data_dc1["breakdown"]["ORPHANED"] == 1
    assert data_dc1["breakdown"]["BROKEN_CHAIN"] == 1
    assert data_dc1["breakdown"]["SNAPSHOT_ORPHAN"] == 0
    assert data_dc1["conclusao"] == "base para auditoria pós-descomissionamento"


def test_get_snapshot_by_id_and_export_formats(client: TestClient):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_snapshot_data())

    created = _create_snapshot(client, vc_id, datacenter="DC1")
    snapshot_id = created["id"]

    get_resp = client.get(f"/api/v1/scan/datastore-snapshots/{snapshot_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == snapshot_id
    assert get_resp.json()["conclusao"] == "base para auditoria pós-descomissionamento"

    csv_resp = client.get(
        f"/api/v1/scan/datastore-snapshots/{snapshot_id}/export?format=csv"
    )
    assert csv_resp.status_code == 200
    assert "text/csv" in csv_resp.headers["content-type"]
    assert "snapshot_id" in csv_resp.text
    assert "conclusao" in csv_resp.text
    assert "ORPHANED" in csv_resp.text
    assert "base para auditoria pós-descomissionamento" in csv_resp.text

    json_resp = client.get(
        f"/api/v1/scan/datastore-snapshots/{snapshot_id}/export?format=json"
    )
    assert json_resp.status_code == 200
    assert "application/json" in json_resp.headers["content-type"]
    assert json_resp.json()["id"] == snapshot_id
    assert json_resp.json()["conclusao"] == "base para auditoria pós-descomissionamento"


def test_laudo_alias_endpoints_behave_like_snapshot(client: TestClient):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_snapshot_data())

    created = _create_laudo(client, vc_id, datacenter="DC1")
    snapshot_id = created["id"]
    assert created["datastore_name"] == "DS1"
    assert created["total_itens"] == 2
    assert created["total_size_gb"] == 15.5
    assert created["conclusao"] == "base para auditoria pós-descomissionamento"

    get_resp = client.get(f"/api/v1/scan/datastore-laudos/{snapshot_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == snapshot_id
    assert get_resp.json()["conclusao"] == "base para auditoria pós-descomissionamento"

    csv_resp = client.get(f"/api/v1/scan/datastore-laudos/{snapshot_id}/export?format=csv")
    assert csv_resp.status_code == 200
    assert "text/csv" in csv_resp.headers["content-type"]
    assert "conclusao" in csv_resp.text
    assert "base para auditoria pós-descomissionamento" in csv_resp.text

    json_resp = client.get(f"/api/v1/scan/datastore-laudos/{snapshot_id}/export?format=json")
    assert json_resp.status_code == 200
    assert "application/json" in json_resp.headers["content-type"]
    assert json_resp.json()["id"] == snapshot_id
    assert json_resp.json()["conclusao"] == "base para auditoria pós-descomissionamento"


def test_snapshot_endpoints_return_clear_errors(client: TestClient):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_snapshot_data())

    not_found_vc = client.post(
        "/api/v1/scan/datastore-snapshots",
        json={"vcenter_id": "vc-inexistente", "datastore_name": "DS1"},
    )
    assert not_found_vc.status_code == 404

    not_found_ds = client.post(
        "/api/v1/scan/datastore-snapshots",
        json={"vcenter_id": vc_id, "datastore_name": "DS_DOES_NOT_EXIST"},
    )
    assert not_found_ds.status_code == 404

    created = _create_snapshot(client, vc_id, datacenter="DC1")
    invalid_format = client.get(
        f"/api/v1/scan/datastore-snapshots/{created['id']}/export?format=xml"
    )
    assert invalid_format.status_code == 422

    unknown_id = client.get("/api/v1/scan/datastore-snapshots/999999")
    assert unknown_id.status_code == 404


def test_generate_executive_report_markdown(client: TestClient):
    asyncio.run(_reset_db())
    asyncio.run(_seed_snapshot_data())

    resp = client.get(
        "/api/v1/scan/jobs/22222222-2222-2222-2222-222222222222/executive-report",
        params={"datastore_name": "DS1", "datacenter": "DC1"},
    )
    assert resp.status_code == 200, resp.text
    assert "text/markdown" in resp.headers["content-type"]
    assert "attachment; filename=" in resp.headers.get("content-disposition", "")
    assert "## Objetivo da analise" in resp.text
    assert "## Datastore analisado" in resp.text
    assert "## Volumetria total (GB)" in resp.text
    assert "## Quantidade de itens por tipo" in resp.text
    assert "| ORPHANED | 1 |" in resp.text
    assert "| BROKEN_CHAIN | 1 |" in resp.text
    assert "| **Total** | **2** |" in resp.text
    assert "## Conclusao" in resp.text
    assert "base para auditoria pós-descomissionamento" in resp.text
    assert "## Evidencias tecnicas" in resp.text


def test_executive_report_errors(client: TestClient):
    asyncio.run(_reset_db())
    asyncio.run(_seed_snapshot_data())
    running_job_id = asyncio.run(_seed_running_job())

    not_found_job = client.get(
        "/api/v1/scan/jobs/99999999-9999-9999-9999-999999999999/executive-report",
        params={"datastore_name": "DS1"},
    )
    assert not_found_job.status_code == 404

    running_job = client.get(
        f"/api/v1/scan/jobs/{running_job_id}/executive-report",
        params={"datastore_name": "DS1"},
    )
    assert running_job.status_code == 409

    not_found_ds = client.get(
        "/api/v1/scan/jobs/22222222-2222-2222-2222-222222222222/executive-report",
        params={"datastore_name": "DS_DOES_NOT_EXIST"},
    )
    assert not_found_ds.status_code == 404

    invalid_dc = client.get(
        "/api/v1/scan/jobs/22222222-2222-2222-2222-222222222222/executive-report",
        params={"datastore_name": "DS1", "datacenter": "   "},
    )
    assert invalid_dc.status_code == 422


def test_snapshot_generation_registers_audit_trail(client: TestClient):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_snapshot_data())

    created = _create_snapshot(client, vc_id, datacenter="DC1")
    assert created["generated_by"] == "integration-tester"

    async def _fetch_audit():
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(AuditLog)
                .where(AuditLog.action == "DATASTORE_SNAPSHOT")
                .order_by(AuditLog.id.desc())
                .limit(1)
            )
            return result.scalar_one_or_none()

    audit = asyncio.run(_fetch_audit())
    assert audit is not None
    assert audit.analyst == "integration-tester"
    assert audit.status == "generated_snapshot"
    assert "[DS1]" in audit.vmdk_path


def test_executive_report_registers_audit_trail(client: TestClient):
    asyncio.run(_reset_db())
    asyncio.run(_seed_snapshot_data())

    resp = client.get(
        "/api/v1/scan/jobs/22222222-2222-2222-2222-222222222222/executive-report",
        params={"datastore_name": "DS1"},
    )
    assert resp.status_code == 200

    async def _fetch_audit():
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(AuditLog)
                .where(AuditLog.action == "DATASTORE_REPORT_MD")
                .order_by(AuditLog.id.desc())
                .limit(1)
            )
            return result.scalar_one_or_none()

    audit = asyncio.run(_fetch_audit())
    assert audit is not None
    assert audit.analyst == "integration-tester"
    assert audit.status == "generated_report_md"
    assert "[DS1]" in audit.vmdk_path
