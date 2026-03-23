from __future__ import annotations

import asyncio

from fastapi.testclient import TestClient
from sqlalchemy import delete

from app.dependencies import get_current_user
from app.models.base import AsyncSessionLocal, init_db
from app.models.vcenter import VCenter
import app.api.routes.capacity as capacity_module
import main as main_module

app = main_module.app


async def _noop_scheduler_start() -> None:
    return None


def _noop_scheduler_stop() -> None:
    return None


main_module.scheduler_start = _noop_scheduler_start
main_module.scheduler_stop = _noop_scheduler_stop


def _auth_override() -> dict:
    return {"sub": "capacity-tester", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(VCenter).where(VCenter.name.like("zz_test_capacity_%")))
        await db.commit()


async def _seed_vcenter(*, is_active: bool = True) -> int:
    async with AsyncSessionLocal() as db:
        vc = VCenter(
            name="zz_test_capacity_vc",
            host="zz-test-capacity.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy-secret",
            disable_ssl_verify=True,
            is_active=is_active,
        )
        db.add(vc)
        await db.commit()
        await db.refresh(vc)
        return vc.id


def _install_connection_stubs(monkeypatch) -> None:
    monkeypatch.setattr(capacity_module.connection_manager, "register", lambda _vc: None)
    monkeypatch.setattr(capacity_module.vcenter_pool, "get_service_instance", lambda _vc_id: object())


def _vm(name: str, size_gb: float) -> dict:
    return {
        "name": name,
        "path": f"[DS] {name}/{name}.vmdk",
        "committed_gb": size_gb,
        "sdrs_policy": {
            "has_independent_disk": False,
            "vm_override_mode": "fullyautomated",
            "keep_vmdks_together": True,
        },
    }


def _ds(
    name: str,
    *,
    use_pct: float,
    free_gb: float,
    used_gb: float,
    capacity_gb: float = 100.0,
    top_vms: list[dict] | None = None,
    accessible: bool = True,
    in_datastore_cluster: bool = True,
    datastore_cluster: str = "POD_A",
    datastore_type: str = "VMFS",
) -> dict:
    return {
        "name": name,
        "accessible": accessible,
        "capacity_gb": capacity_gb,
        "free_gb": free_gb,
        "used_gb": used_gb,
        "use_pct": use_pct,
        "health_status": "critical" if use_pct >= 85 else ("warning" if use_pct >= 70 else "healthy"),
        "in_datastore_cluster": in_datastore_cluster,
        "datastore_cluster": datastore_cluster,
        "datastore_type": datastore_type,
        "top_vms": top_vms or [],
    }


def test_capacity_report_returns_payload(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    async def _fake_get_capacity_report(_si, *_args, **_kwargs):
        return {
            "summary": {
                "total_capacity_gb": 300.0,
                "total_free_gb": 120.0,
                "total_used_gb": 180.0,
                "healthy": 2,
                "warning": 1,
                "critical": 0,
            },
            "datastores": [
                _ds("DS_A", use_pct=50.0, free_gb=50.0, used_gb=50.0),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/capacity/report?vcenter_id={vc_id}")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["vcenter_id"] == vc_id
            assert body["vcenter"] == "zz_test_capacity_vc"
            assert body["summary"]["total_capacity_gb"] == 300.0
            assert len(body["datastores"]) == 1
            assert body["datastores"][0]["name"] == "DS_A"
    finally:
        app.dependency_overrides.clear()


def test_sdrs_recommendations_generates_moves_and_scope(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    async def _fake_get_capacity_report(_si, *_args, **_kwargs):
        return {
            "summary": {},
            "datastores": [
                _ds("DS_HOT", use_pct=90.0, free_gb=10.0, used_gb=90.0, top_vms=[_vm("vm-heavy-01", 8.0), _vm("vm-heavy-02", 4.0)]),
                _ds("DS_COLD", use_pct=40.0, free_gb=60.0, used_gb=40.0),
                _ds("DS_WARM", use_pct=75.0, free_gb=25.0, used_gb=75.0),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(
                (
                    f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}"
                    "&datastores=DS_HOT,DS_COLD,DS_WARM&utilization_threshold_pct=80&max_moves=10"
                )
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["mode"] == "recommend_only"
            assert body["summary"]["datastores_considered"] == 3
            assert body["summary"]["sources_over_threshold"] == 1
            assert body["summary"]["recommendations"] >= 1
            assert body["scope"]["selected_datastores"] == ["ds_cold", "ds_hot", "ds_warm"]
            assert body["recommendations"][0]["source_datastore"] == "DS_HOT"
            assert body["recommendations"][0]["target_datastore"] in {"DS_COLD", "DS_WARM"}
            assert body["recommendations"][0]["reason_code"] == "SOURCE_OVER_THRESHOLD"
            assert body["recommendations"][0]["explanation_text"]
    finally:
        app.dependency_overrides.clear()


def test_sdrs_recommendations_reports_blocked_source(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    async def _fake_get_capacity_report(_si, *_args, **_kwargs):
        return {
            "summary": {},
            "datastores": [
                _ds("DS_SOURCE", use_pct=95.0, free_gb=5.0, used_gb=95.0, top_vms=[_vm("vm-only", 10.0)]),
                _ds("DS_TIGHT", use_pct=98.0, free_gb=2.0, used_gb=98.0),
                _ds("DS_COLD", use_pct=10.0, free_gb=90.0, used_gb=10.0, accessible=False),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(
                (
                    f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}"
                    "&datastores=DS_SOURCE,DS_TIGHT,DS_COLD&utilization_threshold_pct=80&max_moves=10"
                )
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["summary"]["recommendations"] == 0
            assert body["summary"]["blocked_sources"] >= 1
            assert any(row["source_datastore"] == "DS_SOURCE" for row in body["blocked_sources"])
            assert any(row.get("reason_code") == "NO_ELIGIBLE_TARGET" for row in body["blocked_sources"])
    finally:
        app.dependency_overrides.clear()


def test_capacity_report_applies_datacenter_scope(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    calls: dict[str, object] = {}

    async def _fake_get_capacity_report(_si, *args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return {
            "summary": {"total_capacity_gb": 0.0, "total_free_gb": 0.0, "total_used_gb": 0.0},
            "datastores": [],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/capacity/report?vcenter_id={vc_id}&datacenter=DTC-SGI")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["datacenter_scope"] == "DTC-SGI"
            assert calls["args"] == ("DTC-SGI",)
    finally:
        app.dependency_overrides.clear()


def test_sdrs_recommendations_exposes_datacenter_scope(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    calls: dict[str, object] = {}

    async def _fake_get_capacity_report(_si, *args, **kwargs):
        calls["args"] = args
        calls["kwargs"] = kwargs
        return {
            "summary": {},
            "datastores": [
                _ds("DS_A", use_pct=50.0, free_gb=50.0, used_gb=50.0),
                _ds("DS_B", use_pct=60.0, free_gb=40.0, used_gb=60.0),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(
                f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}&datacenter=DTC-SGI"
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["scope"]["datacenter"] == "DTC-SGI"
            assert calls["args"] == ("DTC-SGI",)
            assert "modes" in body
    finally:
        app.dependency_overrides.clear()


def test_sdrs_execution_mode_blocked_when_readonly(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(
                f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}&mode=execution"
            )
            assert resp.status_code == 403
            assert "READONLY_MODE=true" in resp.text
    finally:
        app.dependency_overrides.clear()

def test_sdrs_blocks_datastores_outside_cluster(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    async def _fake_get_capacity_report(_si, *_args, **_kwargs):
        return {
            "summary": {},
            "datastores": [
                _ds("DS_A", use_pct=88.0, free_gb=12.0, used_gb=88.0, in_datastore_cluster=False, top_vms=[_vm("vm-a", 5.0)]),
                _ds("DS_B", use_pct=30.0, free_gb=70.0, used_gb=30.0, in_datastore_cluster=False),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["summary"]["recommendations"] == 0
            assert any(row["reason_code"] == "NOT_IN_DATASTORE_CLUSTER" for row in body["blocked_sources"])
    finally:
        app.dependency_overrides.clear()


def test_sdrs_blocks_mixed_datastore_types(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    async def _fake_get_capacity_report(_si, *_args, **_kwargs):
        return {
            "summary": {},
            "datastores": [
                _ds("DS_VMFS", use_pct=90.0, free_gb=10.0, used_gb=90.0, datastore_type="VMFS", top_vms=[_vm("vm-a", 5.0)]),
                _ds("DS_NFS", use_pct=40.0, free_gb=60.0, used_gb=40.0, datastore_type="NFS"),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["summary"]["recommendations"] == 0
            assert any(row["reason_code"] == "MIXED_DATASTORE_TYPES" for row in body["blocked_sources"])
    finally:
        app.dependency_overrides.clear()


def test_sdrs_approve_endpoint_returns_approval_state(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/capacity/sdrs/approve",
                json={
                    "vcenter_id": vc_id,
                    "datacenter": "DTC-SGI",
                    "recommendation_ids": ["rec-1", "rec-2"],
                    "comment": "ok",
                },
            )
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert body["mode"] == "approval"
            assert body["status"] == "approved_for_execution"
            assert body["approved_count"] == 2
            assert body["execution_enabled"] is False
    finally:
        app.dependency_overrides.clear()

def test_sdrs_recommendations_exposes_decisions_and_audit_trail(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())
    _install_connection_stubs(monkeypatch)

    async def _fake_get_capacity_report(_si, *_args, **_kwargs):
        return {
            "summary": {},
            "datastores": [
                _ds("DS_HOT", use_pct=90.0, free_gb=10.0, used_gb=90.0, datastore_cluster="POD_A", top_vms=[_vm("vm-heavy-01", 8.0)]),
                _ds("DS_COLD", use_pct=30.0, free_gb=70.0, used_gb=30.0, datastore_cluster="POD_A"),
            ],
            "recommendations": [],
        }

    monkeypatch.setattr(capacity_module.balance_service, "get_capacity_report", _fake_get_capacity_report)

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.get(f"/api/v1/capacity/sdrs/recommendations?vcenter_id={vc_id}&datacenter=DTC-SGI")
            assert resp.status_code == 200, resp.text
            body = resp.json()
            assert "decisions" in body
            assert "audit_trail" in body
            assert isinstance(body["decisions"], list)
            assert isinstance(body["audit_trail"], list)
            assert body["decisions"]
            assert body["decisions"][0].get("source_datastore_cluster") == "POD_A"
            assert "audit_payload" in body["decisions"][0]
            assert body["audit_trail"][0].get("decision_index") == 1
    finally:
        app.dependency_overrides.clear()

def test_sdrs_approve_requires_recommendation_ids(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/capacity/sdrs/approve",
                json={
                    "vcenter_id": vc_id,
                    "datacenter": "DTC-SGI",
                    "recommendation_ids": [],
                    "comment": "empty",
                },
            )
            assert resp.status_code == 422, resp.text
            assert "recommendation_ids" in resp.text
    finally:
        app.dependency_overrides.clear()


def test_sdrs_execute_endpoint_blocked_when_readonly(monkeypatch):
    asyncio.run(_reset_db())
    vc_id = asyncio.run(_seed_vcenter())

    app.dependency_overrides[get_current_user] = _auth_override
    try:
        with TestClient(app) as client:
            resp = client.post(
                "/api/v1/capacity/sdrs/execute",
                json={
                    "vcenter_id": vc_id,
                    "datacenter": "DTC-SGI",
                    "recommendation_ids": ["rec-1"],
                    "comment": "try execute",
                },
            )
            assert resp.status_code == 403, resp.text
            assert "READONLY_MODE=true" in resp.text
    finally:
        app.dependency_overrides.clear()
