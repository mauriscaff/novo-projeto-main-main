from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter.db"

from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.api.routes import approvals as approvals_route
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
    return {"sub": "governance-tester", "method": "override"}


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


async def _seed_delete_token(*, token_value: str) -> None:
    now = datetime.now(timezone.utc)
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
        await db.flush()

        token = ApprovalToken(
            token=token_value,
            vmdk_path="[DS1] vm-a/a.vmdk",
            vcenter_id=str(vc.id),
            action="DELETE",
            analyst="governance-tester",
            justification="Token de teste para validar governanca com snapshot obrigatorio.",
            issued_at=now,
            expires_at=now + timedelta(hours=4),
            status="dryrun_done",
            vmdk_datacenter="DC1",
        )
        db.add(token)
        await db.commit()


async def _latest_audit(status_value: str) -> AuditLog | None:
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(AuditLog)
            .where(AuditLog.status == status_value)
            .order_by(AuditLog.id.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()


def test_governance_blocks_delete_without_datastore_snapshot(monkeypatch):
    asyncio.run(_reset_db())
    asyncio.run(_seed_delete_token(token_value="gov-token-missing-snapshot"))

    app.dependency_overrides[get_current_user] = _auth_override
    monkeypatch.setattr(approvals_route.settings, "readonly_mode", False)
    monkeypatch.setattr(
        approvals_route.settings,
        "governance_require_datastore_snapshot_for_delete",
        True,
    )

    with TestClient(app) as client:
        resp = client.post("/api/v1/approvals/gov-token-missing-snapshot/execute")
        assert resp.status_code == 428, resp.text
        assert "snapshot" in resp.json()["detail"].lower()

    audit = asyncio.run(_latest_audit("blocked_missing_ds_report"))
    assert audit is not None
    assert audit.analyst == "governance-tester"
    assert audit.action == "DELETE"

    app.dependency_overrides.clear()


def test_governance_keeps_readonly_precedence(monkeypatch):
    asyncio.run(_reset_db())
    asyncio.run(_seed_delete_token(token_value="gov-token-readonly"))

    app.dependency_overrides[get_current_user] = _auth_override
    monkeypatch.setattr(approvals_route.settings, "readonly_mode", True)
    monkeypatch.setattr(
        approvals_route.settings,
        "governance_require_datastore_snapshot_for_delete",
        True,
    )

    with TestClient(app) as client:
        resp = client.post("/api/v1/approvals/gov-token-readonly/execute")
        assert resp.status_code == 403, resp.text
        assert "READONLY_MODE=true" in resp.json()["detail"]

    audit = asyncio.run(_latest_audit("blocked_readonly"))
    assert audit is not None
    assert audit.analyst == "governance-tester"
    assert audit.action == "DELETE"

    app.dependency_overrides.clear()
