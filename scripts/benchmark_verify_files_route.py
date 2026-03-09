from __future__ import annotations

import asyncio
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import delete

os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./test_zombiehunter_bench.db"
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import main as main_module  # noqa: E402
from app.dependencies import get_current_user  # noqa: E402
from app.models.audit_log import ApprovalToken, AuditLog  # noqa: E402
from app.models.base import AsyncSessionLocal, init_db  # noqa: E402
from app.models.datastore_report_snapshot import DatastoreDecommissionReport  # noqa: E402
from app.models.datastore_snapshot import DatastoreDecomSnapshot  # noqa: E402
from app.models.vcenter import VCenter  # noqa: E402
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord  # noqa: E402


async def _noop_scheduler_start() -> None:
    return None


def _noop_scheduler_stop() -> None:
    return None


main_module.scheduler_start = _noop_scheduler_start
main_module.scheduler_stop = _noop_scheduler_stop
app = main_module.app


def _auth_override() -> dict:
    return {"sub": "bench-reporter", "method": "override"}


async def _reset_db() -> None:
    await init_db()
    async with AsyncSessionLocal() as db:
        await db.execute(delete(AuditLog))
        await db.execute(delete(ApprovalToken))
        await db.execute(delete(DatastoreDecommissionReport))
        await db.execute(delete(DatastoreDecomSnapshot))
        await db.execute(delete(ZombieVmdkRecord))
        await db.execute(delete(ZombieScanJob))
        await db.execute(delete(VCenter).where(VCenter.name.like("zz_test_%")))
        await db.commit()


async def _seed_data(total_pre: int = 6000, total_post: int = 3000) -> tuple[str, str, str]:
    now = datetime.now(timezone.utc)
    async with AsyncSessionLocal() as db:
        vc = VCenter(
            name="zz_test_vc-bench",
            host="zz_test_vc-bench.local",
            port=443,
            username="administrator@vsphere.local",
            password="dummy",
            disable_ssl_verify=True,
            is_active=True,
        )
        db.add(vc)
        await db.flush()

        pre_job_id = "11111111-1111-1111-1111-111111111111"
        post_job_id = "22222222-2222-2222-2222-222222222222"
        datastore = "DSBENCH"

        db.add_all(
            [
                ZombieScanJob(
                    job_id=pre_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=[datastore],
                    status="completed",
                    started_at=now - timedelta(minutes=30),
                    finished_at=now - timedelta(minutes=29),
                    total_vmdks=total_pre,
                    total_size_gb=float(total_pre),
                ),
                ZombieScanJob(
                    job_id=post_job_id,
                    vcenter_ids=[vc.id],
                    datacenters=["DC1"],
                    datastores=[datastore],
                    status="completed",
                    started_at=now - timedelta(minutes=20),
                    finished_at=now - timedelta(minutes=19),
                    total_vmdks=total_post,
                    total_size_gb=float(total_post),
                ),
            ]
        )

        types = [
            "ORPHANED",
            "BROKEN_CHAIN",
            "SNAPSHOT_ORPHAN",
            "UNREGISTERED_DIR",
            "POSSIBLE_FALSE_POSITIVE",
        ]

        pre_rows: list[ZombieVmdkRecord] = []
        for i in range(total_pre):
            pre_rows.append(
                ZombieVmdkRecord(
                    job_id=pre_job_id,
                    path=f"[{datastore}] vm-{i}/disk-{i}.vmdk",
                    datastore=datastore,
                    tamanho_gb=float((i % 20) + 1),
                    tipo_zombie=types[i % len(types)],
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-bench"],
                    likely_causes=[],
                )
            )

        post_rows: list[ZombieVmdkRecord] = []
        for i in range(total_post):
            post_rows.append(
                ZombieVmdkRecord(
                    job_id=post_job_id,
                    path=f"[{datastore}] vm-{i}/disk-{i}.vmdk",
                    datastore=datastore,
                    tamanho_gb=float((i % 20) + 1),
                    tipo_zombie=types[i % len(types)],
                    vcenter_host=vc.host,
                    vcenter_name=vc.name,
                    datacenter="DC1",
                    detection_rules=["rule-bench"],
                    likely_causes=[],
                )
            )

        db.add_all(pre_rows)
        db.add_all(post_rows)
        await db.commit()
        return pre_job_id, post_job_id, datastore


def main() -> None:
    app.dependency_overrides[get_current_user] = _auth_override
    try:
        asyncio.run(_reset_db())
        pre_job_id, post_job_id, datastore = asyncio.run(_seed_data())
        with TestClient(app) as client:
            pre = client.post(
                "/api/v1/datastore-reports/snapshots",
                json={"phase": "pre_delete", "job_id": pre_job_id, "datastore": datastore},
            )
            pre.raise_for_status()
            pair_id = pre.json()["pair_id"]
            post = client.post(
                "/api/v1/datastore-reports/snapshots",
                json={
                    "phase": "post_delete",
                    "job_id": post_job_id,
                    "datastore": datastore,
                    "pair_id": pair_id,
                },
            )
            post.raise_for_status()

            t0 = time.perf_counter()
            before = client.get(
                f"/api/v1/datastore-reports/verify-files/{pair_id}",
                params={"page": 1, "page_size": 1000},
            )
            before.raise_for_status()
            before_ms = (time.perf_counter() - t0) * 1000.0

            t1 = time.perf_counter()
            after = client.get(
                f"/api/v1/datastore-reports/verify-files/{pair_id}",
                params={
                    "page": 1,
                    "page_size": 200,
                    "tipo_zombie": "ORPHANED",
                    "min_size_gb": 8,
                },
            )
            after.raise_for_status()
            after_ms = (time.perf_counter() - t1) * 1000.0

            print(f"BENCHMARK_BEFORE_MS={before_ms:.2f}")
            print(f"BENCHMARK_AFTER_MS={after_ms:.2f}")
            print(f"BENCHMARK_PAIR_ID={pair_id}")
    finally:
        app.dependency_overrides.clear()


if __name__ == "__main__":
    main()
