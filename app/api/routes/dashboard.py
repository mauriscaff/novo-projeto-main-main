"""
Endpoint de dashboard consolidado.

  GET /api/v1/dashboard   Retorna métricas históricas e snapshot atual
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from datetime import datetime, timezone

from app.dependencies import get_current_user, get_db
from app.models.audit_log import ApprovalToken, TERMINAL_STATUSES
from app.models.vcenter import VCenter
from app.models.vmdk_whitelist import VmdkWhitelist
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from app.schemas.dashboard import (
    DashboardResponse,
    RecentVmdkEntry,
    TrendEntry,
    TypeBreakdownEntry,
    VCenterBreakdown,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/",
    response_model=DashboardResponse,
    summary="Dashboard de VMDKs zombie",
    description="""
Retorna métricas consolidadas para o dashboard:

- **Totais históricos**: todas as detecções já realizadas (inclui re-detecções entre varreduras)
- **Snapshot da última varredura**: VMDKs e tamanho do job mais recente
- **Breakdown por vCenter**: distribuição histórica por origem
- **Breakdown por tipo**: distribuição histórica por categoria de zombie
- **Tendência**: últimas 4 varreduras concluídas (cronológico decrescente)
- **Whitelist**: total de VMDKs marcados como seguros
    """,
)
async def get_dashboard(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DashboardResponse:

    # ── Totais all-time (zombie_vmdk_records) deduplicados por path ───────────
    # Subquery para pegar o tamanho de cada VMDK único (agrupando por path)
    unique_vmdks_subq = (
        select(
            ZombieVmdkRecord.path,
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.tipo_zombie,
            func.max(ZombieVmdkRecord.tamanho_gb).label("tamanho_gb")
        )
        .group_by(
            ZombieVmdkRecord.path, 
            ZombieVmdkRecord.vcenter_name, 
            ZombieVmdkRecord.tipo_zombie
        )
        .subquery()
    )

    totals_q = await db.execute(
        select(
            func.count().label("cnt"),
            func.coalesce(func.sum(unique_vmdks_subq.c.tamanho_gb), 0.0).label("size"),
        )
    )
    totals = totals_q.one()
    total_vmdks_all_time: int = totals.cnt or 0
    total_size_all_time_gb: float = round(float(totals.size), 3)

    # ── Total de jobs ─────────────────────────────────────────────────────────
    total_jobs_q = await db.execute(select(func.count()).select_from(ZombieScanJob))
    total_jobs: int = total_jobs_q.scalar_one() or 0

    # ── Data da última varredura concluída ────────────────────────────────────
    last_scan_q = await db.execute(
        select(func.max(ZombieScanJob.finished_at)).where(
            ZombieScanJob.status == "completed"
        )
    )
    last_scan_at = last_scan_q.scalar_one()

    # ── Snapshot da varredura mais recente ────────────────────────────────────
    latest_job_q = await db.execute(
        select(ZombieScanJob)
        .where(ZombieScanJob.status == "completed")
        .order_by(ZombieScanJob.finished_at.desc().nulls_last())
        .limit(1)
    )
    latest_job = latest_job_q.scalar_one_or_none()
    latest_job_id = latest_job.job_id if latest_job else None
    latest_vmdks = latest_job.total_vmdks or 0 if latest_job else 0
    latest_size_gb = round(latest_job.total_size_gb or 0.0, 3) if latest_job else 0.0

    # ── Breakdown por vCenter ─────────────────────────────────────────────────
    vc_rows = await db.execute(
        select(
            unique_vmdks_subq.c.vcenter_name,
            func.count().label("cnt"),
            func.coalesce(func.sum(unique_vmdks_subq.c.tamanho_gb), 0.0).label("size"),
        )
        .group_by(unique_vmdks_subq.c.vcenter_name)
        .order_by(func.count().desc())
    )
    by_vcenter = [
        VCenterBreakdown(
            vcenter=row.vcenter_name or "desconhecido",
            total_vmdks=row.cnt,
            size_gb=round(float(row.size), 3),
        )
        for row in vc_rows
    ]

    # ── Breakdown por tipo_zombie ─────────────────────────────────────────────
    type_rows = await db.execute(
        select(
            unique_vmdks_subq.c.tipo_zombie,
            func.count().label("cnt"),
            func.coalesce(func.sum(unique_vmdks_subq.c.tamanho_gb), 0.0).label("size"),
        )
        .group_by(unique_vmdks_subq.c.tipo_zombie)
        .order_by(func.count().desc())
    )
    by_type: dict[str, TypeBreakdownEntry] = {
        row.tipo_zombie: TypeBreakdownEntry(
            count=row.cnt,
            size_gb=round(float(row.size), 3),
        )
        for row in type_rows
    }

    # ── Tendência — últimas 4 varreduras concluídas ───────────────────────────
    trend_rows = await db.execute(
        select(ZombieScanJob)
        .where(ZombieScanJob.status == "completed")
        .order_by(ZombieScanJob.finished_at.desc().nulls_last())
        .limit(4)
    )
    trend_last_4 = [
        TrendEntry(
            job_id=j.job_id,
            finished_at=j.finished_at,
            total_vmdks=j.total_vmdks or 0,
            total_size_gb=round(j.total_size_gb or 0.0, 3),
            status=j.status,
        )
        for j in trend_rows.scalars()
    ]

    # ── Total de entradas na whitelist ────────────────────────────────────────
    wl_q = await db.execute(select(func.count()).select_from(VmdkWhitelist))
    total_whitelisted: int = wl_q.scalar_one() or 0

    # ── Aprovações pendentes e vCenters (para os cards do frontend) ───────────
    pending_q = await db.execute(
        select(func.count()).select_from(ApprovalToken).where(
            ApprovalToken.status.notin_(TERMINAL_STATUSES),
            ApprovalToken.expires_at > datetime.now(timezone.utc),
        )
    )
    pending_approvals: int = pending_q.scalar_one() or 0

    vc_count_q = await db.execute(
        select(func.count()).select_from(VCenter).where(VCenter.is_active.is_(True))
    )
    vcenter_count: int = vc_count_q.scalar_one() or 0

    # ── Últimos VMDKs detectados (tabela "VMDKs detectados recentemente") ──────
    recent_q = await db.execute(
        select(ZombieVmdkRecord)
        .order_by(ZombieVmdkRecord.created_at.desc().nulls_last())
        .limit(50)
    )
    recent_vmdks = [
        RecentVmdkEntry(
            path=r.path,
            vcenter_host=r.vcenter_host,
            tamanho_gb=r.tamanho_gb,
            tipo_zombie=r.tipo_zombie,
            created_at=r.created_at,
        )
        for r in recent_q.scalars().all()
    ]

    return DashboardResponse(
        total_vmdks_all_time=total_vmdks_all_time,
        total_size_all_time_gb=total_size_all_time_gb,
        total_jobs=total_jobs,
        last_scan_at=last_scan_at,
        latest_job_id=latest_job_id,
        latest_vmdks=latest_vmdks,
        latest_size_gb=latest_size_gb,
        by_vcenter=by_vcenter,
        by_type=by_type,
        trend_last_4=trend_last_4,
        total_whitelisted=total_whitelisted,
        pending_approvals=pending_approvals,
        vcenter_count=vcenter_count,
        recent_vmdks=recent_vmdks,
    )
