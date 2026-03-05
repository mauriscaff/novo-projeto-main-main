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
    TypeBreakdownStorage,
    VCenterBreakdown,
    VCenterStorageBreakdown,
    DatastoreStorageBreakdown,
    RecoverableStorageResponse,
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

    # ── Identificar o LATEST JOB por vcenter_name usando a tabela de records ──
    from sqlalchemy import or_, and_

    latest_job_per_vc_subq = (
        select(
            ZombieVmdkRecord.vcenter_name,
            func.max(ZombieScanJob.finished_at).label("max_finished"),
        )
        .join(ZombieScanJob, ZombieScanJob.job_id == ZombieVmdkRecord.job_id)
        .where(ZombieScanJob.status == "completed")
        .group_by(ZombieVmdkRecord.vcenter_name)
        .subquery()
    )

    target_jobs_q = await db.execute(
        select(
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.job_id,
        )
        .join(ZombieScanJob, ZombieScanJob.job_id == ZombieVmdkRecord.job_id)
        .join(
            latest_job_per_vc_subq,
            (ZombieVmdkRecord.vcenter_name == latest_job_per_vc_subq.c.vcenter_name)
            & (ZombieScanJob.finished_at == latest_job_per_vc_subq.c.max_finished),
        )
        .group_by(ZombieVmdkRecord.vcenter_name)
    )
    vc_job_map = {row.vcenter_name: row.job_id for row in target_jobs_q.all()}

    # Constrói filtro compound: (vc_name = X AND job_id = J1) OR ...
    if vc_job_map:
        vc_job_filters = or_(
            *[
                and_(
                    ZombieVmdkRecord.vcenter_name == vc_name,
                    ZombieVmdkRecord.job_id == jid,
                )
                for vc_name, jid in vc_job_map.items()
            ]
        )
    else:
        # Sem nenhum job → filtro impossível
        vc_job_filters = ZombieVmdkRecord.job_id == "no-jobs-yet"

    # ── Totais deduplicados usando apenas os LATEST JOBS por vCenter ───────
    unique_vmdks_subq = (
        select(
            ZombieVmdkRecord.path,
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.tipo_zombie,
            func.max(ZombieVmdkRecord.tamanho_gb).label("tamanho_gb")
        )
        .where(vc_job_filters)
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


@router.get(
    "/recoverable-storage",
    response_model=RecoverableStorageResponse,
    summary="Dashboard de Storage Recuperável",
    description="Retorna métricas de storage recuperável do último scan completo por vCenter",
)
async def get_recoverable_storage(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> RecoverableStorageResponse:
    # Busca o job mais recente completed (para last_scan_at)
    latest_job_q = await db.execute(
        select(ZombieScanJob)
        .where(ZombieScanJob.status == "completed")
        .order_by(ZombieScanJob.finished_at.desc())
        .limit(1)
    )
    latest_job = latest_job_q.scalar_one_or_none()
    
    if not latest_job:
        return RecoverableStorageResponse(
            total_recoverable_gb=0.0,
            total_recoverable_tb=0.0,
            by_datastore=[],
            by_vcenter=[],
            last_scan_at=None
        )

    # ── Estratégia: pegar o último job_id COMPLETED por vcenter_name ──────
    # Usando a própria tabela de records para descobrir qual job é o mais
    # recente para cada vCenter, evitando o problema de jobs multi-vCenter
    # sobrepondo dados de jobs mais novos de um só vCenter.
    latest_job_per_vc_subq = (
        select(
            ZombieVmdkRecord.vcenter_name,
            func.max(ZombieScanJob.finished_at).label("max_finished"),
        )
        .join(ZombieScanJob, ZombieScanJob.job_id == ZombieVmdkRecord.job_id)
        .where(ZombieScanJob.status == "completed")
        .group_by(ZombieVmdkRecord.vcenter_name)
        .subquery()
    )

    # Agora pegar o job_id correspondente a cada (vcenter_name, max_finished)
    target_jobs_q = await db.execute(
        select(
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.job_id,
        )
        .join(ZombieScanJob, ZombieScanJob.job_id == ZombieVmdkRecord.job_id)
        .join(
            latest_job_per_vc_subq,
            (ZombieVmdkRecord.vcenter_name == latest_job_per_vc_subq.c.vcenter_name)
            & (ZombieScanJob.finished_at == latest_job_per_vc_subq.c.max_finished),
        )
        .group_by(ZombieVmdkRecord.vcenter_name)
    )
    # Map: vcenter_name -> job_id
    vc_job_map = {row.vcenter_name: row.job_id for row in target_jobs_q.all()}

    if not vc_job_map:
        return RecoverableStorageResponse(
            total_recoverable_gb=0.0,
            total_recoverable_tb=0.0,
            by_datastore=[],
            by_vcenter=[],
            last_scan_at=latest_job.finished_at,
        )

    # ── Buscar registros filtrando por (vcenter_name, job_id) correto ────
    # Constrói filtro OR: (vc_name = X AND job_id = J1) OR (vc_name = Y AND job_id = J2) ...
    from sqlalchemy import or_, and_
    vc_job_filters = [
        and_(
            ZombieVmdkRecord.vcenter_name == vc_name,
            ZombieVmdkRecord.job_id == jid,
        )
        for vc_name, jid in vc_job_map.items()
    ]

    whitelist_subq = select(VmdkWhitelist.path).scalar_subquery()

    records_query = await db.execute(
        select(
            ZombieVmdkRecord.datastore,
            ZombieVmdkRecord.vcenter_host,
            ZombieVmdkRecord.tipo_zombie,
            ZombieVmdkRecord.tamanho_gb
        )
        .where(or_(*vc_job_filters))
        .where(ZombieVmdkRecord.path.not_in(whitelist_subq))
    )
    records = records_query.all()
    
    # Agregar os dados via Python dictionary
    # by Datastore grouping
    ds_map = {}
    # by vCenter grouping
    vc_map = {}
    
    total_gb = 0.0
    
    for row in records:
        ds_name = row.datastore or "Unknown"
        vc_host = row.vcenter_host or "Unknown"
        tipo = row.tipo_zombie
        size_gb = float(row.tamanho_gb or 0.0)
        
        # Ignora tipos lixo ou que explicitamente dizem pra não entrar,
        # MAS a instrução diz "Itens com status WHITELIST não entram no cálculo". 
        # WHITELIST é um status possível dentro do tipo_zombie? Sim, o código do pipeline às vezes salva como WHITELIST.
        if tipo == "WHITELIST":
            continue
            
        total_gb += size_gb
        
        # Accumulate by vcenter
        if vc_host not in vc_map:
            vc_map[vc_host] = {"total_gb": 0.0, "zombie_count": 0}
            
        vc_map[vc_host]["total_gb"] += size_gb
        vc_map[vc_host]["zombie_count"] += 1
        
        # Accumulate by datastore
        key = f"{vc_host}_{ds_name}"
        if key not in ds_map:
            ds_map[key] = {
                "datastore_name": ds_name,
                "vcenter": vc_host,
                "total_gb": 0.0,
                "zombie_count": 0,
                "by_type": {}
            }
            
        # Garante que os 5 tipos existam no by_type (ORPHANED, BROKEN_CHAIN, SNAPSHOT_ORPHAN, UNREGISTERED_DIR, POSSIBLE_FALSE_POSITIVE)
        if tipo not in ds_map[key]["by_type"]:
            ds_map[key]["by_type"][tipo] = {"count": 0, "gb": 0.0}
            
        ds_map[key]["total_gb"] += size_gb
        ds_map[key]["zombie_count"] += 1
        ds_map[key]["by_type"][tipo]["count"] += 1
        ds_map[key]["by_type"][tipo]["gb"] += size_gb

    # Format output arrays
    output_vcenter = []
    for vc, data in sorted(vc_map.items()): # Sort a-z
        output_vcenter.append(
            VCenterStorageBreakdown(
                vcenter=vc,
                total_gb=round(data["total_gb"], 2),
                zombie_count=data["zombie_count"]
            )
        )
        
    output_datastore = []
    for ds_data in ds_map.values():
        t_gb = ds_data["total_gb"]
        
        # Garante que as propriedades padrão existam com zero caso não detectadas nesse DB
        types_break = {}
        for fixed_t in ["ORPHANED", "BROKEN_CHAIN", "SNAPSHOT_ORPHAN", "UNREGISTERED_DIR", "POSSIBLE_FALSE_POSITIVE"]:
             td = ds_data["by_type"].get(fixed_t, {"count": 0, "gb": 0.0})
             types_break[fixed_t] = TypeBreakdownStorage(
                 count=td["count"],
                 gb=round(td["gb"], 2)
             )
             
        # Se houver outro tipo bizarro, soma no final listado tbm
        for t, td in ds_data["by_type"].items():
            if t not in types_break:
                 types_break[t] = TypeBreakdownStorage(
                     count=td["count"],
                     gb=round(td["gb"], 2)
                 )

        percent = (t_gb / total_gb * 100) if total_gb > 0 else 0.0

        output_datastore.append(
            DatastoreStorageBreakdown(
                datastore_name=ds_data["datastore_name"],
                vcenter=ds_data["vcenter"],
                total_gb=round(t_gb, 2),
                total_tb=round(t_gb / 1024, 2),
                zombie_count=ds_data["zombie_count"],
                by_type=types_break,
                percentage_of_total=round(percent, 1)
            )
        )
        
    # Sort output_datastore from largest gb to lowest
    output_datastore.sort(key=lambda x: x.total_gb, reverse=True)

    return RecoverableStorageResponse(
        total_recoverable_gb=round(total_gb, 2),
        total_recoverable_tb=round(total_gb / 1024, 2),
        by_datastore=output_datastore,
        by_vcenter=output_vcenter,
        last_scan_at=latest_job.finished_at if latest_job else None
    )
