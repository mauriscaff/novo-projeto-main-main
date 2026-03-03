"""
Endpoints de varredura zombie de VMDKs.

Rotas (prefixo registrado em main.py: /api/v1/scan):

  POST   /start                                    Dispara varredura assíncrona
  GET    /jobs/{job_id}                            Status + summary do job
  GET    /results/{job_id}                         Lista paginada com filtros
  GET    /results/{job_id}/export                  Download CSV ou JSON
  POST   /results/{job_id}/mark-safe/{path:path}   Adiciona VMDK à whitelist
  GET    /whitelist                                 Lista VMDKs na whitelist
  DELETE /whitelist/{id}                            Remove entrada da whitelist
"""

from __future__ import annotations

import csv
import io
import json
import logging
import uuid
from datetime import date, datetime, timezone
from urllib.parse import unquote
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import asc, cast, desc, func, select, Date
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.scanner.scan_runner import resolve_vcenter, run_zombie_scan, get_scan_progress
from app.core.scanner.zombie_detector import TIPOS_EXCLUIVEIS, ZombieType, _CAUSES_BY_TYPE
from app.dependencies import get_current_user, get_db
from app.models.vmdk_whitelist import VmdkWhitelist
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from app.schemas.webhook import MarkSafeRequest, WhitelistEntryResponse
from app.schemas.scanner import (
    DatastoreScanMetricSchema,
    PaginatedResults,
    ScanJobStatusResponse,
    ScanJobSummary,
    ScanStartRequest,
    ScanStartResponse,
    SortByField,
    SortOrder,
    ZombieBreakdown,
    ZombieResultItem,
)
router = APIRouter()
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────────────

# Score de fallback para registros gravados antes do scoring dinâmico (score=0)
_FALLBACK_SCORE: dict[str, int] = {
    "ORPHANED":                70,
    "SNAPSHOT_ORPHAN":         65,
    "BROKEN_CHAIN":            65,
    "UNREGISTERED_DIR":        80,
    "POSSIBLE_FALSE_POSITIVE": 20,
}

_SORT_COLUMNS = {
    "tamanho_gb": ZombieVmdkRecord.tamanho_gb,
    "ultima_modificacao": ZombieVmdkRecord.ultima_modificacao,
    "tipo_zombie": ZombieVmdkRecord.tipo_zombie,
    "datastore": ZombieVmdkRecord.datastore,
    "confidence_score": ZombieVmdkRecord.confidence_score,
}

_CSV_FIELDS = [
    "id", "job_id", "path", "datastore", "folder", "datastore_type",
    "tamanho_gb", "ultima_modificacao", "tipo_zombie",
    "vcenter_host", "vcenter_name", "datacenter",
    "detection_rules", "false_positive_reason",
]



async def _get_job_or_404(job_id: str, db: AsyncSession) -> ZombieScanJob:
    job = await db.get(ZombieScanJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return job


def _job_to_schema(job: ZombieScanJob) -> dict:
    """Converte ORM → dict compatível com ScanJobBase."""
    return {
        "job_id": job.job_id,
        "vcenter_ids": job.vcenter_ids or [],
        "datacenters": job.datacenters,
        "status": job.status,
        "started_at": job.started_at,
        "finished_at": job.finished_at,
        "error_messages": job.error_messages,
        "created_at": job.created_at,
    }


def _record_to_item(
    r: ZombieVmdkRecord,
    whitelist_paths: set[str] | None = None,
) -> ZombieResultItem:
    in_wl = whitelist_paths is not None and r.path in whitelist_paths
    return ZombieResultItem(
        id=r.id,
        job_id=r.job_id,
        path=r.path,
        datastore=r.datastore,
        folder=r.folder or "",
        datastore_type=r.datastore_type or "",
        tamanho_gb=r.tamanho_gb,
        ultima_modificacao=r.ultima_modificacao,
        tipo_zombie=r.tipo_zombie,
        vcenter_host=r.vcenter_host,
        vcenter_name=r.vcenter_name or "",
        datacenter=r.datacenter,
        detection_rules=r.detection_rules or [],
        likely_causes=(
            getattr(r, "likely_causes", None) or
            _CAUSES_BY_TYPE.get(r.tipo_zombie, [])
        ),
        false_positive_reason=r.false_positive_reason,
        created_at=r.created_at,
        status="WHITELIST" if in_wl else "NOVO",
        confidence_score=(
            (getattr(r, "confidence_score", 0) or 0)
            or _FALLBACK_SCORE.get(r.tipo_zombie, 50)
        ),
        vcenter_deeplink_ui=getattr(r, "vcenter_deeplink_ui", None) or "",
        vcenter_deeplink_folder=getattr(r, "vcenter_deeplink_folder", None) or "",
        vcenter_deeplink_folder_dir=getattr(r, "vcenter_deeplink_folder_dir", None) or "",
        datacenter_path=getattr(r, "datacenter_path", None) or "",
        datastore_name=getattr(r, "datastore_name", None) or "",
        vmdk_folder=getattr(r, "vmdk_folder", None) or "",
        vmdk_filename=getattr(r, "vmdk_filename", None) or "",
        rule_evidence=getattr(r, "rule_evidence", None),
    )



# ─────────────────────────────────────────────────────────────────────────────
# POST /start
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/start",
    response_model=ScanStartResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Iniciar varredura zombie",
    description="""
Dispara uma varredura assíncrona de VMDKs zombie em múltiplos vCenters.

- `vcenter_ids`: lista de IDs inteiros ou nomes de vCenters cadastrados.
- `datacenters`: lista de nomes de Datacenters a varrer. Se omitido, todos os
  Datacenters de cada vCenter são varridos.

Retorna imediatamente com `job_id` e `status: running`.
Acompanhe o progresso via `GET /scan/jobs/{job_id}`.
    """,
)
async def start_scan(
    body: ScanStartRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanStartResponse:
    # Valida que pelo menos um vCenter existe antes de criar o job
    found_any = False
    for ref in body.vcenter_ids:
        vc = await resolve_vcenter(db, ref)
        if vc:
            found_any = True
            break
    if not found_any:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Nenhum dos vCenter IDs/nomes informados foi encontrado no banco.",
        )

    job_id = str(uuid.uuid4())
    job = ZombieScanJob(
        job_id=job_id,
        vcenter_ids=list(body.vcenter_ids),
        datacenters=body.datacenters,
        status="pending",
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    background_tasks.add_task(
        run_zombie_scan,
        job_id,
        list(body.vcenter_ids),
        body.datacenters,
    )

    return ScanStartResponse(**_job_to_schema(job))


# ─────────────────────────────────────────────────────────────────────────────
# GET /jobs/{job_id}
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/jobs/{job_id}",
    response_model=ScanJobStatusResponse,
    summary="Status e summary do job",
    description="""
Retorna o estado atual do job de varredura.

Quando `status = completed`, inclui o campo `summary` com:
- `total_vmdks_encontrados`
- `total_size_gb`
- `breakdown`: contagem por tipo (`ORPHANED`, `SNAPSHOT_ORPHAN`, `BROKEN_CHAIN`,
  `UNREGISTERED_DIR`, `POSSIBLE_FALSE_POSITIVE`)
    """,
)
async def get_job_status(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanJobStatusResponse:
    job = await _get_job_or_404(job_id, db)
    data = _job_to_schema(job)

    summary: ScanJobSummary | None = None
    if job.status == "completed":
        # Breakdown por tipo_zombie
        rows = await db.execute(
            select(
                ZombieVmdkRecord.tipo_zombie,
                func.count().label("cnt"),
            )
            .where(ZombieVmdkRecord.job_id == job_id)
            .group_by(ZombieVmdkRecord.tipo_zombie)
        )
        breakdown_dict: dict[str, int] = {r.tipo_zombie: r.cnt for r in rows}
        total_excluiveis = sum(breakdown_dict.get(t, 0) for t in TIPOS_EXCLUIVEIS)
        size_excluiveis_stmt = (
            select(func.sum(ZombieVmdkRecord.tamanho_gb))
            .select_from(ZombieVmdkRecord)
            .where(
                ZombieVmdkRecord.job_id == job_id,
                ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS),
            )
        )
        total_excluiveis_gb: float = (await db.execute(size_excluiveis_stmt)).scalar_one() or 0.0
        summary = ScanJobSummary(
            total_vmdks_encontrados=job.total_vmdks or 0,
            total_size_gb=round(job.total_size_gb or 0.0, 3),
            breakdown=ZombieBreakdown(
                ORPHANED=breakdown_dict.get(ZombieType.ORPHANED.value, 0),
                SNAPSHOT_ORPHAN=breakdown_dict.get(ZombieType.SNAPSHOT_ORPHAN.value, 0),
                BROKEN_CHAIN=breakdown_dict.get(ZombieType.BROKEN_CHAIN.value, 0),
                UNREGISTERED_DIR=breakdown_dict.get(ZombieType.UNREGISTERED_DIR.value, 0),
                POSSIBLE_FALSE_POSITIVE=breakdown_dict.get(
                    ZombieType.POSSIBLE_FALSE_POSITIVE.value, 0
                ),
            ),
            total_excluiveis=total_excluiveis,
            total_excluiveis_gb=round(total_excluiveis_gb, 3),
        )

    # Métricas por datastore (quando job concluído)
    datastore_metrics = None
    if job.status == "completed" and job.datastore_metrics:
        datastore_metrics = [DatastoreScanMetricSchema(**d) for d in job.datastore_metrics]

    # Inclui progresso em tempo real quando o job ainda está rodando
    progress = None
    if job.status in ("running", "pending"):
        progress = get_scan_progress(job_id)

    return ScanJobStatusResponse(
        **data,
        summary=summary,
        datastore_metrics=datastore_metrics,
        progress=progress,
    )


# ─────────────────────────────────────────────────────────────────────────────
# POST /jobs/{job_id}/mark-stuck — recuperação de job travado
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/jobs/mark-latest-stuck",
    response_model=ScanJobStatusResponse,
    summary="Marcar o último job travado como failed (recovery)",
    description="""
Busca o job mais recente com status `running` ou `pending` e marca como **failed**.

Útil quando não se sabe o job_id: um único POST corrige o último rescan travado.
Retorna 404 se não houver nenhum job em execução.
    """,
)
async def mark_latest_stuck_job(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanJobStatusResponse:
    from datetime import datetime, timezone
    q = (
        select(ZombieScanJob)
        .where(ZombieScanJob.status.in_(["running", "pending"]))
        .order_by(ZombieScanJob.started_at.desc().nulls_last(), ZombieScanJob.created_at.desc())
        .limit(1)
    )
    result = await db.execute(q)
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Nenhum job em running ou pending encontrado.",
        )
    job.status = "failed"
    job.finished_at = datetime.now(timezone.utc)
    job.error_messages = (job.error_messages or []) + [
        "Job marcado como travado (recovery manual via mark-latest-stuck).",
    ]
    await db.commit()
    await db.refresh(job)
    data = _job_to_schema(job)
    return ScanJobStatusResponse(**data, summary=None, datastore_metrics=None, progress=None)


@router.post(
    "/jobs/{job_id}/mark-stuck",
    response_model=ScanJobStatusResponse,
    summary="Marcar job específico como travado (recovery)",
    description="""
Marca um job que está `running` ou `pending` como **failed**, com mensagem de recovery.

Use quando o scan parar de responder (ex.: datastore muito grande, vCenter indisponível).
O job será finalizado com `error_messages: ["Job marcado como travado (recovery)."]`.
    """,
)
async def mark_job_stuck(
    job_id: str,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanJobStatusResponse:
    from datetime import datetime, timezone
    job = await _get_job_or_404(job_id, db)
    if job.status not in ("running", "pending"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Job não está em execução (status atual: {job.status}). Só é possível marcar como travado jobs em running ou pending.",
        )
    job.status = "failed"
    job.finished_at = datetime.now(timezone.utc)
    job.error_messages = (job.error_messages or []) + [
        "Job marcado como travado (recovery manual).",
    ]
    await db.commit()
    await db.refresh(job)
    data = _job_to_schema(job)
    return ScanJobStatusResponse(**data, summary=None, datastore_metrics=None, progress=None)


# ─────────────────────────────────────────────────────────────────────────────
# GET /results  (todos os jobs; job_id é filtro opcional)
# ─────────────────────────────────────────────────────────────────────────────

# Mapeamento dos nomes de campo usados pelo frontend para os campos do banco
_SORT_MAP_FRONTEND = {
    "size":     "tamanho_gb",
    "modified": "ultima_modificacao",
    "tipo":     "tipo_zombie",
    "confidence_score":   "confidence_score",
    "tamanho_gb":          "tamanho_gb",
    "ultima_modificacao":  "ultima_modificacao",
    "tipo_zombie":         "tipo_zombie",
    "datastore":           "datastore",
}


@router.get(
    "/results",
    response_model=PaginatedResults,
    summary="Todos os resultados (cross-job, paginado)",
    description="""
Retorna VMDKs zombie de **todos os jobs** concluídos, com filtros opcionais.

Aceita os mesmos filtros de `GET /results/{job_id}` mais:
- `job_id`: filtra por um job específico
- `status`: `NOVO` | `WHITELIST`
- `deletable_only`: se true, retorna apenas tipos excluíveis (ORPHANED, SNAPSHOT_ORPHAN, BROKEN_CHAIN, UNREGISTERED_DIR), reunindo tudo que pode ser excluído do vCenter pelas regras do sistema.

Parâmetros de ordenação aceitos (aliases do frontend):
- `sort_by`: `size` | `modified` | `tipo` | `tamanho_gb` | `ultima_modificacao` | `tipo_zombie` | `datastore`
- `sort_dir`: `asc` | `desc`
    """,
)
async def list_all_results(
    job_id: Annotated[str | None, Query(description="Filtrar por job_id específico")] = None,
    tipo: Annotated[str | None, Query()] = None,
    deletable_only: Annotated[
        bool,
        Query(description="Se true, apenas VMDKs com tipo excluível (ORPHANED, SNAPSHOT_ORPHAN, BROKEN_CHAIN, UNREGISTERED_DIR)"),
    ] = False,
    vcenter: Annotated[str | None, Query()] = None,
    datacenter: Annotated[str | None, Query()] = None,
    min_size_gb: Annotated[float | None, Query(ge=0)] = None,
    min_confidence: Annotated[int | None, Query(ge=0, le=100, description="Score de confiança mínimo (0–100)")] = None,
    max_confidence: Annotated[int | None, Query(ge=0, le=100, description="Score de confiança máximo (0–100)")] = None,
    modified_after: Annotated[str | None, Query(description="Última modificação do VMDK após esta data (YYYY-MM-DD)")] = None,
    modified_before: Annotated[str | None, Query(description="Última modificação do VMDK antes desta data (YYYY-MM-DD)")] = None,
    scan_date: Annotated[str | None, Query(description="Data da varredura (YYYY-MM-DD) — resultados de jobs iniciados nesta data")] = None,
    status: Annotated[str | None, Query(description="NOVO | WHITELIST")] = None,
    sort_by: Annotated[str, Query()] = "tamanho_gb",
    sort_dir: Annotated[str, Query()] = "desc",
    # aceita também `order` como alias
    order: Annotated[str | None, Query()] = None,
    page: Annotated[int, Query(ge=1)] = 1,
    per_page: Annotated[int, Query(ge=1, le=500, alias="per_page")] = 25,
    # aceita page_size como alias
    page_size: Annotated[int | None, Query(ge=1, le=500)] = None,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> PaginatedResults:
    # Normaliza tamanho de página
    effective_page_size = page_size or per_page

    # Normaliza direção
    effective_order = order or sort_dir or "desc"

    # Obtém paths da whitelist para calcular status
    wl_rows = (await db.execute(select(VmdkWhitelist.path))).scalars().all()
    whitelist_paths = set(wl_rows)

    stmt = select(ZombieVmdkRecord)
    count_stmt = select(func.count()).select_from(ZombieVmdkRecord)
    size_stmt = select(func.sum(ZombieVmdkRecord.tamanho_gb)).select_from(ZombieVmdkRecord)

    if job_id:
        stmt = stmt.where(ZombieVmdkRecord.job_id == job_id)
        count_stmt = count_stmt.where(ZombieVmdkRecord.job_id == job_id)
        size_stmt = size_stmt.where(ZombieVmdkRecord.job_id == job_id)

    if tipo:
        valid_tipos = {t.value for t in ZombieType}
        if tipo.upper() in valid_tipos:
            stmt = stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())
            count_stmt = count_stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())
            size_stmt = size_stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())

    if deletable_only:
        stmt = stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))
        count_stmt = count_stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))
        size_stmt = size_stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))

    if vcenter:
        like = f"%{vcenter}%"
        flt = ZombieVmdkRecord.vcenter_name.ilike(like) | ZombieVmdkRecord.vcenter_host.ilike(like)
        stmt = stmt.where(flt)
        count_stmt = count_stmt.where(flt)
        size_stmt = size_stmt.where(flt)

    if datacenter:
        flt = ZombieVmdkRecord.datacenter.ilike(f"%{datacenter}%")
        stmt = stmt.where(flt)
        count_stmt = count_stmt.where(flt)
        size_stmt = size_stmt.where(flt)

    if min_size_gb is not None:
        flt = ZombieVmdkRecord.tamanho_gb >= min_size_gb
        stmt = stmt.where(flt)
        count_stmt = count_stmt.where(flt)
        size_stmt = size_stmt.where(flt)

    if min_confidence is not None:
        flt = ZombieVmdkRecord.confidence_score >= min_confidence
        stmt = stmt.where(flt)
        count_stmt = count_stmt.where(flt)
        size_stmt = size_stmt.where(flt)

    if max_confidence is not None:
        flt = ZombieVmdkRecord.confidence_score <= max_confidence
        stmt = stmt.where(flt)
        count_stmt = count_stmt.where(flt)
        size_stmt = size_stmt.where(flt)

    if modified_after:
        try:
            dt_after = datetime.fromisoformat(modified_after + "T00:00:00+00:00")
            stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
            count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
            size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        except ValueError:
            pass

    if modified_before:
        try:
            dt_before = datetime.fromisoformat(modified_before + "T23:59:59.999999+00:00")
            stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
            count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
            size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        except ValueError:
            pass

    if scan_date:
        try:
            d = date.fromisoformat(scan_date)
            job_subq = select(ZombieScanJob.job_id).where(
                cast(ZombieScanJob.started_at, Date) == d
            )
            stmt = stmt.where(ZombieVmdkRecord.job_id.in_(job_subq))
            count_stmt = count_stmt.where(ZombieVmdkRecord.job_id.in_(job_subq))
            size_stmt = size_stmt.where(ZombieVmdkRecord.job_id.in_(job_subq))
        except ValueError:
            pass

    # Filtro de status (WHITELIST exige cruzamento com a tabela)
    if status == "WHITELIST" and whitelist_paths:
        stmt = stmt.where(ZombieVmdkRecord.path.in_(whitelist_paths))
        count_stmt = count_stmt.where(ZombieVmdkRecord.path.in_(whitelist_paths))
        size_stmt = size_stmt.where(ZombieVmdkRecord.path.in_(whitelist_paths))
    elif status == "NOVO" and whitelist_paths:
        stmt = stmt.where(ZombieVmdkRecord.path.notin_(whitelist_paths))
        count_stmt = count_stmt.where(ZombieVmdkRecord.path.notin_(whitelist_paths))
        size_stmt = size_stmt.where(ZombieVmdkRecord.path.notin_(whitelist_paths))

    total: int = (await db.execute(count_stmt)).scalar_one()
    total_gb: float = (await db.execute(size_stmt)).scalar_one() or 0.0
    total_pages = max(1, (total + effective_page_size - 1) // effective_page_size)

    # Ordenação
    col_name = _SORT_MAP_FRONTEND.get(sort_by, "tamanho_gb")
    sort_col = _SORT_COLUMNS.get(col_name, ZombieVmdkRecord.tamanho_gb)
    sort_fn = desc if effective_order == "desc" else asc
    stmt = stmt.order_by(sort_fn(sort_col).nulls_last(), ZombieVmdkRecord.id.asc())

    stmt = stmt.offset((page - 1) * effective_page_size).limit(effective_page_size)
    rows = (await db.execute(stmt)).scalars().all()

    return PaginatedResults(
        items=[_record_to_item(r, whitelist_paths) for r in rows],
        total=total,
        page=page,
        page_size=effective_page_size,
        total_pages=total_pages,
        total_size_gb=round(total_gb, 3),
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /results/{job_id}
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/results/{job_id}",
    response_model=PaginatedResults,
    summary="Resultados paginados do job",
    description="""
Lista os VMDKs zombie encontrados com suporte a filtros e ordenação.

**Filtros disponíveis:**
- `tipo`: `ORPHANED` | `SNAPSHOT_ORPHAN` | `BROKEN_CHAIN` | `UNREGISTERED_DIR` | `POSSIBLE_FALSE_POSITIVE`
- `deletable_only`: se true, apenas tipos excluíveis (reúne ORPHANED, SNAPSHOT_ORPHAN, BROKEN_CHAIN, UNREGISTERED_DIR)
- `vcenter`: filtra por nome ou hostname do vCenter (busca parcial, case-insensitive)
- `datacenter`: filtra por nome do Datacenter (busca parcial, case-insensitive)
- `min_size_gb`: tamanho mínimo em GB

**Ordenação:**
- `sort_by`: `tamanho_gb` | `ultima_modificacao` | `tipo_zombie` | `datastore`
- `order`: `asc` | `desc` (padrão: `desc`)

**Paginação:**
- `page`: número da página (começa em 1)
- `page_size`: itens por página (padrão 50, máximo 500)
    """,
)
async def get_results(
    job_id: str,
    # Filtros
    tipo: Annotated[
        str | None,
        Query(description="Tipo de zombie: ORPHANED | SNAPSHOT_ORPHAN | BROKEN_CHAIN | UNREGISTERED_DIR | POSSIBLE_FALSE_POSITIVE"),
    ] = None,
    deletable_only: Annotated[
        bool,
        Query(description="Se true, apenas VMDKs com tipo excluível (reúne todos que podem ser excluídos do vCenter)"),
    ] = False,
    vcenter: Annotated[
        str | None,
        Query(description="Filtro por nome ou host do vCenter (parcial, case-insensitive)"),
    ] = None,
    datacenter: Annotated[
        str | None,
        Query(description="Filtro por nome do Datacenter (parcial, case-insensitive)"),
    ] = None,
    min_size_gb: Annotated[
        float | None,
        Query(ge=0, description="Tamanho mínimo do VMDK em GB"),
    ] = None,
    min_confidence: Annotated[int | None, Query(ge=0, le=100)] = None,
    max_confidence: Annotated[int | None, Query(ge=0, le=100)] = None,
    modified_after: Annotated[str | None, Query(description="Última modificação do VMDK após (YYYY-MM-DD)")] = None,
    modified_before: Annotated[str | None, Query(description="Última modificação do VMDK antes de (YYYY-MM-DD)")] = None,
    # Ordenação
    sort_by: Annotated[
        SortByField,
        Query(description="Campo de ordenação"),
    ] = "tamanho_gb",
    order: Annotated[
        SortOrder,
        Query(description="Direção da ordenação"),
    ] = "desc",
    # Paginação
    page: Annotated[int, Query(ge=1, description="Número da página (começa em 1)")] = 1,
    page_size: Annotated[
        int, Query(ge=1, le=500, description="Itens por página (máximo 500)")
    ] = 50,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> PaginatedResults:
    await _get_job_or_404(job_id, db)

    # ── Constrói filtros ───────────────────────────────────────────────────────
    stmt = select(ZombieVmdkRecord).where(ZombieVmdkRecord.job_id == job_id)
    count_stmt = (
        select(func.count())
        .select_from(ZombieVmdkRecord)
        .where(ZombieVmdkRecord.job_id == job_id)
    )

    if tipo:
        # Valida o valor contra o enum
        valid_tipos = {t.value for t in ZombieType}
        if tipo.upper() not in valid_tipos:
            raise HTTPException(
                status_code=422,
                detail=f"tipo inválido. Valores aceitos: {sorted(valid_tipos)}",
            )
        stmt = stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())
        count_stmt = count_stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())

    if deletable_only:
        stmt = stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))
        count_stmt = count_stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))

    if vcenter:
        like = f"%{vcenter}%"
        vcenter_filter = (
            ZombieVmdkRecord.vcenter_name.ilike(like)
            | ZombieVmdkRecord.vcenter_host.ilike(like)
        )
        stmt = stmt.where(vcenter_filter)
        count_stmt = count_stmt.where(vcenter_filter)

    if datacenter:
        dc_filter = ZombieVmdkRecord.datacenter.ilike(f"%{datacenter}%")
        stmt = stmt.where(dc_filter)
        count_stmt = count_stmt.where(dc_filter)

    if min_size_gb is not None:
        stmt = stmt.where(ZombieVmdkRecord.tamanho_gb >= min_size_gb)
        count_stmt = count_stmt.where(ZombieVmdkRecord.tamanho_gb >= min_size_gb)

    if min_confidence is not None:
        stmt = stmt.where(ZombieVmdkRecord.confidence_score >= min_confidence)
        count_stmt = count_stmt.where(ZombieVmdkRecord.confidence_score >= min_confidence)

    if max_confidence is not None:
        stmt = stmt.where(ZombieVmdkRecord.confidence_score <= max_confidence)
        count_stmt = count_stmt.where(ZombieVmdkRecord.confidence_score <= max_confidence)

    if modified_after:
        try:
            dt_after = datetime.fromisoformat(modified_after + "T00:00:00+00:00")
            stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
            count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        except ValueError:
            pass

    if modified_before:
        try:
            dt_before = datetime.fromisoformat(modified_before + "T23:59:59.999999+00:00")
            stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
            count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        except ValueError:
            pass

    # ── Total e tamanho total (mesmos filtros) ──────────────────────────────────
    total: int = (await db.execute(count_stmt)).scalar_one()
    total_pages = max(1, (total + page_size - 1) // page_size)

    size_stmt = (
        select(func.sum(ZombieVmdkRecord.tamanho_gb))
        .select_from(ZombieVmdkRecord)
        .where(ZombieVmdkRecord.job_id == job_id)
    )
    if tipo:
        size_stmt = size_stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())
    if deletable_only:
        size_stmt = size_stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))
    if vcenter:
        like = f"%{vcenter}%"
        vc_flt = ZombieVmdkRecord.vcenter_name.ilike(like) | ZombieVmdkRecord.vcenter_host.ilike(like)
        size_stmt = size_stmt.where(vc_flt)
    if datacenter:
        size_stmt = size_stmt.where(ZombieVmdkRecord.datacenter.ilike(f"%{datacenter}%"))
    if min_size_gb is not None:
        size_stmt = size_stmt.where(ZombieVmdkRecord.tamanho_gb >= min_size_gb)
    if min_confidence is not None:
        size_stmt = size_stmt.where(ZombieVmdkRecord.confidence_score >= min_confidence)
    if max_confidence is not None:
        size_stmt = size_stmt.where(ZombieVmdkRecord.confidence_score <= max_confidence)
    if modified_after:
        try:
            dt_after = datetime.fromisoformat(modified_after + "T00:00:00+00:00")
            size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        except ValueError:
            pass
    if modified_before:
        try:
            dt_before = datetime.fromisoformat(modified_before + "T23:59:59.999999+00:00")
            size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        except ValueError:
            pass
    total_gb: float = (await db.execute(size_stmt)).scalar_one() or 0.0

    # Whitelist para status
    wl_rows = (await db.execute(select(VmdkWhitelist.path))).scalars().all()
    whitelist_paths = set(wl_rows)

    # ── Ordenação ─────────────────────────────────────────────────────────────
    sort_col = _SORT_COLUMNS[sort_by]
    sort_fn = desc if order == "desc" else asc
    stmt = stmt.order_by(sort_fn(sort_col).nulls_last(), ZombieVmdkRecord.id.asc())

    # ── Paginação ─────────────────────────────────────────────────────────────
    stmt = stmt.offset((page - 1) * page_size).limit(page_size)

    rows = (await db.execute(stmt)).scalars().all()

    return PaginatedResults(
        items=[_record_to_item(r, whitelist_paths) for r in rows],
        total=total,
        page=page,
        page_size=page_size,
        total_pages=total_pages,
        total_size_gb=round(total_gb, 3),
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /results/{job_id}/export
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/results/{job_id}/export",
    summary="Exportar resultados (CSV ou JSON)",
    description="""
Exporta todos os VMDKs zombie do job para download.

- `?format=csv`  (padrão) — retorna `Content-Type: text/csv`
- `?format=json`           — retorna `Content-Type: application/json`

Os mesmos filtros de `GET /results/{job_id}` são aceitos para exportar
apenas um subconjunto dos resultados.
    """,
    responses={
        200: {
            "content": {
                "text/csv": {},
                "application/json": {},
            },
            "description": "Arquivo para download.",
        }
    },
)
async def export_results(
    job_id: str,
    # Formato
    format: Annotated[
        str,
        Query(description="Formato de saída: csv | json"),
    ] = "csv",
    # Filtros (mesmos do GET /results)
    tipo: Annotated[str | None, Query()] = None,
    deletable_only: Annotated[bool, Query(description="Se true, apenas tipos excluíveis")] = False,
    vcenter: Annotated[str | None, Query()] = None,
    datacenter: Annotated[str | None, Query()] = None,
    min_size_gb: Annotated[float | None, Query(ge=0)] = None,
    min_confidence: Annotated[int | None, Query(ge=0, le=100)] = None,
    max_confidence: Annotated[int | None, Query(ge=0, le=100)] = None,
    modified_after: Annotated[str | None, Query()] = None,
    modified_before: Annotated[str | None, Query()] = None,
    sort_by: Annotated[SortByField, Query()] = "tamanho_gb",
    order: Annotated[SortOrder, Query()] = "desc",
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> StreamingResponse:
    await _get_job_or_404(job_id, db)

    if format not in ("csv", "json"):
        raise HTTPException(
            status_code=422,
            detail="Parâmetro 'format' deve ser 'csv' ou 'json'.",
        )

    # ── Busca todos os registros (sem paginação) ───────────────────────────────
    stmt = select(ZombieVmdkRecord).where(ZombieVmdkRecord.job_id == job_id)

    if tipo:
        stmt = stmt.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())
    if deletable_only:
        stmt = stmt.where(ZombieVmdkRecord.tipo_zombie.in_(TIPOS_EXCLUIVEIS))
    if vcenter:
        like = f"%{vcenter}%"
        stmt = stmt.where(
            ZombieVmdkRecord.vcenter_name.ilike(like)
            | ZombieVmdkRecord.vcenter_host.ilike(like)
        )
    if datacenter:
        stmt = stmt.where(ZombieVmdkRecord.datacenter.ilike(f"%{datacenter}%"))
    if min_size_gb is not None:
        stmt = stmt.where(ZombieVmdkRecord.tamanho_gb >= min_size_gb)
    if min_confidence is not None:
        stmt = stmt.where(ZombieVmdkRecord.confidence_score >= min_confidence)
    if max_confidence is not None:
        stmt = stmt.where(ZombieVmdkRecord.confidence_score <= max_confidence)
    if modified_after:
        try:
            dt_after = datetime.fromisoformat(modified_after + "T00:00:00+00:00")
            stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        except ValueError:
            pass
    if modified_before:
        try:
            dt_before = datetime.fromisoformat(modified_before + "T23:59:59.999999+00:00")
            stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        except ValueError:
            pass

    sort_col = _SORT_COLUMNS[sort_by]
    sort_fn = desc if order == "desc" else asc
    stmt = stmt.order_by(sort_fn(sort_col).nulls_last(), ZombieVmdkRecord.id.asc())

    rows = (await db.execute(stmt)).scalars().all()

    filename = f"vmdk_zombie_{job_id[:8]}.{format}"
    items = [_record_to_item(r) for r in rows]

    if format == "csv":
        return _build_csv_response(items, filename)
    return _build_json_response(items, filename)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de exportação
# ─────────────────────────────────────────────────────────────────────────────


def _build_csv_response(
    items: list[ZombieResultItem],
    filename: str,
) -> StreamingResponse:
    """Gera StreamingResponse com conteúdo CSV."""

    def _generate():
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=_CSV_FIELDS,
            extrasaction="ignore",
            lineterminator="\r\n",
        )
        writer.writeheader()
        yield buf.getvalue()

        for item in items:
            buf = io.StringIO()
            writer = csv.DictWriter(
                buf,
                fieldnames=_CSV_FIELDS,
                extrasaction="ignore",
                lineterminator="\r\n",
            )
            row = item.model_dump()
            # Serializa campos complexos para string legível
            row["detection_rules"] = " | ".join(row.get("detection_rules") or [])
            row["ultima_modificacao"] = (
                row["ultima_modificacao"].isoformat()
                if row.get("ultima_modificacao")
                else ""
            )
            row["tamanho_gb"] = (
                f"{row['tamanho_gb']:.3f}" if row.get("tamanho_gb") is not None else ""
            )
            writer.writerow(row)
            yield buf.getvalue()

    return StreamingResponse(
        _generate(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Total-Records": str(len(items)),
        },
    )


def _build_json_response(
    items: list[ZombieResultItem],
    filename: str,
) -> StreamingResponse:
    """Gera StreamingResponse com conteúdo JSON (array de objetos)."""

    def _generate():
        yield "[\n"
        for i, item in enumerate(items):
            suffix = ",\n" if i < len(items) - 1 else "\n"
            yield json.dumps(item.model_dump(), default=str, ensure_ascii=False) + suffix
        yield "]\n"

    return StreamingResponse(
        _generate(),
        media_type="application/json; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "X-Total-Records": str(len(items)),
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# POST /results/{job_id}/mark-safe/{vmdk_path:path}
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/results/{job_id}/mark-safe/{vmdk_path_encoded:path}",
    response_model=WhitelistEntryResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Marcar VMDK como seguro (whitelist)",
    description="""
Marca um VMDK detectado como zombie como **revisado e seguro**, adicionando-o
à whitelist. VMDKs na whitelist são **ignorados em varreduras futuras**.

**Codificação do caminho:**  
O `vmdk_path_encoded` deve ser URL-encoded (incluindo `/` como `%2F`).
Exemplo:  
```
/api/v1/scan/results/{job_id}/mark-safe/%5Bdatastore01%5D%20folder%2Fname.vmdk
```

**Justificativa obrigatória** (mínimo 10 caracteres) — registrada para auditoria.

O histórico de detecções no `zombie_vmdk_records` é preservado; apenas
varreduras futuras ignorarão o VMDK.
    """,
)
async def mark_safe(
    job_id: str,
    vmdk_path_encoded: str,
    body: MarkSafeRequest,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> WhitelistEntryResponse:
    vmdk_path = unquote(vmdk_path_encoded)

    # Valida que o job existe
    await _get_job_or_404(job_id, db)

    # Busca o registro correspondente
    rec_q = await db.execute(
        select(ZombieVmdkRecord).where(
            ZombieVmdkRecord.job_id == job_id,
            ZombieVmdkRecord.path == vmdk_path,
        )
    )
    record = rec_q.scalar_one_or_none()
    if not record:
        raise HTTPException(
            status_code=404,
            detail=(
                f"VMDK '{vmdk_path}' não encontrado no job '{job_id}'. "
                "Verifique o path (URL-decode aplicado automaticamente)."
            ),
        )

    # Verifica se já está na whitelist
    existing_q = await db.execute(
        select(VmdkWhitelist).where(VmdkWhitelist.path == vmdk_path)
    )
    if existing_q.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"VMDK '{vmdk_path}' já está na whitelist.",
        )

    marked_by = body.marked_by or user.get("sub", "api-user")

    wl = VmdkWhitelist(
        path=vmdk_path,
        justification=body.justification,
        marked_by=marked_by,
        job_id=job_id,
        record_id=record.id,
    )
    db.add(wl)
    await db.flush()
    await db.refresh(wl)

    logger.info(
        "VMDK '%s' adicionado à whitelist por '%s' (job=%s, record_id=%d).",
        vmdk_path, marked_by, job_id, record.id,
    )
    return WhitelistEntryResponse.model_validate(wl)


# ─────────────────────────────────────────────────────────────────────────────
# GET /whitelist
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/whitelist",
    response_model=list[WhitelistEntryResponse],
    summary="Listar whitelist de VMDKs",
    description="Retorna todos os VMDKs marcados como seguros e excluídos de varreduras futuras.",
)
async def list_whitelist(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[WhitelistEntryResponse]:
    result = await db.execute(
        select(VmdkWhitelist).order_by(VmdkWhitelist.created_at.desc())
    )
    return [WhitelistEntryResponse.model_validate(w) for w in result.scalars()]


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /whitelist/{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.delete(
    "/whitelist/{whitelist_id}",
    status_code=status.HTTP_200_OK,
    summary="Remover entrada da whitelist",
    description=(
        "Remove um VMDK da whitelist. O VMDK voltará a ser detectado "
        "nas próximas varreduras."
    ),
)
async def remove_from_whitelist(
    whitelist_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> None:
    entry = await db.get(VmdkWhitelist, whitelist_id)
    if not entry:
        raise HTTPException(status_code=404, detail="Entrada de whitelist não encontrada.")
    await db.delete(entry)
    logger.info("Whitelist id=%d ('%s') removida.", whitelist_id, entry.path)
