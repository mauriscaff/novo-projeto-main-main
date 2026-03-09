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

import asyncio
import csv
import io
import json
import logging
import re
import uuid
from datetime import date, datetime, timezone
from urllib.parse import unquote
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from sqlalchemy import asc, cast, desc, func, select, Date
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.datastore_report import KNOWN_ZOMBIE_TYPES, aggregate_datastore_rows
from app.core.executive_report import build_datastore_executive_report_markdown
from app.core.scanner.scan_runner import resolve_vcenter, run_zombie_scan, get_scan_progress
from app.core.scanner.zombie_detector import TIPOS_EXCLUIVEIS, ZombieType, _CAUSES_BY_TYPE
from app.core.vcenter.client import list_datastores_async
from app.core.vcenter.connection import vcenter_pool
from app.core.vcenter.connection_manager import connection_manager
from app.dependencies import get_current_user, get_db
from app.models.audit_log import AuditLog
from app.models.datastore_snapshot import DatastoreDecomSnapshot
from app.models.vcenter import VCenter
from app.models.vmdk_whitelist import VmdkWhitelist
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from app.schemas.datastore_snapshot import (
    DatastoreSnapshotCreateRequest,
    DatastoreSnapshotResponse,
)
from app.schemas.webhook import MarkSafeRequest, WhitelistEntryResponse
from app.schemas.scanner import (
    DatastoreScanMetricSchema,
    PaginatedResults,
    ScanStartByDatastoreRequest,
    ScanJobStatusResponse,
    ScanJobSummary,
    ScanStartRequest,
    ScanStartResponse,
    SortByField,
    SortOrder,
    ZombieBreakdown,
    ZombieResultItem,
)
from config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()
_active_scan_tasks: set[asyncio.Task] = set()

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

_SNAPSHOT_CSV_FIELDS = [
    "snapshot_id",
    "timestamp",
    "requested_vcenter_ref",
    "resolved_vcenter_id",
    "resolved_vcenter_name",
    "resolved_vcenter_host",
    "datacenter",
    "datastore_name",
    "source_job_id",
    "total_itens",
    "total_size_gb",
    "conclusao",
]


def _safe_report_filename(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return (normalized or "datastore")[:80]


def _get_client_ip(request: Request) -> str | None:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else None


def _parse_filter_datetime(
    value: str | None,
    *,
    param_name: str,
    end_of_day: bool,
) -> datetime | None:
    if not value:
        return None
    suffix = "T23:59:59.999999+00:00" if end_of_day else "T00:00:00+00:00"
    try:
        return datetime.fromisoformat(value + suffix)
    except ValueError:
        safe_value = value[:64]
        logger.warning(
            "Filtro de data ignorado: parametro '%s' invalido (valor='%s').",
            param_name,
            safe_value,
        )
        return None


def _parse_filter_scan_date(value: str | None) -> date | None:
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        safe_value = value[:64]
        logger.warning(
            "Filtro de data ignorado: parametro 'scan_date' invalido (valor='%s').",
            safe_value,
        )
        return None


async def _audit_datastore_report(
    db: AsyncSession,
    *,
    request: Request,
    analyst: str,
    action: str,
    datastore_name: str,
    vcenter_id: str | None,
    status_value: str,
    detail: str | None = None,
) -> None:
    entry = AuditLog(
        analyst=analyst or "unknown",
        action=action,
        vmdk_path=f"[{datastore_name}] __DATASTORE_REPORT__",
        vcenter_id=vcenter_id,
        approval_token_id=None,
        approval_token_value=None,
        dry_run=False,
        readonly_mode_active=settings.readonly_mode,
        status=status_value,
        detail=detail,
        client_ip=_get_client_ip(request),
        user_agent=(request.headers.get("user-agent") or "")[:512] or None,
    )
    db.add(entry)


async def _get_job_or_404(job_id: str, db: AsyncSession) -> ZombieScanJob:
    job = await db.get(ZombieScanJob, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return job


def _spawn_scan_task(
    *,
    job_id: str,
    vcenter_ids: list[int | str],
    datacenters: list[str] | None,
    datastores: list[str] | None,
) -> None:
    """
    Dispara o runner de scan em task desacoplada da requisição HTTP.

    Mantemos referência forte para evitar coleta prematura e registramos exceções
    não tratadas para facilitar troubleshooting.
    """
    task = asyncio.create_task(
        run_zombie_scan(job_id, vcenter_ids, datacenters, datastores)
    )
    _active_scan_tasks.add(task)

    def _on_done(done_task: asyncio.Task) -> None:
        _active_scan_tasks.discard(done_task)
        try:
            exc = done_task.exception()
        except asyncio.CancelledError:
            logger.warning("[job:%s] Task de scan foi cancelada.", job_id)
            return
        if exc is not None:
            logger.error("[job:%s] Falha não tratada na task de scan: %s", job_id, exc, exc_info=exc)

    task.add_done_callback(_on_done)


def _job_to_schema(job: ZombieScanJob) -> dict:
    """Converte ORM → dict compatível com ScanJobBase."""
    return {
        "job_id": job.job_id,
        "vcenter_ids": job.vcenter_ids or [],
        "datacenters": job.datacenters,
        "datastores": getattr(job, "datastores", None),
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


def _snapshot_to_response(snapshot: DatastoreDecomSnapshot) -> DatastoreSnapshotResponse:
    return DatastoreSnapshotResponse(
        id=snapshot.id,
        requested_vcenter_ref=snapshot.requested_vcenter_ref,
        resolved_vcenter_id=snapshot.resolved_vcenter_id,
        resolved_vcenter_name=snapshot.resolved_vcenter_name,
        resolved_vcenter_host=snapshot.resolved_vcenter_host,
        datacenter=snapshot.datacenter,
        datastore_name=snapshot.datastore_name,
        source_job_id=snapshot.source_job_id,
        total_itens=snapshot.total_itens,
        total_size_gb=round(float(snapshot.total_size_gb or 0.0), 3),
        breakdown={k: int(v) for k, v in (snapshot.breakdown or {}).items()},
        timestamp=snapshot.created_at,
        generated_by=snapshot.generated_by,
        conclusao="base para auditoria p\u00f3s-descomissionamento",
    )


# ─────────────────────────────────────────────────────────────────────────────
# GET /datastores — lista datastores conhecidos para seleção no frontend
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/datastores",
    summary="Listar datastores conhecidos",
    description="""
Retorna a lista distinta de datastores que aparecem nos resultados
de varreduras anteriores, agrupados por vCenter.

Usado pelo frontend para popular o seletor de datastores no modal
de nova varredura.
    """,
)
async def list_known_datastores(
    source: Annotated[
        str,
        Query(
            description="`known` usa histórico do banco. `live` consulta datastores ao vivo por vCenter ativo.",
            pattern="^(known|live)$",
        ),
    ] = "known",
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[dict]:
    if source == "live":
        live_rows: list[dict] = []
        vcs = (
            await db.execute(
                select(VCenter).where(VCenter.is_active.is_(True)).order_by(VCenter.id)
            )
        ).scalars().all()

        for vc in vcs:
            try:
                connection_manager.register(vc)
                si = vcenter_pool.get_service_instance(vc.id)
                datastores = await list_datastores_async(si)
            except Exception as exc:
                logger.warning(
                    "Nao foi possivel listar datastores ao vivo para vCenter '%s' (%s): %s",
                    vc.name,
                    vc.host,
                    exc.__class__.__name__,
                )
                continue

            for ds in datastores:
                ds_name = str(ds.get("name") or "").strip()
                if not ds_name:
                    continue
                maintenance_state = str(ds.get("maintenance_state") or "").strip()
                maintenance_mode = bool(ds.get("maintenance_mode"))
                live_rows.append(
                    {
                        "name": ds_name,
                        "vcenter_name": vc.name or "",
                        "vcenter_host": vc.host or "",
                        "accessible": bool(ds.get("accessible", True)),
                        "maintenance_mode": maintenance_mode,
                        "maintenance_state": maintenance_state,
                    }
                )

        seen: set[tuple[str, str]] = set()
        result: list[dict] = []
        for row in sorted(
            live_rows,
            key=lambda r: (
                str(r.get("vcenter_name") or "").lower(),
                str(r.get("vcenter_host") or "").lower(),
                str(r.get("name") or "").lower(),
            ),
        ):
            key = (
                str(row.get("name") or "").strip().lower(),
                str(row.get("vcenter_host") or "").strip().lower(),
            )
            if not key[0] or key in seen:
                continue
            seen.add(key)
            result.append(row)
        return result

    stmt = (
        select(
            ZombieVmdkRecord.datastore,
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.vcenter_host,
        )
        .distinct()
        .order_by(
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.vcenter_host,
            ZombieVmdkRecord.datastore,
        )
    )
    rows = (await db.execute(stmt)).all()
    result: list[dict] = []
    for row in rows:
        ds_name = row.datastore
        result.append({
            "name": ds_name,
            "vcenter_name": row.vcenter_name or "",
            "vcenter_host": row.vcenter_host or "",
        })
    return result


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
- `datastores`: lista de Datastores (LUNs) a varrer. Se omitido, todos os
  Datastores serão varridos.

Retorna imediatamente com `job_id` e `status: running`.
Acompanhe o progresso via `GET /scan/jobs/{job_id}`.
    """,
)
async def start_scan(
    body: ScanStartRequest,
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
        datastores=body.datastores,
        status="pending",
    )
    db.add(job)
    await db.flush()
    await db.commit()
    await db.refresh(job)

    _spawn_scan_task(
        job_id=job_id,
        vcenter_ids=list(body.vcenter_ids),
        datacenters=body.datacenters,
        datastores=body.datastores,
    )

    return ScanStartResponse(**_job_to_schema(job))


@router.post(
    "/start-by-datastore",
    response_model=ScanStartResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Iniciar varredura zombie por datastore",
    description="""
Dispara uma varredura assíncrona com escopo explícito por datastore.

- `vcenter_ids`: lista de IDs inteiros ou nomes de vCenters cadastrados.
- `datastores`: lista obrigatória de Datastores (LUNs) a varrer.
- `datacenters`: opcional. Se omitido, todos os Datacenters de cada vCenter.

Somente os datastores informados serão varridos.
    """,
)
async def start_scan_by_datastore(
    body: ScanStartByDatastoreRequest,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanStartResponse:
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
        datastores=list(body.datastores),
        status="pending",
    )
    db.add(job)
    await db.flush()
    await db.commit()
    await db.refresh(job)

    _spawn_scan_task(
        job_id=job_id,
        vcenter_ids=list(body.vcenter_ids),
        datacenters=body.datacenters,
        datastores=list(body.datastores),
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
    latest_only: Annotated[bool, Query(description="Se true, filtra apenas pelo job mais recente concluído")] = False,
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
    elif latest_only:
        from app.models.zombie_scan import ZombieScanJob
        latest_job_q = await db.execute(
            select(ZombieScanJob.job_id)
            .where(ZombieScanJob.status == "completed")
            .order_by(ZombieScanJob.finished_at.desc())
            .limit(1)
        )
        latest_job_id = latest_job_q.scalar_one_or_none()
        if latest_job_id:
            stmt = stmt.where(ZombieVmdkRecord.job_id == latest_job_id)
            count_stmt = count_stmt.where(ZombieVmdkRecord.job_id == latest_job_id)
            size_stmt = size_stmt.where(ZombieVmdkRecord.job_id == latest_job_id)

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

    dt_after = _parse_filter_datetime(
        modified_after,
        param_name="modified_after",
        end_of_day=False,
    )
    if dt_after:
        stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)

    dt_before = _parse_filter_datetime(
        modified_before,
        param_name="modified_before",
        end_of_day=True,
    )
    if dt_before:
        stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)

    parsed_scan_date = _parse_filter_scan_date(scan_date)
    if parsed_scan_date:
        job_subq = select(ZombieScanJob.job_id).where(
            cast(ZombieScanJob.started_at, Date) == parsed_scan_date
        )
        stmt = stmt.where(ZombieVmdkRecord.job_id.in_(job_subq))
        count_stmt = count_stmt.where(ZombieVmdkRecord.job_id.in_(job_subq))
        size_stmt = size_stmt.where(ZombieVmdkRecord.job_id.in_(job_subq))

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

    dt_after = _parse_filter_datetime(
        modified_after,
        param_name="modified_after",
        end_of_day=False,
    )
    if dt_after:
        stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
        count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)

    dt_before = _parse_filter_datetime(
        modified_before,
        param_name="modified_before",
        end_of_day=True,
    )
    if dt_before:
        stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
        count_stmt = count_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)

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
    if dt_after:
        size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)
    if dt_before:
        size_stmt = size_stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)
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
    dt_after = _parse_filter_datetime(
        modified_after,
        param_name="modified_after",
        end_of_day=False,
    )
    if dt_after:
        stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao >= dt_after)

    dt_before = _parse_filter_datetime(
        modified_before,
        param_name="modified_before",
        end_of_day=True,
    )
    if dt_before:
        stmt = stmt.where(ZombieVmdkRecord.ultima_modificacao <= dt_before)

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


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# POST /datastore-snapshots
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


@router.post(
    "/datastore-snapshots",
    response_model=DatastoreSnapshotResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Gerar snapshot de descomissionamento de datastore",
    description="""
Gera e persiste um snapshot auditável de volumetria para um datastore.

Entrada:
- `vcenter_id`: ID (int) ou nome do vCenter
- `datacenter`: opcional (se omitido, considera todos)
- `datastore_name`: nome exato do datastore
    """,
)
async def create_datastore_snapshot(
    body: DatastoreSnapshotCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> DatastoreSnapshotResponse:
    vc = await resolve_vcenter(db, body.vcenter_id)
    if not vc:
        raise HTTPException(
            status_code=404,
            detail=f"vCenter '{body.vcenter_id}' não encontrado.",
        )

    vc_filter = (
        (func.lower(ZombieVmdkRecord.vcenter_host) == vc.host.lower())
        | (func.lower(ZombieVmdkRecord.vcenter_name) == vc.name.lower())
    )

    latest_job_stmt = (
        select(ZombieScanJob.job_id)
        .join(ZombieVmdkRecord, ZombieVmdkRecord.job_id == ZombieScanJob.job_id)
        .where(
            ZombieScanJob.status == "completed",
            vc_filter,
        )
        .order_by(ZombieScanJob.finished_at.desc().nulls_last())
        .limit(1)
    )
    latest_job_id = (await db.execute(latest_job_stmt)).scalar_one_or_none()

    if not latest_job_id:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Nenhum scan concluído encontrado para o vCenter '{vc.name}'. "
                "Execute uma varredura antes de gerar o snapshot."
            ),
        )

    rows_stmt = (
        select(
            ZombieVmdkRecord.tipo_zombie,
            ZombieVmdkRecord.tamanho_gb,
        )
        .where(
            ZombieVmdkRecord.job_id == latest_job_id,
            vc_filter,
            func.lower(ZombieVmdkRecord.datastore) == body.datastore_name.lower(),
        )
    )
    if body.datacenter:
        rows_stmt = rows_stmt.where(
            func.lower(ZombieVmdkRecord.datacenter) == body.datacenter.lower()
        )

    rows = (await db.execute(rows_stmt)).all()
    if not rows:
        dc_msg = f" no datacenter '{body.datacenter}'" if body.datacenter else ""
        raise HTTPException(
            status_code=404,
            detail=(
                f"Datastore '{body.datastore_name}' não encontrado{dc_msg} "
                f"no último scan concluído (job_id={latest_job_id}) para o vCenter '{vc.name}'."
            ),
        )

    total_itens, total_size_gb, breakdown = aggregate_datastore_rows(
        [(r.tipo_zombie, r.tamanho_gb) for r in rows]
    )

    snapshot = DatastoreDecomSnapshot(
        requested_vcenter_ref=str(body.vcenter_id),
        resolved_vcenter_id=vc.id,
        resolved_vcenter_name=vc.name,
        resolved_vcenter_host=vc.host,
        datacenter=body.datacenter,
        datastore_name=body.datastore_name,
        source_job_id=latest_job_id,
        total_itens=total_itens,
        total_size_gb=total_size_gb,
        breakdown=breakdown,
        generated_by=user.get("sub"),
        request_payload=body.model_dump(mode="json"),
    )
    db.add(snapshot)
    await db.flush()
    await db.refresh(snapshot)

    await _audit_datastore_report(
        db,
        request=request,
        analyst=user.get("sub", "unknown"),
        action="DATASTORE_SNAPSHOT",
        datastore_name=body.datastore_name,
        vcenter_id=str(vc.id),
        status_value="generated_snapshot",
        detail=(
            f"snapshot_id={snapshot.id}; source_job_id={latest_job_id}; "
            f"total_itens={total_itens}; total_size_gb={total_size_gb:.3f}"
        ),
    )

    return _snapshot_to_response(snapshot)


@router.post(
    "/datastore-laudos",
    response_model=DatastoreSnapshotResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Gerar laudo pre-exclusao de datastore",
    description="""
Gera um laudo pre-exclusao para datastore inteiro com base no ultimo scan concluido.
O laudo e persistido como snapshot auditavel.
    """,
)
async def create_datastore_laudo(
    body: DatastoreSnapshotCreateRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> DatastoreSnapshotResponse:
    return await create_datastore_snapshot(body, request, db, user)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# GET /datastore-snapshots/{id}
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


@router.get(
    "/datastore-snapshots/{snapshot_id}",
    response_model=DatastoreSnapshotResponse,
    summary="Consultar snapshot de descomissionamento por ID",
)
async def get_datastore_snapshot(
    snapshot_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreSnapshotResponse:
    snapshot = await db.get(DatastoreDecomSnapshot, snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot nÃ£o encontrado.")
    return _snapshot_to_response(snapshot)


# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
# GET /datastore-snapshots/{id}/export
# â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


@router.get(
    "/datastore-snapshots/{snapshot_id}/export",
    summary="Exportar snapshot de descomissionamento (CSV ou JSON)",
)
async def export_datastore_snapshot(
    snapshot_id: int,
    format: Annotated[str, Query(description="Formato de saÃ­da: csv | json")] = "csv",
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> StreamingResponse:
    snapshot = await db.get(DatastoreDecomSnapshot, snapshot_id)
    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot nÃ£o encontrado.")

    if format not in ("csv", "json"):
        raise HTTPException(
            status_code=422,
            detail="ParÃ¢metro 'format' deve ser 'csv' ou 'json'.",
        )

    payload = _snapshot_to_response(snapshot).model_dump(mode="json")

    if format == "json":
        content = json.dumps(payload, ensure_ascii=False, indent=2)
        return StreamingResponse(
            iter([content]),
            media_type="application/json; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="datastore_snapshot_{snapshot_id}.json"'
                ),
            },
        )

    breakdown: dict[str, int] = payload.get("breakdown", {})
    dynamic_type_columns = [k for k in sorted(breakdown) if k not in KNOWN_ZOMBIE_TYPES]
    all_type_columns = list(KNOWN_ZOMBIE_TYPES) + dynamic_type_columns
    fieldnames = _SNAPSHOT_CSV_FIELDS + all_type_columns

    row = {
        "snapshot_id": payload["id"],
        "timestamp": payload["timestamp"],
        "requested_vcenter_ref": payload["requested_vcenter_ref"],
        "resolved_vcenter_id": payload["resolved_vcenter_id"],
        "resolved_vcenter_name": payload["resolved_vcenter_name"],
        "resolved_vcenter_host": payload["resolved_vcenter_host"],
        "datacenter": payload.get("datacenter") or "",
        "datastore_name": payload["datastore_name"],
        "source_job_id": payload["source_job_id"],
        "total_itens": payload["total_itens"],
        "total_size_gb": f"{float(payload['total_size_gb']):.3f}",
        "conclusao": payload.get("conclusao", ""),
    }
    for t in all_type_columns:
        row[t] = int(breakdown.get(t, 0))

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\r\n")
    writer.writeheader()
    writer.writerow(row)

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="datastore_snapshot_{snapshot_id}.csv"'
            ),
        },
    )


@router.get(
    "/datastore-laudos/{snapshot_id}",
    response_model=DatastoreSnapshotResponse,
    summary="Consultar laudo pre-exclusao por ID",
)
async def get_datastore_laudo(
    snapshot_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreSnapshotResponse:
    return await get_datastore_snapshot(snapshot_id, db, _)


@router.get(
    "/datastore-laudos/{snapshot_id}/export",
    summary="Exportar laudo pre-exclusao (CSV ou JSON)",
)
async def export_datastore_laudo(
    snapshot_id: int,
    format: Annotated[str, Query(description="Formato de saída: csv | json")] = "csv",
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> StreamingResponse:
    return await export_datastore_snapshot(snapshot_id, format, db, _)


@router.get(
    "/jobs/{job_id}/executive-report",
    summary="Gerar relatorio executivo de descomissionamento por datastore",
    description="""
Gera um relatorio executivo em Markdown para apoiar decisao de exclusao
de datastore inteiro com base nos resultados de um job de scan.
    """,
)
async def get_executive_report(
    job_id: str,
    request: Request,
    datastore_name: Annotated[
        str,
        Query(min_length=1, description="Nome exato do datastore analisado."),
    ],
    datacenter: Annotated[
        str | None,
        Query(description="Datacenter opcional para filtrar o datastore."),
    ] = None,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> StreamingResponse:
    job = await _get_job_or_404(job_id, db)
    if job.status != "completed":
        raise HTTPException(
            status_code=409,
            detail=(
                f"Job '{job_id}' com status '{job.status}'. "
                "Aguarde status 'completed' para gerar relatorio executivo."
            ),
        )

    datastore_clean = datastore_name.strip()
    if not datastore_clean:
        raise HTTPException(status_code=422, detail="Parametro 'datastore_name' nao pode ser vazio.")

    datacenter_clean = datacenter.strip() if datacenter else None
    if datacenter is not None and not datacenter_clean:
        raise HTTPException(status_code=422, detail="Parametro 'datacenter' nao pode ser vazio.")

    rows_stmt = (
        select(
            ZombieVmdkRecord.tipo_zombie,
            ZombieVmdkRecord.tamanho_gb,
            ZombieVmdkRecord.vcenter_name,
            ZombieVmdkRecord.vcenter_host,
            ZombieVmdkRecord.datacenter,
        )
        .where(
            ZombieVmdkRecord.job_id == job_id,
            func.lower(ZombieVmdkRecord.datastore) == datastore_clean.lower(),
        )
    )
    if datacenter_clean:
        rows_stmt = rows_stmt.where(func.lower(ZombieVmdkRecord.datacenter) == datacenter_clean.lower())

    rows = (await db.execute(rows_stmt)).all()
    if not rows:
        dc_msg = f" no datacenter '{datacenter_clean}'" if datacenter_clean else ""
        raise HTTPException(
            status_code=404,
            detail=(
                f"Datastore '{datastore_clean}' nao encontrado{dc_msg} "
                f"no job '{job_id}'."
            ),
        )

    total_itens, total_size_gb, breakdown = aggregate_datastore_rows(
        [(r.tipo_zombie, r.tamanho_gb) for r in rows]
    )
    vcenter_names = sorted({(r.vcenter_name or "").strip() for r in rows if (r.vcenter_name or "").strip()})
    vcenter_hosts = sorted({(r.vcenter_host or "").strip() for r in rows if (r.vcenter_host or "").strip()})
    datacenter_value = datacenter_clean or next(
        ((r.datacenter or "").strip() for r in rows if (r.datacenter or "").strip()),
        None,
    )

    content = build_datastore_executive_report_markdown(
        job_id=job_id,
        datastore_name=datastore_clean,
        datacenter=datacenter_value,
        total_itens=total_itens,
        total_size_gb=total_size_gb,
        breakdown=breakdown,
        generated_at=datetime.now(timezone.utc),
        vcenter_hosts=vcenter_hosts,
        vcenter_names=vcenter_names,
    )

    await _audit_datastore_report(
        db,
        request=request,
        analyst=user.get("sub", "unknown"),
        action="DATASTORE_REPORT_MD",
        datastore_name=datastore_clean,
        vcenter_id=None,
        status_value="generated_report_md",
        detail=f"job_id={job_id}; total_itens={total_itens}; total_size_gb={total_size_gb:.3f}",
    )

    filename = f"relatorio_executivo_{job_id[:8]}_{_safe_report_filename(datastore_clean)}.md"
    return StreamingResponse(
        iter([content]),
        media_type="text/markdown; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
