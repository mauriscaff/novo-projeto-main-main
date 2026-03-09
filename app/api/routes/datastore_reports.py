"""
Endpoints de snapshots pre/pós exclusao de datastore.

Prefixo em main.py: /api/v1/datastore-reports
"""

from __future__ import annotations

import asyncio
import csv
import io
import json
import uuid
from datetime import datetime, timezone
from collections import Counter
from typing import Annotated, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from sqlalchemy import and_, case, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from app.core.datastore_report import KNOWN_ZOMBIE_TYPES
from app.core.scanner.zombie_detector import TIPOS_EXCLUIVEIS
from app.dependencies import get_current_user, get_db
from app.models.datastore_deletion_run import DatastoreDeletionVerificationRun
from app.models.datastore_report_snapshot import DatastoreDecommissionReport
from app.models.vcenter import VCenter
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from app.schemas.datastore_report import (
    DatastoreDeletionVerificationResponse,
    DatastoreDeletionVerificationTotalsResponse,
    DatastoreDeletedVmdkEvidence,
    DeletedVmdkItem,
    DatastoreReportCompareResponse,
    DatastoreReportFileVerificationResponse,
    DatastoreReportSnapshotCreateRequest,
    DatastoreReportSnapshotResponse,
    DatastoreReportTotals,
)
from config import get_settings

router = APIRouter()
settings = get_settings()


def _to_response(row: DatastoreDecommissionReport) -> DatastoreReportSnapshotResponse:
    return DatastoreReportSnapshotResponse(
        report_id=row.report_id,
        pair_id=row.pair_id,
        phase=row.phase,  # type: ignore[arg-type]
        job_id=row.job_id,
        datastore=row.datastore,
        vcenter_name=row.vcenter_name,
        vcenter_host=row.vcenter_host,
        total_items=row.total_items,
        total_size_gb=round(float(row.total_size_gb or 0.0), 3),
        deletable_items=row.deletable_items,
        deletable_size_gb=round(float(row.deletable_size_gb or 0.0), 3),
        breakdown={k: int(v) for k, v in (row.breakdown or {}).items()},
        created_at=row.created_at,
    )


async def _resolve_pair_reports(
    db: AsyncSession,
    pair_id: str,
) -> tuple[DatastoreDecommissionReport, DatastoreDecommissionReport]:
    pair_ref = pair_id.strip()
    rows_stmt = (
        select(DatastoreDecommissionReport)
        .where(DatastoreDecommissionReport.pair_id == pair_ref)
        .order_by(
            DatastoreDecommissionReport.created_at.desc(),
            DatastoreDecommissionReport.report_id.desc(),
        )
    )
    rows = (await db.execute(rows_stmt)).scalars().all()
    if not rows:
        raise HTTPException(status_code=404, detail=f"pair_id '{pair_ref}' nao encontrado.")

    pre = next((row for row in rows if row.phase == "pre_delete"), None)
    post = next((row for row in rows if row.phase == "post_delete"), None)

    if pre is None or post is None:
        raise HTTPException(
            status_code=422,
            detail=(
                f"pair_id '{pair_ref}' precisa ter snapshots pre_delete e post_delete "
                "para verificacao por arquivo."
            ),
        )

    if pre.datastore.lower() != post.datastore.lower():
        raise HTTPException(
            status_code=422,
            detail=(
                f"pair_id '{pair_ref}' inconsistente: pre_delete datastore='{pre.datastore}' "
                f"e post_delete datastore='{post.datastore}'."
            ),
        )

    return pre, post


def _normalize_tipo_zombie_filter(tipo_zombie: list[str] | None) -> list[str] | None:
    if not tipo_zombie:
        return None

    cleaned: list[str] = []
    for raw_value in tipo_zombie:
        value = (raw_value or "").strip().upper()
        if value:
            cleaned.append(value)

    if not cleaned:
        return None

    unknown = sorted(set(cleaned) - set(KNOWN_ZOMBIE_TYPES))
    if unknown:
        raise HTTPException(
            status_code=422,
            detail=(
                "Filtro 'tipo_zombie' invalido. Valores aceitos: "
                + ", ".join(KNOWN_ZOMBIE_TYPES)
                + ". "
                + "Recebido(s): "
                + ", ".join(unknown)
            ),
        )

    # Remove duplicados para evitar clauses IN desnecessarias.
    return sorted(set(cleaned))


def _raise_verify_timeout(pair_id: str, timeout_sec: int) -> None:
    raise HTTPException(
        status_code=504,
        detail=(
            f"Tempo limite excedido ({timeout_sec}s) para verificar pair_id '{pair_id.strip()}'. "
            "Reduza page_size, aplique filtros (tipo_zombie/min_size_gb) "
            "ou divida a consulta em paginas menores."
        ),
    )


def _scope_hosts(raw_hosts: str | None) -> list[str]:
    if not raw_hosts:
        return []
    hosts = [
        h.strip().lower()
        for h in str(raw_hosts).split(",")
        if h and h.strip() and h.strip().lower() not in {"n/a", "na"}
    ]
    return sorted(set(hosts))


def _apply_host_scope(stmt, column, hosts: list[str]):
    if not hosts:
        return stmt
    return stmt.where(func.lower(column).in_(hosts))


def _build_deleted_files_stmt(
    pre: DatastoreDecommissionReport,
    post: DatastoreDecommissionReport,
    *,
    scope_hosts: list[str] | None = None,
):
    pre_rec = aliased(ZombieVmdkRecord)
    post_rec = aliased(ZombieVmdkRecord)
    normalized_hosts = scope_hosts or []

    pre_grouped_stmt = (
        select(
            pre_rec.path.label("path"),
            func.max(func.coalesce(pre_rec.tamanho_gb, 0.0)).label("tamanho_gb"),
            func.min(pre_rec.tipo_zombie).label("tipo_zombie"),
            func.min(pre_rec.datacenter).label("datacenter"),
            func.min(pre_rec.vcenter_name).label("vcenter_name"),
            func.min(pre_rec.vcenter_host).label("vcenter_host"),
        )
        .where(
            pre_rec.job_id == pre.job_id,
            pre_rec.datastore == pre.datastore,
        )
    )
    pre_grouped_stmt = _apply_host_scope(pre_grouped_stmt, pre_rec.vcenter_host, normalized_hosts)
    pre_grouped = pre_grouped_stmt.group_by(pre_rec.path).subquery("pre_grouped")

    post_paths_stmt = (
        select(post_rec.path.label("path"))
        .where(
            post_rec.job_id == post.job_id,
            post_rec.datastore == post.datastore,
        )
    )
    post_paths_stmt = _apply_host_scope(post_paths_stmt, post_rec.vcenter_host, normalized_hosts)
    post_paths = post_paths_stmt.group_by(post_rec.path).subquery("post_paths")

    return (
        select(
            pre_grouped.c.path.label("path"),
            pre_grouped.c.tipo_zombie.label("tipo_zombie"),
            pre_grouped.c.tamanho_gb.label("tamanho_gb"),
            pre_grouped.c.datacenter.label("datacenter"),
            pre_grouped.c.vcenter_name.label("vcenter_name"),
            pre_grouped.c.vcenter_host.label("vcenter_host"),
        )
        .select_from(pre_grouped)
        .outerjoin(post_paths, post_paths.c.path == pre_grouped.c.path)
        .where(post_paths.c.path.is_(None))
    )


def _datastore_in_job_metrics(job: ZombieScanJob, datastore: str) -> bool:
    metrics = job.datastore_metrics or []
    target = datastore.strip().lower()
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        raw_name = metric.get("datastore_name") or metric.get("datastore") or ""
        if str(raw_name).strip().lower() == target:
            return True
    return False


def _job_sort_timestamp(job: ZombieScanJob) -> datetime:
    return (
        job.finished_at
        or job.started_at
        or job.created_at
        or datetime.min.replace(tzinfo=timezone.utc)
    )


def _normalize_scope_host(vcenter_host: str | None) -> str:
    return (vcenter_host or "").strip().lower()


def _scope_candidates(raw_scope: str | None) -> list[str]:
    value = _normalize_scope_host(raw_scope)
    if not value:
        return []
    base = value.split("://", 1)[-1].split("/", 1)[0].strip()
    candidates = {value, base}
    if ":" in base and base.count(":") == 1:
        candidates.add(base.split(":", 1)[0].strip())
    return sorted({c for c in candidates if c})


def _host_like_prefix(raw_scope: str | None) -> str | None:
    candidates = _scope_candidates(raw_scope)
    if not candidates:
        return None
    root = candidates[-1]
    if not root or ":" in root:
        return None
    return f"{root}:%"


def _build_vcenter_scope_condition(column_host, column_name, raw_scope: str):
    candidates = _scope_candidates(raw_scope)
    if not candidates:
        return None
    conditions = [
        func.lower(column_host).in_(candidates),
        func.lower(column_name).in_(candidates),
    ]
    prefix = _host_like_prefix(raw_scope)
    if prefix:
        conditions.append(func.lower(column_host).like(prefix))
    return or_(*conditions)


async def _persist_deletion_verification_run(
    db: AsyncSession,
    *,
    datastore: str,
    vcenter_host: str | None,
    baseline_job_id: str,
    verification_job_id: str,
    status_value: str,
    deleted_vmdk_count: int,
    deleted_size_gb: float,
    remaining_vmdk_count: int,
    remaining_size_gb: float,
) -> None:
    scope_host = _normalize_scope_host(vcenter_host)
    stmt = (
        select(DatastoreDeletionVerificationRun)
        .where(
            DatastoreDeletionVerificationRun.datastore == datastore,
            DatastoreDeletionVerificationRun.vcenter_host_scope == scope_host,
            DatastoreDeletionVerificationRun.baseline_job_id == baseline_job_id,
            DatastoreDeletionVerificationRun.verification_job_id == verification_job_id,
        )
        .limit(1)
    )
    existing = (await db.execute(stmt)).scalar_one_or_none()

    if existing is None:
        db.add(
            DatastoreDeletionVerificationRun(
                datastore=datastore,
                vcenter_host_scope=scope_host,
                baseline_job_id=baseline_job_id,
                verification_job_id=verification_job_id,
                status=status_value,
                deleted_vmdk_count=deleted_vmdk_count,
                deleted_size_gb=round(float(deleted_size_gb or 0.0), 3),
                remaining_vmdk_count=remaining_vmdk_count,
                remaining_size_gb=round(float(remaining_size_gb or 0.0), 3),
            )
        )
        return

    existing.status = status_value
    existing.deleted_vmdk_count = deleted_vmdk_count
    existing.deleted_size_gb = round(float(deleted_size_gb or 0.0), 3)
    existing.remaining_vmdk_count = remaining_vmdk_count
    existing.remaining_size_gb = round(float(remaining_size_gb or 0.0), 3)


async def _load_grouped_job_datastore_vmdks(
    db: AsyncSession,
    *,
    job_id: str,
    datastore: str,
    vcenter_host: str | None = None,
):
    stmt = (
        select(
            ZombieVmdkRecord.path.label("path"),
            func.max(func.coalesce(ZombieVmdkRecord.tamanho_gb, 0.0)).label("tamanho_gb"),
            func.min(ZombieVmdkRecord.tipo_zombie).label("tipo_zombie"),
            func.min(ZombieVmdkRecord.datacenter).label("datacenter"),
            func.min(ZombieVmdkRecord.vcenter_name).label("vcenter_name"),
            func.min(ZombieVmdkRecord.vcenter_host).label("vcenter_host"),
        )
        .where(
            ZombieVmdkRecord.job_id == job_id,
            func.lower(ZombieVmdkRecord.datastore) == datastore.strip().lower(),
        )
    )
    if vcenter_host:
        scope_condition = _build_vcenter_scope_condition(
            ZombieVmdkRecord.vcenter_host,
            ZombieVmdkRecord.vcenter_name,
            vcenter_host,
        )
        if scope_condition is not None:
            stmt = stmt.where(scope_condition)
    stmt = stmt.group_by(ZombieVmdkRecord.path)
    return (await db.execute(stmt)).all()


async def _build_file_verification_payload(
    db: AsyncSession,
    *,
    pair_id: str,
    page: int,
    page_size: int,
    include_evidence: bool = True,
    sort_by: str = "size_desc",
    export_all: bool = False,
    export_limit: int | None = None,
    tipo_zombie: list[str] | None = None,
    min_size_gb: float | None = None,
    include_deleted_limit: int | None = None,
) -> DatastoreReportFileVerificationResponse:
    pre, post = await _resolve_pair_reports(db, pair_id)
    scope_hosts = _scope_hosts(pre.vcenter_host)

    pre_any_stmt = (
        select(ZombieVmdkRecord.path)
        .where(
            ZombieVmdkRecord.job_id == pre.job_id,
            ZombieVmdkRecord.datastore == pre.datastore,
        )
        .limit(1)
    )
    pre_scoped_stmt = _apply_host_scope(pre_any_stmt, ZombieVmdkRecord.vcenter_host, scope_hosts)
    pre_any_exists = (await db.execute(pre_any_stmt)).scalar_one_or_none() is not None
    pre_scoped_exists = (await db.execute(pre_scoped_stmt)).scalar_one_or_none() is not None
    datastore_found_in_pre = pre_scoped_exists if scope_hosts else pre_any_exists
    if not datastore_found_in_pre:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Datastore '{pre.datastore}' nao encontrado no job pre_delete "
                f"'{pre.job_id}' para pair_id '{pair_id.strip()}'."
            ),
        )

    post_any_stmt = (
        select(ZombieVmdkRecord.path)
        .where(
            ZombieVmdkRecord.job_id == post.job_id,
            ZombieVmdkRecord.datastore == post.datastore,
        )
        .limit(1)
    )
    post_scoped_stmt = _apply_host_scope(post_any_stmt, ZombieVmdkRecord.vcenter_host, scope_hosts)
    post_any_exists = (await db.execute(post_any_stmt)).scalar_one_or_none() is not None
    post_scoped_exists = (await db.execute(post_scoped_stmt)).scalar_one_or_none() is not None
    datastore_found_in_post = post_any_exists

    datastore_status: Literal["removed", "still_present", "unknown"]
    if not datastore_found_in_post:
        datastore_status = "removed"
    elif scope_hosts and not post_scoped_exists:
        datastore_status = "unknown"
    else:
        datastore_status = "still_present"

    deleted_subq = _build_deleted_files_stmt(
        pre,
        post,
        scope_hosts=scope_hosts,
    ).subquery("deleted_files")

    filtered_deleted_stmt = select(
        deleted_subq.c.path,
        deleted_subq.c.tipo_zombie,
        deleted_subq.c.tamanho_gb,
        deleted_subq.c.datacenter,
        deleted_subq.c.vcenter_name,
        deleted_subq.c.vcenter_host,
    )
    if datastore_status != "removed" and tipo_zombie:
        filtered_deleted_stmt = filtered_deleted_stmt.where(deleted_subq.c.tipo_zombie.in_(tipo_zombie))
    if datastore_status != "removed" and min_size_gb is not None:
        filtered_deleted_stmt = filtered_deleted_stmt.where(
            func.coalesce(deleted_subq.c.tamanho_gb, 0.0) >= min_size_gb
        )
    filtered_deleted_subq = filtered_deleted_stmt.subquery("filtered_deleted_files")

    if datastore_status == "unknown":
        deleted_files_count = 0
        deleted_size_gb = 0.0
        size_gain_gb = 0.0
        deleted_breakdown: dict[str, int] = {}
        deleted_size_breakdown_gb: dict[str, float] = {}
    elif datastore_status == "removed":
        deleted_files_count = int(pre.total_items or 0)
        deleted_size_gb = round(float(pre.total_size_gb or 0.0), 3)
        size_gain_gb = deleted_size_gb
        deleted_breakdown = {
            str(k): int(v)
            for k, v in (pre.breakdown or {}).items()
        }

        pre_size_breakdown_stmt = (
            select(
                ZombieVmdkRecord.tipo_zombie,
                func.coalesce(func.sum(ZombieVmdkRecord.tamanho_gb), 0.0).label("size_by_type"),
            )
            .where(
                ZombieVmdkRecord.job_id == pre.job_id,
                ZombieVmdkRecord.datastore == pre.datastore,
            )
            .group_by(ZombieVmdkRecord.tipo_zombie)
            .order_by(ZombieVmdkRecord.tipo_zombie.asc())
        )
        pre_size_breakdown_stmt = _apply_host_scope(
            pre_size_breakdown_stmt,
            ZombieVmdkRecord.vcenter_host,
            scope_hosts,
        )
        pre_size_breakdown_rows = (await db.execute(pre_size_breakdown_stmt)).all()
        deleted_size_breakdown_gb = {
            str(r.tipo_zombie): round(float(r.size_by_type or 0.0), 3)
            for r in pre_size_breakdown_rows
        }
    else:
        total_stmt = select(
            func.count().label("total_files"),
            func.coalesce(func.sum(filtered_deleted_subq.c.tamanho_gb), 0.0).label("total_size"),
        )
        total_row = (await db.execute(total_stmt)).one()
        deleted_files_count = int(total_row.total_files or 0)
        deleted_size_gb = round(float(total_row.total_size or 0.0), 3)
        size_gain_gb = deleted_size_gb

        breakdown_stmt = (
            select(
                filtered_deleted_subq.c.tipo_zombie,
                func.count().label("count_by_type"),
                func.coalesce(func.sum(filtered_deleted_subq.c.tamanho_gb), 0.0).label("size_by_type"),
            )
            .group_by(filtered_deleted_subq.c.tipo_zombie)
            .order_by(filtered_deleted_subq.c.tipo_zombie.asc())
        )
        breakdown_rows = (await db.execute(breakdown_stmt)).all()
        deleted_breakdown = {
            str(r.tipo_zombie): int(r.count_by_type)
            for r in breakdown_rows
        }
        deleted_size_breakdown_gb = {
            str(r.tipo_zombie): round(float(r.size_by_type or 0.0), 3)
            for r in breakdown_rows
        }

    pre_total_size = round(float(pre.total_size_gb or 0.0), 3)
    post_total_size = 0.0 if datastore_status == "removed" else round(float(post.total_size_gb or 0.0), 3)
    remaining_size_gb = 0.0 if datastore_status == "removed" else post_total_size
    size_gain_percent = round((size_gain_gb / pre_total_size) * 100.0, 2) if pre_total_size > 0 else 0.0
    remaining_files_count = 0 if datastore_status == "removed" else int(post.total_items or 0)
    has_more_evidence = False

    deleted_vmdks: list[DatastoreDeletedVmdkEvidence] = []
    if include_evidence and datastore_status != "unknown":
        if export_all and export_limit is None:
            raise HTTPException(
                status_code=422,
                detail="Exportacao sem limite explicito nao permitida para evitar uso excessivo de memoria.",
            )
        effective_page_size = page_size
        if include_deleted_limit is not None:
            effective_page_size = max(1, min(page_size, include_deleted_limit))

        offset = max(page - 1, 0) * effective_page_size
        if not export_all and (deleted_files_count == 0 or offset >= deleted_files_count):
            # Evita round-trip adicional com OFFSET alto quando a pagina ja esta fora do total.
            has_more_evidence = False
            rows = []
        else:
            evidence_stmt = select(
                filtered_deleted_subq.c.path,
                filtered_deleted_subq.c.tipo_zombie,
                filtered_deleted_subq.c.tamanho_gb,
                filtered_deleted_subq.c.datacenter,
                filtered_deleted_subq.c.vcenter_name,
                filtered_deleted_subq.c.vcenter_host,
            )
            if sort_by == "size_asc":
                evidence_stmt = evidence_stmt.order_by(
                    filtered_deleted_subq.c.tamanho_gb.asc(),
                    filtered_deleted_subq.c.path.asc(),
                )
            elif sort_by == "path_desc":
                evidence_stmt = evidence_stmt.order_by(filtered_deleted_subq.c.path.desc())
            elif sort_by == "path_asc":
                evidence_stmt = evidence_stmt.order_by(filtered_deleted_subq.c.path.asc())
            else:
                evidence_stmt = evidence_stmt.order_by(
                    filtered_deleted_subq.c.tamanho_gb.desc(),
                    filtered_deleted_subq.c.path.asc(),
                )

            if export_all:
                if export_limit is not None:
                    evidence_stmt = evidence_stmt.limit(export_limit)
            else:
                evidence_stmt = evidence_stmt.offset(offset).limit(effective_page_size)
                has_more_evidence = deleted_files_count > (page * effective_page_size)

            rows = (await db.execute(evidence_stmt)).all()
        deleted_vmdks = [
            DatastoreDeletedVmdkEvidence(
                path=str(r.path),
                tipo_zombie=str(r.tipo_zombie),
                tamanho_gb=round(float(r.tamanho_gb or 0.0), 3),
                last_seen_job_id=pre.job_id,
                datacenter=str(r.datacenter) if r.datacenter else None,
                vcenter_name=str(r.vcenter_name) if r.vcenter_name else None,
                vcenter_host=str(r.vcenter_host) if r.vcenter_host else None,
            )
            for r in rows
        ]

    verification_status = "no_gain"
    if deleted_files_count > 0 and remaining_files_count <= 0:
        verification_status = "fully_removed"
    elif deleted_files_count > 0:
        verification_status = "partially_removed"

    filter_parts: list[str] = []
    if tipo_zombie:
        filter_parts.append(f"tipo_zombie={','.join(tipo_zombie)}")
    if min_size_gb is not None:
        filter_parts.append(f"min_size_gb>={min_size_gb:.3f}")
    filter_suffix = f" (filtros: {'; '.join(filter_parts)})" if filter_parts else ""

    if datastore_status == "unknown":
        status_text = "warning"
        message = (
            f"pair_id '{pair_id.strip()}' com escopo inconclusivo para datastore entre pre/post "
            "(datastore encontrado no pos-scan apenas fora do escopo esperado de vcenter_host)."
        )
    elif datastore_status == "removed":
        status_text = "ok"
        message = (
            f"Datastore '{pre.datastore}' nao encontrado no pos-scan do pair_id '{pair_id.strip()}'; "
            "considerando remocao completa do baseline pre_delete."
        )
    elif deleted_files_count > 0:
        status_text = "ok"
        message = (
            f"{deleted_files_count} arquivo(s) removido(s) identificado(s) no pair_id '{pair_id.strip()}'"
            f"{filter_suffix}."
        )
    else:
        status_text = "ok"
        message = f"Nenhum arquivo removido identificado no pair_id '{pair_id.strip()}'{filter_suffix}."

    return DatastoreReportFileVerificationResponse(
        pair_id=pair_id.strip(),
        datastore=pre.datastore,
        datastore_name=pre.datastore,
        pre_report_id=pre.report_id,
        post_report_id=post.report_id,
        pre_job_id=pre.job_id,
        post_job_id=post.job_id,
        datastore_found_in_pre=datastore_found_in_pre,
        datastore_found_in_post=datastore_found_in_post,
        datastore_status=datastore_status,
        removed_files_count=deleted_files_count,
        removed_size_gb=deleted_size_gb,
        deleted_files_count=deleted_files_count,
        deleted_size_gb=deleted_size_gb,
        size_gain_gb=size_gain_gb,
        size_gain_percent=size_gain_percent,
        pre_total_size_gb=pre_total_size,
        post_total_size_gb=post_total_size,
        remaining_size_gb=remaining_size_gb,
        deleted_breakdown=deleted_breakdown,
        deleted_size_breakdown_gb=deleted_size_breakdown_gb,
        remaining_files_count=remaining_files_count,
        verification_status=verification_status,
        page=page,
        page_size=page_size,
        total_evidence=deleted_files_count,
        has_more_evidence=has_more_evidence,
        status=status_text,
        message=message,
        deleted_vmdks=deleted_vmdks,
    )


@router.post(
    "/snapshots",
    response_model=DatastoreReportSnapshotResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Criar snapshot pre/pós de datastore",
)
async def create_snapshot(
    body: DatastoreReportSnapshotCreateRequest,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreReportSnapshotResponse:
    job = await db.get(ZombieScanJob, body.job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{body.job_id}' nao encontrado.")

    rows_stmt = select(
        ZombieVmdkRecord.datastore,
        ZombieVmdkRecord.tipo_zombie,
        ZombieVmdkRecord.tamanho_gb,
        ZombieVmdkRecord.vcenter_name,
        ZombieVmdkRecord.vcenter_host,
    ).where(
        ZombieVmdkRecord.job_id == body.job_id,
        func.lower(ZombieVmdkRecord.datastore) == body.datastore.lower(),
    )
    rows = (await db.execute(rows_stmt)).all()
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"Datastore '{body.datastore}' sem registros no job '{body.job_id}'.",
        )

    if body.phase == "post_delete" and body.pair_id:
        pair_pre_stmt = (
            select(DatastoreDecommissionReport.report_id)
            .where(
                DatastoreDecommissionReport.pair_id == body.pair_id,
                DatastoreDecommissionReport.phase == "pre_delete",
                func.lower(DatastoreDecommissionReport.datastore) == body.datastore.lower(),
            )
            .limit(1)
        )
        pre_ref = (await db.execute(pair_pre_stmt)).scalar_one_or_none()
        if pre_ref is None:
            raise HTTPException(
                status_code=422,
                detail=(
                    "pair_id informado para post_delete nao possui snapshot pre_delete "
                    f"do datastore '{body.datastore}'."
                ),
            )

    total_items = len(rows)
    total_size_gb = round(sum(float(r.tamanho_gb or 0.0) for r in rows), 3)
    breakdown_counter = Counter(str(r.tipo_zombie) for r in rows)
    breakdown = {k: int(v) for k, v in breakdown_counter.items()}

    deletable_rows = [r for r in rows if str(r.tipo_zombie) in TIPOS_EXCLUIVEIS]
    deletable_items = len(deletable_rows)
    deletable_size_gb = round(sum(float(r.tamanho_gb or 0.0) for r in deletable_rows), 3)

    vcenter_names = sorted({(r.vcenter_name or "").strip() for r in rows if (r.vcenter_name or "").strip()})
    vcenter_hosts = sorted({(r.vcenter_host or "").strip() for r in rows if (r.vcenter_host or "").strip()})
    datastores = sorted({(r.datastore or "").strip() for r in rows if (r.datastore or "").strip()})
    datastore_name = datastores[0] if datastores else body.datastore

    pair_id = body.pair_id or uuid.uuid4().hex
    report = DatastoreDecommissionReport(
        pair_id=pair_id,
        phase=body.phase,
        job_id=body.job_id,
        datastore=datastore_name,
        vcenter_name=", ".join(vcenter_names) or "N/A",
        vcenter_host=", ".join(vcenter_hosts) or "N/A",
        total_items=total_items,
        total_size_gb=total_size_gb,
        deletable_items=deletable_items,
        deletable_size_gb=deletable_size_gb,
        breakdown=breakdown,
    )
    db.add(report)
    await db.flush()
    await db.refresh(report)
    return _to_response(report)


@router.get(
    "/snapshots/{report_id}",
    response_model=DatastoreReportSnapshotResponse,
    summary="Buscar snapshot por report_id",
)
async def get_snapshot(
    report_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreReportSnapshotResponse:
    row = await db.get(DatastoreDecommissionReport, report_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"Report '{report_id}' nao encontrado.")
    return _to_response(row)


@router.get(
    "/compare",
    response_model=DatastoreReportCompareResponse,
    summary="Comparar snapshot pre_delete x post_delete",
)
async def compare_snapshots(
    pre_report_id: Annotated[int, Query(description="report_id do snapshot pre_delete")],
    post_report_id: Annotated[int, Query(description="report_id do snapshot post_delete")],
    strict_pair: Annotated[
        bool,
        Query(
            description=(
                "Quando true, exige que pre/post tenham o mesmo pair_id "
                "(governanca de pareamento estrito)."
            )
        ),
    ] = False,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreReportCompareResponse:
    pre = await db.get(DatastoreDecommissionReport, pre_report_id)
    if not pre:
        raise HTTPException(status_code=404, detail=f"Report '{pre_report_id}' nao encontrado.")

    post = await db.get(DatastoreDecommissionReport, post_report_id)
    if not post:
        raise HTTPException(status_code=404, detail=f"Report '{post_report_id}' nao encontrado.")

    if pre.phase != "pre_delete":
        raise HTTPException(status_code=422, detail=f"Report '{pre_report_id}' deve ser pre_delete.")
    if post.phase != "post_delete":
        raise HTTPException(status_code=422, detail=f"Report '{post_report_id}' deve ser post_delete.")

    if pre.datastore.lower() != post.datastore.lower():
        raise HTTPException(status_code=422, detail="Compare exige snapshots do mesmo datastore.")
    if strict_pair and pre.pair_id != post.pair_id:
        raise HTTPException(
            status_code=422,
            detail=(
                "Compare com strict_pair=true exige snapshots com o mesmo pair_id "
                f"(pre='{pre.pair_id}', post='{post.pair_id}')."
            ),
        )

    removed_breakdown: dict[str, int] = {}
    all_types = set((pre.breakdown or {}).keys()) | set((post.breakdown or {}).keys())
    for zombie_type in sorted(all_types):
        pre_count = int((pre.breakdown or {}).get(zombie_type, 0))
        post_count = int((post.breakdown or {}).get(zombie_type, 0))
        delta = pre_count - post_count
        if delta > 0:
            removed_breakdown[zombie_type] = delta

    removed_items_breakdown = int(sum(removed_breakdown.values()))
    removed_items_totals = max(int(pre.total_items or 0) - int(post.total_items or 0), 0)
    removed_items = max(removed_items_breakdown, removed_items_totals)
    removed_size_gb = round(max(float(pre.total_size_gb or 0.0) - float(post.total_size_gb or 0.0), 0.0), 3)

    return DatastoreReportCompareResponse(
        pre_report_id=pre.report_id,
        post_report_id=post.report_id,
        datastore=pre.datastore,
        removed_items=removed_items,
        removed_size_gb=removed_size_gb,
        removed_breakdown=removed_breakdown,
        pre_totals=DatastoreReportTotals(
            total_items=pre.total_items,
            total_size_gb=round(float(pre.total_size_gb or 0.0), 3),
            deletable_items=pre.deletable_items,
            deletable_size_gb=round(float(pre.deletable_size_gb or 0.0), 3),
        ),
        post_totals=DatastoreReportTotals(
            total_items=post.total_items,
            total_size_gb=round(float(post.total_size_gb or 0.0), 3),
            deletable_items=post.deletable_items,
            deletable_size_gb=round(float(post.deletable_size_gb or 0.0), 3),
        ),
    )


@router.get(
    "/datastore-deletion-verification/totals",
    response_model=DatastoreDeletionVerificationTotalsResponse,
    summary="Totais acumulados de exclusao verificada por datastore",
)
async def get_datastore_deletion_verification_totals(
    datastore: Annotated[
        str | None,
        Query(description="Filtro opcional por datastore."),
    ] = None,
    vcenter_host: Annotated[
        str | None,
        Query(description="Filtro opcional por host de vCenter."),
    ] = None,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreDeletionVerificationTotalsResponse:
    datastore_ref = datastore.strip() if datastore and datastore.strip() else None
    host_ref_raw = vcenter_host.strip() if vcenter_host and vcenter_host.strip() else None
    host_ref = _normalize_scope_host(host_ref_raw)

    stmt = select(
        func.count(DatastoreDeletionVerificationRun.id).label("total_verifications"),
        func.sum(
            case(
                (DatastoreDeletionVerificationRun.status == "datastore_removed", 1),
                else_=0,
            )
        ).label("total_datastores_removed"),
        func.sum(
            case(
                (DatastoreDeletionVerificationRun.status == "partial_cleanup", 1),
                else_=0,
            )
        ).label("total_partial_cleanup"),
        func.sum(
            case(
                (DatastoreDeletionVerificationRun.status == "no_cleanup", 1),
                else_=0,
            )
        ).label("total_no_cleanup"),
        func.sum(DatastoreDeletionVerificationRun.deleted_vmdk_count).label("total_deleted_vmdks"),
        func.sum(DatastoreDeletionVerificationRun.deleted_size_gb).label("total_deleted_size_gb"),
        func.max(DatastoreDeletionVerificationRun.created_at).label("last_verification_at"),
    )

    if datastore_ref:
        stmt = stmt.where(func.lower(DatastoreDeletionVerificationRun.datastore) == datastore_ref.lower())
    if host_ref_raw is not None:
        stmt = stmt.where(DatastoreDeletionVerificationRun.vcenter_host_scope == host_ref)

    row = (await db.execute(stmt)).one()

    return DatastoreDeletionVerificationTotalsResponse(
        datastore=datastore_ref,
        vcenter_host=host_ref_raw,
        total_verifications=int(row.total_verifications or 0),
        total_datastores_removed=int(row.total_datastores_removed or 0),
        total_partial_cleanup=int(row.total_partial_cleanup or 0),
        total_no_cleanup=int(row.total_no_cleanup or 0),
        total_deleted_vmdks=int(row.total_deleted_vmdks or 0),
        total_deleted_size_gb=round(float(row.total_deleted_size_gb or 0.0), 3),
        last_verification_at=row.last_verification_at,
    )


@router.get(
    "/datastore-deletion-verification",
    response_model=DatastoreDeletionVerificationResponse,
    summary="Verificar exclusao de datastore sem pair_id (baseline x verification automaticos)",
)
async def verify_datastore_deletion_without_pair(
    datastore: Annotated[str, Query(min_length=1, description="Nome do datastore a verificar.")],
    vcenter_host: Annotated[
        str | None,
        Query(description="Host do vCenter para restringir escopo (opcional)."),
    ] = None,
    evidence_limit: Annotated[
        int,
        Query(
            ge=1,
            le=5000,
            description="Quantidade maxima de evidencias deleted_vmdks retornadas.",
        ),
    ] = 200,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreDeletionVerificationResponse:
    datastore_ref = datastore.strip()
    host_ref_raw = vcenter_host.strip() if vcenter_host and vcenter_host.strip() else None
    host_ref = host_ref_raw.lower() if host_ref_raw else None

    jobs_stmt = select(ZombieScanJob).where(ZombieScanJob.status == "completed")
    completed_jobs = (await db.execute(jobs_stmt)).scalars().all()

    if host_ref:
        host_candidates = _scope_candidates(host_ref_raw)
        vcenter_id_stmt = (
            select(VCenter.id, VCenter.name, VCenter.host)
            .where(
                or_(
                    func.lower(VCenter.host).in_(host_candidates),
                    func.lower(VCenter.name).in_(host_candidates),
                )
            )
            .limit(1)
        )
        vcenter_row = (await db.execute(vcenter_id_stmt)).first()
        if vcenter_row is None:
            completed_jobs = []
        else:
            vcenter_id = int(vcenter_row.id)
            preferred_scope = host_ref_raw or vcenter_row.host or vcenter_row.name
            completed_jobs = [
                job for job in completed_jobs if vcenter_id in [int(v) for v in (job.vcenter_ids or [])]
            ]
            host_ref_raw = preferred_scope
            host_ref = _normalize_scope_host(preferred_scope)

    completed_jobs = sorted(completed_jobs, key=_job_sort_timestamp, reverse=True)
    if not completed_jobs:
        raise HTTPException(
            status_code=404,
            detail=(
                "Nenhum scan de verificacao (status completed) foi encontrado para o escopo informado."
            ),
        )

    verification_job = completed_jobs[0]

    baseline_job: ZombieScanJob | None = None
    for candidate in completed_jobs[1:]:
        candidate_exists_stmt = (
            select(ZombieVmdkRecord.path)
            .where(
                ZombieVmdkRecord.job_id == candidate.job_id,
                func.lower(ZombieVmdkRecord.datastore) == datastore_ref.lower(),
            )
            .limit(1)
        )
        if host_ref:
            scope_condition = _build_vcenter_scope_condition(
                ZombieVmdkRecord.vcenter_host,
                ZombieVmdkRecord.vcenter_name,
                host_ref_raw or host_ref,
            )
            if scope_condition is not None:
                candidate_exists_stmt = candidate_exists_stmt.where(scope_condition)

        candidate_has_records = (await db.execute(candidate_exists_stmt)).scalar_one_or_none() is not None
        if candidate_has_records or _datastore_in_job_metrics(candidate, datastore_ref):
            baseline_job = candidate
            break

    if baseline_job is None:
        raise HTTPException(
            status_code=404,
            detail=(
                f"Nao foi encontrado baseline anterior ao scan '{verification_job.job_id}' "
                f"onde o datastore '{datastore_ref}' existia."
            ),
        )

    baseline_rows = await _load_grouped_job_datastore_vmdks(
        db,
        job_id=baseline_job.job_id,
        datastore=datastore_ref,
        vcenter_host=host_ref,
    )
    verification_rows = await _load_grouped_job_datastore_vmdks(
        db,
        job_id=verification_job.job_id,
        datastore=datastore_ref,
        vcenter_host=host_ref,
    )

    baseline_map = {str(r.path): r for r in baseline_rows}
    verification_map = {str(r.path): r for r in verification_rows}
    baseline_paths = set(baseline_map.keys())
    verification_paths = set(verification_map.keys())

    deleted_paths = baseline_paths - verification_paths
    remaining_paths = baseline_paths & verification_paths

    deleted_records = [baseline_map[p] for p in deleted_paths]
    remaining_records = [verification_map.get(p) or baseline_map[p] for p in remaining_paths]

    baseline_files_count = len(baseline_rows)
    verification_files_count = len(verification_rows)
    deleted_vmdk_count = len(deleted_records)
    remaining_vmdk_count = len(remaining_records)

    deleted_size_gb = round(sum(float(r.tamanho_gb or 0.0) for r in deleted_records), 3)
    remaining_size_gb = round(sum(float(r.tamanho_gb or 0.0) for r in remaining_records), 3)
    baseline_size_gb = round(sum(float(r.tamanho_gb or 0.0) for r in baseline_rows), 3)

    deleted_breakdown_counter = Counter(str(r.tipo_zombie or "UNKNOWN") for r in deleted_records)
    deleted_breakdown = {k: int(v) for k, v in deleted_breakdown_counter.items()}
    deleted_size_breakdown_gb: dict[str, float] = {}
    for r in deleted_records:
        zombie_type = str(r.tipo_zombie or "UNKNOWN")
        deleted_size_breakdown_gb[zombie_type] = round(
            float(deleted_size_breakdown_gb.get(zombie_type, 0.0)) + float(r.tamanho_gb or 0.0),
            3,
        )

    datastore_found_in_verification_records = len(verification_rows) > 0
    datastore_found_in_verification_metrics = _datastore_in_job_metrics(verification_job, datastore_ref)
    datastore_removed = (
        not datastore_found_in_verification_records and not datastore_found_in_verification_metrics
    )

    if datastore_removed:
        status_value: Literal["datastore_removed", "partial_cleanup", "no_cleanup"] = "datastore_removed"
        message = (
            f"Datastore '{datastore_ref}' nao aparece no scan de verificacao "
            f"'{verification_job.job_id}' (registros e metricas)."
        )
    elif deleted_vmdk_count > 0:
        status_value = "partial_cleanup"
        message = (
            f"Limpeza parcial detectada para '{datastore_ref}': "
            f"{deleted_vmdk_count} VMDK(s) removido(s) e {remaining_vmdk_count} remanescente(s)."
        )
    else:
        status_value = "no_cleanup"
        message = (
            f"Nenhuma remocao de VMDKs foi identificada para '{datastore_ref}' "
            "entre baseline e verificacao."
        )

    size_gain_percent = round((deleted_size_gb / baseline_size_gb) * 100.0, 2) if baseline_size_gb > 0 else 0.0
    baseline_size_tb = round(baseline_size_gb / 1024.0, 6)
    deleted_size_tb = round(deleted_size_gb / 1024.0, 6)
    remaining_size_tb = round(remaining_size_gb / 1024.0, 6)

    sorted_deleted_records = sorted(
        deleted_records,
        key=lambda r: (float(r.tamanho_gb or 0.0), str(r.path)),
        reverse=True,
    )[:evidence_limit]

    deleted_vmdks = [
        DeletedVmdkItem(
            path=str(r.path),
            tipo_zombie=str(r.tipo_zombie or "UNKNOWN"),
            tamanho_gb=round(float(r.tamanho_gb or 0.0), 3),
            last_seen_job_id=baseline_job.job_id,
            datacenter=str(r.datacenter) if r.datacenter else None,
        )
        for r in sorted_deleted_records
    ]

    await _persist_deletion_verification_run(
        db,
        datastore=datastore_ref,
        vcenter_host=host_ref_raw,
        baseline_job_id=baseline_job.job_id,
        verification_job_id=verification_job.job_id,
        status_value=status_value,
        deleted_vmdk_count=deleted_vmdk_count,
        deleted_size_gb=deleted_size_gb,
        remaining_vmdk_count=remaining_vmdk_count,
        remaining_size_gb=remaining_size_gb,
    )
    await db.commit()

    return DatastoreDeletionVerificationResponse(
        datastore=datastore_ref,
        vcenter_host=host_ref_raw,
        verification_job_id=verification_job.job_id,
        baseline_job_id=baseline_job.job_id,
        datastore_removed=datastore_removed,
        status=status_value,
        message=message,
        baseline_files_count=baseline_files_count,
        verification_files_count=verification_files_count,
        deleted_vmdk_count=deleted_vmdk_count,
        remaining_vmdk_count=remaining_vmdk_count,
        baseline_size_gb=baseline_size_gb,
        baseline_size_tb=baseline_size_tb,
        deleted_size_gb=deleted_size_gb,
        deleted_size_tb=deleted_size_tb,
        remaining_size_gb=remaining_size_gb,
        remaining_size_tb=remaining_size_tb,
        size_gain_percent=size_gain_percent,
        deleted_breakdown=deleted_breakdown,
        deleted_size_breakdown_gb=deleted_size_breakdown_gb,
        deleted_vmdks=deleted_vmdks,
    )


@router.get(
    "/verify-files/{pair_id}",
    response_model=DatastoreReportFileVerificationResponse,
    summary="Verificar exclusao por arquivo (evidencia VMDK) via pair_id",
)
async def verify_deleted_files_by_pair(
    pair_id: str,
    page: Annotated[int, Query(ge=1, description="Pagina da evidência (base 1)")] = 1,
    page_size: Annotated[
        int,
        Query(ge=1, le=1000, description="Itens por pagina da evidência"),
    ] = 200,
    tipo_zombie: Annotated[
        list[str] | None,
        Query(description="Filtro opcional por tipo_zombie (parametro repetivel)."),
    ] = None,
    min_size_gb: Annotated[
        float | None,
        Query(ge=0.0, description="Filtro opcional por tamanho minimo (GB)."),
    ] = None,
    sort_by: Annotated[
        Literal["size_desc", "size_asc", "path_asc", "path_desc"],
        Query(description="Ordenacao das evidencias retornadas."),
    ] = "size_desc",
    include_deleted_vmdks: Annotated[
        bool,
        Query(description="Quando false, retorna somente agregados sem listar deleted_vmdks."),
    ] = True,
    include_deleted_limit: Annotated[
        int | None,
        Query(
            ge=1,
            le=5000,
            description="Limite opcional de evidencias no campo deleted_vmdks (cap de page_size).",
        ),
    ] = None,
    timeout_sec: Annotated[
        int,
        Query(ge=1, le=120, description="Timeout defensivo da consulta (segundos)."),
    ] = settings.datastore_reports_verify_timeout_sec,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreReportFileVerificationResponse:
    tipos_filtrados = _normalize_tipo_zombie_filter(tipo_zombie)
    try:
        return await asyncio.wait_for(
            _build_file_verification_payload(
                db,
                pair_id=pair_id,
                page=page,
                page_size=page_size,
                include_evidence=include_deleted_vmdks,
                sort_by=sort_by,
                export_all=False,
                tipo_zombie=tipos_filtrados,
                min_size_gb=min_size_gb,
                include_deleted_limit=include_deleted_limit,
            ),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        _raise_verify_timeout(pair_id, timeout_sec)


@router.get(
    "/post-exclusion-file-verification/{pair_id}",
    response_model=DatastoreReportFileVerificationResponse,
    summary="Alias: verificar exclusao por arquivo (VMDK) via pair_id",
)
async def post_exclusion_file_verification(
    pair_id: str,
    page: Annotated[int, Query(ge=1, description="Pagina da evidência (base 1)")] = 1,
    page_size: Annotated[
        int,
        Query(ge=1, le=1000, description="Itens por pagina da evidência"),
    ] = 200,
    tipo_zombie: Annotated[
        list[str] | None,
        Query(description="Filtro opcional por tipo_zombie (parametro repetivel)."),
    ] = None,
    min_size_gb: Annotated[
        float | None,
        Query(ge=0.0, description="Filtro opcional por tamanho minimo (GB)."),
    ] = None,
    sort_by: Annotated[
        Literal["size_desc", "size_asc", "path_asc", "path_desc"],
        Query(description="Ordenacao das evidencias retornadas."),
    ] = "size_desc",
    include_deleted_vmdks: Annotated[
        bool,
        Query(description="Quando false, retorna somente agregados sem listar deleted_vmdks."),
    ] = True,
    include_deleted_limit: Annotated[
        int,
        Query(ge=1, le=5000, description="Limite maximo de evidencias no campo deleted_vmdks."),
    ] = 200,
    timeout_sec: Annotated[
        int,
        Query(ge=1, le=120, description="Timeout defensivo da consulta (segundos)."),
    ] = settings.datastore_reports_verify_timeout_sec,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> DatastoreReportFileVerificationResponse:
    return await verify_deleted_files_by_pair(
        pair_id=pair_id,
        page=page,
        page_size=page_size,
        tipo_zombie=tipo_zombie,
        min_size_gb=min_size_gb,
        sort_by=sort_by,
        include_deleted_vmdks=include_deleted_vmdks,
        include_deleted_limit=include_deleted_limit,
        timeout_sec=timeout_sec,
        db=db,
        _=_,
    )


@router.get(
    "/verify-files/{pair_id}/export",
    summary="Exportar evidencias de exclusao por arquivo (CSV/JSON) via pair_id",
)
async def export_deleted_files_by_pair(
    pair_id: str,
    format: Annotated[str, Query(description="Formato de exportacao: csv | json")] = "csv",
    max_rows: Annotated[
        int,
        Query(
            ge=1,
            le=200000,
            description="Limite maximo de evidencias exportadas para evitar payload excessivo.",
        ),
    ] = 50000,
    tipo_zombie: Annotated[
        list[str] | None,
        Query(description="Filtro opcional por tipo_zombie (parametro repetivel)."),
    ] = None,
    min_size_gb: Annotated[
        float | None,
        Query(ge=0.0, description="Filtro opcional por tamanho minimo (GB)."),
    ] = None,
    timeout_sec: Annotated[
        int,
        Query(ge=1, le=120, description="Timeout defensivo da consulta (segundos)."),
    ] = settings.datastore_reports_verify_timeout_sec,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> StreamingResponse:
    if format not in {"csv", "json"}:
        raise HTTPException(status_code=422, detail="Parametro 'format' deve ser 'csv' ou 'json'.")

    tipos_filtrados = _normalize_tipo_zombie_filter(tipo_zombie)
    try:
        summary = await asyncio.wait_for(
            _build_file_verification_payload(
                db,
                pair_id=pair_id,
                page=1,
                page_size=1,
                include_evidence=False,
                sort_by="size_desc",
                export_all=False,
                tipo_zombie=tipos_filtrados,
                min_size_gb=min_size_gb,
            ),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        _raise_verify_timeout(pair_id, timeout_sec)
    if summary.total_evidence > max_rows:
        raise HTTPException(
            status_code=422,
            detail=(
                f"Quantidade de evidencias ({summary.total_evidence}) excede max_rows={max_rows}. "
                "Ajuste max_rows ou use paginação no endpoint /verify-files/{pair_id}."
            ),
        )

    try:
        payload = await asyncio.wait_for(
            _build_file_verification_payload(
                db,
                pair_id=pair_id,
                page=1,
                page_size=max_rows,
                include_evidence=True,
                sort_by="size_desc",
                export_all=True,
                export_limit=max_rows,
                tipo_zombie=tipos_filtrados,
                min_size_gb=min_size_gb,
            ),
            timeout=timeout_sec,
        )
    except asyncio.TimeoutError:
        _raise_verify_timeout(pair_id, timeout_sec)

    if format == "json":
        content = json.dumps(payload.model_dump(mode="json"), ensure_ascii=False, indent=2)
        return StreamingResponse(
            iter([content]),
            media_type="application/json; charset=utf-8",
            headers={
                "Content-Disposition": (
                    f'attachment; filename="datastore_deleted_files_{pair_id.strip()}.json"'
                ),
            },
        )

    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "pair_id",
            "datastore",
            "path",
            "tamanho_gb",
            "tipo_zombie",
            "vcenter_host",
            "datacenter",
            "last_seen_job_id",
        ],
        lineterminator="\r\n",
    )
    writer.writeheader()
    for row in payload.deleted_vmdks:
        writer.writerow(
            {
                "pair_id": payload.pair_id,
                "datastore": payload.datastore,
                "path": row.path,
                "tamanho_gb": f"{float(row.tamanho_gb):.3f}",
                "tipo_zombie": row.tipo_zombie,
                "vcenter_host": row.vcenter_host or "",
                "datacenter": row.datacenter or "",
                "last_seen_job_id": row.last_seen_job_id or "",
            }
        )

    return StreamingResponse(
        iter([buf.getvalue()]),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": (
                f'attachment; filename="datastore_deleted_files_{pair_id.strip()}.csv"'
            ),
        },
    )


@router.get(
    "/post-exclusion-file-verification/{pair_id}/export",
    summary="Alias: exportar evidencias de exclusao por arquivo (CSV/JSON) via pair_id",
)
async def export_post_exclusion_file_verification(
    pair_id: str,
    format: Annotated[str, Query(description="Formato de exportacao: csv | json")] = "csv",
    max_rows: Annotated[
        int,
        Query(
            ge=1,
            le=200000,
            description="Limite maximo de evidencias exportadas para evitar payload excessivo.",
        ),
    ] = 50000,
    tipo_zombie: Annotated[
        list[str] | None,
        Query(description="Filtro opcional por tipo_zombie (parametro repetivel)."),
    ] = None,
    min_size_gb: Annotated[
        float | None,
        Query(ge=0.0, description="Filtro opcional por tamanho minimo (GB)."),
    ] = None,
    timeout_sec: Annotated[
        int,
        Query(ge=1, le=120, description="Timeout defensivo da consulta (segundos)."),
    ] = settings.datastore_reports_verify_timeout_sec,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> StreamingResponse:
    return await export_deleted_files_by_pair(
        pair_id=pair_id,
        format=format,
        max_rows=max_rows,
        tipo_zombie=tipo_zombie,
        min_size_gb=min_size_gb,
        timeout_sec=timeout_sec,
        db=db,
        _=_,
    )
