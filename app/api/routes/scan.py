"""
Endpoints para disparar e consultar varreduras de VMDKs.
"""

import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.scanner.vmdk_scanner import scan_vmdks_async
from app.core.vcenter.connection_manager import connection_manager
from app.dependencies import get_current_user, get_db
from app.models.scan_result import ScanJob, ScanStatus, VMDKResult, VMDKStatus
from app.models.vcenter import VCenter
from app.schemas.scan import ScanJobResponse, ScanSummary, VMDKResultResponse

router = APIRouter()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_vcenter_active(vcenter_id: int, db: AsyncSession) -> VCenter:
    result = await db.execute(
        select(VCenter).where(VCenter.id == vcenter_id, VCenter.is_active.is_(True))
    )
    vc = result.scalar_one_or_none()
    if not vc:
        raise HTTPException(
            status_code=404, detail="vCenter não encontrado ou inativo."
        )
    return vc


async def _run_scan_background(job_id: int, vcenter_id: int) -> None:
    """Executado em background: realiza a varredura e persiste os resultados."""
    from app.models.base import AsyncSessionLocal

    async with AsyncSessionLocal() as db:
        # Marca job como running
        result = await db.execute(select(ScanJob).where(ScanJob.id == job_id))
        job = result.scalar_one()
        job.status = ScanStatus.running
        job.started_at = datetime.now(timezone.utc)
        await db.commit()

        try:
            vc_result = await db.execute(
                select(VCenter).where(VCenter.id == vcenter_id)
            )
            vc = vc_result.scalar_one()
            client = connection_manager.get_client(vc)

            vmdk_list = await scan_vmdks_async(client)

            # Persiste os resultados em lote
            for info in vmdk_list:
                db.add(
                    VMDKResult(
                        scan_job_id=job_id,
                        vcenter_id=vcenter_id,
                        datastore_name=info.datastore_name,
                        datastore_url=info.datastore_url,
                        vmdk_path=info.vmdk_path,
                        size_gb=info.size_gb,
                        status=VMDKStatus(info.status),
                        vm_name=info.vm_name,
                        vm_moref=info.vm_moref,
                        last_modified=info.last_modified,
                        days_since_modified=info.days_since_modified,
                    )
                )

            job.status = ScanStatus.completed
            job.finished_at = datetime.now(timezone.utc)
            logger.info("Varredura job_id=%d concluída. %d VMDKs.", job_id, len(vmdk_list))

        except Exception as exc:
            logger.exception("Falha na varredura job_id=%d: %s", job_id, exc)
            job.status = ScanStatus.failed
            job.error_message = str(exc)
            job.finished_at = datetime.now(timezone.utc)

        await db.commit()


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@router.post(
    "/",
    response_model=ScanJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Iniciar varredura",
    description=(
        "Dispara uma varredura assíncrona em background para o vCenter informado. "
        "Use GET /scans/{id} para acompanhar o status."
    ),
)
async def start_scan(
    vcenter_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanJobResponse:
    vc = await _get_vcenter_active(vcenter_id, db)

    job = ScanJob(vcenter_id=vc.id, status=ScanStatus.pending)
    db.add(job)
    await db.flush()
    await db.refresh(job)

    background_tasks.add_task(_run_scan_background, job.id, vc.id)
    return ScanJobResponse.model_validate(job)


@router.get(
    "/",
    response_model=list[ScanJobResponse],
    summary="Listar jobs de varredura",
)
async def list_scan_jobs(
    vcenter_id: int | None = Query(default=None, description="Filtrar por vCenter"),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[ScanJobResponse]:
    stmt = select(ScanJob).order_by(ScanJob.id.desc())
    if vcenter_id:
        stmt = stmt.where(ScanJob.vcenter_id == vcenter_id)
    result = await db.execute(stmt)
    return [ScanJobResponse.model_validate(j) for j in result.scalars()]


@router.get(
    "/{job_id}",
    response_model=ScanJobResponse,
    summary="Status do job",
)
async def get_scan_job(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanJobResponse:
    result = await db.execute(select(ScanJob).where(ScanJob.id == job_id))
    job = result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")
    return ScanJobResponse.model_validate(job)


@router.get(
    "/{job_id}/results",
    response_model=list[VMDKResultResponse],
    summary="Resultados do job",
    description="Lista todos os VMDKs encontrados no job. Use `status` para filtrar.",
)
async def get_scan_results(
    job_id: int,
    vmdk_status: VMDKStatus | None = Query(
        default=None,
        alias="status",
        description="Filtrar por status: attached | orphaned | zombie",
    ),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[VMDKResultResponse]:
    stmt = select(VMDKResult).where(VMDKResult.scan_job_id == job_id)
    if vmdk_status:
        stmt = stmt.where(VMDKResult.status == vmdk_status)
    result = await db.execute(stmt.order_by(VMDKResult.id))
    return [VMDKResultResponse.model_validate(r) for r in result.scalars()]


@router.get(
    "/{job_id}/summary",
    response_model=ScanSummary,
    summary="Resumo do job",
    description="Retorna contagens e totais por categoria de VMDK.",
)
async def get_scan_summary(
    job_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScanSummary:
    job_result = await db.execute(select(ScanJob).where(ScanJob.id == job_id))
    job = job_result.scalar_one_or_none()
    if not job:
        raise HTTPException(status_code=404, detail="Job não encontrado.")

    async def _count(s: VMDKStatus) -> int:
        r = await db.execute(
            select(func.count()).where(
                VMDKResult.scan_job_id == job_id,
                VMDKResult.status == s,
            )
        )
        return r.scalar_one()

    async def _orphaned_size() -> float:
        r = await db.execute(
            select(func.coalesce(func.sum(VMDKResult.size_gb), 0.0)).where(
                VMDKResult.scan_job_id == job_id,
                VMDKResult.status.in_([VMDKStatus.orphaned, VMDKStatus.zombie]),
            )
        )
        return float(r.scalar_one())

    attached, orphaned, zombie, total_orphaned_size_gb = await asyncio.gather(
        _count(VMDKStatus.attached),
        _count(VMDKStatus.orphaned),
        _count(VMDKStatus.zombie),
        _orphaned_size(),
    )

    return ScanSummary(
        scan_job_id=job_id,
        vcenter_id=job.vcenter_id,
        total_vmdks=attached + orphaned + zombie,
        attached=attached,
        orphaned=orphaned,
        zombie=zombie,
        total_orphaned_size_gb=total_orphaned_size_gb,
    )
