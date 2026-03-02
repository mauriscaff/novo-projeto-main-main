"""
Endpoints de CRUD para agendamentos de varredura zombie.

Prefixo registrado em main.py: /api/v1/schedules

  POST   /              Cria agendamento com expressão cron + lista de vCenters
  GET    /              Lista agendamentos ativos (inclui next_run_at do APScheduler)
  GET    /{id}          Detalha um agendamento específico
  PATCH  /{id}          Atualiza campos do agendamento (re-registra no APScheduler)
  DELETE /{id}          Remove agendamento do banco e do APScheduler
  POST   /{id}/run      Dispara execução imediata (fora do horário agendado)
"""

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.scheduler import (
    get_next_run_time,
    register_job,
    unregister_job,
)
from app.core.scanner.scan_runner import run_zombie_scan
from app.dependencies import get_current_user, get_db
from app.models.base import AsyncSessionLocal
from app.models.schedule import ScanSchedule
from app.models.zombie_scan import ZombieScanJob
from app.schemas.schedule import ScheduleCreate, ScheduleResponse, ScheduleUpdate

router = APIRouter()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _to_response(s: ScanSchedule) -> ScheduleResponse:
    """Converte ORM → schema, enriquecendo com next_run_at do APScheduler."""
    return ScheduleResponse(
        id=s.id,
        name=s.name,
        cron_expression=s.cron_expression,
        vcenter_ids=s.vcenter_ids or [],
        datacenters=s.datacenters,
        is_active=s.is_active,
        description=s.description,
        last_run_at=s.last_run_at,
        last_job_id=s.last_job_id,
        next_run_at=get_next_run_time(s.id) if s.is_active else None,
        run_count=s.run_count or 0,
        created_at=s.created_at,
        updated_at=s.updated_at,
    )


async def _get_schedule_or_404(schedule_id: int, db: AsyncSession) -> ScanSchedule:
    s = await db.get(ScanSchedule, schedule_id)
    if not s:
        raise HTTPException(status_code=404, detail="Agendamento não encontrado.")
    return s


# ─────────────────────────────────────────────────────────────────────────────
# POST /
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/",
    response_model=ScheduleResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Criar agendamento",
    description="""
Cria um novo agendamento de varredura zombie.

**Expressão cron** — 5 campos (UTC):
```
┌───────── minuto   (0-59)
│ ┌─────── hora     (0-23)
│ │ ┌───── dia      (1-31)
│ │ │ ┌─── mês      (1-12)
│ │ │ │ ┌─ dia_sem  (0-6, 0=Dom)
│ │ │ │ │
0 2 * * *   → todo dia às 2h UTC
0 */4 * * * → a cada 4 horas
0 8 * * 1   → toda segunda-feira às 8h UTC
```

O agendamento é registrado no APScheduler imediatamente após a criação.
    """,
)
async def create_schedule(
    body: ScheduleCreate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScheduleResponse:
    # Unicidade de nome
    existing = await db.execute(
        select(ScanSchedule).where(ScanSchedule.name == body.name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"Já existe um agendamento com o nome '{body.name}'.",
        )

    schedule = ScanSchedule(
        name=body.name,
        cron_expression=body.cron_expression,
        vcenter_ids=list(body.vcenter_ids),
        datacenters=body.datacenters,
        description=body.description,
    )
    db.add(schedule)
    await db.flush()
    await db.refresh(schedule)

    # Registra no APScheduler — a validação da cron já ocorreu no schema
    registered = register_job(schedule)
    if not registered:
        raise HTTPException(
            status_code=422,
            detail=f"Falha ao registrar o cron '{body.cron_expression}' no scheduler.",
        )

    logger.info(
        "Agendamento '%s' (id=%d) criado. Próxima execução: %s",
        schedule.name,
        schedule.id,
        get_next_run_time(schedule.id),
    )
    return _to_response(schedule)


# ─────────────────────────────────────────────────────────────────────────────
# GET /
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/",
    response_model=list[ScheduleResponse],
    summary="Listar agendamentos",
    description=(
        "Retorna todos os agendamentos ativos. "
        "Use `?include_inactive=true` para incluir os pausados."
    ),
)
async def list_schedules(
    include_inactive: bool = Query(
        default=False,
        description="Incluir agendamentos inativos/pausados na listagem.",
    ),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[ScheduleResponse]:
    stmt = select(ScanSchedule).order_by(ScanSchedule.id)
    if not include_inactive:
        stmt = stmt.where(ScanSchedule.is_active.is_(True))
    result = await db.execute(stmt)
    return [_to_response(s) for s in result.scalars()]


# ─────────────────────────────────────────────────────────────────────────────
# GET /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/{schedule_id}",
    response_model=ScheduleResponse,
    summary="Detalhar agendamento",
)
async def get_schedule(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScheduleResponse:
    s = await _get_schedule_or_404(schedule_id, db)
    return _to_response(s)


# ─────────────────────────────────────────────────────────────────────────────
# PATCH /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.patch(
    "/{schedule_id}",
    response_model=ScheduleResponse,
    summary="Atualizar agendamento",
    description=(
        "Atualiza campos do agendamento. Se `cron_expression` ou `vcenter_ids` "
        "forem alterados, o job é re-registrado no APScheduler. "
        "Definir `is_active=false` pausa o agendamento (remove do APScheduler "
        "sem deletar do banco)."
    ),
)
async def update_schedule(
    schedule_id: int,
    body: ScheduleUpdate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ScheduleResponse:
    s = await _get_schedule_or_404(schedule_id, db)

    updated_fields = body.model_dump(exclude_none=True)
    needs_reregister = any(
        k in updated_fields for k in ("cron_expression", "vcenter_ids", "is_active")
    )

    for field, value in updated_fields.items():
        if field == "vcenter_ids":
            value = list(value)
        setattr(s, field, value)

    await db.flush()
    await db.refresh(s)

    if needs_reregister:
        if s.is_active:
            register_job(s)
        else:
            unregister_job(s.id)
            logger.info(
                "Agendamento '%s' (id=%d) pausado.", s.name, s.id
            )

    return _to_response(s)


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.delete(
    "/{schedule_id}",
    status_code=status.HTTP_200_OK,
    summary="Remover agendamento",
    description=(
        "Remove o agendamento do banco e do APScheduler. "
        "Os jobs de varredura já executados (ZombieScanJob) **não** são removidos — "
        "o histórico de resultados é preservado."
    ),
)
async def delete_schedule(
    schedule_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> None:
    s = await _get_schedule_or_404(schedule_id, db)

    # Remove do APScheduler antes de apagar do banco
    unregister_job(schedule_id)

    await db.delete(s)
    logger.info("Agendamento '%s' (id=%d) removido.", s.name, schedule_id)


# ─────────────────────────────────────────────────────────────────────────────
# POST /{id}/run  — execução imediata fora do horário agendado
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/{schedule_id}/run",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Disparar execução imediata",
    description=(
        "Executa o agendamento imediatamente, independentemente do horário cron. "
        "Gera um novo `job_id` e retorna 202. "
        "Acompanhe o resultado via `GET /api/v1/scan/jobs/{job_id}`."
    ),
)
async def run_schedule_now(
    schedule_id: int,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> dict:
    s = await _get_schedule_or_404(schedule_id, db)
    if not s.is_active:
        raise HTTPException(
            status_code=409,
            detail="Agendamento inativo. Ative-o antes de disparar manualmente.",
        )

    job_id = str(uuid.uuid4())

    db.add(
        ZombieScanJob(
            job_id=job_id,
            vcenter_ids=s.vcenter_ids,
            datacenters=s.datacenters,
            status="pending",
        )
    )
    s.last_run_at = datetime.now(timezone.utc)
    s.last_job_id = job_id
    s.run_count = (s.run_count or 0) + 1
    await db.flush()

    background_tasks.add_task(run_zombie_scan, job_id, s.vcenter_ids, s.datacenters)

    logger.info(
        "Agendamento '%s' (id=%d) disparado manualmente → job_id=%s",
        s.name, schedule_id, job_id,
    )
    return {
        "job_id": job_id,
        "schedule_id": schedule_id,
        "status": "pending",
        "message": f"Varredura iniciada. Acompanhe via GET /api/v1/scan/jobs/{job_id}",
    }
