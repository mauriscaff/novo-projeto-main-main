"""
Módulo central do APScheduler para execução automática de varreduras zombie.

Arquitetura
───────────
  scheduler          AsyncIOScheduler singleton — compartilhado por toda a app
  start()            Inicia o scheduler e recarrega jobs do banco (lifespan startup)
  stop()             Para o scheduler graciosamente (lifespan shutdown)
  register_job()     Adiciona/substitui um job no APScheduler para um ScanSchedule
  unregister_job()   Remove o job correspondente a um schedule
  get_next_run_time()Retorna a próxima data de execução calculada pelo APScheduler
  reload_from_db()   Carrega todos os schedules ativos do SQLite e registra no scheduler

Ciclo de vida de um schedule
─────────────────────────────
  1. POST /api/v1/schedules → ScanSchedule criado no banco → register_job() chamado
  2. APScheduler dispara _execute_schedule(schedule_id) no horário configurado
  3. _execute_schedule cria ZombieScanJob + chama run_zombie_scan()
  4. DELETE /api/v1/schedules/{id} → unregister_job() + delete do banco

Configurações padrão dos jobs
───────────────────────────────
  coalesce=True        Múltiplos misfires são fundidos em um único disparo
  max_instances=1      Nunca executa o mesmo schedule duas vezes em paralelo
  misfire_grace_time   3600s: se o servidor ficou offline até 1h, ainda dispara
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

if TYPE_CHECKING:
    from app.models.schedule import ScanSchedule

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Singleton global
# ─────────────────────────────────────────────────────────────────────────────

scheduler = AsyncIOScheduler(
    timezone="UTC",
    job_defaults={
        "coalesce": True,
        "max_instances": 1,
        "misfire_grace_time": 3600,
    },
)


# ─────────────────────────────────────────────────────────────────────────────
# Ciclo de vida
# ─────────────────────────────────────────────────────────────────────────────


async def start() -> None:
    """
    Inicia o AsyncIOScheduler e recarrega todos os agendamentos ativos do banco.
    Deve ser chamado no lifespan startup do FastAPI, após init_db().
    """
    scheduler.start()
    logger.info("APScheduler (AsyncIOScheduler) iniciado.")
    await reload_from_db()


def stop() -> None:
    """Para o scheduler sem aguardar a conclusão de jobs em andamento."""
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("APScheduler encerrado.")


# ─────────────────────────────────────────────────────────────────────────────
# Registro e remoção de jobs
# ─────────────────────────────────────────────────────────────────────────────


def register_job(schedule: ScanSchedule) -> bool:
    """
    Registra (ou substitui) um APScheduler job para o ScanSchedule informado.

    Comportamento upsert: se já existir um job com o mesmo ID, ele é removido
    e recriado com os novos parâmetros (útil após atualizar cron ou vCenters).

    Retorna True se o job foi registrado com sucesso, False em caso de erro.
    """
    aps_id = _aps_job_id(schedule.id)

    try:
        trigger = CronTrigger.from_crontab(schedule.cron_expression, timezone="UTC")
    except Exception as exc:
        logger.error(
            "Cron expression inválida para schedule id=%d ('%s'): %s",
            schedule.id,
            schedule.cron_expression,
            exc,
        )
        return False

    # Upsert: remove job anterior se existir
    if scheduler.get_job(aps_id):
        scheduler.remove_job(aps_id)

    scheduler.add_job(
        _execute_schedule,
        trigger=trigger,
        id=aps_id,
        name=schedule.name,
        args=[schedule.id],
    )

    job = scheduler.get_job(aps_id)
    next_fire = job.next_run_time if job else None
    logger.info(
        "Schedule '%s' (id=%d, cron='%s') registrado. Próxima execução: %s",
        schedule.name,
        schedule.id,
        schedule.cron_expression,
        next_fire,
    )
    return True


def unregister_job(schedule_id: int) -> None:
    """Remove o APScheduler job correspondente ao schedule_id."""
    aps_id = _aps_job_id(schedule_id)
    if scheduler.get_job(aps_id):
        scheduler.remove_job(aps_id)
        logger.info("Job do schedule id=%d removido do APScheduler.", schedule_id)


def get_next_run_time(schedule_id: int) -> datetime | None:
    """
    Retorna a próxima data de execução calculada pelo APScheduler.
    Retorna None se o schedule não estiver registrado (pausado ou inexistente).
    """
    job = scheduler.get_job(_aps_job_id(schedule_id))
    return job.next_run_time if job else None


# ─────────────────────────────────────────────────────────────────────────────
# Recarga a partir do banco
# ─────────────────────────────────────────────────────────────────────────────


async def reload_from_db() -> None:
    """
    Carrega todos os ScanSchedules ativos do banco SQLite e os registra no
    APScheduler. Chamado automaticamente em `start()` durante o startup.

    Jobs de schedules inativos ou com cron inválido são ignorados com log de aviso.
    """
    from sqlalchemy import select

    from app.models.base import AsyncSessionLocal
    from app.models.schedule import ScanSchedule

    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(ScanSchedule).where(ScanSchedule.is_active.is_(True))
        )
        schedules = result.scalars().all()

    loaded = sum(1 for s in schedules if register_job(s))
    logger.info(
        "%d/%d agendamento(s) ativo(s) carregado(s) do banco.",
        loaded,
        len(schedules),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Execução interna — chamada pelo APScheduler no horário configurado
# ─────────────────────────────────────────────────────────────────────────────


async def _execute_schedule(schedule_id: int) -> None:
    """
    Função assíncrona invocada pelo APScheduler quando o cron dispara.

    Passos:
      1. Carrega o ScanSchedule do banco (verifica is_active)
      2. Gera um novo job_id UUID
      3. Cria ZombieScanJob (status=pending) no banco
      4. Atualiza last_run_at, last_job_id, run_count no schedule
      5. Executa run_zombie_scan() — mesma lógica do endpoint POST /scan/start
    """
    from app.core.scanner.scan_runner import run_zombie_scan
    from app.models.base import AsyncSessionLocal
    from app.models.schedule import ScanSchedule
    from app.models.zombie_scan import ZombieScanJob

    # ── Carrega schedule e cria job ───────────────────────────────────────────
    job_id: str | None = None

    async with AsyncSessionLocal() as db:
        schedule = await db.get(ScanSchedule, schedule_id)

        if not schedule:
            logger.error(
                "Schedule id=%d não encontrado no banco. Removendo do APScheduler.",
                schedule_id,
            )
            unregister_job(schedule_id)
            return

        if not schedule.is_active:
            logger.warning(
                "Schedule '%s' (id=%d) está inativo. Disparo ignorado.",
                schedule.name,
                schedule_id,
            )
            return

        job_id = str(uuid.uuid4())
        db.add(
            ZombieScanJob(
                job_id=job_id,
                vcenter_ids=schedule.vcenter_ids,
                datacenters=schedule.datacenters,
                status="pending",
            )
        )
        schedule.last_run_at = datetime.now(timezone.utc)
        schedule.last_job_id = job_id
        schedule.run_count = (schedule.run_count or 0) + 1
        await db.commit()

    logger.info(
        "Schedule '%s' (id=%d) disparado → job_id=%s",
        schedule.name,
        schedule_id,
        job_id,
    )

    # ── Executa a varredura (mesma lógica do endpoint POST /scan/start) ───────
    await run_zombie_scan(job_id, schedule.vcenter_ids, schedule.datacenters)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _aps_job_id(schedule_id: int) -> str:
    """Gera o ID usado pelo APScheduler para o schedule informado."""
    return f"scan_schedule_{schedule_id}"
