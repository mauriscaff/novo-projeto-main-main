"""
Motor de execução de varreduras zombie.

Este módulo é o único ponto de verdade para a lógica de execução de jobs.
Importado tanto pelo endpoint POST /scan/start (routes/scanner.py) quanto
pelo APScheduler (core/scheduler.py), evitando duplicação de código.

Funções públicas:
  resolve_vcenter(db, ref)         → VCenter | None
  run_zombie_scan(job_id, ...)     → None  (coroutine)
"""

from __future__ import annotations

import asyncio
import logging
import threading
from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.scanner.zombie_detector import DatastoreScanMetric, scan_datacenter
from app.core.vcenter.client import list_datacenters_async
from app.core.vcenter.connection import VCenterNotRegisteredError, vcenter_pool
from app.core.vcenter.connection_manager import connection_manager
from app.models.base import AsyncSessionLocal
from app.models.vcenter import VCenter
from app.models.vmdk_whitelist import VmdkWhitelist
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

# ─────────────────────────────────────────────────────────────────────────────
# Progresso em tempo real (in-memory, thread-safe)
# ─────────────────────────────────────────────────────────────────────────────

_progress_lock = threading.Lock()

# job_id → { "steps": [...], "current": str, "ds_index": int, "ds_total": int,
#            "ds_current": str, "ds_status": str, "started_at": str }
_scan_progress: dict[str, dict] = {}

_LEVEL_ICON = {
    "info":    "ℹ️",
    "success": "✅",
    "warning": "⚠️",
    "error":   "❌",
}


def get_scan_progress(job_id: str) -> dict | None:
    """Retorna o snapshot de progresso atual para o job (thread-safe)."""
    with _progress_lock:
        p = _scan_progress.get(job_id)
        if p is None:
            return None
        return {
            "current": p.get("current", ""),
            "ds_index": p.get("ds_index", 0),
            "ds_total": p.get("ds_total", 0),
            "ds_current": p.get("ds_current", ""),
            "ds_status": p.get("ds_status", ""),
            "steps": list(p.get("steps", [])),
        }


def _init_progress(job_id: str, vc_name: str, dc_name: str) -> None:
    with _progress_lock:
        _scan_progress[job_id] = {
            "current": f"Iniciando varredura em {vc_name} / {dc_name}…",
            "ds_index": 0,
            "ds_total": 0,
            "ds_current": "",
            "ds_status": "",
            "steps": [],
        }


def _make_progress_callback(job_id: str, vc_name: str, dc_name: str):
    """Cria o callback de progresso para um par (vCenter, Datacenter)."""
    prefix = f"[{vc_name}] [{dc_name}]"

    def callback(level: str, msg: str, extra: dict) -> None:
        ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
        icon = _LEVEL_ICON.get(level, "•")
        step = {"ts": ts, "level": level, "msg": f"{prefix} {msg}"}

        with _progress_lock:
            p = _scan_progress.setdefault(job_id, {
                "current": "", "ds_index": 0, "ds_total": 0,
                "ds_current": "", "ds_status": "", "steps": [],
            })
            # Atualiza campos de progresso
            p["current"] = f"{icon} {msg}"
            if "ds_name" in extra:
                p["ds_current"] = extra["ds_name"]
                p["ds_status"] = extra.get("ds_status", "")
            if "ds_index" in extra:
                p["ds_index"] = extra["ds_index"]
            if "ds_total" in extra:
                p["ds_total"] = extra["ds_total"]

            # Adiciona ao log de passos (máx. 200)
            p["steps"].append(step)
            if len(p["steps"]) > 200:
                p["steps"] = p["steps"][-200:]

    return callback


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


async def resolve_vcenter(db: AsyncSession, ref: int | str) -> VCenter | None:
    """Resolve vCenter por ID (int) ou nome (str). Retorna None se não encontrado."""
    if isinstance(ref, int):
        result = await db.execute(select(VCenter).where(VCenter.id == ref))
    else:
        try:
            result = await db.execute(
                select(VCenter).where(VCenter.id == int(ref))
            )
        except (ValueError, TypeError):
            result = await db.execute(
                select(VCenter).where(VCenter.name == ref)
            )
    return result.scalar_one_or_none()


# ─────────────────────────────────────────────────────────────────────────────
# Runner principal
# ─────────────────────────────────────────────────────────────────────────────


async def run_zombie_scan(
    job_id: str,
    vcenter_refs: list[int | str],
    requested_datacenters: list[str] | None,
) -> None:
    """
    Executa a varredura zombie para múltiplos vCenters/Datacenters e persiste
    os resultados no banco SQLite.

    Fluxo:
      1. Marca ZombieScanJob como 'running'
      2. Para cada vCenter: resolve SI e lista Datacenters (se não informados)
      3. Executa scan_datacenter() em paralelo com semáforo (SCAN_MAX_WORKERS)
      4. Persiste cada lote de ZombieVmdkRecord assim que o DC conclui
      5. Atualiza o job com status final, totalizadores e erros parciais
    """

    # ── Marca job como running ────────────────────────────────────────────────
    async with AsyncSessionLocal() as db:
        job = await db.get(ZombieScanJob, job_id)
        if not job:
            logger.error("[job:%s] Job não encontrado no banco. Abortando.", job_id)
            return
        job.status = "running"
        job.started_at = datetime.now(timezone.utc)
        await db.commit()

    errors: list[str] = []
    # Elementos: (vc_id, vc_name, vc_host, dc_name)
    scan_pairs: list[tuple[int, str, str, str]] = []

    # ── Resolve vCenters → pares (vCenter × Datacenter) ──────────────────────
    async with AsyncSessionLocal() as db:
        for ref in vcenter_refs:
            vc = await resolve_vcenter(db, ref)
            if not vc:
                errors.append(f"vCenter '{ref}': não encontrado no banco.")
                continue
            if not vc.is_active:
                errors.append(f"vCenter '{vc.name}': inativo.")
                continue

            # Garante registro no pool (pode ter sido cadastrado após o startup)
            try:
                connection_manager.register(vc)
            except Exception as exc:
                errors.append(
                    f"vCenter '{vc.name}': falha ao registrar no pool — {exc}"
                )
                continue

            try:
                si = vcenter_pool.get_service_instance(vc.id)
            except Exception as exc:
                errors.append(f"vCenter '{vc.name}': falha na conexão — {exc}")
                continue

            if requested_datacenters:
                dc_names = requested_datacenters
            else:
                try:
                    dc_names = await list_datacenters_async(si)
                    if not dc_names:
                        errors.append(
                            f"vCenter '{vc.name}': nenhum Datacenter encontrado."
                        )
                        continue
                except Exception as exc:
                    errors.append(
                        f"vCenter '{vc.name}': falha ao listar Datacenters — {exc}"
                    )
                    continue

            for dc_name in dc_names:
                scan_pairs.append((vc.id, vc.name, vc.host, dc_name))

    if not scan_pairs:
        async with AsyncSessionLocal() as db:
            job = await db.get(ZombieScanJob, job_id)
            job.status = "failed"
            job.finished_at = datetime.now(timezone.utc)
            job.error_messages = errors or ["Nenhum par vCenter/Datacenter válido."]
            await db.commit()
        logger.error("[job:%s] Falhou — nenhum par válido. Erros: %s", job_id, errors)
        return

    logger.info(
        "[job:%s] Iniciando %d varredura(s): %s",
        job_id,
        len(scan_pairs),
        [(name, dc) for _, name, _, dc in scan_pairs],
    )

    # ── Carrega whitelist (caminhos excluídos de varreduras futuras) ──────────
    whitelist_paths: frozenset[str] = frozenset()
    try:
        async with AsyncSessionLocal() as db:
            wl_q = await db.execute(select(VmdkWhitelist.path))
            whitelist_paths = frozenset(wl_q.scalars())
        if whitelist_paths:
            logger.info(
                "[job:%s] %d caminho(s) na whitelist — serão ignorados.",
                job_id, len(whitelist_paths),
            )
    except Exception as exc:
        logger.warning("[job:%s] Não foi possível carregar whitelist: %s", job_id, exc)

    # ── Executa varreduras em paralelo com semáforo ───────────────────────────
    sem = asyncio.Semaphore(settings.scan_max_workers)

    def _metric_to_dict(m: DatastoreScanMetric) -> dict:
        """Serializa DatastoreScanMetric para JSON (job.datastore_metrics)."""
        return {
            "datastore_name": m.datastore_name,
            "scan_start_time": m.scan_start_time.isoformat(),
            "scan_duration_seconds": m.scan_duration_seconds,
            "files_found": m.files_found,
            "zombies_found": m.zombies_found,
        }

    async def _scan_one(
        vc_id: int, vc_name: str, vc_host: str, dc_name: str
    ) -> tuple[int, str | None, list[DatastoreScanMetric]]:
        async with sem:
            logger.info("[job:%s] Varrendo '%s' / '%s'…", job_id, vc_name, dc_name)
            _init_progress(job_id, vc_name, dc_name)
            cb = _make_progress_callback(job_id, vc_name, dc_name)

            try:
                si = vcenter_pool.get_service_instance(vc_id)
            except VCenterNotRegisteredError:
                return 0, f"{vc_name}/{dc_name}: vCenter não registrado no pool.", []

            try:
                results, metrics = await scan_datacenter(
                    si,
                    dc_name,
                    orphan_days=settings.orphan_days,
                    stale_snapshot_days=settings.stale_snapshot_days,
                    min_file_size_mb=settings.min_file_size_mb,
                    progress_callback=cb,
                )
            except Exception as exc:
                exc_str = str(exc)
                # Sessão expirada → força reconexão e retenta uma vez
                if "NotAuthenticated" in exc_str or "not authenticated" in exc_str.lower():
                    logger.warning(
                        "[job:%s] Sessão expirada em '%s'/'%s' — reconectando…",
                        job_id, vc_name, dc_name,
                    )
                    try:
                        si = vcenter_pool.get_service_instance(vc_id)
                        results, metrics = await scan_datacenter(
                            si,
                            dc_name,
                            orphan_days=settings.orphan_days,
                            stale_snapshot_days=settings.stale_snapshot_days,
                            min_file_size_mb=settings.min_file_size_mb,
                            progress_callback=cb,
                        )
                    except Exception as exc2:
                        msg = f"{vc_name}/{dc_name}: reconexão falhou — {exc2}"
                        logger.error("[job:%s] %s", job_id, msg)
                        return 0, msg, []
                else:
                    msg = f"{vc_name}/{dc_name}: {exc}"
                    logger.error("[job:%s] Falha na varredura — %s", job_id, msg)
                    return 0, msg, []

            if not results:
                logger.info(
                    "[job:%s] '%s'/'%s': nenhum zombie encontrado.",
                    job_id, vc_name, dc_name,
                )
                return 0, None, metrics

            # Filtra VMDKs que estão na whitelist
            if whitelist_paths:
                original_count = len(results)
                results = [r for r in results if r.path not in whitelist_paths]
                skipped = original_count - len(results)
                if skipped:
                    logger.info(
                        "[job:%s] '%s'/'%s': %d VMDK(s) ignorados por whitelist.",
                        job_id, vc_name, dc_name, skipped,
                    )
                if not results:
                    return 0, None, metrics

            async with AsyncSessionLocal() as db:
                for r in results:
                    db.add(
                        ZombieVmdkRecord(
                            job_id=job_id,
                            path=r.path,
                            datastore=r.datastore,
                            folder=r.folder,
                            datastore_type=r.datastore_type,
                            tamanho_gb=r.tamanho_gb,
                            ultima_modificacao=r.ultima_modificacao,
                            tipo_zombie=r.tipo_zombie.value,
                            vcenter_host=r.vcenter_host,
                            vcenter_name=vc_name,
                            datacenter=r.datacenter,
                            detection_rules=r.detection_rules,
                            likely_causes=r.likely_causes,
                            false_positive_reason=r.false_positive_reason,
                            confidence_score=r.confidence_score,
                            vcenter_deeplink_ui=getattr(r, "vcenter_deeplink_ui", "") or "",
                            vcenter_deeplink_folder=getattr(r, "vcenter_deeplink_folder", "") or "",
                            vcenter_deeplink_folder_dir=getattr(r, "vcenter_deeplink_folder_dir", "") or "",
                            datacenter_path=getattr(r, "datacenter_path", "") or "",
                            datastore_name=getattr(r, "datastore_name", "") or "",
                            vmdk_folder=getattr(r, "vmdk_folder", "") or "",
                            vmdk_filename=getattr(r, "vmdk_filename", "") or "",
                        )
                    )
                await db.commit()

            logger.info(
                "[job:%s] '%s'/'%s': %d VMDKs persistidos.",
                job_id, vc_name, dc_name, len(results),
            )
            return len(results), None, metrics

    max_duration = getattr(settings, "scan_job_max_duration_sec", 14400)

    async def _gather_and_finish() -> None:
        task_results = await asyncio.gather(
            *(_scan_one(*pair) for pair in scan_pairs),
            return_exceptions=True,
        )
        total_found = 0
        all_metrics: list[DatastoreScanMetric] = []
        for tr in task_results:
            if isinstance(tr, Exception):
                errors.append(str(tr))
            else:
                count, err, metrics = tr
                total_found += count
                if err:
                    errors.append(err)
                all_metrics.extend(metrics)

        async with AsyncSessionLocal() as db:
            job = await db.get(ZombieScanJob, job_id)
            size_q = await db.execute(
                select(func.coalesce(func.sum(ZombieVmdkRecord.tamanho_gb), 0.0)).where(
                    ZombieVmdkRecord.job_id == job_id
                )
            )
            total_size_gb = float(size_q.scalar_one())
            job.status = "failed" if (errors and total_found == 0) else "completed"
            job.finished_at = datetime.now(timezone.utc)
            job.total_vmdks = total_found
            job.total_size_gb = total_size_gb
            job.datastore_metrics = [_metric_to_dict(m) for m in all_metrics]
            if errors:
                job.error_messages = errors
            await db.commit()

        logger.info(
            "[job:%s] Finalizado — status=%s, vmdks=%d, size=%.2f GB, erros=%d",
            job_id, job.status, total_found, total_size_gb, len(errors),
        )
        if total_found > 0:
            try:
                from app.core.webhook_dispatcher import dispatch_scan_complete
                await dispatch_scan_complete(job_id)
            except Exception as exc:
                logger.error("[job:%s] Falha ao disparar webhooks: %s", job_id, exc)

    try:
        await asyncio.wait_for(_gather_and_finish(), timeout=max_duration)
    except asyncio.TimeoutError:
        logger.error(
            "[job:%s] Timeout global (%ds) excedido — marcando job como failed.",
            job_id, max_duration,
        )
        async with AsyncSessionLocal() as db:
            job = await db.get(ZombieScanJob, job_id)
            job.status = "failed"
            job.finished_at = datetime.now(timezone.utc)
            job.error_messages = (job.error_messages or []) + [
                f"Varredura excedeu o tempo máximo de {max_duration}s e foi interrompida."
            ]
            await db.commit()
