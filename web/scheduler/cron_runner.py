"""
web/scheduler/cron_runner.py — Agendador de varreduras do ZombieHunter
=======================================================================

Responsabilidades:
  1. Executar ZombieHunter.ps1 periodicamente via APScheduler
  2. Ler os JSONs gerados em reports/data/ e importar para o SQLite
  3. Atualizar o timestamp "última varredura" no banco
  4. Disparar webhooks via HTTP POST se configurado
  5. Registrar resultados em web/logs/scheduler.log

Variáveis de ambiente (lidas via config.py / .env):
  SCAN_CRON_HOUR    hora de execução (0-23, padrão: 2)
  SCAN_CRON_MINUTE  minuto (0-59, padrão: 0)
  PS_SCRIPT_PATH    caminho do ZombieHunter.ps1 (padrão: scripts/ZombieHunter.ps1)
  REPORTS_DIR       pasta com os JSONs gerados (padrão: reports/data)

Formato JSON esperado do ZombieHunter.ps1 (um arquivo por job):
  {
    "job_id":       "uuid",
    "scan_date":    "2026-02-25T02:00:00Z",
    "vcenter":      "vcenter.empresa.com",
    "vcenter_name": "vc-prod-01",
    "datacenter":   "DC-01",
    "total_found":  10,
    "total_size_gb": 500.0,
    "results": [
      {
        "Path":            "[datastore01] folder/vm.vmdk",
        "Datastore":       "datastore01",
        "SizeGB":          100.0,
        "LastModified":    "2025-01-01T00:00:00Z",
        "ZombieType":      "ORPHANED",
        "VCenterHost":     "vcenter.empresa.com",
        "VCenterName":     "vc-prod-01",
        "Datacenter":      "DC-01",
        "DetectionRules":  ["Regra acionada 1"],
        "Folder":          "[datastore01] folder"
      }
    ]
  }
"""

from __future__ import annotations

import glob
import json
import logging
import os
import subprocess
import sys
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session, sessionmaker

# ── Raiz do projeto ──────────────────────────────────────────────────────────

_SCHEDULER_DIR  = Path(__file__).parent           # web/scheduler/
_WEB_DIR        = _SCHEDULER_DIR.parent           # web/
_PROJECT_ROOT   = _WEB_DIR.parent                 # raiz do projeto

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ── Imports do projeto ────────────────────────────────────────────────────────

from config import get_settings
from app.models.audit_log  import AuditLog
from app.models.vcenter    import VCenter
from app.models.webhook    import WebhookEndpoint
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord

settings = get_settings()

# ── Logging ───────────────────────────────────────────────────────────────────

_LOG_DIR  = _WEB_DIR / "logs"
_LOG_FILE = _LOG_DIR / "scheduler.log"
_LOG_DIR.mkdir(parents=True, exist_ok=True)

logger = logging.getLogger("zombiehunter.scheduler")
logger.setLevel(logging.INFO)

if not logger.handlers:
    _file_handler = RotatingFileHandler(
        str(_LOG_FILE),
        maxBytes=5 * 1024 * 1024,   # 5 MB por arquivo
        backupCount=5,
        encoding="utf-8",
    )
    _file_handler.setFormatter(
        logging.Formatter("%(asctime)s [%(levelname)s] %(message)s",
                          datefmt="%Y-%m-%d %H:%M:%S UTC")
    )
    logger.addHandler(_file_handler)

    # Também propaga para o logger raiz (console)
    _console = logging.StreamHandler()
    _console.setFormatter(logging.Formatter("%(asctime)s [SCHEDULER] %(levelname)s %(message)s"))
    logger.addHandler(_console)

# ── Banco de dados (sync) ─────────────────────────────────────────────────────

_SYNC_DB_URL = settings.database_url.replace("+aiosqlite", "")
_engine      = create_engine(
    _SYNC_DB_URL,
    connect_args={"check_same_thread": False},
)
_Session = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


@contextmanager
def _db():
    """Context manager para sessão sincronizada com commit/rollback automáticos."""
    session: Session = _Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ── Caminhos configuráveis ────────────────────────────────────────────────────

_PS_SCRIPT   = Path(os.environ.get("PS_SCRIPT_PATH",
                                   str(_PROJECT_ROOT / "scripts" / "ZombieHunter.ps1")))
_REPORTS_DIR = Path(os.environ.get("REPORTS_DIR",
                                   str(_PROJECT_ROOT / "reports" / "data")))
_REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# ── Scheduler ─────────────────────────────────────────────────────────────────

scheduler = BackgroundScheduler(timezone="UTC")


# ─────────────────────────────────────────────────────────────────────────────
# Job principal
# ─────────────────────────────────────────────────────────────────────────────

def run_zombie_hunter() -> None:
    """
    Job principal executado pelo APScheduler.

    Fluxo:
      1. Executa ZombieHunter.ps1 (se existir)
      2. Lê todos os JSONs pendentes em reports/data/
      3. Importa cada JSON para o banco SQLite
      4. Dispara webhooks se houver zombies
      5. Loga resultado em web/logs/scheduler.log
    """
    started_at = datetime.now(timezone.utc)
    logger.info("=" * 60)
    logger.info("Iniciando varredura agendada: %s", started_at.isoformat())

    # ── 1. Executa o script PowerShell ────────────────────────────────────────
    ps_success = False
    if _PS_SCRIPT.exists():
        ps_success = _run_powershell_script()
    else:
        logger.warning(
            "ZombieHunter.ps1 não encontrado em '%s'. "
            "Processando apenas JSONs existentes em reports/data/.",
            _PS_SCRIPT,
        )
        ps_success = True  # permite processar JSONs manuais

    if not ps_success:
        logger.error("Execução do PowerShell falhou. Abortando importação.")
        return

    # ── 2. Descobre JSONs pendentes (não importados ainda) ────────────────────
    json_files = sorted(_REPORTS_DIR.glob("*.json"))
    if not json_files:
        logger.info("Nenhum arquivo JSON encontrado em '%s'.", _REPORTS_DIR)
        _finalize(started_at, imported=0, total_zombies=0, total_gb=0.0)
        return

    logger.info("%d arquivo(s) JSON encontrado(s) para importação.", len(json_files))

    # ── 3. Importa cada JSON ──────────────────────────────────────────────────
    imported_total    = 0
    zombies_total     = 0
    gb_total          = 0.0
    last_job_id: str | None = None

    for json_path in json_files:
        try:
            count, size_gb, job_id = import_scan_results(json_path)
            imported_total += 1
            zombies_total  += count
            gb_total       += size_gb
            last_job_id     = job_id

            # Move para subpasta "imported/" após processar
            _archive_json(json_path)

        except Exception as exc:
            logger.error("Erro ao importar '%s': %s", json_path.name, exc, exc_info=True)

    logger.info(
        "Importação concluída: %d arquivo(s), %d zombie(s) detectado(s), %.2f GB.",
        imported_total, zombies_total, gb_total,
    )

    # ── 4. Dispara webhooks ────────────────────────────────────────────────────
    if zombies_total > 0 and last_job_id:
        _dispatch_webhooks_sync(last_job_id, zombies_total, gb_total)

    # ── 5. Registra resultado final ────────────────────────────────────────────
    _finalize(started_at, imported=imported_total,
              total_zombies=zombies_total, total_gb=gb_total)


# ─────────────────────────────────────────────────────────────────────────────
# Execução do PowerShell
# ─────────────────────────────────────────────────────────────────────────────

def _run_powershell_script() -> bool:
    """
    Executa ZombieHunter.ps1 com PowerShell e aguarda conclusão.
    Retorna True em caso de sucesso (exit code 0).
    """
    cmd = [
        "powershell.exe",
        "-NonInteractive",
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", str(_PS_SCRIPT),
        "-OutputDir", str(_REPORTS_DIR),
    ]

    logger.info("Executando: %s", " ".join(cmd))

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=3600,   # máximo 1 hora
            cwd=str(_PROJECT_ROOT),
            encoding="utf-8",
            errors="replace",
        )

        if result.stdout:
            for line in result.stdout.strip().splitlines():
                logger.info("[PS] %s", line)
        if result.stderr:
            for line in result.stderr.strip().splitlines():
                logger.warning("[PS STDERR] %s", line)

        if result.returncode == 0:
            logger.info("ZombieHunter.ps1 concluído com sucesso (exit 0).")
            return True
        else:
            logger.error(
                "ZombieHunter.ps1 terminou com código %d.", result.returncode
            )
            return False

    except subprocess.TimeoutExpired:
        logger.error("ZombieHunter.ps1 excedeu timeout de 3600 segundos.")
        return False
    except FileNotFoundError:
        logger.error("powershell.exe não encontrado no PATH.")
        return False
    except Exception as exc:
        logger.error("Erro inesperado ao executar PowerShell: %s", exc, exc_info=True)
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Importação de resultados JSON → SQLite
# ─────────────────────────────────────────────────────────────────────────────

def import_scan_results(json_path: Path) -> tuple[int, float, str]:
    """
    Lê um arquivo JSON gerado pelo ZombieHunter.ps1 e persiste no banco.

    Retorna: (total_vmdks_importados, total_size_gb, job_id)
    """
    logger.info("Importando '%s'…", json_path.name)

    with open(json_path, encoding="utf-8") as fh:
        data: dict[str, Any] = json.load(fh)

    # Extrai metadados do job
    job_id    = str(data.get("job_id") or uuid.uuid4())
    scan_date = _parse_dt(data.get("scan_date"))
    results   = data.get("results", [])

    total_gb = float(data.get("total_size_gb") or sum(
        float(r.get("SizeGB") or 0) for r in results
    ))

    with _db() as db:
        # Verifica se o job já foi importado (idempotência)
        existing = db.get(ZombieScanJob, job_id)
        if existing:
            logger.warning("job_id '%s' já importado. Pulando.", job_id)
            return 0, 0.0, job_id

        # Cria o registro do job
        job = ZombieScanJob(
            job_id      = job_id,
            vcenter_ids = [data.get("vcenter", "unknown")],
            datacenters = [data.get("datacenter")] if data.get("datacenter") else None,
            status      = "completed",
            started_at  = scan_date,
            finished_at = datetime.now(timezone.utc),
            total_vmdks = len(results),
            total_size_gb = total_gb,
        )
        db.add(job)

        # Insere registros individuais de VMDKs
        imported = 0
        for r in results:
            # Normaliza chaves PowerShell (PascalCase) e Python (snake_case)
            path     = str(r.get("Path") or r.get("path") or "")
            if not path:
                continue

            record = ZombieVmdkRecord(
                job_id             = job_id,
                path               = path,
                datastore          = str(r.get("Datastore")  or r.get("datastore") or ""),
                folder             = str(r.get("Folder")     or r.get("folder")    or ""),
                datastore_type     = str(r.get("DatastoreType") or r.get("datastore_type") or ""),
                tamanho_gb         = float(r.get("SizeGB")    or r.get("tamanho_gb") or 0.0),
                ultima_modificacao = _parse_dt(r.get("LastModified") or r.get("ultima_modificacao")),
                tipo_zombie        = str(r.get("ZombieType")  or r.get("tipo_zombie") or "ORPHANED").upper(),
                vcenter_host       = str(r.get("VCenterHost") or r.get("vcenter_host") or data.get("vcenter") or ""),
                vcenter_name       = str(r.get("VCenterName") or r.get("vcenter_name") or data.get("vcenter_name") or ""),
                datacenter         = str(r.get("Datacenter")  or r.get("datacenter")  or data.get("datacenter") or ""),
                detection_rules    = r.get("DetectionRules")  or r.get("detection_rules") or [],
                false_positive_reason = r.get("FalsePositiveReason") or r.get("false_positive_reason"),
            )
            db.add(record)
            imported += 1

        logger.info(
            "Job '%s': %d VMDK(s) importado(s), %.2f GB total.",
            job_id, imported, total_gb,
        )
        return imported, total_gb, job_id


def _parse_dt(value: Any) -> datetime | None:
    """Converte string ISO8601 ou None para datetime aware."""
    if not value:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError):
        return None


def _archive_json(path: Path) -> None:
    """Move o JSON processado para reports/data/imported/ (evita reimportação)."""
    dest_dir = path.parent / "imported"
    dest_dir.mkdir(exist_ok=True)
    dest = dest_dir / path.name
    # Evita colisão de nomes
    if dest.exists():
        ts   = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        dest = dest_dir / f"{path.stem}_{ts}{path.suffix}"
    path.rename(dest)
    logger.debug("Arquivo arquivado: %s → %s", path.name, dest.name)


# ─────────────────────────────────────────────────────────────────────────────
# Webhooks (síncrono — evita asyncio em contexto de thread APScheduler)
# ─────────────────────────────────────────────────────────────────────────────

def _dispatch_webhooks_sync(job_id: str, total_found: int, total_gb: float) -> None:
    """
    Envia notificações para todos os webhooks ativos cujo limiar seja satisfeito.
    Usa `requests` (sync) para evitar conflito com o event loop do Flask.
    """
    try:
        with _db() as db:
            webhooks = db.execute(
                select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
            ).scalars().all()

            # Top 10 maiores VMDKs do job
            top_rows = db.execute(
                select(ZombieVmdkRecord)
                .where(ZombieVmdkRecord.job_id == job_id)
                .order_by(ZombieVmdkRecord.tamanho_gb.desc().nulls_last())
                .limit(10)
            ).scalars().all()

        if not webhooks:
            logger.info("Nenhum webhook ativo configurado.")
            return

        base_payload = {
            "job_id":       job_id,
            "total_found":  total_found,
            "total_size_gb": round(total_gb, 3),
            "finished_at":  datetime.now(timezone.utc).isoformat(),
            "top_10_largest": [
                {
                    "path":      r.path,
                    "datastore": r.datastore,
                    "size_gb":   round(float(r.tamanho_gb or 0), 3),
                    "type":      r.tipo_zombie,
                    "vcenter":   r.vcenter_name or r.vcenter_host,
                }
                for r in top_rows
            ],
        }

        for wh in webhooks:
            if total_found < wh.min_zombies_to_fire:
                logger.debug(
                    "Webhook '%s': ignorado (total=%d < min=%d).",
                    wh.name, total_found, wh.min_zombies_to_fire,
                )
                continue
            _fire_webhook_sync(wh, base_payload)

    except Exception as exc:
        logger.error("Erro no dispatcher de webhooks: %s", exc, exc_info=True)


def _fire_webhook_sync(wh: WebhookEndpoint, payload: dict) -> None:
    """Dispara um único webhook de forma síncrona."""
    headers = {"Content-Type": "application/json"}
    if wh.secret_header and wh.secret_value:
        headers[wh.secret_header] = wh.secret_value

    # Formata payload conforme provedor
    formatted = _format_payload(wh.provider, payload)

    try:
        resp = requests.post(
            wh.url,
            json=formatted,
            headers=headers,
            timeout=15,
        )
        status_code = resp.status_code

        if resp.ok:
            logger.info("Webhook '%s' disparado → HTTP %d.", wh.name, status_code)
        else:
            logger.warning(
                "Webhook '%s' retornou HTTP %d: %s",
                wh.name, status_code, resp.text[:200],
            )
    except requests.Timeout:
        logger.error("Webhook '%s': timeout.", wh.name)
        status_code = 0
    except Exception as exc:
        logger.error("Webhook '%s': erro — %s", wh.name, exc)
        status_code = 0

    # Atualiza rastreamento no banco
    try:
        with _db() as db:
            wh_obj = db.get(WebhookEndpoint, wh.id)
            if wh_obj:
                wh_obj.last_fired_at    = datetime.now(timezone.utc)
                wh_obj.last_status_code = status_code
                wh_obj.fire_count       = (wh_obj.fire_count or 0) + 1
    except Exception as exc:
        logger.error("Falha ao atualizar rastreamento do webhook '%s': %s", wh.name, exc)


def _format_payload(provider: str, p: dict) -> dict:
    """Formata payload conforme o provedor (teams | slack | generic)."""
    if provider == "teams":
        return {
            "@type":    "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": "D44000",
            "summary": f"ZombieHunter — {p['total_found']} zombie(s) detectado(s)",
            "sections": [
                {
                    "activityTitle": "🚨 VMDKs Zombie Encontrados",
                    "facts": [
                        {"name": "Total",   "value": str(p["total_found"])},
                        {"name": "GB",      "value": str(p["total_size_gb"])},
                        {"name": "Job ID",  "value": p["job_id"]},
                    ],
                }
            ],
        }
    if provider == "slack":
        return {
            "text": f"🚨 *ZombieHunter — {p['total_found']} zombie(s)*",
            "blocks": [
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Total:*\n{p['total_found']}"},
                        {"type": "mrkdwn", "text": f"*GB:*\n{p['total_size_gb']}"},
                        {"type": "mrkdwn", "text": f"*Job:*\n`{p['job_id']}`"},
                    ],
                }
            ],
        }
    return p  # generic


# ─────────────────────────────────────────────────────────────────────────────
# Finalização de cada execução
# ─────────────────────────────────────────────────────────────────────────────

def _finalize(
    started_at: datetime,
    imported: int,
    total_zombies: int,
    total_gb: float,
) -> None:
    """Registra o resultado da execução no log."""
    elapsed = (datetime.now(timezone.utc) - started_at).total_seconds()
    logger.info(
        "Varredura finalizada em %.1fs — %d arquivo(s) importado(s), "
        "%d zombie(s), %.2f GB.",
        elapsed, imported, total_zombies, total_gb,
    )
    logger.info("=" * 60)


# ─────────────────────────────────────────────────────────────────────────────
# Ciclo de vida do scheduler
# ─────────────────────────────────────────────────────────────────────────────

def start() -> None:
    """
    Inicia o BackgroundScheduler com o job de varredura configurado.
    Deve ser chamado uma única vez, antes do app.run().
    """
    if scheduler.running:
        logger.warning("Scheduler já está em execução.")
        return

    cron_hour   = int(os.environ.get("SCAN_CRON_HOUR",   "2"))
    cron_minute = int(os.environ.get("SCAN_CRON_MINUTE", "0"))

    scheduler.add_job(
        func       = run_zombie_hunter,
        trigger    = CronTrigger(hour=cron_hour, minute=cron_minute, timezone="UTC"),
        id         = "zombie_hunter_scan",
        name       = "Varredura periódica ZombieHunter",
        replace_existing = True,
        misfire_grace_time = 3600,   # tolera até 1h de atraso
    )

    scheduler.start()
    logger.info(
        "Scheduler iniciado — próxima execução: todo dia às %02d:%02d UTC.",
        cron_hour, cron_minute,
    )


def stop() -> None:
    """Para o scheduler graciosamente (aguarda jobs em execução terminarem)."""
    if scheduler.running:
        scheduler.shutdown(wait=True)
        logger.info("Scheduler parado.")


def trigger_now() -> None:
    """
    Dispara uma varredura imediatamente (fora do cron).
    Útil para testes manuais e botão "Executar agora" na UI.
    """
    logger.info("Varredura manual disparada via trigger_now().")
    scheduler.add_job(
        func             = run_zombie_hunter,
        id               = f"manual_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        replace_existing = False,
    )
