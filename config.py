from functools import lru_cache
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", ".env.local"),  # .env.local override (não versionado)
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Aplicação
    app_name: str = "VMDK Zombie Scanner"
    app_version: str = "1.0.0"
    debug: bool = False

    # Autenticação
    secret_key: str = "change-me-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    api_key: str = "change-me-in-production"

    # Banco de dados
    database_url: str = "sqlite+aiosqlite:///./vmdk_scanner.db"

    # vCenter padrão (opcional)
    default_vcenter_host: str = ""
    default_vcenter_user: str = ""
    default_vcenter_password: str = ""
    default_vcenter_port: int = 443
    default_vcenter_disable_ssl_verify: bool = True

    # Criptografia de senhas (Fernet)
    # Gere com: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    fernet_key: str = "TROQUE_ESTA_CHAVE_FERNET"

    # Connection pool
    vcenter_connect_timeout_sec: int = 30
    vcenter_max_retries: int = 3
    vcenter_retry_base_delay_sec: float = 2.0

    # Scanner — thresholds configuráveis (Broadcom KB 404094)
    # Tempo mínimo sem referência para VMDK normal ser considerado zombie
    orphan_days: int = 60
    # Tempo mínimo para delta/snapshot orphan ser reportado
    stale_snapshot_days: int = 15
    # Arquivos MENORES que este limite são ignorados (exceto BROKEN_CHAIN)
    min_file_size_mb: int = 50
    scan_max_workers: int = 4
    # Timeout por datastore (segundos) — DatastoreBrowser pode demorar em LUNs/vSAN grandes
    scan_datastore_timeout_sec: int = 900  # 15 minutos
    # Duração máxima total do job (segundos) — ao exceder, job é marcado como failed
    scan_job_max_duration_sec: int = 14400  # 4 horas

    # Mantido por compatibilidade retroativa — use orphan_days
    orphaned_threshold_days: int = 60

    # Agendamento (APScheduler)
    # Expressão cron de exemplo para referência; o valor real fica em cada ScanSchedule.
    scan_cron_default: str = "0 2 * * *"
    """Cron padrão sugerido ao criar agendamentos via API (não é aplicado automaticamente)."""

    # ── Interface Web Flask (web/app.py) ─────────────────────────────────────
    flask_port: int = 5000
    """Porta TCP do servidor Flask (padrão: 5000)."""

    # ── Agendador cron (web/scheduler/cron_runner.py) ─────────────────────────
    scan_cron_hour:   int = 2
    scan_cron_minute: int = 0
    ps_script_path:   str = "scripts/ZombieHunter.ps1"
    reports_dir:      str = "reports/data"

    # ── Modo somente-leitura (READONLY_MODE) ─────────────────────────────────
    # PADRÃO: True — a API nunca executa operações destrutivas por padrão.
    #
    # Para habilitar operações destrutivas (delete/move/rename de VMDKs):
    #   1. Defina READONLY_MODE=false neste arquivo .env
    #   2. Obtenha um ApprovalToken via POST /api/v1/approvals
    #   3. Envie X-Approval-Token no header + "confirmed": true no corpo
    #
    # ⚠️  JAMAIS defina false como padrão neste código.
    readonly_mode: bool = True


@lru_cache
def get_settings() -> Settings:
    return Settings()
