from functools import lru_cache
from pathlib import Path
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_PROJECT_ROOT = Path(__file__).resolve().parent
_DEFAULT_SQLITE_DB = (_PROJECT_ROOT / "vmdk_scanner.db").as_posix()


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

    @field_validator("debug", mode="before")
    @classmethod
    def _normalize_debug(cls, value):
        """
        Aceita aliases comuns de ambiente para evitar queda na inicializacao.
        Ex.: DEBUG=release, DEBUG=prod -> False.
        """
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            normalized = value.strip().lower()
            falsy = {
                "0",
                "false",
                "f",
                "no",
                "n",
                "off",
                "release",
                "prod",
                "production",
            }
            truthy = {
                "1",
                "true",
                "t",
                "yes",
                "y",
                "on",
                "debug",
                "dev",
                "development",
            }
            if normalized in falsy:
                return False
            if normalized in truthy:
                return True
        return value

    @field_validator("database_url", mode="before")
    @classmethod
    def _normalize_database_url(cls, value):
        if value is None:
            return value
        if not isinstance(value, str):
            return value

        raw = value.strip().strip('"').strip("'")
        normalized = raw.replace("\\", "/")

        if normalized.startswith("sqlite+aiosqlite:///./"):
            rel = normalized.removeprefix("sqlite+aiosqlite:///./")
            return f"sqlite+aiosqlite:///{(_PROJECT_ROOT / rel).as_posix()}"
        if normalized.startswith("sqlite+aiosqlite:///.//"):
            rel = normalized.removeprefix("sqlite+aiosqlite:///.//")
            return f"sqlite+aiosqlite:///{(_PROJECT_ROOT / rel).as_posix()}"
        if normalized.startswith("sqlite:///./"):
            rel = normalized.removeprefix("sqlite:///./")
            return f"sqlite:///{(_PROJECT_ROOT / rel).as_posix()}"
        if normalized.startswith("sqlite:///.//"):
            rel = normalized.removeprefix("sqlite:///.//")
            return f"sqlite:///{(_PROJECT_ROOT / rel).as_posix()}"

        return raw

    # Autenticação
    secret_key: str = "change-me-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 60
    api_key: str = "change-me-in-production"

    # Banco de dados
    # Usa caminho absoluto para evitar "perda" aparente de dados quando o processo
    # sobe a partir de diretórios diferentes.
    database_url: str = f"sqlite+aiosqlite:///{_DEFAULT_SQLITE_DB}"

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
    datastore_reports_verify_timeout_sec: int = 30

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

    # ── Governança opcional para descomissionamento de datastore ─────────────
    # Quando True, bloqueia ação DELETE de VMDK no fluxo de approvals se não
    # existir snapshot prévio para o datastore (relatório auditável).
    governance_require_datastore_snapshot_for_delete: bool = False


@lru_cache
def get_settings() -> Settings:
    return Settings()
