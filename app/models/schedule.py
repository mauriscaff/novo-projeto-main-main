"""
Modelo ORM para agendamentos de varredura zombie.
Cada ScanSchedule persiste uma expressão cron + escopo de varredura.
O APScheduler carrega todos os schedules ativos na inicialização da aplicação.
"""

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Integer, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


def _utcnow() -> datetime:
    """Retorna datetime atual em UTC — usado como onupdate pelo SQLAlchemy ORM."""
    return datetime.now(timezone.utc)


class ScanSchedule(Base):
    __tablename__ = "scan_schedules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # Identificação
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    """Nome único e legível do agendamento (ex.: 'daily-prod-2h')."""

    description: Mapped[str | None] = mapped_column(Text)

    # Gatilho temporal
    cron_expression: Mapped[str] = mapped_column(String(64), nullable=False)
    """Expressão cron de 5 campos (minuto hora dia mês dia_semana).
    Exemplos: '0 2 * * *' (todo dia às 2h), '0 */6 * * *' (a cada 6h)."""

    # Escopo da varredura
    vcenter_ids: Mapped[list] = mapped_column(JSON, nullable=False)
    """Lista de IDs (int) ou nomes (str) dos vCenters a varrer."""

    datacenters: Mapped[list | None] = mapped_column(JSON, nullable=True)
    """Lista de nomes de Datacenters; NULL = todos os DCs de cada vCenter."""

    # Estado
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    """False = agendamento pausado (job removido do APScheduler)."""

    # Rastreamento de execuções
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    """Última vez que o schedule foi disparado automaticamente."""

    last_job_id: Mapped[str | None] = mapped_column(String(36))
    """UUID do último ZombieScanJob gerado por este schedule."""

    run_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    """Número total de execuções disparadas por este schedule."""

    # Auditoria
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=_utcnow,
    )
