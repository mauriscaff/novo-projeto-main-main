"""
Modelo ORM para endpoints de webhook que recebem alertas pós-varredura.
"""

from datetime import datetime, timezone

from sqlalchemy import Boolean, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class WebhookEndpoint(Base):
    __tablename__ = "webhook_endpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # Identificação
    name: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    url: Mapped[str] = mapped_column(String(2048), nullable=False)
    """URL de destino do webhook (Teams, Slack, endpoint HTTP genérico)."""

    provider: Mapped[str] = mapped_column(String(32), default="generic", nullable=False)
    """'teams' | 'slack' | 'generic' — define o formato do payload."""

    description: Mapped[str | None] = mapped_column(Text)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    # Filtro de disparo
    min_zombies_to_fire: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    """Só dispara o webhook se total_found >= este valor (evita ruído)."""

    # Autenticação opcional (header customizado)
    secret_header: Mapped[str | None] = mapped_column(String(128))
    """Nome do header HTTP de autenticação, ex.: 'Authorization'."""

    secret_value: Mapped[str | None] = mapped_column(String(512))
    """Valor do header de autenticação, ex.: 'Bearer token123'.
    Armazenado em texto puro — o DB é local e protegido pela auth da API."""

    # Rastreamento de execuções
    last_fired_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_status_code: Mapped[int | None] = mapped_column(Integer)
    """Último HTTP status code recebido (0 = erro de conexão)."""
    fire_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    # Auditoria
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=_utcnow,
    )
