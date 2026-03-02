"""
Modelo ORM para a whitelist de VMDKs revisados e considerados seguros.

VMDKs na whitelist são ignorados em varreduras futuras (excluídos durante a
fase de persistência em scan_runner.py). O registro histórico no
zombie_vmdk_records é preservado para auditoria.
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class VmdkWhitelist(Base):
    __tablename__ = "vmdk_whitelist"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # Caminho único do VMDK (chave de exclusão)
    path: Mapped[str] = mapped_column(String(1024), unique=True, nullable=False, index=True)
    """Caminho completo do VMDK, ex.: '[datastore] folder/name.vmdk'.
    A unicidade garante que cada caminho seja marcado apenas uma vez."""

    # Auditoria obrigatória
    justification: Mapped[str] = mapped_column(Text, nullable=False)
    """Justificativa obrigatória explicando por que o VMDK é seguro."""

    marked_by: Mapped[str] = mapped_column(String(128), nullable=False, default="api")
    """Identificador do usuário ou sistema que marcou o VMDK como seguro."""

    # Rastreabilidade — qual job/registro originou a marcação
    job_id: Mapped[str] = mapped_column(String(36), nullable=False)
    """UUID do ZombieScanJob onde o VMDK foi detectado."""

    record_id: Mapped[int | None] = mapped_column(Integer)
    """ID do ZombieVmdkRecord correspondente (nullable: registro pode ser purgado)."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
