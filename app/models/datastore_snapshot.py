"""
Modelo ORM para snapshot auditável de descomissionamento de datastore.

Cada snapshot registra, em um instante de tempo, o volume encontrado para
um datastore específico de um vCenter (com filtro opcional por datacenter).
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, JSON, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DatastoreDecomSnapshot(Base):
    __tablename__ = "datastore_decom_snapshots"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    requested_vcenter_ref: Mapped[str] = mapped_column(String(128), nullable=False)
    resolved_vcenter_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    resolved_vcenter_name: Mapped[str] = mapped_column(String(128), nullable=False)
    resolved_vcenter_host: Mapped[str] = mapped_column(String(256), nullable=False)

    datacenter: Mapped[str | None] = mapped_column(String(128))
    datastore_name: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    source_job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)

    total_itens: Mapped[int] = mapped_column(Integer, nullable=False)
    total_size_gb: Mapped[float] = mapped_column(Float, nullable=False)
    breakdown: Mapped[dict] = mapped_column(JSON, nullable=False)

    generated_by: Mapped[str | None] = mapped_column(String(128))
    request_payload: Mapped[dict | None] = mapped_column(JSON)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

