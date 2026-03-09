"""
Modelo ORM para snapshots pre/pós descomissionamento de datastore.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, JSON, String, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DatastoreDecommissionReport(Base):
    __tablename__ = "datastore_decommission_reports"

    report_id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    pair_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    phase: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    datastore: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    vcenter_name: Mapped[str] = mapped_column(String(256), nullable=False)
    vcenter_host: Mapped[str] = mapped_column(String(256), nullable=False)
    total_items: Mapped[int] = mapped_column(Integer, nullable=False)
    total_size_gb: Mapped[float] = mapped_column(Float, nullable=False)
    deletable_items: Mapped[int] = mapped_column(Integer, nullable=False)
    deletable_size_gb: Mapped[float] = mapped_column(Float, nullable=False)
    breakdown: Mapped[dict] = mapped_column(JSON, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )


# Alias de compatibilidade para imports antigos no projeto.
DatastoreReportSnapshot = DatastoreDecommissionReport

