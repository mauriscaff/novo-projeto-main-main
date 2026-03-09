"""
Historico auditavel de verificacoes automaticas de exclusao de datastore.

Cada registro representa uma comparacao baseline x verification para um
escopo (datastore + vcenter_host_scope), sem duplicar o mesmo par de jobs.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, Float, Integer, String, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class DatastoreDeletionVerificationRun(Base):
    __tablename__ = "datastore_deletion_verification_runs"
    __table_args__ = (
        UniqueConstraint(
            "datastore",
            "vcenter_host_scope",
            "baseline_job_id",
            "verification_job_id",
            name="uq_ds_delete_run_scope_pair",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    datastore: Mapped[str] = mapped_column(String(256), nullable=False, index=True)
    vcenter_host_scope: Mapped[str] = mapped_column(String(256), nullable=False, default="", index=True)
    baseline_job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    verification_job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    deleted_vmdk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    deleted_size_gb: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    remaining_vmdk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    remaining_size_gb: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
