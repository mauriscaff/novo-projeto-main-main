from datetime import datetime
from enum import Enum as PyEnum

from sqlalchemy import DateTime, Enum, Float, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ScanStatus(str, PyEnum):
    pending = "pending"
    running = "running"
    completed = "completed"
    failed = "failed"


class VMDKStatus(str, PyEnum):
    attached = "attached"
    orphaned = "orphaned"
    zombie = "zombie"


class ScanJob(Base):
    """Representa uma execução de varredura (job)."""

    __tablename__ = "scan_jobs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    vcenter_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    status: Mapped[ScanStatus] = mapped_column(
        Enum(ScanStatus), default=ScanStatus.pending
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_message: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class VMDKResult(Base):
    """Resultado individual de um VMDK encontrado durante a varredura."""

    __tablename__ = "vmdk_results"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    scan_job_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    vcenter_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)

    # Identificação do VMDK
    datastore_name: Mapped[str] = mapped_column(String(256))
    datastore_url: Mapped[str | None] = mapped_column(String(512))
    vmdk_path: Mapped[str] = mapped_column(String(1024))
    size_gb: Mapped[float | None] = mapped_column(Float)

    # Classificação
    status: Mapped[VMDKStatus] = mapped_column(Enum(VMDKStatus))
    # Nome da VM associada (NULL quando orphaned/zombie)
    vm_name: Mapped[str | None] = mapped_column(String(256))
    vm_moref: Mapped[str | None] = mapped_column(String(64))

    # Metadados temporais
    last_modified: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    days_since_modified: Mapped[int | None] = mapped_column(Integer)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
