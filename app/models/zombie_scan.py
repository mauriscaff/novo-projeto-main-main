"""
Modelos ORM para os jobs de varredura zombie e seus resultados individuais.

Separados dos modelos originais (scan_jobs / vmdk_results) para não quebrar
o sistema legado de varredura por threshold de dias.
"""

from datetime import datetime

from sqlalchemy import DateTime, Float, Index, Integer, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base


class ZombieScanJob(Base):
    """Registro de um job de varredura zombie que pode abranger múltiplos vCenters."""

    __tablename__ = "zombie_scan_jobs"

    job_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    """UUID gerado no momento do disparo do job."""

    # Escopo da varredura
    vcenter_ids: Mapped[list] = mapped_column(JSON, nullable=False)
    """IDs dos vCenters varredos (lista de int)."""

    datacenters: Mapped[list | None] = mapped_column(JSON, nullable=True)
    """Nomes dos Datacenters varredos; NULL = todos os Datacenters de cada vCenter."""

    datastores: Mapped[list | None] = mapped_column(JSON, nullable=True)
    """Nomes explícitos dos Datastores/LUNs a varrer; NULL = todos os Datastores dos Datacenters acima."""

    # Estado do job
    status: Mapped[str] = mapped_column(String(20), default="pending", nullable=False)
    """pending | running | completed | failed"""

    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    error_messages: Mapped[list | None] = mapped_column(JSON, nullable=True)
    """Lista de erros parciais (um por vCenter/Datacenter com falha)."""

    # Totalizadores (preenchidos ao finalizar)
    total_vmdks: Mapped[int | None] = mapped_column(Integer)
    total_size_gb: Mapped[float | None] = mapped_column(Float)

    # Métricas por datastore (duração, arquivos, zombies) para troubleshooting
    datastore_metrics: Mapped[list | None] = mapped_column(JSON, nullable=True)
    """Lista de dicts: datastore_name, scan_start_time (ISO), scan_duration_seconds, files_found, zombies_found."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class ZombieVmdkRecord(Base):
    """Resultado individual de um VMDK zombie detectado durante uma varredura."""

    __tablename__ = "zombie_vmdk_records"
    __table_args__ = (
        Index("ix_zombie_vmdk_job_datastore_path", "job_id", "datastore", "path"),
        Index("ix_zombie_vmdk_job_datastore_tipo_size", "job_id", "datastore", "tipo_zombie", "tamanho_gb"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    job_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)

    # Localização
    path: Mapped[str] = mapped_column(String(1024), nullable=False)
    datastore: Mapped[str] = mapped_column(String(256), nullable=False)
    folder: Mapped[str] = mapped_column(String(512), default="")
    datastore_type: Mapped[str] = mapped_column(String(32), default="")

    # Metadados do arquivo
    tamanho_gb: Mapped[float | None] = mapped_column(Float)
    ultima_modificacao: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )

    # Classificação
    tipo_zombie: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    """ZombieType.value: ORPHANED | SNAPSHOT_ORPHAN | BROKEN_CHAIN |
    UNREGISTERED_DIR | POSSIBLE_FALSE_POSITIVE"""

    # Origem
    vcenter_host: Mapped[str] = mapped_column(String(256), nullable=False)
    vcenter_name: Mapped[str] = mapped_column(String(128), default="")
    datacenter: Mapped[str] = mapped_column(String(128), nullable=False)

    # Diagnóstico
    detection_rules: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    """Lista de strings com as regras acionadas (auditoria)."""

    false_positive_reason: Mapped[str | None] = mapped_column(Text)

    likely_causes: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    """Causas comuns prováveis desta classificação (exibido no dashboard)."""

    confidence_score: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    """Score de confiança 0–100 calculado pelo detector conforme critérios Broadcom."""

    # Links e localização para vCenter UI e URL /folder (Broadcom KB 301563)
    vcenter_deeplink_ui: Mapped[str] = mapped_column(String(1024), default="")
    """Link para abrir o datastore no vSphere HTML5 Client (MoRef)."""
    vcenter_deeplink_folder: Mapped[str] = mapped_column(String(1024), default="")
    """URL /folder apontando diretamente ao arquivo VMDK."""
    vcenter_deeplink_folder_dir: Mapped[str] = mapped_column(String(1024), default="")
    """URL /folder apontando à pasta do VMDK."""
    datacenter_path: Mapped[str] = mapped_column(String(256), default="")
    """Nome/path do Datacenter (ex.: Datacenter-Producao)."""
    datastore_name: Mapped[str] = mapped_column(String(256), default="")
    """Nome do datastore (ex.: DS_SSD_01)."""
    vmdk_folder: Mapped[str] = mapped_column(String(512), default="")
    """Pasta do VMDK no datastore (ex.: VM_ANTIGA_01)."""
    vmdk_filename: Mapped[str] = mapped_column(String(256), default="")
    """Nome do arquivo VMDK (ex.: VM_ANTIGA_01.vmdk)."""

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # Evidência estruturada por regra (preenchida pelo scanner, sobrevive sem migração se NULL)
    rule_evidence: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    """Resultado por cheque: orphan_days_check, min_size_check, inventory_check,
    content_library_check, shared_datastore_check, classification_reason."""
