"""
Schemas Pydantic para os endpoints de varredura zombie
(POST /scan/start, GET /scan/jobs, GET /scan/results, GET /scan/results/export).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator


# ─────────────────────────────────────────────────────────────────────────────
# Request
# ─────────────────────────────────────────────────────────────────────────────


class ScanStartRequest(BaseModel):
    vcenter_ids: list[int | str] = Field(
        ...,
        min_length=1,
        description=(
            "Lista de vCenters a varrer. Aceita IDs inteiros ou nomes (string). "
            "Exemplo: [1, 2] ou [\"vcenter-prod\", \"vcenter-dr\"]"
        ),
        examples=[[1, 2]],
    )
    datacenters: list[str] | None = Field(
        default=None,
        description=(
            "Datacenters a varrer em cada vCenter. "
            "Omita para varrer TODOS os Datacenters de cada vCenter."
        ),
        examples=[["Datacenter-Producao", "Datacenter-DR"]],
    )
    datastores: list[str] | None = Field(
        default=None,
        description=(
            "Datastores (LUNs) a varrer nos Datacenters escolhidos. "
            "Omita para varrer TODOS os Datastores."
        ),
        examples=[["ds-prod-01", "ds-backup-02"]],
    )


class ScanStartByDatastoreRequest(BaseModel):
    """
    Requisição para varredura com escopo explícito por datastore.
    Diferente de ScanStartRequest, datastores é obrigatório.
    """

    vcenter_ids: list[int | str] = Field(
        ...,
        min_length=1,
        description=(
            "Lista de vCenters a varrer. Aceita IDs inteiros ou nomes (string). "
            "Exemplo: [1, 2] ou [\"vcenter-prod\", \"vcenter-dr\"]"
        ),
        examples=[[1, 2]],
    )
    datastores: list[str] = Field(
        ...,
        min_length=1,
        description=(
            "Datastores (LUNs) a varrer. Obrigatório neste endpoint."
        ),
        examples=[["ds-prod-01", "ds-backup-02"]],
    )
    datacenters: list[str] | None = Field(
        default=None,
        description=(
            "Datacenters a varrer em cada vCenter. "
            "Omita para varrer TODOS os Datacenters de cada vCenter."
        ),
        examples=[["Datacenter-Producao", "Datacenter-DR"]],
    )

    @field_validator("datastores", "datacenters", mode="before")
    @classmethod
    def normalize_string_lists(cls, value):
        if value is None:
            return value
        if not isinstance(value, list):
            return value
        cleaned: list[str] = []
        for item in value:
            text = str(item).strip()
            if text:
                cleaned.append(text)
        return cleaned

    @model_validator(mode="after")
    def validate_required_datastores(self):
        if not self.datastores:
            raise ValueError("datastores deve conter ao menos um nome de datastore.")
        return self


# ─────────────────────────────────────────────────────────────────────────────
# Job status & summary
# ─────────────────────────────────────────────────────────────────────────────


class ZombieBreakdown(BaseModel):
    """Contagem de VMDKs zombie por categoria."""

    ORPHANED: int = 0
    SNAPSHOT_ORPHAN: int = 0
    BROKEN_CHAIN: int = 0
    UNREGISTERED_DIR: int = 0
    POSSIBLE_FALSE_POSITIVE: int = 0


class DatastoreScanMetricSchema(BaseModel):
    """Métricas de varredura por datastore (duração, arquivos, zombies)."""

    datastore_name: str
    scan_start_time: datetime
    scan_duration_seconds: float
    files_found: int
    zombies_found: int


class ScanJobSummary(BaseModel):
    """Totalizadores calculados ao final de um job."""

    total_vmdks_encontrados: int
    total_size_gb: float
    breakdown: ZombieBreakdown
    # Reúne tudo que pode ser excluído do vCenter (ORPHANED, SNAPSHOT_ORPHAN, BROKEN_CHAIN, UNREGISTERED_DIR)
    total_excluiveis: int = 0
    total_excluiveis_gb: float = 0.0


class ScanJobBase(BaseModel):
    """Campos comuns a todas as respostas de job."""

    job_id: str
    vcenter_ids: list[int | str]
    datacenters: list[str] | None
    datastores: list[str] | None
    status: str
    started_at: datetime | None
    finished_at: datetime | None
    error_messages: list[str] | None
    created_at: datetime


class ScanStartResponse(ScanJobBase):
    """Resposta imediata do POST /scan/start (job ainda pending/running)."""

    pass


class ScanJobProgress(BaseModel):
    """Progresso em tempo real de um job em execução."""

    current: str = ""
    """Etapa atual sendo executada."""

    ds_index: int = 0
    """Número do datastore sendo processado agora."""

    ds_total: int = 0
    """Total de datastores a processar neste datacenter."""

    ds_current: str = ""
    """Nome do datastore sendo varrido agora."""

    ds_status: str = ""
    """Status do datastore atual: scanning | done | failed | inaccessible | maintenance."""

    steps: list[dict] = []
    """Log de passos: [{"ts": "HH:MM:SS", "level": "info|success|warning|error", "msg": "..."}]"""


class ScanJobStatusResponse(ScanJobBase):
    """Resposta detalhada do GET /scan/jobs/{job_id}."""

    summary: ScanJobSummary | None = None
    """Preenchido apenas quando status = completed."""

    datastore_metrics: list[DatastoreScanMetricSchema] | None = None
    """Métricas por datastore (duração, files_found, zombies_found). Preenchido quando status = completed."""

    progress: ScanJobProgress | None = None
    """Progresso em tempo real — preenchido apenas quando status = running/pending."""


# ─────────────────────────────────────────────────────────────────────────────
# Resultado individual de VMDK
# ─────────────────────────────────────────────────────────────────────────────


class ZombieResultItem(BaseModel):
    """Um único VMDK zombie encontrado durante a varredura."""

    id: int
    job_id: str
    path: str
    datastore: str
    folder: str
    datastore_type: str
    tamanho_gb: float | None
    ultima_modificacao: datetime | None
    tipo_zombie: str
    vcenter_host: str
    vcenter_name: str
    datacenter: str
    detection_rules: list[str]
    likely_causes: list[str] = []
    false_positive_reason: str | None
    created_at: datetime
    # Status computado no endpoint (whitelist check)
    status: str = "NOVO"
    # Score calculado pelo detector durante a varredura e persistido no banco
    confidence_score: int = 0
    # Links e localização no vCenter (vSphere UI e URL /folder)
    vcenter_deeplink_ui: str = ""
    vcenter_deeplink_folder: str = ""
    vcenter_deeplink_folder_dir: str = ""
    datacenter_path: str = ""
    datastore_name: str = ""
    vmdk_folder: str = ""
    vmdk_filename: str = ""
    # Evidência estruturada por regra (None para registros anteriores à feature)
    rule_evidence: dict | None = None

    model_config = {"from_attributes": True}



# ─────────────────────────────────────────────────────────────────────────────
# Paginação
# ─────────────────────────────────────────────────────────────────────────────


class PaginatedResults(BaseModel):
    """Lista paginada de resultados com metadados de navegação."""

    items: list[ZombieResultItem]
    total: int
    page: int
    page_size: int
    total_pages: int
    total_size_gb: float = 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Parâmetros de filtro e ordenação (usados via Query params)
# ─────────────────────────────────────────────────────────────────────────────

SortByField = Literal["tamanho_gb", "ultima_modificacao", "tipo_zombie", "datastore", "confidence_score"]
SortOrder = Literal["asc", "desc"]
