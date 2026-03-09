"""
Schemas Pydantic para snapshots pre/pós descomissionamento de datastore.
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator


class DatastoreReportSnapshotCreateRequest(BaseModel):
    phase: Literal["pre_delete", "post_delete"] = Field(
        ...,
        description="Fase do snapshot: pre_delete ou post_delete.",
    )
    job_id: str = Field(..., min_length=1, description="Job ID de origem do scan.")
    datastore: str = Field(..., min_length=1, description="Nome exato do datastore.")
    pair_id: str | None = Field(
        default=None,
        max_length=64,
        description="Identificador opcional para parear pre/post.",
    )

    @field_validator("job_id", "datastore", "pair_id")
    @classmethod
    def strip_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Campo nao pode ser vazio.")
        return cleaned


class DatastoreReportSnapshotResponse(BaseModel):
    report_id: int
    pair_id: str
    phase: Literal["pre_delete", "post_delete"]
    job_id: str
    datastore: str
    vcenter_name: str
    vcenter_host: str
    total_items: int
    total_size_gb: float
    deletable_items: int
    deletable_size_gb: float
    breakdown: dict[str, int]
    created_at: datetime

    model_config = {"from_attributes": True}


class DatastoreReportTotals(BaseModel):
    total_items: int
    total_size_gb: float
    deletable_items: int
    deletable_size_gb: float


class DatastoreReportCompareResponse(BaseModel):
    pre_report_id: int
    post_report_id: int
    datastore: str
    removed_items: int
    removed_size_gb: float
    removed_breakdown: dict[str, int]
    pre_totals: DatastoreReportTotals
    post_totals: DatastoreReportTotals


class DatastoreDeletedVmdkEvidence(BaseModel):
    path: str
    tipo_zombie: str
    tamanho_gb: float
    last_seen_job_id: str | None = None
    datacenter: str | None = None
    vcenter_name: str | None = None
    vcenter_host: str | None = None


class DatastoreReportFileVerificationResponse(BaseModel):
    pair_id: str
    datastore: str
    datastore_name: str
    pre_report_id: int
    post_report_id: int
    pre_job_id: str
    post_job_id: str
    datastore_found_in_pre: bool
    datastore_found_in_post: bool
    datastore_status: Literal["removed", "still_present", "unknown"]
    removed_files_count: int
    removed_size_gb: float
    deleted_files_count: int
    deleted_size_gb: float
    size_gain_gb: float
    size_gain_percent: float
    pre_total_size_gb: float
    post_total_size_gb: float
    remaining_size_gb: float
    deleted_breakdown: dict[str, int]
    deleted_size_breakdown_gb: dict[str, float]
    remaining_files_count: int
    verification_status: Literal["fully_removed", "partially_removed", "no_gain"]
    page: int
    page_size: int
    total_evidence: int
    has_more_evidence: bool
    status: str
    message: str
    deleted_vmdks: list[DatastoreDeletedVmdkEvidence]


class DeletedVmdkItem(BaseModel):
    path: str
    tamanho_gb: float
    tipo_zombie: str
    datacenter: str | None = None
    last_seen_job_id: str


class DatastoreDeletionVerificationResponse(BaseModel):
    datastore: str
    vcenter_host: str | None = None
    baseline_job_id: str
    verification_job_id: str
    datastore_removed: bool
    status: Literal["datastore_removed", "partial_cleanup", "no_cleanup"]
    message: str
    baseline_files_count: int
    verification_files_count: int
    deleted_vmdk_count: int
    remaining_vmdk_count: int
    baseline_size_gb: float
    baseline_size_tb: float
    deleted_size_gb: float
    deleted_size_tb: float
    remaining_size_gb: float
    remaining_size_tb: float
    size_gain_percent: float
    deleted_breakdown: dict[str, int]
    deleted_size_breakdown_gb: dict[str, float]
    deleted_vmdks: list[DeletedVmdkItem]


class DatastoreDeletionVerificationTotalsResponse(BaseModel):
    datastore: str | None = None
    vcenter_host: str | None = None
    total_verifications: int
    total_datastores_removed: int
    total_partial_cleanup: int
    total_no_cleanup: int
    total_deleted_vmdks: int
    total_deleted_size_gb: float
    last_verification_at: datetime | None = None
