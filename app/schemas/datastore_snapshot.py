"""
Schemas Pydantic para snapshot de descomissionamento de datastore.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


class DatastoreSnapshotCreateRequest(BaseModel):
    vcenter_id: int | str = Field(
        ...,
        description="ID inteiro ou nome do vCenter cadastrado.",
        examples=[1, "vcenter-prod"],
    )
    datacenter: str | None = Field(
        default=None,
        description="Nome exato do Datacenter. Omitir para considerar todos.",
        examples=["Datacenter-Prod"],
    )
    datastore_name: str = Field(
        ...,
        min_length=1,
        description="Nome exato do datastore a ser snapshotado.",
        examples=["DS_SSD_01"],
    )

    @field_validator("datacenter", "datastore_name")
    @classmethod
    def strip_and_validate(cls, value: str | None) -> str | None:
        if value is None:
            return value
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("Campo não pode ser vazio.")
        return cleaned


class DatastoreSnapshotResponse(BaseModel):
    id: int
    requested_vcenter_ref: str
    resolved_vcenter_id: int
    resolved_vcenter_name: str
    resolved_vcenter_host: str
    datacenter: str | None
    datastore_name: str
    source_job_id: str
    total_itens: int
    total_size_gb: float
    breakdown: dict[str, int]
    timestamp: datetime
    generated_by: str | None
    conclusao: str = "base para auditoria p\u00f3s-descomissionamento"

    model_config = {"from_attributes": True}
