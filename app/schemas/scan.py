from datetime import datetime

from pydantic import BaseModel

from app.models.scan_result import ScanStatus, VMDKStatus


class ScanJobCreate(BaseModel):
    vcenter_id: int


class ScanJobResponse(BaseModel):
    id: int
    vcenter_id: int
    status: ScanStatus
    started_at: datetime | None
    finished_at: datetime | None
    error_message: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class VMDKResultResponse(BaseModel):
    id: int
    scan_job_id: int
    vcenter_id: int
    datastore_name: str
    datastore_url: str | None
    vmdk_path: str
    size_gb: float | None
    status: VMDKStatus
    vm_name: str | None
    vm_moref: str | None
    last_modified: datetime | None
    days_since_modified: int | None
    created_at: datetime

    model_config = {"from_attributes": True}


class ScanSummary(BaseModel):
    scan_job_id: int
    vcenter_id: int
    total_vmdks: int
    attached: int
    orphaned: int
    zombie: int
    total_orphaned_size_gb: float
