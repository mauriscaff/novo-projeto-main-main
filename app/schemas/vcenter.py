from datetime import datetime

from pydantic import BaseModel, Field


class VCenterCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, examples=["vcenter-prod"])
    host: str = Field(..., examples=["vcenter.empresa.com"])
    port: int = Field(default=443, ge=1, le=65535)
    username: str = Field(..., examples=["administrator@vsphere.local"])
    password: str = Field(..., min_length=1)
    disable_ssl_verify: bool = True


class VCenterUpdate(BaseModel):
    name: str | None = Field(default=None, max_length=128)
    host: str | None = None
    port: int | None = Field(default=None, ge=1, le=65535)
    username: str | None = None
    password: str | None = None
    disable_ssl_verify: bool | None = None
    is_active: bool | None = None


class VCenterResponse(BaseModel):
    id: int
    name: str
    host: str
    port: int
    username: str
    disable_ssl_verify: bool
    is_active: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
