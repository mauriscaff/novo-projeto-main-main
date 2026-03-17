from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum

from pydantic import BaseModel, Field


class MonitoredSourceType(str, Enum):
    VCENTER = "vcenter"
    OCEANSTOR = "oceanstor"


class MonitoredSourceStatus(str, Enum):
    UNKNOWN = "unknown"
    ONLINE = "online"
    OFFLINE = "offline"
    DEGRADED = "degraded"
    DISABLED = "disabled"
    DELETED = "deleted"


class MonitoredSourceCreate(BaseModel):
    source_type: MonitoredSourceType
    name: str = Field(..., min_length=1, max_length=128)
    endpoint: str = Field(..., min_length=1, max_length=256)
    username: str = Field(..., min_length=1, max_length=128)
    secret: str = Field(..., min_length=1)
    is_active: bool = True


class MonitoredSourceUpdate(BaseModel):
    source_type: MonitoredSourceType | None = None
    name: str | None = Field(default=None, min_length=1, max_length=128)
    endpoint: str | None = Field(default=None, min_length=1, max_length=256)
    username: str | None = Field(default=None, min_length=1, max_length=128)
    secret: str | None = Field(default=None, min_length=1)
    status: MonitoredSourceStatus | None = None
    is_active: bool | None = None


class MonitoredSourceResponse(BaseModel):
    id: int
    source_type: MonitoredSourceType
    name: str
    endpoint: str
    username: str
    status: MonitoredSourceStatus
    last_collected_at: datetime | None
    last_connectivity_at: datetime | None
    is_active: bool
    is_deleted: bool
    deleted_at: datetime | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ConnectivityTestResponse(BaseModel):
    source_id: int
    source_type: MonitoredSourceType
    status: MonitoredSourceStatus
    reachable: bool
    checked_at: datetime
    message: str
    collector_stub: bool = True


class CollectionMarkRequest(BaseModel):
    collected_at: datetime | None = None
    status: MonitoredSourceStatus | None = None


class CollectionStatusItem(BaseModel):
    id: int
    source_type: MonitoredSourceType
    name: str
    endpoint: str
    status: MonitoredSourceStatus
    last_collected_at: datetime | None
    last_connectivity_at: datetime | None


class CollectionStatusSummary(BaseModel):
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    total: int
    online: int
    offline: int
    degraded: int
    unknown: int
    disabled: int
    items: list[CollectionStatusItem]
