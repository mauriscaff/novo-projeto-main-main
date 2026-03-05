"""Schemas Pydantic para o endpoint de dashboard."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class VCenterBreakdown(BaseModel):
    vcenter: str
    total_vmdks: int
    size_gb: float


class TypeBreakdownEntry(BaseModel):
    count: int
    size_gb: float


class TrendEntry(BaseModel):
    job_id: str
    finished_at: datetime | None
    total_vmdks: int
    total_size_gb: float
    status: str


class RecentVmdkEntry(BaseModel):
    """Uma linha da tabela 'VMDKs detectados recentemente'."""

    path: str
    vcenter_host: str
    tamanho_gb: float | None
    tipo_zombie: str
    created_at: datetime | None


class DashboardResponse(BaseModel):
    # ── Totais históricos (todos os registros de todas as varreduras) ─────────
    total_vmdks_all_time: int
    """Número total de detecções zombie em todas as varreduras (inclui re-detecções)."""

    total_size_all_time_gb: float
    """Somatório dos tamanhos de todos os VMDKs zombie já detectados (GB)."""

    total_jobs: int
    """Total de jobs de varredura (todos os status)."""

    last_scan_at: datetime | None
    """Data/hora de término da última varredura concluída."""

    # ── Varredura mais recente (snapshot do estado atual) ─────────────────────
    latest_job_id: str | None
    latest_vmdks: int
    latest_size_gb: float

    # ── Breakdowns históricos ─────────────────────────────────────────────────
    by_vcenter: list[VCenterBreakdown]
    """Agregado por vCenter (nome): total de detecções e tamanho acumulado."""

    by_type: dict[str, TypeBreakdownEntry]
    """Agregado por tipo_zombie: {'ORPHANED': {'count': 42, 'size_gb': 512.3}, ...}"""

    # ── Tendência ─────────────────────────────────────────────────────────────
    trend_last_4: list[TrendEntry]
    """Últimas 4 varreduras concluídas (mais recente primeiro)."""

    # ── Whitelist ─────────────────────────────────────────────────────────────
    total_whitelisted: int
    """VMDKs atualmente marcados como 'seguros' e excluídos de varreduras futuras."""

    # ── Campos esperados pelo frontend (cards e tabela) ────────────────────────
    pending_approvals: int = 0
    """Tokens de aprovação pendentes (não executados nem cancelados)."""
    vcenter_count: int = 0
    """Número de vCenters ativos no inventário."""
    recent_vmdks: list[RecentVmdkEntry] = []
    """Últimos VMDKs detectados (para a tabela 'VMDKs detectados recentemente')."""


class TypeBreakdownStorage(BaseModel):
    count: int
    gb: float


class DatastoreStorageBreakdown(BaseModel):
    datastore_name: str
    vcenter: str
    total_gb: float
    total_tb: float
    zombie_count: int
    by_type: dict[str, TypeBreakdownStorage]
    percentage_of_total: float


class VCenterStorageBreakdown(BaseModel):
    vcenter: str
    total_gb: float
    zombie_count: int


class RecoverableStorageResponse(BaseModel):
    total_recoverable_gb: float
    total_recoverable_tb: float
    by_datastore: list[DatastoreStorageBreakdown]
    by_vcenter: list[VCenterStorageBreakdown]
    last_scan_at: datetime | None
