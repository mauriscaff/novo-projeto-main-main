"""
Datastore capacity and SDRS recommendation routes.

Endpoints:
- GET /api/v1/capacity/report
- GET /api/v1/capacity/sdrs/recommendations
- POST /api/v1/capacity/sdrs/approve
- POST /api/v1/capacity/sdrs/execute
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.sdrs_policy_engine import build_sdrs_policy_input, evaluate_sdrs_policy
from app.core.vcenter.connection import vcenter_pool
from app.core.vcenter.connection_manager import connection_manager
from app.dependencies import get_current_user, get_db
from app.models.vcenter import VCenter
from app.services import balance_service
from config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)

SDRS_OPERATIONAL_MARGIN_PCT_DEFAULT = 20.0
SDRS_CAPACITY_BUFFER_PCT_DEFAULT = 12.0


class SdrsActionRequest(BaseModel):
    vcenter_id: int
    datacenter: str | None = None
    recommendation_ids: list[str] = Field(default_factory=list)
    comment: str | None = Field(default=None, max_length=500)


def _parse_datastore_scope(raw_scope: str | None) -> set[str]:
    if not raw_scope:
        return set()
    return {
        part.strip().lower()
        for part in raw_scope.split(",")
        if part and part.strip()
    }


def _build_datastore_cluster_map(datastores: list[dict]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for ds in datastores:
        name = str(ds.get("name") or "").strip()
        if not name:
            continue
        cluster = str(ds.get("datastore_cluster") or "").strip()
        if cluster:
            mapping[name] = cluster
    return mapping


def _build_mode_state(current_mode: str) -> dict:
    settings = get_settings()
    execution_enabled = not settings.readonly_mode
    return {
        "current": current_mode,
        "readonly_mode": settings.readonly_mode,
        "recommendation": {"enabled": True},
        "approval": {"enabled": True},
        "execution": {
            "enabled": execution_enabled,
            "blocked_reason": "readonly_mode" if not execution_enabled else None,
        },
    }


def _log_sdrs_audit(event: str, **fields) -> None:
    payload = {
        "event": event,
        "ts": datetime.now(timezone.utc).isoformat(),
        **fields,
    }
    logger.info("sdrs_audit %s", json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str))


async def _resolve_vcenter_and_service_instance(vcenter_id: int, db: AsyncSession) -> tuple[VCenter, object]:
    result = await db.execute(select(VCenter).where(VCenter.id == vcenter_id))
    vc = result.scalar_one_or_none()
    if not vc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"vCenter com id={vcenter_id} nao encontrado.",
        )
    if not vc.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"vCenter '{vc.name}' esta inativo.",
        )

    try:
        connection_manager.register(vc)
        si = vcenter_pool.get_service_instance(vc.id)
    except Exception as exc:
        logger.error("Falha ao conectar ao vCenter '%s': %s", vc.name, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"Nao foi possivel conectar ao vCenter '{vc.name}' ({vc.host}). "
                f"Verifique conectividade e credenciais. Detalhe: {exc}"
            ),
        )

    if si is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"vCenter '{vc.name}' nao disponivel no pool de conexoes.",
        )

    return vc, si


@router.get(
    "/report",
    summary="Relatorio de Capacidade de Datastores",
    tags=["Capacidade"],
)
async def capacity_report(
    vcenter_id: int = Query(..., description="ID do vCenter cadastrado no sistema"),
    datacenter: str | None = Query(
        default=None,
        description="Escopo opcional por datacenter. Ex.: DTC-SGI",
    ),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    vc, si = await _resolve_vcenter_and_service_instance(vcenter_id, db)
    datacenter_scope = datacenter.strip() if datacenter and datacenter.strip() else None

    try:
        if datacenter_scope:
            report = await balance_service.get_capacity_report(si, datacenter_scope)
        else:
            report = await balance_service.get_capacity_report(si)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except Exception as exc:
        logger.exception("Erro ao gerar relatorio de capacidade para '%s'", vc.name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao processar dados do vCenter: {exc}",
        )

    return {
        "vcenter": vc.name,
        "vcenter_host": vc.host,
        "vcenter_id": vcenter_id,
        "datacenter_scope": datacenter_scope,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **report,
    }


@router.get(
    "/sdrs/recommendations",
    summary="SDRS assistido (recommend-only) para redistribuicao entre datastores",
    tags=["Capacidade"],
)
async def sdrs_recommendations(
    vcenter_id: int = Query(..., description="ID do vCenter cadastrado no sistema"),
    datacenter: str | None = Query(
        default=None,
        description="Escopo opcional por datacenter. Ex.: DTC-SGI",
    ),
    datastores: str | None = Query(
        default=None,
        description="Escopo opcional de datastores (CSV). Ex.: DS01,DS02,DS03",
    ),
    mode: Literal["recommendation", "approval", "execution"] = Query(
        default="recommendation",
        description="Modo do fluxo SDRS: recommendation, approval ou execution.",
    ),
    utilization_threshold_pct: float = Query(
        default=80.0,
        ge=50.0,
        le=95.0,
        description="Threshold soft de utilizacao por datastore para acionar recomendacao.",
    ),
    io_latency_threshold_ms: float = Query(
        default=15.0,
        ge=1.0,
        le=100.0,
        description="Threshold de I/O para referencia (secundario nesta fase).",
    ),
    operational_margin_pct: float = Query(
        default=SDRS_OPERATIONAL_MARGIN_PCT_DEFAULT,
        ge=5.0,
        le=40.0,
        description="Margem minima de capacidade apos recomendacao.",
    ),
    capacity_buffer_pct: float = Query(
        default=SDRS_CAPACITY_BUFFER_PCT_DEFAULT,
        ge=0.0,
        le=50.0,
        description="Buffer de risco (crescimento/snapshot/consolidacao/swap).",
    ),
    max_moves: int = Query(
        default=10,
        ge=1,
        le=100,
        description="Limite maximo de recomendacoes retornadas.",
    ),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    settings = get_settings()
    vc, si = await _resolve_vcenter_and_service_instance(vcenter_id, db)
    datacenter_scope = datacenter.strip() if datacenter and datacenter.strip() else None

    if mode == "execution":
        if settings.readonly_mode:
            _log_sdrs_audit(
                "sdrs_execution_blocked",
                vcenter_id=vcenter_id,
                vcenter=vc.name,
                datacenter=datacenter_scope,
                reason_code="READONLY_MODE_ACTIVE",
            )
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="READONLY_MODE=true bloqueia execution mode.",
            )
        raise HTTPException(
            status_code=status.HTTP_501_NOT_IMPLEMENTED,
            detail="Execution mode ainda nao implementado nesta fase.",
        )

    try:
        if datacenter_scope:
            report = await balance_service.get_capacity_report(si, datacenter_scope)
        else:
            report = await balance_service.get_capacity_report(si)
    except RuntimeError as exc:
        raise HTTPException(status_code=status.HTTP_504_GATEWAY_TIMEOUT, detail=str(exc))
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    except Exception as exc:
        logger.exception("Erro ao gerar recomendacoes SDRS para '%s'", vc.name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao processar dados do vCenter: {exc}",
        )

    selected_scope = _parse_datastore_scope(datastores)
    policy_input = build_sdrs_policy_input(
        report.get("datastores", []),
        selected_scope=selected_scope,
        utilization_threshold_pct=utilization_threshold_pct,
        io_latency_threshold_ms=io_latency_threshold_ms,
        operational_margin_pct=operational_margin_pct,
        capacity_buffer_pct=capacity_buffer_pct,
        max_moves=max_moves,
        mode=mode,
    )
    engine_result = evaluate_sdrs_policy(policy_input)
    summary = engine_result.summary
    recommendations = list(engine_result.recommendations)
    blocked_sources = list(engine_result.blocked_sources)
    notes = list(engine_result.notes)
    decisions = [decision.model_dump() for decision in engine_result.decisions]
    audit_trail: list[dict] = []
    datastore_cluster_map = _build_datastore_cluster_map(report.get("datastores", []))

    for rec in recommendations:
        src = str(rec.get("source_datastore") or "")
        tgt = str(rec.get("target_datastore") or "")
        if src and src in datastore_cluster_map:
            rec["source_datastore_cluster"] = datastore_cluster_map[src]
        if tgt and tgt in datastore_cluster_map:
            rec["target_datastore_cluster"] = datastore_cluster_map[tgt]

    for blocked in blocked_sources:
        src = str(blocked.get("source_datastore") or "")
        if src and src in datastore_cluster_map:
            blocked["source_datastore_cluster"] = datastore_cluster_map[src]

    for index, decision in enumerate(decisions, start=1):
        src = str(decision.get("source_datastore") or "")
        tgt = str(decision.get("target_datastore") or "")
        if src and src in datastore_cluster_map:
            decision["source_datastore_cluster"] = datastore_cluster_map[src]
        if tgt and tgt in datastore_cluster_map:
            decision["target_datastore_cluster"] = datastore_cluster_map[tgt]

        decision_audit = dict(decision.get("audit_payload") or {})
        decision_audit.update(
            {
                "decision_index": index,
                "vcenter_id": vcenter_id,
                "vcenter": vc.name,
                "datacenter": datacenter_scope,
            }
        )
        decision["audit_payload"] = decision_audit
        audit_trail.append(decision_audit)
        _log_sdrs_audit(**decision_audit)

    notes.append(
        f"Threshold de I/O configurado em {io_latency_threshold_ms:.1f} ms (uso secundario/opcional)."
    )

    _log_sdrs_audit(
        "sdrs_plan_generated",
        vcenter_id=vcenter_id,
        vcenter=vc.name,
        datacenter=datacenter_scope,
        mode=mode,
        recommendations=len(recommendations),
        blocked_sources=len(blocked_sources),
    )

    response_mode = "recommend_only" if mode == "recommendation" else mode
    return {
        "mode": response_mode,
        "modes": _build_mode_state(mode),
        "vcenter": vc.name,
        "vcenter_host": vc.host,
        "vcenter_id": vcenter_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": {
            "datacenter": datacenter_scope,
            "selected_datastores": sorted(selected_scope),
            "utilization_threshold_pct": utilization_threshold_pct,
            "io_latency_threshold_ms": io_latency_threshold_ms,
            "operational_margin_pct": operational_margin_pct,
            "capacity_buffer_pct": capacity_buffer_pct,
            "max_moves": max_moves,
        },
        "summary": summary,
        "recommendations": recommendations,
        "blocked_sources": blocked_sources,
        "decisions": decisions,
        "audit_trail": audit_trail,
        "notes": notes,
    }


@router.post(
    "/sdrs/approve",
    summary="Registrar aprovacao de plano SDRS (fase approval)",
    tags=["Capacidade"],
)
async def approve_sdrs_plan(
    body: SdrsActionRequest,
    _: dict = Depends(get_current_user),
):
    if not body.recommendation_ids:
        _log_sdrs_audit(
            "sdrs_approval_rejected",
            vcenter_id=body.vcenter_id,
            datacenter=body.datacenter,
            reason_code="EMPTY_RECOMMENDATION_IDS",
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="recommendation_ids nao pode ser vazio para approval mode.",
        )

    _log_sdrs_audit(
        "sdrs_approval_registered",
        vcenter_id=body.vcenter_id,
        datacenter=body.datacenter,
        recommendation_count=len(body.recommendation_ids),
        comment=(body.comment or ""),
    )
    readonly_mode = get_settings().readonly_mode
    return {
        "mode": "approval",
        "status": "approved_for_execution",
        "readonly_mode": readonly_mode,
        "execution_enabled": not readonly_mode,
        "vcenter_id": body.vcenter_id,
        "datacenter": body.datacenter,
        "approved_count": len(body.recommendation_ids),
        "approved_at": datetime.now(timezone.utc).isoformat(),
    }


@router.post(
    "/sdrs/execute",
    summary="Executar plano SDRS aprovado (fase execution)",
    tags=["Capacidade"],
)
async def execute_sdrs_plan(
    body: SdrsActionRequest,
    _: dict = Depends(get_current_user),
):
    settings = get_settings()
    if settings.readonly_mode:
        _log_sdrs_audit(
            "sdrs_execution_blocked",
            vcenter_id=body.vcenter_id,
            datacenter=body.datacenter,
            reason_code="READONLY_MODE_ACTIVE",
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="READONLY_MODE=true bloqueia execution mode.",
        )

    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Execution mode ainda nao implementado nesta fase.",
    )





