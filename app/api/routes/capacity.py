"""
Rota de Capacidade de Datastores — GET /api/v1/capacity/report

Retorna um relatório de saúde e balanceamento de todos os datastores
visíveis no vCenter especificado.

Resposta JSON:
{
  "vcenter":       "vc1",
  "generated_at":  "2026-03-17T...",
  "summary":       { "total_datastores": N, "red": R, "yellow": Y, "green": G, ... },
  "datastores":    [ { "name", "capacity_gb", "free_gb", "use_pct", "health_status", "top_vms" }, ... ],
  "recommendations": [ "Texto de ação sugerida", ... ]
}
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.vcenter.connection import vcenter_pool
from app.core.vcenter.connection_manager import connection_manager
from app.dependencies import get_current_user, get_db
from app.models.vcenter import VCenter
from app.services import balance_service

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/report",
    summary="Relatório de Capacidade de Datastores",
    description="""
Consulta todos os Datastores do vCenter especificado e retorna:

- **Espaço usado, livre e comprometido** por datastore (em GB)
- **Status de Saúde** baseado na taxa de uso:
  - 🟢 Verde: < 70%
  - 🟡 Amarelo: 70–85%
  - 🔴 Vermelho: > 85%
- **Candidatas à migração**: até 3 VMs mais pesadas nos datastores vermelhos
- **Recomendações de ação** automáticas

Requer autenticação JWT ou API Key.
""",
    tags=["Capacidade"],
)
async def capacity_report(
    vcenter_id: int = Query(..., description="ID do vCenter cadastrado no sistema"),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
):
    # ── Resolve o vCenter ─────────────────────────────────────────────────────
    result = await db.execute(select(VCenter).where(VCenter.id == vcenter_id))
    vc = result.scalar_one_or_none()
    if not vc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"vCenter com id={vcenter_id} não encontrado.",
        )
    if not vc.is_active:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"vCenter '{vc.name}' está inativo.",
        )

    # ── Obtém ServiceInstance do pool ─────────────────────────────────────────
    try:
        connection_manager.register(vc)
        si = vcenter_pool.get_service_instance(vc.id)
    except Exception as exc:
        logger.error("Falha ao conectar ao vCenter '%s': %s", vc.name, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=(
                f"Não foi possível conectar ao vCenter '{vc.name}' ({vc.host}). "
                f"Verifique a conectividade e as credenciais. Detalhe: {exc}"
            ),
        )

    if si is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"vCenter '{vc.name}' não disponível no pool de conexões.",
        )

    # ── Gera o relatório ──────────────────────────────────────────────────────
    try:
        report = await balance_service.get_capacity_report(si)
    except RuntimeError as exc:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=str(exc),
        )
    except Exception as exc:
        logger.exception("Erro ao gerar relatório de capacidade para '%s'", vc.name)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Erro interno ao processar dados do vCenter: {exc}",
        )

    return {
        "vcenter": vc.name,
        "vcenter_host": vc.host,
        "vcenter_id": vcenter_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        **report,
    }
