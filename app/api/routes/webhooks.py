"""
CRUD de endpoints de webhook para alertas pós-varredura.

Prefixo registrado em main.py: /api/v1/webhooks

  POST   /              Cadastra novo webhook (Teams, Slack, HTTP genérico)
  GET    /              Lista webhooks cadastrados
  GET    /{id}          Detalha um webhook
  PATCH  /{id}          Atualiza campos (incluindo ativar/desativar)
  DELETE /{id}          Remove webhook
  POST   /{id}/test     Dispara payload de teste para validar a URL
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.webhook_dispatcher import _format_payload
from app.dependencies import get_current_user, get_db
from app.models.webhook import WebhookEndpoint
from app.schemas.webhook import WebhookCreate, WebhookResponse, WebhookUpdate

router = APIRouter()
logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _to_response(w: WebhookEndpoint) -> WebhookResponse:
    """Converte ORM → schema. Mascara o secret_value."""
    return WebhookResponse(
        id=w.id,
        name=w.name,
        url=w.url,
        provider=w.provider,
        description=w.description,
        is_active=w.is_active,
        min_zombies_to_fire=w.min_zombies_to_fire,
        secret_header=w.secret_header,
        secret_value_masked="***" if w.secret_value else None,
        last_fired_at=w.last_fired_at,
        last_status_code=w.last_status_code,
        fire_count=w.fire_count or 0,
        created_at=w.created_at,
        updated_at=w.updated_at,
    )


async def _get_or_404(webhook_id: int, db: AsyncSession) -> WebhookEndpoint:
    w = await db.get(WebhookEndpoint, webhook_id)
    if not w:
        raise HTTPException(status_code=404, detail="Webhook não encontrado.")
    return w


# ─────────────────────────────────────────────────────────────────────────────
# POST /
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/",
    response_model=WebhookResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cadastrar webhook",
    description="""
Cadastra uma URL para receber alertas após cada varredura que encontrar VMDKs zombie.

**Provedores suportados:**
- `generic` — payload JSON puro (padrão)
- `teams`   — Microsoft Teams Incoming Webhook (MessageCard)
- `slack`   — Slack Incoming Webhook (Block Kit)

**Autenticação:**  
Use `secret_header` + `secret_value` para enviar qualquer header customizado,
ex.: `{"secret_header": "Authorization", "secret_value": "Bearer my-token"}`.

**Filtro de ruído:**  
Configure `min_zombies_to_fire` para não receber alertas de varreduras com
poucos VMDKs (padrão: 1 — dispara sempre que encontrar ao menos 1 zombie).
    """,
)
async def create_webhook(
    body: WebhookCreate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> WebhookResponse:
    existing = await db.execute(
        select(WebhookEndpoint).where(WebhookEndpoint.name == body.name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=409,
            detail=f"Já existe um webhook com o nome '{body.name}'.",
        )

    w = WebhookEndpoint(
        name=body.name,
        url=body.url,
        provider=body.provider,
        description=body.description,
        min_zombies_to_fire=body.min_zombies_to_fire,
        secret_header=body.secret_header,
        secret_value=body.secret_value,
    )
    db.add(w)
    await db.flush()
    await db.refresh(w)
    logger.info("Webhook '%s' (id=%d) cadastrado → %s", w.name, w.id, w.url)
    return _to_response(w)


# ─────────────────────────────────────────────────────────────────────────────
# GET /
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/",
    response_model=list[WebhookResponse],
    summary="Listar webhooks",
)
async def list_webhooks(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[WebhookResponse]:
    result = await db.execute(
        select(WebhookEndpoint).order_by(WebhookEndpoint.id)
    )
    return [_to_response(w) for w in result.scalars()]


# ─────────────────────────────────────────────────────────────────────────────
# GET /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/{webhook_id}",
    response_model=WebhookResponse,
    summary="Detalhar webhook",
)
async def get_webhook(
    webhook_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> WebhookResponse:
    return _to_response(await _get_or_404(webhook_id, db))


# ─────────────────────────────────────────────────────────────────────────────
# PATCH /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.patch(
    "/{webhook_id}",
    response_model=WebhookResponse,
    summary="Atualizar webhook",
    description="Atualiza campos do webhook. Defina `is_active=false` para pausar.",
)
async def update_webhook(
    webhook_id: int,
    body: WebhookUpdate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> WebhookResponse:
    w = await _get_or_404(webhook_id, db)
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(w, field, value)
    await db.flush()
    await db.refresh(w)
    return _to_response(w)


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /{id}
# ─────────────────────────────────────────────────────────────────────────────


@router.delete(
    "/{webhook_id}",
    status_code=status.HTTP_200_OK,
    summary="Remover webhook",
)
async def delete_webhook(
    webhook_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> None:
    w = await _get_or_404(webhook_id, db)
    await db.delete(w)
    logger.info("Webhook '%s' (id=%d) removido.", w.name, webhook_id)


# ─────────────────────────────────────────────────────────────────────────────
# POST /{id}/test — disparo de teste
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/{webhook_id}/test",
    summary="Testar webhook",
    description=(
        "Envia um payload de teste para validar URL e autenticação. "
        "Retorna o HTTP status code recebido."
    ),
)
async def test_webhook(
    webhook_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> dict:
    w = await _get_or_404(webhook_id, db)

    test_payload = {
        "job_id": "00000000-test-0000-0000-000000000000",
        "vcenter": "vcenter-test.example.com",
        "total_found": 3,
        "total_size_gb": 42.5,
        "finished_at": datetime.now(timezone.utc).isoformat(),
        "top_10_largest": [
            {
                "path": "[datastore01] test-vm/test-vm.vmdk",
                "datastore": "datastore01",
                "size_gb": 20.0,
                "type": "ORPHANED",
                "vcenter": "vcenter-test.example.com",
                "datacenter": "DC-Test",
            }
        ],
        "_test": True,
    }

    formatted = _format_payload(w.provider, test_payload)
    headers = {"Content-Type": "application/json"}
    if w.secret_header and w.secret_value:
        headers[w.secret_header] = w.secret_value

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(15.0), follow_redirects=True) as client:
            resp = await client.post(w.url, json=formatted, headers=headers)
        status_code = resp.status_code
        detail = resp.text[:500] if not resp.is_success else "OK"
    except Exception as exc:
        status_code = 0
        detail = str(exc)

    # Atualiza rastreamento
    w.last_fired_at = datetime.now(timezone.utc)
    w.last_status_code = status_code
    w.fire_count = (w.fire_count or 0) + 1
    await db.flush()

    return {
        "webhook_id": webhook_id,
        "url": w.url,
        "provider": w.provider,
        "http_status": status_code,
        "success": 200 <= status_code < 300,
        "detail": detail,
    }
