"""
Dispatcher de webhooks pós-varredura.

Chamado automaticamente ao final de run_zombie_scan() quando zombies são encontrados.
Dispara notificações assíncronas para todos os WebhookEndpoints ativos cujo
limiar `min_zombies_to_fire` seja satisfeito.

Provedores suportados
─────────────────────
  generic   Payload JSON puro (spec do usuário)
  teams     Microsoft Teams Incoming Webhook (MessageCard)
  slack     Slack Incoming Webhook (Block Kit)

Payload genérico enviado / incluído em todos os formatos:
  {
    "job_id": "...",
    "vcenter": "vc1, vc2",
    "total_found": 42,
    "total_size_gb": 1024.5,
    "finished_at": "2026-02-25T02:10:00Z",
    "top_10_largest": [
      {"path": "...", "datastore": "...", "size_gb": 256.0, "type": "ORPHANED", "vcenter": "vc1"}
    ]
  }
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

import httpx
from sqlalchemy import select

from app.models.base import AsyncSessionLocal
from app.models.webhook import WebhookEndpoint
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord

logger = logging.getLogger(__name__)

_TIMEOUT = httpx.Timeout(15.0, connect=5.0)


# ─────────────────────────────────────────────────────────────────────────────
# Ponto de entrada público
# ─────────────────────────────────────────────────────────────────────────────


async def dispatch_scan_complete(job_id: str) -> None:
    """
    Verifica se há zombies no job e dispara todos os webhooks ativos elegíveis.
    Falhas individuais de webhook são logadas mas não propagadas.
    """
    # ── Carrega job e verifica se há zombies ──────────────────────────────────
    async with AsyncSessionLocal() as db:
        job = await db.get(ZombieScanJob, job_id)
        if not job or not job.total_vmdks:
            return

        # Webhooks ativos
        wh_result = await db.execute(
            select(WebhookEndpoint).where(WebhookEndpoint.is_active.is_(True))
        )
        webhooks = wh_result.scalars().all()
        if not webhooks:
            return

        # Top 10 maiores VMDKs do job
        top_result = await db.execute(
            select(ZombieVmdkRecord)
            .where(ZombieVmdkRecord.job_id == job_id)
            .order_by(ZombieVmdkRecord.tamanho_gb.desc().nulls_last())
            .limit(10)
        )
        top_10 = top_result.scalars().all()

        # Nomes dos vCenters presentes no job
        vc_result = await db.execute(
            select(ZombieVmdkRecord.vcenter_name)
            .where(ZombieVmdkRecord.job_id == job_id)
            .distinct()
        )
        vcenter_names = [v for v in vc_result.scalars() if v]

    # ── Monta payload base ────────────────────────────────────────────────────
    base_payload: dict = {
        "job_id": job_id,
        "vcenter": ", ".join(vcenter_names) or "N/A",
        "total_found": job.total_vmdks,
        "total_size_gb": round(job.total_size_gb or 0.0, 3),
        "finished_at": (
            job.finished_at.isoformat() if job.finished_at else None
        ),
        "top_10_largest": [
            {
                "path": r.path,
                "datastore": r.datastore,
                "size_gb": round(r.tamanho_gb or 0.0, 3),
                "type": r.tipo_zombie,
                "vcenter": r.vcenter_name or "",
                "datacenter": r.datacenter,
            }
            for r in top_10
        ],
    }

    # ── Dispara cada webhook ──────────────────────────────────────────────────
    async with httpx.AsyncClient(timeout=_TIMEOUT, follow_redirects=True) as client:
        for webhook in webhooks:
            if job.total_vmdks < webhook.min_zombies_to_fire:
                logger.debug(
                    "Webhook '%s': ignorado (total=%d < min=%d).",
                    webhook.name,
                    job.total_vmdks,
                    webhook.min_zombies_to_fire,
                )
                continue
            await _fire_one(client, webhook, base_payload, job_id)


# ─────────────────────────────────────────────────────────────────────────────
# Disparo individual
# ─────────────────────────────────────────────────────────────────────────────


async def _fire_one(
    client: httpx.AsyncClient,
    webhook: WebhookEndpoint,
    base_payload: dict,
    job_id: str,
) -> None:
    """Envia o webhook e atualiza os campos de rastreamento no banco."""
    status_code = 0
    try:
        headers = {"Content-Type": "application/json"}
        if webhook.secret_header and webhook.secret_value:
            headers[webhook.secret_header] = webhook.secret_value

        formatted = _format_payload(webhook.provider, base_payload)
        resp = await client.post(webhook.url, json=formatted, headers=headers)
        status_code = resp.status_code

        if resp.is_success:
            logger.info(
                "[job:%s] Webhook '%s' disparado com sucesso → HTTP %d.",
                job_id, webhook.name, status_code,
            )
        else:
            logger.warning(
                "[job:%s] Webhook '%s' retornou HTTP %d: %s",
                job_id, webhook.name, status_code, resp.text[:200],
            )

    except httpx.TimeoutException:
        logger.error(
            "[job:%s] Webhook '%s': timeout após %.1fs.",
            job_id, webhook.name, _TIMEOUT.read,
        )
    except Exception as exc:
        logger.error(
            "[job:%s] Webhook '%s': erro inesperado — %s",
            job_id, webhook.name, exc,
        )

    # Atualiza rastreamento (abre nova sessão — a anterior foi fechada acima)
    try:
        async with AsyncSessionLocal() as db:
            wh = await db.get(WebhookEndpoint, webhook.id)
            if wh:
                wh.last_fired_at = datetime.now(timezone.utc)
                wh.last_status_code = status_code
                wh.fire_count = (wh.fire_count or 0) + 1
                await db.commit()
    except Exception as exc:
        logger.error(
            "[job:%s] Falha ao atualizar rastreamento do webhook '%s': %s",
            job_id, webhook.name, exc,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Formatação por provedor
# ─────────────────────────────────────────────────────────────────────────────


def _format_payload(provider: str, payload: dict) -> dict:
    """Formata o payload conforme o provedor do webhook."""
    if provider == "teams":
        return _to_teams_card(payload)
    if provider == "slack":
        return _to_slack_message(payload)
    return payload  # generic: envia o payload bruto


def _to_teams_card(p: dict) -> dict:
    """Microsoft Teams Incoming Webhook — MessageCard format."""
    top_lines = "\n".join(
        f"• `{item['path']}` — {item['size_gb']} GB ({item['type']})"
        for item in p.get("top_10_largest", [])
    ) or "Nenhum."

    return {
        "@type": "MessageCard",
        "@context": "https://schema.org/extensions",
        "themeColor": "D44000",
        "summary": f"VMDK Zombie Scanner — {p['total_found']} zombie(s) encontrado(s)",
        "sections": [
            {
                "activityTitle": "🚨 VMDKs Zombie Encontrados",
                "activitySubtitle": f"Job ID: `{p['job_id']}` | {p.get('finished_at', '')}",
                "facts": [
                    {"name": "vCenter(s)", "value": p["vcenter"]},
                    {"name": "VMDKs encontrados", "value": str(p["total_found"])},
                    {"name": "Espaço recuperável", "value": f"{p['total_size_gb']} GB"},
                ],
            },
            {
                "title": "Top 10 maiores VMDKs zombie",
                "text": top_lines,
            },
        ],
        "potentialAction": [],
    }


def _to_slack_message(p: dict) -> dict:
    """Slack Incoming Webhook — Block Kit format."""
    top_text = "\n".join(
        f"• `{item['path']}` — {item['size_gb']} GB ({item['type']})"
        for item in p.get("top_10_largest", [])
    ) or "Nenhum."

    return {
        "text": f"🚨 *VMDK Zombie Scanner — {p['total_found']} zombie(s) encontrado(s)*",
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 {p['total_found']} VMDKs Zombie Encontrados",
                },
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*vCenter(s):*\n{p['vcenter']}"},
                    {"type": "mrkdwn", "text": f"*VMDKs:*\n{p['total_found']}"},
                    {"type": "mrkdwn", "text": f"*Espaço recuperável:*\n{p['total_size_gb']} GB"},
                    {"type": "mrkdwn", "text": f"*Job ID:*\n`{p['job_id']}`"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Top 10 maiores:*\n{top_text}"},
            },
            {"type": "divider"},
        ],
    }
