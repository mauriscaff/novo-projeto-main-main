"""
Guard genérico de acesso destrutivo via header X-Approval-Token.

REGRA ABSOLUTA: qualquer endpoint que delete, mova ou renomeie VMDKs
DEVE usar `require_write_access` como dependency FastAPI. Não existe exceção.

O fluxo completo de aprovação para VMDKs está em app/api/routes/approvals.py.
Este módulo fornece o dependency de baixo nível para outros endpoints futuros
que precisem verificar um token ativo como pré-condição.

Exportações públicas:
  require_write_access    FastAPI dependency — use em endpoints destrutivos genéricos
  record_audit            Grava entrada imutável no AuditLog
  WriteContext            Contexto retornado pelo dependency
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from fastapi import Depends, Header, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.dependencies import get_current_user, get_db
from app.models.audit_log import TERMINAL_STATUSES, ApprovalToken, AuditLog
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


@dataclass
class WriteContext:
    """Contexto retornado por `require_write_access`."""
    token: ApprovalToken
    analyst: str
    client_ip: str | None
    user_agent: str | None


async def require_write_access(
    request: Request,
    x_approval_token: str = Header(
        ...,
        alias="X-Approval-Token",
        description="Token de aprovação emitido via POST /api/v1/approvals.",
    ),
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> WriteContext:
    """
    FastAPI dependency OBRIGATÓRIO para qualquer endpoint destrutivo genérico.

    Verificações (em ordem):
      1. READONLY_MODE=false
      2. Token existe no banco
      3. Token não está em status terminal (executed/cancelled/invalidated)
      4. Token não está expirado
      5. Token está em status dryrun_done (dry-run foi feito)

    O endpoint ainda deve:
      - Verificar vmdk_path match com token.vmdk_path
      - Chamar record_audit(...) com o resultado real
    """
    analyst = user.get("sub", "unknown")
    client_ip = _get_client_ip(request)
    user_agent = request.headers.get("user-agent", "")[:512]

    # ── 1. READONLY_MODE ──────────────────────────────────────────────────────
    if settings.readonly_mode:
        await _audit_blocked(
            db=db, analyst=analyst, token_value=x_approval_token,
            status="blocked_readonly",
            detail="READONLY_MODE=true bloqueou a operação.",
            client_ip=client_ip, user_agent=user_agent,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "READONLY_MODE=true. Defina READONLY_MODE=false no .env "
                "E obtenha um ApprovalToken via POST /api/v1/approvals."
            ),
        )

    # ── 2. Token existe ───────────────────────────────────────────────────────
    result = await db.execute(
        select(ApprovalToken).where(ApprovalToken.token == x_approval_token)
    )
    token_obj = result.scalar_one_or_none()
    if not token_obj:
        await _audit_blocked(
            db=db, analyst=analyst, token_value=x_approval_token,
            status="blocked_invalid_token",
            detail="Token não encontrado.",
            client_ip=client_ip, user_agent=user_agent,
        )
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="ApprovalToken inválido ou não encontrado.",
        )

    # ── 3. Status terminal ────────────────────────────────────────────────────
    if token_obj.status in TERMINAL_STATUSES:
        await _audit_blocked(
            db=db, analyst=analyst, token_value=x_approval_token,
            status="blocked_terminal",
            detail=f"Token em status terminal: '{token_obj.status}'.",
            client_ip=client_ip, user_agent=user_agent,
        )
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=f"Token em status terminal '{token_obj.status}'. Emita um novo token.",
        )

    # ── 4. Expirado ───────────────────────────────────────────────────────────
    if token_obj.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        await _audit_blocked(
            db=db, analyst=analyst, token_value=x_approval_token,
            status="blocked_expired",
            detail=f"Token expirou em {token_obj.expires_at.isoformat()}.",
            client_ip=client_ip, user_agent=user_agent,
        )
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=f"ApprovalToken expirou em {token_obj.expires_at.isoformat()} UTC.",
        )

    # ── 5. Dry-run obrigatório ────────────────────────────────────────────────
    if token_obj.status != "dryrun_done":
        await _audit_blocked(
            db=db, analyst=analyst, token_value=x_approval_token,
            status="blocked_no_dryrun",
            detail=f"Status atual: '{token_obj.status}'. Dry-run não foi executado.",
            client_ip=client_ip, user_agent=user_agent,
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Dry-run ainda não executado (status='{token_obj.status}'). "
                f"Chame primeiro: GET /api/v1/approvals/{x_approval_token}/dryrun"
            ),
        )

    logger.info(
        "require_write_access: token id=%d validado (ação='%s', path='%s').",
        token_obj.id, token_obj.action, token_obj.vmdk_path,
    )

    return WriteContext(
        token=token_obj,
        analyst=analyst,
        client_ip=client_ip,
        user_agent=user_agent,
    )


async def record_audit(
    *,
    ctx: WriteContext,
    action: str,
    vmdk_path: str,
    dry_run: bool,
    status_value: str,
    db: AsyncSession,
    detail: str | None = None,
) -> AuditLog:
    """Grava entrada IMUTÁVEL no AuditLog e marca token como executado se necessário."""
    entry = AuditLog(
        analyst=ctx.analyst,
        action=action,
        vmdk_path=vmdk_path,
        vcenter_id=ctx.token.vcenter_id,
        approval_token_id=ctx.token.id,
        approval_token_value=ctx.token.token,
        dry_run=dry_run,
        readonly_mode_active=settings.readonly_mode,
        status=status_value,
        detail=detail,
        client_ip=ctx.client_ip,
        user_agent=ctx.user_agent,
    )
    db.add(entry)
    await db.flush()
    await db.refresh(entry)

    if status_value.startswith("executed_"):
        ctx.token.status = "executed"
        ctx.token.executed_at = datetime.now(timezone.utc)

    return entry


async def _audit_blocked(
    *,
    db: AsyncSession,
    analyst: str,
    token_value: str,
    status: str,
    detail: str,
    client_ip: str | None = None,
    user_agent: str | None = None,
) -> None:
    entry = AuditLog(
        analyst=analyst,
        action="UNKNOWN",
        vmdk_path="UNKNOWN",
        approval_token_value=token_value[:64] if token_value else None,
        dry_run=False,
        readonly_mode_active=settings.readonly_mode,
        status=status,
        detail=detail,
        client_ip=client_ip,
        user_agent=user_agent,
    )
    db.add(entry)
    try:
        await db.commit()
    except Exception as exc:
        logger.error("Falha ao gravar AuditLog bloqueado: %s", exc)


def _get_client_ip(request: Request) -> str | None:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else None
