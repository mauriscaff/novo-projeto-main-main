"""
Fluxo de aprovação obrigatório para ações destrutivas sobre VMDKs zombie.

Prefixo registrado em main.py: /api/v1/approvals

Fluxo canônico (nunca pode ser bypassado):

  POST   /                       1. Analista emite token (QUARANTINE | DELETE)
  GET    /{token}/dryrun          2. Analista simula — OBRIGATÓRIO antes do execute
  POST   /{token}/execute         3. Analista confirma — executa com todas as salvaguardas
  DELETE /{token}                 Cancela token antes do uso
  GET    /                        Lista tokens (filtros de status)
  GET    /audit-log               Histórico imutável de todas as tentativas

Regras de segurança (todas verificadas em código):
  ✓ Apenas 1 token ativo por vmdk_path por vez
  ✓ Token expira em 24h
  ✓ Uso único — após execute, token é invalidado
  ✓ dry-run DEVE ser executado antes de execute
  ✓ Se VMDK mudar de status_zombie entre aprovação e execute → token invalidado
  ✓ READONLY_MODE=true bloqueia execute incondicionalmente
"""

from __future__ import annotations

import logging
import secrets
import uuid
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import desc, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.vmdk_actions import (
    ReadOnlyModeError,
    check_vmdk_status_changed,
    dry_run_action,
    execute_action,
)
from app.dependencies import get_current_user, get_db
from app.models.audit_log import TERMINAL_STATUSES, VALID_ACTIONS, ApprovalToken, AuditLog
from app.models.zombie_scan import ZombieVmdkRecord
from sqlalchemy import desc as sa_desc
from config import get_settings

router = APIRouter()
logger = logging.getLogger(__name__)
settings = get_settings()

_TOKEN_TTL_HOURS = 24


# ─────────────────────────────────────────────────────────────────────────────
# Schemas
# ─────────────────────────────────────────────────────────────────────────────


class ApprovalRequest(BaseModel):
    vmdk_path: str = Field(
        ...,
        min_length=1,
        examples=["[datastore01] orphan-vms/old-backup.vmdk"],
        description="Caminho COMPLETO e EXATO do VMDK (formato '[datastore] pasta/arquivo.vmdk').",
    )
    vcenter_id: str = Field(
        ...,
        examples=["1"],
        description="ID (inteiro) ou nome do vCenter onde o VMDK reside.",
    )
    action: str = Field(
        ...,
        examples=["DELETE"],
        description="'QUARANTINE' (move para pasta segura) | 'DELETE' (remove permanentemente).",
    )
    justificativa: str = Field(
        ...,
        min_length=20,
        examples=["VMDK orphaned confirmado — sem VM referenciada há 90 dias, aprovado pelo líder técnico."],
        description="Justificativa obrigatória (mínimo 20 caracteres).",
    )
    analista: str = Field(
        ...,
        min_length=2,
        examples=["joao.silva"],
        description="Identificador do analista humano responsável.",
    )

    @field_validator("action")
    @classmethod
    def validate_action(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in VALID_ACTIONS:
            raise ValueError(
                f"action inválida: '{v}'. Aceitas: {sorted(VALID_ACTIONS)}"
            )
        return v_upper

    @field_validator("vmdk_path")
    @classmethod
    def validate_vmdk_path_format(cls, v: str) -> str:
        import re
        if not re.match(r"^\[.+\]\s*.+\.vmdk$", v, re.IGNORECASE):
            raise ValueError(
                "vmdk_path deve seguir o formato '[datastore] pasta/arquivo.vmdk'. "
                f"Recebido: '{v}'"
            )
        return v


class TokenResponse(BaseModel):
    approval_token: str
    id: int
    status: str
    action: str
    vmdk_path: str
    vcenter_id: str
    analista: str
    justificativa: str
    expires_at: datetime
    expires_in: str
    dry_run_required: bool
    dryrun_completed_at: datetime | None
    executed_at: datetime | None
    invalidation_reason: str | None
    created_at: datetime

    model_config = {"from_attributes": True}


class AuditLogResponse(BaseModel):
    id: int
    timestamp: datetime
    analyst: str
    action: str
    vmdk_path: str
    approval_token_id: int | None
    dry_run: bool
    readonly_mode_active: bool
    status: str
    detail: str | None
    client_ip: str | None

    model_config = {"from_attributes": True}


# ─────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ─────────────────────────────────────────────────────────────────────────────


def _to_response(t: ApprovalToken) -> TokenResponse:
    now = datetime.now(timezone.utc)
    remaining = t.expires_at.replace(tzinfo=timezone.utc) - now
    hours_left = max(0, int(remaining.total_seconds() // 3600))
    mins_left = max(0, int((remaining.total_seconds() % 3600) // 60))
    expires_in = f"{hours_left}h {mins_left}m" if remaining.total_seconds() > 0 else "expirado"

    return TokenResponse(
        approval_token=t.token,
        id=t.id,
        status=t.status,
        action=t.action,
        vmdk_path=t.vmdk_path,
        vcenter_id=t.vcenter_id,
        analista=t.analyst,
        justificativa=t.justification,
        expires_at=t.expires_at,
        expires_in=expires_in,
        dry_run_required=(t.status == "pending_dryrun"),
        dryrun_completed_at=t.dryrun_completed_at,
        executed_at=t.executed_at,
        invalidation_reason=t.invalidation_reason,
        created_at=t.created_at,
    )


async def _get_token_or_404(token_value: str, db: AsyncSession) -> ApprovalToken:
    result = await db.execute(
        select(ApprovalToken).where(ApprovalToken.token == token_value)
    )
    t = result.scalar_one_or_none()
    if not t:
        raise HTTPException(status_code=404, detail="ApprovalToken não encontrado.")
    return t


def _assert_non_terminal(t: ApprovalToken) -> None:
    if t.status in TERMINAL_STATUSES:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Token em status terminal '{t.status}' — "
                "nenhuma ação adicional é possível. "
                "Emita um novo token se necessário."
            ),
        )


def _assert_not_expired(t: ApprovalToken) -> None:
    if t.expires_at.replace(tzinfo=timezone.utc) < datetime.now(timezone.utc):
        raise HTTPException(
            status_code=410,
            detail=f"Token expirou em {t.expires_at.isoformat()} UTC. Emita um novo.",
        )


def _get_client_ip(request: Request) -> str | None:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else None


async def _audit(
    db: AsyncSession,
    *,
    analyst: str,
    action: str,
    vmdk_path: str,
    vcenter_id: str | None,
    token: ApprovalToken | None,
    dry_run: bool,
    status_value: str,
    detail: str | None = None,
    client_ip: str | None = None,
    user_agent: str | None = None,
) -> None:
    entry = AuditLog(
        analyst=analyst,
        action=action,
        vmdk_path=vmdk_path,
        vcenter_id=vcenter_id,
        approval_token_id=token.id if token else None,
        approval_token_value=token.token if token else None,
        dry_run=dry_run,
        readonly_mode_active=settings.readonly_mode,
        status=status_value,
        detail=detail,
        client_ip=client_ip,
        user_agent=(user_agent or "")[:512] if user_agent else None,
    )
    db.add(entry)
    # commit é responsabilidade do caller (está dentro de uma transação maior)


# ─────────────────────────────────────────────────────────────────────────────
# GET /audit-log  (DEVE vir antes de /{token} para não conflitar)
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/audit-log",
    response_model=list[AuditLogResponse],
    summary="Histórico de auditoria",
    description=(
        "Registro imutável de todas as tentativas de operação destrutiva — "
        "incluindo bloqueios, dry-runs e execuções."
    ),
)
async def get_audit_log(
    limit: int = Query(default=100, ge=1, le=1000),
    analyst: str | None = Query(default=None),
    action: str | None = Query(default=None),
    status_filter: str | None = Query(default=None, alias="status"),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[AuditLogResponse]:
    stmt = select(AuditLog).order_by(desc(AuditLog.timestamp)).limit(limit)
    if analyst:
        stmt = stmt.where(AuditLog.analyst == analyst)
    if action:
        stmt = stmt.where(AuditLog.action == action.upper())
    if status_filter:
        stmt = stmt.where(AuditLog.status == status_filter)
    result = await db.execute(stmt)
    return [AuditLogResponse.model_validate(e) for e in result.scalars()]


# ─────────────────────────────────────────────────────────────────────────────
# POST /  — 1. Emitir token de aprovação
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/",
    response_model=TokenResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Emitir ApprovalToken",
    description="""
Inicia o fluxo de aprovação para uma ação sobre um VMDK zombie.

**Regras de negócio:**
- Apenas `QUARANTINE` e `DELETE` são ações aceitas
- Só pode haver **1 token ativo** (não-terminal) por `vmdk_path` por vez
- O token expira automaticamente em **24 horas**
- O **dry-run é obrigatório** antes do execute
- `justificativa` requer mínimo 20 caracteres

**Próximo passo:** `GET /api/v1/approvals/{token}/dryrun`
    """,
)
async def create_approval(
    body: ApprovalRequest,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> TokenResponse:
    analyst = user.get("sub", body.analista)
    client_ip = _get_client_ip(request)

    # ── Verifica token ativo existente para este vmdk_path ────────────────────
    existing_q = await db.execute(
        select(ApprovalToken).where(
            ApprovalToken.vmdk_path == body.vmdk_path,
            ApprovalToken.status.notin_(TERMINAL_STATUSES),
            ApprovalToken.expires_at > datetime.now(timezone.utc),
        )
    )
    existing = existing_q.scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=(
                f"Já existe um token ativo (id={existing.id}, status='{existing.status}') "
                f"para o vmdk_path '{body.vmdk_path}'. "
                "Cancele o token existente antes de criar um novo."
            ),
        )

    # ── Busca o estado atual do VMDK no banco ─────────────────────────────────
    latest_q = await db.execute(
        select(ZombieVmdkRecord)
        .where(ZombieVmdkRecord.path == body.vmdk_path)
        .order_by(sa_desc(ZombieVmdkRecord.created_at))
        .limit(1)
    )
    latest_record = latest_q.scalar_one_or_none()

    # ── Gera token ────────────────────────────────────────────────────────────
    now = datetime.now(timezone.utc)
    token_value = uuid.uuid4().hex + secrets.token_hex(8)

    token = ApprovalToken(
        token=token_value,
        vmdk_path=body.vmdk_path,
        vcenter_id=body.vcenter_id,
        action=body.action,
        analyst=body.analista,
        justification=body.justificativa,
        issued_at=now,
        expires_at=now + timedelta(hours=_TOKEN_TTL_HOURS),
        status="pending_dryrun",
        # Snapshot do estado atual
        vmdk_tipo_zombie=latest_record.tipo_zombie if latest_record else None,
        vmdk_size_gb=latest_record.tamanho_gb if latest_record else None,
        vmdk_last_scan_job_id=latest_record.job_id if latest_record else None,
        vmdk_datacenter=latest_record.datacenter if latest_record else None,
    )
    db.add(token)
    await db.flush()
    await db.refresh(token)

    await _audit(
        db,
        analyst=analyst,
        action="CREATE_TOKEN",
        vmdk_path=body.vmdk_path,
        vcenter_id=body.vcenter_id,
        token=token,
        dry_run=False,
        status_value="created",
        detail=f"Ação: {body.action}. Analista: {body.analista}.",
        client_ip=client_ip,
        user_agent=request.headers.get("user-agent"),
    )

    logger.info(
        "ApprovalToken id=%d criado | ação=%s | path='%s' | analista='%s' | expira em %dh",
        token.id, body.action, body.vmdk_path, body.analista, _TOKEN_TTL_HOURS,
    )
    return _to_response(token)


# ─────────────────────────────────────────────────────────────────────────────
# GET /{token}/dryrun  — 2. Simulação obrigatória
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/{token_value}/dryrun",
    summary="Executar dry-run (obrigatório antes do execute)",
    description="""
Simula a ação **sem executar nada** no vCenter.

O analista DEVE chamar este endpoint e validar o resultado **antes** de chamar
`POST /{token}/execute`. O execute é bloqueado se o dry-run não foi feito.

**Retorna:**
- Arquivos que seriam afetados
- Espaço que seria liberado (GB)
- Estado atual do VMDK (verifica mudança de status)
- Verificação live no vCenter (se disponível)
- Avisos de segurança
- `is_safe_to_proceed`: indicador de que é seguro prosseguir

**Próximo passo:** `POST /api/v1/approvals/{token}/execute`
    """,
)
async def dryrun(
    token_value: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict:
    analyst = user.get("sub", "unknown")
    client_ip = _get_client_ip(request)
    token = await _get_token_or_404(token_value, db)

    _assert_non_terminal(token)
    _assert_not_expired(token)

    # ── Executa simulação ─────────────────────────────────────────────────────
    result = await dry_run_action(token, db)

    # ── Atualiza token para dryrun_done ───────────────────────────────────────
    token.status = "dryrun_done"
    token.dryrun_completed_at = datetime.now(timezone.utc)
    token.dryrun_result = result.as_dict()

    await _audit(
        db,
        analyst=analyst,
        action="DRY_RUN",
        vmdk_path=token.vmdk_path,
        vcenter_id=token.vcenter_id,
        token=token,
        dry_run=True,
        status_value="dry_run_completed",
        detail=(
            f"Arquivos afetados: {len(result.files_affected)}. "
            f"Espaço: {result.space_to_recover_gb:.2f} GB. "
            f"Safe: {result.is_safe_to_proceed}."
        ),
        client_ip=client_ip,
        user_agent=request.headers.get("user-agent"),
    )

    logger.info(
        "Dry-run executado | token=%s | ação=%s | path='%s' | safe=%s",
        token_value[:8], token.action, token.vmdk_path, result.is_safe_to_proceed,
    )

    return {
        "token": token_value,
        "status_after_dryrun": "dryrun_done",
        "next_step": f"POST /api/v1/approvals/{token_value}/execute",
        **result.as_dict(),
    }


# ─────────────────────────────────────────────────────────────────────────────
# POST /{token}/execute  — 3. Execução com todas as salvaguardas
# ─────────────────────────────────────────────────────────────────────────────


@router.post(
    "/{token_value}/execute",
    summary="Executar ação aprovada",
    description="""
Executa a ação destrutiva aprovada (QUARANTINE ou DELETE) no vCenter.

**Salvaguardas verificadas sequencialmente:**
1. `READONLY_MODE=false` no `.env` — **bloqueio incondicional se True**
2. Token não expirado
3. Token em status `dryrun_done` (dry-run obrigatório antes)
4. VMDK não mudou de status zombie desde a emissão do token
5. Token marcado como `executed` (uso único) **antes** da operação no vCenter

**Registra no AuditLog imutável independentemente do resultado.**

Se READONLY_MODE=true, retorna HTTP 403 e registra `blocked_readonly` no AuditLog.
    """,
)
async def execute(
    token_value: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict:
    analyst = user.get("sub", "unknown")
    client_ip = _get_client_ip(request)
    token = await _get_token_or_404(token_value, db)

    # ── Salvaguarda 1: READONLY_MODE ──────────────────────────────────────────
    if settings.readonly_mode:
        await _audit(
            db,
            analyst=analyst,
            action=token.action,
            vmdk_path=token.vmdk_path,
            vcenter_id=token.vcenter_id,
            token=token,
            dry_run=False,
            status_value="blocked_readonly",
            detail="READONLY_MODE=true bloqueou a execução.",
            client_ip=client_ip,
            user_agent=request.headers.get("user-agent"),
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=(
                "READONLY_MODE=true — a API está em modo somente-leitura. "
                "Defina READONLY_MODE=false no .env para habilitar operações destrutivas."
            ),
        )

    # ── Salvaguarda 2: Token terminal ─────────────────────────────────────────
    _assert_non_terminal(token)

    # ── Salvaguarda 3: Token expirado ─────────────────────────────────────────
    _assert_not_expired(token)

    # ── Salvaguarda 4: Dry-run obrigatório ────────────────────────────────────
    if token.status != "dryrun_done":
        await _audit(
            db,
            analyst=analyst,
            action=token.action,
            vmdk_path=token.vmdk_path,
            vcenter_id=token.vcenter_id,
            token=token,
            dry_run=False,
            status_value="blocked_no_dryrun",
            detail="execute chamado sem dry-run anterior.",
            client_ip=client_ip,
            user_agent=request.headers.get("user-agent"),
        )
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"O dry-run ainda não foi executado para este token (status='{token.status}'). "
                f"Chame primeiro: GET /api/v1/approvals/{token_value}/dryrun"
            ),
        )

    # ── Salvaguarda 5: Mudança de status do VMDK ──────────────────────────────
    status_changed, change_reason = await check_vmdk_status_changed(token, db)
    if status_changed:
        token.status = "invalidated"
        token.invalidation_reason = change_reason
        await _audit(
            db,
            analyst=analyst,
            action=token.action,
            vmdk_path=token.vmdk_path,
            vcenter_id=token.vcenter_id,
            token=token,
            dry_run=False,
            status_value="blocked_status_changed",
            detail=change_reason,
            client_ip=client_ip,
            user_agent=request.headers.get("user-agent"),
        )
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Token invalidado — o status zombie do VMDK mudou desde a aprovação. "
                f"Razão: {change_reason}. Emita um novo token para prosseguir."
            ),
        )

    # ── Marca como executado ANTES de chamar o vCenter (uso único garantido) ──
    token.status = "executed"
    token.executed_at = datetime.now(timezone.utc)
    await db.flush()  # persiste antes de chamar o vCenter

    # ── Executa a ação ────────────────────────────────────────────────────────
    exec_result = await execute_action(token, db)
    token.execution_result = exec_result.as_dict()

    audit_status = (
        f"executed_{token.action.lower()}" if exec_result.success else "failed"
    )
    if not exec_result.success:
        # Rollback do status — permite nova tentativa após corrigir o problema
        token.status = "dryrun_done"
        token.executed_at = None

    await _audit(
        db,
        analyst=analyst,
        action=token.action,
        vmdk_path=token.vmdk_path,
        vcenter_id=token.vcenter_id,
        token=token,
        dry_run=False,
        status_value=audit_status,
        detail=(
            exec_result.error
            if not exec_result.success
            else (
                f"Arquivos: {exec_result.files_processed}. "
                f"Espaço liberado: {exec_result.space_recovered_gb:.2f} GB."
            )
        ),
        client_ip=client_ip,
        user_agent=request.headers.get("user-agent"),
    )

    if not exec_result.success:
        logger.error(
            "Falha na execução %s | token=%s | path='%s' | erro: %s",
            token.action, token_value[:8], token.vmdk_path, exec_result.error,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=(
                f"Ação {token.action} falhou no vCenter: {exec_result.error}. "
                "O token foi mantido em 'dryrun_done' para nova tentativa após corrigir o problema."
            ),
        )

    logger.warning(
        "AÇÃO EXECUTADA: %s | path='%s' | analista='%s' | token=%s",
        token.action, token.vmdk_path, analyst, token_value[:8],
    )

    return {
        "status": "executed",
        "token": token_value,
        **exec_result.as_dict(),
        "audit_registered": True,
    }


# ─────────────────────────────────────────────────────────────────────────────
# DELETE /{token}  — Cancelar token antes do uso
# ─────────────────────────────────────────────────────────────────────────────


@router.delete(
    "/{token_value}",
    status_code=status.HTTP_200_OK,
    summary="Cancelar ApprovalToken",
    description="Cancela o token antes da execução. Tokens já executados não podem ser cancelados.",
)
async def cancel_token(
    token_value: str,
    request: Request,
    db: AsyncSession = Depends(get_db),
    user: dict = Depends(get_current_user),
) -> dict:
    analyst = user.get("sub", "unknown")
    token = await _get_token_or_404(token_value, db)

    _assert_non_terminal(token)

    token.status = "cancelled"
    token.invalidation_reason = f"Cancelado pelo analista '{analyst}'."

    await _audit(
        db,
        analyst=analyst,
        action="CANCEL",
        vmdk_path=token.vmdk_path,
        vcenter_id=token.vcenter_id,
        token=token,
        dry_run=False,
        status_value="cancelled",
        detail=token.invalidation_reason,
        client_ip=_get_client_ip(request),
        user_agent=request.headers.get("user-agent"),
    )

    logger.info(
        "ApprovalToken id=%d cancelado por '%s'.", token.id, analyst
    )
    return {
        "cancelled": True,
        "token": token_value,
        "cancelled_by": analyst,
        "vmdk_path": token.vmdk_path,
    }


# ─────────────────────────────────────────────────────────────────────────────
# GET /  — Listar tokens
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/",
    response_model=list[TokenResponse],
    summary="Listar ApprovalTokens",
)
async def list_tokens(
    status_filter: str | None = Query(default=None, alias="status"),
    analyst: str | None = Query(default=None),
    action: str | None = Query(default=None),
    only_active: bool = Query(
        default=False,
        description="Retorna apenas tokens não-terminais e não-expirados.",
    ),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[TokenResponse]:
    stmt = select(ApprovalToken).order_by(sa_desc(ApprovalToken.issued_at))

    if status_filter:
        stmt = stmt.where(ApprovalToken.status == status_filter)
    if analyst:
        stmt = stmt.where(ApprovalToken.analyst == analyst)
    if action:
        stmt = stmt.where(ApprovalToken.action == action.upper())
    if only_active:
        now = datetime.now(timezone.utc)
        stmt = stmt.where(
            ApprovalToken.status.notin_(TERMINAL_STATUSES),
            ApprovalToken.expires_at > now,
        )

    result = await db.execute(stmt)
    return [_to_response(t) for t in result.scalars()]


# ─────────────────────────────────────────────────────────────────────────────
# GET /{token}  — Detalhar token
# ─────────────────────────────────────────────────────────────────────────────


@router.get(
    "/{token_value}",
    response_model=TokenResponse,
    summary="Detalhar ApprovalToken",
)
async def get_token(
    token_value: str,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> TokenResponse:
    return _to_response(await _get_token_or_404(token_value, db))
