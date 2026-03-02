"""
Modelos ORM para o sistema de aprovação e auditoria de operações destrutivas.

REGRA INVIOLÁVEL: Nenhuma operação destrutiva sobre VMDKs pode ocorrer sem
ApprovalToken válido emitido manualmente por analista humano, com dry-run
obrigatório e registro imutável no AuditLog.

Máquina de estados do ApprovalToken:
  pending_dryrun ──[GET /dryrun]──► dryrun_done ──[POST /execute]──► executed
       │                                 │
       └─────[DELETE /{token}]────────────┴──► cancelled
       └─────[vmdk muda / expire]─────────────► invalidated
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, Float, Integer, JSON, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .base import Base

# Ações permitidas (NEVER expand without security review)
VALID_ACTIONS = frozenset({"QUARANTINE", "DELETE"})

# Status terminais — nenhuma transição possível após atingir um destes
TERMINAL_STATUSES = frozenset({"executed", "cancelled", "invalidated"})


class ApprovalToken(Base):
    """
    Token de autorização de uso único emitido manualmente por analista humano.

    Vincula UMA ação (QUARANTINE | DELETE) a UM vmdk_path específico num vCenter
    específico. Expira em 24h. Só pode ser executado após dry-run obrigatório.

    Regras de negócio garantidas em código:
      - Apenas 1 token ativo (não-terminal) por vmdk_path por vez
      - dry-run DEVE ser chamado antes de execute
      - Se o VMDK mudar de status_zombie entre aprovação e execução → invalidado
      - READONLY_MODE=true bloqueia execute independentemente de qualquer token
    """

    __tablename__ = "approval_tokens"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # ── Credencial ────────────────────────────────────────────────────────────
    token: Mapped[str] = mapped_column(
        String(64), unique=True, nullable=False, index=True
    )
    """UUID v4 sem hífens + 8 bytes hex extra. Enviado como X-Approval-Token."""

    # ── Escopo ────────────────────────────────────────────────────────────────
    vmdk_path: Mapped[str] = mapped_column(String(1024), nullable=False, index=True)
    """Caminho completo e exato do VMDK, ex.: '[datastore01] folder/name.vmdk'."""

    vcenter_id: Mapped[str] = mapped_column(String(64), nullable=False)
    """ID (int como string) ou nome do vCenter onde o VMDK reside."""

    action: Mapped[str] = mapped_column(String(32), nullable=False)
    """'QUARANTINE' | 'DELETE' — únicos valores aceitos."""

    # ── Requerente ────────────────────────────────────────────────────────────
    analyst: Mapped[str] = mapped_column(String(128), nullable=False)
    """Identificador do analista humano que emitiu o token."""

    justification: Mapped[str] = mapped_column(Text, nullable=False)
    """Justificativa obrigatória (mínimo 20 caracteres)."""

    # ── Validade ──────────────────────────────────────────────────────────────
    issued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )
    """Tokens expirados são rejeitados (24h padrão)."""

    # ── Máquina de estados ────────────────────────────────────────────────────
    status: Mapped[str] = mapped_column(
        String(32), default="pending_dryrun", nullable=False, index=True
    )
    """
    pending_dryrun  Token criado, dry-run ainda não executado
    dryrun_done     Dry-run concluído, pronto para execute
    executed        Ação executada com sucesso (terminal)
    cancelled       Cancelado pelo analista antes do execute (terminal)
    invalidated     VMDK mudou de status ou outra invalidação (terminal)
    """

    # ── Snapshot do VMDK no momento da aprovação (detecção de mudança) ────────
    vmdk_tipo_zombie: Mapped[str | None] = mapped_column(String(32))
    """Tipo zombie do VMDK na última varredura antes da aprovação."""

    vmdk_size_gb: Mapped[float | None] = mapped_column(Float)
    """Tamanho do VMDK em GB no momento da aprovação."""

    vmdk_last_scan_job_id: Mapped[str | None] = mapped_column(String(36))
    """job_id da varredura mais recente que detectou este VMDK."""

    vmdk_datacenter: Mapped[str | None] = mapped_column(String(128))
    """Datacenter onde o VMDK foi detectado (necessário para operações vCenter)."""

    # ── Dry-run ───────────────────────────────────────────────────────────────
    dryrun_completed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    dryrun_result: Mapped[dict | None] = mapped_column(JSON)
    """Resultado completo do último dry-run (arquivos afetados, espaço, avisos)."""

    # ── Execução ──────────────────────────────────────────────────────────────
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    execution_result: Mapped[dict | None] = mapped_column(JSON)
    """Resultado da execução: arquivos removidos/movidos, tamanho liberado, erros."""

    # ── Invalidação ───────────────────────────────────────────────────────────
    invalidation_reason: Mapped[str | None] = mapped_column(Text)
    """Razão pela qual o token foi invalidado automaticamente."""

    # ── Auditoria ─────────────────────────────────────────────────────────────
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )


class AuditLog(Base):
    """
    Registro IMUTÁVEL de todas as tentativas de operação destrutiva.

    Inclui: tokens bloqueados, dry-runs, execuções e cancelamentos.
    NUNCA deve ser deletado ou alterado após criação.
    """

    __tablename__ = "audit_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)

    # Quando
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False, index=True
    )

    # Quem
    analyst: Mapped[str] = mapped_column(String(128), nullable=False)

    # O quê
    action: Mapped[str] = mapped_column(String(32), nullable=False)
    """'QUARANTINE' | 'DELETE' | 'CANCEL' | 'DRY_RUN' | 'CREATE_TOKEN'"""

    vmdk_path: Mapped[str] = mapped_column(String(1024), nullable=False)
    vcenter_id: Mapped[str | None] = mapped_column(String(64))

    # Token associado
    approval_token_id: Mapped[int | None] = mapped_column(Integer)
    approval_token_value: Mapped[str | None] = mapped_column(String(64))

    # Contexto de segurança
    dry_run: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    readonly_mode_active: Mapped[bool] = mapped_column(Boolean, nullable=False)

    # Resultado
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    """
    created            Token emitido com sucesso
    dry_run_completed  Dry-run executado
    executed_delete    VMDK deletado com sucesso
    executed_quarantine VMDK movido para quarentena com sucesso
    cancelled          Token cancelado pelo analista
    invalidated        Token invalidado automaticamente
    blocked_readonly   READONLY_MODE bloqueou a execução
    blocked_no_dryrun  execute chamado sem dry-run anterior
    blocked_expired    Token expirado
    blocked_status_changed  VMDK mudou de status desde a aprovação
    blocked_invalid_token   Token inválido/não encontrado
    blocked_terminal   Token em status terminal
    failed             Tentativa de execução falhou no vCenter
    """

    detail: Mapped[str | None] = mapped_column(Text)
    """Mensagem de erro, aviso ou descrição do resultado."""

    # Metadados da requisição HTTP
    client_ip: Mapped[str | None] = mapped_column(String(64))
    user_agent: Mapped[str | None] = mapped_column(String(512))
