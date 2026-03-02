"""
Schemas Pydantic para os endpoints de agendamento de varredura.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator


def _validate_cron(expression: str) -> str:
    """Valida a expressão cron usando o próprio APScheduler (fonte da verdade)."""
    try:
        from apscheduler.triggers.cron import CronTrigger

        CronTrigger.from_crontab(expression, timezone="UTC")
    except Exception as exc:
        raise ValueError(
            f"Expressão cron inválida: '{expression}'. "
            f"Use 5 campos (minuto hora dia mês dia_semana). Erro: {exc}"
        ) from exc
    return expression


class ScheduleCreate(BaseModel):
    name: str = Field(
        ...,
        min_length=1,
        max_length=128,
        examples=["daily-prod-2h"],
        description="Nome único do agendamento.",
    )
    cron_expression: str = Field(
        ...,
        examples=["0 2 * * *"],
        description=(
            "Expressão cron padrão de 5 campos: minuto hora dia mês dia_semana. "
            "Exemplos: '0 2 * * *' (todo dia às 2h UTC), "
            "'0 */4 * * *' (a cada 4h), "
            "'0 8 * * 1' (segunda-feira às 8h)."
        ),
    )
    vcenter_ids: list[int | str] = Field(
        ...,
        min_length=1,
        examples=[[1, 2]],
        description="IDs ou nomes dos vCenters a varrer.",
    )
    datacenters: list[str] | None = Field(
        default=None,
        examples=[["Datacenter-Prod"]],
        description="Datacenters a varrer; omitir para varrer todos.",
    )
    description: str | None = Field(
        default=None,
        description="Descrição livre do agendamento.",
    )

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expression(cls, v: str) -> str:
        return _validate_cron(v)


class ScheduleUpdate(BaseModel):
    """Campos opcionalmente atualizáveis via PATCH."""

    cron_expression: str | None = None
    vcenter_ids: list[int | str] | None = None
    datacenters: list[str] | None = None
    description: str | None = None
    is_active: bool | None = None

    @field_validator("cron_expression")
    @classmethod
    def validate_cron_expression(cls, v: str | None) -> str | None:
        if v is not None:
            return _validate_cron(v)
        return v


class ScheduleResponse(BaseModel):
    """Resposta completa de um agendamento, incluindo metadados de runtime."""

    id: int
    name: str
    cron_expression: str
    vcenter_ids: list[int | str]
    datacenters: list[str] | None
    is_active: bool
    description: str | None

    # Rastreamento
    last_run_at: datetime | None
    last_job_id: str | None
    next_run_at: datetime | None
    """Próxima execução calculada pelo APScheduler em runtime (None se pausado)."""
    run_count: int

    # Auditoria
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
