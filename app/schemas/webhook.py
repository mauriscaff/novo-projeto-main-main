"""Schemas Pydantic para os endpoints de webhook."""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, HttpUrl, field_validator

_VALID_PROVIDERS = {"teams", "slack", "generic"}


class WebhookCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=128, examples=["teams-infra"])
    url: str = Field(
        ...,
        examples=["https://outlook.office.com/webhook/..."],
        description="URL do endpoint receptor (Teams, Slack, HTTP genérico).",
    )
    provider: str = Field(
        default="generic",
        examples=["teams"],
        description="'teams' | 'slack' | 'generic' — define o formato do payload.",
    )
    description: str | None = None
    min_zombies_to_fire: int = Field(
        default=1,
        ge=1,
        description="Só dispara se total de VMDKs encontrados >= este valor.",
    )
    secret_header: str | None = Field(
        default=None,
        examples=["Authorization"],
        description="Nome do header de autenticação, ex.: 'Authorization'.",
    )
    secret_value: str | None = Field(
        default=None,
        examples=["Bearer my-token"],
        description="Valor do header de autenticação.",
    )

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str) -> str:
        v = v.lower()
        if v not in _VALID_PROVIDERS:
            raise ValueError(
                f"provider inválido: '{v}'. Aceitos: {sorted(_VALID_PROVIDERS)}"
            )
        return v

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL deve começar com 'http://' ou 'https://'.")
        return v


class WebhookUpdate(BaseModel):
    url: str | None = None
    provider: str | None = None
    description: str | None = None
    is_active: bool | None = None
    min_zombies_to_fire: int | None = Field(default=None, ge=1)
    secret_header: str | None = None
    secret_value: str | None = None

    @field_validator("provider")
    @classmethod
    def validate_provider(cls, v: str | None) -> str | None:
        if v is not None:
            v = v.lower()
            if v not in _VALID_PROVIDERS:
                raise ValueError(
                    f"provider inválido: '{v}'. Aceitos: {sorted(_VALID_PROVIDERS)}"
                )
        return v


class WebhookResponse(BaseModel):
    id: int
    name: str
    url: str
    provider: str
    description: str | None
    is_active: bool
    min_zombies_to_fire: int
    secret_header: str | None
    secret_value_masked: str | None
    """Valor do secret mascarado como '***' para segurança."""
    last_fired_at: datetime | None
    last_status_code: int | None
    fire_count: int
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class MarkSafeRequest(BaseModel):
    justification: str = Field(
        ...,
        min_length=10,
        examples=["VMDK de snapshot manual do time de banco — mantido intencionalmente."],
        description="Justificativa obrigatória (mínimo 10 caracteres).",
    )
    marked_by: str | None = Field(
        default=None,
        description="Identificador do responsável pela marcação (padrão: usuário autenticado).",
    )


class WhitelistEntryResponse(BaseModel):
    id: int
    path: str
    justification: str
    marked_by: str
    job_id: str
    record_id: int | None
    created_at: datetime

    model_config = {"from_attributes": True}
