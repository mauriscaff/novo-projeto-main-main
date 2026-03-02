"""
Rota de autenticação: geração de JWT a partir de usuário/senha estáticos.
Em produção, substitua pela autenticação contra um banco de usuários real.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, HTTPException, status
from jose import jwt

from app.schemas.auth import TokenRequest, TokenResponse
from config import get_settings

router = APIRouter()
settings = get_settings()

# Usuário administrador embutido (apenas para demo).
# Em produção: persista usuários no banco com hash bcrypt.
_DEMO_USER = "admin"
_DEMO_PASSWORD = "admin"  # noqa: S105


def _create_access_token(subject: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(
        minutes=settings.access_token_expire_minutes
    )
    payload = {"sub": subject, "exp": expire}
    return jwt.encode(payload, settings.secret_key, algorithm=settings.algorithm)


@router.post(
    "/token",
    response_model=TokenResponse,
    summary="Gerar JWT",
    description=(
        "Autentica com usuário/senha e retorna um Bearer Token JWT. "
        "Inclua o token no header `Authorization: Bearer <token>` nas demais rotas."
    ),
)
async def login(body: TokenRequest) -> TokenResponse:
    if body.username != _DEMO_USER or body.password != _DEMO_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuário ou senha incorretos.",
        )
    token = _create_access_token(body.username)
    return TokenResponse(access_token=token)
