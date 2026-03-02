"""
Dependências compartilhadas injetadas pelo FastAPI via Depends().
"""

from typing import AsyncGenerator

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer, APIKeyHeader
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.base import AsyncSessionLocal
from config import get_settings

settings = get_settings()

# ---------------------------------------------------------------------------
# Sessão de banco de dados
# ---------------------------------------------------------------------------

async def get_db() -> AsyncGenerator[AsyncSession, None]:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# ---------------------------------------------------------------------------
# Autenticação: JWT Bearer Token
# ---------------------------------------------------------------------------

bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def get_current_user(
    bearer: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
    api_key: str | None = Security(api_key_header),
) -> dict:
    """
    Aceita autenticação via:
      - Bearer JWT no header Authorization
      - API Key no header X-API-Key
    """
    # Tentativa 1: API Key estática
    if api_key and api_key == settings.api_key:
        return {"sub": "api-key-user", "method": "api_key"}

    # Tentativa 2: JWT Bearer
    if bearer:
        try:
            payload = jwt.decode(
                bearer.credentials,
                settings.secret_key,
                algorithms=[settings.algorithm],
            )
            sub: str | None = payload.get("sub")
            if sub:
                return {"sub": sub, "method": "jwt"}
        except JWTError:
            pass

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciais inválidas ou ausentes.",
        headers={"WWW-Authenticate": "Bearer"},
    )
