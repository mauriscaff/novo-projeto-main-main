"""
Dependências compartilhadas injetadas pelo FastAPI via Depends().
"""

import logging
import secrets
from typing import AsyncGenerator

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import APIKeyCookie, APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.base import AsyncSessionLocal
from config import get_settings

settings = get_settings()
logger = logging.getLogger(__name__)

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
            logger.exception("Falha na transacao do banco; rollback aplicado.")
            raise


# ---------------------------------------------------------------------------
# Autenticação: JWT Bearer Token
# ---------------------------------------------------------------------------

bearer_scheme = HTTPBearer(auto_error=False)
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
jwt_cookie = APIKeyCookie(name="zh_access_token", auto_error=False)


async def get_current_user(
    bearer: HTTPAuthorizationCredentials | None = Security(bearer_scheme),
    api_key: str | None = Security(api_key_header),
    jwt_from_cookie: str | None = Security(jwt_cookie),
) -> dict:
    """
    Aceita autenticação via:
      - Bearer JWT no header Authorization
      - API Key no header X-API-Key
    """
    # Tentativa 1: API Key estática
    if api_key and secrets.compare_digest(api_key, settings.api_key):
        return {"sub": "api-key-user", "method": "api_key"}

    # Tentativa 2: JWT Bearer
    token_value = bearer.credentials if bearer else jwt_from_cookie
    if token_value:
        try:
            payload = jwt.decode(
                token_value,
                settings.secret_key,
                algorithms=[settings.algorithm],
            )
            sub: str | None = payload.get("sub")
            if sub:
                method = "jwt" if bearer else "jwt_cookie"
                return {"sub": sub, "method": method}
        except JWTError:
            pass

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Credenciais inválidas ou ausentes.",
        headers={"WWW-Authenticate": "Bearer"},
    )
