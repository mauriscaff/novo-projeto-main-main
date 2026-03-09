"""
Rotas de autenticacao: emissao de JWT por usuario/senha para API e sessao web.
Em producao, substitua credenciais demo por autenticacao real.
"""

from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse
from jose import jwt

from app.dependencies import get_current_user
from app.schemas.auth import TokenRequest, TokenResponse
from config import get_settings

router = APIRouter()
settings = get_settings()
_AUTH_COOKIE_NAME = "zh_access_token"

# Usuario administrador embutido (apenas para demo).
# Em producao: persista usuarios no banco com hash bcrypt.
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
        "Autentica com usuario/senha e retorna um Bearer Token JWT. "
        "Inclua o token no header `Authorization: Bearer <token>` nas demais rotas."
    ),
)
async def login(body: TokenRequest) -> TokenResponse:
    if body.username != _DEMO_USER or body.password != _DEMO_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario ou senha incorretos.",
        )
    token = _create_access_token(body.username)
    return TokenResponse(access_token=token)


@router.post(
    "/session/login",
    summary="Login de sessao web",
    description=(
        "Autentica usuario/senha e grava JWT em cookie HttpOnly (`zh_access_token`) "
        "para uso do frontend Jinja2 sem expor token em JavaScript."
    ),
)
async def session_login(body: TokenRequest, request: Request) -> Response:
    if body.username != _DEMO_USER or body.password != _DEMO_PASSWORD:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Usuario ou senha incorretos.",
        )

    token = _create_access_token(body.username)
    response = JSONResponse(
        status_code=status.HTTP_200_OK,
        content={"status": "ok", "sub": body.username},
    )
    response.set_cookie(
        key=_AUTH_COOKIE_NAME,
        value=token,
        max_age=settings.access_token_expire_minutes * 60,
        httponly=True,
        secure=(request.url.scheme == "https"),
        samesite="strict",
        path="/",
    )
    return response


@router.post(
    "/session/logout",
    summary="Logout de sessao web",
)
async def session_logout() -> Response:
    response = JSONResponse(status_code=status.HTTP_200_OK, content={"status": "ok"})
    response.delete_cookie(key=_AUTH_COOKIE_NAME, path="/")
    return response


@router.get(
    "/session/me",
    summary="Usuario autenticado na sessao web",
)
async def session_me(user: dict = Depends(get_current_user)) -> dict:
    return {"sub": user.get("sub"), "method": user.get("method")}
