"""
CRUD de vCenters + endpoint de teste de conectividade.

Fluxo de senha:
  POST/PATCH → recebe senha em texto puro → cifra com Fernet → persiste no banco
  GET        → retorna apenas metadados (senha nunca exposta na resposta)
  DELETE     → desregistra do pool e remove do banco

Ao criar ou atualizar um vCenter, o slot correspondente no VCenterConnectionPool
é registrado/atualizado imediatamente (lazy — a conexão só abre no primeiro uso).
"""

import asyncio
import concurrent.futures
import logging

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security.crypto import CryptoError, encrypt_password
from app.core.vcenter.connection import vcenter_pool
from app.core.vcenter.connection_manager import connection_manager
from app.dependencies import get_current_user, get_db
from app.models.vcenter import VCenter
from app.schemas.vcenter import VCenterCreate, VCenterResponse, VCenterUpdate

router = APIRouter()
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _get_vcenter_or_404(vcenter_id: int, db: AsyncSession) -> VCenter:
    result = await db.execute(select(VCenter).where(VCenter.id == vcenter_id))
    vc = result.scalar_one_or_none()
    if not vc:
        raise HTTPException(status_code=404, detail="vCenter não encontrado.")
    return vc


def _encrypt_or_422(plain: str) -> str:
    """Cifra a senha ou devolve HTTP 422 se a FERNET_KEY estiver inválida."""
    try:
        return encrypt_password(plain)
    except CryptoError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )


# ---------------------------------------------------------------------------
# Rotas
# ---------------------------------------------------------------------------

@router.post(
    "/",
    response_model=VCenterResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cadastrar vCenter",
    description=(
        "Registra um novo vCenter. A senha é cifrada com Fernet (AES-128-CBC + HMAC-SHA256) "
        "antes de ser persistida no banco. Nunca é retornada nas respostas da API."
    ),
)
async def create_vcenter(
    body: VCenterCreate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> VCenterResponse:
    # Verifica duplicidade de nome
    existing = await db.execute(select(VCenter).where(VCenter.name == body.name))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Nome de vCenter já cadastrado.")

    data = body.model_dump()
    data["password"] = _encrypt_or_422(data["password"])

    vc = VCenter(**data)
    db.add(vc)
    await db.flush()
    await db.refresh(vc)

    # Registra no pool (lazy — não conecta ainda)
    connection_manager.register(vc)

    return VCenterResponse.model_validate(vc)


@router.get(
    "/",
    response_model=list[VCenterResponse],
    summary="Listar vCenters",
    description="Retorna todos os vCenters cadastrados. A senha nunca é incluída na resposta.",
)
async def list_vcenters(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[VCenterResponse]:
    result = await db.execute(select(VCenter).order_by(VCenter.id))
    return [VCenterResponse.model_validate(vc) for vc in result.scalars()]


@router.get(
    "/pool-status",
    summary="Status global do pool de conexões",
    description=(
        "Retorna `{ vcenter_id: 'online'|'offline' }` para todos os vCenters ativos. "
        "Usado pelo frontend para atualizar os indicadores de conectividade em tempo real."
    ),
)
async def all_vcenter_pool_status(
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> dict:
    rows = (await db.execute(select(VCenter).where(VCenter.is_active.is_(True)))).scalars().all()
    loop = asyncio.get_event_loop()

    def _check_one(vc: VCenter) -> tuple[str, str]:
        try:
            connection_manager.register(vc)
            si = vcenter_pool.get_service_instance(vc.id)
            si.RetrieveContent().about
            return str(vc.id), "online"
        except Exception as exc:
            logger.debug(
                "Falha no pool-status para vCenter id=%s name='%s': %s",
                vc.id,
                vc.name,
                exc.__class__.__name__,
            )
            return str(vc.id), "offline"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max(len(rows), 1)) as pool:
        futures = [loop.run_in_executor(pool, _check_one, vc) for vc in rows]
        pairs = await asyncio.gather(*futures, return_exceptions=True)

    result: dict[str, str] = {}
    for item in pairs:
        if isinstance(item, tuple):
            result[item[0]] = item[1]
    return result


@router.get(
    "/{vcenter_id}",
    response_model=VCenterResponse,
    summary="Detalhar vCenter",
)
async def get_vcenter(
    vcenter_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> VCenterResponse:
    vc = await _get_vcenter_or_404(vcenter_id, db)
    return VCenterResponse.model_validate(vc)


@router.patch(
    "/{vcenter_id}",
    response_model=VCenterResponse,
    summary="Atualizar vCenter",
    description=(
        "Atualiza campos de um vCenter. Se `password` for enviada, "
        "ela é re-cifrada antes de ser salva e o slot no pool é atualizado."
    ),
)
async def update_vcenter(
    vcenter_id: int,
    body: VCenterUpdate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> VCenterResponse:
    vc = await _get_vcenter_or_404(vcenter_id, db)

    updates = body.model_dump(exclude_none=True)
    if "password" in updates:
        updates["password"] = _encrypt_or_422(updates["password"])

    for field, value in updates.items():
        setattr(vc, field, value)

    await db.flush()
    await db.refresh(vc)

    # Atualiza credenciais no pool com a nova senha decifrada
    connection_manager.register(vc)

    return VCenterResponse.model_validate(vc)


@router.delete(
    "/{vcenter_id}",
    status_code=status.HTTP_200_OK,
    summary="Remover vCenter",
)
async def delete_vcenter(
    vcenter_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> None:
    vc = await _get_vcenter_or_404(vcenter_id, db)
    connection_manager.disconnect(vcenter_id)
    await db.delete(vc)


@router.post(
    "/{vcenter_id}/test",
    summary="Testar conectividade",
    description=(
        "Abre uma sessão temporária (fora do pool) com o vCenter e retorna "
        "informações básicas como versão da API e UUID da instância."
    ),
)
async def test_vcenter_connection(
    vcenter_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> dict:
    vc = await _get_vcenter_or_404(vcenter_id, db)
    try:
        info = await connection_manager.test_connection_async(vc)
        return {"status": "ok", "vcenter": vc.name, "info": info}
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Falha na conexão com '{vc.name}': {exc}",
        )


@router.get(
    "/{vcenter_id}/pool-status",
    summary="Status do slot no pool",
    description="Retorna o estado da conexão deste vCenter no pool interno.",
)
async def vcenter_pool_status(
    vcenter_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> dict:
    await _get_vcenter_or_404(vcenter_id, db)
    all_status = connection_manager.pool_status()
    slot = all_status.get(vcenter_id)
    if slot is None:
        return {"vcenter_id": vcenter_id, "registered": False}
    return {"vcenter_id": vcenter_id, "registered": True, **slot}
