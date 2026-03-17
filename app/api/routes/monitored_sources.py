from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.security.crypto import CryptoError, decrypt_password, encrypt_password
from app.core.sources import test_connectivity_stub
from app.dependencies import get_current_user, get_db
from app.models.monitored_source import MonitoredSource, MonitoredSourceSecret
from app.schemas.monitored_source import (
    CollectionMarkRequest,
    CollectionStatusItem,
    CollectionStatusSummary,
    ConnectivityTestResponse,
    MonitoredSourceCreate,
    MonitoredSourceResponse,
    MonitoredSourceStatus,
    MonitoredSourceType,
    MonitoredSourceUpdate,
)

router = APIRouter()


def _encrypt_secret_or_422(plain_secret: str) -> str:
    try:
        return encrypt_password(plain_secret)
    except CryptoError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        )


async def _get_source_or_404(
    source_id: int,
    db: AsyncSession,
    *,
    include_deleted: bool = False,
) -> MonitoredSource:
    stmt = select(MonitoredSource).options(selectinload(MonitoredSource.secret)).where(MonitoredSource.id == source_id)
    if not include_deleted:
        stmt = stmt.where(MonitoredSource.is_deleted.is_(False))

    result = await db.execute(stmt)
    source = result.scalar_one_or_none()
    if not source:
        raise HTTPException(status_code=404, detail="Fonte monitorada nao encontrada.")
    return source


async def _assert_no_active_duplicate(
    db: AsyncSession,
    *,
    source_type: str,
    name: str,
    endpoint: str,
    exclude_id: int | None = None,
) -> None:
    stmt = select(MonitoredSource).where(
        MonitoredSource.is_deleted.is_(False),
        MonitoredSource.source_type == source_type,
        or_(
            func.lower(MonitoredSource.name) == name.lower(),
            func.lower(MonitoredSource.endpoint) == endpoint.lower(),
        ),
    )
    if exclude_id is not None:
        stmt = stmt.where(MonitoredSource.id != exclude_id)

    existing = (await db.execute(stmt)).scalar_one_or_none()
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ja existe fonte ativa com o mesmo nome ou endpoint para esse tipo.",
        )


@router.post(
    "/",
    response_model=MonitoredSourceResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Cadastrar fonte monitorada",
)
async def create_source(
    body: MonitoredSourceCreate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> MonitoredSourceResponse:
    source_type = body.source_type.value
    name = body.name.strip()
    endpoint = body.endpoint.strip()
    username = body.username.strip()

    await _assert_no_active_duplicate(
        db,
        source_type=source_type,
        name=name,
        endpoint=endpoint,
    )

    source = MonitoredSource(
        source_type=source_type,
        name=name,
        endpoint=endpoint,
        username=username,
        status=(MonitoredSourceStatus.UNKNOWN.value if body.is_active else MonitoredSourceStatus.DISABLED.value),
        is_active=body.is_active,
        is_deleted=False,
    )
    source.secret = MonitoredSourceSecret(secret_encrypted=_encrypt_secret_or_422(body.secret))

    db.add(source)
    await db.flush()
    await db.refresh(source)
    return MonitoredSourceResponse.model_validate(source)


@router.get(
    "/",
    response_model=list[MonitoredSourceResponse],
    summary="Listar fontes monitoradas",
)
async def list_sources(
    source_type: MonitoredSourceType | None = Query(default=None),
    include_deleted: bool = Query(default=False),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> list[MonitoredSourceResponse]:
    stmt = select(MonitoredSource).order_by(MonitoredSource.source_type, MonitoredSource.name)

    filters = []
    if source_type is not None:
        filters.append(MonitoredSource.source_type == source_type.value)
    if not include_deleted:
        filters.append(MonitoredSource.is_deleted.is_(False))
    if filters:
        stmt = stmt.where(and_(*filters))

    rows = (await db.execute(stmt)).scalars().all()
    return [MonitoredSourceResponse.model_validate(row) for row in rows]


@router.get(
    "/collection-status",
    response_model=CollectionStatusSummary,
    summary="Resumo de status de coleta",
)
async def collection_status(
    source_type: MonitoredSourceType | None = Query(default=None),
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> CollectionStatusSummary:
    stmt = select(MonitoredSource).where(MonitoredSource.is_deleted.is_(False)).order_by(MonitoredSource.source_type, MonitoredSource.name)
    if source_type is not None:
        stmt = stmt.where(MonitoredSource.source_type == source_type.value)

    rows = (await db.execute(stmt)).scalars().all()

    counters = {
        MonitoredSourceStatus.ONLINE.value: 0,
        MonitoredSourceStatus.OFFLINE.value: 0,
        MonitoredSourceStatus.DEGRADED.value: 0,
        MonitoredSourceStatus.UNKNOWN.value: 0,
        MonitoredSourceStatus.DISABLED.value: 0,
    }
    items: list[CollectionStatusItem] = []

    for row in rows:
        status_key = row.status if row.status in counters else MonitoredSourceStatus.UNKNOWN.value
        counters[status_key] += 1
        items.append(
            CollectionStatusItem(
                id=row.id,
                source_type=MonitoredSourceType(row.source_type),
                name=row.name,
                endpoint=row.endpoint,
                status=MonitoredSourceStatus(status_key),
                last_collected_at=row.last_collected_at,
                last_connectivity_at=row.last_connectivity_at,
            )
        )

    return CollectionStatusSummary(
        total=len(rows),
        online=counters[MonitoredSourceStatus.ONLINE.value],
        offline=counters[MonitoredSourceStatus.OFFLINE.value],
        degraded=counters[MonitoredSourceStatus.DEGRADED.value],
        unknown=counters[MonitoredSourceStatus.UNKNOWN.value],
        disabled=counters[MonitoredSourceStatus.DISABLED.value],
        items=items,
    )


@router.get(
    "/{source_id}",
    response_model=MonitoredSourceResponse,
    summary="Detalhar fonte monitorada",
)
async def get_source(
    source_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> MonitoredSourceResponse:
    source = await _get_source_or_404(source_id, db)
    return MonitoredSourceResponse.model_validate(source)


@router.patch(
    "/{source_id}",
    response_model=MonitoredSourceResponse,
    summary="Atualizar fonte monitorada",
)
async def update_source(
    source_id: int,
    body: MonitoredSourceUpdate,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> MonitoredSourceResponse:
    source = await _get_source_or_404(source_id, db, include_deleted=True)
    if source.is_deleted:
        raise HTTPException(status_code=409, detail="Fonte removida logicamente; nao pode ser editada.")

    updates = body.model_dump(exclude_none=True)
    next_type = updates.get("source_type", MonitoredSourceType(source.source_type)).value
    next_name = (updates.get("name", source.name) or "").strip()
    next_endpoint = (updates.get("endpoint", source.endpoint) or "").strip()

    if any(k in updates for k in ("source_type", "name", "endpoint")):
        await _assert_no_active_duplicate(
            db,
            source_type=next_type,
            name=next_name,
            endpoint=next_endpoint,
            exclude_id=source.id,
        )

    if "source_type" in updates:
        source.source_type = updates["source_type"].value
    if "name" in updates:
        source.name = updates["name"].strip()
    if "endpoint" in updates:
        source.endpoint = updates["endpoint"].strip()
    if "username" in updates:
        source.username = updates["username"].strip()
    if "is_active" in updates:
        source.is_active = bool(updates["is_active"])
        if not source.is_active:
            source.status = MonitoredSourceStatus.DISABLED.value
        elif source.status == MonitoredSourceStatus.DISABLED.value:
            source.status = MonitoredSourceStatus.UNKNOWN.value
    if "status" in updates:
        source.status = updates["status"].value

    if "secret" in updates:
        encrypted = _encrypt_secret_or_422(updates["secret"])
        if source.secret:
            source.secret.secret_encrypted = encrypted
        else:
            source.secret = MonitoredSourceSecret(secret_encrypted=encrypted)

    await db.flush()
    await db.refresh(source)
    return MonitoredSourceResponse.model_validate(source)


@router.delete(
    "/{source_id}",
    status_code=status.HTTP_200_OK,
    summary="Remocao logica de fonte monitorada",
)
async def delete_source(
    source_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> dict:
    source = await _get_source_or_404(source_id, db, include_deleted=True)
    if source.is_deleted:
        return {"status": "ok", "source_id": source.id, "deleted": True}

    source.is_deleted = True
    source.is_active = False
    source.status = MonitoredSourceStatus.DELETED.value
    source.deleted_at = datetime.now(timezone.utc)
    await db.flush()
    return {"status": "ok", "source_id": source.id, "deleted": True}


@router.post(
    "/{source_id}/test-connectivity",
    response_model=ConnectivityTestResponse,
    summary="Testar conectividade (stub)",
)
async def test_source_connectivity(
    source_id: int,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> ConnectivityTestResponse:
    source = await _get_source_or_404(source_id, db)
    if source.secret is None:
        raise HTTPException(status_code=422, detail="Credencial nao cadastrada para a fonte.")

    try:
        decrypt_password(source.secret.secret_encrypted)
    except CryptoError as exc:
        raise HTTPException(status_code=422, detail=f"Falha ao decifrar credencial: {exc}")

    result = test_connectivity_stub(source_type=source.source_type, endpoint=source.endpoint)

    source.last_connectivity_at = result.checked_at
    if source.is_active:
        source.status = result.status
    await db.flush()

    return ConnectivityTestResponse(
        source_id=source.id,
        source_type=MonitoredSourceType(source.source_type),
        status=MonitoredSourceStatus(source.status),
        reachable=result.reachable,
        checked_at=result.checked_at,
        message=result.message,
    )


@router.post(
    "/{source_id}/collection/mark",
    response_model=MonitoredSourceResponse,
    summary="Registrar atualizacao de coleta",
)
async def mark_source_collection(
    source_id: int,
    body: CollectionMarkRequest,
    db: AsyncSession = Depends(get_db),
    _: dict = Depends(get_current_user),
) -> MonitoredSourceResponse:
    source = await _get_source_or_404(source_id, db)

    source.last_collected_at = body.collected_at or datetime.now(timezone.utc)
    if body.status is not None:
        source.status = body.status.value
    elif source.status == MonitoredSourceStatus.UNKNOWN.value:
        source.status = MonitoredSourceStatus.ONLINE.value

    await db.flush()
    await db.refresh(source)
    return MonitoredSourceResponse.model_validate(source)
