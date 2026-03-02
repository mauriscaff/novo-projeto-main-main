from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase

from config import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    connect_args={"check_same_thread": False},
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


def _ensure_datastore_metrics_column(sync_conn):  # sync_conn = SQLAlchemy sync connection
    """Adiciona coluna datastore_metrics à tabela zombie_scan_jobs se não existir (migração)."""
    from sqlalchemy import text
    if "sqlite" not in str(sync_conn.engine.url):
        return
    cur = sync_conn.execute(text("PRAGMA table_info(zombie_scan_jobs)"))
    rows = cur.fetchall()
    # PRAGMA retorna (cid, name, type, notnull, default_value, pk)
    if any(r[1] == "datastore_metrics" for r in rows):
        return
    sync_conn.execute(text("ALTER TABLE zombie_scan_jobs ADD COLUMN datastore_metrics TEXT"))


def _ensure_vmdk_deeplink_columns(sync_conn):
    """Adiciona colunas de deeplink vCenter à tabela zombie_vmdk_records se não existirem."""
    from sqlalchemy import text
    if "sqlite" not in str(sync_conn.engine.url):
        return
    cur = sync_conn.execute(text("PRAGMA table_info(zombie_vmdk_records)"))
    rows = cur.fetchall()
    names = {r[1] for r in rows}
    new_columns = [
        ("vcenter_deeplink_ui", "TEXT"),
        ("vcenter_deeplink_folder", "TEXT"),
        ("vcenter_deeplink_folder_dir", "TEXT"),
        ("datacenter_path", "TEXT"),
        ("datastore_name", "TEXT"),
        ("vmdk_folder", "TEXT"),
        ("vmdk_filename", "TEXT"),
    ]
    for col_name, col_type in new_columns:
        if col_name not in names:
            sync_conn.execute(text(f"ALTER TABLE zombie_vmdk_records ADD COLUMN {col_name} {col_type}"))


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_ensure_datastore_metrics_column)
        await conn.run_sync(_ensure_vmdk_deeplink_columns)
