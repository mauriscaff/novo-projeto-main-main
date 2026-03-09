"""
Fixtures globais para testes do ZombieHunter.

Fornece mocks do vCenter (pyVmomi), dados fake de inventário e configuração
para rodar testes sem conexão real com VMware.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

TEST_DATABASE_URL = "sqlite+aiosqlite:///./test_zombiehunter.db"
PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

os.environ.setdefault("DATABASE_URL", TEST_DATABASE_URL)
os.environ.setdefault("SCHEDULER_ENABLED", "false")
os.environ.setdefault("READONLY_MODE", "true")

from app.core.scanner.zombie_detector import (
    _FileEntry,
    _InventorySnapshot,
    ZombieType,
)


@pytest.fixture
def client():
    from main import app

    with TestClient(app) as test_client:
        yield test_client


# ─────────────────────────────────────────────────────────────────────────────
# Dados fake — paths e inventário
# ─────────────────────────────────────────────────────────────────────────────

FAKE_VCENTER_HOST = "vcenter.fake.local"
FAKE_DATACENTER = "Datacenter-Producao"
FAKE_DATASTORE = "DS_SSD_01"
FAKE_VMDK_PATH = f"[{FAKE_DATASTORE}] VM_OLD/disk.vmdk"
FAKE_VMDK_PATH_DELTA = f"[{FAKE_DATASTORE}] VM_SNAP/vm-000001-delta.vmdk"
FAKE_VMX_PATH = f"[{FAKE_DATASTORE}] VM_OLD/VM_OLD.vmx"


@pytest.fixture
def fake_vcenter_host() -> str:
    return FAKE_VCENTER_HOST


@pytest.fixture
def fake_inventory_empty() -> _InventorySnapshot:
    """Inventário vazio — nenhum VMDK/VMx registrado (tudo é candidato a zombie)."""
    return _InventorySnapshot(
        vmdk_paths=frozenset(),
        vmx_paths=frozenset(),
        vm_folders=frozenset(),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VCENTER_HOST,
    )


@pytest.fixture
def fake_inventory_with_vmdk() -> _InventorySnapshot:
    """Inventário com um VMDK registrado (path normalizado)."""
    norm = FAKE_VMDK_PATH.strip().lower().replace("\\", "/")
    return _InventorySnapshot(
        vmdk_paths=frozenset({norm}),
        vmx_paths=frozenset({f"[{FAKE_DATASTORE}] vm_old/vm_old.vmx"}),
        vm_folders=frozenset({f"[{FAKE_DATASTORE}] vm_old/"}),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VCENTER_HOST,
    )


@pytest.fixture
def fake_inventory_content_library() -> _InventorySnapshot:
    """Inventário com pasta de Content Library conhecida (EX-6)."""
    return _InventorySnapshot(
        vmdk_paths=frozenset(),
        vmx_paths=frozenset(),
        vm_folders=frozenset(),
        content_library_paths=frozenset({"[ds1] contentlib-iso/"}),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VCENTER_HOST,
    )


# ─────────────────────────────────────────────────────────────────────────────
# _FileEntry fake — vários tipos de arquivo
# ─────────────────────────────────────────────────────────────────────────────

def make_file_entry(
    full_path: str,
    *,
    is_vmx: bool = False,
    is_descriptor_vmdk: bool = True,
    is_flat_vmdk: bool = False,
    is_delta_vmdk: bool = False,
    is_ctk_vmdk: bool = False,
    size_bytes: int | None = 1024 * 1024 * 100,  # 100 MB
    modification: datetime | None = None,
    disk_extents: list[str] | None = None,
) -> _FileEntry:
    """Constrói um _FileEntry para testes."""
    if modification is None:
        # Data no passado para não ser descartado por filtro de recência (orphan_days/stale_snapshot_days)
        modification = datetime.now(timezone.utc) - timedelta(days=100)
    if "] " in full_path:
        prefix, rest = full_path.split("] ", 1)
        parts = rest.strip().split("/")
        name = parts[-1] if parts else rest
        folder = f"{prefix}] " + ("/".join(parts[:-1]) + "/" if len(parts) > 1 else "")
    else:
        folder = ""
        name = full_path.split("/")[-1]
    return _FileEntry(
        folder=folder,
        name=name,
        full_path=full_path,
        size_bytes=size_bytes,
        modification=modification,
        is_vmx=is_vmx,
        is_descriptor_vmdk=is_descriptor_vmdk,
        is_flat_vmdk=is_flat_vmdk,
        is_delta_vmdk=is_delta_vmdk,
        is_ctk_vmdk=is_ctk_vmdk,
        disk_extents=disk_extents or [],
    )


@pytest.fixture
def file_entry_descriptor() -> _FileEntry:
    """Descriptor .vmdk normal (não flat/delta/ctk)."""
    return make_file_entry(
        FAKE_VMDK_PATH,
        is_descriptor_vmdk=True,
        is_flat_vmdk=False,
        is_delta_vmdk=False,
        is_ctk_vmdk=False,
    )


@pytest.fixture
def file_entry_ctk() -> _FileEntry:
    """Arquivo *-ctk.vmdk (EX-1 — sempre excluído)."""
    return make_file_entry(
        f"[{FAKE_DATASTORE}] VM_OLD/disk-ctk.vmdk",
        is_descriptor_vmdk=False,
        is_ctk_vmdk=True,
    )


@pytest.fixture
def file_entry_vcls() -> _FileEntry:
    """Arquivo vCLS-*.vmdk (EX-4 — sempre excluído)."""
    return make_file_entry(
        f"[{FAKE_DATASTORE}] vCLS-abc/vCLS-abc.vmdk",
        is_descriptor_vmdk=True,
    )


@pytest.fixture
def file_entry_delta() -> _FileEntry:
    """Arquivo *-delta.vmdk (snapshot)."""
    return make_file_entry(
        FAKE_VMDK_PATH_DELTA,
        is_descriptor_vmdk=False,
        is_delta_vmdk=True,
    )


@pytest.fixture
def file_entry_vmx() -> _FileEntry:
    """Arquivo .vmx (não é VMDK — ignorado)."""
    return make_file_entry(
        FAKE_VMX_PATH,
        is_vmx=True,
        is_descriptor_vmdk=False,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Mock vCenter (pyVmomi) — para testes que precisam de service_instance/content
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture
def mock_service_instance() -> MagicMock:
    """Mock de pyVmomi ServiceInstance (RetrieveContent, about.instanceUuid, etc.)."""
    si = MagicMock()
    content = MagicMock()
    content.about.instanceUuid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
    content.about.name = FAKE_VCENTER_HOST
    si.RetrieveContent.return_value = content
    si._stub.host = FAKE_VCENTER_HOST
    return si


@pytest.fixture
def mock_datastore() -> MagicMock:
    """Mock de vim.Datastore com _moId e name."""
    ds = MagicMock()
    ds.name = FAKE_DATASTORE
    ds._moId = "datastore-101"
    ds.summary.accessible = True
    ds.summary.type = "VMFS"
    return ds


@pytest.fixture
def shared_datastores_empty() -> set:
    """Nenhum datastore compartilhado (EX-3 não se aplica)."""
    return set()


@pytest.fixture
def shared_datastores_with_ds() -> set:
    """Datastore na lista de compartilhados (POSSIBLE_FALSE_POSITIVE)."""
    return {FAKE_DATASTORE}
