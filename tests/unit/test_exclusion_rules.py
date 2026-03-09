"""
Testes das regras de exclusão (EX-1 a EX-6) — arquivos que NUNCA são zombie.

Sem conexão vCenter; usa _classify_vmdk com _FileEntry e inventário fake.
"""

from __future__ import annotations

import pytest

from app.core.scanner.zombie_detector import _classify_vmdk

from tests.conftest import make_file_entry

SHARED_EMPTY: set[str] = set()
GLOBAL_FILES_EMPTY: set = set()


def _is_skip(r):
    """True se _classify_vmdk retornou skip (None ou (None, reason))."""
    return r is None or (isinstance(r, tuple) and len(r) == 3 and r[0] is None)


class TestExclusionRules:
    """Cada regra EX-* deve retornar None (não classificar como zombie)."""

    def test_ex1_ctk_excluded(self, fake_inventory_empty):
        """EX-1: *-ctk.vmdk (Change Block Tracking) sempre ignorado."""
        entry = make_file_entry(
            "[DS1] vm/vm-ctk.vmdk",
            is_descriptor_vmdk=False,
            is_ctk_vmdk=True,
        )
        result = _classify_vmdk(
            entry=entry,
            inventory=fake_inventory_empty,
            shared_datastores=SHARED_EMPTY,
            folder_files={"[DS1] vm/": {"vm.vmdk", "vm-ctk.vmdk"}},
            global_files=GLOBAL_FILES_EMPTY,
            datacenter_name="DC",
            datastore_name="DS1",
            ds_type="VMFS",
            orphan_days=60,
            stale_snapshot_days=15,
            min_file_size_mb=50,
        )
        assert _is_skip(result)

    def test_ex4_vcls_excluded(self, fake_inventory_empty):
        """EX-4: vCLS-*.vmdk (vSphere Cluster Services) sempre ignorado."""
        entry = make_file_entry("[DS1] vCLS-xyz/vCLS-xyz.vmdk", is_descriptor_vmdk=True)
        result = _classify_vmdk(
            entry=entry,
            inventory=fake_inventory_empty,
            shared_datastores=SHARED_EMPTY,
            folder_files={"[DS1] vCLS-xyz/": {"vCLS-xyz.vmdk"}},
            global_files=GLOBAL_FILES_EMPTY,
            datacenter_name="DC",
            datastore_name="DS1",
            ds_type="VMFS",
            orphan_days=60,
            stale_snapshot_days=15,
            min_file_size_mb=50,
        )
        assert _is_skip(result)

    def test_ex2_flat_with_descriptor_excluded(self, fake_inventory_empty):
        """EX-2: *-flat.vmdk quando descriptor na mesma pasta → ignorado."""
        entry = make_file_entry(
            "[DS1] vm/vm-flat.vmdk",
            is_descriptor_vmdk=False,
            is_flat_vmdk=True,
        )
        result = _classify_vmdk(
            entry=entry,
            inventory=fake_inventory_empty,
            shared_datastores=SHARED_EMPTY,
            folder_files={"[DS1] vm/": {"vm.vmdk", "vm-flat.vmdk"}},
            global_files=GLOBAL_FILES_EMPTY,
            datacenter_name="DC",
            datastore_name="DS1",
            ds_type="VMFS",
            orphan_days=60,
            stale_snapshot_days=15,
            min_file_size_mb=50,
        )
        assert _is_skip(result)

    def test_ex6_content_library_excluded(self, fake_inventory_content_library):
        """EX-6: VMDK em pasta de Content Library (contentlib-*) ignorado."""
        entry = make_file_entry(
            "[ds1] contentlib-iso/item.vmdk",
            is_descriptor_vmdk=True,
        )
        # folder normalizado no detector usa _normalize
        folder_norm = "[ds1] contentlib-iso/"
        result = _classify_vmdk(
            entry=entry,
            inventory=fake_inventory_content_library,
            shared_datastores=SHARED_EMPTY,
            folder_files={folder_norm: {"item.vmdk"}},
            global_files=GLOBAL_FILES_EMPTY,
            datacenter_name="DC",
            datastore_name="ds1",
            ds_type="VMFS",
            orphan_days=60,
            stale_snapshot_days=15,
            min_file_size_mb=50,
        )
        assert _is_skip(result)
