# Scenario - false positives
"""Cenários conhecidos de falso positivo (EX-3, POSSIBLE_FALSE_POSITIVE)."""

from app.core.scanner.zombie_detector import (
    ZombieType,
    _classify_vmdk,
    _InventorySnapshot,
)
from tests.conftest import make_file_entry

SHARED_EMPTY = set()
FAKE_VC = "vcenter.fake.local"


def _is_zombie(r):
    """True se _classify_vmdk retornou ZombieVmdkResult (não skip)."""
    return r is not None and not (isinstance(r, tuple) and len(r) == 2 and r[0] is None)


def test_shared_datastore_classified_as_possible_false_positive():
    inventory = _InventorySnapshot(
        vmdk_paths=frozenset(),
        vmx_paths=frozenset(),
        vm_folders=frozenset(),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VC,
    )
    entry = make_file_entry("[DS_SHARED] vm/disk.vmdk", is_descriptor_vmdk=True)
    result = _classify_vmdk(
        entry=entry,
        inventory=inventory,
        shared_datastores={"DS_SHARED"},
        folder_files={"[DS_SHARED] vm/": {"disk.vmdk"}},
        global_files=set(),
        datacenter_name="DC1",
        datastore_name="DS_SHARED",
        ds_type="VMFS",
        orphan_days=60,
        stale_snapshot_days=15,
        min_file_size_mb=50,
    )
    assert _is_zombie(result)
    assert result.tipo_zombie == ZombieType.POSSIBLE_FALSE_POSITIVE
    assert result.false_positive_reason is not None
