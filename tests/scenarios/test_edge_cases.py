"""Casos extremos e bugs conhecidos — paths malformados, NFS, etc."""

from __future__ import annotations

from app.core.scanner.zombie_detector import (
    generate_folder_deeplink,
    generate_vsphere_ui_link,
    _normalize,
    _compute_confidence_score,
    ZombieType,
)
from tests.conftest import make_file_entry

FAKE_VC = "vcenter.fake.local"
SHARED_EMPTY: set[str] = set()


def test_normalize_lowercase_and_slashes():
    assert _normalize("  C:\\Folder\\File  ") == "c:/folder/file"


def test_folder_deeplink_path_without_bracket_returns_empty():
    url = generate_folder_deeplink(
        vcenter_host="vc.local",
        datacenter_path="DC",
        datastore_name="DS1",
        vmdk_path="relative/path/file.vmdk",
        link_to_file=True,
    )
    assert url == ""


def test_vsphere_ui_link_with_empty_moref_still_produces_url():
    """Link UI com moref vazio ainda gera URL (pode ser inválido no vCenter)."""
    url = generate_vsphere_ui_link(
        vcenter_host="vc.local",
        vcenter_instance_uuid="uuid",
        datastore_moref="",
    )
    assert "/ui/#?" in url


def test_score_recent_snapshot_orphan_uses_stale_snapshot_days():
    """SNAPSHOT_ORPHAN usa stale_snapshot_days para penalidade de recência."""
    from datetime import datetime, timedelta, timezone
    recent = datetime.now(timezone.utc) - timedelta(days=5)
    score = _compute_confidence_score(
        tipo_zombie=ZombieType.SNAPSHOT_ORPHAN,
        folder_has_registered_vm=False,
        is_shared_datastore=False,
        modification=recent,
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert 5 <= score <= 100
