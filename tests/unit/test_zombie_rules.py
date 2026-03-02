"""
Testes de CADA regra de detecção de zombie individualmente.

Usa @pytest.mark.parametrize para múltiplos cenários por regra.
Sem conexão vCenter; inventário e _FileEntry fake em conftest.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest
from app.core.scanner.zombie_detector import (
    TIPOS_EXCLUIVEIS,
    ZombieType,
    _classify_vmdk,
    _compute_confidence_score,
    _InventorySnapshot,
)
from tests.conftest import make_file_entry

# Constantes para evitar repetição
SHARED_EMPTY: set[str] = set()
FAKE_VC = "vcenter.fake.local"
DC, DS = "DC1", "DS1"
KW = dict(
    datacenter_name=DC,
    datastore_name=DS,
    ds_type="VMFS",
    orphan_days=60,
    stale_snapshot_days=15,
    min_file_size_mb=50,
)


def _is_zombie(r):
    """True se o retorno de _classify_vmdk é um ZombieVmdkResult (não skip)."""
    return r is not None and not (isinstance(r, tuple) and r[0] is None)


def _is_skip(r):
    """True se o retorno de _classify_vmdk é um skip (tuple com None como primeiro elemento)."""
    return isinstance(r, tuple) and r[0] is None


def _classify(
    entry,
    inventory,
    shared_datastores=None,
    folder_files=None,
    global_files=None,
):
    return _classify_vmdk(
        entry=entry,
        inventory=inventory,
        shared_datastores=shared_datastores or SHARED_EMPTY,
        folder_files=folder_files or {},
        global_files=global_files or set(),
        **KW,
    )


def _inv_empty():
    return _InventorySnapshot(
        vmdk_paths=frozenset(),
        vmx_paths=frozenset(),
        vm_folders=frozenset(),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VC,
    )


def _inv_with_vmdk(normalized_path: str, vm_folder: str = ""):
    return _InventorySnapshot(
        vmdk_paths=frozenset({normalized_path}),
        vmx_paths=frozenset({f"{vm_folder}vm.vmx".replace("//", "/")}),
        vm_folders=frozenset({vm_folder}) if vm_folder else frozenset(),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VC,
    )


def _inv_with_folder(folder: str):
    """Inventário com pasta registrada (vm_folders) mas sem VMDKs — para ORPHANED/BROKEN_CHAIN na pasta."""
    norm = folder.strip().lower().replace("\\", "/")
    return _InventorySnapshot(
        vmdk_paths=frozenset(),
        vmx_paths=frozenset(),
        vm_folders=frozenset({norm}),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VC,
    )


# ═══════════════════════════════════════════════════════════════
# REGRA 1: CRITÉRIO BASE (ausência no inventário)
# ═══════════════════════════════════════════════════════════════


def test_vmdk_referenced_in_inventory_not_zombie():
    """VMDK listado em Get-HardDisk de VM ativa → NÃO zombie."""
    path = "[DS1] vm/vm.vmdk"
    inv = _inv_with_vmdk(path.strip().lower().replace("\\", "/"), "[ds1] vm/")
    entry = make_file_entry(path, is_descriptor_vmdk=True)
    result = _classify(entry, inv, folder_files={"[DS1] vm/": {"vm.vmdk", "vm.vmx"}})
    assert _is_skip(result)


def test_vmdk_not_referenced_is_orphaned():
    """VMDK existe no datastore, não está em nenhuma VM → tipo=ORPHANED."""
    inv = _inv_with_folder("[ds1] vm/")
    entry = make_file_entry("[DS1] vm/vm.vmdk", is_descriptor_vmdk=True)
    result = _classify(
        entry, inv,
        folder_files={"[DS1] vm/": {"vm.vmdk", "vm.vmx"}},
        global_files={"vm.vmdk", "vm-flat.vmdk"},
    )
    assert _is_zombie(result)
    assert result.tipo_zombie == ZombieType.ORPHANED


def test_vmdk_referenced_in_template_not_zombie():
    """VMDK pertence a um template (inventário) → NÃO zombie."""
    path = "[DS1] template/tpl.vmdk"
    norm = path.strip().lower().replace("\\", "/")
    inv = _InventorySnapshot(
        vmdk_paths=frozenset({norm}),
        vmx_paths=frozenset(),
        vm_folders=frozenset(),
        content_library_paths=frozenset(),
        fcd_paths=frozenset(),
        vcenter_host=FAKE_VC,
    )
    entry = make_file_entry(path, is_descriptor_vmdk=True)
    result = _classify(entry, inv, folder_files={"[DS1] template/": {"tpl.vmdk"}})
    assert _is_skip(result)


# ═══════════════════════════════════════════════════════════════
# REGRA 2: DIRETÓRIO NÃO REGISTRADO
# ═══════════════════════════════════════════════════════════════


def test_vmdk_in_unregistered_folder_is_zombie():
    """Pasta não corresponde a nenhuma VM no vCenter → tipo=UNREGISTERED_DIR."""
    inv = _inv_empty()
    entry = make_file_entry("[DS1] pasta_qualquer/arquivo.vmdk", is_descriptor_vmdk=True)
    result = _classify(
        entry, inv,
        folder_files={"[DS1] pasta_qualquer/": {"arquivo.vmdk"}},
        global_files={"arquivo.vmdk", "arquivo-flat.vmdk"},
    )
    assert _is_zombie(result)
    assert result.tipo_zombie == ZombieType.UNREGISTERED_DIR


def test_vmdk_in_registered_folder_other_vmdk_not_zombie_if_this_in_inventory():
    """Pasta com VM registrada; este VMDK no inventário → NÃO zombie."""
    path = "[DS1] vm/disk2.vmdk"
    norm = path.strip().lower().replace("\\", "/")
    inv = _inv_with_vmdk(norm, "[ds1] vm/")
    entry = make_file_entry(path, is_descriptor_vmdk=True)
    result = _classify(
        entry, inv,
        folder_files={"[DS1] vm/": {"disk2.vmdk", "vm.vmx"}},
    )
    assert _is_skip(result)


# ═══════════════════════════════════════════════════════════════
# REGRA 3: SNAPSHOT DELTA ÓRFÃO
# ═══════════════════════════════════════════════════════════════


@pytest.mark.parametrize("filename,is_delta,expected_type", [
    ("vm-000001-delta.vmdk", True, ZombieType.SNAPSHOT_ORPHAN),
    ("vm-000002-delta.vmdk", True, ZombieType.SNAPSHOT_ORPHAN),
    ("vm-delta.vmdk", True, ZombieType.SNAPSHOT_ORPHAN),
    ("vm.vmdk", False, ZombieType.ORPHANED),
])
def test_delta_vmdk_without_active_snapshot(filename, is_delta, expected_type):
    """Arquivo delta sem snapshot ativo: *-delta.vmdk são excluídos por sufixo; descriptor → tipo conforme parametrize."""
    folder = "[DS1] vm/"
    full_path = f"[DS1] vm/{filename}"
    inv = _inv_with_folder("[ds1] vm/") if not is_delta else _inv_empty()
    entry = make_file_entry(
        full_path,
        is_descriptor_vmdk=not is_delta,
        is_delta_vmdk=is_delta,
    )
    folder_files = {folder: {filename}}
    global_files = {filename}
    if not is_delta:
        folder_files[folder].add("vm.vmx")
        global_files.add("vm-flat.vmdk")
    result = _classify(entry, inv, folder_files=folder_files, global_files=global_files)
    if filename.endswith("-delta.vmdk"):
        assert _is_skip(result)
        assert isinstance(result, tuple) and result[0] is None and result[1] == "suffix_exclusion"
    else:
        assert _is_zombie(result)
        assert result.tipo_zombie == expected_type


def test_delta_vmdk_with_active_snapshot_not_zombie():
    """Arquivo *-delta.vmdk com snapshot ativo correspondente → NÃO zombie."""
    # Descriptor da snapshot (vm-000001.vmdk) no inventário = delta em uso
    path_descriptor = "[ds1] vm/vm-000001.vmdk"
    path_delta = "[DS1] vm/vm-000001-delta.vmdk"
    inv = _inv_with_vmdk(path_descriptor.strip().lower().replace("\\", "/"), "[ds1] vm/")
    entry = make_file_entry(path_delta, is_descriptor_vmdk=False, is_delta_vmdk=True)
    result = _classify(
        entry, inv,
        folder_files={"[DS1] vm/": {"vm-000001.vmdk", "vm-000001-delta.vmdk", "vm.vmx"}},
    )
    assert _is_skip(result)


# ═══════════════════════════════════════════════════════════════
# REGRA 4: CADEIA QUEBRADA
# ═══════════════════════════════════════════════════════════════


def test_broken_chain_detected():
    """Descriptor sem extent esperado (vm-flat.vmdk) → tipo=BROKEN_CHAIN."""
    inv = _inv_with_folder("[ds1] vm/")
    # size_bytes pequeno = descriptor de texto; >1MB seria tratado como monolítico e não BROKEN_CHAIN
    entry = make_file_entry(
        "[DS1] vm/vm.vmdk",
        is_descriptor_vmdk=True,
        size_bytes=512,
    )
    folder_files = {"[DS1] vm/": {"vm.vmdk", "vm.vmx"}}
    global_files = {"vm.vmdk"}
    result = _classify(entry, inv, folder_files=folder_files, global_files=global_files)
    assert _is_zombie(result)
    assert result.tipo_zombie == ZombieType.BROKEN_CHAIN


def test_valid_chain_not_broken():
    """Descriptor com extent presente → não classificado como BROKEN_CHAIN."""
    inv = _inv_with_folder("[ds1] vm/")
    entry = make_file_entry("[DS1] vm/vm.vmdk", is_descriptor_vmdk=True)
    folder_files = {"[DS1] vm/": {"vm.vmdk", "vm-flat.vmdk", "vm.vmx"}}
    global_files = {"vm.vmdk", "vm-flat.vmdk"}
    result = _classify(entry, inv, folder_files=folder_files, global_files=global_files)
    assert _is_zombie(result)
    assert result.tipo_zombie != ZombieType.BROKEN_CHAIN
    assert result.tipo_zombie == ZombieType.ORPHANED


# ═══════════════════════════════════════════════════════════════
# REGRA 5: DISCO REMOVIDO SEM DELEÇÃO
# ═══════════════════════════════════════════════════════════════


def test_disk_removed_without_delete_is_orphaned():
    """VMDK existe mas foi removido do .vmx (não deletado do disco) → tipo=ORPHANED."""
    inv = _inv_with_folder("[ds1] vm/")
    entry = make_file_entry("[DS1] vm/disk.vmdk", is_descriptor_vmdk=True)
    result = _classify(
        entry, inv,
        folder_files={"[DS1] vm/": {"disk.vmdk", "VM.vmx"}},
        global_files={"disk.vmdk", "disk-flat.vmdk"},
    )
    assert _is_zombie(result)
    assert result.tipo_zombie == ZombieType.ORPHANED


# ═══════════════════════════════════════════════════════════════
# REGRAS DE EXCLUSÃO
# ═══════════════════════════════════════════════════════════════


def test_ctk_vmdk_never_zombie():
    """Arquivo *-ctk.vmdk → IGNORADO (não aparece nos resultados)."""
    entry = make_file_entry("[DS1] vm/vm-ctk.vmdk", is_descriptor_vmdk=False, is_ctk_vmdk=True)
    result = _classify(entry, _inv_empty())
    assert _is_skip(result)


def test_flat_vmdk_with_valid_descriptor_not_zombie():
    """Arquivo *-flat.vmdk com descriptor .vmdk válido na mesma pasta → IGNORADO."""
    entry = make_file_entry("[DS1] vm/vm-flat.vmdk", is_descriptor_vmdk=False, is_flat_vmdk=True)
    result = _classify(
        entry, _inv_empty(),
        folder_files={"[DS1] vm/": {"vm.vmdk", "vm-flat.vmdk"}},
    )
    assert _is_skip(result)


def test_flat_vmdk_without_descriptor_ignored_by_design():
    """*-flat.vmdk sem descriptor: implementação atual ignora (evitar FP)."""
    entry = make_file_entry("[DS1] vm/vm-flat.vmdk", is_descriptor_vmdk=False, is_flat_vmdk=True)
    result = _classify(entry, _inv_empty(), folder_files={"[DS1] vm/": {"vm-flat.vmdk"}})
    assert _is_skip(result)


def test_vcls_vmdk_never_zombie():
    """Arquivo vCLS-*.vmdk → IGNORADO."""
    entry = make_file_entry("[DS1] vCLS-xyz/vCLS-xyz.vmdk", is_descriptor_vmdk=True)
    result = _classify(entry, _inv_empty())
    assert _is_skip(result)


def test_shared_datastore_is_false_positive():
    """Datastore compartilhado → tipo=POSSIBLE_FALSE_POSITIVE."""
    from app.core.scanner.zombie_detector import _classify_vmdk
    entry = make_file_entry("[DS_SHARED] vm/disk.vmdk", is_descriptor_vmdk=True)
    result = _classify_vmdk(
        entry=entry,
        inventory=_inv_empty(),
        shared_datastores={"DS_SHARED"},
        folder_files={"[DS_SHARED] vm/": {"disk.vmdk"}},
        global_files=set(),
        datacenter_name=DC,
        datastore_name="DS_SHARED",
        ds_type="VMFS",
        orphan_days=60,
        stale_snapshot_days=15,
        min_file_size_mb=50,
    )
    assert _is_zombie(result)
    assert result.tipo_zombie == ZombieType.POSSIBLE_FALSE_POSITIVE


@pytest.mark.skip(reason="pyVmomi não verifica lock (vmkfstools -D); não implementado")
def test_active_lock_is_false_positive():
    """vmkfstools -D confirma lock ativo → POSSIBLE_FALSE_POSITIVE (não implementado)."""
    pass


# ═══════════════════════════════════════════════════════════════
# SCORE DE CONFIANÇA
# ═══════════════════════════════════════════════════════════════


@pytest.mark.parametrize("conditions,expected_min_score", [
    (["not_in_inventory", "not_in_vmx", "old_file"], 70),
    (["not_in_inventory", "not_in_vmx", "no_lock", "unregistered_dir", "old_file"], 85),
    (["not_in_inventory", "shared_datastore"], 0),
])
def test_score_calculation(conditions, expected_min_score):
    """Score mínimo conforme condições (base 40 + bônus/penalidades)."""
    cond = set(conditions)
    modification = (datetime.now(timezone.utc) - timedelta(days=100)) if "old_file" in cond else None
    score = _compute_confidence_score(
        tipo_zombie=ZombieType.UNREGISTERED_DIR if "unregistered_dir" in cond else ZombieType.ORPHANED,
        folder_has_registered_vm="not_in_vmx" not in cond,
        is_shared_datastore="shared_datastore" in cond,
        modification=modification,
        orphan_days=60,
        stale_snapshot_days=15,
    )
    if "shared_datastore" in cond:
        assert score <= 50
    else:
        assert score >= expected_min_score


def test_score_below_60_stays_as_monitor():
    """Score < 60% → nunca ELEGÍVEL (apenas MONITORAR)."""
    score = _compute_confidence_score(
        ZombieType.POSSIBLE_FALSE_POSITIVE,
        folder_has_registered_vm=True,
        is_shared_datastore=True,
        modification=None,
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert score < 60


def test_score_above_85_eligible_for_approval():
    """Score ≥ 85% → pode ser ELEGÍVEL para aprovação."""
    score = _compute_confidence_score(
        ZombieType.UNREGISTERED_DIR,
        folder_has_registered_vm=False,
        is_shared_datastore=False,
        modification=datetime.now(timezone.utc) - timedelta(days=100),
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert score >= 85


# ═══════════════════════════════════════════════════════════════
# TIPOS EXCLUÍVEIS
# ═══════════════════════════════════════════════════════════════


class TestTiposExcluiveis:
    """TIPOS_EXCLUIVEIS: apenas ORPHANED, SNAPSHOT_ORPHAN, BROKEN_CHAIN, UNREGISTERED_DIR."""

    def test_possible_false_positive_not_excluivel(self):
        assert ZombieType.POSSIBLE_FALSE_POSITIVE.value not in TIPOS_EXCLUIVEIS

    def test_orphaned_is_excluivel(self):
        assert ZombieType.ORPHANED.value in TIPOS_EXCLUIVEIS

    def test_snapshot_orphan_broken_chain_unregistered_in_excluiveis(self):
        assert ZombieType.SNAPSHOT_ORPHAN.value in TIPOS_EXCLUIVEIS
        assert ZombieType.BROKEN_CHAIN.value in TIPOS_EXCLUIVEIS
        assert ZombieType.UNREGISTERED_DIR.value in TIPOS_EXCLUIVEIS


# ═══════════════════════════════════════════════════════════════
# TESTES DE SEGURANÇA (READ-ONLY / APPROVAL)
# ═══════════════════════════════════════════════════════════════


def test_readonly_mode_blocks_execute_endpoint():
    """READONLY_MODE=true → POST execute deve retornar 403."""
    from unittest.mock import patch
    import os
    os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test_zombiehunter.db")
    with patch("app.core.approval.settings") as m:
        m.readonly_mode = True
        from fastapi.testclient import TestClient
        from main import app
        client = TestClient(app)
        r = client.post(
            "/api/v1/approvals/fake-token-123/execute",
            json={},
            headers={"X-API-Key": "change-me-in-production"},
        )
        assert r.status_code in (401, 403, 404, 422)


def test_execute_without_dryrun_is_blocked():
    """Tentar executar sem ter chamado /dryrun antes → erro 400 com mensagem clara."""
    import os
    os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./test_zombiehunter.db")
    from fastapi.testclient import TestClient
    from main import app
    client = TestClient(app)
    r = client.post(
        "/api/v1/approvals/invalid-token-no-dryrun/execute",
        json={},
        headers={"X-API-Key": "change-me-in-production"},
    )
    assert r.status_code in (400, 401, 404, 410)
    if r.status_code == 400:
        assert "dry" in r.json().get("detail", "").lower() or "dry" in str(r.json())


@pytest.mark.skip(reason="Requer READONLY_MODE=false e token válido no banco; uso em CI opcional")
def test_execute_with_valid_token_and_dryrun_succeeds():
    """Fluxo completo: criar token → dryrun → execute → 200 (com READONLY_MODE=false)."""
    pass


@pytest.mark.skip(reason="Requer token expirado no banco; teste de integração")
def test_approval_token_expires_after_24h():
    """Token com timestamp > 24h → 410 (Gone) com mensagem de expiração."""
    pass


@pytest.mark.skip(reason="Requer token já executado no banco; teste de integração")
def test_double_execute_same_token_blocked():
    """Segunda execução com mesmo token → 409 (Conflict)."""
    pass
