"""
Detector de VMDKs zombie/orphaned conforme as definições OFICIAIS Broadcom/VMware.

═══════════════════════════════════════════════════════════════════════════════
FLUXO DE VALIDAÇÃO OBRIGATÓRIO (7 passos — Broadcom KB 404094)
═══════════════════════════════════════════════════════════════════════════════

PASSO 1  Arquivo existe no datastore?
           Sim → prosseguir | Não → ignorar

PASSO 2  É arquivo excluído (*-ctk.vmdk, vCLS-*)?
           Sim → IGNORAR | Não → prosseguir

PASSO 3  Está referenciado em algum Get-HardDisk do inventário?
           Sim → NÃO é zombie | Não → prosseguir (candidato)

PASSO 4  O datastore é compartilhado entre múltiplos vCenters?
           Sim → POSSIBLE_FALSE_POSITIVE | Não → prosseguir

PASSO 5  É um *-delta.vmdk / *-000001.vmdk?
           Sim → verificar snapshot ativo → sem snapshot → SNAPSHOT_ORPHAN

PASSO 6  vmkfstools -q (equivalente via naming-convention) retorna erro?
           Sim → BROKEN_CHAIN | Não → prosseguir

PASSO 7  vmkfstools -D confirma lock ativo? (pyVmomi não fornece acesso direto)
           Não verificável via API → assume sem lock (+10 no score)
           Se POSSIBLE_FALSE_POSITIVE → assume possível lock (-10 no score)

═══════════════════════════════════════════════════════════════════════════════
REGRAS DE DETECÇÃO (Broadcom KB 404094)
═══════════════════════════════════════════════════════════════════════════════

REGRA 1 — CRITÉRIO BASE (obrigatório para qualquer classificação):
  VMDK existe no datastore MAS não está listado em nenhum Get-HardDisk
  de nenhuma VM/template registrado no vCenter.

REGRA 2 — DIRETÓRIO NÃO REGISTRADO:
  VMDK em pasta/diretório que não corresponde a nenhuma VM registrada.
  Tipo: UNREGISTERED_DIR

REGRA 3 — SNAPSHOT DELTA ÓRFÃO:
  *-delta.vmdk ou *-000001.vmdk sem snapshot ativo correspondente.
  Tipo: SNAPSHOT_ORPHAN

REGRA 4 — CADEIA QUEBRADA:
  Descriptor .vmdk aponta para -flat.vmdk ou parent inexistente.
  Tipo: BROKEN_CHAIN

REGRA 5 — DISCO REMOVIDO SEM DELEÇÃO:
  HD removido via "Remove" (não "Delete from disk") — .vmx parou de
  referenciar o .vmdk mas o arquivo continuou no datastore.
  Detecção: mesma que REGRA 1 (ausência de referência no inventário).
  Tipo: ORPHANED

═══════════════════════════════════════════════════════════════════════════════
REGRAS DE EXCLUSÃO
═══════════════════════════════════════════════════════════════════════════════

EX-1  *-ctk.vmdk (Change Block Tracking): sempre ignorar.
EX-2  *-flat.vmdk com descriptor .vmdk válido na mesma pasta: ignorar.
EX-3  Datastores compartilhados entre múltiplos vCenters: POSSIBLE_FALSE_POSITIVE.
      (Broadcom KB 383876)
EX-4  vCLS-*.vmdk (vSphere Cluster Services): sempre ignorar.
EX-5  Lock ativo (vmkfstools -D): não verificável via pyVmomi —
      documentado no campo false_positive_reason quando aplicável.
EX-6  VMDKs em Content Library do vCenter — sempre ignorar.

═══════════════════════════════════════════════════════════════════════════════
SCORE DE CONFIANÇA (0–100%)
═══════════════════════════════════════════════════════════════════════════════

  Ausente no Get-HardDisk               +40  (pré-requisito de classificação)
  Ausente no .vmx registrado da pasta   +20  (nenhuma VM registrada no folder)
  Diretório não registrado no vCenter   +15  (UNREGISTERED_DIR)
  vmkfstools confirma cadeia corrompida +15  (BROKEN_CHAIN)
  Sem lock ativo (pyVmomi não verifica)  +10  (assume ausência por default)
  Datastore compartilhado multi-vCenter -50  (risco de falso positivo)
  Arquivo recente (< OrphanDays)        -20  (baixa urgência)

  Score ≥ 85 → ELEGÍVEL para aprovação de deleção
  Score 60-84 → SUSPEITO — revisar manualmente
  Score < 60  → MONITORAR — não agir ainda

═══════════════════════════════════════════════════════════════════════════════
CONVENÇÃO DE NOMENCLATURA VMware VMDK
═══════════════════════════════════════════════════════════════════════════════

  vm.vmdk               → descriptor (thin/thick lazy/eager)
  vm-flat.vmdk          → extent de dados (thick, ou sparse thin)
  vm-000001.vmdk        → descriptor de snapshot N
  vm-000001-delta.vmdk  → extent de snapshot N
  vm-ctk.vmdk           → change block tracking
  vCLS-*.vmdk           → vSphere Cluster Services (gerenciado pelo vCenter)

"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

from pyVmomi import vim

from config import get_settings

logger = logging.getLogger(__name__)

# Padrão de descriptor de snapshot: sufixo -NNNNNN.vmdk (6 dígitos)
_SNAPSHOT_DESCRIPTOR_RE = re.compile(r"-\d{6}\.vmdk$", re.IGNORECASE)

# Tipos de datastore onde a convenção descriptor/extent é garantida
_SPLIT_VMDK_DS_TYPES = frozenset({"VMFS", "vsan", "vsanD", "vvol"})

# EX-4: padrão vCLS — VMs de vSphere Cluster Services gerenciadas pelo vCenter
_VCLS_RE = re.compile(r"^vcls[-_]", re.IGNORECASE)

# Causas comuns por tipo de zombie (exibidas no campo likely_causes)
_CAUSES_BY_TYPE: dict[str, list[str]] = {
    "ORPHANED": [
        "VM deletada do inventário sem 'Delete from disk'",
        "Hard disk removido via 'Remove' sem 'Delete from disk' (REGRA 5)",
        "vMotion com falha — arquivos ficaram no datastore de origem",
        "Clone ou deploy de template incompleto/interrompido",
        "Backup (CommVault, Veeam) com job incompleto",
    ],
    "SNAPSHOT_ORPHAN": [
        "Snapshot removido incorretamente sem limpar os deltas",
        "Backup com job incompleto (CommVault, Veeam, etc.)",
        "Storage vMotion interrompido durante operação com snapshots ativos",
        "Cadeia de snapshot corrompida",
    ],
    "BROKEN_CHAIN": [
        "Snapshot com cadeia corrompida — extent/flat ausente",
        "Storage vMotion interrompido — arquivos de extent não foram migrados",
        "Clone ou template com extents ausentes no datastore",
        "Deleção parcial de arquivos da VM diretamente no storage",
    ],
    "UNREGISTERED_DIR": [
        "VM deletada do inventário sem 'Delete from disk'",
        "Registro direto no ESXi sem passar pelo vCenter",
        "vMotion com falha — pasta permaneceu no datastore de origem",
        "Backup ou clone incompleto deixou pasta no datastore",
    ],
    "POSSIBLE_FALSE_POSITIVE": [
        "VMDK pode estar em uso por VM em outro vCenter que compartilha este datastore",
        "Confirme TODOS os vCenters conectados a este datastore antes de qualquer ação",
        "(Broadcom KB 383876 — datastore compartilhado multi-vCenter)",
    ],
}


# ═══════════════════════════════════════════════════════════════════════════════
# Tipos públicos
# ═══════════════════════════════════════════════════════════════════════════════


class ZombieType(str, Enum):
    """Categoria de VMDK zombie conforme regras Broadcom/VMware."""

    ORPHANED = "ORPHANED"
    """REGRAS 1+5: VMDK sem referência em nenhuma VM/template no inventário."""

    SNAPSHOT_ORPHAN = "SNAPSHOT_ORPHAN"
    """REGRA 3: Arquivo *-delta.vmdk sem snapshot chain ativo associado."""

    BROKEN_CHAIN = "BROKEN_CHAIN"
    """REGRA 4: Descriptor VMDK aponta para extent de dados inexistente."""

    UNREGISTERED_DIR = "UNREGISTERED_DIR"
    """REGRA 2: VMDK em pasta sem nenhuma VM registrada no vCenter."""

    POSSIBLE_FALSE_POSITIVE = "POSSIBLE_FALSE_POSITIVE"
    """EX-3: VMDK em datastore compartilhado — pode estar em uso em outro vCenter."""


# Tipos que podem ser excluídos do vCenter (QUARANTINE/DELETE) respeitando as regras
# (approval, readonly_mode, whitelist). POSSIBLE_FALSE_POSITIVE fica de fora por segurança.
TIPOS_EXCLUIVEIS: frozenset[str] = frozenset({
    ZombieType.ORPHANED.value,
    ZombieType.SNAPSHOT_ORPHAN.value,
    ZombieType.BROKEN_CHAIN.value,
    ZombieType.UNREGISTERED_DIR.value,
})


@dataclass
class ZombieVmdkResult:
    """Resultado completo de um VMDK detectado como zombie."""

    # ── Campos exigidos pela spec ────────────────────────────────────────────
    path: str
    """Caminho completo no formato [datastore] folder/arquivo.vmdk"""

    datastore: str
    """Nome do datastore que contém o arquivo."""

    tamanho_gb: float | None
    """Tamanho do arquivo em GiB (None se inacessível)."""

    ultima_modificacao: datetime | None
    """Data/hora UTC da última modificação conforme reportado pelo vCenter."""

    tipo_zombie: ZombieType
    """Categoria de zombie conforme enum ZombieType."""

    vcenter_host: str
    """Hostname/IP do vCenter onde o arquivo foi encontrado."""

    datacenter: str
    """Nome do Datacenter no vCenter."""

    # ── Campos de diagnóstico adicionais ─────────────────────────────────────
    detection_rules: list[str] = field(default_factory=list)
    """Regras acionadas na ordem do fluxo de validação Broadcom (auditoria)."""

    likely_causes: list[str] = field(default_factory=list)
    """Causas comuns prováveis — auxilia o analista na investigação."""

    false_positive_reason: str | None = None
    """Motivo pelo qual o item foi marcado como possível falso positivo."""

    folder: str = ""
    """Pasta dentro do datastore onde o arquivo reside."""

    datastore_type: str = ""
    """Tipo do datastore (VMFS, NFS, vSAN, vVol, etc.)."""

    confidence_score: int = 0
    """Score de confiança 0–100 calculado pelos critérios Broadcom/VMware."""

    # ── Links para vCenter (UI e /folder) ─────────────────────────────────────
    vcenter_deeplink_ui: str = ""
    """Link 1 — vSphere HTML5 Client (abre datastore no contexto visual)."""
    vcenter_deeplink_folder: str = ""
    """Link 2 — URL /folder apontando direto ao arquivo VMDK (KB 301563)."""
    vcenter_deeplink_folder_dir: str = ""
    """Link 2 variante — /folder só da pasta (sem arquivo)."""
    datacenter_path: str = ""
    """Nome/path do Datacenter (ex.: Datacenter-Producao)."""
    datastore_name: str = ""
    """Nome do datastore (ex.: DS_SSD_01). Já existe datastore; este alias para consistência com links."""
    vmdk_folder: str = ""
    """Pasta do VMDK no datastore (ex.: VM_ANTIGA_01)."""
    vmdk_filename: str = ""
    """Nome do arquivo VMDK (ex.: VM_ANTIGA_01.vmdk)."""

    evidence_log: list[str] = field(default_factory=list)
    """Trilha de auditoria: cada checagem que o arquivo passou/falhou em _classify_vmdk.
    Populado em ordem de execução; vazio quando evidence_log não foi solicitado."""


@dataclass
class DatastoreScanMetric:
    """
    Métricas de varredura por datastore (duração, arquivos, zombies).
    Usado para troubleshooting de datastores lentos ou travados.
    """

    datastore_name: str
    """Nome do datastore varrido."""

    scan_start_time: datetime
    """Início da varredura deste datastore (UTC)."""

    scan_duration_seconds: float
    """Tempo total da varredura em segundos."""

    files_found: int
    """Quantidade de arquivos .vmdk/.vmx encontrados pelo browse."""

    zombies_found: int
    """Quantidade de VMDKs classificados como zombie neste datastore."""


# ═══════════════════════════════════════════════════════════════════════════════
# Estruturas internas de coleta
# ═══════════════════════════════════════════════════════════════════════════════


@dataclass
class _FileEntry:
    """Representa um arquivo encontrado pelo DatastoreBrowser."""

    folder: str
    """Caminho da pasta no formato '[ds] folder/' """

    name: str
    """Nome do arquivo (ex.: 'vm.vmdk')."""

    full_path: str
    """Caminho completo: folder + name."""

    size_bytes: int | None
    modification: datetime | None

    # Classificação pelo sufixo do nome
    is_vmx: bool
    is_descriptor_vmdk: bool  # .vmdk que NÃO é flat, delta nem ctk
    is_flat_vmdk: bool        # *-flat.vmdk
    is_delta_vmdk: bool       # *-delta.vmdk
    is_ctk_vmdk: bool         # *-ctk.vmdk

    disk_extents: list[str] = field(default_factory=list)
    """Caminhos completos dos extents (preenchido quando disponível)."""


@dataclass
class _InventorySnapshot:
    """Estado do inventário do vCenter: VMs, templates e caminhos de VMDK."""

    vmdk_paths: frozenset[str]
    """Caminhos normalizados de TODOS os VMDKs referenciados por VMs/templates
    (hardware atual + cadeia completa de snapshots)."""

    vmx_paths: frozenset[str]
    """Caminhos normalizados dos arquivos .vmx de VMs/templates registradas."""

    vm_folders: frozenset[str]
    """Caminhos normalizados das pastas que contêm VMs registradas."""

    content_library_paths: frozenset[str]
    """Prefixos de pasta (normalizados) de Content Library — EX-6. VMDKs aqui são ignorados."""

    fcd_paths: frozenset[str]
    """Caminhos normalizados de VMDKs gerenciados como First Class Disks (FCDs/IVDs). EX-7."""

    vcenter_host: str


# ═══════════════════════════════════════════════════════════════════════════════
# Utilitários
# ═══════════════════════════════════════════════════════════════════════════════


def _normalize(path: str) -> str:
    """Normaliza caminhos para comparação case-insensitive com barras uniformes e sem espaços múltiplos."""
    norm = path.strip().lower().replace("\\", "/")
    return re.sub(r'\s+', ' ', norm)


def _bytes_to_gb(size: int | None) -> float | None:
    if size is None:
        return None
    return round(size / (1024 ** 3), 3)


def _utc(dt: datetime | None) -> datetime | None:
    """Garante que o datetime esteja em UTC com tzinfo."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt


def _extract_folder(ds_path: str) -> str:
    """
    Extrai a pasta de um caminho de datastore.
      '[ds] folder/sub/vm.vmx'  →  '[ds] folder/sub/'
      '[ds] vm.vmx'             →  '[ds] '
    """
    norm = _normalize(ds_path)
    slash_idx = norm.rfind("/")
    if slash_idx >= 0:
        return norm[: slash_idx + 1]
    bracket_idx = norm.rfind("] ")
    if bracket_idx >= 0:
        return norm[: bracket_idx + 2]
    return norm


def generate_folder_deeplink(
    vcenter_host: str,
    datacenter_path: str,
    datastore_name: str,
    vmdk_path: str,
    link_to_file: bool = True,
) -> str:
    """
    Gera URL /folder do vCenter que aponta à pasta ou ao arquivo VMDK (Broadcom KB 301563).
    Requer autenticação no vCenter; permite listagem e download do arquivo.
    """
    if "] " not in vmdk_path:
        return ""
    path_part = vmdk_path.split("] ", 1)[1].strip()
    if "/" in path_part:
        folder, filename = path_part.rsplit("/", 1)
    else:
        folder, filename = "", path_part
    if link_to_file and folder:
        url_path = f"/folder/{urllib.parse.quote(folder)}/{urllib.parse.quote(filename)}"
    elif link_to_file and filename:
        url_path = f"/folder/{urllib.parse.quote(filename)}"
    elif folder:
        url_path = f"/folder/{urllib.parse.quote(folder)}"
    else:
        url_path = "/folder"
    params = {"dcPath": datacenter_path, "dsName": datastore_name}
    base = vcenter_host.rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    return base + url_path + "?" + urllib.parse.urlencode(params)


def generate_vsphere_ui_link(
    vcenter_host: str,
    vcenter_instance_uuid: str,
    datastore_moref: str,
) -> str:
    """
    Gera link para abrir o datastore no vSphere HTML5 Client (navegação visual).
    Analista ainda precisa navegar até a pasta do VMDK.
    """
    object_id = f"urn:vmomi:Datastore:{datastore_moref}:{vcenter_instance_uuid}"
    params = {
        "extensionId": "vsphere.core.inventory.serverObjectViewsExtension",
        "objectId": object_id,
        "navigator": "vsphere.core.viTree.datastoresView",
    }
    base = vcenter_host.rstrip("/")
    if not base.startswith("http"):
        base = f"https://{base}"
    return base + "/ui/#?" + urllib.parse.urlencode(params)


def _wait_for_task(task: Any, timeout: int | None = None) -> None:
    """
    Bloqueia até a tarefa do vCenter completar (sucesso ou erro).
    Mantido síncrono para compatibilidade com chamadas não-async.
    """
    if timeout is None:
        timeout = get_settings().scan_datastore_timeout_sec
    elapsed = 0
    while task.info.state not in (
        vim.TaskInfo.State.success,
        vim.TaskInfo.State.error,
    ):
        time.sleep(2)
        elapsed += 2
        if elapsed >= timeout:
            try:
                task.CancelTask()
            except Exception:
                pass
            raise TimeoutError(
                f"Tarefa do vCenter excedeu {timeout}s sem conclusão."
            )
    if task.info.state == vim.TaskInfo.State.error:
        raise RuntimeError(
            f"Tarefa do vCenter falhou: {task.info.error.msg}"
        )


async def _wait_for_task_async(task: Any, timeout: int | None = None) -> None:
    """
    Versão assíncrona: aguarda a tarefa do vCenter sem bloquear o event loop.
    Usa asyncio.sleep(2) em vez de time.sleep(2), com mesma lógica de timeout
    e cancelamento. Para uso quando o browse é chamado de contexto async.
    """
    if timeout is None:
        timeout = get_settings().scan_datastore_timeout_sec
    elapsed = 0
    while task.info.state not in (
        vim.TaskInfo.State.success,
        vim.TaskInfo.State.error,
    ):
        await asyncio.sleep(2)
        elapsed += 2
        if elapsed >= timeout:
            try:
                task.CancelTask()
            except Exception:
                pass
            raise TimeoutError(
                f"Tarefa do vCenter excedeu {timeout}s sem conclusão."
            )
    if task.info.state == vim.TaskInfo.State.error:
        raise RuntimeError(
            f"Tarefa do vCenter falhou: {task.info.error.msg}"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Score de confiança (critérios Broadcom/VMware)
# ═══════════════════════════════════════════════════════════════════════════════


def _compute_confidence_score(
    tipo_zombie: ZombieType,
    folder_has_registered_vm: bool,
    is_shared_datastore: bool,
    modification: datetime | None,
    orphan_days: int,
    stale_snapshot_days: int,
) -> int:
    """
    Calcula o score de confiança (0–100) com base nos critérios Broadcom/VMware.

    Critério                                Pontos
    ──────────────────────────────────────────────
    Ausente no Get-HardDisk                  +40   (sempre — pré-requisito)
    Sem lock ativo (pyVmomi não verifica)    +10   (assume ausência por default)
    Ausente no .vmx registrado da pasta      +20   (nenhuma VM no folder)
    Diretório não registrado no vCenter      +15   (UNREGISTERED_DIR)
    vmkfstools confirma cadeia corrompida    +15   (BROKEN_CHAIN)
    Datastore compartilhado multi-vCenter   -50   (risco de falso positivo)
    Arquivo recente (< OrphanDays)          -20   (baixa urgência)

    Score ≥ 85 → ELEGÍVEL para aprovação de deleção
    Score 60-84 → SUSPEITO — revisar manualmente
    Score < 60  → MONITORAR — não agir ainda
    """
    score = 40  # Critério base: ausente do Get-HardDisk (sempre verdadeiro aqui)

    if is_shared_datastore:
        score -= 50  # EX-3: datastore compartilhado — alto risco de falso positivo
    else:
        # +10: assume sem lock ativo (vmkfstools -D não disponível via pyVmomi)
        score += 10

        if not folder_has_registered_vm:
            score += 20  # Ausente no .vmx registrado da pasta

        if tipo_zombie == ZombieType.UNREGISTERED_DIR:
            score += 15  # REGRA 2: diretório completamente não registrado

        if tipo_zombie == ZombieType.BROKEN_CHAIN:
            score += 15  # REGRA 4: cadeia corrompida confirmada por naming-convention

    # Penalidade de recência
    if modification is not None:
        now = datetime.now(timezone.utc)
        mod_utc = _utc(modification)
        if mod_utc:
            days_threshold = (
                stale_snapshot_days
                if tipo_zombie == ZombieType.SNAPSHOT_ORPHAN
                else orphan_days
            )
            if (now - mod_utc) < timedelta(days=days_threshold):
                score -= 20  # Arquivo recente — baixa urgência

    return max(5, min(100, score))


# ═══════════════════════════════════════════════════════════════════════════════
# Fase 1 — Coleta de inventário
# ═══════════════════════════════════════════════════════════════════════════════

# Prefixo de pasta do vCenter para Content Library no datastore (fallback EX-6).
_CONTENTLIB_FOLDER_PREFIX = "contentlib-"


def _collect_content_library_paths(content: Any) -> frozenset[str]:
    """
    Coleta prefixos de pasta (normalizados) onde a Content Library do vCenter
    armazena itens (ISOs, VMDKs, etc.). EX-6: VMDKs nessas pastas não são
    referenciados por VMs/templates e devem ser ignorados.

    Tenta usar a API de Content Library (vim.content.LocalLibrary ou equivalente).
    Se a API não estiver disponível ou falhar, retorna frozenset vazio — o
    chamador usa fallback por padrão de nome de pasta ("contentlib-*").
    """
    paths: set[str] = set()
    try:
        # pyVmomi: contentLibraryManager pode não existir em todas as versões/edições
        clm = getattr(content, "contentLibraryManager", None)
        if clm is None:
            return frozenset()

        # Listar bibliotecas locais e obter backing de storage quando disponível
        if hasattr(clm, "listLibrary"):
            libs = clm.listLibrary() or []
        else:
            libs = []

        for lib in libs:
            try:
                # Storage backing pode expor datastore + path (ex.: [ds] contentlib-xxx/)
                storage = getattr(lib, "storage", None)
                if storage is None:
                    continue
                backings = storage if isinstance(storage, list) else [storage]
                for backing in backings:
                    if backing is None:
                        continue
                    # Algumas APIs expõem storageUrl ou datastorePath
                    url = getattr(backing, "storageUrl", None) or getattr(
                        backing, "datastorePath", None
                    )
                    if url and isinstance(url, str):
                        norm = _normalize(url)
                        if norm and norm not in paths:
                            paths.add(norm if norm.endswith("/") else norm + "/")
            except Exception as exc:
                logger.debug(
                    "Content Library: ignorando biblioteca %s — %s",
                    getattr(lib, "name", lib),
                    exc,
                )
    except Exception as exc:
        logger.debug(
            "Content Library API não disponível ou falhou (usando fallback por nome): %s",
            exc,
        )
    return frozenset(paths)


def _collect_fcd_paths(content: Any, datastores: list[Any]) -> frozenset[str]:
    """
    Coleta caminhos de First Class Disks (FCDs/IVDs) — EX-7.
    VMDKs gerenciados via vStorageObjectManager não são orphans.
    Disponível em vSphere 6.5+.
    """
    paths: set[str] = set()
    try:
        vstm = getattr(content, "vStorageObjectManager", None)
        if vstm is None:
            logger.debug("vStorageObjectManager não disponível — FCDs ignorados.")
            return frozenset()
        for ds in datastores:
            try:
                fcd_ids = vstm.ListVStorageObject(ds)
                for fcd_id in fcd_ids or []:
                    try:
                        obj = vstm.RetrieveVStorageObject(fcd_id, ds)
                        fp = getattr(obj.config.backing, "filePath", None)
                        if fp:
                            paths.add(_normalize(fp))
                    except Exception as exc:
                        logger.debug("FCD %s ignorado: %s", fcd_id, exc)
            except Exception as exc:
                logger.debug("FCD listing falhou no DS '%s': %s", ds.name, exc)
    except Exception as exc:
        logger.debug("FCD collection falhou: %s", exc)
    return frozenset(paths)


def _is_content_library_path(
    folder_normalized: str, full_path_normalized: str, content_library_paths: frozenset[str]
) -> bool:
    """
    Retorna True se o arquivo está em pasta de Content Library (EX-6).
    Se content_library_paths foi preenchido pela API, verifica prefixo.
    Caso contrário (fallback), verifica se o nome da pasta contém "contentlib-".
    """
    if content_library_paths:
        for prefix in content_library_paths:
            if full_path_normalized.startswith(prefix) or folder_normalized.startswith(prefix):
                return True
    # Fallback: vCenter usa pastas com prefixo "contentlib-" para Content Library
    if _CONTENTLIB_FOLDER_PREFIX in folder_normalized:
        return True
    return False


def _collect_snapshot_vmdk_paths(
    snapshot_list: list[Any] | None,
    vmdk_paths: set[str],
) -> None:
    """
    Percorre a árvore de snapshots recursivamente e coleta TODOS os caminhos
    de VMDK referenciados (descriptors) em qualquer ponto da cadeia.

    Isso impede que VMDKs de snapshot legítimos sejam classificados como
    zombie mesmo quando a VM usa um delta como disco atual.
    """
    for snap_tree in snapshot_list or []:
        try:
            snap = snap_tree.snapshot
            if snap and snap.config and snap.config.hardware:
                for device in snap.config.hardware.device:
                    if isinstance(device, vim.vm.device.VirtualDisk):
                        backing = device.backing
                        if hasattr(backing, "fileName") and backing.fileName:
                            vmdk_paths.add(_normalize(backing.fileName))
        except Exception as exc:
            logger.debug("Erro ao acessar config de snapshot: %s", exc)

        _collect_snapshot_vmdk_paths(
            getattr(snap_tree, "childSnapshotList", None),
            vmdk_paths,
        )


def _collect_inventory(
    content: Any,
    datacenter: Any,
    vcenter_host: str,
) -> _InventorySnapshot:
    """
    Coleta do inventário do vCenter (escopo do Datacenter):
      - Todos os VMDKs referenciados por VMs e templates (current + snapshots)
      - Todos os arquivos .vmx de VMs/templates registradas
      - Todas as pastas (home directories) de VMs/templates registradas
      - Prefixos de pasta da Content Library (EX-6) para exclusão de candidatos orphan
    """
    vmdk_paths: set[str] = set()
    vmx_paths: set[str] = set()
    vm_folders: set[str] = set()

    content_library_paths = _collect_content_library_paths(content)
    if content_library_paths:
        logger.debug("Content Library: %d prefixo(s) de pasta coletado(s).", len(content_library_paths))

    vm_view = content.viewManager.CreateContainerView(
        datacenter, [vim.VirtualMachine], True
    )
    try:
        for vm in vm_view.view:
            if not vm.config:
                continue

            # ── VMX path e pasta home ────────────────────────────────────────
            vmx_path = getattr(vm.config.files, "vmPathName", None)
            if vmx_path:
                vmx_norm = _normalize(vmx_path)
                vmx_paths.add(vmx_norm)
                vm_folders.add(_extract_folder(vmx_norm))

            # ── VMDKs do hardware atual (inclui discos em estado de snapshot) ─
            for device in vm.config.hardware.device:
                if not isinstance(device, vim.vm.device.VirtualDisk):
                    continue
                backing = device.backing
                if hasattr(backing, "fileName") and backing.fileName:
                    vmdk_paths.add(_normalize(backing.fileName))

                    # Percorre backing chain (parent → grand-parent…)
                    parent = getattr(backing, "parent", None)
                    while parent and hasattr(parent, "fileName"):
                        if parent.fileName:
                            vmdk_paths.add(_normalize(parent.fileName))
                        parent = getattr(parent, "parent", None)

            # ── Cadeia completa de snapshots ─────────────────────────────────
            if vm.snapshot:
                _collect_snapshot_vmdk_paths(
                    vm.snapshot.rootSnapshotList, vmdk_paths
                )
    finally:
        vm_view.Destroy()

    ds_view_fcd = content.viewManager.CreateContainerView(
        datacenter, [vim.Datastore], True
    )
    try:
        fcd_paths = _collect_fcd_paths(content, list(ds_view_fcd.view))
    finally:
        ds_view_fcd.Destroy()

    if fcd_paths:
        logger.info("FCDs coletados: %d arquivo(s) excluídos do scan.", len(fcd_paths))

    logger.debug(
        "Inventário coletado: %d VMDKs, %d VMXs, %d pastas de VM.",
        len(vmdk_paths),
        len(vmx_paths),
        len(vm_folders),
    )
    return _InventorySnapshot(
        vmdk_paths=frozenset(vmdk_paths),
        vmx_paths=frozenset(vmx_paths),
        vm_folders=frozenset(vm_folders),
        content_library_paths=content_library_paths,
        fcd_paths=fcd_paths,
        vcenter_host=vcenter_host,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# Fase 2 — Detecção de datastores compartilhados
# ═══════════════════════════════════════════════════════════════════════════════


def _detect_shared_datastores(content: Any) -> set[str]:
    """
    Retorna nomes de datastores acessíveis a partir de mais de um Datacenter
    dentro do mesmo vCenter.

    EX-3: VMDKs nesses datastores recebem POSSIBLE_FALSE_POSITIVE porque
    a VM que os referencia pode pertencer a um Datacenter diferente do escopo
    atual da varredura.

    Nota: datastores compartilhados entre vCenters distintos não são detectáveis
    via API de um único vCenter — o analista deve verificar manualmente.
    (Broadcom KB 383876)
    """
    ds_datacenter_map: dict[str, set[str]] = {}

    dc_view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.Datacenter], True
    )
    try:
        for dc in dc_view.view:
            ds_view = content.viewManager.CreateContainerView(
                dc, [vim.Datastore], True
            )
            try:
                for ds in ds_view.view:
                    ds_datacenter_map.setdefault(ds.name, set()).add(dc.name)
            finally:
                ds_view.Destroy()
    finally:
        dc_view.Destroy()

    shared = {
        name for name, dcs in ds_datacenter_map.items() if len(dcs) > 1
    }
    if shared:
        logger.info("Datastores compartilhados detectados: %s", shared)
    return shared


# ═══════════════════════════════════════════════════════════════════════════════
# Fase 3 — Varredura de arquivos nos datastores
# ═══════════════════════════════════════════════════════════════════════════════


def _browse_datastore(
    ds: Any,
) -> tuple[list[_FileEntry], dict[str, set[str]], set[str]]:
    """
    Navega num datastore via DatastoreBrowser e coleta:

    - Todos os arquivos .vmdk e .vmx (matchPattern — compatível pyVmomi 8.x)
    - diskExtents derivados por convenção de nomes (VmDiskFileQuery removido no pyVmomi 8.x)

    Retorna:
      entries         → lista de _FileEntry para cada arquivo encontrado
      folder_files    → dict[folder_path → set(lowercase_filenames)]
                        usado para verificar existência de extents/descriptors
      global_files    → set com todos os caminhos normalizados
    """
    entries: list[_FileEntry] = []
    folder_files: dict[str, set[str]] = {}
    global_files: set[str] = set()

    browser = ds.browser
    # pyVmomi 8.x removeu VmDiskFileQuery/VmConfigFileQuery — usa matchPattern
    search_spec = vim.host.DatastoreBrowser.SearchSpec(
        matchPattern=["*.vmdk", "*.vmx", "*.vmxf"],
        details=vim.host.DatastoreBrowser.FileInfo.Details(
            fileType=True,
            fileSize=True,
            modification=True,
        ),
        sortFoldersFirst=False,
    )

    try:
        task = browser.SearchDatastoreSubFolders_Task(
            datastorePath=f"[{ds.name}]",
            searchSpec=search_spec,
        )
        _wait_for_task(task)
    except Exception as exc:
        raise RuntimeError(
            f"DatastoreBrowser falhou no datastore '{ds.name}': {exc}"
        ) from exc

    for folder_result in task.info.result:
        folder = folder_result.folderPath  # ex.: '[ds] vm-folder/'

        if folder not in folder_files:
            folder_files[folder] = set()

        for fi in folder_result.file:
            filename: str = fi.path
            name_lower = filename.lower()
            full_path = f"{folder}{filename}"
            full_path_norm = _normalize(full_path)

            folder_files[folder].add(name_lower)
            global_files.add(full_path_norm)

            is_vmx = name_lower.endswith(".vmx")
            is_ctk = name_lower.endswith("-ctk.vmdk")
            is_flat = name_lower.endswith("-flat.vmdk")
            is_delta = name_lower.endswith("-delta.vmdk")
            is_descriptor = (
                name_lower.endswith(".vmdk")
                and not is_flat
                and not is_delta
                and not is_ctk
            )

            entries.append(
                _FileEntry(
                    folder=folder,
                    name=filename,
                    full_path=full_path,
                    size_bytes=getattr(fi, "fileSize", None),
                    modification=getattr(fi, "modification", None),
                    is_vmx=is_vmx,
                    is_descriptor_vmdk=is_descriptor,
                    is_flat_vmdk=is_flat,
                    is_delta_vmdk=is_delta,
                    is_ctk_vmdk=is_ctk,
                    disk_extents=[],
                )
            )

    return entries, folder_files, global_files


# Executor para rodar _browse_datastore em thread separada e não bloquear o event loop
# quando o fluxo principal for invocado de contexto async (ex.: run_in_executor do scan).
_BROWSE_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix="zombie_browse")


async def _browse_datastore_async(
    ds: Any,
) -> tuple[list[_FileEntry], dict[str, set[str]], set[str]]:
    """
    Versão assíncrona do browse: usa _wait_for_task_async (asyncio.sleep) em vez
    de _wait_for_task (time.sleep), evitando bloquear o event loop. Mesma
    semântica e retorno de _browse_datastore.
    """
    entries: list[_FileEntry] = []
    folder_files: dict[str, set[str]] = {}
    global_files: set[str] = set()

    browser = ds.browser
    search_spec = vim.host.DatastoreBrowser.SearchSpec(
        matchPattern=["*.vmdk", "*.vmx", "*.vmxf"],
        details=vim.host.DatastoreBrowser.FileInfo.Details(
            fileType=True,
            fileSize=True,
            modification=True,
        ),
        sortFoldersFirst=False,
    )

    try:
        task = browser.SearchDatastoreSubFolders_Task(
            datastorePath=f"[{ds.name}]",
            searchSpec=search_spec,
        )
        await _wait_for_task_async(task)
    except Exception as exc:
        raise RuntimeError(
            f"DatastoreBrowser falhou no datastore '{ds.name}': {exc}"
        ) from exc

    for folder_result in task.info.result:
        folder = folder_result.folderPath

        if folder not in folder_files:
            folder_files[folder] = set()

        for fi in folder_result.file:
            filename = fi.path
            name_lower = filename.lower()
            full_path = f"{folder}{filename}"
            full_path_norm = _normalize(full_path)

            folder_files[folder].add(name_lower)
            global_files.add(full_path_norm)

            is_vmx = name_lower.endswith(".vmx")
            is_ctk = name_lower.endswith("-ctk.vmdk")
            is_flat = name_lower.endswith("-flat.vmdk")
            is_delta = name_lower.endswith("-delta.vmdk")
            is_descriptor = (
                name_lower.endswith(".vmdk")
                and not is_flat
                and not is_delta
                and not is_ctk
            )

            entries.append(
                _FileEntry(
                    folder=folder,
                    name=filename,
                    full_path=full_path,
                    size_bytes=getattr(fi, "fileSize", None),
                    modification=getattr(fi, "modification", None),
                    is_vmx=is_vmx,
                    is_descriptor_vmdk=is_descriptor,
                    is_flat_vmdk=is_flat,
                    is_delta_vmdk=is_delta,
                    is_ctk_vmdk=is_ctk,
                    disk_extents=[],
                )
            )

    return entries, folder_files, global_files


# ═══════════════════════════════════════════════════════════════════════════════
# Fase 4 — Regras de detecção
# ═══════════════════════════════════════════════════════════════════════════════


# Threshold para distinguir descriptor de texto de VMDK monolítico.
# Descriptors VMware têm tipicamente 300–800 bytes.
# VMDKs monolíticos (dados embutidos, sem -flat separado) são sempre >> 1 MB.
_MONOLITHIC_THRESHOLD_BYTES = 1 * 1024 * 1024  # 1 MB


def _has_broken_chain(
    entry: _FileEntry,
    folder_files: dict[str, set[str]],
    global_files: set[str],
    ds_type: str,
) -> bool:
    """
    REGRA 4: Verifica se o descriptor VMDK aponta para um extent inexistente.

    Equivalente Python de: vmkfstools -q /vmfs/volumes/<ds>/<folder>/<file>.vmdk

    ── Tipos de arquivo .vmdk ──────────────────────────────────────────────────
    1. Descriptor de texto (~300-800 bytes):
       Contém apenas metadados + referência para -flat.vmdk.
       Se -flat.vmdk ausente → BROKEN_CHAIN.

    2. VMDK monolítico (tamanho = capacidade do disco, tipicamente ≥ 1 MB):
       Dados embutidos no próprio arquivo — não existe -flat.vmdk separado.
       Não deve ser verificado como broken chain; vai para ORPHANED.

    Estratégia:
    1. Se tamanho > 1 MB → VMDK monolítico → não é broken chain (return False).
    2. diskExtents explícitos do VmDiskFileInfo (pyVmomi < 8.x): mais preciso.
    3. Naming-convention fallback (somente VMFS/vSAN):
         vm.vmdk        → extent esperado: vm-flat.vmdk  (mesma pasta)
         vm-000001.vmdk → extent esperado: vm-000001-delta.vmdk
    """
    # Caso 1: VMDK monolítico (dados embutidos) — sem -flat separado
    if (
        entry.size_bytes is not None
        and entry.size_bytes > _MONOLITHIC_THRESHOLD_BYTES
    ):
        logger.debug(
            "VMDK monolítico (%.1f MB) — sem -flat esperado: %s",
            entry.size_bytes / (1024 * 1024),
            entry.full_path,
        )
        return False  # Não é broken chain; avança para ORPHANED/UNREGISTERED_DIR

    # Método 2: diskExtents explícitos (pyVmomi < 8.x)
    if entry.disk_extents:
        for extent_path in entry.disk_extents:
            if _normalize(extent_path) not in global_files:
                logger.debug(
                    "BROKEN_CHAIN (diskExtents): '%s' → extent ausente '%s'",
                    entry.full_path,
                    extent_path,
                )
                return True
        return False

    # Método 2: naming convention (somente VMFS/vSAN para evitar FP em NFS)
    if ds_type.upper() not in _SPLIT_VMDK_DS_TYPES:
        return False

    folder_f = folder_files.get(entry.folder, set())
    name_lower = entry.name.lower()

    # Snapshot descriptor: vm-000001.vmdk → vm-000001-delta.vmdk
    if _SNAPSHOT_DESCRIPTOR_RE.search(name_lower):
        expected_extent = name_lower[:-5] + "-delta.vmdk"
    else:
        # Descriptor padrão: vm.vmdk → vm-flat.vmdk
        expected_extent = name_lower[:-5] + "-flat.vmdk"

    if expected_extent not in folder_f:
        logger.debug(
            "BROKEN_CHAIN (convention): '%s' → extent esperado '%s' ausente.",
            entry.full_path,
            expected_extent,
        )
        return True

    return False


def _skip(entry: _FileEntry, reason: str) -> None:
    """Log estruturado de descarte de VMDK (nível DEBUG)."""
    logger.debug("SKIP [%s] motivo=%r", entry.full_path, reason)


def _classify_vmdk(
    entry: _FileEntry,
    inventory: _InventorySnapshot,
    shared_datastores: set[str],
    folder_files: dict[str, set[str]],
    global_files: set[str],
    datacenter_name: str,
    datastore_name: str,
    ds_type: str,
    orphan_days: int,
    stale_snapshot_days: int,
    min_file_size_mb: int,
) -> "ZombieVmdkResult | tuple[None, str, list[str]]":
    # READ-ONLY: no write operations
    evlog: list[str] = []  # trilha de auditoria deste arquivo

    if entry.is_vmx:
        _skip(entry, "vmx")
        evlog.append("SKIP: arquivo .vmx ignorado")
        return (None, "vmx", evlog)

    name_lower = entry.name.lower()

    # FALSOS POSITIVOS — EXCLUIR SEMPRE DO SCAN:
    if (
        name_lower.endswith("-flat.vmdk")
        or name_lower.endswith("-delta.vmdk")
        or name_lower.endswith("-sesparse.vmdk")
        or name_lower.endswith("-ctk.vmdk")
    ):
        _skip(entry, "suffix_exclusion")
        suffix = name_lower.rsplit("-", 1)[-1] if "-" in name_lower else name_lower
        evlog.append(f"SKIP: sufixo excluído (-{suffix})")
        return (None, "suffix_exclusion", evlog)

    # EX-4: vCLS (vSphere Cluster Services) — sempre ignorar
    if _VCLS_RE.match(entry.name):
        _skip(entry, "vcls")
        evlog.append("SKIP: vCLS (vSphere Cluster Services)")
        return (None, "vcls", evlog)

    if (
        entry.size_bytes is not None
        and entry.size_bytes < (min_file_size_mb * 1024 * 1024)
        and not entry.is_delta_vmdk
        and not entry.is_descriptor_vmdk
    ):
        _skip(entry, "tamanho")
        size_mb = entry.size_bytes / (1024 * 1024)
        evlog.append(f"SKIP: tamanho {size_mb:.1f} MB < min_file_size_mb({min_file_size_mb})")
        return (None, "tamanho", evlog)

    # Rejeitar arquivos modificados dentro do período de graça
    if entry.modification is not None:
        mod_utc = _utc(entry.modification)
        if mod_utc:
            now = datetime.now(timezone.utc)
            threshold_days = (
                stale_snapshot_days
                if ("-000" in name_lower or "snap" in name_lower or "snapshot" in name_lower)
                else orphan_days
            )
            age_days = (now - mod_utc).days
            if age_days < threshold_days:
                size_gb = (entry.size_bytes or 0) / (1024**3)
                log_fn = logger.warning if size_gb > 100 else logger.debug
                log_fn(
                    "SKIP (recente %d dias < %d) tamanho=%.1f GB: %s",
                    age_days, threshold_days, size_gb, entry.full_path,
                )
                _skip(entry, "recente")
                evlog.append(
                    f"FAIL_SKIP: modificação há {age_days} dias < orphan_days({threshold_days})"
                )
                return (None, "recente", evlog)
            else:
                evlog.append(
                    f"PASS: modificação há {age_days} dias ≥ orphan_days({threshold_days})"
                )
    else:
        evlog.append("PASS: data de modificação desconhecida (sem filtro de recência)")

    if entry.size_bytes is not None:
        size_gb_log = entry.size_bytes / (1024 ** 3)
        evlog.append(f"PASS: tamanho {size_gb_log:.2f} GB ≥ min_file_size_mb({min_file_size_mb})")

    # Normalizar o caminho para comparação
    norm_path = _normalize(entry.full_path)

    # Comparar cada VMDK encontrado com os caminhos registrados
    if norm_path in inventory.vmdk_paths:
        _skip(entry, "inventario")
        evlog.append("SKIP: encontrado em inventory.vmdk_paths (VM/template ativo)")
        return (None, "inventario", evlog)
    evlog.append("PASS: não está em inventory.vmdk_paths")

    if norm_path in inventory.fcd_paths:
        _skip(entry, "fcd")
        evlog.append("SKIP: encontrado em inventory.fcd_paths (FCD/IVD gerenciado)")
        return (None, "fcd", evlog)
    evlog.append("PASS: não está em inventory.fcd_paths")

    folder_norm = _normalize(entry.folder)
    if _is_content_library_path(folder_norm, norm_path, inventory.content_library_paths):
        _skip(entry, "content_library")
        evlog.append("SKIP: pasta pertence à Content Library (EX-6)")
        return (None, "content_library", evlog)
    evlog.append("PASS: não está em content_library")

    # Tipos e Motivos baseados nas regras SCAN-REGRAS-VMDK
    is_backup_artifact = "backup" in name_lower or "veeam" in name_lower or "pre-" in name_lower
    is_snapshot_leftover = "-000" in name_lower or "snap" in name_lower or "snapshot" in name_lower

    folder_has_registered_vm = folder_norm in inventory.vm_folders

    # EX-3: datastore compartilhado → POSSIBLE_FALSE_POSITIVE (Broadcom KB 383876)
    is_shared_datastore = datastore_name in shared_datastores
    false_positive_reason_val: str | None = None

    # Inferência de motivo e tipo remapeado para banco de dados legado
    if is_shared_datastore:
        mapped_tipo = ZombieType.POSSIBLE_FALSE_POSITIVE
        reason = "Datastore compartilhado entre múltiplos vCenters (EX-3)"
        false_positive_reason_val = _CAUSES_BY_TYPE["POSSIBLE_FALSE_POSITIVE"][0]
        evlog.append("WARN: datastore compartilhado multi-vCenter → POSSIBLE_FALSE_POSITIVE (EX-3)")
    elif not folder_has_registered_vm:
        reason = "VM removida do inventário mas arquivos não foram deletados"
        mapped_tipo = ZombieType.UNREGISTERED_DIR  # Mais próximo do legado para "pasta inteira órfã"
        evlog.append("PASS: pasta sem VM registrada no vCenter → UNREGISTERED_DIR")
    elif is_backup_artifact:
        reason = "Possível artefato de backup"
        mapped_tipo = ZombieType.ORPHANED
        evlog.append("INFO: nome sugere artefato de backup → ORPHANED")
    else:
        reason = "Arquivo VMDK sem VM associada no inventário"
        if _has_broken_chain(entry, folder_files, global_files, ds_type):
            mapped_tipo = ZombieType.BROKEN_CHAIN
            evlog.append("PASS: extent esperado ausente no datastore → BROKEN_CHAIN")
        elif is_snapshot_leftover:
            mapped_tipo = ZombieType.SNAPSHOT_ORPHAN
            evlog.append("PASS: nome sugere snapshot ativo ausente → SNAPSHOT_ORPHAN")
        else:
            mapped_tipo = ZombieType.ORPHANED
            evlog.append("PASS: sem VM associada no inventário → ORPHANED")

    # Construir score e entry final
    score = _compute_confidence_score(
        tipo_zombie=mapped_tipo,
        folder_has_registered_vm=folder_has_registered_vm,
        is_shared_datastore=is_shared_datastore,
        modification=_utc(entry.modification),
        orphan_days=orphan_days,
        stale_snapshot_days=stale_snapshot_days,
    )
    evlog.append(f"CLASSIFIED: {mapped_tipo.value} | score={score}")

    # Construir e retornar ScanResult com os órfãos detectados
    return ZombieVmdkResult(
        path=entry.full_path,
        datastore=datastore_name,
        tamanho_gb=_bytes_to_gb(entry.size_bytes),
        ultima_modificacao=_utc(entry.modification),
        tipo_zombie=mapped_tipo,
        vcenter_host=inventory.vcenter_host,
        datacenter=datacenter_name,
        detection_rules=[
            "1. Obtido todas as VMs",
            "2. Montado conjunto de VMDKs em uso",
            "3. Discos de VMs orphaned incluídos",
            "4. Varridos datastores",
            "5. Comparação case-insensitive",
            "6. Falsos positivos excluídos",
            "7. FCDs/IVDs excluídos (vStorageObjectManager)",
        ],
        likely_causes=[reason],
        false_positive_reason=false_positive_reason_val,
        folder=entry.folder,
        datastore_type=ds_type,
        confidence_score=score,
        evidence_log=evlog,
    )

def _find_datacenter(content: Any, name: str) -> Any:
    """Localiza o objeto vim.Datacenter pelo nome. Levanta ValueError se não encontrar."""
    dc_view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.Datacenter], True
    )
    try:
        for dc in dc_view.view:
            if dc.name == name:
                return dc
    finally:
        dc_view.Destroy()
    raise ValueError(
        f"Datacenter '{name}' não encontrado no vCenter. "
        "Verifique o nome e as permissões da conta."
    )


def _scan_datacenter_sync(
    service_instance: Any,
    datacenter_name: str,
    orphan_days: int = 60,
    stale_snapshot_days: int = 15,
    min_file_size_mb: int = 50,
    progress_callback: Callable[[str, str, dict], None] | None = None,
    extra_inventories: "list[_InventorySnapshot] | None" = None,
) -> tuple[list[ZombieVmdkResult], list[DatastoreScanMetric]]:
    """
    Núcleo síncrono da varredura. Deve ser invocado via run_in_executor para
    não bloquear o event loop do FastAPI.

    Fluxo:
      1. Coleta inventário (VMs + templates + snapshot chains)
      2. Detecta datastores compartilhados (EX-3)
      3. Para cada datastore do Datacenter: navega com DatastoreBrowser
      4. Aplica fluxo de validação de 7 passos Broadcom em cada arquivo VMDK

    Returns:
        (lista de ZombieVmdkResult, lista de DatastoreScanMetric por datastore)

    Args:
        progress_callback: função opcional chamada em cada etapa significativa.
            Assinatura: callback(level, message, extra_data)
            level: "info" | "warning" | "error" | "success"
        extra_inventories: lista opcional de _InventorySnapshot de outros vCenters
            cadastrados no sistema. Se um VMDK candidato for encontrado em qualquer
            desses inventários, ele é marcado como POSSIBLE_FALSE_POSITIVE
            (Broadcom KB 383876 — datastores compartilhados entre vCenters).
    """
    def _cb(level: str, msg: str, **extra: Any) -> None:
        """Emite progresso via logger E via callback externo."""
        getattr(logger, level if level != "success" else "info")(
            "[%s] %s", datacenter_name, msg
        )
        if progress_callback:
            progress_callback(level, msg, extra)

    content = service_instance.RetrieveContent()

    try:
        vcenter_host: str = service_instance._stub.host
    except Exception:
        vcenter_host = content.about.name or "unknown"

    _cb("info", "Iniciando varredura de VMDKs zombie.")

    datacenter = _find_datacenter(content, datacenter_name)

    _cb("info", f"Coletando inventário do vCenter ({datacenter_name})…")
    inventory = _collect_inventory(content, datacenter, vcenter_host)
    _cb(
        "success",
        f"Inventário coletado: {len(inventory.vmdk_paths)} VMDKs em uso, "
        f"{len(inventory.vmx_paths)} VMs/templates registradas.",
        vmdk_count=len(inventory.vmdk_paths),
        vm_count=len(inventory.vmx_paths),
    )

    shared_datastores = _detect_shared_datastores(content)
    if shared_datastores:
        _cb(
            "warning",
            f"Datastores compartilhados detectados ({len(shared_datastores)}): "
            + ", ".join(sorted(shared_datastores)),
        )

    ds_view = content.viewManager.CreateContainerView(
        datacenter, [vim.Datastore], True
    )
    results: list[ZombieVmdkResult] = []

    try:
        datastores = list(ds_view.view)
    finally:
        ds_view.Destroy()

    total_ds = len(datastores)
    _cb("info", f"{total_ds} datastores encontrados para varrer.")

    vcenter_instance_uuid = getattr(content.about, "instanceUuid", "") or ""

    metrics: list[DatastoreScanMetric] = []
    # Acumuladores globais para scan_summary JSON ao final da sessão
    global_skips: dict[str, int] = {
        "recente": 0,
        "tamanho_minimo": 0,
        "in_inventario": 0,
        "fcd": 0,
        "content_library": 0,
        "suffix_exclusion": 0,
        "vcls": 0,
    }
    total_files_browsed = 0

    for ds_idx, ds in enumerate(datastores, start=1):
        ds_name = ds.name
        ds_type = getattr(ds.summary, "type", "UNKNOWN")
        scan_start_time = datetime.now(timezone.utc)

        if not ds.summary.accessible:
            _cb(
                "warning",
                f"[{ds_idx}/{total_ds}] Datastore '{ds_name}' inacessível — pulando.",
                ds_name=ds_name, ds_index=ds_idx, ds_total=total_ds,
                ds_status="inaccessible",
            )
            metrics.append(
                DatastoreScanMetric(
                    datastore_name=ds_name,
                    scan_start_time=scan_start_time,
                    scan_duration_seconds=0.0,
                    files_found=0,
                    zombies_found=0,
                )
            )
            continue

        _cb(
            "info",
            f"[{ds_idx}/{total_ds}] Varrendo datastore '{ds_name}' (tipo: {ds_type})…",
            ds_name=ds_name, ds_index=ds_idx, ds_total=total_ds,
            ds_type=ds_type, ds_status="scanning",
        )

        # Executa browse em thread separada para não bloquear o event loop
        # quando o scan é invocado via run_in_executor (TAREFA 2).
        try:
            browse_timeout = get_settings().scan_datastore_timeout_sec + 60
            future = _BROWSE_EXECUTOR.submit(_browse_datastore, ds)
            entries, folder_files, global_files = future.result(timeout=browse_timeout)
        except Exception as exc:
            _cb(
                "error",
                f"[{ds_idx}/{total_ds}] Falha em '{ds_name}': {exc}",
                ds_name=ds_name, ds_index=ds_idx, ds_total=total_ds,
                ds_status="failed", error=str(exc),
            )
            duration_sec = (datetime.now(timezone.utc) - scan_start_time).total_seconds()
            metrics.append(
                DatastoreScanMetric(
                    datastore_name=ds_name,
                    scan_start_time=scan_start_time,
                    scan_duration_seconds=round(duration_sec, 2),
                    files_found=0,
                    zombies_found=0,
                )
            )
            continue

        ds_zombies_before = len(results)
        total_files_browsed += len(entries)

        # Contadores de skip por motivo (por datastore)
        skips_recente = skips_tamanho = skips_inventario = skips_fcd = 0
        skips_suffix = skips_vmx = 0

        # Contagem detalhada de tipos de arquivo encontrados
        n_total      = len(entries)
        n_descriptor = sum(1 for e in entries if e.is_descriptor_vmdk)
        n_delta      = sum(1 for e in entries if e.is_delta_vmdk)
        n_flat       = sum(1 for e in entries if e.is_flat_vmdk)
        n_ctk        = sum(1 for e in entries if e.is_ctk_vmdk)
        n_vmx        = sum(1 for e in entries if e.is_vmx)

        datastore_moref = getattr(ds, "_moId", "") or ""

        for entry in entries:
            out = _classify_vmdk(
                entry=entry,
                inventory=inventory,
                shared_datastores=shared_datastores,
                folder_files=folder_files,
                global_files=global_files,
                datacenter_name=datacenter_name,
                datastore_name=ds_name,
                ds_type=ds_type,
                orphan_days=orphan_days,
                stale_snapshot_days=stale_snapshot_days,
                min_file_size_mb=min_file_size_mb,
            )
            if isinstance(out, tuple):
                # Desempacota o 3-tuple (None, reason, evlog)
                _, reason, _skip_evlog = out
                if reason == "recente":
                    skips_recente += 1
                    global_skips["recente"] += 1
                elif reason == "tamanho":
                    skips_tamanho += 1
                    global_skips["tamanho_minimo"] += 1
                elif reason == "inventario":
                    skips_inventario += 1
                    global_skips["in_inventario"] += 1
                elif reason == "fcd":
                    skips_fcd += 1
                    global_skips["fcd"] += 1
                elif reason == "suffix_exclusion":
                    skips_suffix += 1
                    global_skips["suffix_exclusion"] += 1
                elif reason == "content_library":
                    global_skips["content_library"] += 1
                elif reason == "vcls":
                    global_skips["vcls"] += 1
                elif reason == "vmx":
                    skips_vmx += 1
                if _skip_evlog and logger.isEnabledFor(logging.DEBUG):
                    logger.debug("SKIP evlog [%s]: %s", reason, _skip_evlog)
                continue
            zombie = out
            if zombie:
                # Extrai pasta e arquivo do path "[ds] folder/file.vmdk"
                vmdk_path = zombie.path
                if "] " in vmdk_path:
                    path_part = vmdk_path.split("] ", 1)[1].strip()
                    vmdk_folder, vmdk_filename = (
                        path_part.rsplit("/", 1) if "/" in path_part else ("", path_part)
                    )
                else:
                    vmdk_folder, vmdk_filename = "", ""

                deeplink_ui = ""
                if vcenter_instance_uuid and datastore_moref:
                    deeplink_ui = generate_vsphere_ui_link(
                        vcenter_host, vcenter_instance_uuid, datastore_moref
                    )
                deeplink_folder = generate_folder_deeplink(
                    vcenter_host, datacenter_name, ds_name, vmdk_path, link_to_file=True
                )
                deeplink_folder_dir = generate_folder_deeplink(
                    vcenter_host, datacenter_name, ds_name, vmdk_path, link_to_file=False
                )

                zombie = replace(
                    zombie,
                    vcenter_deeplink_ui=deeplink_ui,
                    vcenter_deeplink_folder=deeplink_folder,
                    vcenter_deeplink_folder_dir=deeplink_folder_dir,
                    datacenter_path=datacenter_name,
                    datastore_name=ds_name,
                    vmdk_folder=vmdk_folder,
                    vmdk_filename=vmdk_filename,
                )

                # READ-ONLY: cross-vCenter check — no write operations
                # Se extra_inventories fornecido, verifica se o candidato está em
                # uso por algum outro vCenter cadastrado no sistema (KB 383876).
                if extra_inventories:
                    norm_candidate = _normalize(zombie.path)
                    for ext_inv in extra_inventories:
                        if norm_candidate in ext_inv.vmdk_paths:
                            logger.info(
                                "Cross-vCenter FP: '%s' encontrado em vCenter '%s'",
                                zombie.path, ext_inv.vcenter_host,
                            )
                            zombie = replace(
                                zombie,
                                tipo_zombie=ZombieType.POSSIBLE_FALSE_POSITIVE,
                                false_positive_reason=(
                                    f"VMDK referenciado em inventário de outro vCenter "
                                    f"cadastrado no sistema ({ext_inv.vcenter_host})"
                                ),
                                confidence_score=max(
                                    5,
                                    zombie.confidence_score - 50,
                                ),
                                evidence_log=zombie.evidence_log + [
                                    f"OVERRIDE: encontrado em inventário de {ext_inv.vcenter_host} "
                                    f"→ POSSIBLE_FALSE_POSITIVE (KB 383876)"
                                ],
                            )
                            break  # basta um match para classificar como FP

                results.append(zombie)

        found_here = len(results) - ds_zombies_before
        n_analisados = n_total

        logger.info(
            "DS '%s' resumo: total=%d analisados=%d zombies=%d skips={recente=%d, tamanho=%d, inventario=%d, fcd=%d}",
            ds_name, n_total, n_analisados, found_here,
            skips_recente, skips_tamanho, skips_inventario, skips_fcd,
        )

        # Monta descrição detalhada dos arquivos encontrados
        if n_total == 0:
            detail = "nenhum arquivo .vmdk/.vmx encontrado (datastore vazio ou não-VMFS)"
        else:
            parts = []
            if n_descriptor: parts.append(f"{n_descriptor} descriptor(s)")
            if n_delta:      parts.append(f"{n_delta} delta(s)")
            if n_flat:       parts.append(f"{n_flat} flat(s)")
            if n_ctk:        parts.append(f"{n_ctk} ctk(s)")
            if n_vmx:        parts.append(f"{n_vmx} vmx(s)")
            detail = ", ".join(parts) if parts else "somente arquivos sem VMDKs relevantes"

        if n_descriptor == 0 and n_delta == 0 and n_total > 0:
            # Explica por que 0 VMDKs foram analisados
            if n_flat > 0 and n_vmx == 0:
                detail += " — somente flat sem descriptors (VMs ativas thick-provisioned ou flats órfãs)"
            elif n_vmx > 0 and n_flat > 0:
                detail += " — VMs registradas sem descriptors visíveis (provável uso de RAW/RDM)"

        _cb(
            "success" if found_here > 0 else "info",
            f"[{ds_idx}/{total_ds}] '{ds_name}': {detail}"
            + (f" → {found_here} zombie(s) encontrado(s)" if found_here > 0 else " → nenhum zombie"),
            ds_name=ds_name, ds_index=ds_idx, ds_total=total_ds,
            ds_status="done",
            n_total=n_total, n_descriptor=n_descriptor, n_delta=n_delta,
            n_flat=n_flat, n_ctk=n_ctk, n_vmx=n_vmx,
            zombies_found=found_here,
        )

        duration_sec = (datetime.now(timezone.utc) - scan_start_time).total_seconds()
        metrics.append(
            DatastoreScanMetric(
                datastore_name=ds_name,
                scan_start_time=scan_start_time,
                scan_duration_seconds=round(duration_sec, 2),
                files_found=len(entries),
                zombies_found=found_here,
            )
        )

    _cb(
        "success",
        f"Varredura concluída: {len(results)} VMDKs zombie detectados em {datacenter_name}.",
        total_zombies=len(results),
    )

    scan_summary = {
        "scan_summary": {
            "datacenter": datacenter_name,
            "total_files_browsed": total_files_browsed,
            "zombies_found": len(results),
            "skips": {
                "recente": global_skips["recente"],
                "tamanho_minimo": global_skips["tamanho_minimo"],
                "in_inventario": global_skips["in_inventario"],
                "fcd": global_skips["fcd"],
                "content_library": global_skips["content_library"],
                "suffix_exclusion": global_skips["suffix_exclusion"],
                "vcls": global_skips["vcls"],
            },
        },
    }
    logger.info("SCAN_SUMMARY_JSON %s", json.dumps(scan_summary, ensure_ascii=False))
    # Em modo DEBUG, inclui evidence_log de cada zombie no log (somente para diagnóstico)
    if logger.isEnabledFor(logging.DEBUG) and results:
        for _z in results:
            if _z.evidence_log:
                logger.debug(
                    "EVIDENCE [%s] %s",
                    _z.path,
                    " → ".join(_z.evidence_log),
                )

    return results, metrics


# ═══════════════════════════════════════════════════════════════════════════════
# Ponto de entrada público (assíncrono)
# ═══════════════════════════════════════════════════════════════════════════════


async def scan_datacenter(
    service_instance: Any,
    datacenter_name: str,
    orphan_days: int = 60,
    stale_snapshot_days: int = 15,
    min_file_size_mb: int = 50,
    progress_callback: Callable[[str, str, dict], None] | None = None,
    extra_inventories: "list[_InventorySnapshot] | None" = None,
) -> tuple[list[ZombieVmdkResult], list[DatastoreScanMetric]]:
    """
    Executa a varredura completa de VMDKs zombie em um Datacenter do vCenter.

    Todas as chamadas pyVmomi (bloqueantes) são executadas em ThreadPoolExecutor
    para não bloquear o event loop do FastAPI.

    Para ambientes com datastores compartilhados entre múltiplos vCenters,
    fornecer extra_inventories com os snapshots de inventário dos demais vCenters
    para evitar falsos positivos (Broadcom KB 383876). Cada entrada em
    extra_inventories é um _InventorySnapshot obtido via _collect_inventory()
    conectado ao vCenter correspondente.

    Args:
        service_instance:    vim.ServiceInstance conectado ao vCenter.
        datacenter_name:     Nome exato do Datacenter no inventário do vCenter.
        orphan_days:         Dias mínimos sem referência para VMDK normal (padrão: 60).
        stale_snapshot_days: Dias mínimos para snapshot orphan ser reportado (padrão: 15).
        min_file_size_mb:    Tamanho mínimo para reportar (exceto BROKEN_CHAIN, padrão: 50).
        extra_inventories:   Snapshots de inventário de outros vCenters cadastrados.
                             VMDKs encontrados nesses inventários são marcados como
                             POSSIBLE_FALSE_POSITIVE (KB 383876).

    Returns:
        (lista de ZombieVmdkResult, lista de DatastoreScanMetric por datastore).

    Raises:
        ValueError:   Datacenter não encontrado.
        RuntimeError: Falha no DatastoreBrowser.
        TimeoutError: Tarefa do vCenter excedeu o timeout.
    """
    import functools
    loop = asyncio.get_event_loop()
    fn = functools.partial(
        _scan_datacenter_sync,
        service_instance,
        datacenter_name,
        orphan_days,
        stale_snapshot_days,
        min_file_size_mb,
        progress_callback,
        extra_inventories,
    )
    return await loop.run_in_executor(None, fn)
