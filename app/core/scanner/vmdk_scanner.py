"""
Motor de varredura de VMDKs zombie/orphaned.

Algoritmo:
1. Coleta todos os VMDKs registrados em cada Datastore.
2. Coleta todos os VMDKs referenciados por VMs ativas.
3. A diferença (datastores - VMs) são candidatos orphaned/zombie.
4. Classifica por tempo de última modificação:
   - < threshold_days  → orphaned (recente, pode ser snapshot ou migração)
   - >= threshold_days → zombie   (antigo, muito provável candidato a remoção)
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import NamedTuple

from pyVmomi import vim

from app.core.vcenter.client import VCenterClient
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


class VMDKInfo(NamedTuple):
    datastore_name: str
    datastore_url: str | None
    vmdk_path: str
    size_gb: float | None
    vm_name: str | None
    vm_moref: str | None
    last_modified: datetime | None
    days_since_modified: int | None
    status: str  # "attached" | "orphaned" | "zombie"


# ---------------------------------------------------------------------------
# Helpers internos
# ---------------------------------------------------------------------------

def _collect_vm_vmdk_paths(client: VCenterClient) -> dict[str, tuple[str, str]]:
    """
    Retorna um mapa de caminho-vmdk → (nome-da-vm, moref) para todas as VMs.
    O caminho é normalizado para comparação (minúsculas, barras unificadas).
    """
    vm_vmdks: dict[str, tuple[str, str]] = {}

    view = client.get_container_view([vim.VirtualMachine])
    try:
        for vm in view.view:
            if not vm.config:
                continue
            for device in vm.config.hardware.device:
                if not isinstance(device, vim.vm.device.VirtualDisk):
                    continue
                backing = device.backing
                if hasattr(backing, "fileName"):
                    path = _normalize_path(backing.fileName)
                    vm_vmdks[path] = (vm.name, str(vm._moId))
    finally:
        view.Destroy()

    return vm_vmdks


def _collect_datastore_vmdks(
    client: VCenterClient,
) -> list[tuple[vim.Datastore, str, vim.host.DatastoreBrowser.FileInfo | None]]:
    """
    Navega pelo browser de cada Datastore e coleta todos os arquivos .vmdk.
    Retorna lista de (datastore, caminho_completo, file_info_ou_None).
    """
    results = []
    view = client.get_container_view([vim.Datastore])

    try:
        for ds in view.view:
            if not ds.summary.accessible:
                logger.warning("Datastore '%s' inacessível. Pulando.", ds.name)
                continue

            browser = ds.browser
            search_spec = vim.host.DatastoreBrowser.SearchSpec(
                matchPattern=["*.vmdk"],
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
                # Aguarda conclusão da tarefa (bloqueante - chamado em thread)
                _wait_for_task(task)

                for result in task.info.result:
                    folder_path = result.folderPath
                    for fi in result.file:
                        # Ignora flat/delta VMDKs (apenas descritores)
                        if fi.path.endswith("-flat.vmdk") or fi.path.endswith(
                            "-delta.vmdk"
                        ):
                            continue
                        full_path = f"{folder_path}{fi.path}"
                        results.append((ds, full_path, fi))
            except Exception as exc:
                logger.error(
                    "Erro ao navegar no Datastore '%s': %s", ds.name, exc
                )
    finally:
        view.Destroy()

    return results


def _wait_for_task(task: vim.Task, timeout: int = 120) -> None:
    """Bloqueia até a tarefa do vCenter completar ou o timeout expirar."""
    import time

    elapsed = 0
    while task.info.state not in (
        vim.TaskInfo.State.success,
        vim.TaskInfo.State.error,
    ):
        time.sleep(1)
        elapsed += 1
        if elapsed >= timeout:
            raise TimeoutError(f"Task {task} ultrapassou {timeout}s.")

    if task.info.state == vim.TaskInfo.State.error:
        raise RuntimeError(task.info.error.msg)


def _normalize_path(path: str) -> str:
    return path.strip().lower().replace("\\", "/")


def _file_size_to_gb(size_bytes: int | None) -> float | None:
    if size_bytes is None:
        return None
    return round(size_bytes / (1024**3), 3)


def _days_since(dt: datetime | None) -> int | None:
    if dt is None:
        return None
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (now - dt).days


# ---------------------------------------------------------------------------
# Função pública principal
# ---------------------------------------------------------------------------

def scan_vmdks(client: VCenterClient) -> list[VMDKInfo]:
    """
    Executa a varredura completa de VMDKs em um vCenter.
    Deve ser chamado dentro de um executor de thread (não é coroutine).
    """
    threshold = settings.orphaned_threshold_days

    logger.info("Iniciando coleta de VMDKs referenciados por VMs...")
    vm_vmdks = _collect_vm_vmdk_paths(client)
    logger.info("VMs referenciando %d VMDKs.", len(vm_vmdks))

    logger.info("Iniciando navegação nos Datastores...")
    ds_vmdks = _collect_datastore_vmdks(client)
    logger.info("Total de VMDKs nos Datastores: %d.", len(ds_vmdks))

    vmdk_list: list[VMDKInfo] = []

    for ds, full_path, file_info in ds_vmdks:
        normalized = _normalize_path(full_path)
        vm_entry = vm_vmdks.get(normalized)

        last_modified: datetime | None = None
        size_gb: float | None = None

        if file_info:
            last_modified = getattr(file_info, "modification", None)
            size_gb = _file_size_to_gb(getattr(file_info, "fileSize", None))

        days = _days_since(last_modified)

        if vm_entry:
            vm_name, vm_moref = vm_entry
            status = "attached"
        else:
            vm_name = None
            vm_moref = None
            if days is not None and days >= threshold:
                status = "zombie"
            else:
                status = "orphaned"

        ds_url: str | None = None
        try:
            ds_url = ds.summary.url
        except Exception as exc:
            logger.debug(
                "Nao foi possivel ler datastore_url para datastore '%s': %s",
                getattr(ds, "name", "unknown"),
                exc.__class__.__name__,
            )

        vmdk_list.append(
            VMDKInfo(
                datastore_name=ds.name,
                datastore_url=ds_url,
                vmdk_path=full_path,
                size_gb=size_gb,
                vm_name=vm_name,
                vm_moref=vm_moref,
                last_modified=last_modified,
                days_since_modified=days,
                status=status,
            )
        )

    return vmdk_list


async def scan_vmdks_async(client: VCenterClient) -> list[VMDKInfo]:
    """Versão assíncrona — executa scan_vmdks em thread pool."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, scan_vmdks, client)
