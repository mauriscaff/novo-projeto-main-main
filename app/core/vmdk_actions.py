"""
Lógica de execução de ações sobre VMDKs detectados como zombie.

Ações suportadas: QUARANTINE | DELETE

Cada função pública é assíncrona mas delega as chamadas bloqueantes do pyVmomi
para um ThreadPoolExecutor via `run_in_executor`, evitando travar o event loop.

Módulo READ-ONLY por padrão — as funções de execução verificam `settings.readonly_mode`
antes de qualquer operação e lançam ReadOnlyModeError se estiver habilitado.

Exportações públicas:
  dry_run_action(token, db)   → DryRunResult
  execute_action(token, db)   → ExecutionResult
  ReadOnlyModeError           Exceção lançada quando READONLY_MODE=true
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.vcenter.connection import vcenter_pool
from app.core.vcenter.connection_manager import connection_manager
from app.models.audit_log import ApprovalToken
from app.models.vcenter import VCenter
from app.models.zombie_scan import ZombieVmdkRecord
from config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()


# ─────────────────────────────────────────────────────────────────────────────
# Tipos de resultado
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class DryRunResult:
    vmdk_path: str
    action: str
    files_affected: list[str] = field(default_factory=list)
    space_to_recover_gb: float = 0.0
    current_tipo_zombie: str | None = None
    status_changed: bool = False
    datacenter: str | None = None
    datastore: str | None = None
    live_check: dict = field(default_factory=dict)
    action_preview: str = ""
    warnings: list[str] = field(default_factory=list)
    is_safe_to_proceed: bool = False
    simulated_at: str = ""

    def as_dict(self) -> dict:
        return {
            "vmdk_path": self.vmdk_path,
            "action": self.action,
            "files_affected": self.files_affected,
            "space_to_recover_gb": round(self.space_to_recover_gb, 3),
            "current_tipo_zombie": self.current_tipo_zombie,
            "status_changed_since_approval": self.status_changed,
            "datacenter": self.datacenter,
            "datastore": self.datastore,
            "live_check": self.live_check,
            "action_preview": self.action_preview,
            "warnings": self.warnings,
            "is_safe_to_proceed": self.is_safe_to_proceed,
            "simulated_at": self.simulated_at,
        }


@dataclass
class ExecutionResult:
    success: bool
    action: str
    vmdk_path: str
    files_processed: list[str] = field(default_factory=list)
    destination: str | None = None
    space_recovered_gb: float = 0.0
    error: str | None = None
    executed_at: str = ""

    def as_dict(self) -> dict:
        return {
            "success": self.success,
            "action": self.action,
            "vmdk_path": self.vmdk_path,
            "files_processed": self.files_processed,
            "destination": self.destination,
            "space_recovered_gb": round(self.space_recovered_gb, 3),
            "error": self.error,
            "executed_at": self.executed_at,
        }


class ReadOnlyModeError(Exception):
    """Lançada quando READONLY_MODE=true impede a execução."""


# ─────────────────────────────────────────────────────────────────────────────
# API pública
# ─────────────────────────────────────────────────────────────────────────────


async def dry_run_action(token: ApprovalToken, db: AsyncSession) -> DryRunResult:
    """
    Simula a ação SEM executar nada no vCenter.

    1. Consulta o banco para obter o estado mais recente do VMDK
    2. Detecta mudança de status desde a emissão do token
    3. Tenta verificação live no vCenter (não-bloqueante, falha graciosamente)
    4. Retorna DryRunResult com tudo que seria afetado
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # ── Busca registro mais recente do VMDK ───────────────────────────────────
    latest_q = await db.execute(
        select(ZombieVmdkRecord)
        .where(ZombieVmdkRecord.path == token.vmdk_path)
        .order_by(desc(ZombieVmdkRecord.created_at))
        .limit(1)
    )
    record = latest_q.scalar_one_or_none()

    warnings: list[str] = []
    files_affected: list[str] = [token.vmdk_path]
    space_gb = 0.0
    current_tipo: str | None = None
    datacenter: str | None = None
    datastore: str | None = None
    status_changed = False

    if record:
        space_gb = record.tamanho_gb or 0.0
        current_tipo = record.tipo_zombie
        datacenter = record.datacenter
        datastore = record.datastore

        # Detecta mudança de status
        if token.vmdk_tipo_zombie and current_tipo != token.vmdk_tipo_zombie:
            status_changed = True
            warnings.append(
                f"⚠ ATENÇÃO: O tipo zombie mudou de '{token.vmdk_tipo_zombie}' "
                f"para '{current_tipo}' desde a emissão do token. "
                "O token será invalidado se prosseguir com o execute."
            )

        # Aviso de falso positivo
        if current_tipo == "POSSIBLE_FALSE_POSITIVE":
            warnings.append(
                "⚠ ATENÇÃO: VMDK classificado como POSSIBLE_FALSE_POSITIVE. "
                "Verifique se não está em uso em outro vCenter antes de prosseguir."
            )

        # Adiciona flat file esperado
        if not token.vmdk_path.endswith("-flat.vmdk") and not token.vmdk_path.endswith("-ctk.vmdk"):
            flat_path = token.vmdk_path.replace(".vmdk", "-flat.vmdk")
            files_affected.append(flat_path)

        # Delta snapshots relacionados
        base_name = token.vmdk_path.replace(".vmdk", "")
        delta_q = await db.execute(
            select(ZombieVmdkRecord.path)
            .where(ZombieVmdkRecord.path.like(f"{base_name}%delta.vmdk"))
            .distinct()
        )
        for delta_path in delta_q.scalars():
            if delta_path not in files_affected:
                files_affected.append(delta_path)
    else:
        warnings.append(
            "VMDK não encontrado em nenhuma varredura recente. "
            "Pode ter sido removido manualmente ou movido."
        )

    # ── Preview da ação ───────────────────────────────────────────────────────
    if token.action == "DELETE":
        action_preview = (
            f"DELETE: {len(files_affected)} arquivo(s) serão PERMANENTEMENTE "
            f"removidos do datastore '{datastore or 'desconhecido'}'. "
            "Esta operação é IRREVERSÍVEL."
        )
    else:  # QUARANTINE
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        quarantine_base = f"[{datastore or 'datastore'}] _quarantine/{today}"
        action_preview = (
            f"QUARANTINE: {len(files_affected)} arquivo(s) serão MOVIDOS para "
            f"'{quarantine_base}/'. "
            "O VMDK ficará inacessível para VMs mas poderá ser recuperado."
        )

    # ── Verificação live no vCenter (best-effort) ─────────────────────────────
    live_check = await _live_check_vmdk(token)

    if live_check.get("exists") is False:
        warnings.append(
            "VMDK não encontrado no vCenter no momento da simulação. "
            "Pode ter sido removido manualmente."
        )
    elif live_check.get("error"):
        warnings.append(
            f"Não foi possível verificar o vCenter: {live_check['error']}. "
            "Prossiga com cautela."
        )

    is_safe = (
        not status_changed
        and record is not None
        and not any("⚠ ATENÇÃO" in w for w in warnings)
    )

    return DryRunResult(
        vmdk_path=token.vmdk_path,
        action=token.action,
        files_affected=files_affected,
        space_to_recover_gb=space_gb,
        current_tipo_zombie=current_tipo,
        status_changed=status_changed,
        datacenter=datacenter,
        datastore=datastore,
        live_check=live_check,
        action_preview=action_preview,
        warnings=warnings,
        is_safe_to_proceed=is_safe,
        simulated_at=now_iso,
    )


async def execute_action(token: ApprovalToken, db: AsyncSession) -> ExecutionResult:
    """
    Executa a ação aprovada (QUARANTINE ou DELETE) no vCenter.

    Pré-condições verificadas (lançam exceções específicas):
      - READONLY_MODE=false  → ReadOnlyModeError
      - token.status == "dryrun_done"  (garantido pelo caller)
      - VMDK não mudou de status desde a aprovação  (garantido pelo caller)

    Todas as chamadas pyVmomi são executadas em ThreadPoolExecutor.
    """
    if settings.readonly_mode:
        raise ReadOnlyModeError(
            "READONLY_MODE=true. Defina READONLY_MODE=false no .env "
            "para habilitar operações destrutivas."
        )

    now_iso = datetime.now(timezone.utc).isoformat()

    # Obtém VCenter e ServiceInstance
    vc, si = await _get_vcenter_si(token.vcenter_id, db)
    if si is None:
        return ExecutionResult(
            success=False,
            action=token.action,
            vmdk_path=token.vmdk_path,
            error=f"Não foi possível conectar ao vCenter '{vc.name if vc else token.vcenter_id}'.",
            executed_at=now_iso,
        )

    datacenter_name = token.vmdk_datacenter

    try:
        loop = asyncio.get_event_loop()
        if token.action == "DELETE":
            result_dict = await loop.run_in_executor(
                None, _delete_vmdk_sync, si, token.vmdk_path, datacenter_name
            )
        else:  # QUARANTINE
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            result_dict = await loop.run_in_executor(
                None, _quarantine_vmdk_sync, si, token.vmdk_path, datacenter_name, today
            )

        return ExecutionResult(
            success=True,
            action=token.action,
            vmdk_path=token.vmdk_path,
            files_processed=result_dict.get("files_processed", [token.vmdk_path]),
            destination=result_dict.get("destination"),
            space_recovered_gb=token.vmdk_size_gb or 0.0,
            executed_at=now_iso,
        )

    except Exception as exc:
        logger.error(
            "Falha ao executar %s em '%s': %s", token.action, token.vmdk_path, exc
        )
        return ExecutionResult(
            success=False,
            action=token.action,
            vmdk_path=token.vmdk_path,
            error=str(exc),
            executed_at=now_iso,
        )


async def check_vmdk_status_changed(token: ApprovalToken, db: AsyncSession) -> tuple[bool, str | None]:
    """
    Verifica se o status zombie do VMDK mudou desde a emissão do token.
    Retorna (True, reason) se mudou, (False, None) caso contrário.
    """
    if not token.vmdk_tipo_zombie:
        return False, None

    latest_q = await db.execute(
        select(ZombieVmdkRecord)
        .where(ZombieVmdkRecord.path == token.vmdk_path)
        .order_by(desc(ZombieVmdkRecord.created_at))
        .limit(1)
    )
    record = latest_q.scalar_one_or_none()

    if not record:
        return True, (
            "VMDK não encontrado em nenhuma varredura recente. "
            "Pode ter sido removido ou re-registrado como VM ativa."
        )

    if record.tipo_zombie != token.vmdk_tipo_zombie:
        return True, (
            f"Tipo zombie mudou de '{token.vmdk_tipo_zombie}' para "
            f"'{record.tipo_zombie}' após a emissão do token."
        )

    return False, None


# ─────────────────────────────────────────────────────────────────────────────
# Helpers assíncronos
# ─────────────────────────────────────────────────────────────────────────────


async def _get_vcenter_si(vcenter_id_str: str, db: AsyncSession):
    """Resolve vcenter_id (int string ou nome) → (VCenter, ServiceInstance)."""
    try:
        int_id = int(vcenter_id_str)
        result = await db.execute(select(VCenter).where(VCenter.id == int_id))
    except (ValueError, TypeError):
        result = await db.execute(
            select(VCenter).where(VCenter.name == vcenter_id_str)
        )

    vc = result.scalar_one_or_none()
    if not vc:
        return None, None

    try:
        connection_manager.register(vc)
        si = vcenter_pool.get_service_instance(vc.id)
        return vc, si
    except Exception as exc:
        logger.warning("Não foi possível obter SI para vCenter '%s': %s", vc.name, exc)
        return vc, None


async def _live_check_vmdk(token: ApprovalToken) -> dict:
    """Verifica se o VMDK existe no vCenter (best-effort)."""
    try:
        vcenter_id_int = int(token.vcenter_id)
        si = vcenter_pool.get_service_instance(vcenter_id_int)
    except Exception:
        return {"attempted": False, "reason": "vCenter não disponível no pool"}

    try:
        loop = asyncio.get_event_loop()
        result = await asyncio.wait_for(
            loop.run_in_executor(
                None,
                _check_file_exists_sync,
                si,
                token.vmdk_path,
                token.vmdk_datacenter,
            ),
            timeout=15.0,
        )
        return {"attempted": True, **result}
    except asyncio.TimeoutError:
        return {"attempted": True, "error": "Timeout na verificação do vCenter (15s)"}
    except Exception as exc:
        return {"attempted": True, "error": str(exc)}


# ─────────────────────────────────────────────────────────────────────────────
# Funções síncronas para ThreadPoolExecutor (pyVmomi)
# ─────────────────────────────────────────────────────────────────────────────


def _parse_vmdk_path(vmdk_path: str) -> tuple[str, str]:
    """
    Extrai (datastore_name, relative_path) de '[datastore] folder/file.vmdk'.
    Lança ValueError se o formato for inválido.
    """
    m = re.match(r"^\[([^\]]+)\]\s*(.*)", vmdk_path)
    if not m:
        raise ValueError(
            f"Formato de caminho VMDK inválido: '{vmdk_path}'. "
            "Esperado: '[datastore] pasta/arquivo.vmdk'"
        )
    return m.group(1), m.group(2)


def _find_datacenter(si: Any, name: str | None) -> Any:
    """Encontra o objeto Datacenter no inventário vCenter."""
    try:
        from pyVmomi import vim
        content = si.content
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.Datacenter], True
        )
        dcs = list(view.view)
        view.Destroy()

        if not name:
            if len(dcs) == 1:
                return dcs[0]
            return None

        for dc in dcs:
            if dc.name == name:
                return dc
        return None
    except Exception:
        return None


def _wait_for_task(task: Any, timeout: int = 300) -> Any:
    """Aguarda a conclusão de uma task vCenter. Lança exceção em caso de erro."""
    import time
    from pyVmomi import vim

    start = time.time()
    while True:
        state = task.info.state
        if state == vim.TaskInfo.State.success:
            return task.info.result
        if state == vim.TaskInfo.State.error:
            error = task.info.error
            msg = getattr(error, "msg", str(error))
            raise RuntimeError(f"Task vCenter falhou: {msg}")
        if time.time() - start > timeout:
            raise TimeoutError(f"Task vCenter não concluiu em {timeout}s")
        time.sleep(0.5)


def _check_file_exists_sync(si: Any, vmdk_path: str, datacenter_name: str | None) -> dict:
    """Verifica existência do VMDK via DatastoreBrowser (síncrono)."""
    try:
        from pyVmomi import vim

        ds_name, rel_path = _parse_vmdk_path(vmdk_path)
        dc = _find_datacenter(si, datacenter_name)

        folder = f"[{ds_name}]"
        if "/" in rel_path:
            folder_part = rel_path.rsplit("/", 1)[0]
            filename = rel_path.rsplit("/", 1)[1]
            folder = f"[{ds_name}] {folder_part}"
        else:
            filename = rel_path

        # Encontra o datastore
        content = si.content
        ds_view = content.viewManager.CreateContainerView(
            dc or content.rootFolder, [vim.Datastore], True
        )
        target_ds = None
        for ds in ds_view.view:
            if ds.name == ds_name:
                target_ds = ds
                break
        ds_view.Destroy()

        if not target_ds:
            return {"exists": False, "reason": f"Datastore '{ds_name}' não encontrado"}

        spec = vim.host.DatastoreBrowser.SearchSpec()
        spec.query = [vim.VmDiskFileQuery()]
        spec.matchPattern = [filename]

        task = target_ds.browser.SearchDatastore_Task(
            datastorePath=folder, searchSpec=spec
        )
        res = _wait_for_task(task, timeout=30)

        for f in (res.file if res else []):
            if getattr(f, "path", "") == filename:
                return {
                    "exists": True,
                    "size_bytes": getattr(f, "fileSize", None),
                    "modified": str(getattr(f, "modification", "")),
                }

        return {"exists": False}

    except Exception as exc:
        return {"exists": None, "error": str(exc)}


def _delete_vmdk_sync(si: Any, vmdk_path: str, datacenter_name: str | None) -> dict:
    """
    Deleta o VMDK e seu arquivo flat associado via VirtualDiskManager.
    Operação IRREVERSÍVEL — só executada quando READONLY_MODE=false.
    """
    from pyVmomi import vim

    dc = _find_datacenter(si, datacenter_name)
    if not dc:
        raise ValueError(
            f"Datacenter '{datacenter_name}' não encontrado no vCenter. "
            "Não é possível executar a deleção com segurança."
        )

    vdm = si.content.virtualDiskManager
    task = vdm.DeleteVirtualDisk(name=vmdk_path, datacenter=dc)
    _wait_for_task(task, timeout=300)

    logger.warning(
        "VMDK DELETADO: '%s' no datacenter '%s'.", vmdk_path, datacenter_name
    )
    return {"files_processed": [vmdk_path]}


def _quarantine_vmdk_sync(
    si: Any, vmdk_path: str, datacenter_name: str | None, date_str: str
) -> dict:
    """
    Move o VMDK para pasta de quarentena via FileManager.
    O arquivo fica inacessível para VMs mas pode ser recuperado pelo analista.
    """
    from pyVmomi import vim

    dc = _find_datacenter(si, datacenter_name)
    if not dc:
        raise ValueError(
            f"Datacenter '{datacenter_name}' não encontrado. "
            "Não é possível mover com segurança."
        )

    ds_name, rel_path = _parse_vmdk_path(vmdk_path)
    filename = rel_path.rsplit("/", 1)[-1]
    quarantine_path = f"[{ds_name}] _quarantine/{date_str}/{filename}"

    file_manager = si.content.fileManager
    task = file_manager.MoveDatastoreFile(
        sourceName=vmdk_path,
        sourceDatacenter=dc,
        destinationName=quarantine_path,
        destinationDatacenter=dc,
        force=False,
    )
    _wait_for_task(task, timeout=300)

    # Tenta mover o flat file também (melhor esforço)
    files_processed = [vmdk_path]
    if not vmdk_path.endswith("-flat.vmdk"):
        flat_src = vmdk_path.replace(".vmdk", "-flat.vmdk")
        flat_filename = filename.replace(".vmdk", "-flat.vmdk")
        flat_dest = f"[{ds_name}] _quarantine/{date_str}/{flat_filename}"
        try:
            flat_task = file_manager.MoveDatastoreFile(
                sourceName=flat_src,
                sourceDatacenter=dc,
                destinationName=flat_dest,
                destinationDatacenter=dc,
                force=False,
            )
            _wait_for_task(flat_task, timeout=300)
            files_processed.append(flat_src)
        except Exception as exc:
            logger.warning(
                "Flat file '%s' não encontrado ou não movível: %s", flat_src, exc
            )

    logger.warning(
        "VMDK QUARENTENADO: '%s' → '%s'.", vmdk_path, quarantine_path
    )
    return {
        "files_processed": files_processed,
        "destination": f"[{ds_name}] _quarantine/{date_str}/",
    }
