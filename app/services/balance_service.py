"""
Balance intelligence service for datastore capacity.

Collects datastore and VM usage data from vCenter (pyVmomi) and returns:
- used/free/committed space
- health state per datastore
- top migration candidates for hot datastores
"""

from __future__ import annotations

import asyncio
import json
import logging
from functools import partial
from typing import Any

from config import get_settings

logger = logging.getLogger(__name__)

THRESHOLD_YELLOW = 0.70
THRESHOLD_RED = 0.85
GB = 1024 ** 3
REPORT_TIMEOUT_SEC = 60.0


def _log_collection_warning(event: str, **fields: Any) -> None:
    payload = {"event": event, **fields}
    logger.warning("balance_service %s", json.dumps(payload, ensure_ascii=True, sort_keys=True, default=str))


def _health_status(used_ratio: float) -> str:
    if used_ratio >= THRESHOLD_RED:
        return "red"
    if used_ratio >= THRESHOLD_YELLOW:
        return "yellow"
    return "green"


def _find_datacenter_by_name(content: Any, datacenter_name: str) -> Any | None:
    """Find a datacenter by name (case-insensitive)."""
    from pyVmomi import vim

    wanted = datacenter_name.strip().lower()
    dc_view = content.viewManager.CreateContainerView(
        content.rootFolder,
        [vim.Datacenter],
        True,
    )
    try:
        for dc in dc_view.view:
            name = str(getattr(dc, "name", "") or "").strip().lower()
            if name == wanted:
                return dc
    finally:
        dc_view.Destroy()
    return None


def _extract_vm_sdrs_policy(vm: Any) -> dict:
    """Extract conservative SDRS policy hints from VM config when available."""
    from pyVmomi import vim

    has_independent_disk = False
    try:
        vm_config = getattr(vm, "config", None)
        vm_hw = getattr(vm_config, "hardware", None)
        devices = list(getattr(vm_hw, "device", []) or [])
        for dev in devices:
            if not isinstance(dev, vim.vm.device.VirtualDisk):
                continue
            backing = getattr(dev, "backing", None)
            disk_mode = str(getattr(backing, "diskMode", "") or "").lower()
            if "independent" in disk_mode:
                has_independent_disk = True
                break
    except Exception as exc:
        _log_collection_warning(
            "vm_independent_disk_parse_failed",
            vm_name=str(getattr(vm, "name", "") or ""),
            error=str(exc),
        )
        has_independent_disk = False

    vm_override_mode = "unknown"
    keep_vmdks_together: bool | None = None

    # Best effort: many environments may not expose these fields uniformly.
    try:
        cfg = getattr(vm, "storageDrsVmConfig", None) or getattr(vm, "storageDrsConfig", None)
        if cfg is not None:
            override = getattr(cfg, "automationMode", None)
            if override is not None:
                vm_override_mode = str(override).split(".")[-1].lower()
            affinity = getattr(cfg, "intraVmAffinity", None)
            if affinity is not None:
                keep_vmdks_together = bool(affinity)
    except Exception as exc:
        _log_collection_warning(
            "vm_override_parse_failed",
            vm_name=str(getattr(vm, "name", "") or ""),
            error=str(exc),
        )
        vm_override_mode = "unknown"
        keep_vmdks_together = None

    if keep_vmdks_together is None:
        # Default VMware behavior is keep VMDKs together.
        keep_vmdks_together = True

    return {
        "has_independent_disk": has_independent_disk,
        "vm_override_mode": vm_override_mode,
        "keep_vmdks_together": keep_vmdks_together,
    }


def _get_capacity_report_sync(si: Any, datacenter_name: str | None = None) -> dict:
    """
    Collect datastore capacity report.

    If datacenter_name is provided, report is scoped to that datacenter.
    """
    from pyVmomi import vim

    content = si.RetrieveContent()
    datacenter_scope = (datacenter_name or "").strip()

    root = content.rootFolder
    if datacenter_scope:
        dc = _find_datacenter_by_name(content, datacenter_scope)
        if dc is None:
            raise ValueError(f"Datacenter '{datacenter_scope}' nao encontrado no vCenter.")
        root = dc

    ds_view = content.viewManager.CreateContainerView(root, [vim.Datastore], True)
    try:
        datastores_raw = list(ds_view.view)
    finally:
        ds_view.Destroy()

    scoped_ds_ids = {
        str(getattr(ds, "_moId", "") or "").strip()
        for ds in datastores_raw
        if str(getattr(ds, "_moId", "") or "").strip()
    }
    scoped_ds_names = {
        str(getattr(getattr(ds, "summary", None), "name", "") or "").strip()
        for ds in datastores_raw
        if str(getattr(getattr(ds, "summary", None), "name", "") or "").strip()
    }

    vm_view = content.viewManager.CreateContainerView(root, [vim.VirtualMachine], True)
    try:
        vms_raw = list(vm_view.view)
    finally:
        vm_view.Destroy()

    # key: datastore moid (fallback: name::<datastore>)
    vms_by_ds: dict[str, list[dict]] = {}
    for vm in vms_raw:
        try:
            storage = getattr(vm, "storage", None)
            if storage is None:
                continue

            vm_policy = _extract_vm_sdrs_policy(vm)

            for ds_usage in (storage.perDatastoreUsage or []):
                ds_ref = getattr(ds_usage, "datastore", None)
                ds_name = str(getattr(ds_ref, "name", "") or "").strip()
                if not ds_name:
                    continue

                ds_id = str(getattr(ds_ref, "_moId", "") or "").strip()
                if scoped_ds_ids:
                    if ds_id and ds_id not in scoped_ds_ids:
                        continue
                    if not ds_id and ds_name not in scoped_ds_names:
                        continue

                ds_key = ds_id or f"name::{ds_name}"
                committed_gb = round((getattr(ds_usage, "committed", 0) or 0) / GB, 2)
                vm_path = ""
                vm_config = getattr(vm, "config", None)
                vm_files = getattr(vm_config, "files", None)
                if vm_files is not None:
                    vm_path = getattr(vm_files, "vmPathName", "") or ""

                entry = {
                    "name": getattr(vm, "name", "") or "",
                    "path": vm_path,
                    "committed_gb": committed_gb,
                    "sdrs_policy": vm_policy,
                }
                vms_by_ds.setdefault(ds_key, []).append(entry)
        except Exception as exc:
            _log_collection_warning(
                "vm_datastore_usage_parse_failed",
                vm_name=str(getattr(vm, "name", "") or ""),
                error=str(exc),
            )
            continue

    datastores: list[dict] = []
    total_capacity_gb = 0.0
    total_free_gb = 0.0
    red_count = 0
    yellow_count = 0
    green_count = 0

    for ds in datastores_raw:
        try:
            summary = getattr(ds, "summary", None)
            if not summary:
                continue

            name = str(getattr(summary, "name", "") or "").strip()
            if not name:
                continue

            ds_id = str(getattr(ds, "_moId", "") or "").strip()
            ds_key = ds_id or f"name::{name}"

            capacity_bytes = getattr(summary, "capacity", 0) or 0
            free_bytes = getattr(summary, "freeSpace", 0) or 0
            uncommitted_bytes = getattr(summary, "uncommitted", 0) or 0
            accessible = bool(getattr(summary, "accessible", False))

            capacity_gb = round(capacity_bytes / GB, 2)
            free_gb = round(free_bytes / GB, 2)
            used_gb = round((capacity_bytes - free_bytes) / GB, 2)
            uncommitted_gb = round(uncommitted_bytes / GB, 2)

            committed_ratio = 0.0
            if capacity_gb > 0:
                committed_ratio = (capacity_bytes - free_bytes) / capacity_bytes

            health = _health_status(committed_ratio)
            use_pct = round(committed_ratio * 100, 1)

            parent = getattr(ds, "parent", None)
            in_datastore_cluster = isinstance(parent, vim.StoragePod)
            datastore_cluster = str(getattr(parent, "name", "") or "").strip() if in_datastore_cluster else None
            datastore_type = str(getattr(summary, "type", "") or "").strip()

            top_vms: list[dict] = []
            if health == "red":
                ds_vms = vms_by_ds.get(ds_key, [])
                top_vms = sorted(ds_vms, key=lambda v: v.get("committed_gb", 0), reverse=True)[:3]

            datastores.append(
                {
                    "name": name,
                    "accessible": accessible,
                    "capacity_gb": capacity_gb,
                    "free_gb": free_gb,
                    "used_gb": used_gb,
                    "uncommitted_gb": uncommitted_gb,
                    "use_pct": use_pct,
                    "health_status": health,
                    "datastore_cluster": datastore_cluster,
                    "in_datastore_cluster": in_datastore_cluster,
                    "datastore_type": datastore_type,
                    "top_vms": top_vms,
                }
            )

            total_capacity_gb += capacity_gb
            total_free_gb += free_gb
            if health == "red":
                red_count += 1
            elif health == "yellow":
                yellow_count += 1
            else:
                green_count += 1

        except Exception as exc:
            logger.warning("Erro ao processar datastore: %s", exc)
            continue

    health_order = {"red": 0, "yellow": 1, "green": 2}
    datastores.sort(key=lambda d: (health_order.get(d["health_status"], 3), -d["use_pct"]))

    return {
        "summary": {
            "total_datastores": len(datastores),
            "red": red_count,
            "yellow": yellow_count,
            "green": green_count,
            "total_capacity_gb": round(total_capacity_gb, 2),
            "total_free_gb": round(total_free_gb, 2),
            "total_used_gb": round(total_capacity_gb - total_free_gb, 2),
        },
        "datastores": datastores,
        "recommendations": _build_recommendations(datastores),
    }


def _build_recommendations(datastores: list[dict]) -> list[str]:
    """Generate readable migration recommendations."""
    red_ds = [d for d in datastores if d["health_status"] == "red"]
    target_ds = [d for d in datastores if d["health_status"] in ("green", "yellow")]
    if not target_ds:
        target_ds = [d for d in datastores if d["health_status"] != "red"]

    recommendations: list[str] = []
    for red in red_ds:
        best_target = max(target_ds, key=lambda d: d["free_gb"], default=None) if target_ds else None
        msg = (
            f"Datastore '{red['name']}' esta com {red['use_pct']}% de uso "
            f"({red['health_status'].upper()})."
        )
        if red.get("top_vms"):
            vm_names = ", ".join(v.get("name", "") for v in red["top_vms"][:2])
            msg += f" Candidatas a migracao: {vm_names}."
        if best_target:
            msg += (
                f" Destino sugerido: '{best_target['name']}' "
                f"({best_target['free_gb']} GB livres, {best_target['use_pct']}% de uso)."
            )
        recommendations.append(msg)

    if not recommendations:
        recommendations.append("Todos os datastores estao dentro dos limites saudaveis.")

    return recommendations


async def get_capacity_report(si: Any, datacenter_name: str | None = None) -> dict:
    """
    Return capacity report, optionally scoped by datacenter.

    Includes controlled retries for transient vCenter fetch failures.
    """
    settings = get_settings()
    max_attempts = max(1, int(getattr(settings, "vcenter_max_retries", 1) or 1))
    base_delay = float(getattr(settings, "vcenter_retry_base_delay_sec", 1.0) or 1.0)

    loop = asyncio.get_event_loop()
    last_exc: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return await asyncio.wait_for(
                loop.run_in_executor(None, partial(_get_capacity_report_sync, si, datacenter_name)),
                timeout=REPORT_TIMEOUT_SEC,
            )
        except asyncio.TimeoutError as exc:
            last_exc = RuntimeError("Timeout ao consultar datastores no vCenter (60s).")
            logger.warning("Timeout no get_capacity_report (tentativa %s/%s)", attempt, max_attempts)
        except Exception as exc:
            last_exc = exc
            logger.warning("Falha no get_capacity_report (tentativa %s/%s): %s", attempt, max_attempts, exc)

        if attempt < max_attempts:
            await asyncio.sleep(base_delay * (2 ** (attempt - 1)))

    if last_exc is not None:
        raise last_exc
    raise RuntimeError("Falha inesperada ao consultar datastores no vCenter.")