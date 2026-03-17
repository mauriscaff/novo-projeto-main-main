"""
Serviço de Inteligência de Balanceamento de Datastores.

Consulta o vCenter via pyVmomi e retorna um relatório de capacidade por datastore:
  - Espaço usado/livre/comprometido
  - Status de saúde: green (<70%), yellow (70-85%), red (>85%)
  - Para datastores 'red': lista das 3 maiores VMs candidatas a migração

Todas as chamadas pyVmomi são executadas em ThreadPoolExecutor para não
bloquear o event loop do FastAPI.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Thresholds de saúde
# ─────────────────────────────────────────────────────────────────────────────

THRESHOLD_YELLOW = 0.70  # 70%
THRESHOLD_RED = 0.85     # 85%

GB = 1024 ** 3


def _health_status(used_ratio: float) -> str:
    """Determina o status de saúde com base na proporção de uso comprometido."""
    if used_ratio >= THRESHOLD_RED:
        return "red"
    if used_ratio >= THRESHOLD_YELLOW:
        return "yellow"
    return "green"


# ─────────────────────────────────────────────────────────────────────────────
# Lógica síncrona (executa no ThreadPoolExecutor)
# ─────────────────────────────────────────────────────────────────────────────


def _get_capacity_report_sync(si: Any) -> dict:
    """
    Coleta dados de todos os Datastores e VMs visíveis no vCenter.
    Deve ser chamada dentro de run_in_executor.
    """
    from pyVmomi import vim

    content = si.RetrieveContent()

    # ── Datastores ────────────────────────────────────────────────────────────
    ds_view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.Datastore], True
    )
    try:
        datastores_raw = list(ds_view.view)
    finally:
        ds_view.Destroy()

    # ── VMs ───────────────────────────────────────────────────────────────────
    vm_view = content.viewManager.CreateContainerView(
        content.rootFolder, [vim.VirtualMachine], True
    )
    try:
        vms_raw = list(vm_view.view)
    finally:
        vm_view.Destroy()

    # ── Indexa VMs por datastore ──────────────────────────────────────────────
    # Chave: nome do datastore → lista de {"name", "committed_gb", "path"}
    vms_by_ds: dict[str, list[dict]] = {}
    for vm in vms_raw:
        try:
            if not hasattr(vm, "storage") or vm.storage is None:
                continue
            storage = vm.storage
            # perDatastoreUsage é uma lista de objetos com campos datastore e committed/unshared
            for ds_usage in (storage.perDatastoreUsage or []):
                ds_name = getattr(ds_usage.datastore, "name", None)
                if not ds_name:
                    continue
                committed_gb = round((ds_usage.committed or 0) / GB, 2)
                entry = {
                    "name": vm.name,
                    "path": getattr(vm.config, "files", None) and vm.config.files.vmPathName or "",
                    "committed_gb": committed_gb,
                }
                vms_by_ds.setdefault(ds_name, []).append(entry)
        except Exception:
            continue

    # ── Processa Datastores ───────────────────────────────────────────────────
    datastores: list[dict] = []
    total_capacity_gb = 0.0
    total_free_gb = 0.0
    red_count = 0
    yellow_count = 0
    green_count = 0

    for ds in datastores_raw:
        try:
            summary = ds.summary
            if not summary:
                continue
            name = str(summary.name or "").strip()
            if not name:
                continue

            capacity_bytes = summary.capacity or 0
            free_bytes = summary.freeSpace or 0
            uncommitted_bytes = summary.uncommitted or 0
            accessible = bool(summary.accessible)

            capacity_gb = round(capacity_bytes / GB, 2)
            free_gb = round(free_bytes / GB, 2)
            used_gb = round((capacity_bytes - free_bytes) / GB, 2)
            uncommitted_gb = round(uncommitted_bytes / GB, 2)

            # Razão de comprometimento: uso real + possível alocação futura
            committed_ratio = 0.0
            if capacity_gb > 0:
                # Usa espaço real usado como base do status (mais conservador)
                committed_ratio = (capacity_bytes - free_bytes) / capacity_bytes

            health = _health_status(committed_ratio)
            use_pct = round(committed_ratio * 100, 1)

            # Candidatas a migração apenas em datastores vermelhos
            top_vms: list[dict] = []
            if health == "red":
                ds_vms = vms_by_ds.get(name, [])
                top_vms = sorted(ds_vms, key=lambda v: v["committed_gb"], reverse=True)[:3]

            datastores.append({
                "name": name,
                "accessible": accessible,
                "capacity_gb": capacity_gb,
                "free_gb": free_gb,
                "used_gb": used_gb,
                "uncommitted_gb": uncommitted_gb,
                "use_pct": use_pct,
                "health_status": health,
                "top_vms": top_vms,
            })

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

    # Ordena: vermelhos primeiro, depois por uso desc
    _HEALTH_ORDER = {"red": 0, "yellow": 1, "green": 2}
    datastores.sort(key=lambda d: (_HEALTH_ORDER.get(d["health_status"], 3), -d["use_pct"]))

    # ── Recomendações ──────────────────────────────────────────────────────────
    recommendations = _build_recommendations(datastores)

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
        "recommendations": recommendations,
    }


def _build_recommendations(datastores: list[dict]) -> list[str]:
    """
    Gera mensagens de recomendação de migração legíveis.
    Cruza datastores vermelhos com candidatos verdes/amarelos.
    """
    red_ds = [d for d in datastores if d["health_status"] == "red"]
    target_ds = [d for d in datastores if d["health_status"] in ("green", "yellow")]

    if not target_ds:
        target_ds = [d for d in datastores if d["health_status"] != "red"]

    recommendations: list[str] = []
    for red in red_ds:
        best_target = max(target_ds, key=lambda d: d["free_gb"], default=None) if target_ds else None
        msg = (
            f"⚠️ Datastore '{red['name']}' está com {red['use_pct']}% de uso "
            f"({red['health_status'].upper()})."
        )
        if red["top_vms"]:
            vm_names = ", ".join(v["name"] for v in red["top_vms"][:2])
            msg += f" Candidatas à migração: {vm_names}."
        if best_target:
            msg += (
                f" Datastore de destino sugerido: '{best_target['name']}' "
                f"({best_target['free_gb']} GB livres, {best_target['use_pct']}% de uso)."
            )
        recommendations.append(msg)

    if not recommendations:
        recommendations.append("✅ Todos os datastores estão dentro dos limites saudáveis.")

    return recommendations


# ─────────────────────────────────────────────────────────────────────────────
# API pública assíncrona
# ─────────────────────────────────────────────────────────────────────────────


async def get_capacity_report(si: Any) -> dict:
    """
    Retorna o relatório de capacidade e balanceamento dos datastores.
    Executa a lógica pyVmomi em thread pool para não bloquear o event loop.
    """
    loop = asyncio.get_event_loop()
    try:
        return await asyncio.wait_for(
            loop.run_in_executor(None, _get_capacity_report_sync, si),
            timeout=60.0,
        )
    except asyncio.TimeoutError:
        raise RuntimeError("Timeout ao consultar datastores no vCenter (60s).")
    except Exception as exc:
        logger.error("Erro ao gerar relatório de capacidade: %s", exc)
        raise
