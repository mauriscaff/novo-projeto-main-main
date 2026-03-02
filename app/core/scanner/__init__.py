from .vmdk_scanner import VMDKInfo, scan_vmdks, scan_vmdks_async
from .zombie_detector import (
    TIPOS_EXCLUIVEIS,
    ZombieType,
    ZombieVmdkResult,
    scan_datacenter,
)

__all__ = [
    # vmdk_scanner (varredura genérica por threshold de dias)
    "VMDKInfo",
    "scan_vmdks",
    "scan_vmdks_async",
    # zombie_detector (varredura por regras Broadcom/VMware)
    "TIPOS_EXCLUIVEIS",
    "ZombieType",
    "ZombieVmdkResult",
    "scan_datacenter",
]
