"""
Helpers de agregação para snapshot de descomissionamento de datastore.
"""

from __future__ import annotations

from typing import Iterable

from app.core.scanner.zombie_detector import ZombieType


KNOWN_ZOMBIE_TYPES: tuple[str, ...] = tuple(z.value for z in ZombieType)


def aggregate_datastore_rows(
    rows: Iterable[tuple[str, float | None]],
) -> tuple[int, float, dict[str, int]]:
    """
    Agrega linhas (tipo_zombie, tamanho_gb) em totais para snapshot.
    """
    total_itens = 0
    total_size_gb = 0.0
    breakdown: dict[str, int] = {k: 0 for k in KNOWN_ZOMBIE_TYPES}

    for tipo_zombie, size_gb in rows:
        total_itens += 1
        total_size_gb += float(size_gb or 0.0)
        breakdown[tipo_zombie] = breakdown.get(tipo_zombie, 0) + 1

    return total_itens, round(total_size_gb, 3), breakdown

