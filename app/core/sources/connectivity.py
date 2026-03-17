from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass(slots=True)
class ConnectivityStubResult:
    reachable: bool
    status: str
    message: str
    checked_at: datetime


def test_connectivity_stub(source_type: str, endpoint: str) -> ConnectivityStubResult:
    """
    Teste stubado de conectividade.

    Regra deterministica para esta fase:
      - endpoint contendo offline/down/fail/invalid -> OFFLINE
      - endpoint vazio -> OFFLINE
      - demais -> ONLINE
    """
    checked_at = datetime.now(timezone.utc)
    normalized = (endpoint or "").strip().lower()

    if not normalized:
        return ConnectivityStubResult(
            reachable=False,
            status="offline",
            message="Endpoint ausente.",
            checked_at=checked_at,
        )

    offline_tokens = ("offline", "down", "fail", "invalid")
    if any(token in normalized for token in offline_tokens):
        return ConnectivityStubResult(
            reachable=False,
            status="offline",
            message=f"Stub {source_type}: endpoint marcado como indisponivel.",
            checked_at=checked_at,
        )

    return ConnectivityStubResult(
        reachable=True,
        status="online",
        message=f"Stub {source_type}: conectividade simulada com sucesso.",
        checked_at=checked_at,
    )
