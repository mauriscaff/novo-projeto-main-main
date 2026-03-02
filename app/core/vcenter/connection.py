"""
Pool de conexões com múltiplos vCenters via pyVmomi.

Características:
  - Um slot por vcenter_id com lock independente (sem contenção entre vCenters distintos)
  - Conexão lazy: o SmartConnect só é chamado quando o slot é acessado pela primeira vez
  - Heartbeat antes de reusar uma conexão cacheada (detecta sessões expiradas)
  - Timeout configurável: SmartConnect executado em thread separada via ThreadPoolExecutor
  - Retry com back-off exponencial: tentativas 1, 2, 4, 8… segundos
  - Thread-safe: dict global protegido por RLock; operações de conexão protegidas por lock por slot
"""

from __future__ import annotations

import concurrent.futures
import logging
import ssl
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

from pyVim.connect import Disconnect, SmartConnect
from pyVmomi import vim

from app.core.vcenter.client import VCenterCredentials
from config import get_settings

logger = logging.getLogger(__name__)
_settings = get_settings()


# ---------------------------------------------------------------------------
# Exceções
# ---------------------------------------------------------------------------

class VCenterNotRegisteredError(KeyError):
    """vcenter_id solicitado não está registrado no pool."""


class VCenterConnectionError(ConnectionError):
    """Todas as tentativas de conexão com o vCenter falharam."""


# ---------------------------------------------------------------------------
# Estrutura interna de cada slot do pool
# ---------------------------------------------------------------------------

@dataclass
class _PoolSlot:
    vcenter_id: int
    creds: VCenterCredentials
    si: vim.ServiceInstance | None = None
    lock: threading.Lock = field(default_factory=threading.Lock)
    last_connected_at: datetime | None = None
    consecutive_failures: int = 0

    def mark_connected(self, si: vim.ServiceInstance) -> None:
        self.si = si
        self.last_connected_at = datetime.now(timezone.utc)
        self.consecutive_failures = 0

    def mark_failed(self) -> None:
        self.si = None
        self.consecutive_failures += 1


# ---------------------------------------------------------------------------
# Pool principal
# ---------------------------------------------------------------------------

class VCenterConnectionPool:
    """
    Gerencia conexões simultâneas com múltiplos vCenters.

    Uso básico:
        pool = VCenterConnectionPool()
        pool.register(1, creds)
        si = pool.get_service_instance(1)   # reutiliza se válido
        pool.disconnect(1)
        pool.disconnect_all()
    """

    def __init__(
        self,
        connect_timeout_sec: int = 30,
        max_retries: int = 3,
        retry_base_delay_sec: float = 2.0,
    ) -> None:
        self._connect_timeout_sec = connect_timeout_sec
        self._max_retries = max_retries
        self._retry_base_delay_sec = retry_base_delay_sec

        self._slots: dict[int, _PoolSlot] = {}
        self._dict_lock = threading.RLock()  # guarda mutações em _slots

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def register(
        self,
        vcenter_id: int,
        creds: VCenterCredentials,
    ) -> None:
        """
        Registra (ou atualiza) as credenciais de um vCenter.
        Se já existir uma conexão ativa para o mesmo ID, ela é encerrada antes.
        A nova conexão é lazy — ocorre apenas no primeiro get_service_instance().
        """
        with self._dict_lock:
            old_slot = self._slots.get(vcenter_id)
            if old_slot:
                # Desconecta em background para não bloquear o caller
                threading.Thread(
                    target=self._safe_disconnect_slot,
                    args=(old_slot,),
                    daemon=True,
                ).start()
            self._slots[vcenter_id] = _PoolSlot(vcenter_id=vcenter_id, creds=creds)
            logger.debug("vCenter %d registrado no pool.", vcenter_id)

    def get_service_instance(
        self,
        vcenter_id: int,
        creds: VCenterCredentials | None = None,
    ) -> vim.ServiceInstance:
        """
        Retorna um ServiceInstance válido para o vcenter_id.

        Se o slot ainda não existe e `creds` for fornecido, registra automaticamente.
        Se o slot existe e a sessão está viva, retorna imediatamente (zero overhead).
        Se a sessão está morta, reconecta com retry antes de retornar.

        Raises:
            VCenterNotRegisteredError: se vcenter_id não está no pool e creds é None.
            VCenterConnectionError: se todas as tentativas de conexão falharam.
        """
        with self._dict_lock:
            slot = self._slots.get(vcenter_id)
            if slot is None:
                if creds is None:
                    raise VCenterNotRegisteredError(
                        f"vCenter {vcenter_id} não está registrado no pool. "
                        "Chame register() antes ou forneça creds."
                    )
                self._slots[vcenter_id] = _PoolSlot(
                    vcenter_id=vcenter_id, creds=creds
                )
                slot = self._slots[vcenter_id]
            elif creds is not None:
                # Atualiza credenciais se fornecidas (ex.: rotação de senha)
                slot.creds = creds

        with slot.lock:
            if slot.si is not None and self._is_alive(slot.si):
                logger.debug("vCenter %d: reusando sessão ativa.", vcenter_id)
                return slot.si

            # Sessão inativa ou inexistente — conecta com retry
            if slot.si is not None:
                logger.info("vCenter %d: sessão expirada. Reconectando...", vcenter_id)
                self._safe_disconnect_slot(slot)

            si = self._connect_with_retry(slot)
            slot.mark_connected(si)
            return si

    def disconnect(self, vcenter_id: int) -> None:
        """Remove e encerra a conexão de um vCenter específico."""
        with self._dict_lock:
            slot = self._slots.pop(vcenter_id, None)
        if slot:
            with slot.lock:
                self._safe_disconnect_slot(slot)
            logger.info("vCenter %d desconectado e removido do pool.", vcenter_id)

    def disconnect_all(self) -> None:
        """Encerra todas as conexões ativas (usar em shutdown da aplicação)."""
        with self._dict_lock:
            slots = list(self._slots.values())
            self._slots.clear()

        for slot in slots:
            with slot.lock:
                self._safe_disconnect_slot(slot)

        logger.info("Pool de conexões encerrado (%d vCenters).", len(slots))

    def status(self) -> dict[int, dict]:
        """Retorna o estado atual de cada slot (útil para healthcheck / debug)."""
        with self._dict_lock:
            snapshot = {
                vid: {
                    "host": s.creds.host,
                    "connected": s.si is not None,
                    "last_connected_at": (
                        s.last_connected_at.isoformat()
                        if s.last_connected_at
                        else None
                    ),
                    "consecutive_failures": s.consecutive_failures,
                }
                for vid, s in self._slots.items()
            }
        return snapshot

    # ------------------------------------------------------------------
    # Internos
    # ------------------------------------------------------------------

    def _connect_with_retry(self, slot: _PoolSlot) -> vim.ServiceInstance:
        """Tenta conectar até max_retries vezes com back-off exponencial."""
        last_exc: Exception | None = None

        for attempt in range(1, self._max_retries + 1):
            if attempt > 1:
                delay = self._retry_base_delay_sec * (2 ** (attempt - 2))
                logger.warning(
                    "vCenter %d (%s): tentativa %d/%d em %.1fs...",
                    slot.vcenter_id,
                    slot.creds.host,
                    attempt,
                    self._max_retries,
                    delay,
                )
                time.sleep(delay)

            try:
                si = self._connect_with_timeout(slot.creds)
                logger.info(
                    "vCenter %d (%s): conectado (tentativa %d).",
                    slot.vcenter_id,
                    slot.creds.host,
                    attempt,
                )
                return si
            except Exception as exc:
                slot.mark_failed()
                last_exc = exc
                logger.error(
                    "vCenter %d (%s): falha na tentativa %d — %s",
                    slot.vcenter_id,
                    slot.creds.host,
                    attempt,
                    exc,
                )

        raise VCenterConnectionError(
            f"Impossível conectar ao vCenter {slot.vcenter_id} "
            f"({slot.creds.host}) após {self._max_retries} tentativas."
        ) from last_exc

    def _connect_with_timeout(self, creds: VCenterCredentials) -> vim.ServiceInstance:
        """
        Executa SmartConnect em thread separada e aplica timeout.
        Isso evita que um vCenter irresponsivo bloqueie a thread de evento do FastAPI.
        """
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(_do_smart_connect, creds)
            try:
                return future.result(timeout=self._connect_timeout_sec)
            except concurrent.futures.TimeoutError as exc:
                raise TimeoutError(
                    f"Timeout de {self._connect_timeout_sec}s ao conectar em "
                    f"{creds.host}:{creds.port}."
                ) from exc

    @staticmethod
    def _is_alive(si: vim.ServiceInstance) -> bool:
        """Verifica se a sessão ainda está ativa via heartbeat leve."""
        try:
            si.RetrieveContent().about  # chamada mínima à API
            return True
        except Exception:
            return False

    @staticmethod
    def _safe_disconnect_slot(slot: _PoolSlot) -> None:
        if slot.si is not None:
            try:
                Disconnect(slot.si)
            except Exception as exc:
                logger.debug("Erro ao desconectar vCenter %d: %s", slot.vcenter_id, exc)
            finally:
                slot.si = None


# ---------------------------------------------------------------------------
# Função auxiliar executada na thread do executor
# ---------------------------------------------------------------------------

def _do_smart_connect(creds: VCenterCredentials) -> vim.ServiceInstance:
    """Realiza o SmartConnect de forma síncrona (chamado via executor)."""
    ssl_context: ssl.SSLContext | None = None

    if creds.disable_ssl_verify:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE

    return SmartConnect(
        host=creds.host,
        user=creds.username,
        pwd=creds.password,
        port=creds.port,
        sslContext=ssl_context,
    )


# ---------------------------------------------------------------------------
# Singleton global
# ---------------------------------------------------------------------------

vcenter_pool = VCenterConnectionPool(
    connect_timeout_sec=_settings.vcenter_connect_timeout_sec,
    max_retries=_settings.vcenter_max_retries,
    retry_base_delay_sec=_settings.vcenter_retry_base_delay_sec,
)
