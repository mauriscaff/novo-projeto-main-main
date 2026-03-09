"""
Wrapper sobre pyVmomi para conexão e autenticação com um vCenter.
Gerencia o ciclo de vida da sessão SSL/TLS.
"""

import ssl
from dataclasses import dataclass

from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim


@dataclass
class VCenterCredentials:
    host: str
    username: str
    password: str
    port: int = 443
    disable_ssl_verify: bool = True


class VCenterClient:
    """Contexto gerenciado para uma sessão com o vCenter."""

    def __init__(self, credentials: VCenterCredentials) -> None:
        self._creds = credentials
        self._service_instance: vim.ServiceInstance | None = None

    # ------------------------------------------------------------------
    # Protocolo de contexto (with VCenterClient(...) as client)
    # ------------------------------------------------------------------

    def __enter__(self) -> "VCenterClient":
        self.connect()
        return self

    def __exit__(self, *_) -> None:
        self.disconnect()

    # ------------------------------------------------------------------
    # Conexão / Desconexão
    # ------------------------------------------------------------------

    def connect(self) -> None:
        ssl_context: ssl.SSLContext | None = None

        if self._creds.disable_ssl_verify:
            ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE

        self._service_instance = SmartConnect(
            host=self._creds.host,
            user=self._creds.username,
            pwd=self._creds.password,
            port=self._creds.port,
            sslContext=ssl_context,
        )

    def disconnect(self) -> None:
        if self._service_instance:
            Disconnect(self._service_instance)
            self._service_instance = None

    # ------------------------------------------------------------------
    # Acessores
    # ------------------------------------------------------------------

    @property
    def si(self) -> vim.ServiceInstance:
        if not self._service_instance:
            raise RuntimeError("Cliente não conectado. Use connect() primeiro.")
        return self._service_instance

    @property
    def content(self) -> vim.ServiceInstanceContent:
        return self.si.RetrieveContent()

    def get_container_view(
        self,
        obj_type: list,
        container=None,
        recursive: bool = True,
    ):
        """Retorna uma ContainerView para o tipo de objeto especificado."""
        if container is None:
            container = self.content.rootFolder
        return self.content.viewManager.CreateContainerView(
            container=container,
            type=obj_type,
            recursive=recursive,
        )

    def test_connection(self) -> dict:
        """Testa a conectividade e retorna informações básicas do vCenter."""
        about = self.content.about
        return {
            "api_version": about.apiVersion,
            "full_name": about.fullName,
            "instance_uuid": about.instanceUuid,
            "os_type": about.osType,
        }

    def list_datacenters(self) -> list[str]:
        """Retorna os nomes de todos os Datacenters visíveis no vCenter."""
        view = self.get_container_view([vim.Datacenter])
        try:
            return [dc.name for dc in view.view]
        finally:
            view.Destroy()


# ─────────────────────────────────────────────────────────────────────────────
# Função auxiliar assíncrona (wrapper para run_in_executor)
# ─────────────────────────────────────────────────────────────────────────────


async def list_datacenters_async(service_instance) -> list[str]:
    """
    Lista Datacenters de forma assíncrona a partir de um vim.ServiceInstance.
    Executa a chamada pyVmomi em thread pool para não bloquear o event loop.
    """
    import asyncio

    def _sync(si) -> list[str]:
        content = si.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.Datacenter], True
        )
        try:
            return [dc.name for dc in view.view]
        finally:
            view.Destroy()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync, service_instance)


async def list_datastores_async(service_instance) -> list[dict]:
    """
    Lista datastores visíveis no vCenter com estado de acessibilidade e manutenção.
    Executa pyVmomi em thread pool para não bloquear o event loop.
    """
    import asyncio

    def _sync(si) -> list[dict]:
        content = si.RetrieveContent()
        view = content.viewManager.CreateContainerView(
            content.rootFolder, [vim.Datastore], True
        )
        try:
            rows: list[dict] = []
            for ds in view.view:
                name = str(getattr(ds, "name", "") or "").strip()
                if not name:
                    continue
                summary = getattr(ds, "summary", None)
                maintenance_state = str(getattr(summary, "maintenanceMode", "") or "").strip()
                normalized_state = maintenance_state.lower()
                maintenance_mode = bool(
                    normalized_state
                    and normalized_state not in {"normal", "none", "false", "0"}
                )
                accessible = bool(getattr(summary, "accessible", True))
                rows.append(
                    {
                        "name": name,
                        "accessible": accessible,
                        "maintenance_mode": maintenance_mode,
                        "maintenance_state": maintenance_state,
                    }
                )
            return rows
        finally:
            view.Destroy()

    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _sync, service_instance)
