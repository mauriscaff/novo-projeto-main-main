"""
Fachada de alto nível sobre VCenterConnectionPool.

Responsabilidades:
  - Decifrar a senha (Fernet) armazenada no banco antes de passar ao pool
  - Registrar/desregistrar slots no pool quando vCenters são criados/excluídos
  - Fornecer a API legada (get_client, test_connection_async) para o restante da aplicação
  - Executar operações bloqueantes do pyVmomi em thread pool para não travar o event loop

A lógica de pooling, timeout e retry vive em connection.py (VCenterConnectionPool).
"""

from __future__ import annotations

import asyncio
import logging

from app.core.security.crypto import CryptoError, decrypt_password
from app.core.vcenter.client import VCenterClient, VCenterCredentials
from app.core.vcenter.connection import VCenterConnectionError, vcenter_pool
from app.models.vcenter import VCenter

logger = logging.getLogger(__name__)


def _build_creds(vcenter: VCenter) -> VCenterCredentials:
    """Decifra a senha e monta VCenterCredentials."""
    try:
        plain_password = decrypt_password(vcenter.password)
    except CryptoError:
        logger.warning(
            "vCenter '%s': falha ao decifrar senha — "
            "usando valor bruto (pode estar em texto puro).",
            vcenter.name,
        )
        plain_password = vcenter.password  # fallback para senhas legadas em texto puro

    return VCenterCredentials(
        host=vcenter.host,
        username=vcenter.username,
        password=plain_password,
        port=vcenter.port,
        disable_ssl_verify=vcenter.disable_ssl_verify,
    )


class ConnectionManager:
    """
    Fachada que combina VCenterConnectionPool com descriptografia de senhas.
    Mantém a API pública original para não quebrar os módulos de scan.
    """

    # ------------------------------------------------------------------
    # Registro no pool (chamado pelas rotas de CRUD)
    # ------------------------------------------------------------------

    def register(self, vcenter: VCenter) -> None:
        """Registra ou atualiza um vCenter no pool (lazy — não conecta agora)."""
        vcenter_pool.register(vcenter.id, _build_creds(vcenter))

    def disconnect(self, vcenter_id: int) -> None:
        """Remove o slot do pool e encerra a conexão ativa se houver."""
        vcenter_pool.disconnect(vcenter_id)

    def disconnect_all(self) -> None:
        """Encerra todas as conexões (usar no shutdown da aplicação)."""
        vcenter_pool.disconnect_all()

    # ------------------------------------------------------------------
    # Acesso à conexão (chamado pelos módulos de scan)
    # ------------------------------------------------------------------

    def get_client(self, vcenter: VCenter) -> VCenterClient:
        """
        Retorna um VCenterClient com ServiceInstance válido.

        O pool reutiliza a conexão se estiver viva; caso contrário reconecta
        com retry automático (lógica em VCenterConnectionPool).
        """
        creds = _build_creds(vcenter)
        try:
            si = vcenter_pool.get_service_instance(vcenter.id, creds)
        except VCenterConnectionError as exc:
            raise ConnectionError(str(exc)) from exc

        # VCenterClient aceita um SI já aberto; apenas empacota o acesso
        client = VCenterClient(creds)
        client._service_instance = si  # injeta o SI gerenciado pelo pool
        return client

    # ------------------------------------------------------------------
    # Teste pontual de conectividade (não usa o pool — sessão temporária)
    # ------------------------------------------------------------------

    async def test_connection_async(self, vcenter: VCenter) -> dict:
        """
        Abre uma sessão temporária (sem cache) e retorna informações do vCenter.
        Executado em thread pool para não bloquear o event loop.
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self._test_sync, vcenter)

    def _test_sync(self, vcenter: VCenter) -> dict:
        creds = _build_creds(vcenter)
        with VCenterClient(creds) as client:
            return client.test_connection()

    # ------------------------------------------------------------------
    # Diagnóstico
    # ------------------------------------------------------------------

    def pool_status(self) -> dict[int, dict]:
        """Estado atual de cada slot (host, conectado, falhas consecutivas…)."""
        return vcenter_pool.status()


connection_manager = ConnectionManager()
