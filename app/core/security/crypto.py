"""
Utilitários de criptografia simétrica para senhas de vCenters.

Usa Fernet (AES-128-CBC + HMAC-SHA256) da biblioteca cryptography.
A chave é carregada de FERNET_KEY no .env.

Para gerar uma chave válida:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

from functools import lru_cache
import logging
import os
from pathlib import Path

from cryptography.fernet import Fernet, InvalidToken

from config import get_settings

logger = logging.getLogger(__name__)

_FERNET_PLACEHOLDER_VALUES = {
    "",
    "TROQUE_ESTA_CHAVE_FERNET",
}


class CryptoError(Exception):
    """Falha ao cifrar ou decifrar um dado."""


def _load_or_create_fallback_key() -> str:
    """
    Resolve uma chave de fallback estavel para desenvolvimento.

    Se FERNET_KEY nao estiver configurada (ou estiver no placeholder), usamos
    um arquivo local persistente para evitar invalidar senhas cifradas apos
    restart do processo.
    """
    key_path = Path(os.getenv("FERNET_KEY_FILE", ".fernet.key"))
    if key_path.exists():
        existing = key_path.read_text(encoding="utf-8").strip()
        if existing:
            return existing

    generated = Fernet.generate_key().decode()
    try:
        key_path.parent.mkdir(parents=True, exist_ok=True)
        key_path.write_text(generated, encoding="utf-8")
        try:
            os.chmod(key_path, 0o600)
        except OSError:
            # No Windows, chmod pode nao aplicar como esperado.
            pass
        logger.warning(
            "FERNET_KEY ausente/placeholder: fallback local criado em '%s'. "
            "Defina FERNET_KEY explicita em producao.",
            key_path,
        )
    except OSError as exc:
        logger.warning(
            "Falha ao persistir fallback de FERNET_KEY em '%s' (%s). "
            "Usando chave temporaria em memoria (dados podem invalidar apos restart).",
            key_path,
            exc,
        )
    return generated


def _resolve_fernet_key(raw_key: str | bytes) -> str | bytes:
    if isinstance(raw_key, str):
        candidate = raw_key.strip()
        if candidate not in _FERNET_PLACEHOLDER_VALUES:
            return candidate
        return _load_or_create_fallback_key()
    return raw_key


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet:
    key = _resolve_fernet_key(get_settings().fernet_key)
    raw = key.encode() if isinstance(key, str) else key
    try:
        return Fernet(raw)
    except (ValueError, TypeError) as exc:
        raise CryptoError(
            "FERNET_KEY inválida. Gere uma nova chave com: "
            "python -c \"from cryptography.fernet import Fernet; "
            "print(Fernet.generate_key().decode())\""
        ) from exc


def encrypt_password(plain: str) -> str:
    """Cifra uma senha em texto puro e retorna o token Fernet (string)."""
    if not plain:
        raise CryptoError("Senha não pode ser vazia.")
    return _get_fernet().encrypt(plain.encode()).decode()


def decrypt_password(token: str) -> str:
    """Decifra um token Fernet e retorna a senha em texto puro."""
    if not token:
        raise CryptoError("Token de senha não pode ser vazio.")
    try:
        return _get_fernet().decrypt(token.encode()).decode()
    except InvalidToken as exc:
        raise CryptoError(
            "Falha ao decifrar senha: token inválido ou chave FERNET_KEY incorreta."
        ) from exc
