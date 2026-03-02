"""
Utilitários de criptografia simétrica para senhas de vCenters.

Usa Fernet (AES-128-CBC + HMAC-SHA256) da biblioteca cryptography.
A chave é carregada de FERNET_KEY no .env.

Para gerar uma chave válida:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
"""

from functools import lru_cache

from cryptography.fernet import Fernet, InvalidToken

from config import get_settings


class CryptoError(Exception):
    """Falha ao cifrar ou decifrar um dado."""


@lru_cache(maxsize=1)
def _get_fernet() -> Fernet:
    key = get_settings().fernet_key
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
