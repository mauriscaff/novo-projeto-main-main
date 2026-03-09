from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.core.security import crypto


@pytest.fixture(autouse=True)
def _clear_fernet_cache():
    crypto._get_fernet.cache_clear()
    yield
    crypto._get_fernet.cache_clear()


def test_placeholder_key_uses_persistent_fallback_file(monkeypatch, tmp_path):
    key_file = tmp_path / "fernet-local.key"
    monkeypatch.setenv("FERNET_KEY_FILE", str(key_file))
    monkeypatch.setattr(
        crypto,
        "get_settings",
        lambda: SimpleNamespace(fernet_key="TROQUE_ESTA_CHAVE_FERNET"),
    )
    token = crypto.encrypt_password("super-secret")
    assert key_file.exists()

    # Simula restart de processo limpando cache do Fernet.
    plain = crypto.decrypt_password(token)
    assert plain == "super-secret"


def test_explicit_invalid_fernet_key_raises_crypto_error(monkeypatch):
    monkeypatch.setattr(
        crypto,
        "get_settings",
        lambda: SimpleNamespace(fernet_key="invalid-key"),
    )
    crypto._get_fernet.cache_clear()

    with pytest.raises(crypto.CryptoError):
        crypto.encrypt_password("pwd")
