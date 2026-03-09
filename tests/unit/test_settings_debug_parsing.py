from config import Settings


def test_debug_accepts_release_alias_as_false():
    assert Settings(debug="release").debug is False


def test_debug_accepts_dev_alias_as_true():
    assert Settings(debug="development").debug is True


def test_default_database_url_uses_absolute_project_path(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    settings = Settings(_env_file=None)
    assert settings.database_url.startswith("sqlite+aiosqlite:///")
    assert settings.database_url.endswith("/vmdk_scanner.db")
    assert "/./" not in settings.database_url


def test_relative_database_url_is_normalized_to_absolute_path():
    settings = Settings(_env_file=None, database_url="sqlite+aiosqlite:///./custom_scan.db")
    assert settings.database_url.startswith("sqlite+aiosqlite:///")
    assert settings.database_url.endswith("/custom_scan.db")
    assert "/./" not in settings.database_url
