from __future__ import annotations

import logging

import pytest
from fastapi import HTTPException
from fastapi.security import HTTPAuthorizationCredentials

from app import dependencies
from app.api.routes import scanner as scanner_route


def test_parse_filter_datetime_invalid_logs_warning(caplog):
    with caplog.at_level(logging.WARNING, logger=scanner_route.__name__):
        parsed = scanner_route._parse_filter_datetime(
            "2026-99-99",
            param_name="modified_after",
            end_of_day=False,
        )
    assert parsed is None
    assert "Filtro de data ignorado" in caplog.text
    assert "modified_after" in caplog.text


def test_parse_filter_scan_date_invalid_logs_warning(caplog):
    with caplog.at_level(logging.WARNING, logger=scanner_route.__name__):
        parsed = scanner_route._parse_filter_scan_date("2026-02-99")
    assert parsed is None
    assert "scan_date" in caplog.text


@pytest.mark.asyncio
async def test_get_current_user_logs_warning_for_invalid_bearer_token(caplog):
    creds = HTTPAuthorizationCredentials(scheme="Bearer", credentials="token-invalido")
    with caplog.at_level(logging.WARNING, logger=dependencies.__name__):
        with pytest.raises(HTTPException) as exc_info:
            await dependencies.get_current_user(
                bearer=creds,
                api_key=None,
                jwt_from_cookie=None,
            )

    assert exc_info.value.status_code == 401
    assert "Falha ao validar JWT (bearer)" in caplog.text
