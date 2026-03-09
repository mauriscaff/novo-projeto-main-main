"""Testes do bloqueio de ações destrutivas sem aprovação (READONLY_MODE + token)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from app.core.approval import require_write_access


async def test_require_write_access_raises_403_when_readonly_mode():
    """Com READONLY_MODE=true, require_write_access deve levantar 403."""
    with patch("app.core.approval.settings") as mock_settings:
        mock_settings.readonly_mode = True
        request = MagicMock()
        request.headers.get.return_value = ""
        db = MagicMock()
        db.commit = AsyncMock()
        with pytest.raises(HTTPException) as exc_info:
            await require_write_access(
                request=request,
                x_approval_token="any-token",
                db=db,
                user={"sub": "test"},
            )
        assert exc_info.value.status_code == 403
        assert "READONLY_MODE" in str(exc_info.value.detail)
