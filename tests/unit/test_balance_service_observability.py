from __future__ import annotations

import logging

from app.services.balance_service import _extract_vm_sdrs_policy


class _VmBrokenHardware:
    name = "vm-broken-hw"

    @property
    def config(self):
        raise RuntimeError("config unavailable")


class _VmBrokenOverride:
    name = "vm-broken-override"

    class _Config:
        class _Hardware:
            device: list[object] = []

        hardware = _Hardware()

    config = _Config()

    @property
    def storageDrsVmConfig(self):
        raise RuntimeError("override unavailable")


def test_extract_vm_sdrs_policy_logs_warning_on_hardware_parse_error(caplog):
    with caplog.at_level(logging.WARNING):
        policy = _extract_vm_sdrs_policy(_VmBrokenHardware())

    assert policy["has_independent_disk"] is False
    assert "vm_independent_disk_parse_failed" in caplog.text


def test_extract_vm_sdrs_policy_logs_warning_on_override_parse_error(caplog):
    with caplog.at_level(logging.WARNING):
        policy = _extract_vm_sdrs_policy(_VmBrokenOverride())

    assert policy["vm_override_mode"] == "unknown"
    assert policy["keep_vmdks_together"] is True
    assert "vm_override_parse_failed" in caplog.text
