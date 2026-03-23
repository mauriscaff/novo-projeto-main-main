"""Regressao basica da UI SDRS na tela /balanceamento."""

from __future__ import annotations


def test_balanceamento_focuses_on_sdrs_without_internal_tabs(client):
    resp = client.get("/balanceamento")
    assert resp.status_code == 200

    html = resp.text
    assert 'id="zh-sdrs-rules"' in html
    assert 'id="zh-btn-sdrs-simulate"' in html
    assert 'id="zh-sdrs-table-body"' in html
    assert 'id="zh-sdrs-feedback"' in html
    assert 'id="zh-sdrs-decisions-body"' in html
    assert 'id="zh-sdrs-audit-list"' in html
    assert 'id="zh-sdrs-warn-count"' in html
    assert 'id="zh-balance-right-tabs"' not in html


def test_balanceamento_wires_sdrs_endpoint_and_renderer(client):
    resp = client.get("/balanceamento")
    assert resp.status_code == 200

    html = resp.text
    assert 'const API_SDRS = "/api/v1/capacity/sdrs/recommendations";' in html
    assert 'const SDRS_DATACENTER_SCOPE = "DTC-SGI";' in html
    assert 'function _runSdrsSimulation()' in html
    assert 'function _renderSdrsPlan(payload)' in html
    assert 'params.set("datacenter", SDRS_DATACENTER_SCOPE);' in html
    assert 'payload.decisions ?? []' in html
    assert 'payload.audit_trail ?? []' in html
    assert 'function _statusBadge(status)' in html
    assert 'id="zh-sdrs-select-core"' in html
    assert '_selectDatastoresByPrefixes(["STA", "STB", "STC"])' in html
    assert 'document.getElementById("zh-btn-sdrs-simulate").addEventListener("click", _runSdrsSimulation);' in html

