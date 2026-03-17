"""Regressoes de UX para produtividade na tabela de resultados."""

from __future__ import annotations


def test_scan_results_exposes_quick_filters_and_column_preferences(client):
    resp = client.get("/scan/results")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-qf-size-100"' in html
    assert 'id="zh-qf-last-7d"' in html
    assert 'id="zh-qf-score-85"' in html
    assert 'id="zh-qf-reset"' in html
    assert 'id="zh-col-path"' in html
    assert 'id="zh-col-type"' in html
    assert 'id="zh-col-score"' in html
    assert 'id="zh-col-modified"' in html
    assert 'id="zh-col-status"' in html
    assert 'id="zh-col-reset"' in html
    assert 'id="zh-scan-guide"' in html
    assert 'id="zh-guide-level"' in html
    assert 'id="zh-guide-visible"' in html
    assert 'id="zh-guide-selected"' in html
    assert 'id="zh-guide-action"' in html


def test_scan_results_batch_impact_summary_hooks_exist(client):
    resp = client.get("/scan/results")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-batch-impact"' in html
    assert 'id="batch-impact-count"' in html
    assert 'id="batch-impact-size"' in html
    assert 'id="batch-impact-action"' in html
    assert 'id="batch-impact-warning"' in html


def test_scan_results_script_persists_preferences_and_mobile_details(client):
    resp = client.get("/static/js/scan_results.js")
    assert resp.status_code == 200
    js = resp.text

    assert 'const PREFS_KEY = "zh.scan_results.prefs.v1"' in js
    assert "const TABLE_STATE_KEY = `zh.scan_results.datatable_state.v1:" in js
    assert "stateSave: true" in js
    assert "function _toggleMobileDetailsRow" in js
    assert "function _renderBatchImpactSummary" in js
    assert "function _bindOperationalGuide()" in js
    assert "function _updateOperationalGuide(meta = {})" in js
    assert "function _setOperationalGuideState(tone, titleText, nextStep, action = {})" in js
    assert "async function _apiFetch" in js
    assert 'credentials: "same-origin"' in js
    assert "TROQUE_ESTA_API_KEY" not in js
