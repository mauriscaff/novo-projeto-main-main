"""Regressao basica do layout do dashboard (HTML server-side)."""

from __future__ import annotations


def test_dashboard_has_critical_kpis_and_empty_cta(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-card-total-vmdks"' in html
    assert 'id="zh-card-total-gb"' in html
    assert 'id="zh-card-vcenter-failed"' in html
    assert 'id="zh-operational-focus"' in html
    assert 'id="zh-focus-level-badge"' in html
    assert 'id="zh-focus-primary-action"' in html
    assert 'data-action="scan"' in html
    assert 'id="zh-empty-state"' in html
    assert 'id="zh-empty-run-scan"' in html

def test_dashboard_groups_technical_sections_in_accordion(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-dashboard-ops-accordion"' in html
    assert 'id="zh-collapse-summary"' in html
    assert 'id="zh-collapse-tech"' in html
    assert 'id="zh-collapse-vcenters"' in html


def test_dashboard_scan_modal_matches_datastores_by_vcenter_id_and_normalized_host(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'function zhNormalizeHost(value)' in html
    assert 'const scope = { ids: new Set(), hosts: new Set(), names: new Set() };' in html
    assert 'const dsVcenterId = Number(ds?.vcenter_id);' in html
    assert 'Number.isFinite(dsVcenterId) && scope.ids.has(dsVcenterId)' in html
    assert 'Number.isFinite(dsVcenterId) ? dsVcenterId : (vcHostNorm || vcNameNorm)' in html
    assert 'function zhMergeDatastoreCatalog(liveRows = [], knownRows = [])' in html
    assert '/api/v1/scan/datastores?source=live' in html
    assert '/api/v1/scan/datastores?source=known' in html
    assert 'zhScanDatastoresRaw = zhMergeDatastoreCatalog(liveRows, knownRows);' in html


def test_dashboard_scan_modal_has_standard_feedback_and_scope_meta(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-scan-inline-error" class="mb-3 d-none" aria-live="polite"' in html
    assert 'id="zh-scan-ds-meta"' in html
    assert 'function zhScanSetLoadingFeedback(title, detail)' in html
    assert 'function zhScanSetErrorFeedback(message, opts = {})' in html
    assert 'function zhUpdateDatastoreMeta(meta = {})' in html
    assert 'window.zhFeedback.setInline(scanInlineError' in html




def test_dashboard_scan_modal_header_builder_does_not_reintroduce_api_key(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'function zhBuildApiHeaders(withJsonContentType = false, accept = "application/json")' in html
    assert 'if (apiKey) headers["X-API-Key"] = apiKey;' not in html
