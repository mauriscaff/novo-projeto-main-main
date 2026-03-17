"""Regressoes da lista de datastores na tela de pos-exclusao."""

from __future__ import annotations


def test_post_exclusion_uses_live_and_known_datastore_sources(client):
    resp = client.get("/operations/post-exclusion-report")
    assert resp.status_code == 200
    html = resp.text

    assert "/api/v1/scan/datastores?source=live" in html
    assert "/api/v1/scan/datastores?source=known" in html
    assert "function mergeDatastoreCatalog(liveRows, knownRows)" in html
    assert "state.knownDatastores = mergeDatastoreCatalog(liveRows, knownRows);" in html


def test_post_exclusion_filters_out_maintenance_and_inaccessible_datastores(client):
    resp = client.get("/operations/post-exclusion-report")
    assert resp.status_code == 200
    html = resp.text

    assert "function isDatastoreEligible(ds)" in html
    assert "maintenance_mode" in html
    assert "maintenance_state" in html
    assert "if (ds.accessible === false) return false;" in html


def test_post_exclusion_totals_are_aggregated_by_scope_not_single_datastore(client):
    resp = client.get("/operations/post-exclusion-report")
    assert resp.status_code == 200
    html = resp.text

    assert "await refreshTotals(null, vcenterHost);" in html
    assert "refreshTotals(null, scope.vcenterHost || null);" in html


def test_post_exclusion_api_headers_do_not_send_legacy_api_key(client):
    resp = client.get("/operations/post-exclusion-report")
    assert resp.status_code == 200
    html = resp.text

    assert 'function apiHeaders()' in html
    assert 'if (window.ZH_API_KEY) headers["X-API-Key"] = window.ZH_API_KEY;' not in html
