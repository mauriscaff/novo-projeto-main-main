"""Regressoes da lista de datastores na tela de pos-exclusao."""

from __future__ import annotations


def test_post_exclusion_prefers_live_datastore_source(client):
    resp = client.get("/operations/post-exclusion-report")
    assert resp.status_code == 200
    html = resp.text

    assert "/api/v1/scan/datastores?source=live" in html
    assert "/api/v1/scan/datastores?source=known" in html


def test_post_exclusion_filters_out_maintenance_and_inaccessible_datastores(client):
    resp = client.get("/operations/post-exclusion-report")
    assert resp.status_code == 200
    html = resp.text

    assert "function isDatastoreEligible(ds)" in html
    assert "maintenance_mode" in html
    assert "maintenance_state" in html
    assert "if (ds.accessible === false) return false;" in html
