"""Regressao basica do layout do dashboard (HTML server-side)."""

from __future__ import annotations


def test_dashboard_has_critical_kpis_and_empty_cta(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-card-total-vmdks"' in html
    assert 'id="zh-card-total-gb"' in html
    assert 'id="zh-card-vcenter-failed"' in html
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
