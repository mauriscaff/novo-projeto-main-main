"""Regressoes de navegacao mobile e estado ativo da sidebar."""

from __future__ import annotations


def test_base_includes_mobile_sidebar_overlay_and_hooks(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-sidebar-overlay"' in html
    assert "zh-sidebar-overlay" in html
    assert "zhSetSidebarOpen" in html
    assert "sidebarOverlay" in html
    assert "zh-no-scroll" in html


def test_sidebar_active_state_for_scan_and_whitelist(client):
    scan_html = client.get("/scan/results").text
    assert '<a href="/scan/results" class="zh-nav-link active"' in scan_html
    assert '<a href="/whitelist" class="zh-nav-link active"' not in scan_html

    scan_job_html = client.get("/scan/results/job-demo-001").text
    assert '<a href="/scan/results" class="zh-nav-link active"' in scan_job_html

    wl_html = client.get("/scan/results?status=WHITELIST").text
    assert '<a href="/scan/results" class="zh-nav-link active"' not in wl_html
    assert '<a href="/whitelist" class="zh-nav-link active"' in wl_html


def test_sidebar_active_state_for_audit_related_route(client):
    html = client.get("/audit-log").text
    assert '<a href="/audit-log" class="zh-nav-link active"' in html
