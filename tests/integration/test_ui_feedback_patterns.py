"""Regressoes da padronizacao de feedback de sistema na UI."""

from __future__ import annotations


def test_feedback_script_is_loaded_on_primary_pages(client):
    routes = ["/", "/scan/results", "/approvals", "/vcenters"]
    for route in routes:
        resp = client.get(route)
        assert resp.status_code == 200
        assert "/static/js/ui_feedback.js" in resp.text


def test_primary_pages_expose_feedback_mount_points(client):
    page_assertions = {
        "/": ['id="zh-loading-overlay"', 'id="zh-error-banner"'],
        "/scan/results": ['id="zh-scan-feedback"'],
        "/approvals": ['id="zh-approvals-feedback"'],
        "/vcenters": ['id="zh-vc-feedback"'],
    }

    for route, expected_ids in page_assertions.items():
        resp = client.get(route)
        assert resp.status_code == 200
        html = resp.text
        for expected_id in expected_ids:
            assert expected_id in html
