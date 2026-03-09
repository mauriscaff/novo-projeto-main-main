"""Regressoes basicas de acessibilidade no layout web."""

from __future__ import annotations


def test_base_layout_exposes_keyboard_and_live_region_attributes(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'class="zh-skip-link"' in html
    assert 'href="#zh-main-content"' in html
    assert 'id="zh-sidebar-toggle"' in html
    assert 'aria-controls="zh-sidebar"' in html
    assert 'aria-expanded="false"' in html
    assert 'id="zh-pending-badge"' in html
    assert 'id="zh-vcenter-count"' in html
    assert 'id="zh-global-status-text"' in html
    assert 'id="zh-global-status-icon"' in html


def test_auth_modal_keeps_accessible_error_and_esc_support(client):
    resp = client.get("/")
    assert resp.status_code == 200
    html = resp.text

    assert 'id="zh-auth-modal"' in html
    assert 'data-bs-keyboard="true"' in html
    assert 'id="zh-auth-error"' in html
    assert 'role="alert"' in html
    assert 'aria-live="assertive"' in html
