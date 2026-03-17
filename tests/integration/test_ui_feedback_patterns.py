"""Regressoes da padronizacao de feedback de sistema na UI."""

from __future__ import annotations


def test_feedback_script_is_loaded_on_primary_pages(client):
    routes = ["/", "/scan/results", "/approvals", "/vcenters", "/audit-log"]
    for route in routes:
        resp = client.get(route)
        assert resp.status_code == 200
        assert "/static/js/ui_feedback.js" in resp.text


def test_primary_pages_expose_feedback_mount_points(client):
    page_assertions = {
        "/": ['id="zh-loading-overlay"', 'id="zh-error-banner"'],
        "/scan/results": ['id="zh-scan-feedback"'],
        "/approvals": ['id="zh-approvals-feedback"', 'id="zh-approvals-guide"', 'id="zh-apv-guide-action"'],
        "/vcenters": ['id="zh-vc-feedback"', 'id="zh-vc-guide"', 'id="zh-vc-guide-action"'],
        "/audit-log": ['id="zh-audit-feedback"'],
    }

    for route, expected_ids in page_assertions.items():
        resp = client.get(route)
        assert resp.status_code == 200
        html = resp.text
        for expected_id in expected_ids:
            assert expected_id in html


def test_approvals_script_uses_structured_feedback_for_critical_actions(client):
    resp = client.get("/static/js/approvals.js")
    assert resp.status_code == 200
    js = resp.text

    assert "function _setActionErrorFeedback" in js
    assert "function _clearActionErrorFeedback" in js
    assert "function _bindOperationalGuide()" in js
    assert "function _updateOperationalGuide(meta = {})" in js
    assert "function _setOperationalGuideState(tone, titleText, nextStep, action = {})" in js
    assert "Dry-run indisponivel" in js
    assert "Confirmacao obrigatoria" in js
    assert "Falha ao cancelar token" in js


def test_vcenters_script_uses_structured_feedback_for_form_and_delete(client):
    resp = client.get("/static/js/vcenters.js")
    assert resp.status_code == 200
    js = resp.text

    assert "function _showFormError(msg, opts = {})" in js
    assert "function _showToast(type, msg, opts = {})" in js
    assert "function _bindOperationalGuide()" in js
    assert "function _updateOperationalGuide(meta = {})" in js
    assert "function _setOperationalGuideState(tone, titleText, nextStep, action = {})" in js
    assert "Falha ao atualizar vCenter" in js
    assert "Falha no teste de conexao" in js
    assert "Falha ao remover vCenter" in js


def test_dashboard_script_uses_structured_feedback_categories(client):
    resp = client.get("/static/js/dashboard.js")
    assert resp.status_code == 200
    js = resp.text

    assert "function _setErrorState(errOrMessage)" in js
    assert "window.zhFeedback.toErrorInfo(errorObj, fallbackMessage)" in js
    assert "auth: \"Valide a sessao de usuario e tente atualizar novamente.\"" in js
    assert "transient: \"Verifique rede/API e clique em Atualizar em alguns segundos.\"" in js
