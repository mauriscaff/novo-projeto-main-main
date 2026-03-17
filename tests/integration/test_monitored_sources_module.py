"""Integracao do modulo de fontes monitoradas (vCenter + OceanStor)."""

from __future__ import annotations

import uuid


def _login_web_session(client) -> None:
    resp = client.post(
        "/api/v1/auth/session/login",
        json={"username": "admin", "password": "P@ssw0rd"},
    )
    assert resp.status_code == 200


def test_monitored_sources_crud_logical_delete_connectivity_and_collection(client):
    _login_web_session(client)
    suffix = uuid.uuid4().hex[:8]

    vc_payload = {
        "source_type": "vcenter",
        "name": f"vc-src-{suffix}",
        "endpoint": f"vc-{suffix}.lab.local:443",
        "username": "administrator@vsphere.local",
        "secret": "vc-secret",
        "is_active": True,
    }
    os_payload = {
        "source_type": "oceanstor",
        "name": f"os-src-{suffix}",
        "endpoint": f"ocean-{suffix}.lab.local:8088",
        "username": "admin",
        "secret": "os-secret",
        "is_active": True,
    }

    create_vc = client.post("/api/v1/monitored-sources/", json=vc_payload)
    assert create_vc.status_code == 201
    vc_item = create_vc.json()
    assert vc_item["source_type"] == "vcenter"
    assert vc_item["name"] == vc_payload["name"]
    assert "secret" not in vc_item
    vc_id = vc_item["id"]

    create_os = client.post("/api/v1/monitored-sources/", json=os_payload)
    assert create_os.status_code == 201
    os_item = create_os.json()
    assert os_item["source_type"] == "oceanstor"
    os_id = os_item["id"]

    listed = client.get("/api/v1/monitored-sources/")
    assert listed.status_code == 200
    listed_ids = {item["id"] for item in listed.json()}
    assert vc_id in listed_ids
    assert os_id in listed_ids

    patched = client.patch(
        f"/api/v1/monitored-sources/{vc_id}",
        json={"name": f"vc-src-{suffix}-edit", "is_active": False},
    )
    assert patched.status_code == 200
    patched_item = patched.json()
    assert patched_item["name"] == f"vc-src-{suffix}-edit"
    assert patched_item["status"] == "disabled"

    connectivity_ok = client.post(f"/api/v1/monitored-sources/{os_id}/test-connectivity")
    assert connectivity_ok.status_code == 200
    connectivity_body = connectivity_ok.json()
    assert connectivity_body["source_id"] == os_id
    assert connectivity_body["status"] in {"online", "offline"}
    assert "collector_stub" in connectivity_body

    marked = client.post(f"/api/v1/monitored-sources/{os_id}/collection/mark", json={})
    assert marked.status_code == 200
    marked_item = marked.json()
    assert marked_item["last_collected_at"] is not None

    deleted = client.delete(f"/api/v1/monitored-sources/{vc_id}")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True

    listed_active = client.get("/api/v1/monitored-sources/")
    assert listed_active.status_code == 200
    active_ids = {item["id"] for item in listed_active.json()}
    assert vc_id not in active_ids
    assert os_id in active_ids

    listed_with_deleted = client.get("/api/v1/monitored-sources/?include_deleted=true")
    assert listed_with_deleted.status_code == 200
    by_id = {item["id"]: item for item in listed_with_deleted.json()}
    assert by_id[vc_id]["is_deleted"] is True
    assert by_id[vc_id]["status"] == "deleted"


def test_monitored_sources_connectivity_stub_offline_rule(client):
    _login_web_session(client)
    suffix = uuid.uuid4().hex[:8]

    create = client.post(
        "/api/v1/monitored-sources/",
        json={
            "source_type": "oceanstor",
            "name": f"os-offline-{suffix}",
            "endpoint": f"os-offline-{suffix}.lab.local",
            "username": "admin",
            "secret": "offline-secret",
            "is_active": True,
        },
    )
    assert create.status_code == 201
    source_id = create.json()["id"]

    tested = client.post(f"/api/v1/monitored-sources/{source_id}/test-connectivity")
    assert tested.status_code == 200
    body = tested.json()
    assert body["reachable"] is False
    assert body["status"] == "offline"


def test_sources_page_and_script_are_integrated(client):
    page = client.get("/sources")
    assert page.status_code == 200
    html = page.text
    assert 'id="zh-sources-feedback"' in html
    assert 'id="zh-sources-table"' in html
    assert '/static/js/sources.js' in html

    script = client.get("/static/js/sources.js")
    assert script.status_code == 200
    js = script.text
    assert 'const SOURCES_API = "/api/v1/monitored-sources"' in js
    assert 'credentials: "same-origin"' in js
    assert "X-API-Key" in js
    assert "TROQUE_ESTA_API_KEY" not in js
