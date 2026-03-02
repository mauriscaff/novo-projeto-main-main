"""Testes da geração de URLs vSphere (UI e /folder) — sem vCenter real."""

from __future__ import annotations

import urllib.parse

import pytest

from app.core.scanner.zombie_detector import (
    generate_folder_deeplink,
    generate_vsphere_ui_link,
)


class TestGenerateFolderDeeplink:
    """URL /folder (Broadcom KB 301563) — pasta ou arquivo."""

    def test_extracts_folder_and_file_from_path(self):
        url = generate_folder_deeplink(
            vcenter_host="vcenter.empresa.com",
            datacenter_path="Datacenter-Producao",
            datastore_name="DS_SSD_01",
            vmdk_path="[DS_SSD_01] VM_ANTIGA_01/VM_ANTIGA_01.vmdk",
            link_to_file=True,
        )
        assert "vcenter.empresa.com" in url or "https://vcenter.empresa.com" in url
        assert "/folder/" in url
        assert "dcPath=Datacenter-Producao" in url or urllib.parse.quote("Datacenter-Producao") in url
        assert "dsName=DS_SSD_01" in url

    def test_link_to_file_true_includes_filename(self):
        url = generate_folder_deeplink(
            vcenter_host="vc.local",
            datacenter_path="DC1",
            datastore_name="DS1",
            vmdk_path="[DS1] folder/disk.vmdk",
            link_to_file=True,
        )
        assert "disk.vmdk" in url
        assert "folder" in url

    def test_link_to_file_false_folder_only(self):
        url = generate_folder_deeplink(
            vcenter_host="vc.local",
            datacenter_path="DC1",
            datastore_name="DS1",
            vmdk_path="[DS1] folder/disk.vmdk",
            link_to_file=False,
        )
        assert "/folder/" in url
        assert "dsName=DS1" in url

    def test_invalid_path_returns_empty(self):
        url = generate_folder_deeplink(
            vcenter_host="vc.local",
            datacenter_path="DC1",
            datastore_name="DS1",
            vmdk_path="invalid-no-bracket",
            link_to_file=True,
        )
        assert url == ""

    def test_host_without_http_gets_https(self):
        url = generate_folder_deeplink(
            vcenter_host="vc.local",
            datacenter_path="DC1",
            datastore_name="DS1",
            vmdk_path="[DS1] f/x.vmdk",
            link_to_file=True,
        )
        assert url.startswith("https://")


class TestGenerateVsphereUiLink:
    """Link vSphere HTML5 Client (MoRef + instanceUuid)."""

    def test_contains_extension_and_object_id(self):
        url = generate_vsphere_ui_link(
            vcenter_host="vcenter.empresa.com",
            vcenter_instance_uuid="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            datastore_moref="datastore-101",
        )
        assert "extensionId=" in url
        assert "objectId=urn:vmomi:Datastore:datastore-101:a1b2c3d4-e5f6-7890-abcd-ef1234567890" in url or "urn%3Avmomi%3ADatastore" in url
        assert "navigator=" in url
        assert "/ui/#?" in url

    def test_host_without_http_gets_https(self):
        url = generate_vsphere_ui_link(
            vcenter_host="vc.local",
            vcenter_instance_uuid="uuid",
            datastore_moref="ds-1",
        )
        assert url.startswith("https://")
