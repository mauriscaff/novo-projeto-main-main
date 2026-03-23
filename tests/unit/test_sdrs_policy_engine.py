from __future__ import annotations

import json

from app.core.sdrs_policy_engine import build_sdrs_policy_input, evaluate_sdrs_policy


def _vm(
    name: str,
    size_gb: float,
    *,
    independent: bool = False,
    override_mode: str = "fullyautomated",
    keep_together: bool = True,
) -> dict:
    return {
        "name": name,
        "path": f"[DS] {name}/{name}.vmdk",
        "committed_gb": size_gb,
        "sdrs_policy": {
            "has_independent_disk": independent,
            "vm_override_mode": override_mode,
            "keep_vmdks_together": keep_together,
        },
    }


def _ds(
    name: str,
    *,
    use_pct: float,
    free_gb: float,
    used_gb: float,
    capacity_gb: float = 100.0,
    top_vms: list[dict] | None = None,
    accessible: bool = True,
    in_datastore_cluster: bool = True,
    datastore_type: str = "VMFS",
) -> dict:
    return {
        "name": name,
        "accessible": accessible,
        "capacity_gb": capacity_gb,
        "free_gb": free_gb,
        "used_gb": used_gb,
        "use_pct": use_pct,
        "in_datastore_cluster": in_datastore_cluster,
        "datastore_cluster": "POD_A",
        "datastore_type": datastore_type,
        "top_vms": top_vms or [],
    }


def test_engine_recommendation_contains_decision_and_audit_payload():
    payload = [
        _ds("DS_HOT", use_pct=90.0, free_gb=10.0, used_gb=90.0, top_vms=[_vm("vm-01", 8.0)]),
        _ds("DS_COLD", use_pct=30.0, free_gb=70.0, used_gb=30.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=10,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    assert result.summary["recommendations"] >= 1
    assert result.recommendations[0]["reason_code"] == "SOURCE_OVER_THRESHOLD"

    recommend_decisions = [d for d in result.decisions if d.status == "recommend"]
    assert recommend_decisions
    assert recommend_decisions[0].reason_code == "SOURCE_OVER_THRESHOLD"
    assert recommend_decisions[0].audit_payload["event"] == "sdrs_policy_decision"


def test_engine_blocks_datastore_outside_cluster():
    payload = [
        _ds("DS_A", use_pct=88.0, free_gb=12.0, used_gb=88.0, in_datastore_cluster=False, top_vms=[_vm("vm-a", 6.0)]),
        _ds("DS_B", use_pct=40.0, free_gb=60.0, used_gb=40.0, in_datastore_cluster=False),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=10,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    assert result.summary["recommendations"] == 0
    assert any(item["reason_code"] == "NOT_IN_DATASTORE_CLUSTER" for item in result.blocked_sources)
    assert any(item.reason_code == "NOT_IN_DATASTORE_CLUSTER" for item in result.decisions if item.status == "block")


def test_engine_respects_vm_override_and_independent_disks():
    payload = [
        _ds(
            "DS_SRC",
            use_pct=92.0,
            free_gb=8.0,
            used_gb=92.0,
            top_vms=[
                _vm("vm-ind", 4.0, independent=True),
                _vm("vm-manual", 3.0, override_mode="manual"),
            ],
        ),
        _ds("DS_DST", use_pct=30.0, free_gb=70.0, used_gb=30.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=10,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    reason_codes = {item["reason_code"] for item in result.blocked_sources}
    assert "INDEPENDENT_DISK_BLOCKED" in reason_codes
    assert "VM_OVERRIDE_BLOCKED" in reason_codes


def test_engine_output_is_json_serializable():
    payload = [
        _ds("DS_HOT", use_pct=90.0, free_gb=10.0, used_gb=90.0, top_vms=[_vm("vm-01", 8.0)]),
        _ds("DS_COLD", use_pct=30.0, free_gb=70.0, used_gb=30.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope={"ds_hot", "ds_cold"},
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=5,
        mode="approval",
    )

    result = evaluate_sdrs_policy(input_data)
    raw = result.model_dump()

    assert isinstance(raw, dict)
    json.dumps(raw)

def test_engine_emits_warn_for_datastore_near_threshold():
    payload = [
        _ds("DS_WARN", use_pct=78.0, free_gb=22.0, used_gb=78.0),
        _ds("DS_OK", use_pct=40.0, free_gb=60.0, used_gb=40.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=5,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    warn_decisions = [d for d in result.decisions if d.status == "warn"]
    assert warn_decisions
    assert warn_decisions[0].reason_code == "NEAR_UTILIZATION_THRESHOLD"

def test_engine_blocks_cluster_incompatible_with_policy():
    payload = [
        _ds("DS_A", use_pct=88.0, free_gb=12.0, used_gb=88.0),
        _ds("DS_B", use_pct=40.0, free_gb=60.0, used_gb=40.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=10,
        mode="recommendation",
        allowed_datastore_clusters={"POD_X"},
    )

    result = evaluate_sdrs_policy(input_data)

    assert result.summary["recommendations"] == 0
    assert any(item["reason_code"] == "CLUSTER_POLICY_INCOMPATIBLE" for item in result.blocked_sources)


def test_engine_blocks_missing_connectivity_even_if_accessible():
    payload = [
        {
            **_ds("DS_A", use_pct=88.0, free_gb=12.0, used_gb=88.0, top_vms=[_vm("vm-a", 5.0)]),
            "connectivity_ok": False,
        },
        _ds("DS_B", use_pct=40.0, free_gb=60.0, used_gb=40.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=10,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    assert any(item["reason_code"] == "CONNECTIVITY_REQUIRED_MISSING" for item in result.blocked_sources)


def test_engine_blocks_when_margin_is_insufficient():
    payload = [
        _ds(
            "DS_SRC",
            use_pct=96.0,
            free_gb=4.0,
            used_gb=96.0,
            top_vms=[_vm("vm-big", 15.0)],
        ),
        _ds(
            "DS_TGT",
            use_pct=75.0,
            free_gb=25.0,
            used_gb=75.0,
        ),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=95.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=12.0,
        capacity_buffer_pct=0.0,
        max_moves=10,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    assert result.summary["recommendations"] == 0
    assert any(item["reason_code"] == "CAPACITY_MARGIN_INSUFFICIENT" for item in result.blocked_sources)


def test_engine_exposes_risk_and_rule_on_recommendation_payload():
    payload = [
        _ds("DS_HOT", use_pct=90.0, free_gb=10.0, used_gb=90.0, top_vms=[_vm("vm-01", 8.0)]),
        _ds("DS_COLD", use_pct=30.0, free_gb=70.0, used_gb=30.0),
    ]
    input_data = build_sdrs_policy_input(
        payload,
        selected_scope=set(),
        utilization_threshold_pct=80.0,
        io_latency_threshold_ms=15.0,
        operational_margin_pct=20.0,
        capacity_buffer_pct=12.0,
        max_moves=10,
        mode="recommendation",
    )

    result = evaluate_sdrs_policy(input_data)

    assert result.recommendations
    rec = result.recommendations[0]
    assert rec["risk_detected"]
    assert rec["rule_triggered"] == "SOURCE_OVER_THRESHOLD_AND_TARGET_WITH_MARGIN"
