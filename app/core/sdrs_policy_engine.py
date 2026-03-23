"""SDRS recommendation policy engine (recommendation-only).

This module is framework-agnostic and returns serializable structures that can
be consumed by API/UI layers.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


CONSTRAINTS_APPLIED = [
    "recommend_only",
    "datastore_cluster_only",
    "cluster_policy_compatible",
    "connectivity_required",
    "readonly_mode_guard",
    "keep_vmdks_together_respected",
    "vm_override_respected",
    "independent_disks_blocked",
    "operational_margin_enforced",
    "space_primary_io_secondary",
]


class SdrsVmPolicy(BaseModel):
    has_independent_disk: bool = False
    vm_override_mode: str = "unknown"
    keep_vmdks_together: bool = True


class SdrsVmCandidate(BaseModel):
    name: str = "(vm-sem-nome)"
    path: str = ""
    committed_gb: float = 0.0
    sdrs_policy: SdrsVmPolicy = Field(default_factory=SdrsVmPolicy)


class SdrsDatastoreState(BaseModel):
    name: str
    accessible: bool = False
    connectivity_ok: bool = True
    capacity_gb: float = 0.0
    free_gb: float = 0.0
    used_gb: float = 0.0
    use_pct: float = 0.0
    top_vms: list[SdrsVmCandidate] = Field(default_factory=list)
    datastore_cluster: str | None = None
    in_datastore_cluster: bool = False
    datastore_type: str = ""


class SdrsPolicyEngineInput(BaseModel):
    datastores: list[SdrsDatastoreState]
    selected_scope: set[str] = Field(default_factory=set)
    utilization_threshold_pct: float = 80.0
    io_latency_threshold_ms: float = 15.0
    operational_margin_pct: float = 20.0
    capacity_buffer_pct: float = 12.0
    max_moves: int = 10
    mode: Literal["recommendation", "approval", "execution"] = "recommendation"
    allowed_datastore_clusters: set[str] = Field(default_factory=set)


class SdrsPolicyDecision(BaseModel):
    status: Literal["recommend", "warn", "block"]
    reason_code: str
    explanation_text: str
    actions_suggested: list[str] = Field(default_factory=list)
    audit_payload: dict[str, Any] = Field(default_factory=dict)
    source_datastore: str | None = None
    target_datastore: str | None = None
    vm_name: str | None = None
    risk_detected: str | None = None
    rule_triggered: str | None = None


class SdrsPolicyEngineResult(BaseModel):
    summary: dict[str, Any]
    recommendations: list[dict[str, Any]]
    blocked_sources: list[dict[str, Any]]
    notes: list[str]
    decisions: list[SdrsPolicyDecision]


def _blocked_source(
    source_datastore: str,
    reason_code: str,
    explanation_text: str,
    *,
    use_pct: float | None = None,
    vm_name: str | None = None,
    risk_detected: str | None = None,
    rule_triggered: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "source_datastore": source_datastore,
        "reason_code": reason_code,
        "explanation_text": explanation_text,
        "risk_detected": risk_detected or "operational_risk",
        "rule_triggered": rule_triggered or reason_code,
    }
    if use_pct is not None:
        payload["use_pct"] = round(float(use_pct), 2)
    if vm_name:
        payload["vm_name"] = vm_name
    return payload


def _classify_space_risk(use_pct: float, threshold_pct: float) -> str:
    if use_pct >= max(95.0, threshold_pct + 10.0):
        return "critical"
    if use_pct >= threshold_pct:
        return "high"
    if use_pct >= max(0.0, threshold_pct - 5.0):
        return "medium"
    return "low"


def _select_target(
    targets: list[SdrsDatastoreState],
    vm_size_gb: float,
    utilization_threshold_pct: float,
    operational_margin_pct: float,
) -> tuple[SdrsDatastoreState | None, float, float, str]:
    reason = "INSUFFICIENT_FREE_CAPACITY"

    for candidate in sorted(targets, key=lambda row: float(row.free_gb or 0.0), reverse=True):
        capacity_gb = float(candidate.capacity_gb or 0.0)
        used_gb = float(candidate.used_gb or 0.0)
        free_gb = float(candidate.free_gb or 0.0)
        if capacity_gb <= 0 or free_gb < vm_size_gb:
            continue

        predicted_pct = round(((used_gb + vm_size_gb) / capacity_gb) * 100.0, 2)
        predicted_free_pct = round(100.0 - predicted_pct, 2)

        if predicted_pct > utilization_threshold_pct:
            reason = "TARGET_THRESHOLD_EXCEEDED"
            continue
        if predicted_free_pct < operational_margin_pct:
            reason = "OPERATIONAL_MARGIN_UNDERRUN"
            continue
        return candidate, predicted_pct, predicted_free_pct, "OK"

    return None, 0.0, 0.0, reason


def _to_datastore_state(raw: dict[str, Any]) -> SdrsDatastoreState | None:
    name = str(raw.get("name") or "").strip()
    if not name:
        return None

    vm_rows = []
    for vm in list(raw.get("top_vms") or []):
        policy = vm.get("sdrs_policy") or {}
        vm_rows.append(
            SdrsVmCandidate(
                name=str(vm.get("name") or "").strip() or "(vm-sem-nome)",
                path=str(vm.get("path") or ""),
                committed_gb=float(vm.get("committed_gb") or 0.0),
                sdrs_policy=SdrsVmPolicy(
                    has_independent_disk=bool(policy.get("has_independent_disk", False)),
                    vm_override_mode=str(policy.get("vm_override_mode") or "unknown"),
                    keep_vmdks_together=bool(policy.get("keep_vmdks_together", True)),
                ),
            )
        )

    return SdrsDatastoreState(
        name=name,
        accessible=bool(raw.get("accessible", False)),
        connectivity_ok=bool(raw.get("connectivity_ok", raw.get("accessible", False))),
        capacity_gb=float(raw.get("capacity_gb") or 0.0),
        free_gb=float(raw.get("free_gb") or 0.0),
        used_gb=float(raw.get("used_gb") or 0.0),
        use_pct=float(raw.get("use_pct") or 0.0),
        top_vms=vm_rows,
        datastore_cluster=str(raw.get("datastore_cluster") or "").strip() or None,
        in_datastore_cluster=bool(raw.get("in_datastore_cluster", False)),
        datastore_type=str(raw.get("datastore_type") or "").strip(),
    )


def build_sdrs_policy_input(
    datastores: list[dict[str, Any]],
    *,
    selected_scope: set[str],
    utilization_threshold_pct: float,
    io_latency_threshold_ms: float,
    operational_margin_pct: float,
    capacity_buffer_pct: float,
    max_moves: int,
    mode: Literal["recommendation", "approval", "execution"],
    allowed_datastore_clusters: set[str] | None = None,
) -> SdrsPolicyEngineInput:
    rows: list[SdrsDatastoreState] = []
    for ds in datastores:
        row = _to_datastore_state(ds)
        if row is None:
            continue
        if selected_scope and row.name.lower() not in selected_scope:
            continue
        rows.append(row)

    normalized_allowed_clusters = {
        part.strip().lower()
        for part in (allowed_datastore_clusters or set())
        if part and part.strip()
    }

    return SdrsPolicyEngineInput(
        datastores=rows,
        selected_scope=selected_scope,
        utilization_threshold_pct=utilization_threshold_pct,
        io_latency_threshold_ms=io_latency_threshold_ms,
        operational_margin_pct=operational_margin_pct,
        capacity_buffer_pct=capacity_buffer_pct,
        max_moves=max_moves,
        mode=mode,
        allowed_datastore_clusters=normalized_allowed_clusters,
    )


def evaluate_sdrs_policy(input_data: SdrsPolicyEngineInput) -> SdrsPolicyEngineResult:
    considered = [row.model_copy(deep=True) for row in input_data.datastores]
    blocked_sources: list[dict[str, Any]] = []
    decisions: list[SdrsPolicyDecision] = []

    notes = [
        "Modo recomendacao: nenhuma migracao e executada automaticamente.",
        "Espaco e a regra principal; I/O e secundario/contextual nesta fase.",
        "Toda recomendacao considera margem operacional e buffer de capacidade.",
    ]

    eligible: list[SdrsDatastoreState] = []
    blocked_by_reason: dict[str, int] = {}

    def _register_block(
        source_datastore: str,
        reason_code: str,
        explanation_text: str,
        *,
        use_pct: float | None = None,
        vm_name: str | None = None,
        risk_detected: str | None = None,
        rule_triggered: str | None = None,
    ) -> None:
        blocked = _blocked_source(
            source_datastore,
            reason_code,
            explanation_text,
            use_pct=use_pct,
            vm_name=vm_name,
            risk_detected=risk_detected,
            rule_triggered=rule_triggered,
        )
        blocked_sources.append(blocked)
        blocked_by_reason[reason_code] = blocked_by_reason.get(reason_code, 0) + 1
        decisions.append(
            SdrsPolicyDecision(
                status="block",
                reason_code=reason_code,
                explanation_text=explanation_text,
                actions_suggested=[
                    "review_constraints",
                    "manual_operator_action_required",
                ],
                audit_payload={
                    "event": "sdrs_policy_decision",
                    "status": "block",
                    "reason_code": reason_code,
                    "source_datastore": source_datastore,
                    "vm_name": vm_name,
                    "mode": input_data.mode,
                },
                source_datastore=source_datastore,
                vm_name=vm_name,
                risk_detected=risk_detected,
                rule_triggered=rule_triggered or reason_code,
            )
        )

    for row in considered:
        row_cluster = (row.datastore_cluster or "").strip().lower()

        if not row.accessible:
            _register_block(
                row.name,
                "DATASTORE_INACCESSIBLE",
                "Datastore inacessivel no momento da avaliacao.",
                use_pct=row.use_pct,
                risk_detected="connectivity_or_access_unavailable",
                rule_triggered="datastore_accessible_required",
            )
            continue

        if not row.connectivity_ok:
            _register_block(
                row.name,
                "CONNECTIVITY_REQUIRED_MISSING",
                "Conectividade necessaria ausente para avaliacao segura de SDRS.",
                use_pct=row.use_pct,
                risk_detected="connectivity_validation_failed",
                rule_triggered="connectivity_required",
            )
            continue

        if not row.in_datastore_cluster:
            _register_block(
                row.name,
                "NOT_IN_DATASTORE_CLUSTER",
                "SDRS opera somente dentro de Datastore Cluster.",
                use_pct=row.use_pct,
                risk_detected="scope_not_supported",
                rule_triggered="datastore_cluster_only",
            )
            continue

        if input_data.allowed_datastore_clusters and row_cluster not in input_data.allowed_datastore_clusters:
            _register_block(
                row.name,
                "CLUSTER_POLICY_INCOMPATIBLE",
                "Cluster do datastore incompativel com a politica SDRS configurada.",
                use_pct=row.use_pct,
                risk_detected="policy_scope_mismatch",
                rule_triggered="allowed_datastore_clusters",
            )
            continue

        eligible.append(row)

    datastore_types = {
        str(row.datastore_type or "").strip().lower()
        for row in eligible
        if str(row.datastore_type or "").strip()
    }
    if len(datastore_types) > 1:
        for row in eligible:
            _register_block(
                row.name,
                "MIXED_DATASTORE_TYPES",
                "Tipos de datastore heterogeneos no mesmo escopo de SDRS.",
                use_pct=row.use_pct,
                risk_detected="heterogeneous_storage_profile",
                rule_triggered="homogeneous_datastore_type_required",
            )
        notes.append("Escopo bloqueado por tipos de datastore mistos (homogeneidade obrigatoria).")

    if len(eligible) < 2 or len(datastore_types) > 1:
        return SdrsPolicyEngineResult(
            summary={
                "datastores_considered": len(considered),
                "eligible_datastores": len(eligible),
                "sources_over_threshold": 0,
                "warnings": 0,
                "recommendations": 0,
                "blocked_sources": len(blocked_sources),
                "blocked_by_reason": blocked_by_reason,
            },
            recommendations=[],
            blocked_sources=blocked_sources,
            notes=notes,
            decisions=decisions,
        )

    sources = sorted(
        [row for row in eligible if row.use_pct >= input_data.utilization_threshold_pct],
        key=lambda row: row.use_pct,
        reverse=True,
    )
    targets = sorted(
        [row for row in eligible if row.use_pct < input_data.utilization_threshold_pct],
        key=lambda row: row.free_gb,
        reverse=True,
    )

    recommendations: list[dict[str, Any]] = []
    warnings_count = 0

    near_threshold_floor = max(0.0, input_data.utilization_threshold_pct - 5.0)
    for row in eligible:
        if near_threshold_floor <= row.use_pct < input_data.utilization_threshold_pct:
            warnings_count += 1
            warn_risk = _classify_space_risk(row.use_pct, input_data.utilization_threshold_pct)
            decisions.append(
                SdrsPolicyDecision(
                    status="warn",
                    reason_code="NEAR_UTILIZATION_THRESHOLD",
                    explanation_text=(
                        f"Datastore proximo do threshold ({row.use_pct:.2f}% de uso); "
                        "monitorar crescimento e snapshot/consolidacao."
                    ),
                    actions_suggested=[
                        "monitor_growth_trend",
                        "review_capacity_buffer",
                    ],
                    audit_payload={
                        "event": "sdrs_policy_decision",
                        "status": "warn",
                        "reason_code": "NEAR_UTILIZATION_THRESHOLD",
                        "source_datastore": row.name,
                        "mode": input_data.mode,
                    },
                    source_datastore=row.name,
                    risk_detected=f"space_risk_{warn_risk}",
                    rule_triggered="near_utilization_threshold",
                )
            )

    for source in sources:
        if len(recommendations) >= input_data.max_moves:
            break

        source_capacity = float(source.capacity_gb or 0.0)
        source_used = float(source.used_gb or 0.0)
        threshold_used = (input_data.utilization_threshold_pct / 100.0) * source_capacity
        source_excess_gb = max(0.0, round(source_used - threshold_used, 3))

        source_cluster = (source.datastore_cluster or "").strip().lower()
        target_pool = [
            row for row in targets
            if row.name != source.name and (row.datastore_cluster or "").strip().lower() == source_cluster
        ]

        candidate_vms = sorted(
            source.top_vms or [],
            key=lambda row: float(row.committed_gb or 0.0),
            reverse=True,
        )
        if not candidate_vms:
            _register_block(
                source.name,
                "NO_VM_CANDIDATES",
                "Sem candidatas no top_vms para recomendar Storage vMotion.",
                use_pct=source.use_pct,
                risk_detected="insufficient_vm_signal",
                rule_triggered="top_vms_required_for_recommendation",
            )
            continue

        if not target_pool:
            _register_block(
                source.name,
                "NO_ELIGIBLE_TARGET",
                "Nao ha datastore de destino elegivel no mesmo Datastore Cluster da fonte.",
                use_pct=source.use_pct,
                risk_detected="no_target_in_same_cluster",
                rule_triggered="same_datastore_cluster_required",
            )
            continue

        source_moves = 0
        source_block_reasons: set[str] = set()
        for vm in candidate_vms:
            if len(recommendations) >= input_data.max_moves or source_excess_gb <= 0:
                break

            vm_name = str(vm.name or "").strip() or "(vm-sem-nome)"
            vm_size_gb = float(vm.committed_gb or 0.0)
            if vm_size_gb <= 0:
                continue

            if bool(vm.sdrs_policy.has_independent_disk):
                _register_block(
                    source.name,
                    "INDEPENDENT_DISK_BLOCKED",
                    "VM com disco independente nao e elegivel para automacao live SDRS.",
                    use_pct=source.use_pct,
                    vm_name=vm_name,
                    risk_detected="migration_not_supported_for_independent_disk",
                    rule_triggered="independent_disk_block",
                )
                continue

            override_mode = str(vm.sdrs_policy.vm_override_mode or "unknown").lower()
            if override_mode in {"manual", "disabled"}:
                _register_block(
                    source.name,
                    "VM_OVERRIDE_BLOCKED",
                    "VM override em Manual/Disabled prevalece sobre automacao do cluster.",
                    use_pct=source.use_pct,
                    vm_name=vm_name,
                    risk_detected="vm_policy_override",
                    rule_triggered="vm_override_precedence",
                )
                continue

            keep_together = bool(vm.sdrs_policy.keep_vmdks_together)
            if not keep_together:
                _register_block(
                    source.name,
                    "AFFINITY_RULE_BLOCKED",
                    "Regra de afinidade/keep VMDKs together impede esta recomendacao automatica.",
                    use_pct=source.use_pct,
                    vm_name=vm_name,
                    risk_detected="affinity_constraint",
                    rule_triggered="keep_vmdks_together",
                )
                continue

            projected_move_gb = round(vm_size_gb * (1.0 + (input_data.capacity_buffer_pct / 100.0)), 3)
            target, predicted_pct, predicted_free_pct, target_reason = _select_target(
                target_pool,
                projected_move_gb,
                input_data.utilization_threshold_pct,
                input_data.operational_margin_pct,
            )
            if target is None:
                source_block_reasons.add(target_reason)
                continue

            source_risk_before = _classify_space_risk(source.use_pct, input_data.utilization_threshold_pct)
            target_risk_after = _classify_space_risk(predicted_pct, input_data.utilization_threshold_pct)
            risk_detected = (
                f"source_space_risk={source_risk_before};"
                f"target_projected_risk={target_risk_after};"
                f"snapshot_consolidation_swap_buffer={input_data.capacity_buffer_pct:.1f}%"
            )
            rule_triggered = "SOURCE_OVER_THRESHOLD_AND_TARGET_WITH_MARGIN"
            recommendation_id = f"{source.name}->{target.name}::{vm_name}".replace(" ", "_")
            explanation_text = (
                f"Fonte acima de {input_data.utilization_threshold_pct:.1f}% e destino com margem >= "
                f"{input_data.operational_margin_pct:.1f}% apos buffer de capacidade "
                f"({input_data.capacity_buffer_pct:.1f}%)."
            )

            recommendations.append(
                {
                    "recommendation_id": recommendation_id,
                    "mode": input_data.mode,
                    "source_datastore": source.name,
                    "target_datastore": target.name,
                    "vm_name": vm_name,
                    "vm_path": vm.path or "",
                    "estimated_move_gb": round(projected_move_gb, 3),
                    "source_use_pct_before": round(source.use_pct, 2),
                    "target_use_pct_after": predicted_pct,
                    "target_free_pct_after": predicted_free_pct,
                    "reason_code": "SOURCE_OVER_THRESHOLD",
                    "explanation_text": explanation_text,
                    "risk_detected": risk_detected,
                    "rule_triggered": rule_triggered,
                    "constraints_applied": CONSTRAINTS_APPLIED,
                    "capacity_context": {
                        "use_current_pct": round(source.use_pct, 2),
                        "use_projected_pct": predicted_pct,
                        "growth_snapshot_swap_buffer_pct": input_data.capacity_buffer_pct,
                        "io_latency_threshold_ms": input_data.io_latency_threshold_ms,
                        "operational_margin_pct_required": input_data.operational_margin_pct,
                        "operational_margin_reserved_pct": predicted_free_pct,
                    },
                }
            )
            decisions.append(
                SdrsPolicyDecision(
                    status="recommend",
                    reason_code="SOURCE_OVER_THRESHOLD",
                    explanation_text=explanation_text,
                    actions_suggested=[
                        "submit_for_approval",
                        "review_capacity_after_move",
                    ],
                    audit_payload={
                        "event": "sdrs_policy_decision",
                        "status": "recommend",
                        "reason_code": "SOURCE_OVER_THRESHOLD",
                        "source_datastore": source.name,
                        "target_datastore": target.name,
                        "vm_name": vm_name,
                        "mode": input_data.mode,
                    },
                    source_datastore=source.name,
                    target_datastore=target.name,
                    vm_name=vm_name,
                    risk_detected=risk_detected,
                    rule_triggered=rule_triggered,
                )
            )

            source.used_gb = max(0.0, source.used_gb - projected_move_gb)
            source.free_gb = max(0.0, source.capacity_gb - source.used_gb)
            source.use_pct = round((source.used_gb / source.capacity_gb) * 100.0, 2) if source.capacity_gb > 0 else 0.0

            target.used_gb += projected_move_gb
            target.free_gb = max(0.0, target.capacity_gb - target.used_gb)
            target.use_pct = round((target.used_gb / target.capacity_gb) * 100.0, 2) if target.capacity_gb > 0 else 0.0

            source_excess_gb = max(0.0, round(source.used_gb - threshold_used, 3))
            source_moves += 1

        if source_moves == 0:
            if "OPERATIONAL_MARGIN_UNDERRUN" in source_block_reasons:
                _register_block(
                    source.name,
                    "CAPACITY_MARGIN_INSUFFICIENT",
                    "Margem operacional insuficiente para recomendar migracao segura.",
                    use_pct=source.use_pct,
                    risk_detected="operational_margin_breach",
                    rule_triggered="operational_margin_enforced",
                )
            elif "INSUFFICIENT_FREE_CAPACITY" in source_block_reasons:
                _register_block(
                    source.name,
                    "INSUFFICIENT_FREE_CAPACITY",
                    "Capacidade livre insuficiente nos destinos elegiveis.",
                    use_pct=source.use_pct,
                    risk_detected="free_capacity_shortage",
                    rule_triggered="capacity_minimum_required",
                )
            elif "TARGET_THRESHOLD_EXCEEDED" in source_block_reasons:
                _register_block(
                    source.name,
                    "TARGET_THRESHOLD_EXCEEDED",
                    "Destinos candidatos ultrapassariam o threshold de utilizacao.",
                    use_pct=source.use_pct,
                    risk_detected="target_utilization_breach",
                    rule_triggered="target_threshold_guard",
                )
            else:
                _register_block(
                    source.name,
                    "NO_ELIGIBLE_TARGET",
                    "Nenhum destino elegivel com capacidade e margem operacional suficientes.",
                    use_pct=source.use_pct,
                    risk_detected="no_viable_target",
                    rule_triggered="target_selection_guardrails",
                )

    summary = {
        "datastores_considered": len(considered),
        "eligible_datastores": len(eligible),
        "sources_over_threshold": len(sources),
        "warnings": warnings_count,
        "recommendations": len(recommendations),
        "blocked_sources": len(blocked_sources),
        "blocked_by_reason": blocked_by_reason,
    }
    return SdrsPolicyEngineResult(
        summary=summary,
        recommendations=recommendations,
        blocked_sources=blocked_sources,
        notes=notes,
        decisions=decisions,
    )



