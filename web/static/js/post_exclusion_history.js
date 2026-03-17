(function () {
  const state = {
    items: [],
    selectedRunId: null,
  };

  function byId(id) {
    return document.getElementById(id);
  }

  function escapeHtml(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/\"/g, "&quot;")
      .replace(/'/g, "&#39;");
  }

  function formatCount(value) {
    return Number(value || 0).toLocaleString("pt-BR");
  }

  function formatGb(value) {
    return `${Number(value || 0).toFixed(3)} GB`;
  }

  function formatDate(value) {
    if (!value) return "-";
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) return value;
    return dt.toLocaleString("pt-BR", {
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    });
  }

  function statusMeta(status) {
    if (status === "datastore_removed") {
      return { cls: "ok", label: "Datastore removido" };
    }
    if (status === "partial_cleanup") {
      return { cls: "warn", label: "Limpeza parcial" };
    }
    return { cls: "neutral", label: "Sem limpeza" };
  }

  function showFeedback(type, message) {
    const el = byId("zh-history-feedback");
    if (!el) return;
    if (!message) {
      el.className = "mb-3 d-none";
      el.innerHTML = "";
      return;
    }
    const map = {
      danger: "alert alert-danger",
      warning: "alert alert-warning",
      success: "alert alert-success",
      info: "alert alert-info",
    };
    el.className = `${map[type] || map.info} mb-3`;
    el.textContent = message;
  }

  function setLoading(loading) {
    const spinner = byId("zh-history-spinner");
    const icon = byId("zh-history-refresh-icon");
    const button = byId("zh-history-refresh");
    if (spinner) spinner.classList.toggle("d-none", !loading);
    if (icon) icon.classList.toggle("d-none", loading);
    if (button) button.disabled = loading;
  }

  async function fetchJson(url) {
    const resp = await fetch(url, {
      headers: { Accept: "application/json" },
      credentials: "same-origin",
    });
    const payload = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw new Error(payload?.detail || `Falha HTTP ${resp.status}`);
    }
    return payload;
  }

  function buildHistoryQuery() {
    const params = new URLSearchParams();
    const datastore = byId("zh-history-datastore")?.value?.trim();
    const vcenter = byId("zh-history-vcenter")?.value?.trim();
    const status = byId("zh-history-status")?.value?.trim();
    const limit = byId("zh-history-limit")?.value?.trim() || "100";

    params.set("limit", limit);
    if (datastore) params.set("datastore", datastore);
    if (vcenter) params.set("vcenter_host", vcenter);
    if (status) params.set("status", status);
    return params;
  }

  function renderSummary(summary) {
    byId("zh-history-total-runs").textContent = formatCount(summary?.total_verifications);
    byId("zh-history-total-removed").textContent = formatCount(summary?.total_datastores_removed);
    byId("zh-history-total-vmdks").textContent = formatCount(summary?.total_deleted_vmdks);
    byId("zh-history-total-gb").textContent = formatGb(summary?.total_deleted_size_gb);
    byId("zh-history-updated-at").textContent = summary?.last_verification_at
      ? `Ultima verificacao: ${formatDate(summary.last_verification_at)}`
      : "Historico sem verificacoes registradas.";
  }

  function renderRows(items) {
    const tbody = byId("zh-history-body");
    if (!tbody) return;
    if (!Array.isArray(items) || items.length === 0) {
      tbody.innerHTML = '<tr><td colspan="7" class="text-muted-zh text-center py-4">Nenhum run encontrado para os filtros informados.</td></tr>';
      return;
    }

    tbody.innerHTML = items.map((item) => {
      const meta = statusMeta(item.status);
      return `
        <tr>
          <td>${escapeHtml(formatDate(item.created_at))}</td>
          <td><span class="fw-semibold">${escapeHtml(item.datastore)}</span></td>
          <td>${escapeHtml(item.vcenter_host || '-')}</td>
          <td><span class="zh-history-status ${meta.cls}">${escapeHtml(meta.label)}</span></td>
          <td class="text-end">${escapeHtml(formatCount(item.deleted_vmdk_count))}</td>
          <td class="text-end">${escapeHtml(formatGb(item.deleted_size_gb))}</td>
          <td class="text-end">
            <button type="button" class="btn btn-sm btn-outline-info" data-history-run="${item.run_id}">
              Ver detalhe
            </button>
          </td>
        </tr>
      `;
    }).join("");
  }

  function renderDetail(item) {
    const card = byId("zh-history-detail");
    if (!card || !item) return;
    const meta = statusMeta(item.status);

    byId("zh-history-detail-date").textContent = formatDate(item.created_at);
    byId("zh-history-detail-datastore").textContent = `Datastore: ${item.datastore}`;
    byId("zh-history-detail-vcenter").textContent = `vCenter: ${item.vcenter_host || '-'}`;
    byId("zh-history-detail-baseline").textContent = `Baseline: ${item.baseline_job_id}`;
    byId("zh-history-detail-verification").textContent = `Verificacao: ${item.verification_job_id}`;

    const statusEl = byId("zh-history-detail-status");
    statusEl.className = `badge zh-history-status ${meta.cls}`;
    statusEl.textContent = meta.label;

    byId("zh-history-detail-vmdks").textContent = formatCount(item.deleted_vmdk_count);
    byId("zh-history-detail-gb").textContent = formatGb(item.deleted_size_gb);
    byId("zh-history-detail-remaining").textContent = formatCount(item.remaining_vmdk_count);
    byId("zh-history-detail-consistency").textContent = item.evidence_consistent_with_stored_summary
      ? "Evidencia consistente"
      : "Sumario divergente";

    const note = byId("zh-history-detail-note");
    if (item.evidence_note) {
      note.classList.remove("d-none");
      note.textContent = item.evidence_note;
    } else {
      note.classList.add("d-none");
      note.textContent = "";
    }

    const evidenceBody = byId("zh-history-evidence-body");
    const evidence = Array.isArray(item.deleted_vmdks) ? item.deleted_vmdks : [];
    if (!evidence.length) {
      evidenceBody.innerHTML = '<tr><td colspan="5" class="text-muted-zh text-center py-4">Sem evidencia por arquivo reconstruida para este run.</td></tr>';
    } else {
      evidenceBody.innerHTML = evidence.map((row) => `
        <tr>
          <td class="font-monospace small">${escapeHtml(row.path)}</td>
          <td>${escapeHtml(row.tipo_zombie || '-')}</td>
          <td>${escapeHtml(row.datacenter || '-')}</td>
          <td>${escapeHtml(row.vcenter_host || row.vcenter_name || '-')}</td>
          <td class="text-end">${escapeHtml(formatGb(row.tamanho_gb))}</td>
        </tr>
      `).join("");
    }

    card.classList.remove("d-none");
  }

  async function loadHistory() {
    setLoading(true);
    showFeedback(null, "");
    try {
      const params = buildHistoryQuery();
      const data = await fetchJson(`/api/v1/datastore-reports/datastore-deletion-verification/history?${params.toString()}`);
      state.items = Array.isArray(data.items) ? data.items : [];
      renderSummary(data.summary || {});
      renderRows(state.items);
      if (!state.items.length) {
        byId("zh-history-detail")?.classList.add("d-none");
      }
    } catch (error) {
      renderRows([]);
      renderSummary({});
      byId("zh-history-detail")?.classList.add("d-none");
      showFeedback("danger", error.message || "Nao foi possivel carregar o historico de pos-exclusao.");
    } finally {
      setLoading(false);
    }
  }

  async function loadDetail(runId) {
    try {
      showFeedback(null, "");
      const item = await fetchJson(`/api/v1/datastore-reports/datastore-deletion-verification/history/${encodeURIComponent(runId)}?evidence_limit=200`);
      state.selectedRunId = runId;
      renderDetail(item);
    } catch (error) {
      showFeedback("warning", error.message || "Nao foi possivel carregar o detalhe da execucao.");
    }
  }

  function bindEvents() {
    byId("zh-history-form")?.addEventListener("submit", (event) => {
      event.preventDefault();
      loadHistory();
    });

    byId("zh-history-clear")?.addEventListener("click", () => {
      byId("zh-history-datastore").value = "";
      byId("zh-history-vcenter").value = "";
      byId("zh-history-status").value = "";
      byId("zh-history-limit").value = "100";
      loadHistory();
    });

    byId("zh-history-body")?.addEventListener("click", (event) => {
      const button = event.target.closest("[data-history-run]");
      if (!button) return;
      loadDetail(button.getAttribute("data-history-run"));
    });
  }

  document.addEventListener("DOMContentLoaded", () => {
    bindEvents();
    loadHistory();
  });
})();
