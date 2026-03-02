/**
 * audit.js — Lógica da página de Audit Log do ZombieHunter
 * =========================================================
 * Responsabilidades:
 *   1. Carrega registros via GET /api/v1/approvals/audit-log
 *   2. Inicializa DataTables com filtros client-side por coluna
 *   3. Aplicação de filtros: data, analista, tipo de ação, resultado
 *   4. Exportação CSV direta via link âncora
 *
 * Endpoint consumido:
 *   GET /api/v1/approvals/audit-log?limit=500&analyst=X&action=Y&status=Z
 */

"use strict";

const AUDIT_API = "/api/v1/approvals/audit-log";

// ── Metadados de ação → badge ─────────────────────────────────────────────────

const ACTION_BADGES = {
  // Ações de aprovação / execução
  CREATE_TOKEN:        { label: "CREATE TOKEN",  cls: "zh-badge-blue"   },
  DRY_RUN:             { label: "DRY RUN",       cls: "zh-badge-yellow" },
  DELETE:              { label: "DELETE",        cls: "zh-badge-red"    },
  QUARANTINE:          { label: "QUARANTINE",    cls: "zh-badge-orange" },
  CANCEL:              { label: "CANCEL",        cls: "zh-badge-gray"   },
  UNKNOWN:             { label: "UNKNOWN",       cls: "zh-badge-gray"   },
};

const RESULT_BADGES = {
  // Status de resultado
  created:                { label: "CRIADO",        cls: "zh-badge-blue"   },
  dry_run_completed:      { label: "DRYRUN OK",     cls: "zh-badge-yellow" },
  executed_delete:        { label: "DELETADO",      cls: "zh-badge-red"    },
  executed_quarantine:    { label: "QUARENTENADO",  cls: "zh-badge-orange" },
  cancelled:              { label: "CANCELADO",     cls: "zh-badge-gray"   },
  invalidated:            { label: "INVALIDADO",    cls: "zh-badge-gray"   },
  blocked_readonly:       { label: "BLOQUEADO (RO)",cls: "zh-badge-gray"   },
  blocked_no_dryrun:      { label: "SEM DRYRUN",    cls: "zh-badge-yellow" },
  blocked_expired:        { label: "EXPIRADO",      cls: "zh-badge-gray"   },
  blocked_status_changed: { label: "ST MUDOU",      cls: "zh-badge-yellow" },
  blocked_invalid_token:  { label: "TOKEN INV.",    cls: "zh-badge-gray"   },
  blocked_terminal:       { label: "TERMINAL",      cls: "zh-badge-gray"   },
  failed:                 { label: "FALHOU",        cls: "zh-badge-red"    },
};

// Agrupa para o filtro de resultado no formulário
const RESULT_GROUPS = {
  success: ["created", "dry_run_completed", "executed_delete", "executed_quarantine"],
  blocked: ["blocked_readonly", "blocked_no_dryrun", "blocked_expired",
            "blocked_status_changed", "blocked_invalid_token", "blocked_terminal"],
  failed:  ["failed", "invalidated", "cancelled"],
};

// ── Estado ────────────────────────────────────────────────────────────────────

let dtInstance = null;
let allRecords = [];   // cache local para re-filtrar sem novo fetch

// ── Inicialização ─────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", () => {
  _initTable();
  _bindFilters();
  loadAuditLog();
});

// ── Carregamento ──────────────────────────────────────────────────────────────

async function loadAuditLog() {
  _setLoading(true);

  try {
    const params = _buildApiParams();
    const qs     = new URLSearchParams(params).toString();
    const resp   = await fetch(`${AUDIT_API}${qs ? "?" + qs : ""}`, {
      headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Accept": "application/json" },
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status} ${resp.statusText}`);

    allRecords = await resp.json();
    _applyToTable(allRecords);
    _updateSummary(allRecords);

  } catch (err) {
    console.error("[ZH Audit] Erro:", err);
    _showError(err.message);
  } finally {
    _setLoading(false);
  }
}

// ── DataTables ────────────────────────────────────────────────────────────────

function _initTable() {
  dtInstance = $("#zh-audit-table").DataTable({
    data:        [],
    deferRender: true,
    columns: [
      // 0 — Timestamp
      {
        title:     "Timestamp (UTC)",
        data:      "timestamp",
        width:     "140px",
        render: (d) =>
          `<span class="text-muted-zh font-monospace" style="font-size:.75rem;white-space:nowrap;">`
          + _fmtUtc(d) + `</span>`,
      },
      // 1 — Analista
      {
        title: "Analista",
        data:  "analyst",
        render: (d) =>
          `<span class="fw-semibold" style="font-size:.82rem;">${_esc(d ?? "—")}</span>`,
      },
      // 2 — vCenter
      {
        title: "vCenter",
        data:  "vcenter_id",
        render: (d) =>
          d
          ? `<span class="text-zombie-blue font-monospace" style="font-size:.78rem;">${_esc(d)}</span>`
          : `<span class="text-muted-zh">—</span>`,
      },
      // 3 — VMDK Path
      {
        title: "VMDK Path",
        data:  "vmdk_path",
        render: (d) =>
          `<span class="font-monospace" title="${_esc(d ?? "")}" style="font-size:.75rem;">`
          + _esc(_trunc(d ?? "—", 52)) + `</span>`,
      },
      // 4 — Ação (badge colorido)
      {
        title:     "Ação",
        data:      "action",
        render:    (d) => _actionBadge(d),
      },
      // 5 — Resultado
      {
        title:     "Resultado",
        data:      "status",
        render:    (d) => _resultBadge(d),
      },
      // 6 — Dry-run
      {
        title:     "Dry-run",
        data:      "dry_run",
        width:     "70px",
        className: "text-center",
        render: (d) =>
          d
          ? `<i class="bi bi-check-circle-fill text-zombie-blue" title="Simulação"></i>`
          : `<i class="bi bi-dash-circle text-muted-zh" title="Execução real"></i>`,
      },
      // 7 — Justificativa / Detalhe
      {
        title: "Detalhe",
        data:  "detail",
        render: (d) =>
          d
          ? `<span class="text-muted-zh" style="font-size:.78rem;" title="${_esc(d)}">`
            + _esc(_trunc(d, 60)) + `</span>`
          : `<span class="text-muted-zh">—</span>`,
      },
    ],

    order:       [[0, "desc"]],
    pageLength:  50,
    lengthMenu:  [25, 50, 100, 200],
    searching:   true,    // busca global DataTables
    info:        true,
    scrollX:     true,

    language: {
      search:        "Busca global:",
      info:          "Mostrando _START_–_END_ de _TOTAL_ registros",
      infoEmpty:     "Nenhum registro",
      infoFiltered:  "(filtrado de _MAX_)",
      lengthMenu:    "_MENU_ por página",
      paginate:      { first: "«", last: "»", next: "›", previous: "‹" },
      emptyTable:    "Nenhum registro de auditoria encontrado.",
      zeroRecords:   "Nenhum registro corresponde aos filtros.",
    },

    dom:
      "<'d-flex align-items-center justify-content-between mb-2 flex-wrap gap-2'"
      +   "<'d-flex align-items-center gap-2'l>"
      +   "<'d-flex align-items-center gap-2'f>"
      + ">"
      + "<'table-responsive'tr>"
      + "<'d-flex justify-content-between align-items-center mt-2 flex-wrap gap-1'"
      +   "<'text-muted-zh small'i>"
      +   "<'d-flex justify-content-end'p>"
      + ">",

    drawCallback: () => _updateSummary(dtInstance.rows({ search: "applied" }).data().toArray()),
  });
}

function _applyToTable(records) {
  if (!dtInstance) return;
  dtInstance.clear();
  dtInstance.rows.add(records);
  dtInstance.draw(false);
}

// ── Filtros ───────────────────────────────────────────────────────────────────

function _buildApiParams() {
  const params = { limit: 500 };
  const analyst = document.getElementById("f-analyst")?.value.trim();
  const action  = document.getElementById("f-action")?.value;
  const result  = document.getElementById("f-result")?.value;
  const date    = document.getElementById("f-date")?.value;

  if (analyst) params.analyst = analyst;
  if (action)  params.action  = action;
  if (result)  params.status  = result;
  // A API de audit-log não filtra por data; aplicamos client-side
  return params;
}

function _bindFilters() {
  // Botão Filtrar — re-busca na API com params
  document.getElementById("zh-btn-filter")?.addEventListener("click", () => {
    loadAuditLog();
  });

  // Botão Limpar
  document.getElementById("zh-btn-clear")?.addEventListener("click", () => {
    ["f-analyst", "f-action", "f-result", "f-date"].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });
    loadAuditLog();
  });

  // Filtro de data — client-side sobre allRecords
  document.getElementById("f-date")?.addEventListener("change", function () {
    _applyDateFilter(this.value);
  });

  // Enter em campos de texto
  document.getElementById("f-analyst")?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") loadAuditLog();
  });

  // Botão de exportação CSV
  document.getElementById("zh-btn-export-csv")?.addEventListener("click", _exportCsv);
}

function _applyDateFilter(dateStr) {
  if (!dateStr) {
    _applyToTable(allRecords);
    return;
  }
  const filtered = allRecords.filter((r) => {
    if (!r.timestamp) return false;
    return r.timestamp.startsWith(dateStr);
  });
  _applyToTable(filtered);
}

// ── Exportação CSV client-side ────────────────────────────────────────────────

function _exportCsv() {
  const records = dtInstance
    ? dtInstance.rows({ search: "applied" }).data().toArray()
    : allRecords;

  if (!records.length) {
    alert("Nenhum registro para exportar.");
    return;
  }

  const headers = ["Timestamp (UTC)", "Analista", "vCenter", "VMDK Path",
                   "Acao", "Status", "Dry-run", "Detalhe"];

  const rows = records.map((r) => [
    _fmtUtc(r.timestamp),
    r.analyst ?? "",
    r.vcenter_id ?? "",
    r.vmdk_path ?? "",
    r.action ?? "",
    r.status ?? "",
    r.dry_run ? "SIM" : "NAO",
    (r.detail ?? "").replace(/"/g, '""'),
  ]);

  const csv = [headers, ...rows]
    .map((row) => row.map((cell) => `"${cell}"`).join(","))
    .join("\r\n");

  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8;" });
  const url  = URL.createObjectURL(blob);
  const now  = new Date().toISOString().slice(0, 10);
  const a    = document.createElement("a");
  a.href     = url;
  a.download = `zombiehunter-audit-${now}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

// ── Sumário de estatísticas ────────────────────────────────────────────────────

function _updateSummary(records) {
  const arr = Array.isArray(records) ? records : [];

  const total    = arr.length;
  const executed = arr.filter((r) => r.status?.startsWith("executed_")).length;
  const blocked  = arr.filter((r) => r.status?.startsWith("blocked_")).length;
  const dryruns  = arr.filter((r) => r.dry_run).length;

  _setText("zh-sum-total",    total.toLocaleString("pt-BR"));
  _setText("zh-sum-executed", executed.toLocaleString("pt-BR"));
  _setText("zh-sum-blocked",  blocked.toLocaleString("pt-BR"));
  _setText("zh-sum-dryruns",  dryruns.toLocaleString("pt-BR"));
}

// ── Helpers de renderização ────────────────────────────────────────────────────

function _actionBadge(action) {
  const m = ACTION_BADGES[action?.toUpperCase?.()] ?? ACTION_BADGES[action]
    ?? { label: action ?? "?", cls: "zh-badge-gray" };
  return `<span class="zh-audit-badge ${m.cls}">${_esc(m.label)}</span>`;
}

function _resultBadge(status) {
  const m = RESULT_BADGES[status]
    ?? { label: status ?? "?", cls: "zh-badge-gray" };
  return `<span class="zh-audit-badge ${m.cls}">${_esc(m.label)}</span>`;
}

function _fmtUtc(iso) {
  if (!iso) return "—";
  try {
    const d = new Date(iso);
    return d.toISOString().replace("T", " ").slice(0, 19);
  } catch {
    return String(iso).slice(0, 19);
  }
}

function _setLoading(on) {
  const spinner = document.getElementById("zh-audit-spinner");
  const btn     = document.getElementById("zh-btn-filter");
  if (spinner) spinner.classList.toggle("d-none", !on);
  if (btn)     btn.disabled = on;
}

function _showError(msg) {
  const el = document.getElementById("zh-audit-error");
  if (el) {
    el.textContent = `Erro ao carregar: ${msg}`;
    el.classList.remove("d-none");
  }
}

function _setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? "—";
}

const _esc   = (s) => String(s ?? "")
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;");
const _trunc = (s, n) => s && s.length > n ? s.slice(0, n - 1) + "…" : (s ?? "");
