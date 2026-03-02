/**
 * scan_results.js — Lógica da página de Resultados de Varredura
 * ==============================================================
 * Responsabilidades:
 *   1. DataTables com paginação server-side via GET /api/v1/scan/results
 *   2. Painel de filtros (vCenter, tipo, status, GB mínimo, data)
 *   3. Modal "Detalhes" — exibe todas as informações técnicas do VMDK
 *   4. Modal "Solicitar Aprovação" — POST /api/v1/approvals
 *   5. Seleção em lote + ação "Aprovar Selecionados" em batch
 *   6. Exportação CSV/JSON via /api/v1/scan/results/{job_id}/export
 *
 * Endpoints consumidos:
 *   GET  /api/v1/vcenters                           → popular filtro de vCenter
 *   GET  /api/v1/scan/results                       → paginação server-side
 *   GET  /api/v1/scan/results/{job_id}              → filtro por job específico
 *   POST /api/v1/approvals                          → criar token de aprovação
 *
 * Variáveis globais esperadas (injetadas pelo template Jinja2):
 *   window.ZH_JOB_ID    string | null    — job_id pré-selecionado (rota /scan/results/{job_id})
 */

"use strict";

// ── Constantes ────────────────────────────────────────────────────────────────

const API_RESULTS = "/api/v1/scan/results";
const API_VCENTERS = "/api/v1/vcenters";
const API_APPROVALS = "/api/v1/approvals";
const PAGE_SIZES = [25, 50, 100, 200];

/** Metadados de cada tipo zombie — cores por especificação */
const ZM = {
  ORPHANED: { label: "Orphaned", color: "#dc3545", bg: "rgba(220,53,69,.2)", icon: "bi-x-circle-fill", darkText: false },
  SNAPSHOT_ORPHAN: { label: "Snapshot Orphan", color: "#fd7e14", bg: "rgba(253,126,20,.2)", icon: "bi-camera-fill", darkText: false },
  BROKEN_CHAIN: { label: "Broken Chain", color: "#6f42c1", bg: "rgba(111,66,193,.2)", icon: "bi-link-45deg", darkText: false },
  UNREGISTERED_DIR: { label: "Unregistered Dir", color: "#ffc107", bg: "rgba(255,193,7,.25)", icon: "bi-folder-x", darkText: true },
  POSSIBLE_FALSE_POSITIVE: { label: "False Positive", color: "#6c757d", bg: "rgba(108,117,125,.2)", icon: "bi-question-circle-fill", darkText: false },
};

/** Status do registro VMDK */
const STATUS_META = {
  NOVO: { label: "Novo", cls: "text-bg-danger" },
  EM_QUARENTENA: { label: "Em Quarentena", cls: "text-bg-warning" },
  APROVADO_DELECAO: { label: "Aprovado p/ Deleção", cls: "text-bg-secondary" },
  WHITELIST: { label: "Whitelist", cls: "text-bg-success" },
};

// ── Estado interno ────────────────────────────────────────────────────────────

let dtInstance = null;          // Instância DataTables
let selectedRows = new Set();     // Conjunto de IDs selecionados para lote
let currentDetailRow = null;      // Linha atual aberta no modal de detalhes
let approvalTargetRow = null;     // Linha atual no modal de aprovação

// ── Bootstrap modals (criados uma vez, reutilizados) ─────────────────────────

let bsModalDetails = null;
let bsModalApproval = null;
let bsModalBatch = null;

// ── Inicialização ─────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", async () => {
  await _populateVcenterFilter();
  _initDataTable();
  _initModals();
  _bindFilterEvents();
  _bindBatchBar();

  // Se vier com job_id na URL, pré-preenche o campo oculto
  const jobId = window.ZH_JOB_ID ?? _getUrlParam("job_id");
  if (jobId) {
    const el = document.getElementById("f-job-id");
    if (el) { el.value = jobId; }
    document.getElementById("zh-job-id-badge")?.classList.remove("d-none");
    const jobText = document.getElementById("zh-job-id-text");
    if (jobText) jobText.textContent = jobId.substring(0, 8) + "…";

    // Inicia polling de status do job
    _startJobPolling(jobId);
  }
});

// ── Polling de status do job ──────────────────────────────────────────────────

let _pollTimer = null;
let _lastJobStatus = null;
let _lastStepCount = 0;  // Para detectar novos passos sem re-renderizar tudo

async function _startJobPolling(jobId) {
  await _fetchJobStatus(jobId);
}

async function _fetchJobStatus(jobId) {
  const banner = document.getElementById("zh-scan-status-banner");
  const iconEl = document.getElementById("zh-scan-status-icon");
  const textEl = document.getElementById("zh-scan-status-text");
  const detEl = document.getElementById("zh-scan-status-detail");
  if (!banner) return;

  try {
    const resp = await fetch(`/api/v1/scan/jobs/${jobId}`, { headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", "Accept": "application/json" } });
    if (!resp.ok) return;
    const job = await resp.json();
    const st = job.status ?? "unknown";

    banner.classList.remove("d-none");
    banner.style.display = "block";

    if (st === "running" || st === "pending") {
      banner.style.background = "rgba(56,139,253,.08)";
      banner.style.borderColor = "rgba(56,139,253,.3)";

      const prog = job.progress || {};
      const dsIdx = prog.ds_index || 0;
      const dsTotal = prog.ds_total || 0;
      const dsCur = prog.ds_current || "";
      const dsSt = prog.ds_status || "";

      // ── Linha de título ────────────────────────────────────────────────────
      const spinnerHtml = `<span class="spinner-border spinner-border-sm" style="width:13px;height:13px;border-width:2px;color:var(--zh-blue);vertical-align:middle;"></span>`;
      iconEl.innerHTML = spinnerHtml;
      textEl.innerHTML = `<span style="color:var(--zh-blue);font-weight:600;">${st === "running" ? "Varredura em andamento" : "Aguardando início"}</span>
        <small class="text-muted ms-2">Job ${jobId.substring(0, 8)}</small>`;
      textEl.style.color = "";

      // ── Barra de progresso por datastore ──────────────────────────────────
      let progressBarHtml = "";
      if (dsTotal > 0) {
        const pct = Math.round((dsIdx / dsTotal) * 100);
        const dsStatusIcon = { scanning: "🔍", done: "✅", failed: "❌", inaccessible: "⚠️" }[dsSt] || "⏳";
        progressBarHtml = `
          <div class="mt-1" style="font-size:.78rem;">
            <span class="text-muted">Datastore ${dsIdx}/${dsTotal}</span>
            ${dsCur ? `<span class="ms-2 text-truncate" style="max-width:300px;display:inline-block;vertical-align:bottom;" title="${dsCur}">${dsStatusIcon} <strong>${dsCur}</strong></span>` : ""}
            <div class="progress mt-1" style="height:4px;background:rgba(255,255,255,.1);border-radius:2px;">
              <div class="progress-bar" style="width:${pct}%;background:var(--zh-blue);transition:width .4s;"></div>
            </div>
          </div>`;
      } else if (prog.current) {
        progressBarHtml = `<div class="mt-1 text-muted" style="font-size:.78rem;">⏳ ${prog.current}</div>`;
      }
      detEl.innerHTML = progressBarHtml;

      // ── Painel de log de passos ────────────────────────────────────────────
      const steps = prog.steps || [];
      _renderProgressLog(steps);

      // Agenda próxima verificação
      if (_pollTimer) clearTimeout(_pollTimer);
      _pollTimer = setTimeout(() => _fetchJobStatus(jobId), 3000);

    } else if (st === "completed") {
      banner.style.background = "rgba(63,185,80,.08)";
      banner.style.borderColor = "rgba(63,185,80,.3)";
      iconEl.innerHTML = `<i class="bi bi-check-circle-fill" style="color:var(--zh-green);font-size:1rem;"></i>`;
      const total = job.summary?.total_vmdks_encontrados ?? 0;
      const sizeStr = window.zhFormatSizeGB ? window.zhFormatSizeGB(job.summary?.total_size_gb ?? 0) : (job.summary?.total_size_gb ?? 0).toFixed(2) + " GB";
      textEl.innerHTML = `<span style="color:var(--zh-green);font-weight:600;">Varredura concluída</span>
        <small class="text-muted ms-2">Job ${jobId.substring(0, 8)}</small>`;
      textEl.style.color = "";
      detEl.innerHTML = `<span style="font-size:.82rem;">${total} VMDKs encontrados &middot; ${sizeStr} recuperáveis</span>
        <button class="btn btn-sm btn-outline-secondary ms-2 py-0 px-2" style="font-size:.72rem;" onclick="document.getElementById('zh-progress-panel').classList.toggle('d-none')">Ver log</button>`;

      // Mantém o log visível mas colapsado
      if (_lastJobStatus === "running" || _lastJobStatus === "pending") {
        dtInstance?.ajax.reload(null, false);
      }

    } else if (st === "failed") {
      banner.style.background = "rgba(248,81,73,.08)";
      banner.style.borderColor = "rgba(248,81,73,.3)";
      iconEl.innerHTML = `<i class="bi bi-x-circle-fill" style="color:var(--zh-red);font-size:1rem;"></i>`;
      textEl.innerHTML = `<span style="color:var(--zh-red);font-weight:600;">Varredura falhou</span>`;
      textEl.style.color = "";
      const errs = job.error_messages ?? [];
      detEl.innerHTML = `<span class="text-muted" style="font-size:.82rem;">${errs.length ? errs[0].substring(0, 120) : "Verifique os logs do servidor"}</span>`;
    }

    _lastJobStatus = st;

  } catch (_) { /* falha silenciosa */ }
}

// ── Painel de log de progresso ────────────────────────────────────────────────

const _STEP_ICON = { info: "ℹ️", success: "✅", warning: "⚠️", error: "❌" };
const _STEP_COLOR = {
  info: "rgba(200,200,200,.7)",
  success: "var(--zh-green)",
  warning: "#e3a008",
  error: "var(--zh-red)",
};

function _renderProgressLog(steps) {
  let panel = document.getElementById("zh-progress-panel");
  if (!panel) {
    // Cria o painel de log na primeira chamada
    const banner = document.getElementById("zh-scan-status-banner");
    if (!banner) return;
    panel = document.createElement("div");
    panel.id = "zh-progress-panel";
    panel.style.cssText = `
      margin-top: 8px;
      background: rgba(0,0,0,.35);
      border-radius: 6px;
      padding: 8px 10px;
      font-family: 'Courier New', monospace;
      font-size: .72rem;
      max-height: 220px;
      overflow-y: auto;
      border: 1px solid rgba(255,255,255,.08);
    `;
    banner.appendChild(panel);
  }

  // Só re-renderiza se houver novos passos
  if (steps.length === _lastStepCount) return;
  _lastStepCount = steps.length;

  panel.innerHTML = steps.map((s) => {
    const icon = _STEP_ICON[s.level] || "•";
    const color = _STEP_COLOR[s.level] || "rgba(200,200,200,.7)";
    const msg = (s.msg || "").replace(/</g, "&lt;").replace(/>/g, "&gt;");
    return `<div style="color:${color};line-height:1.6;">
      <span style="opacity:.5;">${s.ts}</span>
      <span style="margin:0 4px;">${icon}</span>
      <span>${msg}</span>
    </div>`;
  }).join("");

  // Auto-scroll para o último passo
  panel.scrollTop = panel.scrollHeight;
}

// ── DataTables server-side ────────────────────────────────────────────────────

function _initDataTable() {
  dtInstance = $("#zh-table-results").DataTable({
    serverSide: true,
    processing: true,
    deferRender: true,
    ajax: {
      url: API_RESULTS,
      type: "GET",
      // Transforma parâmetros DataTables → query params da API
      data: (d) => _buildAjaxParams(d),
      // Mapeia resposta da API para formato DataTables
      dataSrc: (json) => {
        // Atualiza contador de registros totais para paginação
        json.recordsTotal = json.total ?? 0;
        json.recordsFiltered = json.total ?? 0;
        _updateSummaryBar(json);
        return json.items ?? json.results ?? [];
      },
      error: (xhr, err) => {
        console.error("[ZH] Erro na API:", err, xhr.responseText);
        _showTableError(xhr.status);
      },
    },

    columns: [
      // 0 — Checkbox de seleção
      {
        title: `<input type="checkbox" id="zh-check-all" class="form-check-input" title="Selecionar todos visíveis"/>`,
        data: "id",
        orderable: false,
        searchable: false,
        width: "36px",
        className: "text-center",
        render: (id) =>
          `<input type="checkbox" class="form-check-input zh-row-check" data-id="${id}" title="Selecionar"/>`,
      },
      // 1 — vCenter
      {
        title: "vCenter",
        data: "vcenter_host",
        render: (d, t, row) =>
          `<span class="text-zombie-blue fw-semibold" style="font-size:.82rem;">`
          + _esc(row.vcenter_name || d)
          + `</span>`,
      },
      // 2 — Datastore
      {
        title: "Datastore",
        data: "datastore",
        render: (d) =>
          `<span class="text-muted-zh font-monospace" style="font-size:.78rem;">${_esc(d ?? "—")}</span>`,
      },
      // 3 — Caminho completo
      {
        title: "VMDK Path",
        data: "path",
        render: (d) =>
          `<span class="font-monospace" style="font-size:.78rem;" title="${_esc(d)}">`
          + _esc(_trunc(d, 60))
          + `</span>`,
      },
      // 4 — Tamanho GB
      {
        title: "Tamanho",
        data: "tamanho_gb",
        className: "text-end",
        render: (d, t, row) => _renderSize(d, row.tipo_zombie),
      },
      // 5 — Tipo Zombie (badge)
      {
        title: "Tipo",
        data: "tipo_zombie",
        render: (d) => _typeBadge(d),
      },
      // 6 — Score de confiança (barra de progresso)
      {
        title: "Confiança",
        data: "confidence_score",
        className: "text-center",
        render: (d) => _confidenceBar(d),
      },
      // 7 — Última modificação
      {
        title: "Última Mod.",
        data: "ultima_modificacao",
        render: (d) =>
          d
            ? `<span data-zh-date="${d}">${window.zhFormatDate(d)}</span>`
            : `<span class="text-muted-zh">—</span>`,
      },
      // 8 — Status
      {
        title: "Status",
        data: "status",
        render: (d) => _statusBadge(d),
      },
      // 9 — Ações
      {
        title: "Ações",
        data: null,
        orderable: false,
        searchable: false,
        className: "text-end",
        render: (d, t, row) => _actionBtns(row),
      },
    ],

    order: [[4, "desc"]],          // padrão: maior tamanho primeiro
    pageLength: PAGE_SIZES[0],
    lengthMenu: [PAGE_SIZES, PAGE_SIZES.map((n) => `${n} por página`)],
    searching: false,
    info: true,
    responsive: false,
    scrollX: true,

    rowCallback: function (row, data) {
      const score = data.confidence_score != null ? Number(data.confidence_score) : "";
      row.setAttribute("data-score", String(score));
      row.setAttribute("data-tipo", String(data.tipo_zombie || ""));
      const gb = data.tamanho_gb != null && data.tamanho_gb !== "" ? Number(data.tamanho_gb) : "";
      row.setAttribute("data-size", String(gb));
      let ageDays = "";
      if (data.ultima_modificacao) {
        const mod = new Date(data.ultima_modificacao).getTime();
        if (!isNaN(mod)) ageDays = Math.floor((Date.now() - mod) / (24 * 60 * 60 * 1000));
      }
      row.setAttribute("data-modified-days", String(ageDays));
    },

    language: {
      processing: `<span class="text-muted-zh small"><i class="bi bi-hourglass-split me-1"></i>Carregando…</span>`,
      info: "Exibindo _START_–_END_ de _TOTAL_ VMDKs",
      infoEmpty: "Nenhum VMDK encontrado",
      infoFiltered: "(filtrado de _MAX_ total)",
      lengthMenu: "_MENU_",
      paginate: { first: "«", last: "»", next: "›", previous: "‹" },
      emptyTable: "Nenhum VMDK detectado para os filtros aplicados.",
      zeroRecords: "Nenhum resultado. Tente ajustar os filtros.",
    },

    dom:
      "<'d-flex align-items-center justify-content-between mb-2 flex-wrap gap-2'"
      + "<'d-flex align-items-center gap-2'l<'zh-export-btns'>>"
      + "<'text-muted-zh small'i>"
      + ">"
      + "<'table-responsive'tr>"
      + "<'d-flex justify-content-end mt-2'p>",

    drawCallback: function () {
      _rebindRowEvents();
      _syncCheckAll();
      _applyClientFilters();
      _updateVisibleCount();
    },
  });
}

/** Constrói query params da API a partir dos parâmetros DataTables */
function _buildAjaxParams(d) {
  const params = {
    page: Math.floor(d.start / d.length) + 1,
    per_page: d.length,
  };

  // Ordenação
  if (d.order?.length) {
    const colIdx = d.order[0].column;
    const colData = d.columns[colIdx]?.data;
    const sortMap = { tamanho_gb: "tamanho_gb", ultima_modificacao: "ultima_modificacao", tipo_zombie: "tipo_zombie", confidence_score: "confidence_score" };
    params.sort_by = sortMap[colData] ?? colData;
    params.sort_dir = d.order[0].dir;
  }

  // Filtros do painel
  const fVc = document.getElementById("f-vcenter")?.value;
  const fTipo = document.getElementById("f-tipo")?.value;
  const fSts = document.getElementById("f-status")?.value;
  const fGb = document.getElementById("f-min-gb")?.value;
  const fConf = document.getElementById("f-min-confidence")?.value;
  const fModAfter = document.getElementById("f-modified-after")?.value;
  const fModBefore = document.getElementById("f-modified-before")?.value;
  const fDate = document.getElementById("f-date")?.value;
  const fJob = document.getElementById("f-job-id")?.value;

  if (fVc) params.vcenter = fVc;
  if (fTipo) params.tipo = fTipo;
  if (fSts) params.status = fSts;
  if (fGb) params.min_size_gb = parseFloat(fGb);
  if (fConf) params.min_confidence = parseInt(fConf, 10);
  if (fModAfter) params.modified_after = fModAfter;
  if (fModBefore) params.modified_before = fModBefore;
  if (fDate) params.scan_date = fDate;
  if (fJob) params.job_id = fJob;

  _updateActiveFiltersBadge(params);
  return params;
}

/** Atualiza o badge "X filtros ativos" no painel de filtros */
function _updateActiveFiltersBadge(params) {
  const skip = { page: 1, per_page: 1, sort_by: 1, sort_dir: 1 };
  let n = 0;
  for (const k of Object.keys(params)) {
    if (skip[k]) continue;
    if (params[k] !== undefined && params[k] !== "" && params[k] !== null) n++;
  }
  const badge = document.getElementById("zh-active-filters-badge");
  if (badge) {
    if (n > 0) {
      badge.textContent = n + " ativo" + (n === 1 ? "" : "s");
      badge.classList.remove("d-none");
    } else {
      badge.classList.add("d-none");
    }
  }
}

// ── Filtros ───────────────────────────────────────────────────────────────────

/** Popula o select de vCenter com dados da API */
async function _populateVcenterFilter() {
  const sel = document.getElementById("f-vcenter");
  if (!sel) return;
  try {
    const resp = await fetch(API_VCENTERS, { headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", "Accept": "application/json" } });
    if (!resp.ok) return;
    const list = await resp.json();
    list.forEach((vc) => {
      const opt = document.createElement("option");
      opt.value = vc.id;
      opt.textContent = vc.name;
      sel.appendChild(opt);
    });
  } catch (_) { /* falha silenciosa — select fica só com "Todos" */ }
}

/** Aplica filtros client-side (oculta/exibe linhas da tabela) */
function _applyClientFilters() {
  const tbody = document.querySelector("#zh-table-results tbody");
  if (!tbody) return;

  // Lê controles — usa 0 se o score-slider estiver ausente
  const scoreMin = parseInt(document.getElementById("zh-cf-score")?.value ?? "0", 10);
  const sizeMin = parseFloat(document.getElementById("zh-cf-size")?.value ?? "0") || 0;
  const modifiedDaysRaw = document.getElementById("zh-cf-modified-days")?.value?.trim();
  const modifiedDaysMin = (!modifiedDaysRaw || modifiedDaysRaw === "") ? null : parseInt(modifiedDaysRaw, 10);
  const checkedTipos = new Set();
  document.querySelectorAll(".zh-cf-tipo:checked").forEach((cb) => checkedTipos.add(cb.value));

  tbody.querySelectorAll("tr").forEach((tr) => {
    if (tr.cells.length < 2) return;  // linha auxiliar (loading/empty)

    const scoreAttr = tr.getAttribute("data-score");
    // Se score não foi setado ainda pelo rowCallback, deixa passar
    const passScore = !scoreAttr || scoreAttr === "" || parseFloat(scoreAttr) >= scoreMin;

    const tipo = tr.getAttribute("data-tipo") ?? "";
    // Se tipo não foi setado ainda, ou todos os tipos estão marcados, deixa passar
    const passTipo = checkedTipos.size === 0 || !tipo || checkedTipos.has(tipo);

    const sizeAttr = tr.getAttribute("data-size");
    const passSize = !sizeAttr || sizeAttr === "" || parseFloat(sizeAttr) >= sizeMin;

    const modDaysAttr = tr.getAttribute("data-modified-days");
    const passMod = modifiedDaysMin === null
      || !modDaysAttr || modDaysAttr === ""
      || parseInt(modDaysAttr, 10) >= modifiedDaysMin;

    tr.style.display = passScore && passTipo && passSize && passMod ? "" : "none";
  });
}


/** Atualiza o texto "Exibindo X de Y resultados" */
function _updateVisibleCount() {
  const tbody = document.querySelector("#zh-table-results tbody");
  const el = document.getElementById("zh-visible-count");
  if (!tbody || !el) return;
  const rows = tbody.querySelectorAll("tr");
  let total = 0;
  let visible = 0;
  rows.forEach((tr) => {
    if (tr.cells.length < 2) return;
    total++;
    if (tr.style.display !== "none") visible++;
  });
  el.textContent = `Exibindo ${visible} de ${total} resultados`;
}

/** Exporta CSV apenas das linhas visíveis (Blob + createObjectURL) */
function _exportCsvFiltered() {
  if (!dtInstance) return;
  const tbody = document.querySelector("#zh-table-results tbody");
  if (!tbody) return;
  const visibleRows = [];
  tbody.querySelectorAll("tr").forEach((tr) => {
    if (tr.cells.length < 2 || tr.style.display === "none") return;
    const data = dtInstance.row(tr).data();
    if (data) visibleRows.push(data);
  });
  if (visibleRows.length === 0) {
    if (typeof window.alert === "function") window.alert("Nenhuma linha visível para exportar. Ajuste os filtros.");
    return;
  }
  const headers = ["path", "vcenter_host", "datastore", "tamanho_gb", "tipo_zombie", "confidence_score", "ultima_modificacao", "status"];
  const escapeCsv = (v) => {
    const s = v == null ? "" : String(v);
    if (/[,\r\n"]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  };
  const lines = [headers.join(",")];
  visibleRows.forEach((row) => {
    const ultima = row.ultima_modificacao ? (typeof row.ultima_modificacao === "string" ? row.ultima_modificacao : new Date(row.ultima_modificacao).toISOString()) : "";
    lines.push([
      escapeCsv(row.path),
      escapeCsv(row.vcenter_host),
      escapeCsv(row.datastore),
      escapeCsv(row.tamanho_gb),
      escapeCsv(row.tipo_zombie),
      escapeCsv(row.confidence_score),
      escapeCsv(ultima),
      escapeCsv(row.status),
    ].join(","));
  });
  const csv = lines.join("\r\n");
  const blob = new Blob(["\uFEFF" + csv], { type: "text/csv;charset=utf-8" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `vmdk_zombie_visiveis_${new Date().toISOString().slice(0, 10)}.csv`;
  a.click();
  URL.revokeObjectURL(url);
}

/** Vincula eventos dos botões Filtrar / Limpar */
function _bindFilterEvents() {
  document.getElementById("zh-btn-filter")?.addEventListener("click", () => {
    dtInstance?.ajax.reload(null, true); // reinicia na pág 1
  });

  const cfScore = document.getElementById("zh-cf-score");
  const cfScoreVal = document.getElementById("zh-cf-score-val");
  if (cfScore && cfScoreVal) {
    cfScore.addEventListener("input", () => {
      cfScoreVal.textContent = cfScore.value;
      _applyClientFilters();
      _updateVisibleCount();
    });
  }
  document.querySelectorAll(".zh-cf-tipo").forEach((cb) => {
    cb.addEventListener("change", () => { _applyClientFilters(); _updateVisibleCount(); });
  });
  document.getElementById("zh-cf-modified-days")?.addEventListener("input", () => { _applyClientFilters(); _updateVisibleCount(); });
  document.getElementById("zh-cf-size")?.addEventListener("input", () => { _applyClientFilters(); _updateVisibleCount(); });

  document.getElementById("zh-export-csv-filtered")?.addEventListener("click", _exportCsvFiltered);

  document.getElementById("zh-btn-clear")?.addEventListener("click", () => {
    [
      "f-vcenter", "f-tipo", "f-status", "f-min-gb", "f-min-confidence",
      "f-modified-after", "f-modified-before", "f-date",
    ].forEach((id) => {
      const el = document.getElementById(id);
      if (el) el.value = "";
    });
    selectedRows.clear();
    _updateBatchBar();
    _updateActiveFiltersBadge({});
    dtInstance?.ajax.reload(null, true);
  });

  // Filtrar ao pressionar Enter em campos de texto/número
  ["f-min-gb", "f-min-confidence", "f-date", "f-modified-after", "f-modified-before"].forEach((id) => {
    document.getElementById(id)?.addEventListener("keydown", (e) => {
      if (e.key === "Enter") dtInstance?.ajax.reload(null, true);
    });
  });
}

// ── Botões de exportação ──────────────────────────────────────────────────────

/** Injeta botões de exportação CSV/JSON no DOM do DataTables após init */
function _injectExportButtons() {
  const wrapper = document.querySelector(".zh-export-btns");
  if (!wrapper || wrapper.querySelector(".zh-export-csv")) return;

  const jobId = document.getElementById("f-job-id")?.value ?? "";

  const mkBtn = (fmt, icon, cls) => {
    const btn = document.createElement("a");
    btn.className = `btn btn-sm btn-outline-secondary zh-export-${fmt} d-flex align-items-center gap-1`;
    btn.style.cssText = "font-size:.75rem;padding:2px 8px;";
    btn.title = `Exportar ${fmt.toUpperCase()}`;
    btn.innerHTML = `<i class="bi ${icon}"></i> ${fmt.toUpperCase()}`;
    btn.href = jobId
      ? `/api/v1/scan/results/${jobId}/export?format=${fmt}`
      : `/api/v1/scan/results/export?format=${fmt}`;
    btn.target = "_blank";
    return btn;
  };

  const grp = document.createElement("div");
  grp.className = "d-flex gap-1";
  grp.appendChild(mkBtn("csv", "bi-filetype-csv", ""));
  grp.appendChild(mkBtn("json", "bi-filetype-json", ""));
  wrapper.appendChild(grp);
}

// ── Eventos por linha (rebind após cada draw) ─────────────────────────────────

function _rebindRowEvents() {
  _injectExportButtons();

  // Botões "Detalhes"
  document.querySelectorAll(".zh-btn-details").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = dtInstance.row(btn.closest("tr")).data();
      _openDetailsModal(row);
    });
  });

  // Botões "Solicitar Aprovação"
  document.querySelectorAll(".zh-btn-approval").forEach((btn) => {
    btn.addEventListener("click", () => {
      const row = dtInstance.row(btn.closest("tr")).data();
      _openApprovalModal(row);
    });
  });

  // Checkboxes individuais
  document.querySelectorAll(".zh-row-check").forEach((chk) => {
    chk.addEventListener("change", () => {
      const id = chk.dataset.id;
      chk.checked ? selectedRows.add(id) : selectedRows.delete(id);
      _updateBatchBar();
      _syncCheckAll();
    });

    // Restaura estado visual após redraw
    if (selectedRows.has(chk.dataset.id)) chk.checked = true;
  });

  // Checkbox "selecionar todos"
  document.getElementById("zh-check-all")?.addEventListener("change", function () {
    document.querySelectorAll(".zh-row-check").forEach((chk) => {
      chk.checked = this.checked;
      this.checked ? selectedRows.add(chk.dataset.id) : selectedRows.delete(chk.dataset.id);
    });
    _updateBatchBar();
  });
}

function _syncCheckAll() {
  const all = document.querySelectorAll(".zh-row-check");
  const chkd = document.querySelectorAll(".zh-row-check:checked");
  const master = document.getElementById("zh-check-all");
  if (!master) return;
  master.indeterminate = chkd.length > 0 && chkd.length < all.length;
  master.checked = chkd.length > 0 && chkd.length === all.length;
}

// ── Barra de ação em lote ─────────────────────────────────────────────────────

function _bindBatchBar() {
  document.getElementById("zh-btn-batch-approve")?.addEventListener("click", () => {
    if (selectedRows.size === 0) return;
    _openBatchModal();
  });

  document.getElementById("zh-btn-batch-clear")?.addEventListener("click", () => {
    selectedRows.clear();
    document.querySelectorAll(".zh-row-check").forEach((c) => (c.checked = false));
    const master = document.getElementById("zh-check-all");
    if (master) { master.checked = false; master.indeterminate = false; }
    _updateBatchBar();
  });
}

function _updateBatchBar() {
  const bar = document.getElementById("zh-batch-bar");
  const count = document.getElementById("zh-batch-count");
  if (!bar) return;
  bar.classList.toggle("d-none", selectedRows.size === 0);
  if (count) count.textContent = selectedRows.size;
}

// ── Modal: Detalhes ───────────────────────────────────────────────────────────

function _openDetailsModal(row) {
  currentDetailRow = row;
  const m = document.getElementById("zh-modal-details");
  if (!m) return;

  const zm = ZM[row.tipo_zombie] ?? { label: row.tipo_zombie, color: "#6e7681", icon: "bi-question" };

  // Cabeçalho
  _setHtml("det-title", _esc(row.path ?? "VMDK"));
  _setHtml("det-type-badge", _typeBadge(row.tipo_zombie));
  _setHtml("det-status-badge", _statusBadge(row.status));

  // Campos principais
  _setText("det-path", row.path);
  _setText("det-datastore", row.datastore_name || row.datastore);
  _setText("det-vcenter", row.vcenter_name ?? row.vcenter_host);
  _setText("det-datacenter", row.datacenter_path || row.datacenter);
  _setText("det-folder", row.vmdk_folder || row.folder || "—");
  _setText("det-vmdk-filename", row.vmdk_filename || "—");
  const sizeGb = row.tamanho_gb != null ? +row.tamanho_gb : null;
  const sizeStr = sizeGb == null
    ? "—"
    : sizeGb < 0.001
      ? "< 1 MB (apenas descriptor — dados ausentes)"
      : (window.zhFormatSizeGB ? window.zhFormatSizeGB(sizeGb) : `${sizeGb.toFixed(3)} GB`);
  _setText("det-size", sizeStr);
  _setText("det-modified", window.zhFormatDate(row.ultima_modificacao));
  _setText("det-job-id", row.job_id);
  _setText("det-ds-type", row.datastore_type || "—");
  _setText("det-false-pos", row.false_positive_reason || "—");

  // Links vCenter — mostrar apenas quando disponíveis
  const btnUi = document.getElementById("det-btn-vsphere-ui");
  const btnFolder = document.getElementById("det-btn-folder");
  const warnFolder = document.getElementById("det-folder-warning");
  if (btnUi) {
    const linkUi = row.vcenter_deeplink_ui || "";
    btnUi.href = linkUi;
    btnUi.classList.toggle("d-none", !linkUi);
  }
  if (btnFolder) {
    const linkFolder = row.vcenter_deeplink_folder || "";
    btnFolder.href = linkFolder;
    btnFolder.classList.toggle("d-none", !linkFolder);
  }
  if (warnFolder) warnFolder.classList.toggle("d-none", !(row.vcenter_deeplink_folder || ""));

  // Copiar path ao clicar no botão
  const copyBtn = document.getElementById("det-copy-path");
  if (copyBtn) {
    copyBtn.replaceWith(copyBtn.cloneNode(true));
    document.getElementById("det-copy-path")?.addEventListener("click", () => {
      if (row.path && navigator.clipboard?.writeText) {
        navigator.clipboard.writeText(row.path);
        const btn = document.getElementById("det-copy-path");
        if (btn) { btn.title = "Copiado!"; setTimeout(() => { btn.title = "Copiar caminho"; }, 1500); }
      }
    }, { once: false });
  }

  // Score de confiança
  const scoreEl = document.getElementById("det-confidence");
  if (scoreEl) {
    const score = row.confidence_score ?? null;
    scoreEl.innerHTML = score != null
      ? _confidenceBar(score, true)
      : `<span class="text-muted-zh">Não calculado</span>`;
  }

  // Regras de detecção (lista técnica)
  const rulesEl = document.getElementById("det-rules");
  if (rulesEl) {
    const rules = row.detection_rules ?? [];
    if (rules.length) {
      rulesEl.innerHTML = rules.map((r) =>
        `<li class="mb-1">`
        + `<i class="bi bi-chevron-right text-zombie-red me-2" style="font-size:.7rem;"></i>`
        + `<code style="font-size:.8rem;color:#e6edf3;">${_esc(r)}</code>`
        + `</li>`
      ).join("");
    } else {
      rulesEl.innerHTML = `<li class="text-muted-zh">Nenhuma regra registrada.</li>`;
    }
  }

  // Botão "Solicitar Aprovação" dentro do modal de detalhes
  document.getElementById("det-btn-approval")?.addEventListener("click", () => {
    bsModalDetails?.hide();
    setTimeout(() => _openApprovalModal(row), 350);
  }, { once: true });

  bsModalDetails?.show();
}

// ── Modal: Solicitar Aprovação ────────────────────────────────────────────────

function _openApprovalModal(row) {
  approvalTargetRow = row;

  // Preenche preview do VMDK no modal
  _setText("apv-vmdk-path", row.path);
  _setText("apv-vcenter", row.vcenter_name ?? row.vcenter_host ?? "");
  _setText("apv-size", row.tamanho_gb != null ? (window.zhFormatSizeGB ? window.zhFormatSizeGB(row.tamanho_gb) : `${(+row.tamanho_gb).toFixed(2)} GB`) : "—");
  _setHtml("apv-type-badge", _typeBadge(row.tipo_zombie));

  // Campos do formulário
  const fPath = document.getElementById("apv-field-path");
  const fVcenter = document.getElementById("apv-field-vcenter");
  if (fPath) fPath.value = row.path ?? "";
  if (fVcenter) fVcenter.value = row.vcenter_host ?? "";

  // Limpa erros e estado anterior
  _clearApprovalForm();

  bsModalApproval?.show();
}

function _clearApprovalForm() {
  ["apv-justificativa", "apv-analista"].forEach((id) => {
    const el = document.getElementById(id);
    if (el) { el.value = ""; el.classList.remove("is-invalid"); }
  });
  const actSel = document.getElementById("apv-acao");
  if (actSel) actSel.value = "QUARANTINE";
  document.getElementById("apv-error")?.classList.add("d-none");
  document.getElementById("apv-success")?.classList.add("d-none");
}

// ── Modal: Aprovação em lote ──────────────────────────────────────────────────

function _openBatchModal() {
  document.getElementById("batch-count-text").textContent = selectedRows.size;
  document.getElementById("batch-justificativa").value = "";
  document.getElementById("batch-analista").value = "";
  document.getElementById("batch-acao").value = "QUARANTINE";
  document.getElementById("batch-error")?.classList.add("d-none");
  document.getElementById("batch-progress-wrap")?.classList.add("d-none");
  bsModalBatch?.show();
}

// ── Submissão de aprovação (individual) ──────────────────────────────────────

async function submitApproval() {
  const row = approvalTargetRow;
  if (!row) return;

  const justEl = document.getElementById("apv-justificativa");
  const analEl = document.getElementById("apv-analista");
  const acaoEl = document.getElementById("apv-acao");
  const errEl = document.getElementById("apv-error");
  const okEl = document.getElementById("apv-success");
  const btnEl = document.getElementById("apv-btn-submit");

  // Validação
  let valid = true;
  justEl.classList.remove("is-invalid");
  analEl.classList.remove("is-invalid");

  if (!justEl.value.trim() || justEl.value.trim().length < 20) {
    justEl.classList.add("is-invalid");
    valid = false;
  }
  if (!analEl.value.trim()) {
    analEl.classList.add("is-invalid");
    valid = false;
  }
  if (!valid) return;

  // Loading
  btnEl.disabled = true;
  btnEl.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Enviando…`;

  try {
    const resp = await fetch(API_APPROVALS, {
      method: "POST",
      headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({
        vmdk_path: row.path,
        vcenter_id: String(row.vcenter_id ?? row.vcenter_host ?? ""),
        action: acaoEl.value,
        justificativa: justEl.value.trim(),
        analista: analEl.value.trim(),
      }),
    });

    const data = await resp.json();
    if (!resp.ok) throw new Error(data.detail ?? `HTTP ${resp.status}`);

    // Exibe token gerado
    okEl.innerHTML =
      `<i class="bi bi-check-circle-fill me-2 text-zombie-green"></i>`
      + `Token gerado com sucesso! `
      + `<code class="ms-1" style="font-size:.8rem;">${_esc(data.approval_token)}</code>`
      + `<br/><small class="text-muted-zh">Expira em: ${data.expires_in}. `
      + `Próximo passo: executar o dry-run.</small>`;
    okEl.classList.remove("d-none");
    errEl?.classList.add("d-none");

    // Atualiza badge de pendentes na sidebar
    setTimeout(() => window.refreshPendingBadge?.(), 1000);

    // Fecha modal após 4s
    setTimeout(() => bsModalApproval?.hide(), 4000);

  } catch (err) {
    if (errEl) {
      errEl.textContent = `Erro: ${err.message}`;
      errEl.classList.remove("d-none");
    }
  } finally {
    btnEl.disabled = false;
    btnEl.innerHTML = `<i class="bi bi-shield-plus me-2"></i>Confirmar Solicitação`;
  }
}

// ── Submissão em lote ─────────────────────────────────────────────────────────

async function submitBatchApproval() {
  const justEl = document.getElementById("batch-justificativa");
  const analEl = document.getElementById("batch-analista");
  const acaoEl = document.getElementById("batch-acao");
  const errEl = document.getElementById("batch-error");
  const progWrap = document.getElementById("batch-progress-wrap");
  const progBar = document.getElementById("batch-progress-bar");
  const btnEl = document.getElementById("batch-btn-submit");

  // Validação
  justEl.classList.remove("is-invalid");
  analEl.classList.remove("is-invalid");
  let valid = true;
  if (!justEl.value.trim() || justEl.value.trim().length < 20) {
    justEl.classList.add("is-invalid"); valid = false;
  }
  if (!analEl.value.trim()) {
    analEl.classList.add("is-invalid"); valid = false;
  }
  if (!valid) return;

  const ids = [...selectedRows];
  const total = ids.length;
  let done = 0;
  let errors = 0;

  btnEl.disabled = true;
  progWrap?.classList.remove("d-none");
  errEl?.classList.add("d-none");

  // Busca dados das linhas selecionadas pelo ID
  const rowsMap = {};
  dtInstance.rows().data().each((row) => {
    if (ids.includes(String(row.id))) rowsMap[String(row.id)] = row;
  });

  for (const id of ids) {
    const row = rowsMap[id];
    if (!row) { done++; continue; }

    try {
      const resp = await fetch(API_APPROVALS, {
        method: "POST",
        headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY", "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({
          vmdk_path: row.path,
          vcenter_id: String(row.vcenter_id ?? row.vcenter_host ?? ""),
          action: acaoEl.value,
          justificativa: justEl.value.trim(),
          analista: analEl.value.trim(),
        }),
      });
      if (!resp.ok) errors++;
    } catch (_) {
      errors++;
    }
    done++;
    const pct = Math.round((done / total) * 100);
    if (progBar) {
      progBar.style.width = `${pct}%`;
      progBar.textContent = `${done}/${total}`;
    }
  }

  btnEl.disabled = false;

  if (errors > 0 && errEl) {
    errEl.textContent = `${errors} de ${total} solicitações falharam (conflito de token ou erro na API).`;
    errEl.classList.remove("d-none");
  }

  if (done - errors > 0) {
    setTimeout(() => {
      bsModalBatch?.hide();
      selectedRows.clear();
      _updateBatchBar();
      dtInstance?.ajax.reload(null, false);
      window.refreshPendingBadge?.();
    }, 1500);
  }
}

// ── Inicialização dos modais Bootstrap ───────────────────────────────────────

function _initModals() {
  const detEl = document.getElementById("zh-modal-details");
  const apvEl = document.getElementById("zh-modal-approval");
  const batEl = document.getElementById("zh-modal-batch");

  if (detEl) bsModalDetails = new bootstrap.Modal(detEl);
  if (apvEl) bsModalApproval = new bootstrap.Modal(apvEl);
  if (batEl) bsModalBatch = new bootstrap.Modal(batEl);

  // Vincula botão de submit do modal de aprovação individual
  document.getElementById("apv-btn-submit")
    ?.addEventListener("click", submitApproval);

  // Vincula botão de submit do lote
  document.getElementById("batch-btn-submit")
    ?.addEventListener("click", submitBatchApproval);
}

// ── Helpers de sumário ────────────────────────────────────────────────────────

function _updateSummaryBar(json) {
  _setText("zh-summary-total", (json.total ?? 0).toLocaleString("pt-BR"));
  _setText("zh-summary-gb", json.total_size_gb != null && window.zhFormatSizeGB
    ? window.zhFormatSizeGB(json.total_size_gb) : (json.total_size_gb != null ? `${(+json.total_size_gb).toFixed(2)} GB` : "—"));
}

function _showTableError(httpStatus) {
  const tbody = document.querySelector("#zh-table-results tbody");
  if (tbody) {
    tbody.innerHTML =
      `<tr><td colspan="10" class="text-center py-4 text-zombie-red">`
      + `<i class="bi bi-wifi-off me-2"></i>Erro HTTP ${httpStatus} ao carregar resultados.`
      + `</td></tr>`;
  }
}

// ── Helpers de renderização ───────────────────────────────────────────────────

/**
 * Renderiza o tamanho em GB ou TB (>= 1024 GB) com tratamento para arquivos muito pequenos.
 *
 * Para BROKEN_CHAIN com tamanho ~0: o arquivo de descriptor (.vmdk texto) existe
 * mas o flat/extent de dados está ausente — há pouco ou nenhum espaço a recuperar.
 */
function _renderSize(gb, tipo) {
  if (gb == null) {
    return `<span class="text-muted-zh">—</span>`;
  }
  const n = +gb;
  if (n < 0.001) {
    const tooltip = tipo === "BROKEN_CHAIN"
      ? "Apenas o arquivo descriptor existe (dados ausentes — cadeia corrompida). Espaço a recuperar ≈ 0."
      : "Arquivo muito pequeno (< 1 MB)";
    return (
      `<span class="text-muted-zh" title="${tooltip}" style="font-size:.8rem;cursor:help;">`
      + `<i class="bi bi-info-circle me-1" style="font-size:.7rem;"></i>Descriptor`
      + `</span>`
    );
  }
  const label = n >= 1024 ? (n / 1024).toFixed(2) + " TB" : n.toFixed(2) + " GB";
  const parts = label.split(" ");
  return (
    `<span class="fw-semibold">${parts[0]}</span>`
    + `<span class="text-muted-zh ms-1" style="font-size:.72rem;">${parts[1]}</span>`
  );
}


function _typeBadge(tipo) {
  const m = ZM[tipo] ?? { label: tipo, color: "#6c757d", bg: "rgba(108,117,125,.2)", icon: "bi-question-circle", darkText: false };
  const textColor = m.darkText ? "#212529" : m.color;
  return (
    `<span class="d-inline-flex align-items-center gap-1 px-2 py-1 rounded-2 text-nowrap"`
    + ` style="background:${m.bg};color:${textColor};font-size:.72rem;font-weight:600;border:1px solid ${m.color}40;">`
    + `<i class="bi ${m.icon}" aria-hidden="true"></i>${_esc(m.label)}`
    + `</span>`
  );
}

function _statusBadge(status) {
  const m = STATUS_META[status] ?? { label: status ?? "—", cls: "text-bg-secondary" };
  return `<span class="badge ${m.cls}" style="font-size:.7rem;">${_esc(m.label)}</span>`;
}

/**
 * Barra de progresso de confiança: ≥85 verde, 60–84 amarelo, <60 cinza.
 * @param {number|null} score 0–100
 * @param {boolean} [wide=false] Exibe percentual textual também
 */
function _confidenceBar(score, wide = false) {
  if (score == null) return `<span class="text-muted-zh">—</span>`;
  const pct = Math.max(0, Math.min(100, Math.round(score)));
  const color = pct >= 85 ? "#198754" : pct >= 60 ? "#ffc107" : "#6c757d";
  const label = wide ? `<div class="mt-1 text-center fw-semibold" style="font-size:.8rem;color:${color};">${pct}%</div>` : "";
  return (
    `<div class="score-bar" style="position:relative;height:20px;background:#30363d;border-radius:4px;min-width:50px;overflow:hidden;">`
    + `<div class="score-fill" role="progressbar" style="width:${pct}%;background:${color};height:100%;border-radius:4px;transition:width .2s ease;"></div>`
    + `<span class="score-label" style="position:absolute;left:0;right:0;top:0;bottom:0;display:flex;align-items:center;justify-content:center;font-size:.7rem;font-weight:600;color:#e6edf3;text-shadow:0 0 2px #000;">${pct}%</span>`
    + `</div>`
    + (wide ? label : "")
  );
}

function _actionBtns(row) {
  const isWhitelist = row.status === "WHITELIST";
  const linkUi = row.vcenter_deeplink_ui || "";
  const linkFolder = row.vcenter_deeplink_folder || "";
  let btns = (
    `<div class="d-flex gap-1 justify-content-end flex-nowrap align-items-center">`
    + `<button class="btn btn-sm btn-outline-secondary zh-btn-details"`
    + ` style="padding:2px 8px;font-size:.75rem;" title="Ver detalhes técnicos">`
    + `<i class="bi bi-eye"></i></button>`
  );
  if (linkUi) {
    btns += `<a href="${_esc(linkUi)}" target="_blank" rel="noopener noreferrer" class="btn btn-sm btn-outline-primary" style="padding:2px 8px;font-size:.75rem;" title="Abrir no vSphere Client"><i class="bi bi-display"></i></a>`;
  }
  if (linkFolder) {
    btns += `<a href="${_esc(linkFolder)}" target="_blank" rel="noopener noreferrer" class="btn btn-sm btn-outline-secondary" style="padding:2px 8px;font-size:.75rem;" title="Abrir arquivo direto"><i class="bi bi-folder2-open"></i></a>`;
  }
  btns += (
    `<button class="btn btn-sm btn-outline-danger zh-btn-approval"`
    + ` style="padding:2px 8px;font-size:.75rem;"`
    + ` title="${isWhitelist ? 'VMDK em whitelist' : 'Solicitar aprovação de ação'}"`
    + `${isWhitelist ? " disabled" : ""}>`
    + `<i class="bi bi-shield-plus"></i></button>`
    + `</div>`
  );
  return btns;
}

// ── Utilitários gerais ────────────────────────────────────────────────────────

const _esc = (s) => String(s ?? "")
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;").replace(/'/g, "&#039;");

const _trunc = (s, n) => s && s.length > n ? s.slice(0, n - 1) + "…" : (s ?? "");

function _setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? "—";
}

function _setHtml(id, html) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
}

function _getUrlParam(key) {
  return new URLSearchParams(window.location.search).get(key);
}
