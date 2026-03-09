/**
 * dashboard.js — Lógica de apresentação do Dashboard do ZombieHunter
 * ==================================================================
 * Responsabilidades:
 *   1. Busca dados via GET /api/v1/dashboard (AJAX)
 *   2. Preenche os 5 cards de resumo
 *   3. Renderiza 3 gráficos Chart.js (donut, barras, linha)
 *   4. Popula a tabela de alertas recentes via DataTables
 *   5. Atualiza automaticamente a cada REFRESH_INTERVAL_MS
 *
 * Paleta de cores semânticas por tipo zombie:
 *   ORPHANED              → #f85149  (vermelho)
 *   SNAPSHOT_ORPHAN       → #fb8500  (laranja)
 *   BROKEN_CHAIN          → #d29922  (amarelo/âmbar)
 *   UNREGISTERED_DIR      → #bc8cff  (roxo)
 *   POSSIBLE_FALSE_POSITIVE → #6e7681 (cinza)
 *
 * Dependências globais (carregadas pelo base.html):
 *   Chart.js 4.x, DataTables 2.x, Bootstrap 5.3, jQuery 3.x
 *   window.zhFormatGB, window.zhFormatDate  (helpers do base.html)
 */

"use strict";

// ── Constantes de configuração ────────────────────────────────────────────────

const API_URL = "/api/v1/dashboard";
const REFRESH_INTERVAL_MS = 120_000;   // 2 minutos

/** Mapeamento tipo_zombie → { label, cor, ícone Bootstrap } */
const ZOMBIE_META = {
  ORPHANED: {
    label: "Orphaned",
    color: "#f85149",
    bg: "rgba(248,81,73,0.15)",
    icon: "bi-x-circle-fill",
  },
  SNAPSHOT_ORPHAN: {
    label: "Snapshot Orphan",
    color: "#fb8500",
    bg: "rgba(251,133,0,0.15)",
    icon: "bi-camera-fill",
  },
  BROKEN_CHAIN: {
    label: "Broken Chain",
    color: "#d29922",
    bg: "rgba(210,153,34,0.15)",
    icon: "bi-link-45deg",
  },
  UNREGISTERED_DIR: {
    label: "Unregistered Dir",
    color: "#bc8cff",
    bg: "rgba(188,140,255,0.15)",
    icon: "bi-folder-x",
  },
  POSSIBLE_FALSE_POSITIVE: {
    label: "False Positive",
    color: "#6e7681",
    bg: "rgba(110,118,129,0.15)",
    icon: "bi-question-circle-fill",
  },
};

// Ordem canônica para exibição consistente nas legendas
const ZOMBIE_TYPE_ORDER = [
  "ORPHANED",
  "SNAPSHOT_ORPHAN",
  "BROKEN_CHAIN",
  "UNREGISTERED_DIR",
  "POSSIBLE_FALSE_POSITIVE",
];

// ── Estado interno ────────────────────────────────────────────────────────────

let charts = {};          // { donut, bars, trend } — Chart.js instances
let dataTable = null;        // DataTable instance
let lastData = null;        // Último JSON retornado pela API
let refreshTimer = null;

// ── Inicialização ─────────────────────────────────────────────────────────────

function _initCharts() {
  // Configurações globais do Chart.js para o tema escuro
  if (window.Chart) {
    Chart.defaults.color = "#c9d1d9";
    Chart.defaults.font.family = "system-ui, -apple-system, sans-serif";
  }
}

document.addEventListener("DOMContentLoaded", () => {
  _initCharts();
  _initTable();
  _initStorageTable();
  _bindUiHooks();
  loadDashboard();

  // Atualização periódica automática
  refreshTimer = setInterval(loadDashboard, REFRESH_INTERVAL_MS);

  // Botão de atualização manual
  const btnRefresh = document.getElementById("zh-btn-refresh");
  if (btnRefresh) {
    btnRefresh.addEventListener("click", () => {
      loadDashboard(true);
    });
  }
});

function _bindUiHooks() {
  const emptyRunScanBtn = document.getElementById("zh-empty-run-scan");
  if (emptyRunScanBtn) {
    emptyRunScanBtn.addEventListener("click", () => {
      document.getElementById("zh-btn-new-scan")?.click();
    });
  }

  document.querySelectorAll("#zh-dashboard-ops-accordion .accordion-collapse").forEach((el) => {
    el.addEventListener("shown.bs.collapse", () => {
      _resizeAllCharts();
    });
  });
}

function _resizeAllCharts() {
  Object.values(charts).forEach((chart) => {
    if (chart && typeof chart.resize === "function") {
      chart.resize();
      chart.update("none");
    }
  });
}

// ── Carregamento principal ────────────────────────────────────────────────────

/**
 * Busca dados da API e atualiza toda a página.
 * @param {boolean} [forceSpinner=false] Exibe spinner mesmo que já haja dados
 */
async function loadDashboard(forceSpinner = false) {
  if (!lastData || forceSpinner) {
    _setLoadingState(true);
  }
  console.log("Starting loadDashboard... API_URL:", API_URL);

  try {
    const resp = await fetch(API_URL, {
      headers: {
        "Accept": "application/json",
      },
      credentials: "same-origin",
      cache: "no-store",
    });

    const storageResp = await fetch(API_URL + "/recoverable-storage", {
      headers: {
        "Accept": "application/json",
      },
      credentials: "same-origin",
      cache: "no-store",
    });

    if (!resp.ok) {
      throw new Error(`API Dashboard retornou HTTP ${resp.status}: ${resp.statusText}`);
    }
    if (!storageResp.ok) {
      console.warn(`API Storage retornou HTTP ${storageResp.status}`);
    }

    const data = await resp.json();
    const storageData = storageResp.ok ? await storageResp.json() : null;
    lastData = data;

    // Normaliza formato da API Flask para o esperado pelos gráficos
    // by_type: Flask retorna [{tipo_zombie, count, size_gb}]
    const byTypeRaw = Array.isArray(data.by_type)
      ? data.by_type
      : Object.entries(data.by_type || {}).map(([tipo_zombie, e]) => ({ tipo_zombie, count: e.count ?? 0 }));
    // Normaliza: garante que cada item tenha a chave `tipo` usada internamente
    const byTypeArray = byTypeRaw.map((x) => ({
      ...x,
      tipo: x.tipo_zombie ?? x.tipo,
    }));

    // top_vcenters: Flask retorna [{name, size_gb}]
    const byVcenter = (data.top_vcenters ?? data.by_vcenter ?? []).map((o) => ({
      ...o,
      vcenter: o.name ?? o.vcenter ?? o.datastore,
      total_size_gb: o.size_gb ?? o.total_size_gb ?? 0,
      count: o.count ?? o.total_vmdks ?? 0,
    }));

    // trend: Flask retorna [{job_id, scan_date, total_gb}]
    const trend = (data.trend ?? data.trend_last_10 ?? data.trend_last_4 ?? []).map((x) => ({
      ...x,
      started_at: x.scan_date ?? x.started_at ?? x.finished_at,
      completed_at: x.finished_at ?? x.completed_at,
      total_size_gb: x.total_gb ?? x.total_size_gb ?? 0,
      total_vmdks: x.total_vmdks ?? 0,
    }));

    _renderCards(data);
    _renderDonut(byTypeArray);
    _renderBars(byVcenter);
    _renderTrend(trend);
    // Flask retorna recent_alerts; FastAPI retornava recent_vmdks
    _renderTable(data.recent_alerts ?? data.recent_vmdks ?? []);
    if (storageData) {
      _renderStorageTab(storageData);
    }
    _renderLastUpdated();
    _setLoadingState(false);
    _setErrorState(null);
    document.dispatchEvent(new CustomEvent("zh-dashboard-loaded", { detail: data }));

  } catch (err) {
    console.error("[ZombieHunter Dashboard] Erro ao carregar dados:", err);
    _setErrorState(err.message);
    _setLoadingState(false);
  }
}

// ── Cards de resumo ───────────────────────────────────────────────────────────

/**
 * Preenche os cards de resumo com os totais do dashboard.
 * @param {Object} data Resposta da API /dashboard
 */
function _renderCards(data) {
  const totalVmdks = Number(data.total_zombies ?? data.total_vmdks_all_time ?? 0);
  const totalGb = Number(data.total_size_gb ?? data.total_size_all_time_gb ?? 0);

  // Flask: total_zombies / total_size_gb; FastAPI: total_vmdks_all_time / total_size_all_time_gb
  _setCard("zh-card-total-vmdks", _fmt(totalVmdks));
  _setCard("zh-card-total-gb", _fmtGb(totalGb));
  _setCard("zh-card-pending", _fmt(data.pending_approvals ?? 0));
  _setCard("zh-card-vcenter-count", _fmt(data.vcenter_count ?? 0));
  const failedVcenters = _getFailedVcenters(data);
  _setCard("zh-card-vcenter-failed", _fmt(failedVcenters));
  _setCard(
    "zh-card-vcenter-failed-sub",
    failedVcenters > 0 ? "requer atencao imediata" : "sem falhas de conectividade"
  );
  _renderEmptyState(data, totalVmdks, totalGb, failedVcenters);

  // Timestamp da última varredura com formatação local
  const lastScanEl = document.getElementById("zh-card-last-scan");
  if (lastScanEl) {
    if (data.last_scan_at) {
      lastScanEl.innerHTML =
        `<time datetime="${data.last_scan_at}" title="${data.last_scan_at}">`
        + window.zhFormatDate(data.last_scan_at)
        + `</time>`;
    } else {
      lastScanEl.textContent = "—";
    }
  }

  // Aplica destaque vermelho se houver aprovações pendentes
  const pendingCard = document.getElementById("zh-pending-card-wrapper");
  if (pendingCard) {
    const count = data.pending_approvals ?? 0;
    pendingCard.classList.toggle("zh-card-alert", count > 0);
  }
}

function _getFailedVcenters(data) {
  const explicitFailed = Number(data.vcenters_failed ?? data.failed_vcenters ?? 0);
  if (!Number.isNaN(explicitFailed) && explicitFailed > 0) {
    return explicitFailed;
  }

  const statuses = Array.isArray(window.ZH_VCENTER_STATUS) ? window.ZH_VCENTER_STATUS : [];
  if (statuses.length) {
    return statuses.filter((vc) => vc && vc.connected === false).length;
  }

  const expectedCount = Number(data.vcenter_count ?? 0);
  const seenInDashboard = Array.isArray(data.by_vcenter) ? data.by_vcenter.length : 0;
  return Math.max(0, expectedCount - seenInDashboard);
}

function _renderEmptyState(data, totalVmdks, totalGb, failedVcenters) {
  const wrapper = document.getElementById("zh-empty-state");
  if (!wrapper) return;

  const hasRecentRows = Array.isArray(data.recent_alerts ?? data.recent_vmdks)
    && (data.recent_alerts ?? data.recent_vmdks).length > 0;
  const hasMetrics = totalVmdks > 0 || totalGb > 0 || Boolean(data.last_scan_at) || hasRecentRows;
  wrapper.classList.toggle("d-none", hasMetrics);

  const msgEl = document.getElementById("zh-empty-state-msg");
  if (!msgEl) return;
  if (Number(data.vcenter_count ?? 0) === 0) {
    msgEl.textContent = "Nenhum vCenter ativo encontrado. Cadastre um vCenter e execute uma varredura.";
    return;
  }
  if (failedVcenters > 0) {
    msgEl.textContent = "Ha falhas de conectividade em vCenters. Corrija a conexao e execute uma varredura agora.";
    return;
  }
  msgEl.textContent = "Execute uma varredura para preencher os indicadores operacionais.";
}

function _setCard(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value;
}

// ── Gráfico 1: Donut — distribuição por tipo_zombie ──────────────────────────

/**
 * Renderiza ou atualiza o gráfico de rosca com a distribuição de tipos.
 * @param {Array<{tipo: string, count: number}>} byType
 */
function _renderDonut(byType) {
  const ctx = document.getElementById("zh-chart-donut");
  if (!ctx) return;

  // Ordena pelos tipos canônicos (os desconhecidos ficam no fim)
  const ordered = ZOMBIE_TYPE_ORDER
    .map((t) => byType.find((x) => x.tipo === t) ?? { tipo: t, count: 0 })
    .filter((x) => x.count > 0);

  // Adiciona tipos desconhecidos que vieram da API
  byType.forEach((x) => {
    if (!ZOMBIE_TYPE_ORDER.includes(x.tipo) && x.count > 0) ordered.push(x);
  });

  const labels = ordered.map((x) => (ZOMBIE_META[x.tipo]?.label ?? x.tipo));
  const values = ordered.map((x) => x.count);
  const colors = ordered.map((x) => ZOMBIE_META[x.tipo]?.color ?? "#6e7681");
  const bgs = ordered.map((x) => ZOMBIE_META[x.tipo]?.bg ?? "rgba(110,118,129,0.15)");

  const total = values.reduce((a, b) => a + b, 0);

  if (charts.donut) {
    // Atualiza dataset existente sem recriar o canvas (evita flicker)
    charts.donut.data.labels = labels;
    charts.donut.data.datasets[0].data = values;
    charts.donut.data.datasets[0].backgroundColor = colors;
    charts.donut.data.datasets[0].hoverBackgroundColor = colors.map((c) => c + "cc");
    charts.donut.update("active");
    return;
  }

  charts.donut = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels,
      datasets: [{
        data: values,
        backgroundColor: colors,
        hoverBackgroundColor: colors.map((c) => c + "cc"),
        borderColor: "#1c2128",
        borderWidth: 3,
        hoverOffset: 8,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      cutout: "68%",
      plugins: {
        legend: {
          position: "bottom",
          labels: {
            padding: 16,
            usePointStyle: true,
            pointStyleWidth: 10,
            font: { size: 12 },
          },
        },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const pct = total > 0 ? ((ctx.parsed / total) * 100).toFixed(1) : 0;
              return ` ${ctx.label}: ${ctx.parsed} (${pct}%)`;
            },
          },
        },
        // Texto central com o total
        beforeDraw: undefined,
      },
    },
    plugins: [{
      id: "zh-donut-center",
      beforeDraw(chart) {
        const { width, height, ctx: c } = chart;
        c.save();
        const cx = width / 2;
        const cy = height / 2 - 20; // acima da legenda
        c.font = `bold ${Math.min(width * 0.11, 28)}px system-ui`;
        c.fillStyle = "#e6edf3";
        c.textAlign = "center";
        c.textBaseline = "middle";
        c.fillText(total.toLocaleString("pt-BR"), cx, cy);
        c.font = `${Math.min(width * 0.055, 12)}px system-ui`;
        c.fillStyle = "#8b949e";
        c.fillText("VMDKs", cx, cy + Math.min(width * 0.09, 20));
        c.restore();
      },
    }],
  });
}

// ── Gráfico 2: Barras horizontais — Top 5 vCenters/datastores por GB ─────────

/**
 * Renderiza o gráfico de barras com os 5 maiores acumulados de GB por vCenter.
 * @param {Array<{vcenter: string, total_size_gb: number, count: number}>} byVcenter
 */
function _renderBars(byVcenter) {
  const ctx = document.getElementById("zh-chart-bars");
  if (!ctx) return;

  // Top 5 por tamanho, ordem decrescente
  const top5 = [...byVcenter]
    .sort((a, b) => (b.total_size_gb ?? 0) - (a.total_size_gb ?? 0))
    .slice(0, 5);

  const labels = top5.map((x) => _truncate(x.vcenter ?? x.datastore ?? "?", 24));
  const gbValues = top5.map((x) => +(x.total_size_gb ?? 0).toFixed(2));
  const counts = top5.map((x) => x.count ?? 0);

  if (charts.bars) {
    charts.bars.data.labels = labels;
    charts.bars.data.datasets[0].data = gbValues;
    charts.bars.data.datasets[0].customCounts = counts;
    charts.bars.update("active");
    return;
  }

  charts.bars = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "GB zombie",
        data: gbValues,
        customCounts: counts,
        backgroundColor: "rgba(88,166,255,0.25)",
        borderColor: "#58a6ff",
        borderWidth: 1,
        borderRadius: 6,
        borderSkipped: false,
      }],
    },
    options: {
      indexAxis: "y",
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: (ctx) => {
              const count = ctx.dataset.customCounts?.[ctx.dataIndex] ?? "?";
              return ` ${_fmtGb(ctx.parsed.x)}  (${count} VMDKs)`;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { color: "#30363d" },
          ticks: {
            callback: (v) => v + " GB",
            maxTicksLimit: 5,
          },
        },
        y: {
          grid: { display: false },
          ticks: { font: { size: 12 } },
        },
      },
    },
  });
}

// ── Gráfico 3: Linha — Histórico de varreduras ────────────────────────────────

/**
 * Renderiza o gráfico de linha com o histórico de GB detectados por varredura.
 * @param {Array<{started_at: string, total_size_gb: number, total_vmdks: number}>} trend
 */
function _renderTrend(trend) {
  const ctx = document.getElementById("zh-chart-trend");
  if (!ctx) return;

  // Mais recentes primeiro → inverte para ordem cronológica no eixo X
  const ordered = [...trend].reverse();

  const labels = ordered.map((x) => _shortDate(x.started_at ?? x.completed_at ?? ""));
  const gbData = ordered.map((x) => +(x.total_size_gb ?? 0).toFixed(2));
  const cntData = ordered.map((x) => x.total_vmdks ?? 0);

  if (charts.trend) {
    charts.trend.data.labels = labels;
    charts.trend.data.datasets[0].data = gbData;
    charts.trend.data.datasets[1].data = cntData;
    charts.trend.update("active");
    return;
  }

  charts.trend = new Chart(ctx, {
    type: "line",
    data: {
      labels,
      datasets: [
        {
          label: "GB detectados",
          data: gbData,
          borderColor: "#f85149",
          backgroundColor: "rgba(248,81,73,0.08)",
          borderWidth: 2,
          pointBackgroundColor: "#f85149",
          pointRadius: 4,
          pointHoverRadius: 6,
          fill: true,
          tension: 0.35,
          yAxisID: "yGb",
        },
        {
          label: "VMDKs encontrados",
          data: cntData,
          borderColor: "#58a6ff",
          backgroundColor: "transparent",
          borderWidth: 2,
          pointBackgroundColor: "#58a6ff",
          pointRadius: 4,
          pointHoverRadius: 6,
          fill: false,
          tension: 0.35,
          yAxisID: "yCnt",
          borderDash: [5, 3],
        },
      ],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      interaction: { mode: "index", intersect: false },
      plugins: {
        legend: {
          position: "top",
          labels: { usePointStyle: true, padding: 16, font: { size: 12 } },
        },
        tooltip: {
          callbacks: {
            title: (items) => `Varredura: ${items[0].label}`,
          },
        },
      },
      scales: {
        x: {
          grid: { color: "#30363d" },
          ticks: { font: { size: 11 } },
        },
        yGb: {
          position: "left",
          grid: { color: "#30363d" },
          ticks: {
            callback: (v) => _fmtGb(v),
            maxTicksLimit: 5,
          },
        },
        yCnt: {
          position: "right",
          grid: { display: false },
          ticks: {
            callback: (v) => v + " VMDKs",
            maxTicksLimit: 5,
          },
        },
      },
    },
  });
}

// ── Tabela de alertas recentes ────────────────────────────────────────────────

/**
 * Inicializa a instância DataTables com colunas e opções de localização pt-BR.
 */
function _initTable() {
  if (!document.getElementById("zh-table-recent")) return;

  dataTable = $("#zh-table-recent").DataTable({
    // Não carrega dados do DOM — será preenchida via _renderTable()
    data: [],
    columns: [
      {
        title: "VMDK Path",
        data: "path",
        render: (d) =>
          `<span class="font-monospace" style="font-size:.8rem" title="${_escHtml(d)}">`
          + _escHtml(_truncate(d, 55))
          + `</span>`,
      },
      {
        title: "vCenter",
        data: "vcenter_host",
        render: (d) => `<span class="text-zombie-blue">${_escHtml(d ?? "—")}</span>`,
      },
      {
        title: "Tamanho",
        data: "tamanho_gb",
        render: (d) =>
          `<span class="text-end d-block">${_fmtGb(d)}</span>`,
      },
      {
        title: "Tipo",
        data: "tipo_zombie",
        render: (d) => _typeBadge(d),
      },
      {
        title: "Detectado em",
        data: "created_at",
        render: (d) =>
          `<span data-zh-date="${d ?? ""}">${window.zhFormatDate(d)}</span>`,
      },
      {
        title: "Ações",
        data: null,
        orderable: false,
        render: (d, t, row) => _actionButtons(row),
      },
    ],
    order: [[4, "desc"]],
    pageLength: 10,
    lengthMenu: [10, 25, 50],
    searching: true,
    info: true,
    responsive: true,
    language: {
      url: null,
      // Textos traduzidos para pt-BR inline
      search: "Buscar:",
      lengthMenu: "Exibir _MENU_ por página",
      info: "Mostrando _START_–_END_ de _TOTAL_",
      infoEmpty: "Nenhum resultado",
      infoFiltered: "(filtrado de _MAX_ no total)",
      paginate: {
        first: "«",
        last: "»",
        next: "›",
        previous: "‹",
      },
      emptyTable: "Nenhum VMDK recente — execute uma varredura.",
      zeroRecords: "Nenhum resultado para o filtro aplicado.",
      loadingRecords: "Carregando...",
    },
    dom:
      "<'row align-items-center mb-2'"
      + "<'col-sm-6'l>"
      + "<'col-sm-6 d-flex justify-content-end'f>"
      + ">"
      + "<'row'<'col-12'tr>>"
      + "<'row mt-2'"
      + "<'col-sm-5 text-muted-zh small'i>"
      + "<'col-sm-7 d-flex justify-content-end'p>"
      + ">",
  });
}

/**
 * Atualiza os dados da tabela sem destruir e recriar a instância.
 * @param {Array} rows
 */
function _renderTable(rows) {
  if (!dataTable) return;
  dataTable.clear();
  if (rows.length > 0) {
    dataTable.rows.add(rows);
  }
  dataTable.draw(false); // false = mantém página atual
}

// ── Aba Storage Recuperável ───────────────────────────────────────────────────

let storageTable = null;

function _initStorageTable() {
  if (!document.getElementById("zh-table-storage")) return;

  storageTable = $("#zh-table-storage").DataTable({
    data: [],
    columns: [
      {
        data: "datastore_name",
        render: (d) => `<span class="fw-semibold text-zombie-blue">${_escHtml(d)}</span>`,
      },
      {
        data: "vcenter",
        render: (d) => `<span class="text-muted-zh">${_escHtml(d)}</span>`,
      },
      {
        data: "total_gb",
        render: (d) => `<span class="text-end d-block fw-semibold">${_fmtGb(d)}</span>`,
      },
      {
        data: "total_tb",
        render: (d) => `<span class="text-end d-block font-monospace" style="font-size: .8rem;">${(d || 0).toFixed(2)}</span>`,
      },
      {
        data: "zombie_count",
        render: (d) => `<span class="text-end d-block">${_fmt(d)}</span>`,
      },
      {
        data: "percentage_of_total",
        render: (d) => {
          const pct = parseFloat(d || 0).toFixed(1);
          return `
            <div class="d-flex align-items-center justify-content-start gap-2">
              <span style="font-size:.8rem; min-width:35px;" class="text-end">${pct}%</span>
              <div class="progress" style="width:60px;height:4px;background-color:#30363d;">
                <div class="progress-bar bg-zombie-yellow" role="progressbar" style="width:${pct}%;"></div>
              </div>
            </div>`;
        }
      },
      // Breakdowns por tipo (GB)
      {
        data: "by_type",
        className: "text-end",
        render: (d) => {
          const v = d?.ORPHANED?.gb || 0;
          return v > 0 ? `<span class="fw-semibold" style="color:var(--zh-accent-red);">${v.toFixed(1)}</span>` : '<span class="text-muted-zh" style="opacity:0.3;">0</span>';
        }
      },
      {
        data: "by_type",
        className: "text-end",
        render: (d) => {
          const v = d?.SNAPSHOT_ORPHAN?.gb || 0;
          return v > 0 ? `<span class="fw-semibold" style="color:var(--zh-accent-yellow);">${v.toFixed(1)}</span>` : '<span class="text-muted-zh" style="opacity:0.3;">0</span>';
        }
      },
      {
        data: "by_type",
        className: "text-end",
        render: (d) => {
          const v = d?.UNREGISTERED_DIR?.gb || 0;
          return v > 0 ? `<span class="fw-semibold" style="color:var(--zh-accent-purple);">${v.toFixed(1)}</span>` : '<span class="text-muted-zh" style="opacity:0.3;">0</span>';
        }
      },
      {
        data: "by_type",
        className: "text-end",
        render: (d) => {
          const bc = d?.BROKEN_CHAIN?.gb || 0;
          const fp = d?.POSSIBLE_FALSE_POSITIVE?.gb || 0;
          const v = bc + fp;
          return v > 0 ? `<span class="fw-semibold" style="color:var(--zh-accent-green);">${v.toFixed(1)}</span>` : '<span class="text-muted-zh" style="opacity:0.3;">0</span>';
        }
      }
    ],
    // Destaque visual (rowCallback) para linhas com mais de 500GB
    rowCallback: function (row, data) {
      if (data.total_gb > 500) {
        $(row).addClass('table-danger').css('opacity', '0.9'); // Usando classe Bootstrap ou custom
        // ou via style direto (melhor contraste no dark tema):
        $(row).find('td').css('background-color', 'rgba(248, 81, 73, 0.15)');
      } else if (data.total_gb > 200) {
        $(row).find('td').css('background-color', 'rgba(210, 153, 34, 0.1)'); // yellow warning
      }
    },
    footerCallback: function (row, data, start, end, display) {
      const api = this.api();

      // Funções helper para somar
      const intVal = function (i) {
        return typeof i === 'string' ? i.replace(/[\$,]/g, '') * 1 : typeof i === 'number' ? i : 0;
      };

      // Só soma a página atual
      const totObj = data.reduce((acc, curr) => {
        acc.gb += curr.total_gb || 0;
        acc.tb += curr.total_tb || 0;
        acc.cnt += curr.zombie_count || 0;
        acc.orph += curr.by_type?.ORPHANED?.gb || 0;
        acc.snap += curr.by_type?.SNAPSHOT_ORPHAN?.gb || 0;
        acc.unreg += curr.by_type?.UNREGISTERED_DIR?.gb || 0;
        acc.oth += (curr.by_type?.BROKEN_CHAIN?.gb || 0) + (curr.by_type?.POSSIBLE_FALSE_POSITIVE?.gb || 0);
        return acc;
      }, { gb: 0, tb: 0, cnt: 0, orph: 0, snap: 0, unreg: 0, oth: 0 });


      // Update footer elements
      $('#zh-st-tot-gb').html(_fmtGb(totObj.gb));
      $('#zh-st-tot-tb').html(totObj.tb.toFixed(2));
      $('#zh-st-tot-vmdks').html(_fmt(totObj.cnt));
      $('#zh-st-tot-orph').html(totObj.orph.toFixed(1));
      $('#zh-st-tot-snap').html(totObj.snap.toFixed(1));
      $('#zh-st-tot-unreg').html(totObj.unreg.toFixed(1));
      $('#zh-st-tot-oth').html(totObj.oth.toFixed(1));
    },
    order: [[2, "desc"]], // Ordena por Total GB desc
    pageLength: 10,
    lengthMenu: [10, 25, 50],
    searching: true,
    info: true,
    responsive: true,
    language: dataTable?.context[0].oLanguage, // Reaproveita language do dataTable principal
    dom:
      "<'row align-items-center mb-2'"
      + "<'col-sm-6'l>"
      + "<'col-sm-6 d-flex justify-content-end'f>"
      + ">"
      + "<'row'<'col-12'tr>>"
      + "<'row mt-2'"
      + "<'col-sm-5 text-muted-zh small'i>"
      + "<'col-sm-7 d-flex justify-content-end'p>"
      + ">",
  });
}

function _renderStorageTab(data) {
  _setCard("zh-storage-total-tb", data.total_recoverable_tb?.toFixed(2) ?? "—");
  _setCard("zh-storage-ds-count", _fmt(data.by_datastore?.length ?? 0));

  // Resume os tipos com base nos by_datastore
  const summaryByType = {};
  (data.by_datastore || []).forEach(ds => {
    Object.entries(ds.by_type || {}).forEach(([t, d]) => {
      if (!summaryByType[t]) summaryByType[t] = { count: 0, gb: 0 };
      summaryByType[t].count += d.count;
      summaryByType[t].gb += d.gb;
    });
  });

  const typeSummaryEl = document.getElementById("zh-storage-type-summary");
  if (typeSummaryEl) {
    let ht = '';
    // Filtra e ordena
    const typeEntries = Object.entries(summaryByType).filter(x => x[1].count > 0).sort((a, b) => b[1].gb - a[1].gb);
    if (typeEntries.length > 0) {
      ht = typeEntries.map(([t, tData]) => {
        const meta = ZOMBIE_META[t] ?? { label: t, color: "#6e7681" };
        return `
          <div class="d-flex flex-column" style="min-width:60px;">
            <span style="color:${meta.color};font-size:.7rem;font-weight:600;text-transform:uppercase;">${meta.label}</span>
            <span class="fs-5 fw-bold" style="line-height:1.2;">${_fmtGb(tData.gb)}</span>
            <span class="text-muted-zh" style="font-size:.75rem;">${tData.count} arquiv.</span>
          </div>`;
      }).join('');
    } else {
      ht = '<span class="text-muted-zh small">Nenhum dado por tipo.</span>';
    }
    typeSummaryEl.innerHTML = ht;
  }

  // Tabela
  if (storageTable && data.by_datastore) {
    storageTable.clear();
    storageTable.rows.add(data.by_datastore);
    storageTable.draw(false);
  }

  // Gráfico do TOP 10 Datastores
  _renderStorageDatastoresChart(data.by_datastore || []);

  // Cards de vCenter
  _renderVCenterCards(data.by_vcenter || []);
}

function _renderStorageDatastoresChart(byDatastore) {
  const ctx = document.getElementById("zh-chart-storage-datastores");
  if (!ctx || byDatastore.length === 0) return;

  const top10 = [...byDatastore].sort((a, b) => b.total_gb - a.total_gb).slice(0, 10);
  const labels = top10.map(d => _truncate(d.datastore_name, 20));

  // Preparando datasets agrupados
  const orphanedData = top10.map(d => d.by_type?.ORPHANED?.gb || 0);
  const snapshotData = top10.map(d => d.by_type?.SNAPSHOT_ORPHAN?.gb || 0);
  const unregData = top10.map(d => d.by_type?.UNREGISTERED_DIR?.gb || 0);
  const brokenData = top10.map(d => d.by_type?.BROKEN_CHAIN?.gb || 0);
  const falsePosData = top10.map(d => d.by_type?.POSSIBLE_FALSE_POSITIVE?.gb || 0);

  const datasets = [
    {
      label: ZOMBIE_META.ORPHANED.label,
      data: orphanedData,
      backgroundColor: ZOMBIE_META.ORPHANED.color,
    },
    {
      label: ZOMBIE_META.SNAPSHOT_ORPHAN.label,
      data: snapshotData,
      backgroundColor: ZOMBIE_META.SNAPSHOT_ORPHAN.color,
    },
    {
      label: ZOMBIE_META.UNREGISTERED_DIR.label,
      data: unregData,
      backgroundColor: ZOMBIE_META.UNREGISTERED_DIR.color,
    },
    {
      label: ZOMBIE_META.BROKEN_CHAIN.label,
      data: brokenData,
      backgroundColor: ZOMBIE_META.BROKEN_CHAIN.color,
    },
    {
      label: ZOMBIE_META.POSSIBLE_FALSE_POSITIVE.label,
      data: falsePosData,
      backgroundColor: ZOMBIE_META.POSSIBLE_FALSE_POSITIVE.color,
    }
  ].filter(ds => ds.data.some(v => v > 0)); // Remove datasets vazios

  if (charts.storageDatastores) {
    charts.storageDatastores.data.labels = labels;
    charts.storageDatastores.data.datasets = datasets;
    charts.storageDatastores.update();
    return;
  }

  charts.storageDatastores = new Chart(ctx, {
    type: "bar",
    data: {
      labels,
      datasets
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: {
          position: "bottom",
          labels: { usePointStyle: true, font: { size: 12 } }
        },
        tooltip: {
          mode: 'index',
          intersect: false,
          callbacks: {
            label: (ctx) => ` ${ctx.dataset.label}: ${_fmtGb(ctx.raw)}`
          }
        }
      },
      scales: {
        x: {
          stacked: true,
          grid: { display: false }
        },
        y: {
          stacked: true,
          grid: { color: "#30363d" },
          ticks: { callback: v => v + ' GB' }
        }
      }
    }
  });
}

function _renderVCenterCards(byVcenter) {
  const container = document.getElementById("zh-vcenter-cards-row");
  if (!container) return;
  document.getElementById("zh-vcenter-loading")?.remove();

  if (byVcenter.length === 0) {
    container.innerHTML = `<div class="col-12 text-muted-zh small">Nenhum dado de vCenter disponível.</div>`;
    return;
  }

  let html = '';
  byVcenter.forEach(vc => {
    // Top 3 datastores do vCenter
    const topDs = (vc.top_datastores || []).slice(0, 3).map(ds => `
      <div class="d-flex justify-content-between align-items-center mb-1">
        <span class="text-muted-zh text-truncate" style="font-size:.75rem; max-width:140px;" title="${_escHtml(ds.datastore_name)}">
          <i class="bi bi-hdd-fill me-1"></i>${_escHtml(ds.datastore_name)}
        </span>
        <span class="fw-semibold" style="font-size:.8rem;">${_fmtGb(ds.total_gb)}</span>
      </div>
    `).join('');

    html += `
    <div class="col-12 col-md-6 col-xl-4">
      <div class="zh-vc-card h-100 p-3">
        <div class="d-flex justify-content-between align-items-start mb-3">
          <div>
            <div class="fw-bold fs-6 mb-1 text-truncate" style="max-width:220px;" title="${_escHtml(vc.vcenter)}">
              <i class="bi bi-hdd-network-fill text-zombie-blue me-2"></i>${_escHtml(vc.vcenter)}
            </div>
            <span class="badge bg-secondary opacity-75" style="font-size:.65rem;">
              ${_fmt(vc.zombie_count)} problemas
            </span>
          </div>
          <div class="text-end">
            <div class="fs-4 fw-bold text-zombie-green">${(vc.total_tb || 0).toFixed(2)} TB</div>
            <div class="text-muted-zh" style="font-size:.7rem;">Potencial Liberável</div>
          </div>
        </div>
        
        <div class="mt-3 pt-3" style="border-top:1px dashed var(--zh-border);">
          <div class="text-muted-zh mb-2" style="font-size:.7rem;text-transform:uppercase;letter-spacing:.5px;">Top Datastores Alvo</div>
          ${topDs || '<span class="text-muted-zh small">Nenhum datastore reportado.</span>'}
        </div>
      </div>
    </div>`;
  });

  container.innerHTML = html;
}

// ── Helpers de UI ─────────────────────────────────────────────────────────────

/**
 * Gera badge HTML colorido para o tipo zombie.
 * @param {string} tipo
 * @returns {string} HTML
 */
function _typeBadge(tipo) {
  const meta = ZOMBIE_META[tipo] ?? { label: tipo, color: "#6e7681", bg: "rgba(110,118,129,0.15)", icon: "bi-question" };
  return (
    `<span class="d-inline-flex align-items-center gap-1 px-2 py-1 rounded-2" `
    + `style="background:${meta.bg};color:${meta.color};font-size:.75rem;font-weight:600;">`
    + `<i class="bi ${meta.icon}" aria-hidden="true"></i>`
    + `${_escHtml(meta.label)}`
    + `</span>`
  );
}

/**
 * Gera os botões de ação para cada linha da tabela.
 * @param {Object} row Linha do resultado
 * @returns {string} HTML
 */
function _actionButtons(row) {
  const encodedPath = encodeURIComponent(row.path ?? "");
  const jobId = row.job_id ?? "";
  return (
    `<div class="d-flex gap-1 justify-content-end">`
    // Botão de detalhes do job
    + `<a href="/scan/results/${jobId}" class="btn btn-sm btn-outline-secondary" `
    + `title="Ver varredura completa" style="padding:2px 8px;font-size:.75rem;">`
    + `<i class="bi bi-eye"></i>`
    + `</a>`
    // Botão de solicitar aprovação
    + `<a href="/approvals/new?vmdk_path=${encodedPath}" `
    + `class="btn btn-sm btn-outline-danger" `
    + `title="Solicitar aprovação para ação" style="padding:2px 8px;font-size:.75rem;">`
    + `<i class="bi bi-shield-plus"></i>`
    + `</a>`
    + `</div>`
  );
}

/**
 * Atualiza o timestamp "Última atualização" no card de status.
 */
function _renderLastUpdated() {
  const el = document.getElementById("zh-last-updated");
  if (el) {
    el.textContent = window.zhFormatDate(new Date().toISOString());
  }
}

/**
 * Exibe/oculta o estado de carregamento nos cards e gráficos.
 * @param {boolean} loading
 */
function _setLoadingState(loading) {
  const overlay = document.getElementById("zh-loading-overlay");
  if (overlay) {
    if (loading && window.zhFeedback) {
      window.zhFeedback.setInline(overlay, {
        state: "loading",
        text: "Carregando dashboard",
        detail: "Atualizando indicadores criticos e graficos.",
      });
    }
    overlay.classList.toggle("d-none", !loading);
  }

  const btnRefresh = document.getElementById("zh-btn-refresh");
  if (btnRefresh) {
    btnRefresh.disabled = loading;
    btnRefresh.querySelector(".bi")?.classList.toggle("spin", loading);
  }
}

/**
 * Exibe ou limpa a mensagem de erro global.
 * @param {string|null} msg
 */
function _setErrorState(msg) {
  const el = document.getElementById("zh-error-banner");
  if (!el) return;
  if (msg) {
    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo({ message: String(msg || "") }, "Falha ao carregar dashboard.")
      : { category: "unknown", message: String(msg || "Falha ao carregar dashboard.") };

    const nextByCategory = {
      auth: "Valide a sessao de usuario e tente atualizar novamente.",
      validation: "Revise os parametros da requisicao e tente novamente.",
      transient: "Verifique rede/API e clique em Atualizar em alguns segundos.",
      unknown: "Tente atualizar novamente. Se persistir, verifique logs da API.",
    };

    if (window.zhFeedback) {
      window.zhFeedback.setInline(el, {
        state: "error",
        category: info.category,
        title: "Falha ao carregar dashboard",
        happened: info.message,
        impact: "Os KPIs e graficos podem estar desatualizados.",
        nextStep: nextByCategory[info.category] || nextByCategory.unknown,
      });
    } else {
      el.textContent = `Erro ao carregar dados: ${info.message}`;
    }
    el.classList.remove("d-none");
  } else {
    if (window.zhFeedback) {
      window.zhFeedback.clear(el);
    } else {
      el.classList.add("d-none");
    }
  }
}

// ── Utilitários de formatação ─────────────────────────────────────────────────

/** Formata número inteiro com separador de milhar pt-BR. */
function _fmt(n) {
  return n != null ? Number(n).toLocaleString("pt-BR") : "—";
}

/** Formata GB → "X.XX GB" ou "X.XX TB" (>= 1024 GB). */
function _fmtGb(gb) {
  if (gb == null) return "—";
  const n = Number(gb);
  if (n >= 1024) return `${(n / 1024).toFixed(2)} TB`;
  return `${n.toFixed(2)} GB`;
}

/** Trunca string longa com reticências. */
function _truncate(str, max) {
  return str && str.length > max ? str.slice(0, max - 1) + "…" : (str ?? "");
}

/** Formata ISO para exibição curta no eixo X dos gráficos (DD/MM HH:mm). */
function _shortDate(iso) {
  if (!iso) return "?";
  try {
    const d = new Date(iso);
    return d.toLocaleDateString("pt-BR", { day: "2-digit", month: "2-digit" })
      + " " + d.toLocaleTimeString("pt-BR", { hour: "2-digit", minute: "2-digit" });
  } catch {
    return iso.slice(0, 10);
  }
}

/** Escapa caracteres HTML perigosos para evitar XSS em innerHTML. */
function _escHtml(str) {
  return String(str ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

// CSS de animação de rotação para o ícone de refresh
const _styleEl = document.createElement("style");
_styleEl.textContent = `
  @keyframes zh-spin { to { transform: rotate(360deg); } }
  .spin { display: inline-block; animation: zh-spin .7s linear infinite; }
`;
document.head.appendChild(_styleEl);
