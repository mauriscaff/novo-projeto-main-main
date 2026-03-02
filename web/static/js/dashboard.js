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
        "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",
        "Accept": "application/json",

      },
      cache: "no-store",
    });

    if (!resp.ok) {
      throw new Error(`API retornou HTTP ${resp.status}: ${resp.statusText}`);
    }

    const data = await resp.json();
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
    _renderLastUpdated();
    _setLoadingState(false);
    _setErrorState(null);

  } catch (err) {
    console.error("[ZombieHunter Dashboard] Erro ao carregar dados:", err);
    _setErrorState(err.message);
    _setLoadingState(false);
  }
}

// ── Cards de resumo ───────────────────────────────────────────────────────────

/**
 * Preenche os 5 cards superiores com os totais do dashboard.
 * @param {Object} data Resposta da API /dashboard
 */
function _renderCards(data) {
  // Flask: total_zombies / total_size_gb; FastAPI: total_vmdks_all_time / total_size_all_time_gb
  _setCard("zh-card-total-vmdks", _fmt(data.total_zombies ?? data.total_vmdks_all_time));
  _setCard("zh-card-total-gb", _fmtGb(data.total_size_gb ?? data.total_size_all_time_gb));
  _setCard("zh-card-pending", _fmt(data.pending_approvals ?? 0));
  _setCard("zh-card-vcenter-count", _fmt(data.vcenter_count ?? 0));

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
  if (overlay) overlay.classList.toggle("d-none", !loading);

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
    el.textContent = `Erro ao carregar dados: ${msg}`;
    el.classList.remove("d-none");
  } else {
    el.classList.add("d-none");
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
