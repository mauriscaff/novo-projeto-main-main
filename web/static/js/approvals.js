/**
 * approvals.js â€” LÃ³gica da pÃ¡gina de AprovaÃ§Ãµes do ZombieHunter
 * ==============================================================
 * Esta Ã© a pÃ¡gina mais crÃ­tica do sistema.
 * Toda interaÃ§Ã£o aqui tem efeito REAL no storage VMware.
 *
 * Responsabilidades:
 *   1. Carrega tokens por aba (pendentes / executados / cancelados+expirados)
 *   2. Renderiza cards com countdown de expiraÃ§Ã£o em tempo real
 *   3. Executa DRY-RUN via GET /api/v1/approvals/{token}/dryrun
 *   4. Exibe resultado do dry-run em modal â€” analista valida antes de prosseguir
 *   5. Executa aÃ§Ã£o real via POST /api/v1/approvals/{token}/execute
 *   6. Cancela tokens via DELETE /api/v1/approvals/{token}
 *   7. Carrega audit log na aba de histÃ³rico
 *
 * Endpoints consumidos:
 *   GET    /api/v1/approvals/                â†’ lista tokens
 *   GET    /api/v1/approvals/{token}/dryrun  â†’ simula aÃ§Ã£o
 *   POST   /api/v1/approvals/{token}/execute â†’ executa aÃ§Ã£o (bloqueado se READONLY)
 *   DELETE /api/v1/approvals/{token}         â†’ cancela token
 *   GET    /api/v1/approvals/audit-log       â†’ histÃ³rico imutÃ¡vel
 *
 * VariÃ¡veis globais injetadas pelo Jinja2:
 *   window.ZH_READONLY_MODE  boolean  â€” true se READONLY_MODE=true no .env
 */

"use strict";

// â”€â”€ Constantes â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

const API_BASE   = "/api/v1/approvals";
const POLL_MS    = 30_000;   // atualiza cards a cada 30s
const TICK_MS    = 1_000;    // tick do countdown
function _buildApiHeaders(headersInit = {}) {
  const headers = new Headers(headersInit || {});
  headers.delete("X-API-Key");
  if (!headers.has("Accept")) {
    headers.set("Accept", "application/json");
  }
  return headers;
}

async function _apiFetch(url, init = {}) {
  return fetch(url, {
    credentials: "same-origin",
    ...init,
    headers: _buildApiHeaders(init.headers || {}),
  });
}

/** Mapeamento tipo_zombie â†’ cores (consistente com dashboard.js e scan_results.js) */
const ZM = {
  ORPHANED:                { label: "Orphaned",         color: "#f85149", bg: "rgba(248,81,73,.15)",  icon: "bi-x-circle-fill" },
  SNAPSHOT_ORPHAN:         { label: "Snapshot Orphan",  color: "#fb8500", bg: "rgba(251,133,0,.15)",  icon: "bi-camera-fill" },
  BROKEN_CHAIN:            { label: "Broken Chain",     color: "#d29922", bg: "rgba(210,153,34,.15)", icon: "bi-link-45deg" },
  UNREGISTERED_DIR:        { label: "Unregistered Dir", color: "#bc8cff", bg: "rgba(188,140,255,.15)",icon: "bi-folder-x" },
  POSSIBLE_FALSE_POSITIVE: { label: "False Positive",   color: "#6e7681", bg: "rgba(110,118,129,.15)",icon: "bi-question-circle-fill" },
};

const ACTION_META = {
  QUARANTINE: { label: "QUARANTINE",  color: "#d29922", icon: "bi-archive-fill",  desc: "Move para pasta de quarentena (reversÃ­vel)" },
  DELETE:     { label: "DELETE",      color: "#f85149", icon: "bi-trash3-fill",   desc: "Remove permanentemente (IRREVERSÃVEL)" },
};

// â”€â”€ Estado â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

let countdownTimers = {};          // { tokenValue: intervalId }
let pollTimer       = null;
let activeTab       = "pending";   // "pending" | "executed" | "cancelled"

// Dry-run state por token
let dryRunResults   = {};          // { tokenValue: resultObject }

// Modal Bootstrap instances
let bsModalDryRun   = null;
let bsModalExecute  = null;
let bsModalCancel   = null;
let bsModalAudit    = null;

// Token atualmente em operaÃ§Ã£o
let activeToken     = null;
const _tabTokenCache = { pending: [], executed: [], cancelled: [] };

// â”€â”€ Bootstrap isReadOnly â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

const isReadOnly = () => window.ZH_READONLY_MODE === true;

// â”€â”€ InicializaÃ§Ã£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

document.addEventListener("DOMContentLoaded", () => {
  _initModals();
  _bindTabs();
  _bindOperationalGuide();
  _updateOperationalGuide({ loading: true, tab: "pending" });
  loadTab("pending");

  // Polling automÃ¡tico
  pollTimer = setInterval(() => loadTab(activeTab, false), POLL_MS);
});

// â”€â”€ Carregamento por aba â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

/**
 * Carrega tokens de acordo com a aba ativa.
 * @param {"pending"|"executed"|"cancelled"} tab
 * @param {boolean} [showLoader=true]
 */
async function loadTab(tab, showLoader = true) {
  activeTab = tab;

  // Cancelar countdowns ativos antes de re-renderizar
  _clearCountdowns();

  const container = document.getElementById(`zh-tab-${tab}`);
  if (!container) return;

  if (showLoader) {
    _setLoading(container, true);
    _updateOperationalGuide({ loading: true, tab });
    if (window.zhFeedback) {
      window.zhFeedback.setInline("#zh-approvals-feedback", {
        state: "loading",
        text: "Carregando aprovacoes",
        detail: "Sincronizando tokens pendentes, executados e expiracao.",
      });
    }
  }

  try {
    let tokens = [];

    if (tab === "pending") {
      // Tokens nÃ£o-terminais nÃ£o-expirados
      tokens = await _fetchTokens({ only_active: "true" });

    } else if (tab === "executed") {
      tokens = await _fetchTokens({ status: "executed" });

    } else {
      // Cancelados + invalidados
      const [cancelled, invalidated] = await Promise.all([
        _fetchTokens({ status: "cancelled" }),
        _fetchTokens({ status: "invalidated" }),
      ]);
      // Expirados (active=false, nÃ£o executados, nÃ£o cancelados)
      const expired = await _fetchTokens({ status: "pending_dryrun" });
      const now = Date.now();
      const expiredFiltered = expired.filter(
        (t) => new Date(t.expires_at).getTime() < now
      );
      tokens = [...cancelled, ...invalidated, ...expiredFiltered];
    }

    _renderCards(container, tokens, tab);
    _updateTabCounter(tab, tokens.length);
    _tabTokenCache[tab] = Array.isArray(tokens) ? tokens : [];
    window.zhFeedback?.clear("#zh-approvals-feedback");
    _updateOperationalGuide({ tab, tokens: _tabTokenCache[tab] });

  } catch (err) {
    console.error(`[ZH Approvals] Erro ao carregar aba '${tab}':`, err);
    _setError(container, err.message);
    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo(err, "Falha ao carregar aprovacoes.")
      : { category: "unknown", message: err?.message || "Falha ao carregar aprovacoes." };
    if (window.zhFeedback) {
      window.zhFeedback.setInline("#zh-approvals-feedback", {
        state: "error",
        category: info.category,
        title: "Falha ao carregar aprovacoes",
        happened: info.message,
        impact: "A lista da aba selecionada pode ficar incompleta.",
        nextStep: info.category === "auth"
          ? "Refaca o login e atualize a aba."
          : "Tente atualizar novamente em alguns segundos.",
      });
    }
    _updateOperationalGuide({ tab, error: err });
  } finally {
    _setLoading(container, false);
  }
}

async function _fetchTokens(params = {}) {
  const qs  = new URLSearchParams(params).toString();
  const url = `${API_BASE}/${qs ? "?" + qs : ""}`;
  const resp = await _apiFetch(url);
  if (!resp.ok) {
    const err = new Error(`HTTP ${resp.status}`);
    err.status = resp.status;
    throw err;
  }
  return resp.json();
}

// â”€â”€ RenderizaÃ§Ã£o de cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _renderCards(container, tokens, tab) {
  if (tokens.length === 0) {
    container.innerHTML = _emptyState(tab);
    return;
  }

  // Ordena: mais recentes primeiro
  tokens.sort((a, b) => new Date(b.created_at) - new Date(a.created_at));

  container.innerHTML = tokens.map((t) => _buildCard(t, tab)).join("");

  // Inicia countdowns nos cards pendentes
  if (tab === "pending") {
    tokens.forEach((t) => _startCountdown(t.approval_token, t.expires_at));
  }
}

/**
 * ConstrÃ³i HTML de um card de aprovaÃ§Ã£o.
 * @param {Object} token
 * @param {string} tab
 */
function _buildCard(token, tab) {
  const isPending   = tab === "pending";
  const isExecuted  = tab === "executed";
  const zm          = ZM[token.vmdk_tipo_zombie] ?? { label: "Desconhecido", color: "#6e7681", bg: "rgba(110,118,129,.15)", icon: "bi-question-circle" };
  const actionMeta  = ACTION_META[token.action] ?? { label: token.action, color: "#6e7681", icon: "bi-gear", desc: "" };
  const dryRunDone  = token.status === "dryrun_done";
  const tokenVal    = token.approval_token;

  // Classe de borda lateral baseada na aÃ§Ã£o
  const borderColor = token.action === "DELETE" ? "#f85149" : "#d29922";

  return `
  <div class="zh-approval-card mb-3" id="card-${_safeId(tokenVal)}"
    style="border-left: 4px solid ${borderColor};">

    <!-- â”€â”€ CabeÃ§alho do card â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ -->
    <div class="d-flex align-items-start justify-content-between flex-wrap gap-2 mb-3">

      <div class="d-flex flex-column gap-1">
        <!-- Badge de aÃ§Ã£o -->
        <div class="d-flex align-items-center gap-2 flex-wrap">
          <span class="zh-action-badge"
            style="background:${actionMeta.color}22;color:${actionMeta.color};border:1px solid ${actionMeta.color}55;">
            <i class="bi ${actionMeta.icon} me-1"></i>${actionMeta.label}
          </span>
          ${_typeBadge(token.vmdk_tipo_zombie)}
          ${_statusPill(token.status)}
        </div>
        <!-- Token ID (truncado para UX) -->
        <code class="text-muted-zh" style="font-size:.7rem;" title="${_esc(tokenVal)}">
          Token: ${_esc(tokenVal.slice(0, 16))}â€¦
        </code>
      </div>

      <!-- Countdown de expiraÃ§Ã£o (apenas pendentes) -->
      ${isPending ? `
      <div class="text-end flex-shrink-0">
        <div class="text-muted-zh" style="font-size:.7rem;">Expira em</div>
        <div class="zh-countdown fw-bold" id="cd-${_safeId(tokenVal)}"
          data-expires="${token.expires_at}" style="font-size:1rem;">
          â€¦
        </div>
      </div>
      ` : `
      <div class="text-end flex-shrink-0">
        <div class="text-muted-zh" style="font-size:.7rem;">${isExecuted ? "Executado em" : "Encerrado em"}</div>
        <div style="font-size:.82rem;color:#8b949e;">
          ${window.zhFormatDate(isExecuted ? token.executed_at : token.created_at)}
        </div>
      </div>
      `}
    </div>

    <!-- â”€â”€ InformaÃ§Ãµes do VMDK â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ -->
    <div class="zh-vmdk-info mb-3">
      <div class="d-flex align-items-start gap-2 mb-2">
        <i class="bi bi-file-earmark-code text-muted-zh mt-1 flex-shrink-0"></i>
        <code class="d-block" style="word-break:break-all;font-size:.82rem;color:#e6edf3;"
          title="${_esc(token.vmdk_path)}">${_esc(token.vmdk_path)}</code>
      </div>
      <div class="row g-2 mt-1">
        <div class="col-6 col-md-3">
          <div class="zh-info-cell">
            <span class="zh-info-label">vCenter</span>
            <span class="zh-info-value text-zombie-blue">${_esc(token.vcenter_id)}</span>
          </div>
        </div>
        <div class="col-6 col-md-3">
          <div class="zh-info-cell">
            <span class="zh-info-label">Tamanho</span>
            <span class="zh-info-value text-zombie-yellow fw-bold">
              ${token.vmdk_size_gb != null ? (window.zhFormatSizeGB ? window.zhFormatSizeGB(token.vmdk_size_gb) : (+token.vmdk_size_gb).toFixed(2) + " GB") : "â€”"}
            </span>
          </div>
        </div>
        <div class="col-6 col-md-3">
          <div class="zh-info-cell">
            <span class="zh-info-label">Datacenter</span>
            <span class="zh-info-value">${_esc(token.vmdk_datacenter ?? "â€”")}</span>
          </div>
        </div>
        <div class="col-6 col-md-3">
          <div class="zh-info-cell">
            <span class="zh-info-label">Tipo zombie</span>
            <span class="zh-info-value" style="color:${zm.color};">${zm.label}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- â”€â”€ Solicitante + Justificativa â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ -->
    <div class="row g-2 mb-3">
      <div class="col-md-4">
        <div class="zh-info-cell">
          <span class="zh-info-label"><i class="bi bi-person-fill me-1"></i>Analista</span>
          <span class="zh-info-value fw-semibold">${_esc(token.analista)}</span>
        </div>
      </div>
      <div class="col-md-4">
        <div class="zh-info-cell">
          <span class="zh-info-label"><i class="bi bi-calendar3 me-1"></i>Solicitado em</span>
          <span class="zh-info-value">${window.zhFormatDate(token.created_at)}</span>
        </div>
      </div>
      <div class="col-md-4">
        <div class="zh-info-cell">
          <span class="zh-info-label"><i class="bi bi-shield-check me-1"></i>Dry-run</span>
          <span class="zh-info-value ${dryRunDone ? "text-zombie-green" : "text-zombie-yellow"}">
            ${dryRunDone
              ? `<i class="bi bi-check-circle-fill me-1"></i>ConcluÃ­do em ${window.zhFormatDate(token.dryrun_completed_at)}`
              : `<i class="bi bi-hourglass me-1"></i>Pendente`}
          </span>
        </div>
      </div>
      <div class="col-12">
        <div class="zh-info-cell">
          <span class="zh-info-label"><i class="bi bi-chat-text-fill me-1"></i>Justificativa</span>
          <span class="zh-info-value" style="font-style:italic;color:#c9d1d9;">
            "${_esc(token.justificativa)}"
          </span>
        </div>
      </div>
    </div>

    <!-- â”€â”€ Resultado do dry-run (se jÃ¡ executado) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ -->
    ${dryRunDone && token.dryrun_result ? _dryRunSummary(token.dryrun_result) : ""}

    <!-- â”€â”€ BotÃµes de aÃ§Ã£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€ -->
    <div class="d-flex align-items-center gap-2 flex-wrap mt-3 pt-3"
      style="border-top:1px solid var(--zh-border);">

      ${isPending ? _pendingButtons(token, dryRunDone) : _historicBadge(token)}

    </div>
  </div>`;
}

/** BotÃµes de aÃ§Ã£o para tokens PENDENTES */
function _pendingButtons(token, dryRunDone) {
  const tv = token.approval_token;
  const sid = _safeId(tv);

  if (isReadOnly()) {
    return `
    <div class="d-flex align-items-center gap-2 flex-grow-1 py-2 px-3 rounded-2"
      style="background:rgba(248,81,73,.08);border:1px solid rgba(248,81,73,.3);">
      <i class="bi bi-lock-fill text-zombie-red flex-shrink-0"></i>
      <span style="font-size:.84rem;color:#f85149;font-weight:600;">
        READONLY_MODE Ativo â€” AÃ§Ãµes bloqueadas pelo administrador
      </span>
    </div>`;
  }

  return `
  <!-- BotÃ£o 1: DRY-RUN (sempre habilitado) -->
  <button
    class="btn btn-sm btn-outline-warning d-flex align-items-center gap-2 zh-btn-dryrun"
    data-token="${_esc(tv)}"
    title="Simular a aÃ§Ã£o sem executar nada (obrigatÃ³rio)"
    type="button"
    id="btn-dry-${sid}"
  >
    <i class="bi bi-search"></i>
    <span>${dryRunDone ? "Re-executar DRY-RUN" : "Executar DRY-RUN"}</span>
  </button>

  <!-- BotÃ£o 2: CONFIRMAR (sÃ³ habilitado apÃ³s dry-run) -->
  <button
    class="btn btn-sm zh-btn-execute d-flex align-items-center gap-2"
    data-token="${_esc(tv)}"
    data-action="${_esc(token.action)}"
    title="${dryRunDone ? "Executar aÃ§Ã£o aprovada" : "Execute o DRY-RUN primeiro"}"
    type="button"
    id="btn-exec-${sid}"
    ${dryRunDone ? "" : "disabled"}
    style="${token.action === "DELETE"
      ? "background:rgba(248,81,73,.15);color:#f85149;border:1px solid rgba(248,81,73,.4);"
      : "background:rgba(210,153,34,.15);color:#d29922;border:1px solid rgba(210,153,34,.4);"}"
  >
    <i class="bi bi-${dryRunDone ? "play-fill" : "lock-fill"}"></i>
    <span>Confirmar ${token.action}</span>
  </button>

  <!-- BotÃ£o 3: CANCELAR -->
  <button
    class="btn btn-sm btn-outline-secondary d-flex align-items-center gap-2 zh-btn-cancel ms-auto"
    data-token="${_esc(tv)}"
    data-path="${_esc(token.vmdk_path)}"
    title="Cancelar este token de aprovaÃ§Ã£o"
    type="button"
    id="btn-cancel-${sid}"
  >
    <i class="bi bi-x-lg"></i>
    <span>Cancelar</span>
  </button>`;
}

/** Badge de status para tokens nÃ£o-pendentes */
function _historicBadge(token) {
  const meta = {
    executed:    { icon: "bi-check-circle-fill",  color: "#3fb950", label: "Executado com sucesso" },
    cancelled:   { icon: "bi-x-circle-fill",       color: "#6e7681", label: "Cancelado" },
    invalidated: { icon: "bi-exclamation-triangle-fill", color: "#d29922", label: "Invalidado" },
    dryrun_done: { icon: "bi-hourglass-split",     color: "#58a6ff", label: "Aguardando execuÃ§Ã£o" },
  };
  const m = meta[token.status] ?? { icon: "bi-question", color: "#6e7681", label: token.status };
  return `
  <div class="d-flex align-items-center gap-2" style="color:${m.color};font-size:.84rem;">
    <i class="bi ${m.icon}"></i>
    <span>${m.label}</span>
    ${token.invalidation_reason
      ? `<span class="text-muted-zh ms-2" style="font-size:.78rem;">â€” ${_esc(token.invalidation_reason)}</span>`
      : ""}
  </div>`;
}

/** Mini-resumo do resultado do dry-run dentro do card */
function _dryRunSummary(result) {
  const files    = result.files_affected?.length ?? 0;
  const gb       = result.space_to_recover_gb ?? 0;
  const warnings = result.warnings ?? [];
  const safe     = result.is_safe_to_proceed;

  return `
  <div class="rounded-2 p-3 mb-1"
    style="background:${safe ? "rgba(63,185,80,.08)" : "rgba(210,153,34,.08)"};
           border:1px solid ${safe ? "rgba(63,185,80,.3)" : "rgba(210,153,34,.3)"};">
    <div class="d-flex align-items-center gap-2 mb-2">
      <i class="bi bi-${safe ? "check-circle-fill text-zombie-green" : "exclamation-triangle-fill text-zombie-yellow"}"></i>
      <span class="fw-semibold" style="font-size:.84rem;">
        Resultado do DRY-RUN ${safe ? "â€” Seguro para prosseguir" : "â€” AtenÃ§Ã£o necessÃ¡ria"}
      </span>
    </div>
    <div class="row g-2 mb-2" style="font-size:.8rem;">
      <div class="col-auto">
        <span class="text-muted-zh">Arquivos afetados:</span>
        <strong class="ms-1">${files}</strong>
      </div>
      <div class="col-auto">
        <span class="text-muted-zh">EspaÃ§o a liberar:</span>
        <strong class="ms-1 text-zombie-yellow">${window.zhFormatSizeGB ? window.zhFormatSizeGB(gb) : (+gb).toFixed(2) + " GB"}</strong>
      </div>
    </div>
    ${warnings.length ? `
    <ul class="mb-0 ps-3" style="font-size:.78rem;">
      ${warnings.map((w) => `<li class="text-zombie-yellow">${_esc(w)}</li>`).join("")}
    </ul>` : ""}
  </div>`;
}

// â”€â”€ Countdown de expiraÃ§Ã£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _startCountdown(tokenVal, expiresAt) {
  const el  = document.getElementById(`cd-${_safeId(tokenVal)}`);
  if (!el) return;

  const expires = new Date(expiresAt).getTime();

  const tick = () => {
    const remaining = expires - Date.now();
    if (remaining <= 0) {
      el.textContent = "EXPIRADO";
      el.style.color = "#f85149";
      el.closest(".zh-approval-card")?.classList.add("zh-card-expired");
      clearInterval(countdownTimers[tokenVal]);
      delete countdownTimers[tokenVal];
      // Desabilita botÃµes deste card
      const sid = _safeId(tokenVal);
      document.getElementById(`btn-dry-${sid}`)?.setAttribute("disabled", "");
      document.getElementById(`btn-exec-${sid}`)?.setAttribute("disabled", "");
      return;
    }

    const h = Math.floor(remaining / 3_600_000);
    const m = Math.floor((remaining % 3_600_000) / 60_000);
    const s = Math.floor((remaining % 60_000) / 1_000);

    el.textContent = `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;

    // Cor baseada no tempo restante
    if (remaining < 3_600_000) {          // < 1h â†’ vermelho
      el.style.color = "#f85149";
    } else if (remaining < 7_200_000) {   // < 2h â†’ amarelo
      el.style.color = "#d29922";
    } else {
      el.style.color = "#3fb950";
    }
  };

  tick();
  countdownTimers[tokenVal] = setInterval(tick, TICK_MS);
}

function _clearCountdowns() {
  Object.values(countdownTimers).forEach(clearInterval);
  countdownTimers = {};
}

// â”€â”€ DRY-RUN â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function executeDryRun(tokenVal) {
  activeToken = tokenVal;
  const sid   = _safeId(tokenVal);

  // Estado visual do botÃ£o
  const btn = document.getElementById(`btn-dry-${sid}`);
  if (btn) {
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Simulandoâ€¦`;
  }

  // Limpa estado anterior
  _setModalContent("dry-modal-result", `
    <div class="text-center py-4">
      <div class="spinner-border text-zombie-yellow mb-3" style="width:2rem;height:2rem;"></div>
      <p class="text-muted-zh">Consultando vCenterâ€¦</p>
    </div>`);

  bsModalDryRun?.show();

  try {
    const resp = await _apiFetch(`${API_BASE}/${encodeURIComponent(tokenVal)}/dryrun`);
    const data = await resp.json();

    if (!resp.ok) {
      throw new Error(data.detail ?? `HTTP ${resp.status}`);
    }

    dryRunResults[tokenVal] = data;
    _renderDryRunResult(data);

    // Habilita botÃ£o de execuÃ§Ã£o no card
    const execBtn = document.getElementById(`btn-exec-${sid}`);
    if (execBtn) {
      execBtn.removeAttribute("disabled");
      execBtn.querySelector("i")?.classList.replace("bi-lock-fill", "bi-play-fill");
      execBtn.title = "Executar aÃ§Ã£o aprovada";
    }

    // Atualiza mini-resumo no card
    const card = document.getElementById(`card-${sid}`);
    if (card) {
      const existing = card.querySelector(".zh-dryrun-summary");
      const summaryHtml = _dryRunSummary(data);
      const tempDiv = document.createElement("div");
      tempDiv.innerHTML = summaryHtml;
      tempDiv.firstElementChild.classList.add("zh-dryrun-summary");
      if (existing) existing.replaceWith(tempDiv.firstElementChild);
      else {
        const btnRow = card.querySelector(".zh-btn-dryrun")?.closest(".d-flex.align-items-center");
        btnRow?.before(tempDiv.firstElementChild);
      }
    }

  } catch (err) {
    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo(err, "Falha ao executar dry-run.")
      : { category: "unknown", message: err?.message || "Falha ao executar dry-run." };

    if (window.zhFeedback) {
      _setModalContent("dry-modal-result", window.zhFeedback.renderAlert({
        state: "error",
        category: info.category,
        title: "Dry-run indisponivel",
        happened: info.message,
        impact: "A validacao de seguranca nao foi concluida para este token.",
        nextStep: info.category === "auth"
          ? "Refaca o login e repita o dry-run."
          : "Verifique conectividade/permissoes e tente novamente.",
      }));
    } else {
      _setModalContent("dry-modal-result", `
      <div class="alert alert-danger d-flex gap-2 py-2 mb-0">
        <i class="bi bi-x-circle-fill flex-shrink-0 mt-1"></i>
        <div>
          <strong>Erro ao executar dry-run:</strong><br/>
          <span style="font-size:.85rem;">${_esc(info.message)}</span>
        </div>
      </div>`);
    }

    _showToast("danger", `Falha no dry-run: ${info.message}`);
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.innerHTML = `<i class="bi bi-search me-2"></i>Re-executar DRY-RUN`;
    }
  }
}

function _renderDryRunResult(data) {
  const safe = data.is_safe_to_proceed;
  const warnings = data.warnings ?? [];

  const html = `
  <!-- Status de seguranÃ§a -->
  <div class="d-flex align-items-center gap-3 p-3 rounded-2 mb-3"
    style="background:${safe ? "rgba(63,185,80,.1)" : "rgba(210,153,34,.12)"};
           border:1px solid ${safe ? "rgba(63,185,80,.35)" : "rgba(210,153,34,.35)"};">
    <i class="bi bi-${safe ? "shield-fill-check text-zombie-green" : "shield-exclamation text-zombie-yellow"}"
      style="font-size:1.8rem;"></i>
    <div>
      <div class="fw-bold" style="color:${safe ? "#3fb950" : "#d29922"};">
        ${safe ? "Seguro para prosseguir" : "AtenÃ§Ã£o necessÃ¡ria antes de confirmar"}
      </div>
      <div class="text-muted-zh" style="font-size:.8rem;">${_esc(data.action_preview ?? "")}</div>
    </div>
  </div>

  <!-- MÃ©tricas -->
  <div class="row g-2 mb-3">
    <div class="col-6">
      <div class="zh-info-cell text-center">
        <div class="text-zombie-yellow fw-bold fs-4">${window.zhFormatSizeGB ? window.zhFormatSizeGB(data.space_to_recover_gb ?? 0) : (+(data.space_to_recover_gb ?? 0)).toFixed(2) + " GB"}</div>
        <div class="text-muted-zh" style="font-size:.72rem;">EspaÃ§o a liberar</div>
      </div>
    </div>
    <div class="col-6">
      <div class="zh-info-cell text-center">
        <div class="text-zombie-blue fw-bold fs-4">${data.files_affected?.length ?? 0}</div>
        <div class="text-muted-zh" style="font-size:.72rem;">Arquivos afetados</div>
      </div>
    </div>
  </div>

  <!-- Arquivos que serÃ£o afetados -->
  ${data.files_affected?.length ? `
  <div class="mb-3">
    <div class="text-muted-zh mb-1" style="font-size:.72rem;text-transform:uppercase;letter-spacing:.6px;">
      Arquivos que serÃ£o afetados
    </div>
    <div class="zh-file-list">
      ${data.files_affected.map((f) =>
        `<div class="d-flex align-items-center gap-2 py-1" style="border-bottom:1px solid var(--zh-border);font-size:.78rem;">
          <i class="bi bi-file-earmark-code text-muted-zh flex-shrink-0"></i>
          <code style="word-break:break-all;color:#e6edf3;">${_esc(f)}</code>
        </div>`
      ).join("")}
    </div>
  </div>` : ""}

  <!-- VerificaÃ§Ã£o live no vCenter -->
  ${data.live_check?.attempted ? `
  <div class="mb-3">
    <div class="text-muted-zh mb-1" style="font-size:.72rem;text-transform:uppercase;letter-spacing:.6px;">
      VerificaÃ§Ã£o Live no vCenter
    </div>
    <div class="d-flex align-items-center gap-2 p-2 rounded-2"
      style="background:var(--zh-bg-sidebar);border:1px solid var(--zh-border);font-size:.82rem;">
      ${data.live_check.exists === true
        ? `<i class="bi bi-check-circle-fill text-zombie-green"></i><span>Arquivo encontrado no vCenter (${data.live_check.size_bytes ? (window.zhFormatSizeGB ? window.zhFormatSizeGB(data.live_check.size_bytes / 1073741824) : (data.live_check.size_bytes / 1073741824).toFixed(2) + " GB") : "tamanho desconhecido"})</span>`
        : data.live_check.exists === false
          ? `<i class="bi bi-x-circle-fill text-zombie-red"></i><span>Arquivo NÃƒO encontrado no vCenter â€” pode ter sido removido</span>`
          : `<i class="bi bi-question-circle text-zombie-yellow"></i><span>${_esc(data.live_check.error ?? "VerificaÃ§Ã£o inconclusiva")}</span>`}
    </div>
  </div>` : ""}

  <!-- Avisos -->
  ${warnings.length ? `
  <div class="mb-0">
    <div class="text-muted-zh mb-1" style="font-size:.72rem;text-transform:uppercase;letter-spacing:.6px;">
      Avisos
    </div>
    <ul class="list-unstyled mb-0">
      ${warnings.map((w) => `
        <li class="d-flex align-items-start gap-2 mb-2">
          <i class="bi bi-exclamation-triangle-fill text-zombie-yellow flex-shrink-0 mt-1"></i>
          <span style="font-size:.82rem;">${_esc(w)}</span>
        </li>`).join("")}
    </ul>
  </div>` : ""}`;

  _setModalContent("dry-modal-result", html);

  // Configura o botÃ£o "Confirmar" dentro do modal de dry-run
  const confirmBtn = document.getElementById("dry-modal-confirm");
  if (confirmBtn) {
    confirmBtn.classList.toggle("d-none", !safe);
    confirmBtn.onclick = () => {
      bsModalDryRun?.hide();
      setTimeout(() => openExecuteConfirm(activeToken), 350);
    };
  }
}

// â”€â”€ EXECUTE CONFIRM â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function openExecuteConfirm(tokenVal) {
  activeToken = tokenVal;
  const result = dryRunResults[tokenVal];

  // Preenche resumo no modal de confirmaÃ§Ã£o
  const gb     = result?.space_to_recover_gb ?? 0;
  const files  = result?.files_affected?.length ?? 0;
  const action = result?.action ?? "?";
  const path   = result?.vmdk_path ?? "?";

  document.getElementById("exec-confirm-action")?.replaceChildren(
    ...(_parseHtml(`<span class="zh-action-badge"
      style="background:${action === "DELETE" ? "rgba(248,81,73,.15)" : "rgba(210,153,34,.15)"};
             color:${action === "DELETE" ? "#f85149" : "#d29922"};
             border:1px solid ${action === "DELETE" ? "rgba(248,81,73,.4)" : "rgba(210,153,34,.4)"};">
      <i class="bi ${action === "DELETE" ? "bi-trash3-fill" : "bi-archive-fill"} me-1"></i>
      ${_esc(action)}
    </span>`))
  );
  document.getElementById("exec-confirm-path").textContent = path;
  document.getElementById("exec-confirm-gb").textContent   = window.zhFormatSizeGB ? window.zhFormatSizeGB(gb) : (+gb).toFixed(2) + " GB";
  document.getElementById("exec-confirm-files").textContent = files + " arquivo(s)";

  // Reseta campos de confirmaÃ§Ã£o
  const analystEl = document.getElementById("exec-confirm-analyst");
  const checkEl   = document.getElementById("exec-confirm-check");
  if (analystEl) analystEl.value = "";
  if (checkEl)   checkEl.checked = false;

  _clearActionErrorFeedback(document.getElementById("exec-modal-error"));
  document.getElementById("exec-btn-submit").disabled = true;

  bsModalExecute?.show();
}

async function submitExecute() {
  const tokenVal  = activeToken;
  const analystEl = document.getElementById("exec-confirm-analyst");
  const checkEl   = document.getElementById("exec-confirm-check");
  const errEl     = document.getElementById("exec-modal-error");
  const btnEl     = document.getElementById("exec-btn-submit");

  _clearActionErrorFeedback(errEl);

  if (!checkEl?.checked) {
    _setActionErrorFeedback(errEl, {
      category: "validation",
      title: "Confirmacao obrigatoria",
      message: "Marque a caixa de confirmacao para prosseguir.",
      impact: "A execucao permanece bloqueada ate a confirmacao.",
      nextStep: "Marque a confirmacao, revise o dry-run e tente novamente.",
    });
    return;
  }

  btnEl.disabled = true;
  btnEl.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Executandoâ€¦`;

  try {
    const resp = await _apiFetch(`${API_BASE}/${encodeURIComponent(tokenVal)}/execute`, {
      method:  "POST",
      headers: { Accept: "application/json", "Content-Type": "application/json" },
      body:    "{}",
    });
    const data = await resp.json();

    if (!resp.ok) {
      throw new Error(data.detail ?? `HTTP ${resp.status}`);
    }

    // Sucesso â€” fecha modal e recarrega
    bsModalExecute?.hide();
    _showToast(
      "success",
      `AÃ§Ã£o executada com sucesso! ${window.zhFormatSizeGB ? window.zhFormatSizeGB(data.space_recovered_gb ?? 0) : (data.space_recovered_gb?.toFixed(2) ?? 0) + " GB"} liberados.`
    );
    setTimeout(() => loadTab("pending"), 800);
    setTimeout(() => loadTab("executed"), 1000);
    window.refreshPendingBadge?.();

  } catch (err) {
    _setActionErrorFeedback(errEl, {
      title: "Falha ao executar acao",
      message: err?.message || "Falha nao detalhada ao executar.",
      impact: "O token permaneceu no estado anterior.",
      nextStep: "Valide permissao/conectividade e repita a execucao.",
    });
    _showToast("danger", `Falha ao executar token: ${err?.message || "erro nao detalhado"}`);
  } finally {
    btnEl.disabled = false;
    btnEl.innerHTML = `<i class="bi bi-play-fill me-2"></i>Confirmar ExecuÃ§Ã£o`;
  }
}

// â”€â”€ CANCEL â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function openCancelConfirm(tokenVal, vmdk_path) {
  activeToken = tokenVal;
  document.getElementById("cancel-modal-path").textContent = vmdk_path ?? tokenVal;
  _clearActionErrorFeedback(document.getElementById("cancel-modal-error"));
  bsModalCancel?.show();
}

async function submitCancel() {
  const tokenVal = activeToken;
  const errEl    = document.getElementById("cancel-modal-error");
  const btnEl    = document.getElementById("cancel-btn-submit");

  _clearActionErrorFeedback(errEl);
  btnEl.disabled = true;
  btnEl.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Cancelandoâ€¦`;

  try {
    const resp = await _apiFetch(`${API_BASE}/${encodeURIComponent(tokenVal)}`, {
      method:  "DELETE",
    });
    const data = await resp.json();

    if (!resp.ok) throw new Error(data.detail ?? `HTTP ${resp.status}`);

    bsModalCancel?.hide();
    _showToast("info", "Token cancelado com sucesso.");
    setTimeout(() => loadTab("pending"), 600);
    window.refreshPendingBadge?.();

  } catch (err) {
    _setActionErrorFeedback(errEl, {
      title: "Falha ao cancelar token",
      message: err?.message || "Falha nao detalhada no cancelamento.",
      impact: "O token pode permanecer ativo ate nova tentativa.",
      nextStep: "Revise a conectividade e tente cancelar novamente.",
    });
    _showToast("danger", `Falha ao cancelar token: ${err?.message || "erro nao detalhado"}`);
  } finally {
    btnEl.disabled = false;
    btnEl.innerHTML = `<i class="bi bi-x-circle me-2"></i>Confirmar Cancelamento`;
  }
}
// â”€â”€ AUDIT LOG â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function loadAuditLog() {
  const container = document.getElementById("zh-audit-table-body");
  if (!container) return;

  container.innerHTML = `<tr><td colspan="7" class="text-center py-3 text-muted-zh">
    <span class="spinner-border spinner-border-sm me-2"></span>Carregandoâ€¦</td></tr>`;

  try {
    const resp = await _apiFetch(`${API_BASE}/audit-log?limit=100`);
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const logs = await resp.json();

    if (!logs.length) {
      container.innerHTML = `<tr><td colspan="7" class="text-center py-4 text-muted-zh">
        Nenhum registro no audit log.</td></tr>`;
      return;
    }

    container.innerHTML = logs.map((log) => {
      const statusMeta = {
        "created":              { cls: "text-bg-secondary",  label: "Criado" },
        "dry_run_completed":    { cls: "text-bg-warning",    label: "Dry-run OK" },
        "executed_delete":      { cls: "text-bg-danger",     label: "Deletado" },
        "executed_quarantine":  { cls: "text-bg-warning",    label: "Quarentenado" },
        "cancelled":            { cls: "text-bg-secondary",  label: "Cancelado" },
        "invalidated":          { cls: "text-bg-secondary",  label: "Invalidado" },
        "blocked_readonly":     { cls: "text-bg-secondary",  label: "Bloqueado (RO)" },
        "blocked_no_dryrun":    { cls: "text-bg-warning",    label: "Bloqueado (sem dryrun)" },
        "blocked_expired":      { cls: "text-bg-secondary",  label: "Bloqueado (expirado)" },
        "blocked_status_changed":{ cls:"text-bg-warning",   label: "Bloqueado (mudanÃ§a)" },
        "failed":               { cls: "text-bg-danger",     label: "Falhou" },
      };
      const sm = statusMeta[log.status] ?? { cls: "text-bg-secondary", label: log.status };

      return `<tr>
        <td class="text-muted-zh" style="font-size:.75rem;white-space:nowrap;">
          ${window.zhFormatDate(log.timestamp)}
        </td>
        <td style="font-size:.8rem;font-weight:600;">${_esc(log.analyst)}</td>
        <td style="font-size:.78rem;">${_esc(log.action)}</td>
        <td>
          <code style="font-size:.72rem;word-break:break-all;color:#c9d1d9;"
            title="${_esc(log.vmdk_path)}">${_esc(_trunc(log.vmdk_path, 50))}</code>
        </td>
        <td class="text-center">
          <i class="bi bi-${log.dry_run ? "check-circle-fill text-zombie-blue" : "dash-circle text-muted-zh"}"></i>
        </td>
        <td>
          <span class="badge ${sm.cls}" style="font-size:.68rem;">${sm.label}</span>
        </td>
        <td class="text-muted-zh" style="font-size:.75rem;max-width:200px;">
          ${_esc(_trunc(log.detail ?? "â€”", 60))}
        </td>
      </tr>`;
    }).join("");

  } catch (err) {
    container.innerHTML = `<tr><td colspan="7" class="text-center text-zombie-red py-3">
      Erro: ${_esc(err.message)}</td></tr>`;
  }
}

function _bindOperationalGuide() {
  const btn = document.getElementById("zh-apv-guide-action");
  if (btn && !btn.dataset.bound) {
    btn.dataset.bound = "true";
    btn.addEventListener("click", () => {
      const action = btn.dataset.action || "focus_pending";
      if (action === "focus_pending") {
        document.getElementById("tab-btn-pending")?.click();
        return;
      }
      if (action === "focus_filters") {
        document.getElementById("zh-quick-filters")?.scrollIntoView({ behavior: "smooth", block: "start" });
        return;
      }
      if (action === "focus_audit") {
        document.getElementById("tab-btn-audit")?.click();
        return;
      }
      loadTab(activeTab);
    });
  }

  const filterForm = document.getElementById("zh-filter-form-approvals");
  if (filterForm && !filterForm.dataset.guideBound) {
    filterForm.dataset.guideBound = "true";
    filterForm.addEventListener("submit", () => setTimeout(() => _updateOperationalGuide(), 80));
  }

  const clearBtn = document.getElementById("apf-clear");
  if (clearBtn && !clearBtn.dataset.guideBound) {
    clearBtn.dataset.guideBound = "true";
    clearBtn.addEventListener("click", () => setTimeout(() => _updateOperationalGuide(), 80));
  }

  ["pending", "executed", "cancelled"].forEach((tab) => {
    const container = document.getElementById(`zh-tab-${tab}`);
    if (!container || container.dataset.guideObserved) return;
    container.dataset.guideObserved = "true";
    const obs = new MutationObserver(() => _updateOperationalGuide());
    obs.observe(container, { childList: true, subtree: false });
  });
}

function _getTabVisibility(tab = activeTab) {
  if (!tab || tab === "audit") return { visible: 0, total: 0 };
  const container = document.getElementById(`zh-tab-${tab}`);
  if (!container) return { visible: 0, total: 0 };
  const cards = container.querySelectorAll(".zh-approval-card");
  let total = 0;
  let visible = 0;
  cards.forEach((card) => {
    total += 1;
    if (card.style.display !== "none") visible += 1;
  });
  return { visible, total };
}

function _getExpiringSoonCount() {
  const now = Date.now();
  const threshold = now + (2 * 60 * 60 * 1000);
  return (_tabTokenCache.pending || []).filter((token) => {
    if (!token?.expires_at) return false;
    const status = token.status;
    if (status !== "pending_dryrun" && status !== "dryrun_done") return false;
    const ts = new Date(token.expires_at).getTime();
    return ts > now && ts <= threshold;
  }).length;
}

function _updateOperationalGuide(meta = {}) {
  const visibleEl = document.getElementById("zh-apv-guide-visible");
  const pendingEl = document.getElementById("zh-apv-guide-pending");
  const expiringEl = document.getElementById("zh-apv-guide-expiring");
  if (!visibleEl || !pendingEl || !expiringEl) return;

  const currentTab = meta.tab || activeTab;
  const pendingCount = (_tabTokenCache.pending || []).length;
  const expiringCount = _getExpiringSoonCount();
  const visibility = _getTabVisibility(currentTab);

  visibleEl.textContent = String(visibility.visible);
  pendingEl.textContent = String(pendingCount);
  expiringEl.textContent = String(expiringCount);

  if (meta.loading) {
    _setOperationalGuideState(
      "info",
      "Carregando aprovacoes da aba selecionada",
      "aguarde a sincronizacao para revisar impacto e expiracao.",
      { key: "refresh", label: "Atualizar", btnClass: "btn-outline-primary" }
    );
    return;
  }

  if (meta.error) {
    _setOperationalGuideState(
      "danger",
      "Falha ao sincronizar aprovacoes",
      "valide sessao/permissoes e tente novamente.",
      { key: "refresh", label: "Tentar novamente", btnClass: "btn-outline-danger" }
    );
    return;
  }

  if (currentTab === "audit") {
    _setOperationalGuideState(
      "info",
      "Aba de auditoria ativa",
      "retorne para pendentes para priorizar tokens antes do vencimento.",
      { key: "focus_pending", label: "Ir para pendentes", btnClass: "btn-outline-primary" }
    );
    return;
  }

  if (pendingCount === 0 && currentTab === "pending") {
    _setOperationalGuideState(
      "success",
      "Nenhuma aprovacao pendente no momento",
      "use audit log para validar a trilha recente de execucao.",
      { key: "focus_audit", label: "Abrir audit log", btnClass: "btn-outline-success" }
    );
    return;
  }

  if (expiringCount > 0) {
    _setOperationalGuideState(
      "warning",
      `${expiringCount} token(s) expira(m) em ate 2 horas`,
      "priorize dry-run e confirmacao para evitar expirar solicitacoes validas.",
      { key: "focus_pending", label: "Priorizar pendentes", btnClass: "btn-warning" }
    );
    return;
  }

  if (visibility.total > 0 && visibility.visible === 0) {
    _setOperationalGuideState(
      "info",
      "Filtros ocultaram todos os cards da aba atual",
      "limpe filtros ou ajuste criterios para voltar a visualizar tokens.",
      { key: "focus_filters", label: "Revisar filtros", btnClass: "btn-outline-primary" }
    );
    return;
  }

  if (currentTab !== "pending" && pendingCount > 0) {
    _setOperationalGuideState(
      "warning",
      `Existem ${pendingCount} aprovacao(oes) pendente(s)`,
      "retorne para pendentes antes de seguir com analise historica.",
      { key: "focus_pending", label: "Voltar para pendentes", btnClass: "btn-outline-warning" }
    );
    return;
  }

  if (visibility.total === 0) {
    _setOperationalGuideState(
      "info",
      "Nenhum card disponivel na aba selecionada",
      "atualize a tela para verificar novos eventos de aprovacao.",
      { key: "refresh", label: "Atualizar", btnClass: "btn-outline-primary" }
    );
    return;
  }

  _setOperationalGuideState(
    "success",
    `${visibility.visible} card(s) visivel(is) para revisao`,
    "siga o fluxo: dry-run, justificativa e confirmacao final.",
    { key: "focus_pending", label: "Revisar fluxo", btnClass: "btn-outline-success" }
  );
}

function _setOperationalGuideState(tone, titleText, nextStep, action = {}) {
  const card = document.getElementById("zh-approvals-guide");
  const level = document.getElementById("zh-apv-guide-level");
  const title = document.getElementById("zh-apv-guide-title");
  const next = document.getElementById("zh-apv-guide-next");
  const btn = document.getElementById("zh-apv-guide-action");
  if (!card || !level || !title || !next || !btn) return;

  card.classList.remove("is-info", "is-success", "is-warning", "is-danger");
  card.classList.add(`is-${tone}`);

  const badgeByTone = {
    info: { cls: "text-bg-info", label: "Fluxo sugerido" },
    success: { cls: "text-bg-success", label: "Operacao estavel" },
    warning: { cls: "text-bg-warning", label: "Atencao operacional" },
    danger: { cls: "text-bg-danger", label: "Falha na carga" },
  };
  const badge = badgeByTone[tone] || badgeByTone.info;

  level.className = `badge rounded-pill ${badge.cls} mb-2`;
  level.textContent = badge.label;
  title.textContent = titleText;
  next.textContent = `Proximo passo: ${nextStep}`;

  btn.dataset.action = action.key || "focus_pending";
  btn.className = `btn btn-sm ${action.btnClass || "btn-outline-primary"}`;
  btn.textContent = action.label || "Revisar pendentes";
}

// â”€â”€ Abas â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _bindTabs() {
  document.querySelectorAll("[data-zh-tab]").forEach((btn) => {
    btn.addEventListener("click", () => {
      const tab = btn.dataset.zhTab;
      activeTab = tab;
      if (tab === "audit") {
        loadAuditLog();
        _updateOperationalGuide({ tab });
      } else {
        _updateOperationalGuide({ loading: true, tab });
        loadTab(tab);
      }
    });
  });
}

function _updateTabCounter(tab, count) {
  const el = document.getElementById(`zh-tab-count-${tab}`);
  if (el) {
    el.textContent = count;
    el.classList.toggle("d-none", count === 0);
  }
}

// â”€â”€ InicializaÃ§Ã£o dos modais Bootstrap â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _initModals() {
  bsModalDryRun  = new bootstrap.Modal(document.getElementById("zh-modal-dryrun"));
  bsModalExecute = new bootstrap.Modal(document.getElementById("zh-modal-execute"));
  bsModalCancel  = new bootstrap.Modal(document.getElementById("zh-modal-cancel"));

  // DelegaÃ§Ã£o de eventos para botÃµes dinamicamente criados
  document.addEventListener("click", (e) => {
    const btnDry    = e.target.closest(".zh-btn-dryrun");
    const btnExec   = e.target.closest(".zh-btn-execute");
    const btnCancel = e.target.closest(".zh-btn-cancel");

    if (btnDry)    executeDryRun(btnDry.dataset.token);
    if (btnExec && !btnExec.disabled) openExecuteConfirm(btnExec.dataset.token);
    if (btnCancel) openCancelConfirm(btnCancel.dataset.token, btnCancel.dataset.path);
  });

  // BotÃµes de submit dentro dos modais
  document.getElementById("exec-btn-submit")
    ?.addEventListener("click", submitExecute);
  document.getElementById("cancel-btn-submit")
    ?.addEventListener("click", submitCancel);

  // Habilita o botÃ£o de executar somente quando checkbox marcado
  document.getElementById("exec-confirm-check")
    ?.addEventListener("change", function () {
      const btn = document.getElementById("exec-btn-submit");
      if (btn) btn.disabled = !this.checked;
    });
}

// â”€â”€ Toast de feedback â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _showToast(type, message) {
  if (window.zhFeedback) {
    const map = {
      success: {
        state: "success",
        title: "Operacao concluida",
        impact: "A trilha de auditoria foi atualizada.",
        nextStep: "Revise as abas para confirmar o novo estado.",
      },
      info: {
        state: "success",
        title: "Atualizacao registrada",
        impact: "A lista sera sincronizada na proxima atualizacao.",
        nextStep: "Aguarde alguns segundos ou clique em Atualizar.",
      },
      warning: {
        state: "error",
        category: "validation",
        title: "Atencao",
        impact: "A acao nao foi concluida integralmente.",
        nextStep: "Revise os dados informados e tente novamente.",
      },
      danger: {
        state: "error",
        category: "transient",
        title: "Falha na operacao",
        impact: "O token pode permanecer no estado anterior.",
        nextStep: "Verifique conectividade/permissao e tente novamente.",
      },
    };
    const cfg = map[type] || map.info;
    window.zhFeedback.showToast({
      state: cfg.state,
      category: cfg.category,
      title: cfg.title,
      happened: message,
      impact: cfg.impact,
      nextStep: cfg.nextStep,
    });
    return;
  }

  const container = document.getElementById("zh-toast-container");
  if (!container) return;

  const colors = { success: "#3fb950", danger: "#f85149", info: "#58a6ff", warning: "#d29922" };
  const icons  = { success: "check-circle-fill", danger: "x-circle-fill", info: "info-circle-fill", warning: "exclamation-triangle-fill" };

  const toast = document.createElement("div");
  toast.className = "toast align-items-center show mb-2";
  toast.style.cssText = `background:var(--zh-bg-card);border:1px solid ${colors[type] ?? "#30363d"};border-radius:8px;min-width:280px;`;
  toast.setAttribute("role", "alert");
  toast.innerHTML = `
    <div class="d-flex align-items-center gap-2 p-3">
      <i class="bi bi-${icons[type] ?? "info-circle"} flex-shrink-0" style="color:${colors[type]};font-size:1.1rem;"></i>
      <span style="font-size:.85rem;flex-grow:1;">${_esc(message)}</span>
      <button type="button" class="btn-close btn-close-white btn-sm ms-2" onclick="this.closest('.toast').remove()"></button>
    </div>`;
  container.appendChild(toast);
  setTimeout(() => toast.remove(), 6000);
}

// â”€â”€ Helpers de UI â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _setLoading(container, loading) {
  if (loading) {
    if (window.zhFeedback) {
      window.zhFeedback.setInline(container, {
        state: "loading",
        text: "Carregando aprovacoes",
        detail: "Preparando cards e contagem de expiracao.",
      });
      return;
    }
    container.innerHTML = `
      <div class="text-center py-5">
        <div class="spinner-border text-zombie-blue mb-3" style="width:2.2rem;height:2.2rem;"></div>
        <p class="text-muted-zh mb-0">Carregando aprovaÃ§Ãµesâ€¦</p>
      </div>`;
  }
}

function _setError(container, msg) {
  if (window.zhFeedback) {
    const info = window.zhFeedback.toErrorInfo({ message: String(msg || "") }, "Falha ao carregar aprovacoes.");
    container.innerHTML = window.zhFeedback.renderAlert({
      state: "error",
      category: info.category,
      title: "Falha ao carregar aprovacoes",
      happened: info.message,
      impact: "Cards podem nao refletir o estado real dos tokens.",
      nextStep: info.category === "auth"
        ? "Refaca o login e recarregue a aba."
        : "Tente novamente e valide conectividade com a API.",
    });
    return;
  }
  container.innerHTML = `
    <div class="alert alert-danger d-flex align-items-center gap-2 py-2">
      <i class="bi bi-wifi-off flex-shrink-0"></i>
      <span>Erro ao carregar: ${_esc(msg)}</span>
    </div>`;
}

function _emptyState(tab) {
  const msgs = {
    pending:   { icon: "bi-shield-check", color: "#3fb950", text: "Nenhuma aprovaÃ§Ã£o pendente.", sub: "Todas as solicitaÃ§Ãµes foram processadas." },
    executed:  { icon: "bi-check2-all",   color: "#58a6ff", text: "Nenhuma aÃ§Ã£o executada ainda.", sub: "As execuÃ§Ãµes aparecerÃ£o aqui apÃ³s confirmaÃ§Ã£o." },
    cancelled: { icon: "bi-x-circle",     color: "#6e7681", text: "Nenhum token cancelado ou expirado.", sub: "" },
  };
  const m = msgs[tab] ?? msgs.pending;
  return `
    <div class="text-center py-5">
      <i class="bi ${m.icon} mb-3" style="font-size:2.5rem;color:${m.color};"></i>
      <p class="fw-semibold mb-1">${m.text}</p>
      <small class="text-muted-zh">${m.sub}</small>
    </div>`;
}

function _typeBadge(tipo) {
  const m = ZM[tipo] ?? { label: tipo, color: "#6e7681", bg: "rgba(110,118,129,.15)", icon: "bi-question-circle" };
  return `<span class="d-inline-flex align-items-center gap-1 px-2 py-1 rounded-2"
    style="background:${m.bg};color:${m.color};font-size:.7rem;font-weight:600;">
    <i class="bi ${m.icon}"></i>${_esc(m.label)}</span>`;
}

function _statusPill(status) {
  const pills = {
    pending_dryrun: { bg: "rgba(88,166,255,.15)",  color: "#58a6ff",  label: "Aguard. dry-run" },
    dryrun_done:    { bg: "rgba(63,185,80,.15)",   color: "#3fb950",  label: "Dry-run OK" },
    executed:       { bg: "rgba(63,185,80,.15)",   color: "#3fb950",  label: "Executado" },
    cancelled:      { bg: "rgba(110,118,129,.15)", color: "#6e7681",  label: "Cancelado" },
    invalidated:    { bg: "rgba(210,153,34,.15)",  color: "#d29922",  label: "Invalidado" },
  };
  const p = pills[status] ?? { bg: "rgba(110,118,129,.15)", color: "#6e7681", label: status };
  return `<span class="px-2 py-1 rounded-2"
    style="background:${p.bg};color:${p.color};font-size:.68rem;font-weight:600;">${p.label}</span>`;
}

function _setModalContent(id, html) {
  const el = document.getElementById(id);
  if (el) el.innerHTML = html;
}

function _setActionErrorFeedback(target, opts = {}) {
  const el = typeof target === "string" ? document.querySelector(target) : target;
  if (!el) return;

  const rawMessage = String(opts.message || "Falha nao detalhada.");
  const info = window.zhFeedback
    ? window.zhFeedback.toErrorInfo({ status: opts.status, message: rawMessage }, rawMessage)
    : { category: opts.category || "unknown", message: rawMessage };

  if (window.zhFeedback) {
    window.zhFeedback.setInline(el, {
      state: "error",
      category: opts.category || info.category,
      title: opts.title || "Falha na operacao",
      happened: info.message,
      impact: opts.impact || "A acao solicitada nao foi concluida.",
      nextStep: opts.nextStep || "Revise os dados e tente novamente.",
    });
    return;
  }

  el.classList.remove("d-none");
  el.textContent = `Erro: ${info.message}`;
}

function _clearActionErrorFeedback(target) {
  const el = typeof target === "string" ? document.querySelector(target) : target;
  if (!el) return;
  if (window.zhFeedback) {
    window.zhFeedback.clear(el);
    return;
  }
  el.classList.add("d-none");
  el.textContent = "";
}

function _parseHtml(html) {
  const div = document.createElement("div");
  div.innerHTML = html;
  return [...div.childNodes];
}

function _updateTabCounter(tab, count) {
  const el = document.getElementById(`zh-tab-count-${tab}`);
  if (el) {
    el.textContent = count;
    el.classList.toggle("d-none", count === 0);
  }
}

const _esc   = (s) => String(s ?? "").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;");
const _trunc = (s, n) => s && s.length > n ? s.slice(0, n - 1) + "â€¦" : (s ?? "");
const _safeId = (s) => String(s ?? "").replace(/[^a-zA-Z0-9]/g, "_");









