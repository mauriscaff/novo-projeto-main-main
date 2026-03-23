/**
 * vcenters.js â€” Gerenciamento de vCenters do ZombieHunter
 * ========================================================
 * Responsabilidades:
 *   1. Carrega e renderiza cards dos vCenters via GET /api/v1/vcenters
 *   2. Polling de conectividade via GET /api/v1/vcenters/{id}/test a cada 60s
 *   3. Cadastro de novo vCenter via POST /api/v1/vcenters
 *   4. EdiÃ§Ã£o via PATCH /api/v1/vcenters/{id}
 *   5. RemoÃ§Ã£o via DELETE /api/v1/vcenters/{id}
 *   6. Teste de conexÃ£o sob demanda
 *   7. "Testar antes de salvar" no formulÃ¡rio de cadastro/ediÃ§Ã£o
 *
 * Endpoints consumidos:
 *   GET    /api/v1/vcenters
 *   POST   /api/v1/vcenters
 *   GET    /api/v1/vcenters/{id}
 *   PATCH  /api/v1/vcenters/{id}
 *   DELETE /api/v1/vcenters/{id}
 *   POST   /api/v1/vcenters/{id}/test
 *   GET    /api/v1/vcenters/pool-status
 */

"use strict";

const VC_API = "/api/v1/vcenters";
const POLL_INTERVAL_MS = 60_000;

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

function _createModalController(modalEl) {
  if (!modalEl) {
    return { show() {}, hide() {} };
  }
  if (window.bootstrap?.Modal) {
    return new window.bootstrap.Modal(modalEl, { keyboard: false });
  }
  return {
    show() {
      modalEl.classList.add("show");
      modalEl.style.display = "block";
      modalEl.removeAttribute("aria-hidden");
      document.body.classList.add("modal-open");
    },
    hide() {
      modalEl.classList.remove("show");
      modalEl.style.display = "none";
      modalEl.setAttribute("aria-hidden", "true");
      document.body.classList.remove("modal-open");
    },
  };
}

// â”€â”€ Estado â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

let vcenters    = [];            // lista completa
let pollTimer   = null;
let editingId   = null;          // id do vCenter em ediÃ§Ã£o (null = novo)
let vcenterConnectivity = {};      // status por vCenter

// â”€â”€ Bootstrap modals (inicializados no DOMContentLoaded) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

let modalAdd   = null;
let modalDel   = null;
let deletingId = null;

// â”€â”€ InicializaÃ§Ã£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

document.addEventListener("DOMContentLoaded", async () => {
  modalAdd = _createModalController(document.getElementById("zh-modal-vcenter"));
  modalDel = _createModalController(document.getElementById("zh-modal-delete"));
  if (!window.bootstrap?.Modal) {
    console.warn("[vcenters] Bootstrap JS indisponivel; modais em modo simplificado.");
  }

  _bindFormEvents();
  _bindOperationalGuide();
  _updateOperationalGuide({ loading: true });

  await loadVcenters();
  _startPolling();
});

// â”€â”€ Carregamento â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function loadVcenters() {
  _setLoading(true);
  _updateOperationalGuide({ loading: true });
  if (window.zhFeedback) {
    window.zhFeedback.setInline("#zh-vc-feedback", {
      state: "loading",
      text: "Carregando vCenters",
      detail: "Atualizando conectividade e inventario monitorado.",
    });
  }
  try {
    const resp = await _apiFetch(VC_API);
    if (!resp.ok) {
      const err = new Error(`HTTP ${resp.status}`);
      err.status = resp.status;
      throw err;
    }
    vcenters = await resp.json();
    _renderCards();
    _setText("zh-vc-count", vcenters.length);
    window.zhFeedback?.clear("#zh-vc-feedback");
    _updateOperationalGuide();
  } catch (err) {
    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo(err, "Falha ao carregar lista de vCenters.")
      : { category: "unknown", message: err?.message || "Falha ao carregar lista de vCenters." };
    if (window.zhFeedback) {
      window.zhFeedback.setInline("#zh-vc-feedback", {
        state: "error",
        category: info.category,
        title: "Falha ao carregar vCenters",
        happened: info.message,
        impact: "A grade pode ficar vazia ou desatualizada.",
        nextStep: info.category === "auth"
          ? "Refaca o login para consultar os vCenters."
          : "Verifique rede/API e clique novamente em atualizar.",
      });
    }
    _updateOperationalGuide({ error: err });
    _showToast("danger", `Erro ao carregar vCenters: ${info.message}`);
  } finally {
    _setLoading(false);
  }
}

// â”€â”€ RenderizaÃ§Ã£o de cards â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _renderCards() {
  const grid = document.getElementById("zh-vc-grid");
  if (!grid) return;

  if (!vcenters.length) {
    grid.innerHTML = `
      <div class="col-12 text-center py-5">
        <i class="bi bi-server fs-1 text-muted-zh opacity-25"></i>
        <p class="text-muted-zh mt-3">Nenhum vCenter cadastrado.</p>
        <button class="btn btn-primary btn-sm" onclick="openAddModal()">
          <i class="bi bi-plus-circle me-1"></i>Adicionar primeiro vCenter
        </button>
      </div>`;
    vcenterConnectivity = {};
    _updateOperationalGuide();
    return;
  }

  vcenterConnectivity = {};
  vcenters.forEach((vc) => { vcenterConnectivity[vc.id] = "unknown"; });
  grid.innerHTML = vcenters.map(_cardHtml).join("");

  // Re-vincular tooltips apÃ³s render
  if (window.bootstrap?.Tooltip) {
    document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
      new window.bootstrap.Tooltip(el, { trigger: "hover" });
    });
  }
  _updateOperationalGuide();
}

function _cardHtml(vc) {
  const statusDot = `<span id="zh-dot-${vc.id}" class="zh-vc-dot zh-dot-unknown"
    title="Conectividade desconhecida â€” aguarde polling"></span>`;

  const statusLabel = `<span id="zh-status-label-${vc.id}" class="small text-muted-zh">Verificandoâ€¦</span>`;

  const lastScan = vc.last_scan_at
    ? `<span class="small text-muted-zh">${_fmtRelative(vc.last_scan_at)}</span>`
    : `<span class="small text-muted-zh">Nunca varrido</span>`;

  const zombieCount = vc.zombie_count != null
    ? `<span class="zh-badge-red" style="font-size:.75rem;">${vc.zombie_count} zombie${vc.zombie_count !== 1 ? "s" : ""}</span>`
    : `<span class="small text-muted-zh">â€”</span>`;

  const sslBadge = vc.disable_ssl_verify
    ? `<span class="zh-badge-yellow" style="font-size:.68rem;">SSL ignorado</span>`
    : `<span class="zh-badge-gray" style="font-size:.68rem;">SSL verificado</span>`;

  return `
  <div class="col-xl-4 col-lg-6 col-md-6" id="zh-vc-card-${vc.id}">
    <div class="card bg-dark-2 border-dark-3 h-100">
      <div class="card-header d-flex align-items-center justify-content-between py-2">
        <div class="d-flex align-items-center gap-2">
          ${statusDot}
          <span class="fw-semibold text-truncate" title="${_esc(vc.name)}" style="max-width:180px;">${_esc(vc.name)}</span>
          ${sslBadge}
        </div>
        <span class="text-muted-zh small">#${vc.id}</span>
      </div>

      <div class="card-body py-3 px-3">
        <!-- Host / Porta -->
        <div class="mb-2 d-flex align-items-center gap-2">
          <i class="bi bi-hdd-network text-zombie-blue opacity-75"></i>
          <span class="font-monospace small text-body-secondary">${_esc(vc.host)}:${vc.port}</span>
        </div>
        <!-- UsuÃ¡rio -->
        <div class="mb-2 d-flex align-items-center gap-2">
          <i class="bi bi-person text-muted-zh opacity-75"></i>
          <span class="small text-muted-zh">${_esc(vc.username)}</span>
        </div>
        <!-- Status de conectividade -->
        <div class="mb-2 d-flex align-items-center gap-2">
          <i class="bi bi-wifi text-muted-zh opacity-75"></i>
          ${statusLabel}
        </div>
        <!-- Ãšltima varredura -->
        <div class="mb-2 d-flex align-items-center gap-2">
          <i class="bi bi-clock-history text-muted-zh opacity-75"></i>
          ${lastScan}
        </div>
        <!-- Total de zombies -->
        <div class="d-flex align-items-center gap-2">
          <i class="bi bi-biohazard text-zombie-red opacity-75"></i>
          ${zombieCount}
        </div>
      </div>

      <div class="card-footer d-flex gap-2 flex-wrap py-2 px-3">
        <button class="btn btn-sm btn-outline-info flex-fill"
                onclick="testConnection(${vc.id})"
                id="zh-btn-test-${vc.id}">
          <i class="bi bi-plug me-1"></i>Testar
        </button>
        <button class="btn btn-sm btn-outline-secondary flex-fill"
                onclick="openEditModal(${vc.id})">
          <i class="bi bi-pencil me-1"></i>Editar
        </button>
        <button class="btn btn-sm btn-outline-danger"
                onclick="confirmDelete(${vc.id}, '${_esc(vc.name)}')">
          <i class="bi bi-trash3"></i>
        </button>
      </div>
    </div>
  </div>`;
}

// â”€â”€ Teste de conexÃ£o (card individual) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async function testConnection(id) {
  const btn    = document.getElementById(`zh-btn-test-${id}`);
  const dot    = document.getElementById(`zh-dot-${id}`);
  const label  = document.getElementById(`zh-status-label-${id}`);

  if (btn) { btn.disabled = true; btn.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Testandoâ€¦`; }
  if (dot)   dot.className = "zh-vc-dot zh-dot-unknown";
  if (label) label.textContent = "Testandoâ€¦";

  try {
    const resp = await _apiFetch(`${VC_API}/${id}/test`, {
      method:  "POST",
    });

    if (resp.ok) {
      const data = await resp.json();
      _setCardOnline(id, data);
    } else {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      _setCardOffline(id, err.detail ?? "Falha desconhecida");
    }
  } catch (e) {
    _setCardOffline(id, e.message);
  } finally {
    if (btn) { btn.disabled = false; btn.innerHTML = `<i class="bi bi-plug me-1"></i>Testar`; }
  }
}

function _setCardOnline(id, data) {
  const dot   = document.getElementById(`zh-dot-${id}`);
  const label = document.getElementById(`zh-status-label-${id}`);
  vcenterConnectivity[id] = "online";
  if (dot)   dot.className = "zh-vc-dot zh-dot-online";
  if (label) {
    const ver = data?.api_version ?? "";
    label.innerHTML = `<span class="text-success-zh fw-semibold">Online</span>`
      + (ver ? ` <span class="text-muted-zh">â€” API v${_esc(ver)}</span>` : "");
  }
  _updateOperationalGuide();
}

function _setCardOffline(id, reason) {
  const dot   = document.getElementById(`zh-dot-${id}`);
  const label = document.getElementById(`zh-status-label-${id}`);
  vcenterConnectivity[id] = "offline";
  if (dot)   dot.className = "zh-vc-dot zh-dot-offline";
  if (label) label.innerHTML =
    `<span class="text-danger-zh fw-semibold">Offline</span>`
    + ` <span class="text-muted-zh" title="${_esc(reason)}">â€” ${_esc(_trunc(reason, 40))}</span>`;
  _updateOperationalGuide();
}

// â”€â”€ Polling de status â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _startPolling() {
  pollTimer = setInterval(_pollAllVcenters, POLL_INTERVAL_MS);
  // Dispara imediatamente na inicializaÃ§Ã£o
  _pollAllVcenters();
}

async function _pollAllVcenters() {
  // Usa pool-status (endpoint Ãºnico) quando disponÃ­vel; fallback por card
  try {
    const resp = await _apiFetch(`${VC_API}/pool-status`);
    if (resp.ok) {
      const data = await resp.json();          // { "1": "online", "2": "offline", ... }
      Object.entries(data).forEach(([id, state]) => {
        if (state === "online")  _setCardOnline(Number(id), {});
        else                     _setCardOffline(Number(id), "InacessÃ­vel");
      });
      return;
    }
  } catch (_) { /* fallback abaixo */ }

  // Fallback: testa cada vCenter individualmente (silencioso)
  for (const vc of vcenters) {
    try {
      const r = await _apiFetch(`${VC_API}/${vc.id}/test`, {
        method: "POST",
      });
      if (r.ok) _setCardOnline(vc.id, await r.json());
      else      _setCardOffline(vc.id, `HTTP ${r.status}`);
    } catch (e) {
      _setCardOffline(vc.id, e.message);
    }
  }
}

function _bindOperationalGuide() {
  const btn = document.getElementById("zh-vc-guide-action");
  if (!btn || btn.dataset.bound) return;

  btn.dataset.bound = "true";
  btn.addEventListener("click", () => {
    const action = btn.dataset.action || "focus_grid";
    if (action === "open_create") {
      openAddModal();
      return;
    }
    if (action === "focus_offline") {
      const offlineDot = document.querySelector(".zh-dot-offline");
      offlineDot?.closest(".card")?.scrollIntoView({ behavior: "smooth", block: "center" });
      return;
    }
    if (action === "focus_grid") {
      document.getElementById("zh-vc-grid")?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    loadVcenters();
  });
}

function _computeConnectivityStats() {
  const total = vcenters.length;
  let online = 0;
  let offline = 0;
  let unknown = 0;

  vcenters.forEach((vc) => {
    const state = vcenterConnectivity[vc.id] || "unknown";
    if (state === "online") {
      online += 1;
    } else if (state === "offline") {
      offline += 1;
    } else {
      unknown += 1;
    }
  });

  return { total, online, offline, unknown };
}

function _updateOperationalGuide(meta = {}) {
  const totalEl = document.getElementById("zh-vc-guide-total");
  const onlineEl = document.getElementById("zh-vc-guide-online");
  const offlineEl = document.getElementById("zh-vc-guide-offline");
  if (!totalEl || !onlineEl || !offlineEl) return;

  const stats = _computeConnectivityStats();
  totalEl.textContent = String(stats.total);
  onlineEl.textContent = String(stats.online);
  offlineEl.textContent = String(stats.offline);

  if (meta.loading) {
    _setOperationalGuideState(
      "info",
      "Sincronizando inventario de vCenters",
      "aguarde o carregamento para validar conectividade antes de operar.",
      { key: "refresh", label: "Atualizar", btnClass: "btn-outline-primary" }
    );
    return;
  }

  if (meta.error) {
    _setOperationalGuideState(
      "danger",
      "Falha ao carregar status dos vCenters",
      "verifique sessao/rede e execute nova atualizacao.",
      { key: "refresh", label: "Tentar novamente", btnClass: "btn-outline-danger" }
    );
    return;
  }

  if (stats.total === 0) {
    _setOperationalGuideState(
      "info",
      "Nenhum vCenter cadastrado",
      "cadastre ao menos um endpoint para habilitar varredura monitorada.",
      { key: "open_create", label: "Cadastrar vCenter", btnClass: "btn-primary" }
    );
    return;
  }

  if (stats.offline > 0) {
    _setOperationalGuideState(
      "warning",
      `${stats.offline} vCenter(s) offline`,
      "revise conectividade/permissoes antes da proxima varredura.",
      { key: "focus_offline", label: "Ver offline", btnClass: "btn-warning" }
    );
    return;
  }

  if (stats.unknown > 0) {
    _setOperationalGuideState(
      "info",
      "Aguardando resultado de polling de conectividade",
      "aguarde a validacao automatica ou force um refresh manual.",
      { key: "refresh", label: "Atualizar", btnClass: "btn-outline-primary" }
    );
    return;
  }

  _setOperationalGuideState(
    "success",
    "Todos os vCenters estao online",
    "prossiga com varreduras mantendo monitoramento periodico ativo.",
    { key: "focus_grid", label: "Revisar grade", btnClass: "btn-outline-success" }
  );
}

function _setOperationalGuideState(tone, titleText, nextStep, action = {}) {
  const card = document.getElementById("zh-vc-guide");
  const level = document.getElementById("zh-vc-guide-level");
  const title = document.getElementById("zh-vc-guide-title");
  const next = document.getElementById("zh-vc-guide-next");
  const btn = document.getElementById("zh-vc-guide-action");
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

  btn.dataset.action = action.key || "focus_grid";
  btn.className = `btn btn-sm ${action.btnClass || "btn-outline-primary"}`;
  btn.textContent = action.label || "Ir para grade";
}

// â”€â”€ Modal de cadastro / ediÃ§Ã£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function openAddModal() {
  editingId = null;
  _resetForm();
  document.getElementById("zh-modal-vcenter-title").textContent = "Novo vCenter";
  document.getElementById("zh-field-password").placeholder      = "Senha de acesso";
  document.getElementById("zh-field-password").required         = true;
  document.getElementById("zh-pass-note").classList.add("d-none");
  modalAdd.show();
}

async function openEditModal(id) {
  editingId = id;
  const vc  = vcenters.find((v) => v.id === id);
  if (!vc) return;

  _resetForm();
  document.getElementById("zh-modal-vcenter-title").textContent = `Editar â€” ${vc.name}`;
  document.getElementById("zh-field-name").value               = vc.name;
  document.getElementById("zh-field-host").value               = vc.host;
  document.getElementById("zh-field-port").value               = vc.port;
  document.getElementById("zh-field-username").value           = vc.username;
  document.getElementById("zh-field-password").placeholder     = "(deixar vazio para manter a senha atual)";
  document.getElementById("zh-field-password").required        = false;
  document.getElementById("zh-field-ssl").checked              = vc.disable_ssl_verify;
  document.getElementById("zh-pass-note").classList.remove("d-none");

  modalAdd.show();
}

function _resetForm() {
  document.getElementById("zh-vc-form").reset();
  document.getElementById("zh-form-feedback").classList.add("d-none");
  _setTestBtnState("idle");
}

// â”€â”€ Envio do formulÃ¡rio â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _bindFormEvents() {
  document.getElementById("zh-vc-form")?.addEventListener("submit", _handleFormSubmit);
  document.getElementById("zh-btn-test-form")?.addEventListener("click", _handleTestBeforeSave);
}

async function _handleFormSubmit(e) {
  e.preventDefault();

  const btn = document.getElementById("zh-btn-save");
  btn.disabled = true;
  btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Salvandoâ€¦`;

  try {
    const payload = _collectFormPayload();
    let resp;

    if (editingId) {
      // EdiÃ§Ã£o â€” senha sÃ³ vai se preenchida
      if (!payload.password) delete payload.password;
      resp = await _apiFetch(`${VC_API}/${editingId}`, {
        method:  "PATCH",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body:    JSON.stringify(payload),
      });
    } else {
      resp = await _apiFetch(VC_API, {
        method:  "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body:    JSON.stringify(payload),
      });
    }

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      _showFormError(_extractDetail(err), {
        title: editingId ? "Falha ao atualizar vCenter" : "Falha ao cadastrar vCenter",
        impact: "Nenhuma alteracao foi persistida.",
        nextStep: "Revise os dados e valide conectividade/permissoes antes de salvar.",
      });
      return;
    }

    modalAdd.hide();
    _showToast("success", editingId ? "vCenter atualizado com sucesso." : "vCenter cadastrado com sucesso.");
    await loadVcenters();

  } catch (err) {
    _showFormError(err.message, {
      title: editingId ? "Falha ao atualizar vCenter" : "Falha ao cadastrar vCenter",
      impact: "A operacao foi interrompida antes de persistir os dados.",
      nextStep: "Verifique conectividade com a API e tente novamente.",
    });
  } finally {
    btn.disabled = false;
    btn.innerHTML = `<i class="bi bi-floppy me-1"></i>Salvar`;
  }
}

async function _handleTestBeforeSave() {
  const payload = _collectFormPayload();
  if (!payload.host || !payload.username || !payload.password) {
    _showFormError("Preencha host, usuario e senha antes de testar.", {
      category: "validation",
      title: "Dados incompletos para teste",
      impact: "O teste de conectividade nao foi iniciado.",
      nextStep: "Preencha host, usuario e senha e tente novamente.",
    });
    return;
  }

  _setTestBtnState("loading");
  _clearFormError();

  try {
    // Cadastra temporariamente? NÃ£o â€” chamamos /test com um body ad-hoc
    // Como nÃ£o hÃ¡ endpoint /test sem id, usamos POST /vcenters + DELETE
    // Strategy: POST vCenter sem nome Ãºnico â†’ usar name "zh-test-probe-<ts>"
    const probeName = `zh-test-probe-${Date.now()}`;
    const createPayload = { ...payload, name: probeName };

    const createResp = await _apiFetch(VC_API, {
      method:  "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body:    JSON.stringify(createPayload),
    });

    if (!createResp.ok) {
      const err = await createResp.json().catch(() => ({}));
      _setTestBtnState("error");
      _showFormError(`Erro ao criar probe: ${_extractDetail(err)}`, {
        title: "Falha ao preparar teste de conexao",
        impact: "Nao foi possivel validar a conectividade do vCenter.",
        nextStep: "Revise credenciais/permissoes e repita o teste.",
      });
      return;
    }

    const created = await createResp.json();
    const probeId = created.id;

    const testResp = await _apiFetch(`${VC_API}/${probeId}/test`, {
      method:  "POST",
    });

    // Limpa probe independente do resultado
    await _apiFetch(`${VC_API}/${probeId}`, { method: "DELETE" }).catch(() => {});

    if (testResp.ok) {
      const result = await testResp.json();
      _setTestBtnState("success");
      _showFormSuccess(
        `ConexÃ£o OK â€” API v${result.api_version ?? "?"} | UUID: ${result.instance_uuid ?? "?"}`
      );
    } else {
      const err = await testResp.json().catch(() => ({}));
      _setTestBtnState("error");
      _showFormError(`Falha na conexao: ${_extractDetail(err)}`, {
        title: "Falha no teste de conexao",
        impact: "A conexao com o vCenter nao foi validada.",
        nextStep: "Valide host, porta, credenciais e SSL antes de salvar.",
      });
    }

  } catch (e) {
    _setTestBtnState("error");
    _showFormError(e.message, {
      title: "Erro inesperado no teste de conexao",
      impact: "A validacao de conectividade foi interrompida.",
      nextStep: "Verifique conectividade com a API e tente novamente.",
    });
  }
}

function _collectFormPayload() {
  return {
    name:               document.getElementById("zh-field-name").value.trim(),
    host:               document.getElementById("zh-field-host").value.trim(),
    port:               parseInt(document.getElementById("zh-field-port").value) || 443,
    username:           document.getElementById("zh-field-username").value.trim(),
    password:           document.getElementById("zh-field-password").value,
    disable_ssl_verify: document.getElementById("zh-field-ssl").checked,
  };
}

function _setTestBtnState(state) {
  const btn = document.getElementById("zh-btn-test-form");
  if (!btn) return;
  const map = {
    idle:    { cls: "btn-outline-info",    ico: "bi-plug",            label: "Testar conexÃ£o", disabled: false },
    loading: { cls: "btn-outline-secondary",ico: "",                  label: "Testandoâ€¦",      disabled: true  },
    success: { cls: "btn-outline-success", ico: "bi-check-circle",    label: "ConexÃ£o OK",     disabled: false },
    error:   { cls: "btn-outline-danger",  ico: "bi-x-circle",        label: "Falhou",         disabled: false },
  };
  const s = map[state] ?? map.idle;
  btn.className = `btn btn-sm ${s.cls}`;
  btn.disabled  = s.disabled;
  btn.innerHTML = s.ico ? `<i class="bi ${s.ico} me-1"></i>${s.label}` : `<span class="spinner-border spinner-border-sm me-1"></span>${s.label}`;
}

// â”€â”€ ExclusÃ£o â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function confirmDelete(id, name) {
  deletingId = id;
  const nameEl = document.getElementById("zh-delete-name");
  if (nameEl) nameEl.textContent = name;
  modalDel.show();
}

document.addEventListener("DOMContentLoaded", () => {
  document.getElementById("zh-btn-confirm-delete")?.addEventListener("click", async () => {
    if (!deletingId) return;
    const btn = document.getElementById("zh-btn-confirm-delete");
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Removendoâ€¦`;
    try {
      const resp = await _apiFetch(`${VC_API}/${deletingId}`, { method: "DELETE" });
      if (!resp.ok && resp.status !== 200) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(_extractDetail(err));
      }
      modalDel.hide();
      _showToast("success", "vCenter removido.");
      await loadVcenters();
    } catch (e) {
      _showToast("danger", `Erro ao remover: ${e.message}`, {
        title: "Falha ao remover vCenter",
        impact: "O vCenter permanece cadastrado e pode continuar listado.",
        nextStep: "Verifique dependencias/permissoes e tente novamente.",
      });
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<i class="bi bi-trash3 me-1"></i>Confirmar remoÃ§Ã£o`;
      deletingId = null;
    }
  });
});

// â”€â”€ UI helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

function _showFormError(msg, opts = {}) {
  const el = document.getElementById("zh-form-feedback");
  if (!el) return;
  const fallbackMessage = opts.fallbackMessage || "Falha ao validar dados do vCenter.";
  if (window.zhFeedback) {
    const info = window.zhFeedback.toErrorInfo({ message: String(msg || "") }, fallbackMessage);
    el.classList.remove("d-none");
    el.innerHTML = window.zhFeedback.renderAlert({
      state: "error",
      category: opts.category || info.category,
      title: opts.title || "Falha ao salvar vCenter",
      happened: info.message,
      impact: opts.impact || "Nenhuma alteracao foi persistida.",
      nextStep: opts.nextStep || (info.category === "validation"
        ? "Revise os campos obrigatorios e tente novamente."
        : "Valide conectividade/permissoes e repita a operacao."),
    });
    return;
  }
  el.className  = "alert alert-danger py-2 mt-2 small";
  el.textContent = msg;
  el.classList.remove("d-none");
}

function _showFormSuccess(msg) {
  const el = document.getElementById("zh-form-feedback");
  if (!el) return;
  if (window.zhFeedback) {
    el.classList.remove("d-none");
    el.innerHTML = window.zhFeedback.renderAlert({
      state: "success",
      category: "success",
      title: "Validacao concluida",
      happened: msg,
      impact: "A conexao com o vCenter foi validada com sucesso.",
      nextStep: "Agora voce pode salvar o cadastro com seguranca.",
    });
    return;
  }
  el.className  = "alert alert-success py-2 mt-2 small";
  el.textContent = msg;
  el.classList.remove("d-none");
}

function _clearFormError() {
  const el = document.getElementById("zh-form-feedback");
  if (el) el.classList.add("d-none");
}

function _showToast(type, msg, opts = {}) {
  if (window.zhFeedback) {
    const map = {
      success: {
        state: "success",
        category: "success",
        title: "Operacao concluida",
        impact: "A configuracao de vCenters foi atualizada.",
        nextStep: "Valide o status de conectividade dos cards.",
      },
      danger: {
        state: "error",
        category: "transient",
        title: "Falha na operacao",
        impact: "A mudanca solicitada pode nao ter sido aplicada.",
        nextStep: "Verifique a API e tente novamente.",
      },
      warning: {
        state: "error",
        category: "validation",
        title: "Atencao",
        impact: "Os dados enviados nao passaram na validacao.",
        nextStep: "Revise campos e repita o envio.",
      },
      info: {
        state: "success",
        category: "success",
        title: "Informacao",
        impact: "A tela foi atualizada com o status mais recente.",
        nextStep: "Continue com a proxima acao desejada.",
      },
    };
    const cfg = map[type] || map.info;
    window.zhFeedback.showToast({
      state: opts.state || cfg.state,
      category: opts.category || cfg.category,
      title: opts.title || cfg.title,
      happened: msg,
      impact: opts.impact || cfg.impact,
      nextStep: opts.nextStep || cfg.nextStep,
    });
    return;
  }

  const container = document.getElementById("zh-toast-container");
  if (!container) { alert(msg); return; }
  const id = `toast-${Date.now()}`;
  const icons = { success: "bi-check-circle-fill text-success-zh", danger: "bi-x-circle-fill text-danger-zh",
                  warning: "bi-exclamation-triangle-fill text-warning" };
  const ico   = icons[type] ?? "bi-info-circle-fill text-zombie-blue";
  container.insertAdjacentHTML("beforeend", `
    <div id="${id}" class="toast align-items-center border-0 bg-dark-2 text-body show" role="alert" aria-live="assertive">
      <div class="d-flex">
        <div class="toast-body d-flex align-items-center gap-2">
          <i class="bi ${ico} fs-5"></i> ${_esc(msg)}
        </div>
        <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
      </div>
    </div>`);
  const el = document.getElementById(id);
  if (!window.bootstrap?.Toast) {
    setTimeout(() => el?.remove(), 4000);
    return;
  }
  const t  = new window.bootstrap.Toast(el, { delay: 4000 });
  t.show();
  el.addEventListener("hidden.bs.toast", () => el.remove());
}

function _setLoading(on) {
  const spinner = document.getElementById("zh-vc-spinner");
  if (spinner) spinner.classList.toggle("d-none", !on);
}

function _extractDetail(err) {
  if (!err) return "Erro desconhecido";
  if (typeof err.detail === "string") return err.detail;
  if (Array.isArray(err.detail)) return err.detail.map((e) => e.msg).join("; ");
  return JSON.stringify(err);
}

function _fmtRelative(iso) {
  if (!iso) return "â€”";
  try {
    const diff = Date.now() - new Date(iso).getTime();
    if (diff < 60_000) return "agora";
    if (diff < 3_600_000) return `hÃ¡ ${Math.floor(diff / 60_000)}min`;
    if (diff < 86_400_000) return `hÃ¡ ${Math.floor(diff / 3_600_000)}h`;
    return `hÃ¡ ${Math.floor(diff / 86_400_000)}d`;
  } catch {
    return String(iso).slice(0, 10);
  }
}

function _setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val ?? "â€”";
}

const _esc   = (s) => String(s ?? "")
  .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
  .replace(/"/g, "&quot;");
const _trunc = (s, n) => s && s.length > n ? s.slice(0, n - 1) + "â€¦" : (s ?? "");













