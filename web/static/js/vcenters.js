/**
 * vcenters.js — Gerenciamento de vCenters do ZombieHunter
 * ========================================================
 * Responsabilidades:
 *   1. Carrega e renderiza cards dos vCenters via GET /api/v1/vcenters
 *   2. Polling de conectividade via GET /api/v1/vcenters/{id}/test a cada 60s
 *   3. Cadastro de novo vCenter via POST /api/v1/vcenters
 *   4. Edição via PATCH /api/v1/vcenters/{id}
 *   5. Remoção via DELETE /api/v1/vcenters/{id}
 *   6. Teste de conexão sob demanda
 *   7. "Testar antes de salvar" no formulário de cadastro/edição
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

// ── Estado ────────────────────────────────────────────────────────────────────

let vcenters    = [];            // lista completa
let pollTimer   = null;
let editingId   = null;          // id do vCenter em edição (null = novo)

// ── Bootstrap modals (inicializados no DOMContentLoaded) ──────────────────────

let modalAdd   = null;
let modalDel   = null;
let deletingId = null;

// ── Inicialização ─────────────────────────────────────────────────────────────

document.addEventListener("DOMContentLoaded", async () => {
  modalAdd = new bootstrap.Modal(document.getElementById("zh-modal-vcenter"), { keyboard: false });
  modalDel = new bootstrap.Modal(document.getElementById("zh-modal-delete"),  { keyboard: false });

  _bindFormEvents();

  await loadVcenters();
  _startPolling();
});

// ── Carregamento ──────────────────────────────────────────────────────────────

async function loadVcenters() {
  _setLoading(true);
  try {
    const resp = await fetch(VC_API, { headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Accept": "application/json" } });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    vcenters = await resp.json();
    _renderCards();
    _setText("zh-vc-count", vcenters.length);
  } catch (err) {
    _showToast("danger", `Erro ao carregar vCenters: ${err.message}`);
  } finally {
    _setLoading(false);
  }
}

// ── Renderização de cards ─────────────────────────────────────────────────────

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
    return;
  }

  grid.innerHTML = vcenters.map(_cardHtml).join("");

  // Re-vincular tooltips após render
  document.querySelectorAll('[data-bs-toggle="tooltip"]').forEach((el) => {
    new bootstrap.Tooltip(el, { trigger: "hover" });
  });
}

function _cardHtml(vc) {
  const statusDot = `<span id="zh-dot-${vc.id}" class="zh-vc-dot zh-dot-unknown"
    title="Conectividade desconhecida — aguarde polling"></span>`;

  const statusLabel = `<span id="zh-status-label-${vc.id}" class="small text-muted-zh">Verificando…</span>`;

  const lastScan = vc.last_scan_at
    ? `<span class="small text-muted-zh">${_fmtRelative(vc.last_scan_at)}</span>`
    : `<span class="small text-muted-zh">Nunca varrido</span>`;

  const zombieCount = vc.zombie_count != null
    ? `<span class="zh-badge-red" style="font-size:.75rem;">${vc.zombie_count} zombie${vc.zombie_count !== 1 ? "s" : ""}</span>`
    : `<span class="small text-muted-zh">—</span>`;

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
        <!-- Usuário -->
        <div class="mb-2 d-flex align-items-center gap-2">
          <i class="bi bi-person text-muted-zh opacity-75"></i>
          <span class="small text-muted-zh">${_esc(vc.username)}</span>
        </div>
        <!-- Status de conectividade -->
        <div class="mb-2 d-flex align-items-center gap-2">
          <i class="bi bi-wifi text-muted-zh opacity-75"></i>
          ${statusLabel}
        </div>
        <!-- Última varredura -->
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

// ── Teste de conexão (card individual) ───────────────────────────────────────

async function testConnection(id) {
  const btn    = document.getElementById(`zh-btn-test-${id}`);
  const dot    = document.getElementById(`zh-dot-${id}`);
  const label  = document.getElementById(`zh-status-label-${id}`);

  if (btn) { btn.disabled = true; btn.innerHTML = `<span class="spinner-border spinner-border-sm"></span> Testando…`; }
  if (dot)   dot.className = "zh-vc-dot zh-dot-unknown";
  if (label) label.textContent = "Testando…";

  try {
    const resp = await fetch(`${VC_API}/${id}/test`, {
      method:  "POST",
      headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Accept": "application/json" },
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
  if (dot)   dot.className = "zh-vc-dot zh-dot-online";
  if (label) {
    const ver = data?.api_version ?? "";
    label.innerHTML = `<span class="text-success-zh fw-semibold">Online</span>`
      + (ver ? ` <span class="text-muted-zh">— API v${_esc(ver)}</span>` : "");
  }
}

function _setCardOffline(id, reason) {
  const dot   = document.getElementById(`zh-dot-${id}`);
  const label = document.getElementById(`zh-status-label-${id}`);
  if (dot)   dot.className = "zh-vc-dot zh-dot-offline";
  if (label) label.innerHTML =
    `<span class="text-danger-zh fw-semibold">Offline</span>`
    + ` <span class="text-muted-zh" title="${_esc(reason)}">— ${_esc(_trunc(reason, 40))}</span>`;
}

// ── Polling de status ─────────────────────────────────────────────────────────

function _startPolling() {
  pollTimer = setInterval(_pollAllVcenters, POLL_INTERVAL_MS);
  // Dispara imediatamente na inicialização
  _pollAllVcenters();
}

async function _pollAllVcenters() {
  // Usa pool-status (endpoint único) quando disponível; fallback por card
  try {
    const resp = await fetch(`${VC_API}/pool-status`, { headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Accept": "application/json" } });
    if (resp.ok) {
      const data = await resp.json();          // { "1": "online", "2": "offline", ... }
      Object.entries(data).forEach(([id, state]) => {
        if (state === "online")  _setCardOnline(Number(id), {});
        else                     _setCardOffline(Number(id), "Inacessível");
      });
      return;
    }
  } catch (_) { /* fallback abaixo */ }

  // Fallback: testa cada vCenter individualmente (silencioso)
  for (const vc of vcenters) {
    try {
      const r = await fetch(`${VC_API}/${vc.id}/test`, {
        method: "POST", headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Accept": "application/json" },
      });
      if (r.ok) _setCardOnline(vc.id, await r.json());
      else      _setCardOffline(vc.id, `HTTP ${r.status}`);
    } catch (e) {
      _setCardOffline(vc.id, e.message);
    }
  }
}

// ── Modal de cadastro / edição ────────────────────────────────────────────────

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
  document.getElementById("zh-modal-vcenter-title").textContent = `Editar — ${vc.name}`;
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

// ── Envio do formulário ───────────────────────────────────────────────────────

function _bindFormEvents() {
  document.getElementById("zh-vc-form")?.addEventListener("submit", _handleFormSubmit);
  document.getElementById("zh-btn-test-form")?.addEventListener("click", _handleTestBeforeSave);
}

async function _handleFormSubmit(e) {
  e.preventDefault();

  const btn = document.getElementById("zh-btn-save");
  btn.disabled = true;
  btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Salvando…`;

  try {
    const payload = _collectFormPayload();
    let resp;

    if (editingId) {
      // Edição — senha só vai se preenchida
      if (!payload.password) delete payload.password;
      resp = await fetch(`${VC_API}/${editingId}`, {
        method:  "PATCH",
        headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Content-Type": "application/json", Accept: "application/json" },
        body:    JSON.stringify(payload),
      });
    } else {
      resp = await fetch(VC_API, {
        method:  "POST",
        headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Content-Type": "application/json", Accept: "application/json" },
        body:    JSON.stringify(payload),
      });
    }

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({ detail: resp.statusText }));
      _showFormError(_extractDetail(err));
      return;
    }

    modalAdd.hide();
    _showToast("success", editingId ? "vCenter atualizado com sucesso." : "vCenter cadastrado com sucesso.");
    await loadVcenters();

  } catch (err) {
    _showFormError(err.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = `<i class="bi bi-floppy me-1"></i>Salvar`;
  }
}

async function _handleTestBeforeSave() {
  const payload = _collectFormPayload();
  if (!payload.host || !payload.username || !payload.password) {
    _showFormError("Preencha host, usuário e senha antes de testar.");
    return;
  }

  _setTestBtnState("loading");
  _clearFormError();

  try {
    // Cadastra temporariamente? Não — chamamos /test com um body ad-hoc
    // Como não há endpoint /test sem id, usamos POST /vcenters + DELETE
    // Strategy: POST vCenter sem nome único → usar name "zh-test-probe-<ts>"
    const probeName = `zh-test-probe-${Date.now()}`;
    const createPayload = { ...payload, name: probeName };

    const createResp = await fetch(VC_API, {
      method:  "POST",
      headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Content-Type": "application/json", Accept: "application/json" },
      body:    JSON.stringify(createPayload),
    });

    if (!createResp.ok) {
      const err = await createResp.json().catch(() => ({}));
      _setTestBtnState("error");
      _showFormError(`Erro ao criar probe: ${_extractDetail(err)}`);
      return;
    }

    const created = await createResp.json();
    const probeId = created.id;

    const testResp = await fetch(`${VC_API}/${probeId}/test`, {
      method:  "POST",
      headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",  "Accept": "application/json" },
    });

    // Limpa probe independente do resultado
    await fetch(`${VC_API}/${probeId}`, { method: "DELETE", headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",   } }).catch(() => {});

    if (testResp.ok) {
      const result = await testResp.json();
      _setTestBtnState("success");
      _showFormSuccess(
        `Conexão OK — API v${result.api_version ?? "?"} | UUID: ${result.instance_uuid ?? "?"}`
      );
    } else {
      const err = await testResp.json().catch(() => ({}));
      _setTestBtnState("error");
      _showFormError(`Falha na conexão: ${_extractDetail(err)}`);
    }

  } catch (e) {
    _setTestBtnState("error");
    _showFormError(e.message);
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
    idle:    { cls: "btn-outline-info",    ico: "bi-plug",            label: "Testar conexão", disabled: false },
    loading: { cls: "btn-outline-secondary",ico: "",                  label: "Testando…",      disabled: true  },
    success: { cls: "btn-outline-success", ico: "bi-check-circle",    label: "Conexão OK",     disabled: false },
    error:   { cls: "btn-outline-danger",  ico: "bi-x-circle",        label: "Falhou",         disabled: false },
  };
  const s = map[state] ?? map.idle;
  btn.className = `btn btn-sm ${s.cls}`;
  btn.disabled  = s.disabled;
  btn.innerHTML = s.ico ? `<i class="bi ${s.ico} me-1"></i>${s.label}` : `<span class="spinner-border spinner-border-sm me-1"></span>${s.label}`;
}

// ── Exclusão ──────────────────────────────────────────────────────────────────

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
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Removendo…`;
    try {
      const resp = await fetch(`${VC_API}/${deletingId}`, { method: "DELETE", headers: { "X-API-Key": window.ZH_API_KEY || "TROQUE_ESTA_API_KEY",   } });
      if (!resp.ok && resp.status !== 200) {
        const err = await resp.json().catch(() => ({}));
        throw new Error(_extractDetail(err));
      }
      modalDel.hide();
      _showToast("success", "vCenter removido.");
      await loadVcenters();
    } catch (e) {
      _showToast("danger", `Erro ao remover: ${e.message}`);
    } finally {
      btn.disabled = false;
      btn.innerHTML = `<i class="bi bi-trash3 me-1"></i>Confirmar remoção`;
      deletingId = null;
    }
  });
});

// ── UI helpers ────────────────────────────────────────────────────────────────

function _showFormError(msg) {
  const el = document.getElementById("zh-form-feedback");
  if (!el) return;
  el.className  = "alert alert-danger py-2 mt-2 small";
  el.textContent = msg;
  el.classList.remove("d-none");
}

function _showFormSuccess(msg) {
  const el = document.getElementById("zh-form-feedback");
  if (!el) return;
  el.className  = "alert alert-success py-2 mt-2 small";
  el.textContent = msg;
  el.classList.remove("d-none");
}

function _clearFormError() {
  const el = document.getElementById("zh-form-feedback");
  if (el) el.classList.add("d-none");
}

function _showToast(type, msg) {
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
  const t  = new bootstrap.Toast(el, { delay: 4000 });
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
  if (!iso) return "—";
  try {
    const diff = Date.now() - new Date(iso).getTime();
    if (diff < 60_000) return "agora";
    if (diff < 3_600_000) return `há ${Math.floor(diff / 60_000)}min`;
    if (diff < 86_400_000) return `há ${Math.floor(diff / 3_600_000)}h`;
    return `há ${Math.floor(diff / 86_400_000)}d`;
  } catch {
    return String(iso).slice(0, 10);
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
