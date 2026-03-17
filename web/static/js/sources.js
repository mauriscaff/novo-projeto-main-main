"use strict";

const SOURCES_API = "/api/v1/monitored-sources";

let sourcesState = [];
let sourceModal = null;
let deleteModal = null;
let deletingSourceId = null;

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

function _esc(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;");
}

function _formatDate(value) {
  if (!value) return "-";
  if (typeof window.zhFormatDate === "function") {
    return window.zhFormatDate(value);
  }
  try {
    return new Date(value).toLocaleString("pt-BR");
  } catch {
    return String(value);
  }
}

function _statusClass(status) {
  return (status || "unknown").toLowerCase();
}

function _statusLabel(status) {
  const map = {
    online: "Online",
    offline: "Offline",
    degraded: "Degradado",
    unknown: "Desconhecido",
    disabled: "Desativado",
    deleted: "Removido",
  };
  return map[(status || "unknown").toLowerCase()] || "Desconhecido";
}

function _typeLabel(type) {
  return type === "oceanstor" ? "OceanStor" : "vCenter";
}

function _selectedTypeFilter() {
  const el = document.getElementById("zh-filter-source-type");
  return (el?.value || "").trim();
}

function _setInlineFeedback(payload) {
  if (!window.zhFeedback) return;
  if (!payload) {
    window.zhFeedback.clear("#zh-sources-feedback");
    return;
  }
  window.zhFeedback.setInline("#zh-sources-feedback", payload);
}

function _renderSummaryFromStatus(statusPayload) {
  const setText = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.textContent = String(value ?? 0);
  };

  setText("zh-sum-total", statusPayload?.total ?? 0);
  setText("zh-sum-online", statusPayload?.online ?? 0);
  setText("zh-sum-offline", statusPayload?.offline ?? 0);
  setText("zh-sum-degraded", statusPayload?.degraded ?? 0);
  setText("zh-sum-unknown", statusPayload?.unknown ?? 0);
  setText("zh-sum-disabled", statusPayload?.disabled ?? 0);
}

function _renderSummaryFallback() {
  const counters = {
    total: sourcesState.length,
    online: 0,
    offline: 0,
    degraded: 0,
    unknown: 0,
    disabled: 0,
  };
  sourcesState.forEach((source) => {
    const key = _statusClass(source.status);
    if (Object.prototype.hasOwnProperty.call(counters, key)) {
      counters[key] += 1;
    } else {
      counters.unknown += 1;
    }
  });
  _renderSummaryFromStatus(counters);
}

async function loadCollectionStatus() {
  const selectedType = _selectedTypeFilter();
  const qs = selectedType ? `?source_type=${encodeURIComponent(selectedType)}` : "";

  try {
    const resp = await _apiFetch(`${SOURCES_API}/collection-status${qs}`);
    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }
    const payload = await resp.json();
    _renderSummaryFromStatus(payload);
  } catch {
    _renderSummaryFallback();
  }
}

function _renderRows() {
  const tbody = document.querySelector("#zh-sources-table tbody");
  const visible = document.getElementById("zh-sources-visible");
  if (!tbody) return;

  if (!sourcesState.length) {
    tbody.innerHTML = `
      <tr>
        <td colspan="8" class="text-center py-4 text-muted-zh">
          Nenhuma fonte monitorada cadastrada.
        </td>
      </tr>`;
    if (visible) visible.textContent = "Exibindo 0 fonte(s)";
    return;
  }

  tbody.innerHTML = sourcesState.map((source) => {
    const statusCls = _statusClass(source.status);
    const sourceType = source.source_type === "oceanstor" ? "oceanstor" : "vcenter";

    return `
      <tr>
        <td><span class="zh-source-type ${sourceType}">${_typeLabel(source.source_type)}</span></td>
        <td class="fw-semibold">${_esc(source.name)}</td>
        <td><code>${_esc(source.endpoint)}</code></td>
        <td>${_esc(source.username)}</td>
        <td>
          <span class="zh-source-status ${statusCls}">
            <i class="bi bi-circle-fill" style="font-size:.5rem"></i>${_statusLabel(source.status)}
          </span>
        </td>
        <td>${_esc(_formatDate(source.last_collected_at))}</td>
        <td>${_esc(_formatDate(source.last_connectivity_at))}</td>
        <td class="text-end">
          <div class="btn-group btn-group-sm">
            <button class="btn btn-outline-info" data-action="test" data-id="${source.id}" title="Testar conectividade">
              <i class="bi bi-plug"></i>
            </button>
            <button class="btn btn-outline-secondary" data-action="collect" data-id="${source.id}" title="Registrar coleta">
              <i class="bi bi-clock-history"></i>
            </button>
            <button class="btn btn-outline-primary" data-action="edit" data-id="${source.id}" title="Editar">
              <i class="bi bi-pencil"></i>
            </button>
            <button class="btn btn-outline-danger" data-action="delete" data-id="${source.id}" title="Remocao logica">
              <i class="bi bi-trash"></i>
            </button>
          </div>
        </td>
      </tr>`;
  }).join("");

  if (visible) visible.textContent = `Exibindo ${sourcesState.length} fonte(s)`;
}

async function loadSources(showLoader = true) {
  const selectedType = _selectedTypeFilter();
  const qs = selectedType ? `?source_type=${encodeURIComponent(selectedType)}` : "";

  if (showLoader) {
    _setInlineFeedback({
      state: "loading",
      text: "Carregando fontes monitoradas",
      detail: "Sincronizando vCenter e OceanStor cadastrados.",
    });
  }

  try {
    const resp = await _apiFetch(`${SOURCES_API}/${qs}`);
    if (!resp.ok) {
      const error = new Error(`HTTP ${resp.status}`);
      error.status = resp.status;
      throw error;
    }

    const payload = await resp.json();
    sourcesState = Array.isArray(payload) ? payload : [];
    _renderRows();
    await loadCollectionStatus();
    _setInlineFeedback(null);
  } catch (err) {
    sourcesState = [];
    _renderRows();
    _renderSummaryFallback();

    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo(err, "Falha ao carregar fontes monitoradas.")
      : { category: "unknown", message: err?.message || "Falha ao carregar fontes monitoradas." };

    _setInlineFeedback({
      state: "error",
      category: info.category,
      title: "Falha ao carregar fontes monitoradas",
      happened: info.message,
      impact: "A lista pode estar vazia ou desatualizada.",
      nextStep: info.category === "auth"
        ? "Refaca o login e tente novamente."
        : "Verifique a API e clique em Atualizar.",
    });
  }
}

function _resetForm() {
  document.getElementById("zh-source-id").value = "";
  document.getElementById("zh-source-type").value = "vcenter";
  document.getElementById("zh-source-name").value = "";
  document.getElementById("zh-source-endpoint").value = "";
  document.getElementById("zh-source-username").value = "";
  document.getElementById("zh-source-secret").value = "";
  document.getElementById("zh-source-secret").setAttribute("placeholder", "Informe o secret");
  document.getElementById("zh-source-secret").setAttribute("type", "password");
  document.getElementById("zh-source-active").checked = true;
  document.getElementById("zh-source-modal-title").textContent = "Nova fonte monitorada";
  document.getElementById("zh-source-save").innerHTML = '<i class="bi bi-floppy me-1"></i>Salvar';
  if (window.zhFeedback) {
    window.zhFeedback.clear("#zh-source-form-feedback");
  }
}

function openCreateSourceModal() {
  _resetForm();
  sourceModal?.show();
}

function openEditSourceModal(sourceId) {
  const source = sourcesState.find((item) => Number(item.id) === Number(sourceId));
  if (!source) return;

  _resetForm();
  document.getElementById("zh-source-id").value = String(source.id);
  document.getElementById("zh-source-type").value = source.source_type;
  document.getElementById("zh-source-name").value = source.name || "";
  document.getElementById("zh-source-endpoint").value = source.endpoint || "";
  document.getElementById("zh-source-username").value = source.username || "";
  document.getElementById("zh-source-secret").setAttribute("placeholder", "Preencha somente para alterar");
  document.getElementById("zh-source-active").checked = !!source.is_active;
  document.getElementById("zh-source-modal-title").textContent = `Editar fonte: ${source.name}`;
  document.getElementById("zh-source-save").innerHTML = '<i class="bi bi-floppy me-1"></i>Atualizar';
  sourceModal?.show();
}

function _collectFormPayload() {
  return {
    source_type: String(document.getElementById("zh-source-type").value || "").trim(),
    name: String(document.getElementById("zh-source-name").value || "").trim(),
    endpoint: String(document.getElementById("zh-source-endpoint").value || "").trim(),
    username: String(document.getElementById("zh-source-username").value || "").trim(),
    secret: String(document.getElementById("zh-source-secret").value || ""),
    is_active: !!document.getElementById("zh-source-active").checked,
  };
}

function _setFormFeedback(opts) {
  if (!window.zhFeedback) return;
  if (!opts) {
    window.zhFeedback.clear("#zh-source-form-feedback");
    return;
  }
  window.zhFeedback.setInline("#zh-source-form-feedback", opts);
}

async function saveSource(event) {
  event.preventDefault();
  const payload = _collectFormPayload();
  const sourceId = String(document.getElementById("zh-source-id").value || "").trim();
  const isEdit = sourceId !== "";

  if (!payload.name || !payload.endpoint || !payload.username) {
    _setFormFeedback({
      state: "error",
      category: "validation",
      title: "Campos obrigatorios",
      happened: "Nome, endpoint e usuario sao obrigatorios.",
      impact: "A fonte nao foi salva.",
      nextStep: "Preencha os campos e tente novamente.",
    });
    return;
  }

  if (!isEdit && !payload.secret) {
    _setFormFeedback({
      state: "error",
      category: "validation",
      title: "Secret obrigatorio",
      happened: "Informe o secret para cadastrar a fonte.",
      impact: "Sem credencial, a fonte nao pode ser usada.",
      nextStep: "Preencha o secret e tente novamente.",
    });
    return;
  }

  const btn = document.getElementById("zh-source-save");
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Salvando...';

  try {
    let resp;
    if (isEdit) {
      const body = {
        source_type: payload.source_type,
        name: payload.name,
        endpoint: payload.endpoint,
        username: payload.username,
        is_active: payload.is_active,
      };
      if (payload.secret) {
        body.secret = payload.secret;
      }

      resp = await _apiFetch(`${SOURCES_API}/${encodeURIComponent(sourceId)}`, {
        method: "PATCH",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(body),
      });
    } else {
      resp = await _apiFetch(`${SOURCES_API}/`, {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(payload),
      });
    }

    if (!resp.ok) {
      const errBody = await resp.json().catch(() => ({}));
      throw Object.assign(new Error(errBody.detail || `HTTP ${resp.status}`), { status: resp.status });
    }

    sourceModal?.hide();
    if (window.zhFeedback) {
      window.zhFeedback.showToast({
        state: "success",
        title: isEdit ? "Fonte atualizada" : "Fonte cadastrada",
        happened: isEdit ? "Alteracoes salvas com sucesso." : "Nova fonte monitorada cadastrada.",
        impact: "A fonte ja esta disponivel para teste e coleta.",
        nextStep: "Use o teste de conectividade para validar acesso.",
      });
    }

    await loadSources(false);
  } catch (err) {
    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo(err, "Falha ao salvar fonte monitorada.")
      : { category: "unknown", message: err?.message || "Falha ao salvar fonte monitorada." };

    _setFormFeedback({
      state: "error",
      category: info.category,
      title: "Falha ao salvar fonte monitorada",
      happened: info.message,
      impact: "Nenhuma alteracao foi persistida.",
      nextStep: info.category === "validation"
        ? "Revise os dados informados."
        : "Verifique conectividade/permissoes e tente novamente.",
    });
  } finally {
    btn.disabled = false;
    btn.innerHTML = isEdit
      ? '<i class="bi bi-floppy me-1"></i>Atualizar'
      : '<i class="bi bi-floppy me-1"></i>Salvar';
  }
}

function openDeleteSourceModal(sourceId) {
  const source = sourcesState.find((item) => Number(item.id) === Number(sourceId));
  if (!source) return;

  deletingSourceId = Number(sourceId);
  document.getElementById("zh-delete-source-name").textContent = source.name;
  deleteModal?.show();
}

async function deleteSource() {
  if (!deletingSourceId) return;
  const sourceId = deletingSourceId;
  const btn = document.getElementById("zh-source-delete-confirm");
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner-border spinner-border-sm me-1"></span>Removendo...';

  try {
    const resp = await _apiFetch(`${SOURCES_API}/${encodeURIComponent(sourceId)}`, { method: "DELETE" });
    if (!resp.ok) {
      const errBody = await resp.json().catch(() => ({}));
      throw new Error(errBody.detail || `HTTP ${resp.status}`);
    }

    deleteModal?.hide();
    deletingSourceId = null;
    if (window.zhFeedback) {
      window.zhFeedback.showToast({
        state: "success",
        title: "Fonte removida",
        happened: "Remocao logica concluida com sucesso.",
        impact: "A fonte nao aparecera na lista ativa.",
        nextStep: "Reative ou recadastre a fonte apenas se necessario.",
      });
    }
    await loadSources(false);
  } catch (err) {
    if (window.zhFeedback) {
      const info = window.zhFeedback.toErrorInfo(err, "Falha ao remover fonte monitorada.");
      window.zhFeedback.showToast({
        state: "error",
        category: info.category,
        title: "Falha ao remover fonte",
        happened: info.message,
        impact: "A fonte permanece ativa na listagem.",
        nextStep: "Tente novamente apos revisar permissao/sessao.",
      });
    }
  } finally {
    btn.disabled = false;
    btn.textContent = "Remover";
  }
}

async function testConnectivity(sourceId) {
  try {
    const resp = await _apiFetch(`${SOURCES_API}/${encodeURIComponent(sourceId)}/test-connectivity`, {
      method: "POST",
    });

    const body = await resp.json().catch(() => ({}));
    if (!resp.ok) {
      throw Object.assign(new Error(body.detail || `HTTP ${resp.status}`), { status: resp.status });
    }

    if (window.zhFeedback) {
      window.zhFeedback.showToast({
        state: body.reachable ? "success" : "error",
        category: body.reachable ? "success" : "transient",
        title: body.reachable ? "Conectividade OK" : "Conectividade indisponivel",
        happened: body.message || "Teste concluido.",
        impact: body.reachable ? "Fonte pronta para coleta." : "Coleta pode falhar para esta fonte.",
        nextStep: body.reachable ? "Siga com a operacao." : "Revise endpoint e credenciais.",
      });
    }

    await loadSources(false);
  } catch (err) {
    const info = window.zhFeedback
      ? window.zhFeedback.toErrorInfo(err, "Falha no teste de conectividade.")
      : { category: "unknown", message: err?.message || "Falha no teste de conectividade." };

    if (window.zhFeedback) {
      window.zhFeedback.showToast({
        state: "error",
        category: info.category,
        title: "Falha no teste de conectividade",
        happened: info.message,
        impact: "Status da fonte nao foi atualizado.",
        nextStep: "Verifique sessao e tente novamente.",
      });
    }
  }
}

async function markCollection(sourceId) {
  try {
    const resp = await _apiFetch(`${SOURCES_API}/${encodeURIComponent(sourceId)}/collection/mark`, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({}),
    });

    if (!resp.ok) {
      const body = await resp.json().catch(() => ({}));
      throw new Error(body.detail || `HTTP ${resp.status}`);
    }

    if (window.zhFeedback) {
      window.zhFeedback.showToast({
        state: "success",
        title: "Coleta registrada",
        happened: "Timestamp de ultima coleta atualizado.",
        impact: "Status de coleta refletira o registro mais recente.",
        nextStep: "Continue com o monitoramento das fontes.",
      });
    }

    await loadSources(false);
  } catch (err) {
    if (window.zhFeedback) {
      const info = window.zhFeedback.toErrorInfo(err, "Falha ao registrar coleta.");
      window.zhFeedback.showToast({
        state: "error",
        category: info.category,
        title: "Falha ao registrar coleta",
        happened: info.message,
        impact: "Ultima coleta permanece sem atualizacao.",
        nextStep: "Tente novamente em instantes.",
      });
    }
  }
}

function _bindTableActions() {
  const table = document.getElementById("zh-sources-table");
  if (!table) return;

  table.addEventListener("click", async (event) => {
    const btn = event.target.closest("button[data-action][data-id]");
    if (!btn) return;

    const action = btn.dataset.action;
    const sourceId = Number(btn.dataset.id);
    if (!Number.isFinite(sourceId)) return;

    if (action === "edit") {
      openEditSourceModal(sourceId);
      return;
    }
    if (action === "delete") {
      openDeleteSourceModal(sourceId);
      return;
    }
    if (action === "test") {
      await testConnectivity(sourceId);
      return;
    }
    if (action === "collect") {
      await markCollection(sourceId);
    }
  });
}

function _bindSecretToggle() {
  const input = document.getElementById("zh-source-secret");
  const btn = document.getElementById("zh-source-secret-toggle");
  if (!input || !btn) return;

  btn.addEventListener("click", () => {
    const isPassword = input.type === "password";
    input.type = isPassword ? "text" : "password";
    btn.innerHTML = `<i class="bi ${isPassword ? "bi-eye-slash" : "bi-eye"}"></i>`;
  });
}

document.addEventListener("DOMContentLoaded", async () => {
  sourceModal = new bootstrap.Modal(document.getElementById("zh-source-modal"), { keyboard: false });
  deleteModal = new bootstrap.Modal(document.getElementById("zh-source-delete-modal"), { keyboard: false });

  document.getElementById("zh-sources-new")?.addEventListener("click", openCreateSourceModal);
  document.getElementById("zh-sources-refresh")?.addEventListener("click", () => loadSources(true));
  document.getElementById("zh-filter-source-type")?.addEventListener("change", () => loadSources(true));
  document.getElementById("zh-source-form")?.addEventListener("submit", saveSource);
  document.getElementById("zh-source-delete-confirm")?.addEventListener("click", deleteSource);

  _bindTableActions();
  _bindSecretToggle();
  await loadSources(true);
});
