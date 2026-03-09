"use strict";

(function () {
  const META = {
    transient: {
      title: "Falha temporaria",
      icon: "bi-wifi-off",
      border: "rgba(210, 153, 34, 0.45)",
      bg: "rgba(210, 153, 34, 0.12)",
      color: "#d29922",
    },
    auth: {
      title: "Permissao insuficiente",
      icon: "bi-shield-lock-fill",
      border: "rgba(248, 81, 73, 0.45)",
      bg: "rgba(248, 81, 73, 0.12)",
      color: "#f85149",
    },
    validation: {
      title: "Dados invalidos",
      icon: "bi-exclamation-circle-fill",
      border: "rgba(88, 166, 255, 0.45)",
      bg: "rgba(88, 166, 255, 0.12)",
      color: "#58a6ff",
    },
    success: {
      title: "Operacao concluida",
      icon: "bi-check-circle-fill",
      border: "rgba(63, 185, 80, 0.45)",
      bg: "rgba(63, 185, 80, 0.12)",
      color: "#3fb950",
    },
    unknown: {
      title: "Falha nao classificada",
      icon: "bi-exclamation-triangle-fill",
      border: "rgba(248, 81, 73, 0.35)",
      bg: "rgba(248, 81, 73, 0.1)",
      color: "#f85149",
    },
  };

  function _esc(value) {
    return String(value ?? "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;")
      .replace(/'/g, "&#039;");
  }

  function _toElement(target) {
    if (!target) return null;
    if (typeof target === "string") return document.querySelector(target);
    return target;
  }

  function _parseStatus(value) {
    if (value == null) return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  function _statusFromMessage(msg) {
    const found = String(msg || "").match(/\bHTTP\s*(\d{3})\b/i);
    if (!found) return null;
    const parsed = Number(found[1]);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function _errorMessage(input, fallback) {
    if (typeof input === "string") return input || fallback;
    if (input instanceof Error) return input.message || fallback;
    if (input && typeof input.message === "string") return input.message || fallback;
    return fallback;
  }

  function classifyError(input) {
    const msg = _errorMessage(input, "Erro nao detalhado").toLowerCase();
    const status = _parseStatus(input?.status) ?? _statusFromMessage(msg);

    if (
      status === 401 || status === 403 ||
      /unauthor|forbidden|nao autenticad|nao autorizado|sessao|login|permissa/.test(msg)
    ) {
      return "auth";
    }

    if (
      status === 400 || status === 409 || status === 422 ||
      /validation|validac|invalido|obrigator|conflict|conflito|campo/.test(msg)
    ) {
      return "validation";
    }

    if (
      status === 408 || status === 429 || (status != null && status >= 500) ||
      /timeout|network|failed to fetch|temporar|indispon|conexao/.test(msg)
    ) {
      return "transient";
    }

    return "unknown";
  }

  function toErrorInfo(input, fallbackMessage) {
    const message = _errorMessage(input, fallbackMessage || "Erro nao detalhado");
    const status = _parseStatus(input?.status) ?? _statusFromMessage(message);
    const category = classifyError({ status, message });
    return { status, message, category };
  }

  function renderLoading(opts = {}) {
    const text = _esc(opts.text || "Carregando dados");
    const detail = _esc(opts.detail || "Aguarde alguns segundos.");
    return `
      <div class="zh-feedback zh-feedback--loading" role="status" aria-live="polite">
        <div class="zh-feedback__title">${text}</div>
        <div class="zh-feedback__meta">${detail}</div>
        <div class="zh-feedback-skeleton mt-2">
          <div class="zh-feedback-skeleton__line w-100"></div>
          <div class="zh-feedback-skeleton__line w-75"></div>
          <div class="zh-feedback-skeleton__line w-50"></div>
        </div>
      </div>
    `;
  }

  function renderAlert(opts = {}) {
    const state = opts.state || "error";
    const category = state === "success" ? "success" : (opts.category || "unknown");
    const meta = META[category] || META.unknown;
    const title = _esc(opts.title || meta.title);
    const happened = _esc(opts.happened || "Nao foi possivel concluir a operacao.");
    const impact = _esc(opts.impact || "Os dados desta tela podem estar incompletos.");
    const nextStep = _esc(opts.nextStep || "Tente novamente em instantes.");
    const role = state === "success" ? "status" : "alert";

    return `
      <div class="zh-feedback zh-feedback--alert" role="${role}" aria-live="polite"
        style="border-color:${meta.border};background:${meta.bg};">
        <div class="d-flex align-items-start gap-3">
          <i class="bi ${meta.icon} flex-shrink-0 zh-feedback__icon" style="color:${meta.color};"></i>
          <div class="flex-grow-1">
            <div class="zh-feedback__title" style="color:${meta.color};">${title}</div>
            <div class="zh-feedback__line"><strong>O que aconteceu:</strong> ${happened}</div>
            <div class="zh-feedback__line"><strong>Impacto:</strong> ${impact}</div>
            <div class="zh-feedback__line"><strong>Proximo passo:</strong> ${nextStep}</div>
          </div>
        </div>
      </div>
    `;
  }

  function setInline(target, opts) {
    const el = _toElement(target);
    if (!el) return;
    if (!opts || opts.state === "clear") {
      el.innerHTML = "";
      el.classList.add("d-none");
      return;
    }

    let html = "";
    if (opts.state === "loading") {
      html = renderLoading(opts);
    } else {
      html = renderAlert(opts);
    }
    el.innerHTML = html;
    el.classList.remove("d-none");
  }

  function _ensureToastContainer() {
    let container = document.getElementById("zh-feedback-toast-container");
    if (container) return container;

    container = document.createElement("div");
    container.id = "zh-feedback-toast-container";
    container.className = "toast-container position-fixed bottom-0 end-0 p-3";
    container.style.zIndex = "1150";
    document.body.appendChild(container);
    return container;
  }

  function showToast(opts = {}) {
    const container = _ensureToastContainer();
    const explicitCategory = typeof opts.category === "string" ? opts.category : null;
    const info = opts.state === "success"
      ? { category: "success", message: opts.happened || "Operacao concluida com sucesso." }
      : explicitCategory
        ? { category: explicitCategory, message: opts.happened || opts.message || "Falha nao detalhada" }
        : toErrorInfo({ status: opts.status, message: opts.happened || opts.message || "Falha nao detalhada" });
    const meta = META[info.category] || META.unknown;
    const impact = _esc(opts.impact || "");
    const nextStep = _esc(opts.nextStep || "");
    const happened = _esc(opts.happened || info.message);
    const id = `zh-toast-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;

    const toast = document.createElement("div");
    toast.id = id;
    toast.className = "toast align-items-start border-0 zh-feedback-toast";
    toast.setAttribute("role", "alert");
    toast.setAttribute("aria-live", "assertive");
    toast.setAttribute("aria-atomic", "true");
    toast.innerHTML = `
      <div class="d-flex">
        <div class="toast-body">
          <div class="d-flex align-items-start gap-2">
            <i class="bi ${meta.icon} mt-1" style="color:${meta.color};"></i>
            <div>
              <div class="fw-semibold">${_esc(opts.title || meta.title)}</div>
              <div>${happened}</div>
              ${impact ? `<div class="small text-muted mt-1"><strong>Impacto:</strong> ${impact}</div>` : ""}
              ${nextStep ? `<div class="small text-muted"><strong>Proximo passo:</strong> ${nextStep}</div>` : ""}
            </div>
          </div>
        </div>
        <button type="button" class="btn-close btn-close-white me-2 mt-2" data-bs-dismiss="toast" aria-label="Fechar"></button>
      </div>
    `;
    container.appendChild(toast);

    if (window.bootstrap?.Toast) {
      const bsToast = new bootstrap.Toast(toast, { delay: opts.delayMs || 7000 });
      bsToast.show();
      toast.addEventListener("hidden.bs.toast", () => toast.remove());
    } else {
      toast.classList.add("show");
      setTimeout(() => toast.remove(), opts.delayMs || 7000);
    }
  }

  window.zhFeedback = {
    classifyError,
    toErrorInfo,
    renderLoading,
    renderAlert,
    setInline,
    clear: (target) => setInline(target, { state: "clear" }),
    showToast,
  };
})();
