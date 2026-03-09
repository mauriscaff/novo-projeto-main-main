from contextlib import asynccontextmanager
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from fastapi import Depends, FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy import select, text

from app.api.routes import approvals, auth, dashboard, datastore_reports, scan, scanner, schedules, vcenter, webhooks
from app.core.scheduler import scheduler, start as scheduler_start, stop as scheduler_stop
from app.core.vcenter.connection_manager import connection_manager
from app.dependencies import get_current_user
from app.models.base import AsyncSessionLocal, init_db
from app.models.audit_log import ApprovalToken, TERMINAL_STATUSES
from app.models.vcenter import VCenter
from config import get_settings

# ── Jinja2 + arquivos estáticos ───────────────────────────────────────────────
_WEB_DIR = Path(__file__).parent / "web"
templates = Jinja2Templates(directory=str(_WEB_DIR / "templates"))

settings = get_settings()
logger = logging.getLogger(__name__)
_WEB_AUTH_COOKIE_NAME = "zh_api_session"
_WEB_AUTH_COOKIE_MAX_AGE_SEC = 8 * 60 * 60


def _attach_web_auth_cookie(request: Request, response: HTMLResponse) -> None:
    """
    Injeta API key em cookie HttpOnly para o frontend web consumir a API sem
    expor segredo em variavel JavaScript.
    """
    key = (settings.api_key or "").strip()
    defaults = {"", "change-me-in-production", "TROQUE_ESTA_API_KEY", "YOUR_VALUE_HERE"}
    if key in defaults:
        return

    response.set_cookie(
        key=_WEB_AUTH_COOKIE_NAME,
        value=key,
        max_age=_WEB_AUTH_COOKIE_MAX_AGE_SEC,
        httponly=True,
        secure=(request.url.scheme == "https"),
        samesite="strict",
        path="/",
    )


def _render_web_template(request: Request, template_name: str, ctx: dict) -> HTMLResponse:
    response = templates.TemplateResponse(template_name, ctx)
    _attach_web_auth_cookie(request, response)
    return response


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ── Startup ──────────────────────────────────────────────────────────
    await init_db()
    await _register_existing_vcenters()
    if settings.scheduler_enabled:
        await scheduler_start()      # inicia APScheduler + recarrega schedules do banco
    else:
        logger.info("APScheduler desabilitado por configuracao (SCHEDULER_ENABLED=false).")
    yield
    # ── Shutdown ─────────────────────────────────────────────────────────
    if settings.scheduler_enabled:
        scheduler_stop()             # para o APScheduler graciosamente
    connection_manager.disconnect_all()


async def _register_existing_vcenters() -> None:
    """
    Ao iniciar, carrega todos os vCenters ativos do banco e os registra no pool.
    As conexões são lazy — o pool conecta apenas no primeiro uso.
    """
    async with AsyncSessionLocal() as db:
        result = await db.execute(
            select(VCenter).where(VCenter.is_active.is_(True))
        )
        vcenters = result.scalars().all()

    for vc in vcenters:
        try:
            connection_manager.register(vc)
            logger.info("vCenter '%s' (%s) registrado no pool.", vc.name, vc.host)
        except Exception as exc:
            logger.warning(
                "Não foi possível registrar vCenter '%s' no pool: %s", vc.name, exc
            )

    logger.info("%d vCenter(s) registrado(s) no pool de conexões.", len(vcenters))


app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description=(
        "API REST para varredura de VMDKs zombie/orphaned "
        "em múltiplos vCenters VMware.\n\n"
        "**Autenticação:** Bearer JWT (`POST /api/v1/auth/token`) "
        "ou header `X-API-Key`."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
    lifespan=lifespan,
)

# CORS: origens permitidas (CORS_ALLOWED_ORIGINS, separado por vírgula)
# Fallback seguro: apenas localhost:8000 quando variável não definida
_cors_origins_raw = os.getenv("CORS_ALLOWED_ORIGINS", "http://localhost:8000")
_cors_origins = [x.strip() for x in _cors_origins_raw.split(",") if x.strip()] or ["http://localhost:8000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth.router,      prefix="/api/v1/auth",       tags=["Autenticação"])
app.include_router(vcenter.router,   prefix="/api/v1/vcenters",   tags=["vCenters"])
app.include_router(scan.router,      prefix="/api/v1/scans",      tags=["Varredura VMDK (legado)"])
app.include_router(scanner.router,   prefix="/api/v1/scan",       tags=["Varredura Zombie"])
app.include_router(datastore_reports.router, prefix="/api/v1/datastore-reports", tags=["Datastore Reports"])
app.include_router(schedules.router, prefix="/api/v1/schedules",  tags=["Agendamentos"])
app.include_router(webhooks.router,   prefix="/api/v1/webhooks",   tags=["Webhooks"])
app.include_router(dashboard.router,  prefix="/api/v1/dashboard",  tags=["Dashboard"])
app.include_router(approvals.router,  prefix="/api/v1/approvals",  tags=["Aprovações & Auditoria"])

# ── Arquivos estáticos (CSS, JS) ──────────────────────────────────────────────
app.mount("/static", StaticFiles(directory=str(_WEB_DIR / "static")), name="static")


# ─────────────────────────────────────────────────────────────────────────────
# Helper: contexto Jinja2 base (variáveis compartilhadas entre todos os templates)
# ─────────────────────────────────────────────────────────────────────────────

async def _base_ctx(request: Request) -> dict:
    """
    Monta o dicionário de contexto comum a todas as páginas web.
    Inclui status dos vCenters, modo readonly, contagem de aprovações pendentes.
    """
    # Status de conectividade dos vCenters registrados no pool
    pool = connection_manager.pool_status()
    vcenter_status = [
        {"name": name, "connected": info.get("connected", False)}
        for name, info in pool.items()
    ]

    # Contagem de tokens de aprovação pendentes
    pending = 0
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(
                select(ApprovalToken).where(
                    ApprovalToken.status.notin_(TERMINAL_STATUSES),
                    ApprovalToken.expires_at > datetime.now(timezone.utc),
                )
            )
            pending = len(result.scalars().all())
    except Exception as exc:
        logger.warning("Falha ao carregar contagem de aprovacoes pendentes: %s", exc, exc_info=True)

    return {
        "request":          request,
        "readonly_mode":    settings.readonly_mode,
        "vcenter_status":   vcenter_status,
        "api_version":      settings.app_version,
        "last_scan_at":     None,   # preenchido por cada rota que precise
        "pending_approvals": pending,
        "flash_messages":   [],     # lista de (category, message) — sem Flask
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rotas web (HTML)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_dashboard(request: Request):
    ctx = await _base_ctx(request)
    return _render_web_template(request, "dashboard.html", ctx)


@app.get("/dashboard", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_dashboard_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/")


@app.get("/scan/results", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_scan_results(request: Request):
    ctx = await _base_ctx(request)
    ctx["job_id"] = None
    return _render_web_template(request, "scan_results.html", ctx)


@app.get("/scan/results/{job_id}", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_scan_results_job(request: Request, job_id: str):
    ctx = await _base_ctx(request)
    ctx["job_id"] = job_id
    return _render_web_template(request, "scan_results.html", ctx)


@app.get("/operations/post-exclusion-report", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_post_exclusion_report(request: Request):
    ctx = await _base_ctx(request)
    return _render_web_template(request, "post_exclusion_report.html", ctx)


@app.get("/approvals", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_approvals(request: Request):
    ctx = await _base_ctx(request)
    return _render_web_template(request, "approvals.html", ctx)


@app.get("/audit-log", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_audit_log(request: Request):
    ctx = await _base_ctx(request)
    return _render_web_template(request, "audit.html", ctx)


@app.get("/vcenters", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_vcenters(request: Request):
    ctx = await _base_ctx(request)
    return _render_web_template(request, "vcenters.html", ctx)


@app.get("/whitelist", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_whitelist(request: Request):
    """Redireciona para a página de resultados com filtro de whitelist."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/scan/results?status=WHITELIST")


@app.get("/settings", response_class=HTMLResponse, tags=["Web"], include_in_schema=False)
async def web_settings(request: Request):
    """Página de configurações — redireciona para vcenters por enquanto."""
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/vcenters")


@app.get("/health", tags=["Health"])
async def health_check() -> dict:
    return {
        "status": "ok",
        "service": settings.app_name,
        "version": settings.app_version,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


async def _build_readiness_report() -> dict:
    db_status = "ok"
    db_detail: str | None = None
    try:
        async with AsyncSessionLocal() as db:
            await db.execute(text("SELECT 1"))
    except Exception as exc:
        db_status = "error"
        db_detail = str(exc)
        logger.error("Readiness: falha no banco de dados: %s", exc)

    aps_jobs = scheduler.get_jobs() if scheduler.running else []
    scheduler_info = {
        "enabled": settings.scheduler_enabled,
        "running": scheduler.running,
        "jobs_count": len(aps_jobs),
        "jobs": [
            {
                "id": j.id,
                "name": j.name,
                "next_run_at": j.next_run_time.isoformat() if j.next_run_time else None,
            }
            for j in aps_jobs
        ],
    }

    pool = connection_manager.pool_status()
    connected_count = sum(1 for s in pool.values() if s.get("connected", False))
    overall = "ok" if db_status == "ok" else "degraded"

    response: dict = {
        "status": overall,
        "version": settings.app_version,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "database": {"status": db_status},
        "scheduler": scheduler_info,
        "vcenters": {
            "total": len(pool),
            "connected": connected_count,
            "pool": pool,
        },
    }
    if db_detail:
        response["database"]["detail"] = db_detail

    return response


@app.get("/health/readiness", tags=["Health"])
async def readiness_check(_: dict = Depends(get_current_user)) -> dict:
    return await _build_readiness_report()
