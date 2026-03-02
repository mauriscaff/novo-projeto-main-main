"""
web/app.py — Aplicação Flask standalone do ZombieHunter
========================================================

Esta aplicação Flask é INDEPENDENTE do backend FastAPI (main.py).
Acessa diretamente o mesmo banco SQLite e inicia o scheduler APScheduler
ao ser executada.

Uso:
    cd "c:\\Users\\mscaff\\novo projeto"
    python web/app.py

    # Ou com Gunicorn em produção:
    gunicorn -w 4 -b 0.0.0.0:5000 "web.app:app"

Variáveis de ambiente relevantes (.env):
    FLASK_PORT      porta HTTP (padrão: 5000)
    READONLY_MODE   true|false (padrão: true — nunca altere o padrão)
    API_KEY         chave para autenticação nas rotas /api/
"""

from __future__ import annotations

# ── sys.path: garante que "app.*" e "config" sejam importáveis ────────────────
import sys
import os
from pathlib import Path

_WEB_DIR      = Path(__file__).parent          # web/
_PROJECT_ROOT = _WEB_DIR.parent                # raiz do projeto

# Insere raiz do projeto ANTES de web/ para que "app.*" (pacote)
# tenha prioridade sobre "app.py" (módulo Flask).
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
# web/ fica no FINAL — permite "from scheduler.cron_runner import …"
# sem sobrepor o pacote app/ da raiz do projeto.
if str(_WEB_DIR) not in sys.path:
    sys.path.append(str(_WEB_DIR))

# ── Imports padrão ────────────────────────────────────────────────────────────
import csv
import io
import json
import logging
import secrets
import ssl
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

# ── Flask ─────────────────────────────────────────────────────────────────────
from flask import (
    Flask, Response, g, jsonify, redirect,
    render_template, request, url_for as _flask_url_for,
)

# ── SQLAlchemy (sync) ─────────────────────────────────────────────────────────
from sqlalchemy import and_, create_engine, func, or_, select, text
from sqlalchemy.orm import Session, sessionmaker

# ── Modelos do projeto (importados após sys.path estar configurado) ────────────
from app.models.audit_log   import ApprovalToken, AuditLog, TERMINAL_STATUSES
from app.models.vcenter     import VCenter
from app.models.vmdk_whitelist import VmdkWhitelist
from app.models.webhook     import WebhookEndpoint
from app.models.zombie_scan import ZombieScanJob, ZombieVmdkRecord
from config import get_settings

# ─────────────────────────────────────────────────────────────────────────────
# Configurações
# ─────────────────────────────────────────────────────────────────────────────

settings = get_settings()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("zombiehunter.web")

# ─────────────────────────────────────────────────────────────────────────────
# Flask App
# ─────────────────────────────────────────────────────────────────────────────

app = Flask(
    __name__,
    template_folder = str(_WEB_DIR / "templates"),
    static_folder   = str(_WEB_DIR / "static"),
    static_url_path = "/static",
)
app.config["SECRET_KEY"]        = settings.secret_key
app.config["JSON_ENSURE_ASCII"] = False


# ── Jinja2: compatibilidade com templates FastAPI ─────────────────────────────
# Os templates foram criados para FastAPI e usam url_for('static', path='...')
# Flask usa url_for('static', filename='...'). Este wrapper aceita ambos.

def _compat_url_for(endpoint: str, **kwargs) -> str:
    if endpoint == "static" and "path" in kwargs and "filename" not in kwargs:
        kwargs["filename"] = kwargs.pop("path")
    return _flask_url_for(endpoint, **kwargs)

app.jinja_env.globals["url_for"] = _compat_url_for


# ── CORS ──────────────────────────────────────────────────────────────────────

@app.after_request
def _add_cors(response: Response) -> Response:
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = (
        "Content-Type, Authorization, X-API-Key, X-Approval-Token"
    )
    response.headers["Access-Control-Allow-Methods"] = (
        "GET, POST, PATCH, PUT, DELETE, OPTIONS"
    )
    return response


@app.route("/api/v1/<path:_>", methods=["OPTIONS"])
@app.route("/api/<path:_>",    methods=["OPTIONS"])
def _preflight(_: str) -> Response:
    return Response(status=204)


# ─────────────────────────────────────────────────────────────────────────────
# Banco de dados (SQLAlchemy síncrono)
# ─────────────────────────────────────────────────────────────────────────────

_SYNC_DB_URL = settings.database_url.replace("+aiosqlite", "")
_engine      = create_engine(
    _SYNC_DB_URL,
    connect_args={"check_same_thread": False},
    echo=settings.debug,
)
_SyncSession = sessionmaker(bind=_engine, autocommit=False, autoflush=False)


@contextmanager
def _db():
    """Context manager com commit/rollback automáticos."""
    session: Session = _SyncSession()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers de resposta e utilitários
# ─────────────────────────────────────────────────────────────────────────────

def _ok(data: Any, code: int = 200) -> tuple[Response, int]:
    return jsonify(data), code


def _err(msg: str, code: int = 400) -> tuple[Response, int]:
    return jsonify({"detail": msg}), code


def _dt_iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# ─────────────────────────────────────────────────────────────────────────────
# Autenticação simplificada (API Key)
# ─────────────────────────────────────────────────────────────────────────────

def _is_authenticated() -> bool:
    """
    Verifica API Key enviada via X-API-Key ou Authorization: Bearer.
    Se nenhuma chave estiver configurada (valor padrão), permite tudo.
    """
    key = settings.api_key
    _defaults = {"change-me-in-production", "TROQUE_ESTA_API_KEY", ""}
    if key in _defaults:
        return True
    candidates = [
        request.headers.get("X-API-Key", ""),
        request.headers.get("Authorization", "").removeprefix("Bearer ").strip(),
    ]
    return key in candidates


# ─────────────────────────────────────────────────────────────────────────────
# Contexto base dos templates Jinja2
# ─────────────────────────────────────────────────────────────────────────────

def _base_ctx() -> dict[str, Any]:
    """Variáveis compartilhadas por todas as páginas web."""
    pending      = 0
    last_scan_at = None
    vc_status    = []
    try:
        with _db() as db:
            now = _now_utc()

            # Aprovações pendentes ativas
            pending = db.execute(
                select(func.count()).select_from(ApprovalToken).where(
                    ApprovalToken.status.notin_(TERMINAL_STATUSES),
                    ApprovalToken.expires_at > now,
                )
            ).scalar_one() or 0

            # Última varredura concluída
            last_scan_at = db.execute(
                select(ZombieScanJob.finished_at)
                .where(ZombieScanJob.status == "completed")
                .order_by(ZombieScanJob.finished_at.desc())
                .limit(1)
            ).scalar_one_or_none()

            # Lista de vCenters para dots de status no navbar
            vcs = db.execute(
                select(VCenter.name).where(VCenter.is_active.is_(True))
            ).scalars().all()
            vc_status = [{"name": v, "connected": False} for v in vcs]

    except Exception as exc:
        logger.warning("_base_ctx error: %s", exc)

    return {
        "request":           request,              # necessário para url_for nos templates
        "readonly_mode":     settings.readonly_mode,
        "vcenter_status":    vc_status,
        "api_version":       settings.app_version,
        "last_scan_at":      last_scan_at,
        "pending_approvals": pending,
        "flash_messages":    [],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Rotas Web (HTML)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/")
def web_dashboard():
    return render_template("dashboard.html", **_base_ctx())


@app.get("/results")
@app.get("/scan/results")
def web_scan_results():
    ctx          = _base_ctx()
    ctx["job_id"] = request.args.get("job_id")
    return render_template("scan_results.html", **ctx)


@app.get("/scan/results/<job_id>")
def web_scan_results_job(job_id: str):
    ctx          = _base_ctx()
    ctx["job_id"] = job_id
    return render_template("scan_results.html", **ctx)


@app.get("/approvals")
def web_approvals():
    return render_template("approvals.html", **_base_ctx())


@app.get("/vcenters")
def web_vcenters():
    return render_template("vcenters.html", **_base_ctx())


@app.get("/audit-log")
@app.get("/audit")
def web_audit():
    return render_template("audit.html", **_base_ctx())


# ─────────────────────────────────────────────────────────────────────────────
# API — /health
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health")
@app.get("/api/health")
@app.get("/api/v1/health")
def api_health():
    db_ok     = True
    db_detail = None
    try:
        with _db() as db:
            db.execute(text("SELECT 1"))
    except Exception as e:
        db_ok     = False
        db_detail = str(e)

    try:
        from scheduler.cron_runner import scheduler as _sch
        sched = {"running": _sch.running, "jobs_count": len(_sch.get_jobs())}
    except Exception:
        sched = {"running": False, "jobs_count": 0}

    payload: dict = {
        "status":       "ok" if db_ok else "degraded",
        "version":      settings.app_version,
        "readonly_mode": settings.readonly_mode,
        "timestamp":    _now_utc().isoformat(),
        "database":     {"status": "ok" if db_ok else "error"},
        "scheduler":    sched,
    }
    if db_detail:
        payload["database"]["detail"] = db_detail

    return _ok(payload)


# ─────────────────────────────────────────────────────────────────────────────
# API — Dashboard
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/dashboard")
@app.get("/api/v1/dashboard")
def api_dashboard():
    with _db() as db:
        # Totalizadores históricos
        row = db.execute(
            select(
                func.count(ZombieVmdkRecord.id),
                func.coalesce(func.sum(ZombieVmdkRecord.tamanho_gb), 0.0),
            )
        ).one()
        total_zombies = row[0]
        total_gb      = round(float(row[1]), 3)

        # Aprovações pendentes ativas
        pending = db.execute(
            select(func.count()).select_from(ApprovalToken).where(
                ApprovalToken.status.notin_(TERMINAL_STATUSES),
                ApprovalToken.expires_at > _now_utc(),
            )
        ).scalar_one() or 0

        # Última varredura
        last_scan = db.execute(
            select(ZombieScanJob.finished_at)
            .where(ZombieScanJob.status == "completed")
            .order_by(ZombieScanJob.finished_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        # Contagem de vCenters
        vc_count = db.execute(
            select(func.count()).select_from(VCenter).where(VCenter.is_active.is_(True))
        ).scalar_one() or 0

        # Breakdown por tipo de zombie
        type_rows = db.execute(
            select(
                ZombieVmdkRecord.tipo_zombie,
                func.count(ZombieVmdkRecord.id),
                func.coalesce(func.sum(ZombieVmdkRecord.tamanho_gb), 0.0),
            ).group_by(ZombieVmdkRecord.tipo_zombie)
        ).all()
        by_type = [
            {"tipo_zombie": r[0], "count": r[1], "size_gb": round(float(r[2]), 3)}
            for r in type_rows
        ]

        # Top 5 vCenters por GB recuperável
        vc_rows = db.execute(
            select(
                func.coalesce(ZombieVmdkRecord.vcenter_name, ZombieVmdkRecord.vcenter_host),
                func.coalesce(func.sum(ZombieVmdkRecord.tamanho_gb), 0.0),
            )
            .group_by(func.coalesce(ZombieVmdkRecord.vcenter_name, ZombieVmdkRecord.vcenter_host))
            .order_by(func.sum(ZombieVmdkRecord.tamanho_gb).desc())
            .limit(5)
        ).all()
        top_vcenters = [
            {"name": r[0] or "N/A", "size_gb": round(float(r[1]), 3)}
            for r in vc_rows
        ]

        # Tendência das últimas 10 varreduras
        trend_rows = db.execute(
            select(
                ZombieScanJob.job_id,
                ZombieScanJob.finished_at,
                ZombieScanJob.total_size_gb,
            )
            .where(ZombieScanJob.status == "completed")
            .order_by(ZombieScanJob.finished_at.desc())
            .limit(10)
        ).all()
        trend = [
            {
                "job_id":    r[0],
                "scan_date": _dt_iso(r[1]),
                "total_gb":  round(float(r[2] or 0.0), 3),
            }
            for r in reversed(trend_rows)
        ]

        # Últimos 10 VMDKs (tabela de alertas recentes)
        recent = db.execute(
            select(ZombieVmdkRecord)
            .order_by(ZombieVmdkRecord.created_at.desc())
            .limit(10)
        ).scalars().all()

    return _ok({
        "total_zombies":     total_zombies,
        "total_size_gb":     total_gb,
        "pending_approvals": pending,
        "last_scan_at":      _dt_iso(last_scan),
        "vcenter_count":     vc_count,
        "by_type":           by_type,
        "top_vcenters":      top_vcenters,
        "trend":             trend,
        "recent_alerts":     [_vmdk_to_dict(r) for r in recent],
    })


# ─────────────────────────────────────────────────────────────────────────────
# API — Resultados de varredura
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/scan/results")
@app.get("/api/v1/scan/results")
def api_scan_results():
    # Parâmetros de paginação e filtros
    page      = max(1, int(request.args.get("page", 1)))
    per_page  = min(int(request.args.get("per_page", 25)), 200)
    tipo      = request.args.get("tipo")
    vcenter   = request.args.get("vcenter")
    job_id    = request.args.get("job_id")
    min_gb    = request.args.get("min_size_gb", type=float)
    scan_date = request.args.get("scan_date")
    sort_by   = request.args.get("sort_by", "created_at")
    sort_dir  = request.args.get("sort_dir", "desc")

    with _db() as db:
        q = select(ZombieVmdkRecord)

        if tipo:
            q = q.where(ZombieVmdkRecord.tipo_zombie == tipo.upper())
        if vcenter:
            q = q.where(or_(
                ZombieVmdkRecord.vcenter_host.ilike(f"%{vcenter}%"),
                ZombieVmdkRecord.vcenter_name.ilike(f"%{vcenter}%"),
            ))
        if job_id:
            q = q.where(ZombieVmdkRecord.job_id == job_id)
        if min_gb is not None:
            q = q.where(ZombieVmdkRecord.tamanho_gb >= min_gb)
        if scan_date:
            try:
                day_start = datetime.fromisoformat(scan_date).replace(
                    hour=0, minute=0, second=0, tzinfo=timezone.utc
                )
                day_end = day_start + timedelta(days=1)
                q = q.where(and_(
                    ZombieVmdkRecord.created_at >= day_start,
                    ZombieVmdkRecord.created_at <  day_end,
                ))
            except (ValueError, TypeError):
                pass

        total = db.execute(
            select(func.count()).select_from(q.subquery())
        ).scalar_one() or 0

        _cols = {
            "tamanho_gb":         ZombieVmdkRecord.tamanho_gb,
            "ultima_modificacao": ZombieVmdkRecord.ultima_modificacao,
            "tipo_zombie":        ZombieVmdkRecord.tipo_zombie,
            "created_at":         ZombieVmdkRecord.created_at,
        }
        col = _cols.get(sort_by, ZombieVmdkRecord.created_at)
        q = q.order_by(col.desc() if sort_dir == "desc" else col.asc())
        q = q.offset((page - 1) * per_page).limit(per_page)
        rows = db.execute(q).scalars().all()

        # Marca VMDKs que estão na whitelist
        wl_paths: set[str] = set()
        if rows:
            wl_paths = set(
                db.execute(
                    select(VmdkWhitelist.path).where(
                        VmdkWhitelist.path.in_([r.path for r in rows])
                    )
                ).scalars().all()
            )

        data = []
        for r in rows:
            d = _vmdk_to_dict(r)
            if r.path in wl_paths:
                d["status"] = "whitelist"
            data.append(d)

    return _ok({
        "data":         data,
        "total":        total,
        "pages":        max(1, -(-total // per_page)),
        "current_page": page,
        "per_page":     per_page,
    })


def _vmdk_to_dict(r: ZombieVmdkRecord) -> dict:
    return {
        "id":                    r.id,
        "job_id":                r.job_id,
        "path":                  r.path,
        "datastore":             r.datastore,
        "folder":                r.folder,
        "tamanho_gb":            round(float(r.tamanho_gb or 0.0), 3),
        "tipo_zombie":           r.tipo_zombie,
        "vcenter_host":          r.vcenter_host,
        "vcenter_name":          r.vcenter_name,
        "datacenter":            r.datacenter,
        "detection_rules":       r.detection_rules or [],
        "false_positive_reason": r.false_positive_reason,
        "ultima_modificacao":    _dt_iso(r.ultima_modificacao),
        "created_at":            _dt_iso(r.created_at),
        "status":                "novo",
        # Score de confiança: POSSIBLE_FALSE_POSITIVE = 40, outros = 85-100
        "confidence_score": (
            40 if r.tipo_zombie == "POSSIBLE_FALSE_POSITIVE" else
            70 if r.tipo_zombie == "BROKEN_CHAIN"            else
            85 if r.tipo_zombie == "UNREGISTERED_DIR"        else 95
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# API — vCenters
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/vcenters")
@app.get("/api/v1/vcenters")
def api_list_vcenters():
    with _db() as db:
        rows = db.execute(select(VCenter).order_by(VCenter.id)).scalars().all()
        result = [_vc_to_dict(v) for v in rows]
    return _ok(result)


@app.post("/api/vcenters")
@app.post("/api/v1/vcenters")
def api_create_vcenter():
    from app.core.security.crypto import encrypt_password, CryptoError
    data = request.get_json(silent=True) or {}
    for field in ("name", "host", "username", "password"):
        if not data.get(field):
            return _err(f"Campo obrigatório ausente: {field}", 422)
    try:
        enc = encrypt_password(data["password"])
    except CryptoError as e:
        return _err(str(e), 422)

    with _db() as db:
        if db.execute(select(VCenter).where(VCenter.name == data["name"])).scalar_one_or_none():
            return _err("Nome de vCenter já cadastrado.", 409)
        vc = VCenter(
            name               = data["name"],
            host               = data["host"],
            port               = int(data.get("port", 443)),
            username           = data["username"],
            password           = enc,
            disable_ssl_verify = bool(data.get("disable_ssl_verify", True)),
        )
        db.add(vc)
        db.flush()
        db.refresh(vc)
        result = _vc_to_dict(vc)
    return _ok(result, 201)


@app.route("/api/vcenters/<int:vcenter_id>",    methods=["PATCH"])
@app.route("/api/v1/vcenters/<int:vcenter_id>", methods=["PATCH"])
def api_update_vcenter(vcenter_id: int):
    from app.core.security.crypto import encrypt_password, CryptoError
    data = request.get_json(silent=True) or {}
    with _db() as db:
        vc = db.get(VCenter, vcenter_id)
        if not vc:
            return _err("vCenter não encontrado.", 404)
        if "password" in data and data["password"]:
            try:
                data["password"] = encrypt_password(data["password"])
            except CryptoError as e:
                return _err(str(e), 422)
        else:
            data.pop("password", None)
        _allowed = {"name", "host", "port", "username", "password",
                    "disable_ssl_verify", "is_active"}
        for k, v in data.items():
            if k in _allowed:
                setattr(vc, k, v)
        db.flush()
        db.refresh(vc)
        result = _vc_to_dict(vc)
    return _ok(result)


@app.route("/api/vcenters/<int:vcenter_id>",    methods=["DELETE"])
@app.route("/api/v1/vcenters/<int:vcenter_id>", methods=["DELETE"])
def api_delete_vcenter(vcenter_id: int):
    with _db() as db:
        vc = db.get(VCenter, vcenter_id)
        if not vc:
            return _err("vCenter não encontrado.", 404)
        db.delete(vc)
    return _ok({"deleted": True, "id": vcenter_id})


@app.get("/api/vcenters/pool-status")
@app.get("/api/v1/vcenters/pool-status")
def api_vcenter_pool_status():
    """Testa conectividade de todos os vCenters ativos e retorna mapa id→status."""
    with _db() as db:
        vcs = db.execute(
            select(VCenter).where(VCenter.is_active.is_(True))
        ).scalars().all()
        vc_snapshots = [
            {
                "id": vc.id, "host": vc.host, "port": vc.port,
                "username": vc.username, "password": vc.password,
                "disable_ssl_verify": vc.disable_ssl_verify,
            }
            for vc in vcs
        ]

    status_map: dict[str, str] = {}
    for vc in vc_snapshots:
        try:
            _test_vcenter_sync(vc, timeout=5)
            status_map[str(vc["id"])] = "online"
        except Exception:
            status_map[str(vc["id"])] = "offline"
    return _ok(status_map)


@app.post("/api/vcenters/<int:vcenter_id>/test")
@app.post("/api/v1/vcenters/<int:vcenter_id>/test")
@app.get("/api/vcenters/<int:vcenter_id>/ping")
@app.get("/api/v1/vcenters/<int:vcenter_id>/ping")
def api_test_vcenter(vcenter_id: int):
    with _db() as db:
        vc = db.get(VCenter, vcenter_id)
        if not vc:
            return _err("vCenter não encontrado.", 404)
        vc_data = {
            "host": vc.host, "port": vc.port,
            "username": vc.username, "password": vc.password,
            "disable_ssl_verify": vc.disable_ssl_verify,
        }
    try:
        return _ok(_test_vcenter_sync(vc_data))
    except Exception as e:
        return _err(f"Falha na conexão: {e}", 503)


def _test_vcenter_sync(vc_data: dict, timeout: int = 15) -> dict:
    """Testa conectividade com vCenter via pyVmomi (síncrono)."""
    from app.core.security.crypto import decrypt_password, CryptoError
    from pyVim.connect import SmartConnect, Disconnect

    try:
        password = decrypt_password(vc_data["password"])
    except CryptoError as e:
        raise ConnectionError(f"Erro de criptografia: {e}") from e

    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    if vc_data.get("disable_ssl_verify", True):
        context.check_hostname = False
        context.verify_mode    = ssl.CERT_NONE

    si = SmartConnect(
        host                  = vc_data["host"],
        user                  = vc_data["username"],
        pwd                   = password,
        port                  = int(vc_data.get("port", 443)),
        sslContext            = context,
        connectionPoolTimeout = timeout,
    )
    try:
        about = si.RetrieveContent().about
        return {
            "status":        "online",
            "api_version":   about.apiVersion,
            "instance_uuid": about.instanceUuid,
            "full_name":     about.fullName,
        }
    finally:
        Disconnect(si)


def _vc_to_dict(vc: VCenter) -> dict:
    return {
        "id":                 vc.id,
        "name":               vc.name,
        "host":               vc.host,
        "port":               vc.port,
        "username":           vc.username,
        "disable_ssl_verify": vc.disable_ssl_verify,
        "is_active":          vc.is_active,
        "created_at":         _dt_iso(vc.created_at),
        "updated_at":         _dt_iso(vc.updated_at),
    }


# ─────────────────────────────────────────────────────────────────────────────
# API — Aprovações (ApprovalToken)
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/approvals")
@app.get("/api/v1/approvals")
def api_list_approvals():
    status_filter = request.args.get("status")
    analyst       = request.args.get("analyst")
    action        = request.args.get("action")
    only_active   = request.args.get("only_active", "").lower() in ("1", "true", "yes")

    with _db() as db:
        q = select(ApprovalToken)
        if only_active:
            now = _now_utc()
            q = q.where(
                ApprovalToken.status.notin_(TERMINAL_STATUSES),
                ApprovalToken.expires_at > now,
            )
        elif status_filter:
            statuses = [s.strip() for s in status_filter.split(",")]
            q = q.where(ApprovalToken.status.in_(statuses))
        if analyst:
            q = q.where(ApprovalToken.analyst.ilike(f"%{analyst}%"))
        if action:
            q = q.where(ApprovalToken.action == action.upper())
        q = q.order_by(ApprovalToken.issued_at.desc())
        rows = db.execute(q).scalars().all()
        result = [_token_to_dict(t) for t in rows]
    return _ok(result)


@app.get("/api/approvals/<token_value>")
@app.get("/api/v1/approvals/<token_value>")
def api_get_approval(token_value: str):
    with _db() as db:
        t = db.execute(
            select(ApprovalToken).where(ApprovalToken.token == token_value)
        ).scalar_one_or_none()
        if not t:
            return _err("Token não encontrado.", 404)
        result = _token_to_dict(t)
    return _ok(result)


@app.post("/api/approvals")
@app.post("/api/v1/approvals")
def api_create_approval():
    data          = request.get_json(silent=True) or {}
    vmdk_path     = data.get("vmdk_path", "").strip()
    vcenter_id    = str(data.get("vcenter_id", "")).strip()
    action        = str(data.get("action", "")).upper()
    justification = data.get("justification", data.get("justificativa", "")).strip()
    analyst       = data.get("analyst", data.get("analista", "")).strip()

    if not vmdk_path:
        return _err("vmdk_path é obrigatório.", 422)
    if action not in ("QUARANTINE", "DELETE"):
        return _err("action deve ser QUARANTINE ou DELETE.", 422)
    if len(justification) < 20:
        return _err("justification deve ter no mínimo 20 caracteres.", 422)
    if not analyst:
        return _err("analyst é obrigatório.", 422)

    now = _now_utc()
    with _db() as db:
        # Garante apenas 1 token ativo por vmdk_path
        existing = db.execute(
            select(ApprovalToken).where(
                ApprovalToken.vmdk_path == vmdk_path,
                ApprovalToken.status.notin_(TERMINAL_STATUSES),
                ApprovalToken.expires_at > now,
            )
        ).scalar_one_or_none()
        if existing:
            return _err(
                f"Token ativo já existe para este VMDK: {existing.token[:16]}…", 409
            )

        # Snapshot do VMDK (última varredura)
        latest = db.execute(
            select(ZombieVmdkRecord)
            .where(ZombieVmdkRecord.path == vmdk_path)
            .order_by(ZombieVmdkRecord.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        token_val = secrets.token_hex(32)   # 64 hex chars
        token = ApprovalToken(
            token                = token_val,
            vmdk_path            = vmdk_path,
            vcenter_id           = vcenter_id,
            action               = action,
            analyst              = analyst,
            justification        = justification,
            issued_at            = now,
            expires_at           = now + timedelta(hours=24),
            status               = "pending_dryrun",
            vmdk_tipo_zombie     = latest.tipo_zombie if latest else None,
            vmdk_size_gb         = latest.tamanho_gb  if latest else None,
            vmdk_last_scan_job_id= latest.job_id      if latest else None,
            vmdk_datacenter      = latest.datacenter  if latest else None,
        )
        db.add(token)
        db.add(AuditLog(
            analyst              = analyst,
            action               = "CREATE_TOKEN",
            vmdk_path            = vmdk_path,
            vcenter_id           = vcenter_id,
            approval_token_value = token_val,
            dry_run              = False,
            readonly_mode_active = settings.readonly_mode,
            status               = "created",
            client_ip            = request.remote_addr,
            user_agent           = request.user_agent.string,
        ))
        db.flush()
        db.refresh(token)
        result = _token_to_dict(token)

    return _ok({**result, "dry_run_required": True, "expires_in": "24h"}, 201)


@app.get("/api/approvals/<token_value>/dryrun")
@app.get("/api/v1/approvals/<token_value>/dryrun")
def api_dryrun(token_value: str):
    """Simula a ação sem executar. OBRIGATÓRIO antes de /execute."""
    with _db() as db:
        token = db.execute(
            select(ApprovalToken).where(ApprovalToken.token == token_value)
        ).scalar_one_or_none()
        if not token:
            return _err("Token não encontrado.", 404)
        if token.status in TERMINAL_STATUSES:
            return _err(f"Token em status terminal: {token.status}.", 422)
        if _now_utc() > token.expires_at:
            token.status = "invalidated"
            token.invalidation_reason = "Token expirado."
            return _err("Token expirado.", 410)

        # Estado atual do VMDK no banco
        latest = db.execute(
            select(ZombieVmdkRecord)
            .where(ZombieVmdkRecord.path == token.vmdk_path)
            .order_by(ZombieVmdkRecord.created_at.desc())
            .limit(1)
        ).scalar_one_or_none()

        status_changed = bool(
            latest
            and token.vmdk_tipo_zombie
            and latest.tipo_zombie != token.vmdk_tipo_zombie
        )

        # Estimativa de arquivos afetados
        vmdk = token.vmdk_path
        flat = vmdk.replace(".vmdk", "-flat.vmdk")
        files_affected = [vmdk, flat]

        if token.action == "DELETE":
            action_preview = f"DELETAR permanentemente: {vmdk} e {flat}"
            destination    = None
        else:
            ds_name    = (token.vmdk_datacenter or "DATASTORE").split("]")[0].strip("[")
            destination = f"[{ds_name}] _QUARANTINE_/{Path(vmdk).name}"
            action_preview = f"MOVER para quarentena: {vmdk} → {destination}"

        dryrun_result = {
            "vmdk_path":           vmdk,
            "action":              token.action,
            "files_affected":      files_affected,
            "space_to_recover_gb": round(float(token.vmdk_size_gb or 0.0), 3),
            "current_tipo_zombie": latest.tipo_zombie if latest else token.vmdk_tipo_zombie,
            "status_changed":      status_changed,
            "datacenter":          token.vmdk_datacenter,
            "action_preview":      action_preview,
            "destination":         destination,
            "warnings": (
                ["⚠ Status do VMDK mudou desde a aprovação — revise!"]
                if status_changed else []
            ),
            "is_safe_to_proceed": not status_changed,
            "simulated_at":        _now_utc().isoformat(),
        }

        token.status             = "dryrun_done"
        token.dryrun_completed_at = _now_utc()
        token.dryrun_result      = dryrun_result

        db.add(AuditLog(
            analyst              = token.analyst,
            action               = "DRY_RUN",
            vmdk_path            = token.vmdk_path,
            vcenter_id           = token.vcenter_id,
            approval_token_id    = token.id,
            approval_token_value = token.token,
            dry_run              = True,
            readonly_mode_active = settings.readonly_mode,
            status               = "dry_run_completed",
            detail               = action_preview,
            client_ip            = request.remote_addr,
            user_agent           = request.user_agent.string,
        ))

    return _ok(dryrun_result)


@app.post("/api/approvals/<token_value>/execute")
@app.post("/api/v1/approvals/<token_value>/execute")
def api_execute(token_value: str):
    """Executa a ação aprovada. BLOQUEADO se READONLY_MODE=true."""
    # 1. Bloqueia se READONLY_MODE ativo
    if settings.readonly_mode:
        _log_blocked_audit(token_value, "blocked_readonly",
                           "READONLY_MODE=true bloqueia execução.")
        return _err(
            "READONLY_MODE=true — altere no .env para false antes de executar.", 403
        )

    body = request.get_json(silent=True) or {}
    if not body.get("confirmed"):
        return _err("Campo 'confirmed': true é obrigatório no corpo da requisição.", 422)

    with _db() as db:
        token = db.execute(
            select(ApprovalToken).where(ApprovalToken.token == token_value)
        ).scalar_one_or_none()
        if not token:
            return _err("Token não encontrado.", 404)
        if token.status in TERMINAL_STATUSES:
            _record_audit(db, token, "blocked_terminal", "Token em status terminal.")
            return _err(f"Token em status terminal: {token.status}.", 422)
        if _now_utc() > token.expires_at:
            token.status = "invalidated"
            _record_audit(db, token, "blocked_expired", "Token expirado.")
            return _err("Token expirado.", 410)
        if token.status != "dryrun_done":
            _record_audit(db, token, "blocked_no_dryrun", "Dry-run não foi executado.")
            return _err("Execute o dry-run antes de confirmar a ação.", 422)

        # Marca como executando (previne execução dupla)
        token.status      = "executed"
        token.executed_at = _now_utc()
        db.flush()

        # ── Delega para o módulo async via asyncio.run ─────────────────────────
        import asyncio
        try:
            from app.core.vmdk_actions import execute_action
            from app.models.base import AsyncSessionLocal

            async def _run_async():
                async with AsyncSessionLocal() as async_db:
                    from sqlalchemy import select as aselect
                    t = (await async_db.execute(
                        aselect(ApprovalToken).where(ApprovalToken.token == token_value)
                    )).scalar_one()
                    return await execute_action(t, async_db)

            exec_result = asyncio.run(_run_async())
            token.execution_result = {
                "success":            exec_result.success,
                "action":             exec_result.action,
                "files_processed":    exec_result.files_processed,
                "space_recovered_gb": exec_result.space_recovered_gb,
                "destination":        exec_result.destination,
                "error":              exec_result.error,
                "executed_at":        exec_result.executed_at,
            }
            audit_status = (
                f"executed_{token.action.lower()}" if exec_result.success else "failed"
            )
            _record_audit(db, token, audit_status,
                          exec_result.error or "Execução concluída.")

        except Exception as exc:
            logger.error("Falha na execução do token %s: %s", token_value, exc)
            token.status              = "invalidated"
            token.invalidation_reason = str(exc)
            _record_audit(db, token, "failed", str(exc))
            return _err(f"Falha na execução: {exc}", 500)

    return _ok({
        "status": "executed",
        "token":  token_value,
        "result": token.execution_result,
    })


@app.delete("/api/approvals/<token_value>")
@app.delete("/api/v1/approvals/<token_value>")
def api_cancel_approval(token_value: str):
    with _db() as db:
        token = db.execute(
            select(ApprovalToken).where(ApprovalToken.token == token_value)
        ).scalar_one_or_none()
        if not token:
            return _err("Token não encontrado.", 404)
        if token.status in TERMINAL_STATUSES:
            return _err(f"Token já está em status terminal: {token.status}.", 422)
        token.status = "cancelled"
        _record_audit(db, token, "cancelled", "Cancelado pelo analista.")
    return _ok({"cancelled": True, "token": token_value})


def _token_to_dict(t: ApprovalToken) -> dict:
    return {
        "id":                    t.id,
        "token":                 t.token,
        "vmdk_path":             t.vmdk_path,
        "vcenter_id":            t.vcenter_id,
        "action":                t.action,
        "analyst":               t.analyst,
        "justification":         t.justification,
        "issued_at":             _dt_iso(t.issued_at),
        "expires_at":            _dt_iso(t.expires_at),
        "status":                t.status,
        "vmdk_tipo_zombie":      t.vmdk_tipo_zombie,
        "vmdk_size_gb":          t.vmdk_size_gb,
        "vmdk_datacenter":       t.vmdk_datacenter,
        "vmdk_last_scan_job_id": t.vmdk_last_scan_job_id,
        "dryrun_completed_at":   _dt_iso(t.dryrun_completed_at),
        "dryrun_result":         t.dryrun_result,
        "executed_at":           _dt_iso(t.executed_at),
        "execution_result":      t.execution_result,
        "invalidation_reason":   t.invalidation_reason,
        "created_at":            _dt_iso(t.created_at),
    }


def _record_audit(db: Session, token: ApprovalToken,
                  status: str, detail: str) -> None:
    db.add(AuditLog(
        analyst              = token.analyst,
        action               = token.action,
        vmdk_path            = token.vmdk_path,
        vcenter_id           = token.vcenter_id,
        approval_token_id    = token.id,
        approval_token_value = token.token,
        dry_run              = False,
        readonly_mode_active = settings.readonly_mode,
        status               = status,
        detail               = detail,
        client_ip            = request.remote_addr,
        user_agent           = request.user_agent.string,
    ))


def _log_blocked_audit(token_value: str, status: str, detail: str) -> None:
    """Registra bloqueio no audit log sem precisar do token em mãos."""
    try:
        with _db() as db:
            token = db.execute(
                select(ApprovalToken).where(ApprovalToken.token == token_value)
            ).scalar_one_or_none()
            if token:
                _record_audit(db, token, status, detail)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────────────────────
# API — Audit Log
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/api/audit")
@app.get("/api/v1/audit")
@app.get("/api/approvals/audit-log")
@app.get("/api/v1/approvals/audit-log")
def api_audit_log():
    limit   = min(int(request.args.get("limit", 200)), 1000)
    analyst = request.args.get("analyst")
    action  = request.args.get("action")
    status  = request.args.get("status")
    export  = request.args.get("export", "").lower()

    with _db() as db:
        q = select(AuditLog)
        if analyst:
            q = q.where(AuditLog.analyst.ilike(f"%{analyst}%"))
        if action:
            q = q.where(AuditLog.action == action.upper())
        if status:
            q = q.where(AuditLog.status == status)
        q = q.order_by(AuditLog.timestamp.desc()).limit(limit)
        rows = db.execute(q).scalars().all()

    records = [_audit_to_dict(r) for r in rows]

    if export == "csv":
        return _audit_csv_response(records)
    return _ok(records)


def _audit_to_dict(r: AuditLog) -> dict:
    return {
        "id":                   r.id,
        "timestamp":            _dt_iso(r.timestamp),
        "analyst":              r.analyst,
        "action":               r.action,
        "vmdk_path":            r.vmdk_path,
        "vcenter_id":           r.vcenter_id,
        "approval_token_value": r.approval_token_value,
        "dry_run":              r.dry_run,
        "readonly_mode_active": r.readonly_mode_active,
        "status":               r.status,
        "detail":               r.detail,
        "client_ip":            r.client_ip,
    }


def _audit_csv_response(records: list[dict]) -> Response:
    buf = io.StringIO()
    fields = ["timestamp", "analyst", "vcenter_id", "vmdk_path",
              "action", "status", "dry_run", "readonly_mode_active",
              "detail", "client_ip"]
    writer = csv.DictWriter(buf, fieldnames=fields)
    writer.writeheader()
    for r in records:
        writer.writerow({k: r.get(k, "") for k in fields})

    ts  = _now_utc().strftime("%Y%m%d-%H%M%S")
    csv_data = "\ufeff" + buf.getvalue()   # BOM UTF-8 para Excel
    return Response(
        csv_data,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="audit-{ts}.csv"'},
    )


# ─────────────────────────────────────────────────────────────────────────────
# API — Scheduler (trigger manual)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/scheduler/run-now")
@app.post("/api/v1/scheduler/run-now")
def api_scheduler_run_now():
    """Dispara uma varredura manual imediatamente (sem aguardar o cron)."""
    try:
        from scheduler.cron_runner import trigger_now
        trigger_now()
        return _ok({"triggered": True, "message": "Varredura disparada manualmente."})
    except Exception as e:
        return _err(f"Erro ao disparar varredura: {e}", 500)


# ─────────────────────────────────────────────────────────────────────────────
# Tratamento de erros
# ─────────────────────────────────────────────────────────────────────────────

@app.errorhandler(404)
def error_404(e):
    if request.path.startswith("/api"):
        return _err("Recurso não encontrado.", 404)
    ctx            = _base_ctx()
    ctx["error_code"] = 404
    ctx["error_msg"]  = "Página não encontrada."
    return render_template("error.html", **ctx), 404


@app.errorhandler(500)
def error_500(e):
    logger.exception("Erro interno: %s", e)
    if request.path.startswith("/api"):
        return _err("Erro interno do servidor.", 500)
    ctx            = _base_ctx()
    ctx["error_code"] = 500
    ctx["error_msg"]  = "Erro interno do servidor."
    return render_template("error.html", **ctx), 500


@app.errorhandler(405)
def error_405(e):
    return _err("Método não permitido.", 405)


# ─────────────────────────────────────────────────────────────────────────────
# Entrypoint
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    # Inicia o scheduler em thread daemon antes do app.run()
    try:
        from scheduler.cron_runner import start as _start_scheduler
        _start_scheduler()
    except Exception as exc:
        logger.warning("Scheduler não pôde ser iniciado: %s", exc)

    port  = int(os.environ.get("FLASK_PORT", "5000"))
    debug = settings.debug

    logger.info(
        "ZombieHunter Web (Flask) iniciando em http://0.0.0.0:%d  debug=%s",
        port, debug,
    )
    # use_reloader=False evita dupla inicialização do scheduler no modo debug
    app.run(host="0.0.0.0", port=port, debug=debug, use_reloader=False)
