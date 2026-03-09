# Frontend Auth Migration (Jinja2 + FastAPI)

## Goal
Remove static API key exposure in browser HTML/JS and move web frontend auth to server-issued JWT session cookie.

## Scope
- Keep `/api/v1/*` endpoints compatible.
- Keep `/health` public.
- Keep `/health/readiness` authenticated.
- Keep `READONLY_MODE=true` default unchanged.

## Target Design
- Browser no longer receives real `API_KEY` in template context or JavaScript globals.
- Web login uses `POST /api/v1/auth/session/login` with username/password.
- Backend sets JWT cookie `zh_access_token` with:
  - `HttpOnly=true`
  - `SameSite=Strict`
  - `Secure=true` when running on HTTPS
- Browser calls same-origin `/api/v1/*` with cookie credentials.
- Backend auth dependency accepts:
  - `X-API-Key` (backward compatibility for existing API clients)
  - `Authorization: Bearer <jwt>`
  - `zh_access_token` cookie (web session)

## Phased Plan

### Phase 1 (low complexity): Stop key exposure in templates
- Remove API key injection into HTML/JS.
- Keep compatibility shim `window.ZH_API_KEY = ""` to avoid frontend breakage.
- Add global `fetch` wrapper to strip legacy `X-API-Key` header and enforce same-origin credentials.

Rollback:
- Re-enable prior template key injection only if web UI becomes unusable.

### Phase 2 (medium complexity): Add session endpoints + backend auth path
- Add routes:
  - `POST /api/v1/auth/session/login`
  - `POST /api/v1/auth/session/logout`
  - `GET /api/v1/auth/session/me`
- Update auth dependency to accept JWT from `zh_access_token` cookie.
- Keep API key header auth untouched for external clients.

Rollback:
- Disable session endpoints and restore previous auth dependency branch.

### Phase 3 (medium complexity): Update base template UX
- Add login modal for web session bootstrap.
- Add session state indicator and logout button in navbar.
- Guard periodic background calls when unauthenticated to avoid noisy 401 loops.

Rollback:
- Remove modal wiring and return to old frontend bootstrap while keeping backend compatibility.

### Phase 4 (low complexity): Test and docs hardening
- Add integration tests for session login/logout/me.
- Confirm `/health/readiness` works with authenticated session cookie.
- Confirm dashboard no longer exposes secret and no legacy auth cookie is set.

Rollback:
- Revert test/documentation changes only if CI unexpectedly blocks release.

## Security Checklist
- [x] No static API key in rendered HTML.
- [x] No static API key in browser JavaScript payload.
- [x] Session cookie is `HttpOnly`.
- [x] Session cookie uses `SameSite=Strict`.
- [x] Session cookie uses `Secure` on HTTPS.
- [x] `/health` remains unauthenticated.
- [x] `/health/readiness` remains authenticated.
- [x] `/api/v1/*` clients using `X-API-Key` still work.
- [x] Failed auth returns 401 (no silent pass-through).

## Validation Commands
```powershell
python -m py_compile app/dependencies.py app/api/routes/auth.py main.py tests/integration/test_web_auth_cookie.py
pytest -q tests/integration/test_web_auth_cookie.py
pytest -q tests/integration/test_api_endpoints.py
```

## Notes
- Current session login uses demo credentials (`admin/P@ssw0rd`) from existing auth module behavior.
- For production, integrate real user storage and password hashing before internet exposure.
