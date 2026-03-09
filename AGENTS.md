# AGENTS.md

## Repository Overview
Projeto Python/FastAPI para detectar VMDKs zombie/orphaned em vCenter, com API REST e interface web Jinja2.

## Key Components
- `main.py`: bootstrap da API, lifespan, rotas HTML e healthcheck.
- `app/api/routes/`: endpoints REST (`scan`, `vcenters`, `datastore-reports`, `approvals`, etc.).
- `app/core/`: conexao com vCenter, scheduler e logica de negocio.
- `app/models/`: modelos SQLAlchemy e inicializacao de banco.
- `web/`: templates e assets estaticos.
- `tests/`: testes `unit`, `integration` e `scenarios`.
- `start.ps1` / `stop.ps1`: ciclo de execucao local no Windows.

## Workflow Rules
- Instalar dependencias: `pip install -r requirements.txt`
- Rodar API (Windows): `.\start.ps1`
- Rodar API (cross-platform): `python -m uvicorn main:app --host 0.0.0.0 --port 8000`
- Parar API (Windows): `.\stop.ps1`
- Rodar testes: `pytest`
- Smoke rapido apos alteracoes pequenas: `python -m py_compile main.py config.py`

## Critical Patterns
- Preserve compatibilidade das rotas publicas ja expostas em `/api/v1/*` e `/health`.
- Mantenha `READONLY_MODE=true` como comportamento padrao de seguranca.
- Evite alterar scripts operacionais (`start.ps1`, `stop.ps1`) sem necessidade clara.
- Prefira patches pequenos e localizados; nao fazer refatoracao ampla em tarefas pequenas.

## Security Rules
- Nunca versionar segredos reais em `.env`.
- Nao expor credenciais de vCenter em logs, testes ou fixtures.
- Evitar operacoes destrutivas sem validacao explicita de seguranca.

## Do NOT
- Nao mexer em schemas/contratos de resposta sem atualizar testes e documentacao.
- Nao remover verificacoes de saude, auditoria ou aprovacao.
- Nao introduzir novas dependencias sem necessidade funcional objetiva.
