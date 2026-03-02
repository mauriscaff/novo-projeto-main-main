# ZombieHunter — Scanner de VMDKs zombie/orphaned em vCenter

## Visão Geral

Este projeto é uma API REST (FastAPI) para **varredura e gestão de VMDKs zombie/orphaned** em ambientes VMware vCenter. Ele detecta discos virtuais que existem no datastore mas não estão referenciados por nenhuma VM ou template no inventário do vCenter, seguindo as definições oficiais Broadcom/VMware (KB 404094).

Principais funções:

- Varredura de um ou vários vCenters e Datacenters
- Classificação dos VMDKs por tipo (ORPHANED, SNAPSHOT_ORPHAN, BROKEN_CHAIN, etc.)
- Score de confiança (0–100) para apoio à decisão de remoção
- Modo **somente leitura** por padrão; operações destrutivas exigem aprovação explícita e token
- Métricas por datastore (duração da varredura, arquivos e zombies encontrados) para troubleshooting

---

## Pré-requisitos

- **Python 3.11+**
- Acesso de rede aos vCenters (porta 443)
- Credenciais de usuário vCenter com permissão para:
  - Ler inventário (VMs, templates, datastores)
  - Navegar em datastores (DatastoreBrowser)
- Dependências principais: **pyVmomi** (SDK VMware), FastAPI, SQLAlchemy, Pydantic

---

## Instalação

1. **Clonar o repositório** (ou baixar o código):

   ```bash
   git clone <url-do-repositorio>
   cd <diretorio-do-projeto>
   ```

2. **Criar ambiente virtual** (recomendado):

   ```bash
   python -m venv .venv
   .venv\Scripts\activate   # Windows
   # ou: source .venv/bin/activate   # Linux/macOS
   ```

3. **Instalar dependências**:

   ```bash
   pip install -r requirements.txt
   ```

4. **Configurar variáveis de ambiente**:

   - Copiar o arquivo de exemplo: `copy .env.example .env` (Windows) ou `cp .env.example .env` (Linux/macOS)
   - Editar `.env` (ou `.env.local`) e preencher os valores reais (segredos, vCenter, etc.). **Nunca** versionar `.env` ou `.env.local`.

---

## Configuração

As variáveis abaixo são lidas do `.env` ou `.env.local`. Valores sensíveis devem usar placeholders em `.env.example` (ex.: `YOUR_VALUE_HERE`).

| Variável | Significado | Exemplo / Padrão |
|----------|-------------|-------------------|
| `APP_NAME` | Nome da aplicação | `VMDK Zombie Scanner` |
| `DEBUG` | Modo debug | `false` |
| `CORS_ALLOWED_ORIGINS` | Origens permitidas para CORS (separadas por vírgula) | `http://localhost:8000` |
| `SECRET_KEY` | Chave para assinatura JWT | Gerar com `openssl rand -hex 32` |
| `API_KEY` | Chave estática (alternativa ao JWT) | Definir valor seguro |
| `DATABASE_URL` | URL do banco (SQLite por padrão) | `sqlite+aiosqlite:///./vmdk_scanner.db` |
| `DEFAULT_VCENTER_*` | vCenter padrão (opcional; cadastro via API) | Host, user, password, port, disable_ssl_verify |
| `FERNET_KEY` | Chave Fernet para criptografia de senhas de vCenter | Gerar com `Fernet.generate_key()` |
| `ORPHAN_DAYS` | Dias mínimos sem referência para considerar VMDK zombie | `60` |
| `STALE_SNAPSHOT_DAYS` | Dias mínimos para reportar snapshot orphan | `15` |
| `MIN_FILE_SIZE_MB` | Arquivos menores que isso são ignorados (exceto BROKEN_CHAIN) | `50` |
| `SCAN_MAX_WORKERS` | Workers simultâneos na varredura | `4` |
| `SCAN_DATASTORE_TIMEOUT_SEC` | Timeout por datastore (segundos) | `900` |
| `READONLY_MODE` | **true** = API não executa delete/move/rename em VMDKs | `true` (obrigatório como padrão) |
| `FLASK_PORT` | Porta do servidor Flask (interface web alternativa) | `5000` |
| `SCAN_CRON_*`, `PS_SCRIPT_PATH`, `REPORTS_DIR` | Agendamento e caminhos para scripts/relatórios | Conforme `.env.example` |

---

## Como usar

1. **Subir a API**:

   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

2. **Documentação interativa**:
   - Swagger UI: **http://localhost:8000/docs**
   - ReDoc: **http://localhost:8000/redoc**

3. **Autenticar**:
   - Obter token: `POST /api/v1/auth/token` (form: username, password) ou usar header `X-API-Key` com a `API_KEY` configurada.
   - Nas requisições seguintes: header `Authorization: Bearer <token>` ou `X-API-Key: <sua_api_key>`.

4. **Cadastrar vCenter** (se não usar apenas o padrão do `.env`):
   - `POST /api/v1/vcenters` com host, usuário, senha, etc.

5. **Rodar o primeiro scan**:
   - `POST /api/v1/scan/start` com `vcenter_ids` e opcionalmente `datacenters`.
   - Retorna `job_id`. Consultar status e resultados em `GET /api/v1/scan/jobs/{job_id}` e `GET /api/v1/scan/results/{job_id}`.

---

## Tipos de Zombie detectados

| Tipo | Descrição |
|------|------------|
| **ORPHANED** | VMDK existe no datastore mas não está referenciado em nenhuma VM/template (Get-HardDisk). Inclui disco removido com “Remove” sem “Delete from disk”. |
| **SNAPSHOT_ORPHAN** | Arquivo *-delta.vmdk ou *-000001.vmdk sem snapshot ativo correspondente na VM. |
| **BROKEN_CHAIN** | Descriptor .vmdk aponta para extent (-flat.vmdk ou parent) inexistente; cadeia corrompida. |
| **UNREGISTERED_DIR** | VMDK em pasta que não corresponde a nenhuma VM registrada no vCenter. |
| **POSSIBLE_FALSE_POSITIVE** | Datastore compartilhado entre múltiplos Datacenters/vCenters; o VMDK pode estar em uso em outro escopo. Não deve ser tratado como candidato a deleção sem verificação manual. |

Os tipos **ORPHANED**, **SNAPSHOT_ORPHAN**, **BROKEN_CHAIN** e **UNREGISTERED_DIR** são considerados excluíveis (podem ser alvo de ações de remoção após aprovação). **POSSIBLE_FALSE_POSITIVE** permanece apenas para revisão.

---

## Salvaguardas de segurança

- **READONLY_MODE**: O padrão é `true`. Enquanto estiver ativo, a API **não executa** deleção, movimentação ou renomeação de VMDKs.
- **Aprovação para operações destrutivas**: É obrigatório:
  1. Definir `READONLY_MODE=false` no ambiente (decisão explícita do analista).
  2. Obter um **ApprovalToken** via `POST /api/v1/approvals` (gerado por analista humano).
  3. Incluir o token no header **X-Approval-Token** e enviar **"confirmed": true** no **corpo** da requisição (nunca só na query string).
- **Dry-run**: Executar primeiro o endpoint de **dry-run** correspondente e validar o resultado antes da execução real.
- **AuditLog**: Toda operação destrutiva gera registro imutável em **AuditLog** (analista, timestamp, justificativa, token).

Nenhuma deleção automática é feita com base apenas em score de confiança; o fluxo de aprovação e auditoria é sempre exigido.
