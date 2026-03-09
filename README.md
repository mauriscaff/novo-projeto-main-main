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
| `DATABASE_URL` | URL do banco (SQLite por padrão) | `sqlite+aiosqlite:///ABSOLUTE_PATH/vmdk_scanner.db` |
| `DEFAULT_VCENTER_*` | vCenter padrão (opcional; cadastro via API) | Host, user, password, port, disable_ssl_verify |
| `FERNET_KEY` | Chave Fernet para criptografia de senhas de vCenter | Gerar com `Fernet.generate_key()` |
| `FERNET_KEY_FILE` | Caminho de fallback para chave Fernet local (dev) quando `FERNET_KEY` estiver vazia/placeholder | `.fernet.key` |
| `ORPHAN_DAYS` | Dias mínimos sem referência para considerar VMDK zombie | `60` |
| `STALE_SNAPSHOT_DAYS` | Dias mínimos para reportar snapshot orphan | `15` |
| `MIN_FILE_SIZE_MB` | Arquivos menores que isso são ignorados (exceto BROKEN_CHAIN) | `50` |
| `SCAN_MAX_WORKERS` | Workers simultâneos na varredura | `4` |
| `SCAN_DATASTORE_TIMEOUT_SEC` | Timeout por datastore (segundos) | `900` |
| `DATASTORE_REPORTS_VERIFY_TIMEOUT_SEC` | Timeout defensivo da verificacao por arquivo (segundos) | `30` |
| `READONLY_MODE` | **true** = API não executa delete/move/rename em VMDKs | `true` (obrigatório como padrão) |
| `FLASK_PORT` | Porta do servidor Flask (interface web alternativa) | `5000` |
| `SCAN_CRON_*`, `PS_SCRIPT_PATH`, `REPORTS_DIR` | Agendamento e caminhos para scripts/relatórios | Conforme `.env.example` |

Observação: se `DATABASE_URL` vier como SQLite relativo (`./arquivo.db`), a aplicação normaliza para caminho absoluto no diretório do projeto para evitar perda aparente de histórico entre execuções.

---

## Como usar

1. **Subir a API**:

   ```bash
   uvicorn main:app --reload --host 0.0.0.0 --port 8000
   ```

   No Windows/PowerShell (recomendado para manter padrao do projeto):

   ```powershell
   .\start.ps1
   ```

   Se houver bloqueio de politica de execucao, use:

   ```bat
   start.cmd
   ```

   Para ver erro em tempo real (foreground):

   ```powershell
   .\start.ps1 -Foreground
   ```

   Para parar:

   ```powershell
   .\stop.ps1
   ```

   Alternativa:

   ```bat
   stop.cmd
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

## Fluxo operacional: relatorio pre/pós exclusao de datastore

Use este fluxo quando a operacao for excluir o datastore inteiro.

1. **Executar scan pre-exclusao**:
   - Rode um scan atualizado (`POST /api/v1/scan/start` ou `/api/v1/scan/start-by-datastore`).
   - Aguarde status `completed`.

2. **Gerar snapshot pre_delete**:
   - `POST /api/v1/datastore-reports/snapshots`
   - Exemplo de payload:
   ```json
   {
     "phase": "pre_delete",
     "job_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
     "datastore": "DS1"
   }
   ```
   - Guarde o `pair_id` retornado.

3. **Excluir o datastore no ambiente VMware**:
   - Operacao manual fora da API (janela de mudanca aprovada).

4. **Executar scan post-exclusao**:
   - Rode novo scan apos a mudanca.
   - Aguarde status `completed`.

5. **Gerar snapshot post_delete (mesmo pair_id)**:
   - `POST /api/v1/datastore-reports/snapshots`
   - Exemplo:
   ```json
   {
     "phase": "post_delete",
     "job_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
     "datastore": "DS1",
     "pair_id": "a8f9fdb9f1ef4ab987ce8d8e2f31db23"
   }
   ```

6. **Comparar pre/pós para medir remocao**:
   - `GET /api/v1/datastore-reports/compare?pre_report_id=10&post_report_id=11`
   - Opcional de governança estrita de pareamento:
     - `GET /api/v1/datastore-reports/compare?pre_report_id=10&post_report_id=11&strict_pair=true`
   - Retorno principal:
     - `removed_items`
     - `removed_size_gb`
     - `removed_breakdown` por `tipo_zombie`
     - `pre_totals` e `post_totals`

7. **Consultar snapshot por ID quando necessario**:
   - `GET /api/v1/datastore-reports/snapshots/{report_id}`

8. **Regras de validacao do fluxo**:
   - `job_id` inexistente retorna `404`.
   - datastore sem registros no job retorna `404`.
   - compare entre datastores diferentes retorna `422`.
   - verificacao por `pair_id` sem datastore no `pre_delete` retorna `404`.
   - par inconsistente de datastore entre pre/post retorna `422`.
   - compare exige `pre_delete` no `pre_report_id` e `post_delete` no `post_report_id`.

9. **Verificar evidencia real por arquivo (VMDK) via pair_id**:
   - Principal: `GET /api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}`
   - Equivalente: `GET /api/v1/datastore-reports/verify-files/{pair_id}?page=1&page_size=200`
   - Exemplo com filtros e timeout:
     - `GET /api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}?include_deleted_limit=200&tipo_zombie=ORPHANED&min_size_gb=10&timeout_sec=30`
   - Query params suportados:
     - `page` (>=1), `page_size` (1..1000)
     - `sort_by` = `size_desc|size_asc|path_asc|path_desc`
     - `include_deleted_vmdks` = `true|false`
     - `include_deleted_limit` (1..5000, default `200`) — cap de evidencias em `deleted_vmdks`
     - `tipo_zombie` (repetivel)
     - `min_size_gb` (float >= 0)
     - `timeout_sec` (1..120)
   - Campos principais no payload:
     - `datastore_name`
     - `pre_job_id`, `post_job_id`
     - `datastore_found_in_pre` e `datastore_found_in_post`
     - `removed_files_count`
     - `removed_size_gb`
     - `deleted_files_count`
     - `deleted_size_gb`
     - `size_gain_gb`
     - `size_gain_percent`
     - `datastore_status` = `removed|still_present|unknown`
     - `deleted_breakdown`
     - `deleted_size_breakdown_gb`
     - `has_more_evidence`
     - `deleted_vmdks` (paginado)
   - Quando `page` estiver fora do total de evidencias, a API retorna `200` com `deleted_vmdks=[]` e `has_more_evidence=false`.
   - Exemplo de resposta:
   ```json
   {
     "pair_id": "a8f9fdb9f1ef4ab987ce8d8e2f31db23",
     "datastore": "DS1",
     "datastore_name": "DS1",
     "pre_job_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
     "post_job_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
     "datastore_found_in_pre": true,
     "datastore_found_in_post": true,
     "datastore_status": "still_present",
     "removed_files_count": 2,
     "removed_size_gb": 12.0,
     "deleted_files_count": 2,
     "deleted_size_gb": 12.0,
     "size_gain_gb": 12.0,
     "size_gain_percent": 70.59,
     "deleted_breakdown": {
       "ORPHANED": 1,
       "POSSIBLE_FALSE_POSITIVE": 1
     },
     "deleted_size_breakdown_gb": {
       "ORPHANED": 10.0,
       "POSSIBLE_FALSE_POSITIVE": 2.0
     },
     "page": 1,
     "page_size": 200,
     "total_evidence": 2,
     "has_more_evidence": false,
     "status": "ok",
     "message": "2 arquivo(s) removido(s) identificado(s) no pair_id 'a8f9fdb9f1ef4ab987ce8d8e2f31db23'.",
     "deleted_vmdks": [
       {
         "path": "[DS1] vm-a/a.vmdk",
         "tipo_zombie": "ORPHANED",
         "tamanho_gb": 10.0,
         "last_seen_job_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
       }
     ]
   }
   ```

10. **Exportar evidencias por arquivo**:
   - CSV: `GET /api/v1/datastore-reports/verify-files/{pair_id}/export?format=csv`
   - JSON completo: `GET /api/v1/datastore-reports/verify-files/{pair_id}/export?format=json`
   - Filtros opcionais tambem aceitos: `tipo_zombie`, `min_size_gb`, `timeout_sec`.
   - Para datasets muito grandes, limite de exportacao por `max_rows` (default `50000`).
   - Se `total_evidence > max_rows`, a API retorna `422` com mensagem para ajustar `max_rows` ou usar paginacao.

11. **Limites recomendados para ambientes grandes**:
   - `page_size`: prefira entre `100` e `500`.
   - `include_deleted_vmdks=false` para consultar apenas agregados.
   - Use `tipo_zombie` e `min_size_gb` para reduzir cardinalidade da evidencia.
   - Comece com `timeout_sec=30`; aumente somente quando necessario.
   - Em export, mantenha `max_rows` <= `50000` para evitar uso excessivo de memoria.

12. **Verificacao automatica sem pair_id (baseline x verification)**:
   - Endpoint: `GET /api/v1/datastore-reports/datastore-deletion-verification`
   - Query params:
     - `datastore` (obrigatorio)
     - `vcenter_host` (opcional)
     - `evidence_limit` (opcional, default `200`)
   - Comportamento:
     - Seleciona o scan `completed` mais recente como `verification`.
     - Busca o `baseline` imediatamente anterior onde o datastore existia (por registros ou `datastore_metrics`).
     - Compara por `path` de VMDK:
       - presente no baseline e ausente no verification = excluido
       - presente em ambos = remanescente
     - Define `datastore_removed` quando o verification nao tem VMDKs do datastore **e** o datastore nao aparece em `datastore_metrics`.
   - Status possiveis:
     - `datastore_removed`
     - `partial_cleanup`
     - `no_cleanup`
   - Erros:
     - `404` se nao houver scan de verificacao
     - `404` se nao houver baseline anterior para o datastore
   - Exemplo:
   ```bash
    curl -H "X-API-Key: <API_KEY>" \
      "http://localhost:8000/api/v1/datastore-reports/datastore-deletion-verification?datastore=DS1&vcenter_host=vc01.local&evidence_limit=200"
    ```

13. **Totais acumulados do que ja foi excluido (auditavel)**:
   - Endpoint: `GET /api/v1/datastore-reports/datastore-deletion-verification/totals`
   - Filtros opcionais:
     - `datastore`
     - `vcenter_host`
   - Retorna:
     - `total_verifications`
     - `total_datastores_removed`
     - `total_deleted_vmdks`
     - `total_deleted_size_gb`
     - `last_verification_at`
   - Exemplo:
   ```bash
   curl -H "X-API-Key: <API_KEY>" \
     "http://localhost:8000/api/v1/datastore-reports/datastore-deletion-verification/totals?datastore=DS1&vcenter_host=vc01.local"
   ```

## Relatório pós-exclusão com detecção automática de datastore

### Fluxo legado (mantido): com `pair_id`
- Continua suportado para ciclos pre/post explícitos:
  - `GET /api/v1/datastore-reports/post-exclusion-file-verification/{pair_id}`
  - `GET /api/v1/datastore-reports/verify-files/{pair_id}`
- Útil quando já existe pareamento formal de snapshots pre_delete/post_delete.

### Fluxo recomendado: sem `pair_id` (automático)
- Endpoint:
  - `GET /api/v1/datastore-reports/datastore-deletion-verification?datastore=<NOME>&vcenter_host=<HOST_OPCIONAL>`
- O endpoint seleciona automaticamente:
  - scan `verification` mais recente (`completed`)
  - scan `baseline` imediatamente anterior onde o datastore existia

### O que esse relatório retorna
- Inferência de datastore removido no vCenter:
  - `datastore_removed` (`true|false`) e `status` (`datastore_removed|partial_cleanup|no_cleanup`)
- Volumetria removida:
  - `deleted_size_gb` e `deleted_size_tb`
  - além de `baseline_size_gb/tb`, `remaining_size_gb/tb` e `size_gain_percent`
- Evidências por VMDK:
  - `deleted_vmdks[]` com `path`, `tamanho_gb`, `tipo_zombie`, `datacenter`, `last_seen_job_id`

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
