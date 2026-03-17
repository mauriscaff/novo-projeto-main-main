# Prompts prontos para criação de melhorias de UI/UX (ZombieHunter)

Use os prompts abaixo em IA (Codex/ChatGPT/Copilot) para implementar as melhorias no seu site de forma incremental e segura.

## Prompt 1 — Plano mestre de UI/UX (por fases)

```text
Você é um engenheiro sênior de Front-end e UX com experiência em FastAPI + Jinja2 + Bootstrap.
Analise este projeto e proponha um plano de implementação de melhorias de UI/UX em 3 sprints.

Objetivos:
1) Melhorar clareza visual e foco nas tarefas críticas.
2) Melhorar feedback de sistema (loading, sucesso, erro).
3) Melhorar acessibilidade (WCAG AA, teclado, aria).
4) Melhorar navegação em desktop e mobile.
5) Melhorar produtividade em tabelas e fluxos de aprovação.
6) Reduzir risco de erro humano em ações sensíveis.

Restrições:
- Não quebrar rotas públicas existentes: /api/v1/* e /health.
- Manter READONLY_MODE=true como padrão de segurança.
- Fazer patches pequenos e localizados.
- Não adicionar dependências sem necessidade objetiva.

Entregue:
- Diagnóstico por severidade (alto/médio/baixo impacto).
- Roadmap por sprint (quick wins vs mudanças estruturais).
- Diff incremental por fase.
- Testes de regressão (UI e API) e checklist de validação.
- Plano de rollback para cada fase.
```

## Prompt 2 — Hierarquia visual e dashboard acionável

```text
Refatore a tela de dashboard para destacar apenas indicadores críticos no primeiro viewport.

Faça:
1) Defina 3 KPIs principais no topo:
   - total de VMDKs zombie,
   - espaço recuperável,
   - vCenters com falha.
2) Reorganize cards/gráficos por prioridade operacional.
3) Mova detalhes técnicos para seções recolhíveis (accordion).
4) Preserve identidade visual atual (tema escuro e componentes existentes).

Requisitos:
- Manter compatibilidade com dados atuais da API.
- Sem alterar contratos de resposta.
- Criar estados vazios claros com CTA (“Executar varredura agora”).

Entregue:
- Patch de HTML/CSS/JS,
- teste visual/manual guiado,
- critérios objetivos de aceitação.
```

## Prompt 3 — Feedback de sistema padronizado

```text
Padronize feedback de sistema em toda a UI (loading, sucesso, erro).

Faça:
1) Crie padrão único para loading (skeleton + texto curto).
2) Crie padrão único de alertas com:
   - o que aconteceu,
   - impacto,
   - próximo passo recomendado.
3) Diferencie visualmente:
   - erro transitório (rede/timeout),
   - erro de permissão/autorização,
   - erro de validação.
4) Aplique nas páginas principais (dashboard, varredura, approvals, vcenters).

Entregue:
- componentes reaproveitáveis,
- patch mínimo por página,
- validação com cenários de falha simulados.
```

## Prompt 4 — Acessibilidade (A11y) prática

```text
Faça auditoria e correções de acessibilidade na interface web.

Checklist mínimo:
1) Contraste de cores (WCAG AA) para textos, badges e botões.
2) Navegação por teclado completa (tab, shift+tab, ESC em menus/dialogs).
3) Foco visível em todos elementos interativos.
4) ARIA adequado em conteúdo dinâmico (aria-live="polite" para feedbacks).
5) Não depender apenas de cor para status: incluir ícone + texto.

Restrições:
- Não alterar backend sem necessidade.
- Não quebrar layout atual.

Entregue:
- lista de problemas encontrados,
- patch por problema,
- evidência de validação (passo a passo de teste por teclado).
```

## Prompt 5 — Navegação mobile e consistência

```text
Melhore navegação da sidebar para mobile e consistência de estados ativos.

Faça:
1) Implementar overlay ao abrir sidebar em mobile.
2) Fechar menu com clique fora e tecla ESC.
3) Garantir destaque correto da rota ativa em páginas relacionadas.
4) Revisar títulos/tooltips para consistência de linguagem.

Entregue:
- patch em base template e scripts globais,
- checklist de responsividade (320px, 375px, 768px),
- captura de tela antes/depois.
```

## Prompt 6 — Tabelas orientadas à produtividade

```text
Melhore UX de tabelas operacionais com foco em eficiência.

Faça:
1) Salvar preferências do usuário (ordenação, filtros, colunas, paginação).
2) Adicionar filtros rápidos prontos (ex.: >100 GB, últimos 7 dias).
3) Criar ações em lote com resumo de impacto.
4) Garantir comportamento estável em mobile (colunas prioritárias + detalhes expansíveis).

Requisitos:
- Reaproveitar DataTables já existente.
- Não introduzir nova lib sem justificativa clara.

Entregue:
- patch incremental,
- cenários de teste funcionais,
- risco/mitigação por mudança.
```

## Prompt 7 — Segurança UX para ações destrutivas

```text
Implemente fluxo de confirmação em 2 etapas para operações sensíveis.

Fluxo:
1) Modal com resumo de impacto (quantidade, tamanho, alvo).
2) Confirmação explícita (digitar termo de confirmação).
3) Exibir claramente READONLY_MODE e motivo de bloqueio quando ativo.
4) Mostrar rastreabilidade pós-ação (quem, quando, o quê).

Restrições:
- Não remover regras atuais de approval/auditoria.
- Manter compatibilidade com endpoints existentes.

Entregue:
- ajustes de UI + integração com endpoints atuais,
- validação de casos de erro e cancelamento,
- checklist de prevenção de erro humano.
```

## Prompt 8 — Performance percebida da UI

```text
Otimize performance percebida da interface sem reescrever arquitetura.

Faça:
1) Lazy loading para gráficos fora da primeira dobra.
2) Atualização incremental de cards/indicadores.
3) Debounce em campos de busca/filtros.
4) Reduzir trabalho JS no carregamento inicial.

Entregue:
- medição antes/depois (tempo de primeira interação),
- patch por módulo,
- análise de trade-offs.
```

## Prompt 9 — Microcopy e linguagem operacional

```text
Padronize textos da interface para linguagem curta e orientada à ação.

Faça:
1) Criar guia de termos padrão (VMDK zombie, Aprovação, Varredura, etc.).
2) Reescrever mensagens genéricas para mensagens acionáveis.
3) Revisar títulos de cards, botões e alerts.

Entregue:
- tabela “antes/depois” dos principais textos,
- patch de templates,
- validação com checklist de consistência.
```

## Prompt 10 — PR final de governança

```text
Gere PR final com estrutura objetiva:
1) Resumo executivo.
2) Mudanças por arquivo.
3) Evidências (testes e screenshots).
4) Riscos e mitigação.
5) Plano de rollback.
6) Itens de segurança validados (READONLY_MODE, approvals, auditoria).

Inclua lista de verificação para QA manual de UI/UX.
```
