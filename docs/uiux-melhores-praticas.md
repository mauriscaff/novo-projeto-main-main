# UI/UX — melhores práticas recomendadas para o ZombieHunter

Este guia reúne melhorias práticas para aumentar clareza operacional, acessibilidade e confiança do usuário na interface web.

## 1) Hierarquia visual e foco em tarefas críticas

1. Destacar no topo apenas 3 indicadores essenciais: total de VMDKs zombie, espaço recuperável e vCenters com falha.
2. Manter ações críticas sempre próximas do contexto (ex.: “Gerar approval token” junto de resultados com risco).
3. Evitar excesso de informação no primeiro viewport; mover detalhes técnicos para seções recolhíveis (accordion/drawer).

## 2) Feedback de sistema (loading, sucesso, erro)

1. Padronizar estados de carregamento em todas as telas (skeleton + texto objetivo).
2. Exibir mensagens de erro com:
   - o que aconteceu,
   - impacto,
   - próximo passo recomendado.
3. Diferenciar visualmente erros transitórios (rede) de erros de autorização/validação.

## 3) Acessibilidade (A11y)

1. Garantir contraste mínimo WCAG AA em todos os textos e badges.
2. Garantir navegação por teclado (tab order, foco visível, escape para fechar menus/dialogs).
3. Adicionar `aria-live="polite"` para avisos dinâmicos e alertas de sucesso/erro.
4. Evitar depender só de cor para status; incluir ícone e texto (“Conectado”, “Desconectado”).

## 4) Consistência de navegação

1. Sidebar deve refletir estado ativo de forma inequívoca e consistente entre rotas relacionadas.
2. Em mobile, incluir overlay ao abrir menu lateral e fechar com clique fora + ESC.
3. Incluir breadcrumb quando houver fluxo em múltiplos passos (ex.: scan → revisão → aprovação).

## 5) Tabelas e produtividade operacional

1. Salvar preferências de tabela por usuário (ordenação, filtros, colunas visíveis, paginação).
2. Adicionar filtros rápidos prontos (ex.: “>100 GB”, “últimos 7 dias”, “somente desconectados”).
3. Habilitar ações em lote com confirmação contextual e resumo de impacto.

## 6) Segurança UX (evitar erro humano)

1. Para ações destrutivas, usar confirmação em 2 etapas:
   - resumo do impacto,
   - confirmação explícita (digitar nome do vCenter/quantidade).
2. Mostrar claramente quando sistema está em `READONLY_MODE` e por que isso protege o ambiente.
3. Sempre exibir auditoria de “quem, quando, o quê” após cada operação sensível.

## 7) Performance percebida

1. Lazy loading para gráficos abaixo da dobra.
2. Atualização incremental de cards/contadores sem recarregar a página inteira.
3. Debounce em campos de busca e filtros para reduzir requisições desnecessárias.

## 8) Conteúdo e microcopy

1. Usar linguagem operacional curta e orientada à ação.
2. Padronizar termos (ex.: sempre “VMDK zombie”, sempre “Aprovação”).
3. Substituir mensagens genéricas por orientações acionáveis.

## 9) Responsividade

1. Garantir layout principal funcional em 320px–480px (mobile menor).
2. Evitar tabelas quebradas; usar colunas prioritárias + detalhes expansíveis.
3. Garantir botões críticos com área de toque adequada (mín. 44px).

## 10) Métricas de UX para evolução contínua

1. Tempo para completar fluxo de aprovação.
2. Taxa de erro por etapa (scan, aprovação, execução).
3. Taxa de abandono em telas críticas.
4. Tempo médio para identificar um problema real (MTTI operacional).

---

## Quick wins (baixo esforço, alto impacto)

1. Revisar contraste de badges e textos secundários no tema escuro.
2. Padronizar banners de erro/sucesso com template único.
3. Adicionar atalhos de filtro rápidos nas tabelas mais usadas.
4. Melhorar estados vazios com CTA claro (“Executar varredura agora”).
5. Adicionar ajuda contextual (tooltips curtos) nos campos técnicos.

## Roadmap sugerido

- **Sprint 1:** contraste, feedback de erro e consistência de navegação.
- **Sprint 2:** produtividade em tabelas e melhorias de fluxo de aprovação.
- **Sprint 3:** métricas de UX, ajustes finos por dados reais de uso.
