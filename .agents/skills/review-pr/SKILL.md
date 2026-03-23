---
name: review-pr
description: Use esta skill para revisar PRs com foco em risco funcional, seguranca, contratos de API, testes e plano de rollback.
---

Objetivo:
Fazer review tecnico objetivo com foco em risco e operabilidade.

Ordem da revisao:
1. regressao funcional
2. seguranca e readonly
3. contratos de API e compatibilidade
4. observabilidade e logs
5. cobertura de testes
6. rollout e rollback

Sempre:
- listar findings por severidade
- incluir arquivo/trecho afetado
- apontar risco concreto e impacto
- sugerir mitigacao pratica
- validar se reason_code e explanation_text estao presentes

Nunca:
- review superficial sem evidencias
- aprovar mudanca sem testar caminho critico
- ignorar impacto de operacao em producao

Saida esperada:
- findings priorizados
- riscos e mitigacoes
- resumo de compatibilidade
- plano de rollback