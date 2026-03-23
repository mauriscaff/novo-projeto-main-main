---
name: readonly-guard
description: Use esta skill para proteger fluxos sensiveis com READONLY_MODE, separacao de modos e bloqueio de execucao nao autorizada.
---

Objetivo:
Evitar execucao indevida e manter operacao segura por padrao.

Sempre:
- manter READONLY_MODE=true como padrao
- separar claramente recommendation, approval e execution
- bloquear execution quando readonly estiver ativo
- registrar tentativa bloqueada em log auditavel
- responder com mensagem clara e acionavel

Nunca:
- habilitar execucao por padrao
- bypass de readonly por parametro de frontend
- esconder motivo de bloqueio

Checklist minimo de implementacao:
- guard clause para execucao
- status HTTP consistente para bloqueio
- evento de auditoria com reason_code
- testes para caminho bloqueado e permitido

Saida esperada:
- guardrails no backend
- testes de seguranca de fluxo
- evidencias de auditoria