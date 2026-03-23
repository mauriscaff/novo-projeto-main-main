---
name: capacity-safety
description: Use esta skill ao definir regras de capacidade, margem operacional e bloqueios para recomendacoes de placement.
---

Objetivo:
Garantir que nenhuma recomendacao comprometa operacao segura de capacidade.

Sempre considerar:
- uso atual
- crescimento previsto
- risco de snapshot
- consolidacao
- swap
- margem operacional minima

Sempre:
- bloquear recomendacao sem margem minima
- usar calculo com buffer explicito
- retornar reason_code e explanation_text
- manter logs estruturados com valores de entrada e saida

Nunca:
- recomendar destino com free headroom insuficiente
- retornar apenas "erro generico" sem contexto tecnico
- misturar tipos de datastore no mesmo plano sem bloqueio

Checklist minimo de implementacao:
- thresholds parametrizaveis
- validacao de elegibilidade por datastore
- explicacao de capacidade no payload de resposta
- teste de regressao para cenarios limite

Saida esperada:
- regras implementadas
- cobertura de testes
- justificativa tecnica de calculo