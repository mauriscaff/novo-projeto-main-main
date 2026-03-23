---
name: sdrs-policy-engine
description: Use esta skill ao criar ou alterar codigo relacionado a regras de Storage DRS, placement, pre-checks, bloqueios e explicacoes de decisao.
---

Objetivo:
Implementar ou revisar a engine de decisao do SDRS.

Sempre:
- separar recommendation mode, approval mode e execution mode
- gerar reason_code em toda recomendacao e bloqueio
- gerar explanation_text claro para usuario
- gerar logs estruturados e auditaveis
- respeitar READONLY_MODE=true como padrao
- incluir testes de caminho feliz e cenarios de bloqueio

Nunca:
- assumir automacao destrutiva sem controle explicito
- executar mudancas sem pre-check de compatibilidade e capacidade
- esconder motivo de recomendacao ou bloqueio
- ignorar regras de datastore cluster

Checklist minimo de implementacao:
- validar escopo (datacenter, datastore cluster, datastores elegiveis)
- validar pre-condicoes de VM (override, affinity, independent disk)
- validar capacidade (margem operacional e buffer)
- produzir payload com campos de rastreabilidade
- registrar trilha de auditoria por decisao

Saida esperada:
- codigo
- testes
- resumo tecnico da mudanca
- riscos identificados