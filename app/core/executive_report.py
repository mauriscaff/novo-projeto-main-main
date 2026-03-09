"""
Helpers para relatorio executivo de descomissionamento de datastore.
"""

from __future__ import annotations

from datetime import datetime


def build_datastore_executive_report_markdown(
    *,
    job_id: str,
    datastore_name: str,
    datacenter: str | None,
    total_itens: int,
    total_size_gb: float,
    breakdown: dict[str, int],
    generated_at: datetime,
    vcenter_hosts: list[str],
    vcenter_names: list[str],
) -> str:
    lines: list[str] = []
    lines.append("# Relatorio Executivo - Descomissionamento de Datastore")
    lines.append(f"**Job ID:** `{job_id}`")
    lines.append(f"**Data do relatorio:** `{generated_at.strftime('%d/%m/%Y %H:%M:%S UTC')}`")
    lines.append("")
    lines.append("## Objetivo da analise")
    lines.append(
        "Registrar inventario e volumetria do datastore antes da exclusao "
        "integral, apoiando decisao gerencial com rastreabilidade tecnica."
    )
    lines.append("")
    lines.append("## Datastore analisado")
    lines.append(f"`{datastore_name}`")
    if datacenter:
        lines.append(f"Datacenter aplicado no filtro: `{datacenter}`")
    lines.append("")
    lines.append("## Volumetria total (GB)")
    lines.append(f"`{total_size_gb:.3f} GB`")
    lines.append("")
    lines.append("## Quantidade de itens por tipo")
    lines.append("| Tipo de item (tipo_zombie) | Quantidade |")
    lines.append("|---|---:|")
    shown = 0
    for tipo in sorted(breakdown):
        qtd = int(breakdown.get(tipo, 0))
        if qtd <= 0:
            continue
        lines.append(f"| {tipo} | {qtd} |")
        shown += 1
    if shown == 0:
        lines.append("| SEM_DADOS | 0 |")
    lines.append(f"| **Total** | **{total_itens}** |")
    lines.append("")
    lines.append("## Recomendacao operacional para exclusao do datastore inteiro")
    lines.append("1. Executar a mudanca em janela formal com aprovacao registrada.")
    lines.append("2. Confirmar ausencia de dependencia ativa (backup, replicacao e montagem).")
    lines.append("3. Remover o datastore em duas etapas: desanexar e excluir definitivamente.")
    lines.append("4. Preservar este relatorio como evidencia de auditoria da decisao.")
    lines.append("")
    lines.append("## Riscos e validacoes pos-exclusao")
    lines.append("- Risco de indisponibilidade por referencia residual em VM/servico.")
    lines.append("- Risco de falha de backup/replicacao por cadeia ainda apontando para o datastore.")
    lines.append("- Risco de inconformidade se nao houver trilha de evidencia tecnica.")
    lines.append("")
    lines.append("Validacoes recomendadas:")
    lines.append("1. Rodar novo scan apos a mudanca e confirmar ausencia de referencias ao datastore.")
    lines.append("2. Validar jobs de backup e replicacao no ciclo seguinte.")
    lines.append("3. Monitorar alarmes e incidentes por ao menos 24 horas.")
    lines.append("")
    lines.append("## Conclusao")
    lines.append("base para auditoria p\u00f3s-descomissionamento")
    lines.append("")
    lines.append("## Evidencias tecnicas")
    lines.append(f"- Fonte: `zombie_vmdk_records` filtrado por `job_id={job_id}`.")
    lines.append(f"- Filtro aplicado: `datastore_name={datastore_name}`.")
    if datacenter:
        lines.append(f"- Filtro adicional: `datacenter={datacenter}`.")
    lines.append(f"- Itens considerados: `{total_itens}`.")
    lines.append(f"- Volumetria consolidada: `{total_size_gb:.3f} GB`.")
    lines.append(f"- vCenter(s) nome: `{', '.join(vcenter_names) or 'N/A'}`.")
    lines.append(f"- vCenter(s) host: `{', '.join(vcenter_hosts) or 'N/A'}`.")
    lines.append(f"- Breakdown bruto: `{breakdown}`.")
    lines.append(
        "- Endpoint gerador: "
        f"`/api/v1/scan/jobs/{job_id}/executive-report?datastore_name={datastore_name}`."
    )

    return "\n".join(lines) + "\n"
