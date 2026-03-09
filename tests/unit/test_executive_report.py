from datetime import datetime, timezone

from app.core.executive_report import build_datastore_executive_report_markdown


def test_build_datastore_executive_report_markdown_contains_required_sections():
    md = build_datastore_executive_report_markdown(
        job_id="22222222-2222-2222-2222-222222222222",
        datastore_name="DS1",
        datacenter="DC1",
        total_itens=3,
        total_size_gb=18.5,
        breakdown={"ORPHANED": 1, "BROKEN_CHAIN": 1, "SNAPSHOT_ORPHAN": 1},
        generated_at=datetime(2026, 3, 5, 12, 0, 0, tzinfo=timezone.utc),
        vcenter_hosts=["vc-prod.local"],
        vcenter_names=["vc-prod"],
    )

    assert "# Relatorio Executivo - Descomissionamento de Datastore" in md
    assert "## Objetivo da analise" in md
    assert "## Datastore analisado" in md
    assert "`DS1`" in md
    assert "## Volumetria total (GB)" in md
    assert "`18.500 GB`" in md
    assert "## Quantidade de itens por tipo" in md
    assert "| ORPHANED | 1 |" in md
    assert "| BROKEN_CHAIN | 1 |" in md
    assert "| SNAPSHOT_ORPHAN | 1 |" in md
    assert "| **Total** | **3** |" in md
    assert "## Recomendacao operacional para exclusao do datastore inteiro" in md
    assert "## Riscos e validacoes pos-exclusao" in md
    assert "## Evidencias tecnicas" in md
    assert "job_id=22222222-2222-2222-2222-222222222222" in md
