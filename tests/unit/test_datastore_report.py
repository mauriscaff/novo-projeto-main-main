from app.core.datastore_report import aggregate_datastore_rows


def test_aggregate_datastore_rows_counts_and_total_size():
    rows = [
        ("ORPHANED", 10.0),
        ("BROKEN_CHAIN", None),
        ("ORPHANED", 1.25),
        ("CUSTOM_TYPE", 2.0),
    ]

    total_itens, total_size_gb, breakdown = aggregate_datastore_rows(rows)

    assert total_itens == 4
    assert total_size_gb == 13.25
    assert breakdown["ORPHANED"] == 2
    assert breakdown["BROKEN_CHAIN"] == 1
    assert breakdown["CUSTOM_TYPE"] == 1


def test_aggregate_datastore_rows_empty_input():
    total_itens, total_size_gb, breakdown = aggregate_datastore_rows([])

    assert total_itens == 0
    assert total_size_gb == 0.0
    assert isinstance(breakdown, dict)
    # Tipos padrão devem existir com zero.
    assert breakdown["ORPHANED"] == 0
    assert breakdown["SNAPSHOT_ORPHAN"] == 0
