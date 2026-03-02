"""Testes do cálculo de score de confiança (0-100) conforme critérios Broadcom."""

from datetime import datetime, timedelta, timezone

from app.core.scanner.zombie_detector import (
    ZombieType,
    _compute_confidence_score,
)


def test_shared_datastore_heavy_penalty():
    """EX-3: datastore compartilhado -50 -> score baixo."""
    score = _compute_confidence_score(
        tipo_zombie=ZombieType.POSSIBLE_FALSE_POSITIVE,
        folder_has_registered_vm=False,
        is_shared_datastore=True,
        modification=None,
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert 5 <= score <= 100
    assert score == 5  # 40 - 50 = -10 -> clamp 5


def test_unregistered_dir_bonus():
    """UNREGISTERED_DIR +15."""
    score = _compute_confidence_score(
        tipo_zombie=ZombieType.UNREGISTERED_DIR,
        folder_has_registered_vm=False,
        is_shared_datastore=False,
        modification=None,
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert score >= 85


def test_score_clamped_between_5_and_100():
    """Score sempre entre 5 e 100."""
    score_low = _compute_confidence_score(
        ZombieType.POSSIBLE_FALSE_POSITIVE,
        folder_has_registered_vm=True,
        is_shared_datastore=True,
        modification=None,
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert score_low >= 5
    score_high = _compute_confidence_score(
        ZombieType.UNREGISTERED_DIR,
        folder_has_registered_vm=False,
        is_shared_datastore=False,
        modification=datetime.now(timezone.utc) - timedelta(days=200),
        orphan_days=60,
        stale_snapshot_days=15,
    )
    assert score_high <= 100
