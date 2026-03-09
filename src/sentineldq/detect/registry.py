"""
Pluggable detection rule registry. Table-level and column-level rules are
driven by dataset.checks; add new rules here without changing the runner.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, List

from .volume import detect_volume_anomaly
from .freshness import detect_freshness_anomaly
from .null_spike import detect_null_spike

if TYPE_CHECKING:
    from ..config import DatasetSpec


def _run_volume(
    dataset: "DatasetSpec",
    profile: dict,
    run_id: str,
    store: Any,
) -> List[dict]:
    if "volume_change_pct" not in dataset.checks and "volume_min_history" not in dataset.checks:
        return []
    history = store.get_recent_row_counts(
        table_name=dataset.name,
        limit=7,
        exclude_run_id=run_id,
    )
    alert = detect_volume_anomaly(
        table_name=dataset.name,
        current_row_count=int(profile["row_count"]),
        history_row_counts=history,
        change_pct_threshold=float(dataset.checks.get("volume_change_pct", 0.30)),
        min_history=int(dataset.checks.get("volume_min_history", 3)),
    )
    return [alert] if alert else []


def _run_freshness(
    dataset: "DatasetSpec",
    profile: dict,
    run_id: str,
    store: Any,
) -> List[dict]:
    if not dataset.freshness_column or "freshness_minutes" not in dataset.checks:
        return []
    alert = detect_freshness_anomaly(
        table_name=dataset.name,
        max_freshness_ts=profile.get("max_freshness_ts"),
        threshold_minutes=int(dataset.checks.get("freshness_minutes", 180)),
    )
    return [alert] if alert else []


def _run_null_spike(
    dataset: "DatasetSpec",
    column_profile: dict,
    run_id: str,
    store: Any,
) -> List[dict]:
    if "null_spike_abs" not in dataset.checks and "null_min_history" not in dataset.checks:
        return []
    history = store.get_recent_null_rates(
        table_name=dataset.name,
        column_name=column_profile["column_name"],
        limit=7,
        exclude_run_id=run_id,
    )
    alert = detect_null_spike(
        table_name=dataset.name,
        column_name=column_profile["column_name"],
        current_null_rate=float(column_profile["null_rate"]),
        history_null_rates=history,
        spike_abs_threshold=float(dataset.checks.get("null_spike_abs", 0.10)),
        min_history=int(dataset.checks.get("null_min_history", 3)),
    )
    return [alert] if alert else []


# Table-level: (dataset, profile, run_id, store) -> list of alert dicts
TABLE_RULES: List[Callable[..., List[dict]]] = [
    _run_volume,
    _run_freshness,
]

# Column-level: (dataset, column_profile, run_id, store) -> list of alert dicts
COLUMN_RULES: List[Callable[..., List[dict]]] = [
    _run_null_spike,
]


def run_table_rules(
    dataset: "DatasetSpec",
    profile: dict,
    run_id: str,
    store: Any,
) -> List[dict]:
    """Run all applicable table-level rules; return list of alert payloads."""
    alerts: List[dict] = []
    for rule_fn in TABLE_RULES:
        alerts.extend(rule_fn(dataset, profile, run_id, store))
    return alerts


def run_column_rules(
    dataset: "DatasetSpec",
    column_profiles: List[dict],
    run_id: str,
    store: Any,
) -> List[dict]:
    """Run all applicable column-level rules for each column; return list of alert payloads."""
    alerts: List[dict] = []
    for cp in column_profiles:
        for rule_fn in COLUMN_RULES:
            alerts.extend(rule_fn(dataset, cp, run_id, store))
    return alerts
