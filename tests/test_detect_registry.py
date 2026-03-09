"""Tests for pluggable detection registry."""
import pytest
from sentineldq.config import DatasetSpec
from sentineldq.detect.registry import run_table_rules, run_column_rules
from sentineldq.metadata import store as metadata_store


@pytest.fixture
def db():
    metadata_store.init_db()
    return metadata_store


def test_run_table_rules_returns_list(db):
    dataset = DatasetSpec(
        name="public.t",
        freshness_column="updated_at",
        checks={"volume_change_pct": 0.3, "freshness_minutes": 60},
    )
    profile = {
        "table_name": "public.t",
        "row_count": 100,
        "schema_hash": "x",
        "max_freshness_ts": None,
    }
    alerts = run_table_rules(dataset, profile, "run-1", db)
    assert isinstance(alerts, list)
    for a in alerts:
        assert "severity" in a and "rule_name" in a and "message" in a


def test_run_column_rules_returns_list(db):
    dataset = DatasetSpec(name="public.t", checks={"null_spike_abs": 0.10})
    column_profiles = [
        {"column_name": "c1", "null_rate": 0.05, "distinct_count": 10},
    ]
    alerts = run_column_rules(dataset, column_profiles, "run-1", db)
    assert isinstance(alerts, list)
    for a in alerts:
        assert "severity" in a and "rule_name" in a and "message" in a


def test_run_table_rules_volume_applicable_when_checks_present(db):
    dataset = DatasetSpec(name="t", checks={"volume_change_pct": 0.5})
    profile = {"table_name": "t", "row_count": 50, "schema_hash": "x", "max_freshness_ts": None}
    # No history -> volume rule returns []; freshness not run (no freshness_column)
    alerts = run_table_rules(dataset, profile, "run-1", db)
    assert isinstance(alerts, list)
