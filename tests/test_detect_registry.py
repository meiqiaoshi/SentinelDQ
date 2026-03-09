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


def test_run_table_rules_schema_drift_alert_when_hash_changed(db):
    from sentineldq.models import Run
    run_old = Run.start()
    run_old.finalize("success")
    db.save_run(run_old)
    db.save_profile(
        run_old.run_id,
        {"table_name": "drift_t", "row_count": 10, "schema_hash": "old_hash", "max_freshness_ts": None},
    )
    dataset = DatasetSpec(name="drift_t", checks={"schema_drift": True})
    profile = {"table_name": "drift_t", "row_count": 10, "schema_hash": "new_hash", "max_freshness_ts": None}
    alerts = run_table_rules(dataset, profile, "run_new", db)
    assert len(alerts) >= 1
    schema_alerts = [a for a in alerts if a.get("rule_name") == "schema_drift"]
    assert len(schema_alerts) == 1
    assert schema_alerts[0]["details"]["previous_schema_hash"] == "old_hash"
    assert schema_alerts[0]["details"]["current_schema_hash"] == "new_hash"
