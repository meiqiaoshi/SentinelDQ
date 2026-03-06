"""Tests for metadata store (SQLite persistence)."""
import pytest
from sentineldq.metadata import store
from sentineldq.models import Run


@pytest.fixture
def db():
    """Ensure DB is initialized; tests use the temp DB from conftest."""
    store.init_db()
    return store


def test_save_run_and_profile(db):
    run = Run.start()
    run.finalize("success")
    db.save_run(run)
    db.save_profile(
        run.run_id,
        {
            "table_name": "public.t",
            "row_count": 100,
            "schema_hash": "abc",
            "max_freshness_ts": "2026-03-01T12:00:00+00:00",
        },
    )
    counts = db.get_recent_row_counts("public.t", limit=5)
    assert counts == [100]


def test_get_recent_row_counts_exclude_run_id(db):
    run1 = Run.start()
    run1.finalize("success")
    db.save_run(run1)
    db.save_profile(run1.run_id, {"table_name": "t2", "row_count": 50, "schema_hash": "x", "max_freshness_ts": None})
    run2 = Run.start()
    run2.finalize("success")
    db.save_run(run2)
    db.save_profile(run2.run_id, {"table_name": "t2", "row_count": 60, "schema_hash": "x", "max_freshness_ts": None})
    # exclude current run2 so we only see run1's 50
    counts = db.get_recent_row_counts("t2", limit=7, exclude_run_id=run2.run_id)
    assert 50 in counts
    assert counts[0] == 50  # most recent in history is run1


def test_save_alert_and_get_recent_alerts(db):
    run = Run.start()
    run.finalize("success")
    db.save_run(run)
    aid = db.save_alert(run.run_id, "public.t", "high", "volume_anomaly", "test message")
    assert aid is not None
    assert len(aid) == 36  # uuid4 hex
    rows = db.get_recent_alerts(limit=5)
    assert len(rows) >= 1
    created_at, severity, rule_name, table_name, message = rows[0]
    assert severity == "high"
    assert rule_name == "volume_anomaly"
    assert table_name == "public.t"
    assert "test message" in message


def test_get_recent_null_rates(db):
    run = Run.start()
    run.finalize("success")
    db.save_run(run)
    db.save_column_profile(
        run.run_id, "t", {"column_name": "c1", "null_rate": 0.1, "distinct_count": 10}
    )
    rates = db.get_recent_null_rates("t", "c1", limit=5)
    assert rates == [0.1]


def test_get_latest_dataset_health(db):
    run = Run.start()
    run.finalize("success")
    db.save_run(run)
    db.save_profile(
        run.run_id,
        {"table_name": "health_t", "row_count": 99, "schema_hash": "h", "max_freshness_ts": None},
    )
    rows = db.get_latest_dataset_health(limit=10)
    names = [r[0] for r in rows]
    assert "health_t" in names
