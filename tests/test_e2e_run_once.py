"""End-to-end test: run_once with temp config and temp DB, assert profiles and store state."""
import json
import tempfile
from pathlib import Path

import pytest

from sentineldq.runner import run_once
from sentineldq.metadata import store as metadata_store


@pytest.fixture
def temp_config():
    """Minimal config: one dataset, DuckDB in-memory with demo tables. No metadata_db_path so conftest's temp DB is used."""
    config = {
        "source": {
            "type": "duckdb",
            "path": ":memory:",
            "create_demo_tables": True,
        },
        "datasets": [
            {
                "name": "public.e2e_table",
                "freshness_column": "updated_at",
                "checks": {"volume_change_pct": 0.5, "volume_min_history": 1},
            },
        ],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        path = f.name
    yield path
    Path(path).unlink(missing_ok=True)


def test_run_once_produces_profile_and_persists_to_store(temp_config):
    run = run_once(temp_config)

    assert run.run_id
    assert run.status in ("success", "failed")
    assert run.started_at is not None
    assert run.finished_at is not None

    rows = metadata_store.get_latest_dataset_health(limit=10)
    assert len(rows) >= 1
    table_names = [r[0] for r in rows]
    assert "public.e2e_table" in table_names

    # Profile for our table: row_count, schema_hash should be present
    for r in rows:
        if r[0] == "public.e2e_table":
            _, _, status, row_count, max_ts, schema_hash = r
            assert row_count == 100  # demo table has 100 rows
            assert schema_hash is not None and len(schema_hash) > 0
            break
    else:
        pytest.fail("public.e2e_table not found in get_latest_dataset_health")

    # Run was persisted
    with metadata_store.get_connection() as conn:
        cur = conn.execute(
            "SELECT run_id, status FROM runs WHERE run_id = ?", (run.run_id,)
        )
        row = cur.fetchone()
    assert row is not None
    assert row[1] == run.status


def test_run_once_empty_datasets_completes_successfully():
    """Edge case: config with empty datasets list still runs and finalizes with success."""
    config = {
        "source": {"type": "duckdb", "path": ":memory:", "create_demo_tables": False},
        "datasets": [],
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(config, f)
        path = f.name
    try:
        run = run_once(path)
        assert run.run_id is not None
        assert run.status == "success"
        assert run.started_at is not None
        assert run.finished_at is not None
    finally:
        Path(path).unlink(missing_ok=True)
