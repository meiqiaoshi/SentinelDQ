"""Data source connectors. Returns a connection for profiling; optionally prepares demo tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import AppConfig, DatasetSpec


def get_connection(cfg: "AppConfig") -> Any:
    """Return a database connection for the configured source. Currently only DuckDB is supported."""
    if cfg.source.type != "duckdb":
        raise ValueError(f"Unsupported source type: {cfg.source.type}. Use 'duckdb'.")
    import duckdb
    return duckdb.connect(cfg.source.path)


def prepare_demo_tables(conn: Any, datasets: list["DatasetSpec"]) -> None:
    """Create in-DB demo tables so profiling runs without an external warehouse. Idempotent."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    for dataset in datasets:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {dataset.name} AS
            SELECT
                i AS id,
                now() - (i * INTERVAL '1 minute') AS updated_at
            FROM range(100) t(i)
        """)
