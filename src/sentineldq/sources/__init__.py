"""Data source connectors. Returns a connection for profiling; optionally prepares demo tables."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ..config import AppConfig, DatasetSpec


def get_connection(cfg: "AppConfig") -> Any:
    """Return a database connection for the configured source (DuckDB or PostgreSQL)."""
    if cfg.source.type == "duckdb":
        import duckdb
        return duckdb.connect(cfg.source.path)
    if cfg.source.type == "postgres":
        try:
            import psycopg2
        except ImportError:
            raise ImportError(
                "PostgreSQL source requires psycopg2. Install with: pip install -e '.[postgres]'"
            ) from None
        uri = (cfg.source.connection_uri or "").strip()
        if not uri:
            raise ValueError("source.connection_uri is required when source.type is 'postgres'")
        return psycopg2.connect(uri)
    raise ValueError(f"Unsupported source type: {cfg.source.type}. Use 'duckdb' or 'postgres'.")


def prepare_demo_tables(conn: Any, datasets: list["DatasetSpec"]) -> None:
    """Create in-DB demo tables so profiling runs without an external warehouse. Idempotent. DuckDB only."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    for dataset in datasets:
        conn.execute(f"""
            CREATE OR REPLACE TABLE {dataset.name} AS
            SELECT
                i AS id,
                now() - (i * INTERVAL '1 minute') AS updated_at
            FROM range(100) t(i)
        """)
