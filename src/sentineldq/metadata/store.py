import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import uuid
from typing import List, Optional, Any

DB_PATH = Path(os.environ.get("SENTINELDQ_DB", "sentineldq.db"))


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_connection():
    return sqlite3.connect(DB_PATH)


def init_db():
    with get_connection() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            started_at TEXT,
            finished_at TEXT,
            status TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS dataset_profiles (
            run_id TEXT,
            table_name TEXT,
            row_count INTEGER,
            schema_hash TEXT,
            max_freshness_ts TEXT,
            created_at TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS column_profiles (
            run_id TEXT,
            table_name TEXT,
            column_name TEXT,
            null_rate REAL,
            distinct_count INTEGER,
            created_at TEXT
        )
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS alerts (
            alert_id TEXT PRIMARY KEY,
            run_id TEXT,
            table_name TEXT,
            severity TEXT,
            rule_name TEXT,
            message TEXT,
            created_at TEXT
        )
        """)


def save_run(run):
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO runs (run_id, started_at, finished_at, status) VALUES (?, ?, ?, ?)",
            (
                run.run_id,
                run.started_at.isoformat(),
                run.finished_at.isoformat() if run.finished_at else None,
                run.status,
            ),
        )


def save_profile(run_id: str, profile: dict):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO dataset_profiles
            (run_id, table_name, row_count, schema_hash, max_freshness_ts, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                profile["table_name"],
                int(profile["row_count"]),
                profile["schema_hash"],
                profile.get("max_freshness_ts"),
                _utc_now_iso(),
            ),
        )


def save_column_profile(run_id: str, table_name: str, column_profile: dict):
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO column_profiles
            (run_id, table_name, column_name, null_rate, distinct_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                table_name,
                column_profile["column_name"],
                float(column_profile["null_rate"]),
                int(column_profile["distinct_count"]),
                _utc_now_iso(),
            ),
        )


def get_previous_schema_hash(
    table_name: str,
    exclude_run_id: Optional[str] = None,
) -> Optional[str]:
    """Return the most recent schema_hash for this table from a previous run, or None."""
    sql = """
    SELECT schema_hash FROM dataset_profiles
    WHERE table_name = ?
    """
    params: List[Any] = [table_name]
    if exclude_run_id:
        sql += " AND run_id != ?"
        params.append(exclude_run_id)
    sql += " ORDER BY created_at DESC LIMIT 1"
    with get_connection() as conn:
        row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def get_recent_row_counts(
    table_name: str,
    limit: int = 7,
    exclude_run_id: Optional[str] = None,
) -> List[int]:
    sql = """
    SELECT row_count
    FROM dataset_profiles
    WHERE table_name = ?
    """
    params: List[Any] = [table_name]

    if exclude_run_id:
        sql += " AND run_id != ?"
        params.append(exclude_run_id)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [int(r[0]) for r in rows]


def get_recent_null_rates(
    table_name: str,
    column_name: str,
    limit: int = 7,
    exclude_run_id: Optional[str] = None,
) -> List[float]:
    sql = """
    SELECT null_rate
    FROM column_profiles
    WHERE table_name = ? AND column_name = ?
    """
    params: List[Any] = [table_name, column_name]

    if exclude_run_id:
        sql += " AND run_id != ?"
        params.append(exclude_run_id)

    sql += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)

    with get_connection() as conn:
        rows = conn.execute(sql, params).fetchall()

    return [float(r[0]) for r in rows]


def save_alert(
    run_id: str,
    table_name: str,
    severity: str,
    rule_name: str,
    message: str,
) -> str:
    alert_id = str(uuid.uuid4())
    with get_connection() as conn:
        conn.execute(
            """
            INSERT INTO alerts (alert_id, run_id, table_name, severity, rule_name, message, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert_id,
                run_id,
                table_name,
                severity,
                rule_name,
                message,
                _utc_now_iso(),
            ),
        )
    return alert_id


def get_recent_alerts(limit: int = 10):
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT created_at, severity, rule_name, table_name, message
            FROM alerts
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return rows


def get_latest_dataset_health(limit: int = 50):
    """
    Return the latest observed state per dataset, joined with run status.

    Columns:
      table_name, created_at, status, row_count, max_freshness_ts, schema_hash
    """
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                dp.table_name,
                dp.created_at,
                r.status,
                dp.row_count,
                dp.max_freshness_ts,
                dp.schema_hash
            FROM dataset_profiles dp
            JOIN runs r ON r.run_id = dp.run_id
            JOIN (
                SELECT table_name, MAX(created_at) AS max_created_at
                FROM dataset_profiles
                GROUP BY table_name
            ) latest
              ON dp.table_name = latest.table_name
             AND dp.created_at = latest.max_created_at
            ORDER BY dp.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return rows


def get_alert_counts_by_table(hours: int = 24):
    """
    Returns dict:
      table_name -> (alerts_count, high_count)
    Time window is computed in SQLite using local 'now'.
    """
    with get_connection() as conn:
        rows = conn.execute(
            """
            SELECT
                table_name,
                COUNT(*) AS alert_cnt,
                SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) AS high_cnt
            FROM alerts
            WHERE created_at >= datetime('now', ?)
            GROUP BY table_name
            """,
            (f"-{hours} hours",),
        ).fetchall()

    return {r[0]: (int(r[1]), int(r[2] or 0)) for r in rows}