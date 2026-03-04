import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import uuid
from typing import List, Optional

DB_PATH = Path("sentineldq.db")


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
                column_profile["null_rate"],
                column_profile["distinct_count"],
                _utc_now_iso(),
            ),
        )


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
    params = [table_name]
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
    """
    Return recent null_rate history for a column, newest -> oldest.
    """
    sql = """
    SELECT null_rate
    FROM column_profiles
    WHERE table_name = ? AND column_name = ?
    """
    params = [table_name, column_name]

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