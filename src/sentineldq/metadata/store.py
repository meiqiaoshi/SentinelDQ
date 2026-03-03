import sqlite3
from pathlib import Path
from datetime import datetime


DB_PATH = Path("sentineldq.db")


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
            created_at TEXT
        )
        """)


def save_run(run):
    with get_connection() as conn:
        conn.execute(
            "INSERT INTO runs VALUES (?, ?, ?, ?)",
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
            "INSERT INTO dataset_profiles VALUES (?, ?, ?, ?, ?)",
            (
                run_id,
                profile["table_name"],
                profile["row_count"],
                profile["schema_hash"],
                datetime.utcnow().isoformat(),
            ),
        )