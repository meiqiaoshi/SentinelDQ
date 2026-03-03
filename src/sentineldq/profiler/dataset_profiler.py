import hashlib


def compute_schema_fingerprint(con, table_name: str) -> str:
    schema_info = con.execute(f"DESCRIBE {table_name}").fetchall()
    schema_string = "|".join(f"{col[0]}:{col[1]}" for col in schema_info)
    return hashlib.md5(schema_string.encode()).hexdigest()


def profile_dataset(con, table_name: str, freshness_column: str | None = None) -> dict:
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    schema_hash = compute_schema_fingerprint(con, table_name)

    max_freshness_ts = None
    if freshness_column:
        # DuckDB returns a Python datetime for TIMESTAMP columns
        max_freshness_ts = con.execute(
            f"SELECT MAX({freshness_column}) FROM {table_name}"
        ).fetchone()[0]
        if max_freshness_ts is not None:
            max_freshness_ts = max_freshness_ts.isoformat()

    return {
        "table_name": table_name,
        "row_count": int(row_count),
        "schema_hash": schema_hash,
        "max_freshness_ts": max_freshness_ts,  # ISO string or None
    }