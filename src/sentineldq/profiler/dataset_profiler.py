import hashlib
from typing import Any, Optional, Tuple


def _parse_table_name(table_name: str) -> Tuple[str, str]:
    """Return (schema, table). If no dot, schema is 'public'."""
    if "." in table_name:
        parts = table_name.split(".", 1)
        return (parts[0].strip(), parts[1].strip())
    return ("public", table_name.strip())


def _quoted_table_ref(table_name: str, dialect: str) -> str:
    if dialect == "postgres":
        schema, table = _parse_table_name(table_name)
        return f'"{schema}"."{table}"'
    return table_name


def _query(con: Any, dialect: str, sql: str, params: Optional[tuple] = None):
    """Run query and return rows. dialect 'duckdb' | 'postgres'."""
    if dialect == "duckdb":
        return con.execute(sql).fetchall()
    cur = con.cursor()
    try:
        cur.execute(sql, params or ())
        return cur.fetchall()
    finally:
        cur.close()


def compute_schema_fingerprint(con: Any, table_name: str, dialect: str = "duckdb") -> str:
    if dialect == "postgres":
        schema, table = _parse_table_name(table_name)
        sql = """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        schema_info = _query(con, dialect, sql, (schema, table))
    else:
        schema_info = _query(con, dialect, f"DESCRIBE {table_name}")
    schema_string = "|".join(f"{col[0]}:{col[1]}" for col in schema_info)
    return hashlib.md5(schema_string.encode()).hexdigest()


def profile_dataset(
    con: Any,
    table_name: str,
    freshness_column: Optional[str] = None,
    dialect: str = "duckdb",
) -> dict:
    table_ref = _quoted_table_ref(table_name, dialect)
    row_count = _query(con, dialect, f"SELECT COUNT(*) FROM {table_ref}")[0][0]
    schema_hash = compute_schema_fingerprint(con, table_name, dialect)

    max_freshness_ts = None
    if freshness_column:
        col_ref = f'"{freshness_column}"' if dialect == "postgres" else freshness_column
        r = _query(con, dialect, f"SELECT MAX({col_ref}) FROM {table_ref}")[0][0]
        if r is not None:
            max_freshness_ts = r.isoformat() if hasattr(r, "isoformat") else str(r)

    return {
        "table_name": table_name,
        "row_count": int(row_count),
        "schema_hash": schema_hash,
        "max_freshness_ts": max_freshness_ts,
    }