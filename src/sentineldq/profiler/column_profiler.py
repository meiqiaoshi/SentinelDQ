from typing import Any, List, Optional

from .dataset_profiler import _parse_table_name, _quoted_table_ref, _query


def _get_columns(con: Any, table_name: str, dialect: str) -> List[tuple]:
    """Return list of (column_name, ) or (column_name, data_type)."""
    if dialect == "postgres":
        schema, table = _parse_table_name(table_name)
        sql = """
            SELECT column_name FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
        """
        rows = _query(con, dialect, sql, (schema, table))
        return [(r[0],) for r in rows]
    rows = _query(con, dialect, f"DESCRIBE {table_name}")
    return [(r[0],) for r in rows]


def _quoted_column(column_name: str, dialect: str) -> str:
    if dialect == "postgres":
        return f'"{column_name}"'
    return column_name


def profile_columns(con: Any, table_name: str, dialect: str = "duckdb") -> List[dict]:
    columns = _get_columns(con, table_name, dialect)
    table_ref = _quoted_table_ref(table_name, dialect)
    results = []

    for col in columns:
        column_name = col[0]
        col_ref = _quoted_column(column_name, dialect)

        sql = f"""
        SELECT
            COUNT(*) as total,
            COUNT({col_ref}) as non_null,
            COUNT(DISTINCT {col_ref}) as distinct_count
        FROM {table_ref}
        """
        row = _query(con, dialect, sql)[0]

        total = row[0]
        non_null = row[1]
        distinct_count = row[2]

        null_rate = 1 - (non_null / total if total else 0)

        results.append({
            "column_name": column_name,
            "null_rate": null_rate,
            "distinct_count": distinct_count
        })

    return results