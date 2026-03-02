import duckdb
import hashlib


def compute_schema_fingerprint(con, table_name: str) -> str:
    schema_info = con.execute(f"DESCRIBE {table_name}").fetchall()
    schema_string = "|".join(f"{col[0]}:{col[1]}" for col in schema_info)
    return hashlib.md5(schema_string.encode()).hexdigest()


def profile_dataset(con, table_name: str) -> dict:
    row_count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    schema_hash = compute_schema_fingerprint(con, table_name)

    return {
        "table_name": table_name,
        "row_count": row_count,
        "schema_hash": schema_hash,
    }