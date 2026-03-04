def profile_columns(con, table_name: str):
    columns = con.execute(f"DESCRIBE {table_name}").fetchall()

    results = []

    for col in columns:
        column_name = col[0]

        row = con.execute(f"""
        SELECT
            COUNT(*) as total,
            COUNT({column_name}) as non_null,
            COUNT(DISTINCT {column_name}) as distinct_count
        FROM {table_name}
        """).fetchone()

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