import duckdb

from .models import Run
from .config import load_config
from .profiler.dataset_profiler import profile_dataset
from .profiler.column_profiler import profile_columns
from .metadata.store import save_column_profile
from .metadata.store import (
    init_db,
    save_run,
    save_profile,
    get_recent_row_counts,
    save_alert,
)
from .detect.volume import detect_volume_anomaly
from .detect.freshness import detect_freshness_anomaly


def run_once(config_path: str):
    init_db()

    cfg = load_config(config_path)
    run = Run.start()

    con = duckdb.connect()

    # Demo: ensure tables exist so profiling succeeds (in-memory DuckDB)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")

    # Create demo tables with an updated_at column so freshness checks work
    # newest row has updated_at=now(), oldest is now()-99 minutes
    for dataset in cfg.datasets:
        con.execute(f"""
            CREATE OR REPLACE TABLE {dataset.name} AS
            SELECT
                i AS id,
                now() - (i * INTERVAL '1 minute') AS updated_at
            FROM range(100) t(i)
        """)

    results = []
    any_failed = False

    for dataset in cfg.datasets:
        try:
            # 1) profile (row_count, schema_hash, max_freshness_ts)
            profile = profile_dataset(
                con,
                dataset.name,
                freshness_column=dataset.freshness_column,
            )
            results.append(profile)
            print(profile)

            # Column profiling
            column_profiles = profile_columns(con, dataset.name)

            for cp in column_profiles:
                save_column_profile(run.run_id, dataset.name, cp)

            # 2) volume baseline & detect (exclude current run)
            history = get_recent_row_counts(
                table_name=dataset.name,
                limit=7,
                exclude_run_id=run.run_id,
            )
            volume_alert = detect_volume_anomaly(
                table_name=dataset.name,
                current_row_count=int(profile["row_count"]),
                history_row_counts=history,
                change_pct_threshold=float(dataset.checks.get("volume_change_pct", 0.30)),
                min_history=int(dataset.checks.get("volume_min_history", 3)),
            )

            # 3) persist profile
            save_profile(run.run_id, profile)

            # 4) freshness detect (only if configured)
            freshness_alert = None
            if dataset.freshness_column:
                freshness_minutes = int(dataset.checks.get("freshness_minutes", 180))
                freshness_alert = detect_freshness_anomaly(
                    table_name=dataset.name,
                    max_freshness_ts=profile.get("max_freshness_ts"),
                    threshold_minutes=freshness_minutes,
                )

            # 5) persist alerts (if any)
            for alert_payload in [volume_alert, freshness_alert]:
                if alert_payload:
                    alert_id = save_alert(
                        run_id=run.run_id,
                        table_name=dataset.name,
                        severity=alert_payload["severity"],
                        rule_name=alert_payload["rule_name"],
                        message=alert_payload["message"],
                    )
                    print(f"ALERT[{alert_payload['severity']}]: {alert_payload['message']} (alert_id={alert_id})")

        except Exception as e:
            any_failed = True
            print(f"Failed profiling {dataset.name}: {e}")

    run.finalize(status="failed" if any_failed else "success")
    save_run(run)
    return run