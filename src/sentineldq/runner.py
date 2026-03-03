import duckdb

from .models import Run
from .config import load_config
from .profiler.dataset_profiler import profile_dataset
from .metadata.store import (
    init_db,
    save_run,
    save_profile,
    get_recent_row_counts,
    save_alert,
)
from .detect.volume import detect_volume_anomaly


def run_once(config_path: str):
    init_db()

    cfg = load_config(config_path)
    run = Run.start()

    con = duckdb.connect()

    # Demo: ensure tables exist so profiling succeeds (in-memory DuckDB)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    for dataset in cfg.datasets:
        con.execute(f"CREATE OR REPLACE TABLE {dataset.name} AS SELECT * FROM range(100)")

    results = []
    any_failed = False

    for dataset in cfg.datasets:
        try:
            # 1) profile
            profile = profile_dataset(con, dataset.name)
            results.append(profile)
            print(profile)

            # 2) baseline (exclude current run_id so we compare to previous runs only)
            history = get_recent_row_counts(
                table_name=dataset.name,
                limit=7,
                exclude_run_id=run.run_id,
            )

            # 3) detect
            alert_payload = detect_volume_anomaly(
                table_name=dataset.name,
                current_row_count=int(profile["row_count"]),
                history_row_counts=history,
                change_pct_threshold=0.30,  # TODO: later read from config
                min_history=3,
            )

            # 4) persist profile
            save_profile(run.run_id, profile)

            # 5) persist alert (if any)
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