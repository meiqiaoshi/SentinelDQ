from .models import Run
from .config import load_config
from .sources import get_connection, prepare_demo_tables
from .profiler.dataset_profiler import profile_dataset
from .profiler.column_profiler import profile_columns
from .metadata import store as metadata_store
from .detect.registry import run_table_rules, run_column_rules


def run_once(config_path: str):
    metadata_store.init_db()

    cfg = load_config(config_path)
    run = Run.start()

    con = get_connection(cfg)
    if cfg.source.create_demo_tables:
        prepare_demo_tables(con, cfg.datasets)

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

            # Column profiling and column-level rules
            column_profiles = profile_columns(con, dataset.name)
            for cp in column_profiles:
                metadata_store.save_column_profile(run.run_id, dataset.name, cp)

            # Persist profile then run all detection rules (table + column)
            metadata_store.save_profile(run.run_id, profile)

            table_alerts = run_table_rules(dataset, profile, run.run_id, metadata_store)
            column_alerts = run_column_rules(dataset, column_profiles, run.run_id, metadata_store)

            for alert_payload in table_alerts + column_alerts:
                alert_id = metadata_store.save_alert(
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
    metadata_store.save_run(run)
    return run