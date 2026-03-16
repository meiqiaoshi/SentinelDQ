"""
Single-run orchestration: load config, connect to source, profile each dataset,
run detection rules, and dispatch alerts via the configured sink.
"""
import logging
import os
from typing import Optional

from .models import Run
from .config import load_config
from .sources import get_connection, prepare_demo_tables
from .profiler.dataset_profiler import profile_dataset
from .profiler.column_profiler import profile_columns
from .metadata import store as metadata_store
from .detect.registry import run_table_rules, run_column_rules
from .alerts import AlertSink, ConsoleSink

logger = logging.getLogger(__name__)


def run_once(config_path: str, sink: Optional[AlertSink] = None):
    """
    Run one observability pass: profile all configured datasets, run table/column
    rules, persist metrics and send alerts. Returns the Run with status and timestamps.
    """
    if sink is None:
        sink = ConsoleSink(metadata_store)
    cfg = load_config(config_path)
    if cfg.metadata_db_path:
        os.environ["SENTINELDQ_DB"] = cfg.metadata_db_path
    metadata_store.init_db()

    run = Run.start()

    con = get_connection(cfg)
    dialect = cfg.source.type
    if dialect == "duckdb" and cfg.source.create_demo_tables:
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
                dialect=dialect,
            )
            results.append(profile)
            logger.info("Profile: %s", profile)

            # Column profiling and column-level rules
            column_profiles = profile_columns(con, dataset.name, dialect=dialect)
            for cp in column_profiles:
                metadata_store.save_column_profile(run.run_id, dataset.name, cp)

            # Persist profile then run all detection rules (table + column)
            metadata_store.save_profile(run.run_id, profile)

            table_alerts = run_table_rules(dataset, profile, run.run_id, metadata_store)
            column_alerts = run_column_rules(dataset, column_profiles, run.run_id, metadata_store)

            for alert_payload in table_alerts + column_alerts:
                sink.send(run.run_id, dataset.name, alert_payload)

        except Exception as e:
            any_failed = True
            logger.exception("Failed profiling %s: %s", dataset.name, e)

    run.finalize(status="failed" if any_failed else "success")
    metadata_store.save_run(run)
    return run