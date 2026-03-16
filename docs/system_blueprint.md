# SentinelDQ System Blueprint

## 1. System Goal

SentinelDQ provides an independent observability layer that profiles
datasets, stores historical metadata, and detects data quality anomalies
over time. The system focuses on monitoring data reliability rather than
producing or transforming data.

------------------------------------------------------------------------

## 2. Execution Lifecycle

User triggers execution:

sentineldq run --config <path-to-datasets.json>

Execution Flow:

Load Configuration ↓ Initialize Run Context ↓ Discover Datasets ↓
Profile Dataset Metrics ↓ Persist Metrics ↓ Load Historical Baselines ↓
Run Detection Rules ↓ Generate Alerts ↓ Finalize Run

Each execution represents a single observability run responsible for
evaluating dataset health at a specific point in time.

------------------------------------------------------------------------

## 3. Core Modules

### Config Layer (`config.py`)

Responsible for: dataset configuration (JSON, e.g. `datasets.json`),
threshold definitions in `checks`, and dataset specs (name,
freshness_column, checks). Data source connection is currently in
`runner`; dedicated source layer is planned.

### Source Layer (`sources/`)

Currently: `get_connection(cfg)` and `prepare_demo_tables(conn, datasets)` in
`sources/__init__.py`. Config can include optional `source` (type, path,
create_demo_tables). Default is DuckDB in-memory with demo tables. Planned:
PostgreSQL, file-based datasets, extensible source integrations.

### Profiling Layer (`profiler/`)

Responsible for: row count measurement, null rate and distinct count
per column, schema fingerprint (hash), max freshness timestamp. Implemented
in `dataset_profiler.py` and `column_profiler.py`.

### Metadata Store (`metadata/store.py`)

Responsible for: run history, dataset and column profiling metrics,
alert records (SQLite). Historical baselines are read from here by
detectors.

### Detection Engine (`detect/`)

Implemented: freshness validation (`freshness.py`), volume anomaly
(`volume.py`), null-rate spike (`null_spike.py`), schema drift
(`schema_drift.py`). Planned: distribution deviation checks.

### Alert Layer (`alerts/`)

Alerts are dispatched via an `AlertSink`; default `ConsoleSink` persists
to `metadata/store` and logs. CLI commands `alerts` and `datasets` query
the store. Implement `AlertSink` for Slack, email, etc.

### API Layer (Future)

Planned: dataset health endpoints, alert querying, observability
dashboards.

------------------------------------------------------------------------

## 4. Execution call flow

One full run (e.g. `sentineldq run --config path`) follows this call chain:

```
cli.main(argv)
  → build_parser(); parse_args(argv)
  → run_once(config_path, sink=None)
       → load_config(config_path)           # config.py: AppConfig
       → os.environ["SENTINELDQ_DB"] if cfg.metadata_db_path
       → metadata_store.init_db()
       → Run.start()
       → get_connection(cfg)                  # sources: DuckDB or Postgres
       → prepare_demo_tables(con, datasets)   # if dialect=duckdb and create_demo_tables
       → for each dataset in cfg.datasets:
            profile_dataset(con, name, freshness_column, dialect)   # profiler
            profile_columns(con, name, dialect)
            metadata_store.save_column_profile(run_id, name, cp)     # each column
            metadata_store.save_profile(run_id, profile)
            run_table_rules(dataset, profile, run_id, metadata_store)   # detect/
            run_column_rules(dataset, column_profiles, run_id, metadata_store)
            for each alert_payload in table_alerts + column_alerts:
                sink.send(run_id, dataset.name, alert_payload)       # alerts (Console/File/Slack)
       → run.finalize(status); metadata_store.save_run(run)
  → return run (log run_id, status, timestamps)
```

Key entry points: `cli.main` → `runner.run_once` → `load_config`, `get_connection`, `profile_dataset` / `profile_columns`, `run_table_rules` / `run_column_rules`, `sink.send`.

------------------------------------------------------------------------

## 5. Data Flow

Dataset ↓ Profiler ↓ Metrics Store ↓ Detector ↓ Alert

The profiler extracts dataset metrics which are persisted and evaluated
against historical baselines to detect anomalies.

------------------------------------------------------------------------

## 6. Execution Boundary

SentinelDQ does NOT: - Modify datasets - Execute ETL jobs - Perform data
transformations

SentinelDQ ONLY observes datasets and evaluates their operational
health.

------------------------------------------------------------------------

## 7. MVP Scope

### Included (Phase 1)

-   Single-node execution
-   Batch dataset profiling
-   Basic anomaly detection
-   Metadata persistence

### Excluded (Initial Phase)

-   Streaming observability
-   Distributed execution
-   Complex UI systems
-   Automated remediation

------------------------------------------------------------------------

## 8. Design Principles

-   Metadata-driven execution
-   Separation from data production pipelines
-   Incremental observability
-   Explainable anomaly detection
-   Modular extensibility
-   Operational simplicity

------------------------------------------------------------------------

## 9. Future Expansion

Potential future extensions include: - Data lineage tracking -
Multi-environment monitoring - Real-time anomaly detection - Distributed
observability agents - Automated incident management
