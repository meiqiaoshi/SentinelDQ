# SentinelDQ

SentinelDQ is a lightweight data observability platform designed to
monitor data freshness, schema evolution, and data quality anomalies
across modern data pipelines.

The system continuously profiles datasets, tracks historical metadata,
detects abnormal changes, and provides centralized visibility into data
reliability and operational health.

SentinelDQ aims to serve as an independent observability layer sitting
alongside existing ETL and analytics workflows, enabling early detection
of silent data failures before they impact downstream systems.

------------------------------------------------------------------------

## ⚡ Quick Start

1. **Install** (from the project root):

   ```bash
   pip install -e .
   ```

2. **Run one observability pass** with the example config (uses in-memory
   DuckDB and creates demo tables):

   ```bash
   sentineldq run --config datasets.example.json
   ```

3. **Inspect results**:

   ```bash
   sentineldq datasets    # latest profile and alert counts per dataset
   sentineldq alerts      # recent alerts
   ```

   Optionally use `--quiet` to reduce log output, or `sentineldq --version`
   to check the version. For production, point `path` at your DuckDB file
   and set `create_demo_tables: false` (see `datasets.production.example.json`).

------------------------------------------------------------------------

## 🚀 Vision

Modern organizations depend heavily on data pipelines, yet failures in
data systems rarely occur as explicit crashes. Instead, data silently
becomes:

-   stale or delayed
-   partially missing
-   structurally inconsistent
-   statistically abnormal

These issues often remain unnoticed until dashboards break, models
degrade, or business decisions are affected.

SentinelDQ introduces a proactive observability layer that continuously
monitors dataset health over time, transforming data reliability from a
reactive debugging process into a measurable engineering discipline.

The long-term vision of SentinelDQ is to provide:

-   Continuous dataset health monitoring
-   Automated anomaly detection
-   Metadata-driven validation
-   Historical data reliability tracking
-   Centralized operational visibility for data platforms

------------------------------------------------------------------------

## 🧱 System Overview

SentinelDQ operates independently from data pipelines and focuses on
observing data rather than producing it.

Data Sources ↓ ETL / Data Pipelines ↓ Data Warehouse / Tables ↓
SentinelDQ Observability Layer ↓ Metrics • Alerts • Health Dashboard

Each SentinelDQ run performs:

1.  Dataset discovery and configuration loading
2.  Data profiling and metric extraction
3.  Metadata persistence
4.  Historical baseline comparison
5.  Anomaly detection
6.  Alert generation
7.  Observability reporting

------------------------------------------------------------------------

## 🔍 Core Capabilities

### Dataset Profiling

-   Row count monitoring
-   Column statistics collection
-   Null rate tracking
-   Cardinality measurement
-   Distribution summaries

### Data Freshness Monitoring

-   Detect delayed or stale datasets
-   Monitor update timestamps
-   Configurable freshness thresholds

### Schema Drift Detection

-   Alerts when the table schema changes compared to the previous run (hash of
    column names and types). Enable per dataset with `"schema_drift": true` in
    `checks`.

### Data Quality Anomaly Detection

-   Volume spikes or drops
-   Null value anomalies
-   Distribution shifts
-   Historical deviation detection

### Observability Metrics Store

-   Time-series dataset metrics
-   Historical profiling snapshots
-   Run metadata tracking
-   Alert history persistence

### Alert Sinks

-   Alerts are dispatched via an `AlertSink` (default: `ConsoleSink` persists to store and logs).
-   `FileSink(store, file_path)` persists to store and appends one JSON line per alert to a file (e.g. for log aggregation).
-   `SlackSink(store, webhook_url)` persists to store and posts a summary to a Slack incoming webhook (create one in your Slack app settings); use `run_once(config_path, sink=SlackSink(store, "https://hooks.slack.com/..."))` for Slack notifications. Uses stdlib only; webhook failures are logged but do not fail the run.
-   Implement the `AlertSink` protocol (`send(run_id, table_name, alert_payload) -> Optional[str]`) to add email or other channels; pass your sink into `run_once(config_path, sink=...)`.

------------------------------------------------------------------------

## ⚙️ Architecture Principles

SentinelDQ follows several data platform design principles:

-   Metadata-driven configuration
-   Decoupled from ETL execution
-   Incremental observability
-   Explainable anomaly detection
-   Operational simplicity
-   Extensible detection framework

The platform is intentionally modular to allow integration with existing
orchestration systems such as Airflow, Prefect, or cron-based jobs.

------------------------------------------------------------------------

## 📦 Project Structure

```
SentinelDQ/
├── src/sentineldq/              # main package
│   ├── config.py                # dataset configuration (JSON → DatasetSpec, AppConfig)
│   ├── models.py                # run model (e.g. Run)
│   ├── cli.py                   # CLI entry: run, alerts, datasets
│   ├── runner.py                # single-run orchestration: profile → detect → persist
│   ├── profiler/                # dataset & column profiling
│   │   ├── dataset_profiler.py  # table-level: row_count, schema_hash, max_freshness_ts
│   │   └── column_profiler.py   # column-level: null_rate, distinct_count
│   ├── detect/                  # anomaly detection rules (pluggable via registry)
│   │   ├── registry.py          # table/column rule registry; config-driven
│   │   ├── volume.py            # row-count anomaly vs. history
│   │   ├── freshness.py         # freshness threshold (max_ts)
│   │   ├── schema_drift.py      # schema hash change vs. previous run
│   │   └── null_spike.py        # null-rate spike
│   └── metadata/                # metrics persistence
│       └── store.py             # SQLite: runs, dataset_profiles, column_profiles, alerts
│   ├── alerts/                  # alert sinks (console, file, Slack; extend for email)
│   │   └── __init__.py          # AlertSink protocol, ConsoleSink, FileSink, SlackSink
│   └── sources/                 # data source connectors
│       └── __init__.py          # get_connection, prepare_demo_tables (DuckDB)
├── scripts/
│   └── run_once.py              # optional execution script
├── docs/
│   └── system_blueprint.md      # system design and execution lifecycle
├── datasets.example.json           # example dataset config (demo mode)
├── datasets.production.example.json # example for existing DuckDB tables
├── datasets.postgres.example.json   # example for PostgreSQL source
├── Dockerfile                       # optional: run SentinelDQ in containers
├── CHANGELOG.md                     # version history
├── CONTRIBUTING.md                  # how to contribute
├── pyproject.toml
└── README.md
```

------------------------------------------------------------------------

## ▶️ Execution Model

A typical SentinelDQ execution lifecycle:

Load Configuration ↓ Connect to Data Source ↓ Profile Dataset Metrics ↓
Store Observability Metadata ↓ Compare With Historical Baselines ↓
Detect Anomalies ↓ Generate Alerts

SentinelDQ is designed to run periodically as part of operational data
infrastructure.

------------------------------------------------------------------------

## 🧩 Example Use Cases

-   Detect upstream ingestion failures
-   Monitor warehouse table health
-   Prevent silent analytical data corruption
-   Validate production datasets
-   Provide operational visibility for data teams

------------------------------------------------------------------------

## 🛠️ Technology Stack

-   Python
-   SQL / DuckDB / PostgreSQL
-   FastAPI (planned)
-   Metadata-driven configuration
-   Statistical anomaly detection

------------------------------------------------------------------------

## ⚙️ Configuration

-   **Dataset config:** pass a JSON file to `sentineldq run --config <path>` (see
    `datasets.example.json`).
-   **Data source:** optional `source` in the config JSON: `type` (e.g. `duckdb` or
    `postgres`), `path` (DuckDB: `:memory:` or file path), `connection_uri` (PostgreSQL:
    optional; if omitted, use env `SENTINELDQ_PG_URI`), and `create_demo_tables` (DuckDB
    only). If you omit `source`, defaults are DuckDB in-memory with demo tables.
    **PostgreSQL:** set `type` to `postgres` and either `connection_uri` in config or
    `SENTINELDQ_PG_URI` in the environment; install with `pip install -e ".[postgres]"`.
    See `datasets.postgres.example.json`. **Using existing DuckDB tables:** point `path` at
    your DuckDB file and set `create_demo_tables` to `false`; see
    `datasets.production.example.json`.
-   **Metadata DB:** observability data (runs, profiles, alerts) is stored in a
    SQLite file. Default path is `sentineldq.db` in the current working directory.
    Override with the environment variable `SENTINELDQ_DB`, or set a top-level
    `metadata_db_path` in the dataset config (used by `sentineldq run` so one config
    can target different DBs per environment).

------------------------------------------------------------------------

## 🚀 Running in production

Run SentinelDQ on a schedule so it keeps profiling and detecting anomalies.

-   **Cron:** Run every hour (or your interval). Set `SENTINELDQ_DB` and use a
    config that points at your real data (e.g. `datasets.production.example.json`):

    ```bash
    0 * * * * SENTINELDQ_DB=/var/lib/sentineldq/sentineldq.db /usr/local/bin/sentineldq run --config /etc/sentineldq/datasets.json --quiet
    ```

-   **Airflow / Prefect:** Add a task that runs the same command (e.g. `BashOperator` or
    `subprocess.run(["sentineldq", "run", "--config", config_path, "--quiet"])`). Ensure
    the worker has the config file and write access to the metadata DB path.

-   **Docker:** From the repo root, build and run with a mounted config (and optional
    volume for the metadata DB):

    ```bash
    docker build -t sentineldq .
    docker run --rm -v /path/to/your/datasets.json:/config/datasets.json -e SENTINELDQ_DB=/data/sentineldq.db -v /path/to/data:/data sentineldq run --config /config/datasets.json --quiet
    ```

------------------------------------------------------------------------

## 🧪 Development

-   See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, tests, and how to submit changes.
-   Install with dev dependencies: `pip install -e ".[dev]"` (adds pytest).
-   Run tests: `pytest tests -v`. Tests use a temporary SQLite DB (via
    `SENTINELDQ_DB` in `tests/conftest.py`).
-   CI runs pytest on push/PR to `main` (see `.github/workflows/test.yml`).
-   Logging: runner and CLI use the standard `logging` module; adjust the
    logging level (e.g. `logging.basicConfig(level=logging.WARNING)`) to reduce output.
-   CLI: `sentineldq --version` (or `-V`) prints the version; for `run`, use
    `--verbose`/`-v` (DEBUG), `--quiet`/`-q` (WARNING only), or `--dry-run` (validate config
    only, no connect or profile). Use `alerts --json` and `datasets --json` for
    machine-readable output.

------------------------------------------------------------------------

## 🗺️ Roadmap

### Phase 1 --- Core Observability

-   Dataset profiling engine ✓
-   Metrics persistence ✓
-   Freshness monitoring ✓
-   Schema drift detection ✓

### Phase 2 --- Detection Engine

-   Historical baselines ✓ (store-backed history for volume, null-spike)
-   Statistical anomaly detection ✓ (median-based volume, null-spike, freshness)
-   Alert framework ✓ (AlertSink, ConsoleSink, FileSink, persistence, CLI)

### Phase 3 --- Platform Layer

-   Observability API
-   Health dashboard
-   Dataset lineage integration

### Phase 4 --- Production Readiness

-   Containerization ✓ (Dockerfile, run-in-production docs)
-   Distributed execution
-   Multi-source monitoring ✓ (DuckDB, PostgreSQL)

------------------------------------------------------------------------

## 🎯 Project Goal

SentinelDQ is developed as a practical exploration of modern Data
Engineering observability systems, focusing on how organizations can
build trust in data through continuous monitoring rather than manual
validation.

------------------------------------------------------------------------

## 📄 License

MIT License
