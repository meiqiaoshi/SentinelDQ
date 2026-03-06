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

-   Column additions/removals
-   Data type changes
-   Structural fingerprint comparison

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
│   ├── detect/                  # anomaly detection rules
│   │   ├── volume.py            # row-count anomaly vs. history
│   │   ├── freshness.py         # freshness threshold (max_ts)
│   │   └── null_spike.py        # null-rate spike
│   └── metadata/                # metrics persistence
│       └── store.py             # SQLite: runs, dataset_profiles, column_profiles, alerts
├── scripts/
│   └── run_once.py              # optional execution script
├── docs/
│   └── system_blueprint.md      # system design and execution lifecycle
├── datasets.example.json       # example dataset config
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
-   **Metadata DB:** observability data (runs, profiles, alerts) is stored in a
    SQLite file. Default path is `sentineldq.db` in the current working directory.
    Override with the environment variable `SENTINELDQ_DB` (e.g. for cron or CI:
    `export SENTINELDQ_DB=/var/lib/sentineldq/sentineldq.db`).

------------------------------------------------------------------------

## 🗺️ Roadmap

### Phase 1 --- Core Observability

-   Dataset profiling engine
-   Metrics persistence
-   Freshness monitoring
-   Schema drift detection

### Phase 2 --- Detection Engine

-   Historical baselines
-   Statistical anomaly detection
-   Alert framework

### Phase 3 --- Platform Layer

-   Observability API
-   Health dashboard
-   Dataset lineage integration

### Phase 4 --- Production Readiness

-   Containerization
-   Distributed execution
-   Multi-source monitoring

------------------------------------------------------------------------

## 🎯 Project Goal

SentinelDQ is developed as a practical exploration of modern Data
Engineering observability systems, focusing on how organizations can
build trust in data through continuous monitoring rather than manual
validation.

------------------------------------------------------------------------

## 📄 License

MIT License
