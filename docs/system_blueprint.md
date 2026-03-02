# SentinelDQ System Blueprint

## 1. System Goal

SentinelDQ provides an independent observability layer that profiles
datasets, stores historical metadata, and detects data quality anomalies
over time. The system focuses on monitoring data reliability rather than
producing or transforming data.

------------------------------------------------------------------------

## 2. Execution Lifecycle

User triggers execution:

sentineldq run

Execution Flow:

Load Configuration ↓ Initialize Run Context ↓ Discover Datasets ↓
Profile Dataset Metrics ↓ Persist Metrics ↓ Load Historical Baselines ↓
Run Detection Rules ↓ Generate Alerts ↓ Finalize Run

Each execution represents a single observability run responsible for
evaluating dataset health at a specific point in time.

------------------------------------------------------------------------

## 3. Core Modules

### Config Layer

Responsible for: - Dataset configuration (datasets.yaml) - Threshold
definitions - Data source configuration

### Source Layer

Responsible for: - Database connectors (PostgreSQL, DuckDB) - File-based
datasets - Future extensible source integrations

### Profiling Layer

Responsible for: - Row count measurement - Null rate calculation -
Statistical summaries - Schema fingerprint generation

### Metadata Store

Responsible for: - Run history persistence - Dataset profiling metrics -
Alert records - Historical observability tracking

### Detection Engine

Responsible for: - Freshness validation - Volume anomaly detection -
Schema drift detection - Distribution deviation checks

### Alert Layer

Responsible for: - Console alerts - Notification abstraction - Future
Slack/email integrations

### API Layer (Future)

Responsible for: - Dataset health endpoints - Alert querying -
Observability dashboards

------------------------------------------------------------------------

## 4. Data Flow

Dataset ↓ Profiler ↓ Metrics Store ↓ Detector ↓ Alert

The profiler extracts dataset metrics which are persisted and evaluated
against historical baselines to detect anomalies.

------------------------------------------------------------------------

## 5. Execution Boundary

SentinelDQ does NOT: - Modify datasets - Execute ETL jobs - Perform data
transformations

SentinelDQ ONLY observes datasets and evaluates their operational
health.

------------------------------------------------------------------------

## 6. MVP Scope

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

## 7. Design Principles

-   Metadata-driven execution
-   Separation from data production pipelines
-   Incremental observability
-   Explainable anomaly detection
-   Modular extensibility
-   Operational simplicity

------------------------------------------------------------------------

## 8. Future Expansion

Potential future extensions include: - Data lineage tracking -
Multi-environment monitoring - Real-time anomaly detection - Distributed
observability agents - Automated incident management
