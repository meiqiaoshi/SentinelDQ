# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- `SlackSink` for Slack webhook notifications (persist + POST; stdlib only).
- CLI: `--dry-run` for `run` (validate config only); `--json` for `alerts` and `datasets`.

### Changed

- Docs: Python 3.10+ requirement in README Quick Start; CHANGELOG release link and project structure (CHANGELOG.md, CONTRIBUTING.md) in README.

------------------------------------------------------------------------

## [0.1.0] - Unreleased

### Added

- Dataset and column profiling (row count, schema hash, null rate, distinct count, max freshness timestamp).
- Metadata store (SQLite) for runs, dataset profiles, column profiles, and alerts.
- Pluggable detection rules: volume anomaly, freshness, null-rate spike, schema drift (config-driven registry).
- Alert sink abstraction: `ConsoleSink`, `FileSink`; implement `AlertSink` for Slack/email.
- Data sources: DuckDB (in-memory or file), PostgreSQL (optional `.[postgres]`); configurable via `source` and `SENTINELDQ_PG_URI` env.
- CLI: `sentineldq run`, `alerts`, `datasets`; `--version`, `--verbose`/`--quiet` for run.
- Config validation (JSON) and optional `metadata_db_path`, `connection_uri` (or env).
- Quick start, run-in-production docs (cron, Airflow, Docker), optional Dockerfile.
- Unit and e2e tests; GitHub Actions CI (pytest on Python 3.10–3.12).
- CONTRIBUTING.md and README roadmap aligned with current features.

[Unreleased]: https://github.com/meiqiaoshi/SentinelDQ/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/meiqiaoshi/SentinelDQ/releases/tag/v0.1.0
