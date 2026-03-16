"""
CLI entry point: run (one observability pass), alerts (recent alerts), datasets
(latest health summary). Parses args and delegates to runner / metadata store.
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from importlib.metadata import version as _pkg_version

from sentineldq.config import load_config
from sentineldq.runner import run_once
from sentineldq.metadata.store import (
    get_recent_alerts,
    get_latest_dataset_health,
    get_alert_counts_by_table,
)

logger = logging.getLogger(__name__)


def _get_version() -> str:
    try:
        return _pkg_version("sentineldq")
    except Exception:
        return "0.1.0"


def build_parser() -> argparse.ArgumentParser:
    """Build the sentineldq argument parser (run, alerts, datasets subcommands)."""
    parser = argparse.ArgumentParser(
        prog="sentineldq",
        description="SentinelDQ - data observability CLI",
    )
    parser.add_argument(
        "--version",
        "-V",
        action="version",
        version=f"%(prog)s {_get_version()}",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # sentineldq run --config <path>
    p_run = sub.add_parser("run", help="Run one observability execution")
    p_run.add_argument(
        "--config",
        required=True,
        help="Path to dataset config (json for now)",
    )
    p_run.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Use DEBUG log level for this run",
    )
    p_run.add_argument(
        "--quiet",
        "-q",
        action="store_true",
        help="Use WARNING log level (only errors and warnings)",
    )
    p_run.add_argument(
        "--dry-run",
        action="store_true",
        help="Only load and validate config; do not connect or profile",
    )

    # sentineldq alerts --limit N [--json]
    p_alerts = sub.add_parser("alerts", help="Show recent alerts")
    p_alerts.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of alerts to show",
    )
    p_alerts.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
    )

    # sentineldq datasets --limit N [--json]
    p_ds = sub.add_parser("datasets", help="Show latest dataset health summary")
    p_ds.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Number of datasets to show",
    )
    p_ds.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Lookback window (hours) for alert counts",
    )
    p_ds.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    """Parse CLI, dispatch to run / alerts / datasets; return exit code."""
    argv = argv if argv is not None else sys.argv[1:]
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        if getattr(args, "verbose", False):
            logging.getLogger().setLevel(logging.DEBUG)
        elif getattr(args, "quiet", False):
            logging.getLogger().setLevel(logging.WARNING)
        if getattr(args, "dry_run", False):
            load_config(args.config)
            logger.info("Config valid.")
            return 0
        run = run_once(args.config)
        logger.info("SentinelDQ run completed")
        logger.info("run_id=%s", run.run_id)
        logger.info("status=%s", run.status)
        logger.info("started_at=%s", run.started_at)
        logger.info("finished_at=%s", run.finished_at)
        return 0

    if args.command == "alerts":
        rows = get_recent_alerts(args.limit)

        if getattr(args, "json", False):
            out = [
                {
                    "created_at": created_at,
                    "severity": severity,
                    "rule_name": rule_name,
                    "table_name": table_name,
                    "message": message,
                }
                for created_at, severity, rule_name, table_name, message in rows
            ]
            print(json.dumps(out, indent=2))
            return 0

        if not rows:
            logger.info("No alerts found.")
            return 0

        logger.info("%s", f"{'TIME':<24} {'SEVERITY':<8} {'RULE':<18} {'TABLE'}")
        for created_at, severity, rule_name, table_name, message in rows:
            logger.info("%s", f"{created_at:<24} {severity:<8} {rule_name:<18} {table_name}")
        return 0

    if args.command == "datasets":
        rows = get_latest_dataset_health(args.limit)
        counts = get_alert_counts_by_table(hours=getattr(args, "hours", 24))

        if getattr(args, "json", False):
            out = []
            for table_name, created_at, status, row_count, max_ts, schema_hash in rows:
                alert_cnt, high_cnt = counts.get(table_name, (0, 0))
                out.append({
                    "dataset": table_name,
                    "last_seen": created_at,
                    "status": status,
                    "rows": row_count,
                    "max_ts": max_ts or None,
                    "alerts": alert_cnt,
                    "high": high_cnt,
                })
            print(json.dumps(out, indent=2))
            return 0

        if not rows:
            logger.info("No dataset profiles found.")
            logger.info("Tip: run `sentineldq run --config <path>` first.")
            return 0

        logger.info(
            "%s",
            f"{'DATASET':<22} {'LAST_SEEN':<24} {'STATUS':<8} {'ROWS':<8} "
            f"{'MAX_TS':<24} {'ALERTS':<8} {'HIGH'}",
        )

        for table_name, created_at, status, row_count, max_ts, schema_hash in rows:
            max_ts_display = max_ts if max_ts else "-"
            alert_cnt, high_cnt = counts.get(table_name, (0, 0))
            logger.info(
                "%s",
                f"{table_name:<22} {created_at:<24} {status:<8} {row_count:<8} "
                f"{max_ts_display:<24} {alert_cnt:<8} {high_cnt}",
            )
        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())