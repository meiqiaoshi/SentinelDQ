from __future__ import annotations

import argparse
import logging
import sys

from sentineldq.runner import run_once
from sentineldq.metadata.store import (
    get_recent_alerts,
    get_latest_dataset_health,
    get_alert_counts_by_table,
)

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="sentineldq",
        description="SentinelDQ - data observability CLI",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # sentineldq run --config <path>
    p_run = sub.add_parser("run", help="Run one observability execution")
    p_run.add_argument(
        "--config",
        required=True,
        help="Path to dataset config (json for now)",
    )

    # sentineldq alerts --limit N
    p_alerts = sub.add_parser("alerts", help="Show recent alerts")
    p_alerts.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of alerts to show",
    )

    # sentineldq datasets --limit N
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

    return parser


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        run = run_once(args.config)
        logger.info("SentinelDQ run completed")
        logger.info("run_id=%s", run.run_id)
        logger.info("status=%s", run.status)
        logger.info("started_at=%s", run.started_at)
        logger.info("finished_at=%s", run.finished_at)
        return 0

    if args.command == "alerts":
        rows = get_recent_alerts(args.limit)

        if not rows:
            logger.info("No alerts found.")
            return 0

        logger.info("%s", f"{'TIME':<24} {'SEVERITY':<8} {'RULE':<18} {'TABLE'}")
        for created_at, severity, rule_name, table_name, message in rows:
            logger.info("%s", f"{created_at:<24} {severity:<8} {rule_name:<18} {table_name}")
        return 0

    if args.command == "datasets":
        rows = get_latest_dataset_health(args.limit)

        if not rows:
            logger.info("No dataset profiles found.")
            logger.info("Tip: run `sentineldq run --config <path>` first.")
            return 0

        counts = get_alert_counts_by_table(hours=args.hours)

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