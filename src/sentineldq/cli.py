from __future__ import annotations

import argparse
import sys

from sentineldq.runner import run_once
from sentineldq.metadata.store import get_recent_alerts


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

    p_alerts = sub.add_parser("alerts", help="Show recent alerts")
    p_alerts.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of alerts to show",
    )

    return parser


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run":
        run = run_once(args.config)
        print("SentinelDQ run completed")
        print(f"run_id={run.run_id}")
        print(f"status={run.status}")
        print(f"started_at={run.started_at}")
        print(f"finished_at={run.finished_at}")
        return 0
    if args.command == "alerts":
        alerts = get_recent_alerts(args.limit)

        if not alerts:
            print("No alerts found.")
            return 0

        print(f"{'TIME':<24} {'SEVERITY':<8} {'RULE':<16} {'TABLE'}")

        for row in alerts:
            created_at, severity, rule_name, table_name, message = row
            print(f"{created_at:<24} {severity:<8} {rule_name:<16} {table_name}")

        return 0

    parser.print_help()
    return 2


if __name__ == "__main__":
    raise SystemExit(main())