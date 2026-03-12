"""Tests for CLI (version, verbose/quiet, --json)."""
import json
import sys

import pytest

from sentineldq.cli import build_parser, main, _get_version


def test_get_version():
    v = _get_version()
    assert isinstance(v, str)
    assert len(v) >= 5 and "0" in v


def test_cli_version_exits_zero():
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])
    assert exc_info.value.code == 0


def test_cli_run_parser_accepts_verbose_quiet():
    parser = build_parser()
    args = parser.parse_args(["run", "--config", "x.json", "--quiet"])
    assert args.quiet is True
    args = parser.parse_args(["run", "--config", "x.json", "--verbose"])
    assert args.verbose is True


def test_cli_alerts_json_output_valid():
    """alerts --json prints a JSON array to stdout (empty if no alerts)."""
    old_stdout = sys.stdout
    try:
        buf = []
        class Writer:
            def write(self, s):
                buf.append(s)
            def flush(self):
                pass
        sys.stdout = Writer()
        main(["alerts", "--json", "--limit", "5"])
        out = "".join(buf)
        data = json.loads(out)
        assert isinstance(data, list)
        if data:
            assert "created_at" in data[0] and "severity" in data[0] and "table_name" in data[0]
    finally:
        sys.stdout = old_stdout


def test_cli_datasets_json_output_valid():
    """datasets --json prints a JSON array to stdout (empty if no profiles)."""
    old_stdout = sys.stdout
    try:
        buf = []
        class Writer:
            def write(self, s):
                buf.append(s)
            def flush(self):
                pass
        sys.stdout = Writer()
        main(["datasets", "--json", "--limit", "5"])
        out = "".join(buf)
        data = json.loads(out)
        assert isinstance(data, list)
        if data:
            assert "dataset" in data[0] and "rows" in data[0] and "status" in data[0]
    finally:
        sys.stdout = old_stdout


def test_cli_parser_accepts_json_for_alerts_and_datasets():
    parser = build_parser()
    args = parser.parse_args(["alerts", "--json", "--limit", "3"])
    assert args.json is True
    args = parser.parse_args(["datasets", "--json", "--limit", "2", "--hours", "48"])
    assert args.json is True
