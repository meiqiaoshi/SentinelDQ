"""Tests for CLI (version, verbose/quiet)."""
import io
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
