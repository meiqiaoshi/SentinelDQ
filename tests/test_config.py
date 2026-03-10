"""Tests for config loading and validation."""
import json
import tempfile
from pathlib import Path

import pytest
from sentineldq.config import load_config, ConfigError, _validate_config


def test_load_valid_config():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(
            {
                "datasets": [{"name": "public.t", "freshness_column": "updated_at", "checks": {}}],
            },
            f,
        )
        path = f.name
    try:
        cfg = load_config(path)
        assert len(cfg.datasets) == 1
        assert cfg.datasets[0].name == "public.t"
        assert cfg.datasets[0].freshness_column == "updated_at"
        assert cfg.datasets[0].checks == {}
        assert cfg.source.type == "duckdb"
    finally:
        Path(path).unlink(missing_ok=True)


def test_validate_config_missing_datasets():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({})
    assert "datasets" in str(exc_info.value).lower()


def test_validate_config_datasets_not_list():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({"datasets": "not a list"})
    assert "list" in str(exc_info.value).lower()


def test_validate_config_dataset_missing_name():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({"datasets": [{"freshness_column": "ts"}]})
    assert "name" in str(exc_info.value).lower()


def test_validate_config_dataset_name_empty():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({"datasets": [{"name": ""}]})
    assert "name" in str(exc_info.value).lower()


def test_validate_config_checks_not_object():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({"datasets": [{"name": "t", "checks": "not a dict"}]})
    assert "checks" in str(exc_info.value).lower()


def test_validate_config_source_not_object():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({"datasets": [], "source": "not an object"})
    assert "source" in str(exc_info.value).lower()


def test_load_config_with_metadata_db_path():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(
            {"datasets": [{"name": "t"}], "metadata_db_path": "/var/lib/sentineldq/db.sqlite"},
            f,
        )
        path = f.name
    try:
        cfg = load_config(path)
        assert cfg.metadata_db_path == "/var/lib/sentineldq/db.sqlite"
    finally:
        Path(path).unlink(missing_ok=True)


def test_validate_config_metadata_db_path_must_be_non_empty_string():
    with pytest.raises(ConfigError) as exc_info:
        _validate_config({"datasets": [], "metadata_db_path": ""})
    assert "metadata_db_path" in str(exc_info.value).lower()


def test_load_config_invalid_structure_raises_config_error():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"not_datasets": []}, f)
        path = f.name
    try:
        with pytest.raises(ConfigError) as exc_info:
            load_config(path)
        assert "datasets" in str(exc_info.value).lower()
    finally:
        Path(path).unlink(missing_ok=True)
