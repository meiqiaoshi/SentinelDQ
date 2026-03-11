import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


class ConfigError(ValueError):
    """Raised when dataset config is invalid."""


def _validate_config(raw: dict) -> None:
    """Validate raw config structure; raise ConfigError with a clear message if invalid."""
    if not isinstance(raw, dict):
        raise ConfigError("Config must be a JSON object")

    datasets_raw = raw.get("datasets")
    if datasets_raw is None:
        raise ConfigError("Config must have a 'datasets' key")
    if not isinstance(datasets_raw, list):
        raise ConfigError("'datasets' must be a list")

    for i, d in enumerate(datasets_raw):
        if not isinstance(d, dict):
            raise ConfigError(f"datasets[{i}] must be an object")
        if "name" not in d:
            raise ConfigError(f"datasets[{i}] must have a 'name' key")
        name = d["name"]
        if not isinstance(name, str) or not name.strip():
            raise ConfigError(f"datasets[{i}].name must be a non-empty string")
        if "checks" in d and d["checks"] is not None and not isinstance(d["checks"], dict):
            raise ConfigError(f"datasets[{i}].checks must be an object or omitted")
        if "freshness_column" in d and d["freshness_column"] is not None:
            if not isinstance(d["freshness_column"], str):
                raise ConfigError(f"datasets[{i}].freshness_column must be a string or null")

    if "source" in raw:
        src = raw["source"]
        if not isinstance(src, dict):
            raise ConfigError("'source' must be an object")
        if "type" in src and not isinstance(src["type"], str):
            raise ConfigError("source.type must be a string")
        if "path" in src and not isinstance(src["path"], str):
            raise ConfigError("source.path must be a string")
        if "create_demo_tables" in src and not isinstance(src["create_demo_tables"], bool):
            raise ConfigError("source.create_demo_tables must be a boolean")
        if src.get("type") == "postgres":
            if not src.get("connection_uri") or not isinstance(src["connection_uri"], str) or not str(src["connection_uri"]).strip():
                raise ConfigError("source.connection_uri is required when source.type is 'postgres' and must be a non-empty string")
        if "connection_uri" in src and src["connection_uri"] is not None:
            if not isinstance(src["connection_uri"], str) or not src["connection_uri"].strip():
                raise ConfigError("source.connection_uri must be a non-empty string or omitted")

    if "metadata_db_path" in raw and raw["metadata_db_path"] is not None:
        if not isinstance(raw["metadata_db_path"], str) or not raw["metadata_db_path"].strip():
            raise ConfigError("metadata_db_path must be a non-empty string or omitted")


@dataclass
class DatasetSpec:
    name: str
    freshness_column: Optional[str] = None
    checks: Dict[str, Any] = None


@dataclass
class SourceSpec:
    """Data source for profiling. Default: in-memory DuckDB with demo tables."""

    type: str = "duckdb"
    path: str = ":memory:"
    create_demo_tables: bool = True
    connection_uri: Optional[str] = None  # required when type == "postgres"


@dataclass
class AppConfig:
    datasets: List[DatasetSpec]
    source: SourceSpec
    metadata_db_path: Optional[str] = None


def load_config(path: str) -> AppConfig:
    with open(path) as f:
        raw = json.load(f)
    _validate_config(raw)
    datasets = [DatasetSpec(**d) for d in raw.get("datasets", [])]
    for ds in datasets:
        if ds.checks is None:
            ds.checks = {}

    source = None
    if "source" in raw:
        src_raw = dict(raw["source"])
        if "connection_uri" not in src_raw:
            src_raw["connection_uri"] = None
        source = SourceSpec(**src_raw)
    else:
        source = SourceSpec()  # default: duckdb :memory:, create_demo_tables=True

    metadata_db_path = raw.get("metadata_db_path")
    if isinstance(metadata_db_path, str) and metadata_db_path.strip():
        metadata_db_path = metadata_db_path.strip()
    else:
        metadata_db_path = None

    return AppConfig(datasets=datasets, source=source, metadata_db_path=metadata_db_path)