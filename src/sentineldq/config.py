import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


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


@dataclass
class AppConfig:
    datasets: List[DatasetSpec]
    source: SourceSpec


def load_config(path: str) -> AppConfig:
    raw = json.load(open(path))
    datasets = [DatasetSpec(**d) for d in raw.get("datasets", [])]
    for ds in datasets:
        if ds.checks is None:
            ds.checks = {}

    source = None
    if "source" in raw:
        source = SourceSpec(**raw["source"])
    else:
        source = SourceSpec()  # default: duckdb :memory:, create_demo_tables=True

    return AppConfig(datasets=datasets, source=source)