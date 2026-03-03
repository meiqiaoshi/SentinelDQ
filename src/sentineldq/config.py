import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class DatasetSpec:
    name: str
    freshness_column: Optional[str] = None
    checks: Dict[str, Any] = None


@dataclass
class AppConfig:
    datasets: List[DatasetSpec]


def load_config(path: str) -> AppConfig:
    raw = json.load(open(path))
    datasets = [DatasetSpec(**d) for d in raw.get("datasets", [])]
    # normalize checks
    for ds in datasets:
        if ds.checks is None:
            ds.checks = {}
    return AppConfig(datasets=datasets)