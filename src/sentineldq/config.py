import json
from dataclasses import dataclass
from typing import List


@dataclass
class DatasetSpec:
    name: str


@dataclass
class AppConfig:
    datasets: List[DatasetSpec]


def load_config(path: str) -> AppConfig:
    raw = json.load(open(path))
    datasets = [DatasetSpec(**d) for d in raw.get("datasets", [])]
    return AppConfig(datasets=datasets)