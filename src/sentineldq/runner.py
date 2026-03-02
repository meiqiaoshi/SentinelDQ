import duckdb
from .models import Run
from .config import load_config
from .profiler.dataset_profiler import profile_dataset


def run_once(config_path: str):
    cfg = load_config(config_path)
    run = Run.start()

    con = duckdb.connect()

    # Demo: ensure tables exist so profiling succeeds (in-memory DuckDB)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    for dataset in cfg.datasets:
        con.execute(
            f"CREATE OR REPLACE TABLE {dataset.name} AS SELECT * FROM range(100)"
        )

    results = []

    for dataset in cfg.datasets:
        try:
            profile = profile_dataset(con, dataset.name)
            results.append(profile)
            print(profile)
        except Exception as e:
            print(f"Failed profiling {dataset.name}: {e}")

    run.finalize(status="success")
    return run