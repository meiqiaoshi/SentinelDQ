import duckdb

from .models import Run
from .config import load_config
from .profiler.dataset_profiler import profile_dataset
from .metadata.store import init_db, save_run, save_profile


def run_once(config_path: str):
    # 1) ensure metadata store ready
    init_db()

    # 2) load config and start run
    cfg = load_config(config_path)
    run = Run.start()

    con = duckdb.connect()

    # Demo: ensure tables exist so profiling succeeds (in-memory DuckDB)
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    for dataset in cfg.datasets:
        con.execute(f"CREATE OR REPLACE TABLE {dataset.name} AS SELECT * FROM range(100)")

    results = []

    # 3) profile datasets and persist each profile
    for dataset in cfg.datasets:
        try:
            profile = profile_dataset(con, dataset.name)
            results.append(profile)
            print(profile)

            # persist profile metrics for this run
            save_profile(run.run_id, profile)

        except Exception as e:
            print(f"Failed profiling {dataset.name}: {e}")
            # optional: if any dataset fails, you can mark run as degraded/failed later

    # 4) finalize run and persist run record
    run.finalize(status="success")
    save_run(run)

    return run