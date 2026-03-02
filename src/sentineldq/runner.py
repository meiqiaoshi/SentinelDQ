from .models import Run
from .config import load_config


def run_once(config_path: str):
    cfg = load_config(config_path)

    run = Run.start()

    # just stub for now
    print(f"Running SentinelDQ with {len(cfg.datasets)} datasets")

    run.finalize(status="success")
    return run