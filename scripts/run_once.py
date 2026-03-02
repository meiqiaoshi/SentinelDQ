import argparse
from sentineldq.runner import run_once


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()

    run = run_once(args.config)

    print("Run completed")
    print(f"run_id={run.run_id}")
    print(f"status={run.status}")
    print(f"started_at={run.started_at}")
    print(f"finished_at={run.finished_at}")


if __name__ == "__main__":
    main()