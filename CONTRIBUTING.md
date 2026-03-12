# Contributing to SentinelDQ

Thanks for your interest in contributing. This document covers how to get set up and submit changes.

## Setup

1. Clone the repository and enter the project directory.
2. Use Python 3.10 or newer; a virtual environment is recommended:
   ```bash
   python -m venv .venv
   source .venv/bin/activate   # or .venv\Scripts\activate on Windows
   ```
3. Install the package in editable mode with dev dependencies:
   ```bash
   pip install -e ".[dev]"
   ```
   For PostgreSQL source support (optional):
   ```bash
   pip install -e ".[dev,postgres]"
   ```

## Running tests

From the project root:

```bash
pytest tests -v
```

Tests use a temporary SQLite database (see `tests/conftest.py`). CI runs the same suite on push and pull requests to `main` (`.github/workflows/test.yml`).

## Code style

- Follow the existing style in the codebase (PEP 8–style, type hints where present).
- New detection rules belong in `sentineldq/detect/` and should be registered in `registry.py`.
- New alert sinks implement the `AlertSink` protocol in `sentineldq/alerts/`.

## Submitting changes

1. Create a branch from `main` for your change.
2. Make your edits and run `pytest tests -v` to ensure nothing is broken.
3. Open a pull request against `main` with a clear description of the change.
4. Prefer conventional commit–style messages for the final squash/merge (e.g. `feat: add X`, `docs: update Y`, `fix: Z`).

If you have questions or ideas, open an issue to discuss.
