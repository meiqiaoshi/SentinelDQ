# Minimal image to run SentinelDQ (e.g. in cron or Airflow workers).
# Build from repo root: docker build -t sentineldq .
# Run: mount config and set SENTINELDQ_DB; use --config and optionally --quiet.

FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml .
COPY src ./src

RUN pip install --no-cache-dir -e .

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["sentineldq"]
CMD ["--help"]
