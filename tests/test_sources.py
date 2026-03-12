"""Tests for data source connectors."""
import os

import pytest
from sentineldq.config import AppConfig, DatasetSpec, SourceSpec
from sentineldq.sources import get_connection

try:
    import psycopg2  # noqa: F401
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False


@pytest.mark.skipif(not HAS_PSYCOPG2, reason="psycopg2 not installed")
def test_get_connection_postgres_requires_uri_or_env():
    """When type is postgres and connection_uri is missing, get_connection uses SENTINELDQ_PG_URI or raises ValueError."""
    cfg = AppConfig(
        datasets=[DatasetSpec(name="public.t")],
        source=SourceSpec(type="postgres", connection_uri=None, create_demo_tables=False),
    )
    env_key = "SENTINELDQ_PG_URI"
    old = os.environ.pop(env_key, None)
    try:
        with pytest.raises(ValueError) as exc_info:
            get_connection(cfg)
        assert "SENTINELDQ_PG_URI" in str(exc_info.value) or "connection_uri" in str(exc_info.value).lower()
    finally:
        if old is not None:
            os.environ[env_key] = old
