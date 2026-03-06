"""Pytest config: use a temp DB for metadata store tests so we don't touch sentineldq.db."""
import os
import tempfile

_tmpdir = tempfile.mkdtemp(prefix="sentineldq_test_")
os.environ["SENTINELDQ_DB"] = os.path.join(_tmpdir, "sentineldq.db")
