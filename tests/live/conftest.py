"""Live tests against the Databricks workspace. Opt in with LOOM_LIVE=1 (they create and delete rows)."""
import os

import pytest

if os.environ.get("LOOM_LIVE") != "1":
    collect_ignore_glob = ["test_*.py"]


@pytest.fixture(scope="session")
def live():
    from Loom.wiring import build
    svc, ingestor, raw = build()
    return svc, ingestor, raw
