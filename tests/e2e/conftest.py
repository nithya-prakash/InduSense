import os
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent

if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import pytest
from redis import Redis


def redis_addr() -> tuple[str, int]:
    host = os.environ.get("REDIS_HOST", "localhost")
    port = int(os.environ.get("REDIS_PORT", "6379"))
    return host, port


@pytest.fixture(scope="session")
def redis_client():
    """Session-scoped Redis client for flushing rate-limit buckets --
    mirrors the identical fixture in tests/integration/conftest.py. This
    package logs in at least once per test against the same real-IP
    "auth" bucket that package shares, so both need the same flush to
    avoid inheriting stale counters across consecutive runs."""
    host, port = redis_addr()
    client = Redis(host=host, port=port, socket_connect_timeout=3.0)
    try:
        client.ping()
    except Exception:  # noqa: BLE001
        yield None
        return
    yield client
    client.close()
