import os
import sys
import tempfile
import uuid
from contextlib import asynccontextmanager
from datetime import timedelta
from pathlib import Path

import pytest
import pytest_asyncio

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Isolated business DB + fault injection for the whole test session (inherited by subprocess workers).
_TMP = tempfile.mkdtemp(prefix="wfengine-tests-")
os.environ["BUSINESS_DB_PATH"] = os.path.join(_TMP, "business.db")
os.environ["ENABLE_FAULT_INJECTION"] = "1"

from temporalio.client import Client  # noqa: E402
from temporalio.testing import WorkflowEnvironment  # noqa: E402
from temporalio.worker import Worker  # noqa: E402

from wfengine.activities import execute_operation  # noqa: E402
from wfengine.workflows import JsonWorkflow  # noqa: E402


@pytest_asyncio.fixture(scope="session")
async def temporal():
    """A Temporal server for integration tests.

    TEMPORAL_TEST_ADDRESS=host:port uses an existing server (e.g. docker compose's);
    otherwise the SDK downloads/starts the Temporal CLI dev server (needs internet once).
    """
    addr = os.environ.get("TEMPORAL_TEST_ADDRESS")
    if addr:
        yield {"client": await Client.connect(addr), "address": addr}
        return
    try:
        env = await WorkflowEnvironment.start_local()
    except Exception as e:  # noqa: BLE001
        pytest.skip(f"cannot start a local Temporal server ({e}); set TEMPORAL_TEST_ADDRESS to use an existing one")
    try:
        yield {"client": env.client, "address": env.client.service_client.config.target_host}
    finally:
        await env.shutdown()


@pytest.fixture
def client(temporal) -> Client:
    return temporal["client"]


@pytest.fixture
def address(temporal) -> str:
    return temporal["address"]


@pytest.fixture
def queue() -> str:
    return f"tq-{uuid.uuid4().hex[:10]}"


@asynccontextmanager
async def running_worker(client: Client, queue: str, **kw):
    async with Worker(client, task_queue=queue, workflows=[JsonWorkflow], activities=[execute_operation],
                      graceful_shutdown_timeout=timedelta(seconds=1),
                      max_heartbeat_throttle_interval=timedelta(seconds=2),
                      default_heartbeat_throttle_interval=timedelta(seconds=2), **kw):
        yield


@pytest.fixture
def worker(client, queue):
    return lambda **kw: running_worker(client, queue, **kw)


def wid(prefix: str = "wf") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10]}"


def fast_retry(**over) -> dict:
    return {"initial_interval_seconds": 0.1, "backoff_coefficient": 1.0, **over}
