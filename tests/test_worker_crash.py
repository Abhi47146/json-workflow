"""Challenge A: a REAL worker process is killed mid-activity (os._exit, no cleanup, no result
reported). A second worker process picks the workflow up. Verifies:
  * completed steps are not re-executed (Temporal replays history, it does not re-run activities)
  * the interrupted step is retried and, thanks to the idempotency key, charges exactly once
"""
import asyncio
import os
import subprocess
import sys
from pathlib import Path

import pytest

from business import storage
from wfengine.activities import make_idempotency_key

from .conftest import fast_retry, wid
from .helpers import act, start

pytestmark = [pytest.mark.integration, pytest.mark.crash]
ROOT = Path(__file__).resolve().parent.parent


def spawn_worker(address: str, queue: str) -> subprocess.Popen:
    env = {**os.environ, "TEMPORAL_ADDRESS": address, "TASK_QUEUE": queue, "LOG_LEVEL": "INFO",
           "GRACEFUL_SHUTDOWN_SECONDS": "2", "PYTHONPATH": str(ROOT)}
    return subprocess.Popen([sys.executable, "-m", "wfengine.worker"], cwd=ROOT, env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


async def test_worker_crash_mid_workflow_recovers_without_repeating_completed_work(client, address, queue):
    w = wid("crash")
    order = f"ORD-{w}"
    definition = {"workflow_id": w, "steps": [
        act("validate", "validate_order", input={"order_id": order}),
        act("pay", "process_payment", ["validate"],
            input={"order_id": "${steps.validate.output.order_id}", "amount": 20, "_fault": "crash_after_charge"},
            timeouts={"start_to_close_seconds": 30, "heartbeat_seconds": 3}, retry=fast_retry(maximum_attempts=3)),
        act("notify", "send_notification", ["pay"], input={"recipient": f"{w}@example.com"}),
    ]}

    worker_a = spawn_worker(address, queue)
    try:
        handle = await start(client, queue, definition)
        rc = await asyncio.wait_for(asyncio.to_thread(worker_a.wait), 60)   # it dies by itself, right after the charge
        assert rc == 137, f"worker should have crashed, exit code {rc}"
    finally:
        if worker_a.poll() is None:
            worker_a.kill()

    # State at the moment of the crash: money moved, nothing recorded, workflow still RUNNING.
    assert storage.provider_charge_count(order) == 1
    assert storage.ledger_get(make_idempotency_key(w, "pay")) is None
    assert (await handle.describe()).status.name == "RUNNING"

    worker_b = spawn_worker(address, queue)                               # "restart": brand-new process, empty cache
    try:
        res = await asyncio.wait_for(handle.result(), 90)
    finally:
        worker_b.terminate()
        worker_b.wait(timeout=20)

    assert res["status"] == "COMPLETED"
    assert storage.invocation_count(make_idempotency_key(w, "validate")) == 1   # completed before the crash: NOT repeated
    assert storage.invocation_count(make_idempotency_key(w, "pay")) == 2        # interrupted attempt + retry
    assert storage.provider_charge_count(order) == 1                            # but only ONE charge
    assert storage.notification_count(f"{w}@example.com") == 1
