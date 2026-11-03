"""Submission API against a real Temporal server: duplicate submission, status, cancel."""
import asyncio
import json

import httpx
import pytest

from business import storage
from wfengine.activities import make_idempotency_key
from wfengine.api import create_app
from wfengine.config import Settings

from .conftest import wid
from .helpers import act, sleep_step

pytestmark = pytest.mark.integration


@pytest.fixture
async def http(client, queue):
    app = create_app(client=client, settings=Settings(task_queue=queue))
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t", timeout=30) as c:
        yield c


async def poll_status(http, w, want, timeout=30):
    for _ in range(int(timeout / 0.3)):
        doc = (await http.get(f"/workflows/{w}")).json()
        if doc.get("temporal_status") == want or doc.get("status") == want:
            return doc
        await asyncio.sleep(0.3)
    raise AssertionError(f"never reached {want}: {doc}")


async def test_duplicate_submission_executes_business_logic_once(http, worker):
    w = wid("dup")
    order = f"ORD-{w}"
    definition = {"workflow_id": w, "steps": [
        act("pay", "process_payment", input={"order_id": order, "amount": 9}),
        sleep_step("hold", 1, ["pay"]),
    ]}
    body = json.dumps(definition)
    async with worker():
        # five simultaneous submissions of the same definition
        responses = await asyncio.gather(*[http.post("/workflows", content=body) for _ in range(5)])
        assert {r.status_code for r in responses} <= {200, 202}
        assert sum(1 for r in responses if r.json()["duplicate"] is False) == 1
        assert len({r.json()["run_id"] for r in responses}) == 1
        await poll_status(http, w, "COMPLETED")

        again = await http.post("/workflows", content=body)         # resubmit after completion
        assert again.status_code == 200 and again.json()["duplicate"] is True

        changed = dict(definition, steps=definition["steps"][:1])   # same id, different definition
        conflict = await http.post("/workflows", content=json.dumps(changed))
        assert conflict.status_code == 409 and conflict.json()["error"]["code"] == "WORKFLOW_ID_CONFLICT"

    assert storage.invocation_count(make_idempotency_key(w, "pay")) == 1
    assert storage.provider_charge_count(order) == 1


async def test_status_and_cancel_endpoints(http, worker):
    w = wid("api")
    definition = {"workflow_id": w, "steps": [sleep_step("long", 120)]}
    async with worker():
        assert (await http.post("/workflows", content=json.dumps(definition))).status_code == 202
        running = await poll_status(http, w, "RUNNING")
        assert running["state_source"] == "query"
        for _ in range(50):
            if (await http.get(f"/workflows/{w}")).json()["steps"]["long"]["status"] == "running":
                break
            await asyncio.sleep(0.2)
        assert (await http.post(f"/workflows/{w}/pause")).json()["status"] == "PAUSED"
        assert (await http.post(f"/workflows/{w}/resume")).status_code == 200
        assert (await http.post(f"/workflows/{w}/cancel")).status_code == 202
        final = await poll_status(http, w, "CANCELED")
        assert final["steps"]["long"]["status"] == "cancelled"
    assert (await http.get("/workflows/does-not-exist")).status_code == 404


async def test_signal_endpoint_resumes_workflow(http, worker):
    w = wid("apisig")
    definition = {"workflow_id": w, "steps": [{"id": "gate", "type": "wait_signal", "signal": "ok"}]}
    async with worker():
        await http.post("/workflows", content=json.dumps(definition))
        await poll_status(http, w, "WAITING_SIGNAL")
        r = await http.post(f"/workflows/{w}/signal/ok", content=json.dumps({"payload": {"by": "alice"}, "event_id": "e1"}))
        assert r.status_code == 202
        done = await poll_status(http, w, "COMPLETED")
        assert done["outputs"]["gate"]["payload"] == {"by": "alice"}
