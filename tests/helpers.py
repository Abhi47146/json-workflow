import asyncio
import time
from datetime import timedelta
from typing import Callable, Optional

from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.exceptions import ApplicationError

from wfengine.workflows import JsonWorkflow


def act(id, name, deps=(), **kw) -> dict:
    return {"id": id, "type": "activity", "name": name, "depends_on": list(deps), **kw}


def sleep_step(id, seconds, deps=(), **kw) -> dict:
    return act(id, "sleep", deps, input={"seconds": seconds, "label": id}, **kw)


async def start(client: Client, queue: str, definition: dict) -> WorkflowHandle:
    return await client.start_workflow(JsonWorkflow.run, definition, id=definition["workflow_id"], task_queue=queue,
                                       execution_timeout=timedelta(minutes=3))


async def state(handle: WorkflowHandle) -> dict:
    return await handle.query("get_status", result_type=dict)


async def wait_until(predicate: Callable[[dict], bool], handle: WorkflowHandle, timeout: float = 30.0, interval: float = 0.2) -> dict:
    deadline = time.monotonic() + timeout
    last: Optional[dict] = None
    while time.monotonic() < deadline:
        try:
            last = await state(handle)
            if predicate(last):
                return last
        except Exception:  # noqa: BLE001 - query can fail while a worker is (re)starting
            pass
        await asyncio.sleep(interval)
    raise AssertionError(f"condition not met within {timeout}s; last state: {last}")


async def failure_of(handle: WorkflowHandle) -> ApplicationError:
    try:
        await handle.result()
    except WorkflowFailureError as e:
        assert isinstance(e.cause, ApplicationError), repr(e.cause)
        return e.cause
    raise AssertionError("workflow was expected to fail")


def step_statuses(doc: dict) -> dict:
    return {sid: s["status"] for sid, s in doc["steps"].items()}
