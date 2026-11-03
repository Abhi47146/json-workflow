"""The single generic activity. It is the ONLY place where Temporal meets business code.

Activities are where all non-determinism lives: I/O, clocks, randomness, external calls.
This adapter (a) derives the idempotency key, (b) heartbeats so a dead worker is detected
quickly and cancellation is delivered, (c) translates business errors into Temporal
``ApplicationError`` with the right retryability.
"""
import asyncio
import hashlib
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

from business import BusinessError, OpContext, invoke
from wfengine.models import StepActivityInput

HEARTBEAT_EVERY_SECONDS = 2.0


def make_idempotency_key(workflow_id: str, step_id: str) -> str:
    """Stable per (workflow_id, step). Deliberately NOT based on run_id or attempt:
    a retry, a worker crash, or a re-run of a failed workflow under the same business id must
    all map to the same external operation."""
    return hashlib.sha256(f"{workflow_id}\x1f{step_id}".encode()).hexdigest()[:40]


async def _heartbeat_loop() -> None:
    while True:
        activity.heartbeat()
        await asyncio.sleep(HEARTBEAT_EVERY_SECONDS)


@activity.defn(name="execute_operation")
async def execute_operation(inp: StepActivityInput) -> Any:
    info = activity.info()
    ctx = OpContext(
        idempotency_key=make_idempotency_key(info.workflow_id, inp.step_id),
        workflow_id=info.workflow_id,
        step_id=inp.step_id,
        attempt=info.attempt,
        heartbeat=activity.heartbeat,
    )
    activity.logger.info("operation started", extra={"operation": inp.operation, "step_id": inp.step_id,
                                                      "idempotency_key": ctx.idempotency_key})
    hb = asyncio.create_task(_heartbeat_loop())
    try:
        result = await invoke(inp.operation, ctx, inp.input)
    except BusinessError as e:
        activity.logger.warning("operation failed", extra={"operation": inp.operation, "code": e.code,
                                                             "retryable": e.retryable, "error": str(e)})
        raise ApplicationError(str(e), type=e.code, non_retryable=not e.retryable) from e
    finally:
        hb.cancel()
    activity.logger.info("operation finished", extra={"operation": inp.operation, "step_id": inp.step_id})
    return result if isinstance(result, dict) else {"value": result}
