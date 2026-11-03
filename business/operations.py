"""Demo business operations.

Idempotency strategy (see docs/ARCHITECTURE.md, "Challenge B"):

1. Every operation receives a deterministic ``idempotency_key`` = hash(workflow_id, step_id).
   It is identical on every retry / worker restart, unlike the activity attempt.
2. External calls pass that key to the provider (``Idempotency-Key`` semantics), so repeating
   the call is harmless even if our own bookkeeping was lost.
3. Our own ledger (``operation_results``) short-circuits already-finished work and makes
   concurrent duplicates agree on one result.

Crash window "provider succeeded, worker died before recording": the retry finds nothing in
the ledger, calls the provider again with the SAME key, receives the ORIGINAL charge back,
records it, and returns. One charge, no matter how many attempts.
"""
import asyncio
import os
import time
from typing import Any

from business import storage
from business.registry import BusinessError, OpContext, operation


def _find(inp: dict, key: str, default: Any = None) -> Any:
    """Look in the step input, then in outputs of upstream steps (default input shape)."""
    if inp.get(key) is not None:
        return inp[key]
    for out in (inp.get("upstream") or {}).values():
        if isinstance(out, dict) and out.get(key) is not None:
            return out[key]
    wf_in = inp.get("workflow_input") or {}
    return wf_in.get(key, default)


def _fault(ctx: OpContext, inp: dict, point: str) -> None:
    """Fault injection for demos/tests; disabled unless ENABLE_FAULT_INJECTION=1.

    Triggers only on the first attempt, so the retry succeeds. 'crash_*' kills the whole
    worker process abruptly (no cleanup, no result reported), like a node dying.
    """
    if os.environ.get("ENABLE_FAULT_INJECTION") != "1" or ctx.attempt != 1:
        return
    fault = inp.get("_fault")
    if fault == f"crash_{point}":
        os._exit(137)
    if fault == f"error_{point}":
        raise BusinessError(f"injected failure {point}", code="INJECTED_FAULT", retryable=True)


@operation("validate_order")
async def validate_order(ctx: OpContext, inp: dict) -> dict:
    order_id = _find(inp, "order_id")
    if not order_id:
        raise BusinessError("order_id is required", code="MISSING_ORDER_ID", retryable=False)
    if str(order_id).startswith("INVALID"):
        raise BusinessError(f"order {order_id} is invalid", code="INVALID_ORDER", retryable=False)
    amount = float(_find(inp, "amount", 100.0))
    return {"valid": True, "order_id": order_id, "amount": amount, "currency": "USD"}


@operation("process_payment")
async def process_payment(ctx: OpContext, inp: dict) -> dict:
    order_id = _find(inp, "order_id")
    if not order_id:
        raise BusinessError("order_id is required", code="MISSING_ORDER_ID", retryable=False)
    amount = _find(inp, "amount")

    prior = await asyncio.to_thread(storage.ledger_get, ctx.idempotency_key)
    if prior is not None:  # already finished in an earlier attempt
        return prior

    # External side effect. Safe to repeat: the provider dedupes on the idempotency key.
    charge = await asyncio.to_thread(storage.provider_charge, ctx.idempotency_key, str(order_id), amount)
    _fault(ctx, inp, "after_charge")  # <- the dangerous window: money moved, result not yet recorded

    result = {"charged": True, "charge_id": charge["charge_id"], "order_id": order_id, "amount": amount}
    return await asyncio.to_thread(storage.ledger_record, ctx.idempotency_key, "process_payment", result)


@operation("send_notification")
async def send_notification(ctx: OpContext, inp: dict) -> dict:
    recipient = str(_find(inp, "recipient", "customer@example.com"))
    body = str(_find(inp, "message", f"Order {_find(inp, 'order_id', '?')} processed"))
    sent = await asyncio.to_thread(storage.send_notification_once, ctx.idempotency_key, recipient, body)
    return {"sent": True, "deduplicated": not sent, "recipient": recipient}


@operation("reserve_inventory")
async def reserve_inventory(ctx: OpContext, inp: dict) -> dict:
    prior = await asyncio.to_thread(storage.ledger_get, ctx.idempotency_key)
    if prior is not None:
        return prior
    result = {"reserved": True, "sku": _find(inp, "sku", "SKU-1"), "quantity": int(_find(inp, "quantity", 1))}
    return await asyncio.to_thread(storage.ledger_record, ctx.idempotency_key, "reserve_inventory", result)


# ------------------------------------------------------------- utility / test ops
@operation("echo")
async def echo(ctx: OpContext, inp: dict) -> dict:
    return {k: v for k, v in inp.items() if k not in ("upstream", "workflow_input", "_fault")} | {"echoed": True}


@operation("sleep")
async def sleep(ctx: OpContext, inp: dict) -> dict:
    """Heartbeating sleep: stands in for slow work. Reports wall-clock times to prove overlap."""
    seconds = float(inp.get("seconds", 1))
    started = time.time()
    deadline = started + seconds
    while (remaining := deadline - time.time()) > 0:
        ctx.heartbeat("sleeping")
        await asyncio.sleep(min(0.25, remaining))
    return {"started_at": started, "finished_at": time.time(), "slept": seconds, "label": inp.get("label")}


@operation("flaky")
async def flaky(ctx: OpContext, inp: dict) -> dict:
    """Fails (retryably) until attempt number > fail_times."""
    fail_times = int(inp.get("fail_times", 2))
    if ctx.attempt <= fail_times:
        raise BusinessError(f"transient failure on attempt {ctx.attempt}", code="TRANSIENT", retryable=True)
    return {"attempts_used": ctx.attempt}


@operation("always_fail")
async def always_fail(ctx: OpContext, inp: dict) -> dict:
    after = float(inp.get("after_seconds", 0))
    if after:
        await asyncio.sleep(after)
    raise BusinessError(str(inp.get("message", "this operation always fails")),
                        code=str(inp.get("code", "ALWAYS_FAIL")), retryable=bool(inp.get("retryable", True)))

