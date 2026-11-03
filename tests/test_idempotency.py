"""Challenge B at the activity level, using Temporal's ActivityEnvironment (no server needed).

Simulates exactly what Temporal does after a crash: the SAME activity is invoked again with
attempt=2 under the same workflow id.
"""
import dataclasses
import uuid

import pytest
from temporalio.exceptions import ApplicationError
from temporalio.testing import ActivityEnvironment

from business import storage
from wfengine.activities import execute_operation, make_idempotency_key
from wfengine.models import StepActivityInput


def run_activity(workflow_id: str, step_id: str, operation: str, inp: dict, attempt: int = 1):
    env = ActivityEnvironment()
    env.info = dataclasses.replace(env.info, workflow_id=workflow_id, attempt=attempt, activity_id=step_id)
    return env.run(execute_operation, StepActivityInput(step_id=step_id, operation=operation, input=inp))


def test_key_is_stable_per_step_and_distinct_across_steps():
    assert make_idempotency_key("wf", "pay") == make_idempotency_key("wf", "pay")
    assert make_idempotency_key("wf", "pay") != make_idempotency_key("wf", "notify")
    assert make_idempotency_key("wf", "pay") != make_idempotency_key("wf2", "pay")


async def test_payment_runs_once_across_retries():
    wf, order = f"wf-{uuid.uuid4().hex[:8]}", f"ORD-{uuid.uuid4().hex[:6]}"
    first = await run_activity(wf, "pay", "process_payment", {"order_id": order, "amount": 42}, attempt=1)
    again = await run_activity(wf, "pay", "process_payment", {"order_id": order, "amount": 42}, attempt=2)
    assert first == again
    assert storage.provider_charge_count(order) == 1


async def test_result_lost_after_external_success_does_not_double_charge():
    """Provider call succeeded, then the attempt died before the result was recorded."""
    wf, order = f"wf-{uuid.uuid4().hex[:8]}", f"ORD-{uuid.uuid4().hex[:6]}"
    with pytest.raises(ApplicationError) as ei:
        await run_activity(wf, "pay", "process_payment", {"order_id": order, "amount": 10, "_fault": "error_after_charge"}, attempt=1)
    assert ei.value.type == "INJECTED_FAULT" and not ei.value.non_retryable
    assert storage.provider_charge_count(order) == 1                         # money already moved...
    assert storage.ledger_get(make_idempotency_key(wf, "pay")) is None       # ...but nothing recorded locally

    retry = await run_activity(wf, "pay", "process_payment", {"order_id": order, "amount": 10, "_fault": "error_after_charge"}, attempt=2)
    assert retry["charged"] is True
    assert storage.provider_charge_count(order) == 1                         # still exactly one charge
    assert storage.ledger_get(make_idempotency_key(wf, "pay")) == retry      # now recorded


async def test_notification_is_deduplicated():
    wf, rcpt = f"wf-{uuid.uuid4().hex[:8]}", f"{uuid.uuid4().hex[:6]}@example.com"
    a = await run_activity(wf, "n", "send_notification", {"recipient": rcpt}, attempt=1)
    b = await run_activity(wf, "n", "send_notification", {"recipient": rcpt}, attempt=2)
    assert a["deduplicated"] is False and b["deduplicated"] is True
    assert storage.notification_count(rcpt) == 1


async def test_business_errors_map_to_retryability():
    with pytest.raises(ApplicationError) as ei:
        await run_activity("wf-x", "v", "validate_order", {"order_id": "INVALID-1"})
    assert ei.value.non_retryable and ei.value.type == "INVALID_ORDER"
    with pytest.raises(ApplicationError) as ei:
        await run_activity("wf-x", "f", "flaky", {"fail_times": 3}, attempt=1)
    assert not ei.value.non_retryable
    assert (await run_activity("wf-x", "f", "flaky", {"fail_times": 3}, attempt=4))["attempts_used"] == 4


async def test_unknown_operation_is_non_retryable():
    with pytest.raises(ApplicationError) as ei:
        await run_activity("wf-x", "u", "does_not_exist", {})
    assert ei.value.non_retryable and ei.value.type == "UNKNOWN_OPERATION"


async def test_fault_injection_is_off_without_the_env_flag(monkeypatch):
    monkeypatch.delenv("ENABLE_FAULT_INJECTION")
    wf, order = f"wf-{uuid.uuid4().hex[:8]}", f"ORD-{uuid.uuid4().hex[:6]}"
    res = await run_activity(wf, "pay", "process_payment", {"order_id": order, "_fault": "crash_after_charge"}, attempt=1)
    assert res["charged"] is True  # would have killed the test process if the guard were missing
