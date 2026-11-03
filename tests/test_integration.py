"""End-to-end scenarios against a real Temporal server (see conftest.temporal)."""
import asyncio
import time

import pytest
from temporalio.client import WorkflowFailureError, WorkflowExecutionStatus
from temporalio.exceptions import CancelledError as TemporalCancelled
from temporalio.worker import Replayer

from business import storage
from wfengine.activities import make_idempotency_key
from wfengine.models import ExternalEvent
from wfengine.workflows import JsonWorkflow

from .conftest import fast_retry, wid
from .helpers import act, failure_of, sleep_step, start, state, step_statuses, wait_until

pytestmark = pytest.mark.integration


# ------------------------------------------------------------------ valid JSON executes
async def test_assignment_example_executes_in_dependency_order(client, queue, worker):
    w = wid("order")
    order = f"ORD-{w}"
    definition = {"workflow_id": w, "steps": [
        act("validate", "validate_order", input={"order_id": order}),
        act("payment", "process_payment", ["validate"], retry={"maximum_attempts": 3}),
        act("notification", "send_notification", ["payment"]),
    ]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    assert res["status"] == "COMPLETED"
    assert set(res["outputs"]) == {"validate", "payment", "notification"}
    assert res["outputs"]["payment"]["order_id"] == order          # upstream output flowed in implicitly
    t = {s: res["steps"][s]["finished_at"] for s in res["steps"]}
    assert t["validate"] <= t["payment"] <= t["notification"]
    assert storage.provider_charge_count(order) == 1


async def test_outputs_flow_between_steps_via_templates(client, queue, worker):
    w = wid("flow")
    definition = {"workflow_id": w, "input": {"order_id": "ORD-7", "greeting": "hi"}, "steps": [
        act("validate", "validate_order", input={"order_id": "${input.order_id}", "amount": 55}),
        act("echo", "echo", ["validate"], input={"amount_seen": "${steps.validate.output.amount}",
                                                 "msg": "${input.greeting} for ${steps.validate.output.order_id}"}),
    ]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    assert res["outputs"]["echo"]["amount_seen"] == 55.0
    assert res["outputs"]["echo"]["msg"] == "hi for ORD-7"


async def test_conditional_execution_and_skip_propagation(client, queue, worker):
    w = wid("cond")
    definition = {"workflow_id": w, "steps": [
        act("risk", "echo", input={"score": 90}),
        act("manual_review", "echo", ["risk"], condition={"step": "risk", "path": "score", "op": "gt", "value": 80}),
        act("auto_approve", "echo", ["risk"], condition={"step": "risk", "path": "score", "op": "lte", "value": 80}),
        act("after_approve", "echo", ["auto_approve"]),
        act("join", "echo", ["manual_review", "auto_approve"], trigger_rule="all_done"),
    ]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    assert step_statuses(res) == {"risk": "completed", "manual_review": "completed", "auto_approve": "skipped",
                                  "after_approve": "skipped", "join": "completed"}
    assert res["steps"]["auto_approve"]["reason"] == "condition_false"
    assert res["steps"]["after_approve"]["reason"] == "upstream_not_successful"


async def test_workflow_started_outside_the_api_is_still_validated(client, queue, worker):
    w = wid("bad")
    async with worker():
        h = await start(client, queue, {"workflow_id": w, "steps": [act("a", "echo", ["b"]), act("b", "echo", ["a"])]})
        err = await failure_of(h)
    assert err.type == "InvalidWorkflowDefinition" and err.non_retryable


# ------------------------------------------------------- retries, timeouts, failures
async def test_retry_policy_applies_then_succeeds(client, queue, worker):
    w = wid("retry")
    definition = {"workflow_id": w, "steps": [act("f", "flaky", input={"fail_times": 2}, retry=fast_retry(maximum_attempts=3))]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    assert res["outputs"]["f"]["attempts_used"] == 3
    assert storage.invocation_count(make_idempotency_key(w, "f")) == 3


async def test_retries_are_bounded_and_failure_is_reported(client, queue, worker):
    w = wid("exhaust")
    definition = {"workflow_id": w, "steps": [act("f", "always_fail", input={"code": "UPSTREAM_DOWN"}, retry=fast_retry(maximum_attempts=2))]}
    async with worker():
        err = await failure_of(await start(client, queue, definition))
    assert err.type == "WorkflowStepFailed"
    detail = err.details[0]
    assert detail["status"] == "FAILED" and detail["failure"]["step"] == "f" and detail["failure"]["type"] == "UPSTREAM_DOWN"
    assert storage.invocation_count(make_idempotency_key(w, "f")) == 2


async def test_non_retryable_business_error_is_not_retried(client, queue, worker):
    w = wid("nonretry")
    definition = {"workflow_id": w, "steps": [act("v", "validate_order", input={"order_id": "INVALID-9"}, retry=fast_retry(maximum_attempts=5))]}
    async with worker():
        err = await failure_of(await start(client, queue, definition))
    assert err.details[0]["failure"]["type"] == "INVALID_ORDER"
    assert storage.invocation_count(make_idempotency_key(w, "v")) == 1


async def test_activity_timeout_is_configurable(client, queue, worker):
    w = wid("timeout")
    definition = {"workflow_id": w, "steps": [
        act("slow", "sleep", input={"seconds": 30}, timeouts={"start_to_close_seconds": 1}, retry=fast_retry(maximum_attempts=1))]}
    async with worker():
        t0 = time.monotonic()
        err = await failure_of(await start(client, queue, definition))
    assert err.details[0]["failure"]["type"] == "Timeout" and time.monotonic() - t0 < 15


async def test_on_failure_continue_yields_partial_completion(client, queue, worker):
    w = wid("partial")
    definition = {"workflow_id": w, "steps": [
        act("optional", "always_fail", on_failure="continue", retry=fast_retry(maximum_attempts=1)),
        act("independent", "echo"),
        act("needs_optional", "echo", ["optional"]),
        act("cleanup", "echo", ["optional"], trigger_rule="all_done"),
    ]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    assert res["status"] == "COMPLETED_WITH_ERRORS"
    assert step_statuses(res) == {"optional": "failed", "independent": "completed", "needs_optional": "skipped", "cleanup": "completed"}


# ----------------------------------------------------------------------- parallelism
async def test_independent_steps_run_concurrently(client, queue, worker):
    w = wid("par")
    definition = {"workflow_id": w, "steps": [sleep_step("a", 1.5), sleep_step("b", 1.5), sleep_step("c", 1.5),
                                              act("join", "echo", ["a", "b", "c"])]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    outs = [res["outputs"][s] for s in "abc"]
    assert max(o["started_at"] for o in outs) < min(o["finished_at"] for o in outs)   # all intervals overlap
    span = max(o["finished_at"] for o in outs) - min(o["started_at"] for o in outs)   # measured inside the activities
    assert span < 3.0                                                                  # sequential would be >= 4.5


async def test_parallel_branch_failure_fail_fast_cancels_running_sibling(client, queue, worker):
    w = wid("ff")
    definition = {"workflow_id": w, "failure_policy": "fail_fast", "steps": [
        act("bad", "always_fail", input={"after_seconds": 1, "retryable": False}),
        sleep_step("slow", 60),
        act("join", "echo", ["bad", "slow"]),
    ]}
    async with worker():
        t0 = time.monotonic()
        h = await start(client, queue, definition)
        err = await failure_of(h)
        elapsed = time.monotonic() - t0
        final = await state(h)
    assert elapsed < 20                                  # did not wait for the 60s sibling
    assert err.type == "WorkflowStepFailed"
    assert step_statuses(final) == {"bad": "failed", "slow": "cancelled", "join": "cancelled"}
    assert final["steps"]["slow"]["reason"] == "sibling_failed"


async def test_parallel_branch_failure_wait_all_lets_sibling_finish(client, queue, worker):
    w = wid("wa")
    definition = {"workflow_id": w, "failure_policy": "wait_all", "steps": [
        act("bad", "always_fail", input={"after_seconds": 0.5, "retryable": False}),
        sleep_step("slow", 2),
        act("join", "echo", ["bad", "slow"]),
    ]}
    async with worker():
        err = await failure_of(await start(client, queue, definition))
    detail = err.details[0]                              # partial results preserved in the failure
    assert step_statuses(detail) == {"bad": "failed", "slow": "completed", "join": "skipped"}
    assert "slow" in detail["outputs"]


# ------------------------------------------------------------------------ cancellation
async def test_cancellation_stops_running_work(client, queue, worker):
    w = wid("cancel")
    definition = {"workflow_id": w, "steps": [sleep_step("long", 120), sleep_step("also", 120), act("after", "echo", ["long", "also"])]}
    async with worker():
        h = await start(client, queue, definition)
        await wait_until(lambda s: s["steps"].get("long", {}).get("status") == "running", h)
        await h.cancel()
        with pytest.raises(WorkflowFailureError) as ei:
            await asyncio.wait_for(h.result(), 30)
        assert isinstance(ei.value.cause, TemporalCancelled)
        final = await state(h)
    assert (await h.describe()).status == WorkflowExecutionStatus.CANCELED
    assert final["status"] == "CANCELLED"
    assert set(step_statuses(final).values()) == {"cancelled"}


# --------------------------------------------------------------------- pause / resume
async def test_pause_blocks_new_steps_and_resume_continues(client, queue, worker):
    w = wid("pause")
    definition = {"workflow_id": w, "steps": [sleep_step("a", 1), act("b", "echo", ["a"]), act("c", "echo", ["b"])]}
    async with worker():
        h = await start(client, queue, definition)
        await wait_until(lambda s: s["steps"].get("a", {}).get("status") == "running", h)
        assert await h.execute_update("pause", result_type=str) == "PAUSED"
        await asyncio.sleep(2.5)                          # a finishes meanwhile; b must not start
        paused = await state(h)
        assert paused["status"] == "PAUSED"
        assert step_statuses(paused) == {"a": "completed", "b": "pending", "c": "pending"}
        await h.execute_update("resume", result_type=str)
        res = await asyncio.wait_for(h.result(), 20)
    assert res["status"] == "COMPLETED"


# ------------------------------------------------------- signals & long-running waits
async def test_external_signal_resumes_waiting_workflow(client, queue, worker):
    w = wid("sig")
    definition = {"workflow_id": w, "steps": [
        act("prep", "echo", input={"x": 1}),
        {"id": "approval", "type": "wait_signal", "signal": "approved", "depends_on": ["prep"]},
        act("ship", "echo", ["approval"], input={"amount": "${steps.approval.output.payload.amount}"}),
    ]}
    async with worker():
        h = await start(client, queue, definition)
        waiting = await wait_until(lambda s: s["status"] == "WAITING_SIGNAL", h)
        assert waiting["steps"]["approval"]["status"] == "waiting_signal"
        await h.signal("send_event", ExternalEvent(name="approved", payload={"amount": 250}))
        res = await asyncio.wait_for(h.result(), 20)
    assert res["outputs"]["ship"]["amount"] == 250


async def test_signal_before_the_wait_is_buffered_and_duplicates_are_ignored(client, queue, worker):
    w = wid("early")
    definition = {"workflow_id": w, "steps": [
        sleep_step("work", 1.5),
        {"id": "gate", "type": "wait_signal", "signal": "go", "depends_on": ["work"]},
    ]}
    async with worker():
        h = await start(client, queue, definition)
        for _ in range(3):  # same event delivered three times (at-least-once webhook)
            await h.signal("send_event", ExternalEvent(name="go", payload="p", event_id="evt-1"))
        res = await asyncio.wait_for(h.result(), 20)
        assert res["outputs"]["gate"]["payload"] == "p"


async def test_wait_signal_timeout_fails_the_step(client, queue, worker):
    w = wid("sigto")
    definition = {"workflow_id": w, "steps": [{"id": "gate", "type": "wait_signal", "signal": "never", "timeout_seconds": 1}]}
    async with worker():
        err = await failure_of(await start(client, queue, definition))
    assert err.details[0]["failure"]["type"] == "SignalTimeout"


async def test_waiting_workflow_survives_worker_restart(client, queue, worker):
    """Challenge E / 'long-running execution: state survives worker restart'."""
    w = wid("restart")
    definition = {"workflow_id": w, "steps": [
        act("before", "echo", input={"n": 1}),
        {"id": "gate", "type": "wait_signal", "signal": "resume_me", "depends_on": ["before"]},
        act("after", "echo", ["gate"], input={"got": "${steps.gate.output.payload}"}),
    ]}
    async with worker():                                   # worker #1
        h = await start(client, queue, definition)
        await wait_until(lambda s: s["status"] == "WAITING_SIGNAL", h)
    # worker #1 is gone: nothing is polling the queue. The signal is still accepted and recorded.
    await h.signal("send_event", ExternalEvent(name="resume_me", payload="hello-after-restart"))
    assert (await h.describe()).status == WorkflowExecutionStatus.RUNNING
    async with worker():                                   # worker #2: empty cache -> rebuilds state by replaying history
        res = await asyncio.wait_for(h.result(), 20)
    assert res["outputs"]["after"]["got"] == "hello-after-restart"
    assert storage.invocation_count(make_idempotency_key(w, "before")) == 1   # not re-executed after replay


async def test_delay_step_is_a_durable_timer(client, queue, worker):
    w = wid("delay")
    definition = {"workflow_id": w, "steps": [{"id": "d", "type": "delay", "seconds": 1}, act("e", "echo", ["d"])]}
    async with worker():
        t0 = time.monotonic()
        res = await (await start(client, queue, definition)).result()
    assert res["status"] == "COMPLETED" and time.monotonic() - t0 >= 1


# ---------------------------------------------------------- determinism (Challenge F)
async def test_history_replays_deterministically(client, queue, worker):
    """Run a workflow using parallelism, conditions, signals, timers and failures, then replay its
    recorded history through the workflow code. Any nondeterminism raises here."""
    w = wid("replay")
    definition = {"workflow_id": w, "failure_policy": "wait_all", "steps": [
        act("a", "echo", input={"v": 3}),
        sleep_step("p1", 0.5, ["a"]), sleep_step("p2", 0.5, ["a"]),
        {"id": "gate", "type": "wait_signal", "signal": "go", "depends_on": ["a"]},
        {"id": "pause", "type": "delay", "seconds": 0.5, "depends_on": ["p1", "p2"]},
        act("maybe", "echo", ["a", "gate"], condition={"step": "a", "path": "v", "op": "gt", "value": 1}),
        act("never", "echo", ["a"], condition={"step": "a", "path": "v", "op": "gt", "value": 10}),
        act("opt_fail", "always_fail", ["a"], on_failure="continue", retry=fast_retry(maximum_attempts=2)),
        act("join", "echo", ["maybe", "pause", "never", "opt_fail"], trigger_rule="all_done"),
    ]}
    async with worker():
        h = await start(client, queue, definition)
        await wait_until(lambda s: s["status"] == "WAITING_SIGNAL", h)
        await h.signal("send_event", ExternalEvent(name="go", payload=1))
        res = await asyncio.wait_for(h.result(), 30)
        history = await h.fetch_history()
    assert res["status"] == "COMPLETED_WITH_ERRORS"
    await Replayer(workflows=[JsonWorkflow]).replay_workflow(history)   # raises on nondeterminism


# --------------------------------------------------- retried activity, lost result (B)
async def test_result_lost_after_external_success_in_a_real_workflow(client, queue, worker):
    w = wid("lost")
    order = f"ORD-{w}"
    definition = {"workflow_id": w, "steps": [
        act("pay", "process_payment", input={"order_id": order, "amount": 5, "_fault": "error_after_charge"},
            retry=fast_retry(maximum_attempts=3)),
        act("notify", "send_notification", ["pay"], input={"recipient": f"{w}@example.com"}),
    ]}
    async with worker():
        res = await (await start(client, queue, definition)).result()
    assert res["status"] == "COMPLETED"
    assert storage.invocation_count(make_idempotency_key(w, "pay")) == 2   # attempted twice...
    assert storage.provider_charge_count(order) == 1                       # ...charged once
