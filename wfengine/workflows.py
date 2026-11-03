"""The interpreter workflow: one generic, deterministic Temporal workflow that executes any
validated JSON definition. The definition is *data* passed as the workflow argument, so it is
recorded in event history: running executions keep the definition they started with even if a
newer definition (or newer interpreter code) is deployed later.

Determinism rules followed here (Challenge F)
  * Only Temporal APIs are used for time (``workflow.now``, ``workflow.sleep``), concurrency
    (asyncio primitives run on Temporal's deterministic event loop) and waiting
    (``workflow.wait_condition``).
  * No I/O, no randomness, no ``datetime.now``, no environment reads, no global mutable state.
  * Iteration order is fixed by the validator's stable topological order, never by set/dict
    hash order.
  * Condition evaluation and template resolution are pure functions over recorded results.
  * Everything with side effects is the ``execute_operation`` activity.
"""
import asyncio
from datetime import timedelta
from typing import Any, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError, CancelledError as TemporalCancelledError
from temporalio.exceptions import TimeoutError as TemporalTimeoutError
from temporalio.workflow import ActivityCancellationType

with workflow.unsafe.imports_passed_through():
    from wfengine.dag import TemplateError, evaluate_condition, resolve_templates
    from wfengine.models import ExternalEvent, StepActivityInput
    from wfengine.validation import DefinitionValidationError, Step, WorkflowSpec, validate_definition

TERMINAL = ("completed", "failed", "skipped", "cancelled")


def _secs(v: Optional[float]) -> Optional[timedelta]:
    return None if v is None else timedelta(seconds=v)


def _describe_error(e: BaseException) -> dict:
    cause = e.cause if isinstance(e, ActivityError) else e
    if isinstance(cause, ApplicationError):
        return {"type": cause.type or "ApplicationError", "message": cause.message, "non_retryable": cause.non_retryable}
    if isinstance(cause, TemporalTimeoutError):
        return {"type": "Timeout", "message": str(cause), "non_retryable": False}
    return {"type": type(cause).__name__, "message": str(cause), "non_retryable": False}


@workflow.defn(name="JsonWorkflow")
class JsonWorkflow:
    def __init__(self) -> None:
        self._spec: Optional[WorkflowSpec] = None
        self._steps: dict = {}
        self._tasks: dict = {}
        self._events: dict = {}  # event name -> buffered payloads (signal may arrive before the wait)
        self._seen_event_ids: set = set()
        self._paused = False
        self._aborting = False
        self._final: Optional[str] = None
        self._fatal: Optional[dict] = None
        self._started_at: Optional[str] = None

    # ------------------------------------------------------------------ main entry
    @workflow.run
    async def run(self, definition: dict) -> dict:
        try:
            spec = validate_definition(definition)
        except DefinitionValidationError as e:
            # Defence in depth: the API validates first, but anything can start a workflow.
            self._final = "FAILED"
            raise ApplicationError(str(e), {"issues": e.to_list()}, type="InvalidWorkflowDefinition",
                                   non_retryable=True)
        self._spec = spec
        self._started_at = workflow.now().isoformat()
        for sid in spec.order:
            self._steps[sid] = {"status": "pending", "type": spec.steps[sid].type, "output": None,
                                "error": None, "started_at": None, "finished_at": None, "reason": None}
        workflow.logger.info("workflow started", extra={"steps": len(spec.order), "failure_policy": spec.failure_policy})

        for sid in spec.order:  # topological, stable order
            self._tasks[sid] = asyncio.ensure_future(self._run_step(spec.steps[sid]))

        try:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        except asyncio.CancelledError:
            # Workflow cancellation requested. gather() already cancelled the children.
            self._aborting = True
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            for st in self._steps.values():
                if st["status"] not in TERMINAL:
                    st["status"], st["reason"] = "cancelled", "workflow_cancelled"
            self._final = "CANCELLED"
            workflow.logger.info("workflow cancelled")
            raise

        failed = [sid for sid in spec.order if self._steps[sid]["status"] == "failed"]
        if self._fatal is not None:
            self._final = "FAILED"
            workflow.logger.error("workflow failed", extra={"failed_step": self._fatal["step"]})
            raise ApplicationError(
                f"step '{self._fatal['step']}' failed: {self._fatal['message']}", self._result(),
                type="WorkflowStepFailed", non_retryable=True,
            )
        self._final = "COMPLETED_WITH_ERRORS" if failed else "COMPLETED"
        workflow.logger.info("workflow finished", extra={"status": self._final, "failed_steps": failed})
        return self._result()

    # ------------------------------------------------------------------- one step
    async def _run_step(self, step: Step) -> None:
        st = self._steps[step.id]
        try:
            await workflow.wait_condition(lambda: all(self._steps[d]["status"] in TERMINAL for d in step.depends_on))
            if self._aborting:
                return self._finish(step, "cancelled", reason="aborted")
            if step.trigger_rule == "all_success" and any(self._steps[d]["status"] != "completed" for d in step.depends_on):
                return self._finish(step, "skipped", reason="upstream_not_successful")

            await workflow.wait_condition(lambda: not self._paused)  # pause gate: no new work starts while paused
            if self._aborting:
                return self._finish(step, "cancelled", reason="aborted")

            ctx = self._context()
            if step.condition is not None and not evaluate_condition(step.condition, ctx):
                return self._finish(step, "skipped", reason="condition_false")

            st["status"], st["started_at"] = "running", workflow.now().isoformat()
            if step.type == "activity":
                output = await self._run_activity(step, ctx)
            elif step.type == "wait_signal":
                output = await self._wait_signal(step)
            else:
                await workflow.sleep(step.seconds or 0)
                output = {"slept_seconds": step.seconds}
            self._finish(step, "completed", output=output)
        except asyncio.CancelledError:
            if st["status"] not in TERMINAL:
                st["status"], st["reason"] = "cancelled", "workflow_cancelled" if not self._aborting_by_failure() else "sibling_failed"
                st["finished_at"] = workflow.now().isoformat()
            raise
        except ActivityError as e:
            if isinstance(e.cause, TemporalCancelledError):  # our activity was cancelled (fail-fast / workflow cancel)
                return self._finish(step, "cancelled", reason="sibling_failed" if self._aborting_by_failure() else "workflow_cancelled")
            self._fail(step, _describe_error(e))
        except (ApplicationError, TemplateError, TemporalTimeoutError) as e:
            self._fail(step, _describe_error(e) if not isinstance(e, TemplateError)
                       else {"type": "TemplateError", "message": str(e), "non_retryable": True})

    def _aborting_by_failure(self) -> bool:
        return self._fatal is not None

    async def _run_activity(self, step: Step, ctx: dict) -> Any:
        if step.input is not None:
            payload = resolve_templates(step.input, ctx)
        else:  # default input shape: everything upstream produced + the workflow input
            payload = {
                "upstream": {d: self._steps[d]["output"] for d in step.depends_on},
                "workflow_input": self._spec.input,
            }
        t, r = step.timeouts, step.retry
        return await workflow.execute_activity(
            "execute_operation",
            StepActivityInput(step_id=step.id, operation=step.name, input=payload),
            result_type=dict,
            activity_id=step.id,
            task_queue=step.task_queue,
            start_to_close_timeout=_secs(t.start_to_close_seconds),
            schedule_to_close_timeout=_secs(t.schedule_to_close_seconds),
            heartbeat_timeout=_secs(t.heartbeat_seconds),
            retry_policy=RetryPolicy(
                initial_interval=timedelta(seconds=r.initial_interval_seconds),
                backoff_coefficient=r.backoff_coefficient,
                maximum_interval=_secs(r.maximum_interval_seconds),
                maximum_attempts=r.maximum_attempts,
                non_retryable_error_types=list(r.non_retryable_error_types) or None,
            ),
            cancellation_type=ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
        )

    async def _wait_signal(self, step: Step) -> Any:
        st = self._steps[step.id]
        st["status"] = "waiting_signal"
        try:
            await workflow.wait_condition(lambda: bool(self._events.get(step.signal)), timeout=_secs(step.timeout_seconds))
        except asyncio.TimeoutError:
            raise ApplicationError(f"timed out after {step.timeout_seconds}s waiting for event '{step.signal}'",
                                   type="SignalTimeout", non_retryable=True)
        st["status"] = "running"
        return {"event": step.signal, "payload": self._events[step.signal].pop(0)}

    # ------------------------------------------------------------- state helpers
    def _context(self) -> dict:
        return {
            "input": self._spec.input,
            "steps": {sid: {"status": s["status"], "output": s["output"]} for sid, s in self._steps.items()},
        }

    def _finish(self, step: Step, status: str, output: Any = None, reason: Optional[str] = None) -> None:
        st = self._steps[step.id]
        st["status"], st["output"], st["reason"] = status, output, reason
        st["finished_at"] = workflow.now().isoformat()
        workflow.logger.info("step finished", extra={"step_id": step.id, "step_status": status, "reason": reason})

    def _fail(self, step: Step, err: dict) -> None:
        st = self._steps[step.id]
        st["status"], st["error"], st["finished_at"] = "failed", err, workflow.now().isoformat()
        workflow.logger.error("step failed", extra={"step_id": step.id, "error": err})
        if step.on_failure == "continue":
            return
        if self._fatal is None:
            self._fatal = {"step": step.id, **err}
        if self._spec.failure_policy == "fail_fast" and not self._aborting:
            self._aborting = True  # stop starting new steps and cancel everything still in flight
            for other, task in self._tasks.items():
                if other != step.id and not task.done():
                    task.cancel()

    def _result(self) -> dict:
        return {
            "workflow_id": self._spec.workflow_id,
            "status": self._final or self._derive_status(),
            "outputs": {sid: s["output"] for sid, s in self._steps.items() if s["status"] == "completed"},
            "steps": self._steps,
            "failure": self._fatal,
        }

    def _derive_status(self) -> str:
        if self._final:
            return self._final
        if self._paused:
            return "PAUSED"
        if any(s["status"] == "waiting_signal" for s in self._steps.values()):
            return "WAITING_SIGNAL"
        return "RUNNING"

    # -------------------------------------------------- signals, queries, updates
    @workflow.signal(name="send_event")
    def send_event(self, event: ExternalEvent) -> None:
        """External event (webhook, human approval...). Buffered, so it may arrive before the
        step starts waiting. ``event_id`` makes redelivery idempotent."""
        if event.event_id is not None:
            if event.event_id in self._seen_event_ids:
                return
            self._seen_event_ids.add(event.event_id)
        self._events.setdefault(event.name, []).append(event.payload)

    @workflow.query(name="get_status")
    def get_status(self) -> dict:
        if self._spec is None:
            return {"status": "STARTING", "steps": {}}
        res = self._result()
        res.update({"status": self._derive_status(), "paused": self._paused, "started_at": self._started_at,
                    "pending_events": {k: len(v) for k, v in self._events.items() if v}})
        return res

    @workflow.update(name="pause")
    def pause(self) -> str:
        """Stop *starting* new steps. Steps already running are allowed to finish."""
        self._paused = True
        return self._derive_status()

    @pause.validator
    def _validate_pause(self) -> None:
        if self._final:
            raise ValueError("workflow already finished")

    @workflow.update(name="resume")
    def resume(self) -> str:
        self._paused = False
        return self._derive_status()

    @resume.validator
    def _validate_resume(self) -> None:
        if self._final:
            raise ValueError("workflow already finished")
