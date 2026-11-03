"""HTTP API: submit / status / cancel / pause / resume / signal.

The API never executes workflow logic; it validates, then hands the definition to Temporal.
It can be scaled and deployed independently of workers.
"""
import asyncio
import hashlib
import inspect
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from temporalio.client import Client, WorkflowFailureError, WorkflowHandle
from temporalio.common import WorkflowIDConflictPolicy, WorkflowIDReusePolicy
from temporalio.exceptions import ApplicationError, WorkflowAlreadyStartedError
from temporalio.service import RPCError, RPCStatusCode

from business import known_operations
from wfengine.client import connect
from wfengine.config import Settings
from wfengine.logging_setup import configure_logging
from wfengine.models import ExternalEvent
from wfengine.validation import DefinitionValidationError, validate_definition
from wfengine.workflows import JsonWorkflow

log = logging.getLogger("wfengine.api")
SCHEMA_PATH = Path(__file__).parent / "schema" / "workflow.schema.json"
RESTARTABLE = {"FAILED", "CANCELED", "TERMINATED", "TIMED_OUT"}


def _err(status: int, code: str, message: str, **extra: Any) -> JSONResponse:
    return JSONResponse({"error": {"code": code, "message": message, **extra}}, status_code=status)


def definition_hash(definition: dict) -> str:
    return hashlib.sha256(json.dumps(definition, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


async def _memo(desc: Any) -> dict:
    m = desc.memo
    m = m() if callable(m) else m
    if inspect.isawaitable(m):
        m = await m
    return dict(m or {})


def _pending_activities(desc: Any) -> list:
    out = []
    try:
        for p in desc.raw_description.pending_activities:
            out.append({
                "step_id": p.activity_id, "attempt": p.attempt, "maximum_attempts": p.maximum_attempts,
                "last_failure": p.last_failure.message if p.HasField("last_failure") else None,
            })
    except Exception:  # noqa: BLE001 - informational only
        pass
    return out


def _failure_details(exc: WorkflowFailureError) -> Optional[dict]:
    cause = exc.cause
    if isinstance(cause, ApplicationError):
        return {"type": cause.type, "message": cause.message, "details": list(cause.details)}
    return {"type": type(cause).__name__, "message": str(cause)}


async def build_status(handle: WorkflowHandle, settings: Settings) -> dict:
    desc = await handle.describe()
    status = desc.status.name if desc.status else "UNKNOWN"
    doc: dict = {
        "workflow_id": handle.id, "run_id": desc.run_id, "temporal_status": status,
        "task_queue": desc.task_queue, "start_time": desc.start_time.isoformat() if desc.start_time else None,
        "close_time": desc.close_time.isoformat() if desc.close_time else None,
        "pending_activities": _pending_activities(desc),
    }
    try:  # live (or replayed) state from the workflow itself; needs a worker online
        state = await asyncio.wait_for(handle.query("get_status", result_type=dict), settings.query_timeout_seconds)
        doc.update(state)
        doc["state_source"] = "query"
    except Exception as e:  # noqa: BLE001
        doc["state_source"] = "unavailable"
        doc["state_note"] = f"workflow state query failed ({type(e).__name__}); is a worker running for this task queue?"
        if status in RESTARTABLE | {"COMPLETED", "CONTINUED_AS_NEW"}:
            try:
                res = await asyncio.wait_for(handle.result(), 2)
                doc.update(res)
                doc["state_source"] = "result"
            except WorkflowFailureError as fe:
                doc["failure"] = _failure_details(fe)
                details = (doc["failure"] or {}).get("details") or []
                if details and isinstance(details[0], dict):
                    doc.update(details[0])
                    doc["state_source"] = "failure_details"
            except Exception:  # noqa: BLE001
                pass
    return doc


def create_app(client: Optional[Client] = None, settings: Optional[Settings] = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        configure_logging(settings.log_level, settings.log_format)
        app.state.client = client or await connect(settings)
        log.info("api ready", extra={"temporal": settings.temporal_address, "default_task_queue": settings.task_queue})
        yield

    app = FastAPI(title="JSON Workflow Engine", version="1.0.0", lifespan=lifespan)
    app.state.client = client
    known = known_operations() if settings.strict_operations else None

    def handle_for(request: Request, workflow_id: str) -> WorkflowHandle:
        return request.app.state.client.get_workflow_handle(workflow_id)

    async def parse_and_validate(request: Request):
        raw = await request.body()
        try:
            definition = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            return None, None, _err(400, "MALFORMED_JSON", f"request body is not valid JSON: {e}")
        try:
            spec = validate_definition(definition, known_operations=known)
        except DefinitionValidationError as e:
            log.warning("definition rejected", extra={"issues": len(e.issues)})
            return None, None, _err(422, "INVALID_WORKFLOW_DEFINITION", "workflow definition failed validation", issues=e.to_list())
        return definition, spec, None

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/operations")
    async def operations() -> dict:
        return {"operations": sorted(known_operations())}

    @app.get("/schema")
    async def schema() -> JSONResponse:
        return JSONResponse(json.loads(SCHEMA_PATH.read_text()))

    @app.post("/workflows/validate")
    async def validate_only(request: Request):
        _, spec, error = await parse_and_validate(request)
        if error:
            return error
        return {"valid": True, "workflow_id": spec.workflow_id, "execution_order": list(spec.order)}

    @app.post("/workflows")
    async def submit(request: Request):
        definition, spec, error = await parse_and_validate(request)
        if error:
            return error
        c: Client = request.app.state.client
        wid, dhash = spec.workflow_id, definition_hash(definition)
        queue = spec.task_queue or settings.task_queue

        # Idempotent submission keyed by the business workflow_id.
        try:
            existing = c.get_workflow_handle(wid)
            desc = await existing.describe()
            status = desc.status.name if desc.status else "UNKNOWN"
            if status not in RESTARTABLE:
                if (await _memo(desc)).get("definition_hash") != dhash:
                    return _err(409, "WORKFLOW_ID_CONFLICT",
                                f"workflow '{wid}' already exists with a different definition", workflow_id=wid)
                log.info("duplicate submission", extra={"workflow_id": wid, "temporal_status": status})
                return JSONResponse({"workflow_id": wid, "run_id": desc.run_id, "status": status, "duplicate": True}, status_code=200)
        except RPCError as e:
            if e.status != RPCStatusCode.NOT_FOUND:
                raise

        try:
            handle = await c.start_workflow(
                JsonWorkflow.run, definition, id=wid, task_queue=queue,
                # Failed/cancelled runs may be resubmitted under the same id; completed ones may not.
                id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                # Two concurrent submissions race to start inside Temporal; exactly one wins and the
                # loser gets WorkflowAlreadyStartedError, which we report as a duplicate.
                id_conflict_policy=WorkflowIDConflictPolicy.FAIL,
                memo={"definition_hash": dhash, "definition_version": spec.version},
            )
        except WorkflowAlreadyStartedError:
            desc = await c.get_workflow_handle(wid).describe()
            if (await _memo(desc)).get("definition_hash") != dhash:
                return _err(409, "WORKFLOW_ID_CONFLICT",
                            f"workflow '{wid}' already exists with a different definition", workflow_id=wid)
            log.info("duplicate submission (race)", extra={"workflow_id": wid})
            return JSONResponse({"workflow_id": wid, "run_id": desc.run_id,
                                 "status": desc.status.name if desc.status else "UNKNOWN", "duplicate": True}, status_code=200)
        except RPCError as e:
            if e.status == RPCStatusCode.ALREADY_EXISTS:
                return JSONResponse({"workflow_id": wid, "status": "COMPLETED", "duplicate": True}, status_code=200)
            raise
        log.info("workflow submitted", extra={"workflow_id": wid, "run_id": handle.result_run_id, "task_queue": queue})
        return JSONResponse({"workflow_id": wid, "run_id": handle.result_run_id, "status": "RUNNING",
                             "task_queue": queue, "duplicate": False}, status_code=202)

    async def _guard(coro):
        try:
            return await coro
        except RPCError as e:
            if e.status == RPCStatusCode.NOT_FOUND:
                return _err(404, "WORKFLOW_NOT_FOUND", "no such workflow")
            if e.status in (RPCStatusCode.FAILED_PRECONDITION, RPCStatusCode.INVALID_ARGUMENT):
                return _err(409, "INVALID_STATE", e.message)
            raise
        except Exception as e:  # noqa: BLE001 - update validator rejections etc.
            if type(e).__name__ == "WorkflowUpdateFailedError":
                return _err(409, "UPDATE_REJECTED", str(e.cause) if getattr(e, "cause", None) else str(e))
            raise

    @app.get("/workflows/{workflow_id}")
    async def status(request: Request, workflow_id: str):
        return await _guard(build_status(handle_for(request, workflow_id), settings))

    @app.post("/workflows/{workflow_id}/cancel")
    async def cancel(request: Request, workflow_id: str):
        async def go():
            await handle_for(request, workflow_id).cancel()
            log.info("cancellation requested", extra={"workflow_id": workflow_id})
            return JSONResponse({"workflow_id": workflow_id, "cancel_requested": True}, status_code=202)
        return await _guard(go())

    @app.post("/workflows/{workflow_id}/pause")
    async def pause(request: Request, workflow_id: str):
        async def go():
            s = await handle_for(request, workflow_id).execute_update("pause", result_type=str)
            return {"workflow_id": workflow_id, "status": s}
        return await _guard(go())

    @app.post("/workflows/{workflow_id}/resume")
    async def resume(request: Request, workflow_id: str):
        async def go():
            s = await handle_for(request, workflow_id).execute_update("resume", result_type=str)
            return {"workflow_id": workflow_id, "status": s}
        return await _guard(go())

    @app.post("/workflows/{workflow_id}/signal/{event_name}")
    async def signal(request: Request, workflow_id: str, event_name: str):
        raw = await request.body()
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError as e:
            return _err(400, "MALFORMED_JSON", str(e))
        if not isinstance(body, dict):
            return _err(422, "INVALID_BODY", "body must be an object: {payload?, event_id?}")

        async def go():
            await handle_for(request, workflow_id).signal(
                "send_event", ExternalEvent(name=event_name, payload=body.get("payload"), event_id=body.get("event_id")))
            log.info("signal sent", extra={"workflow_id": workflow_id, "event": event_name})
            return JSONResponse({"workflow_id": workflow_id, "event": event_name, "delivered": True}, status_code=202)
        return await _guard(go())

    return app


def app_factory() -> FastAPI:  # uvicorn --factory wfengine.api:app_factory
    return create_app()
