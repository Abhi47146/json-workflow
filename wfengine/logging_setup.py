"""Structured (JSON) logging. Temporal's ``workflow.logger`` / ``activity.logger`` attach
``temporal_workflow`` / ``temporal_activity`` context to each record; we surface it as
top-level fields so logs can be filtered by workflow_id, run_id, step/activity id."""
import json
import logging
import sys
from datetime import datetime, timezone

_STD = set(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {"message", "asctime", "taskName"}


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for key in ("temporal_workflow", "temporal_activity"):
            ctx = getattr(record, key, None)
            if isinstance(ctx, dict):
                doc.update({
                    "workflow_id": ctx.get("workflow_id"), "run_id": ctx.get("run_id"),
                    "task_queue": ctx.get("task_queue"),
                    **({"activity_id": ctx.get("activity_id"), "attempt": ctx.get("attempt")} if key == "temporal_activity" else {}),
                })
        for k, v in record.__dict__.items():
            if k not in _STD and not k.startswith("temporal_"):
                doc[k] = v
        if record.exc_info:
            doc["exc"] = self.formatException(record.exc_info)
        return json.dumps(doc, default=str)


def configure_logging(level: str = "INFO", fmt: str = "json") -> None:
    handler = logging.StreamHandler(sys.stdout)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
