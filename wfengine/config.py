"""Runtime configuration, read from environment variables (12-factor)."""
import os
from dataclasses import dataclass


def _int(name: str, default: int) -> int:
    return int(os.environ.get(name, default))


@dataclass(frozen=True)
class Settings:
    temporal_address: str = "localhost:7233"
    temporal_namespace: str = "default"
    task_queue: str = "json-workflows"
    log_level: str = "INFO"
    log_format: str = "json"  # json | console
    # worker tuning
    max_concurrent_activities: int = 100
    max_concurrent_workflow_tasks: int = 100
    graceful_shutdown_seconds: int = 30
    # Heartbeats are batched by the SDK (default: 80% of heartbeat_timeout, i.e. ~24s). Cancellation is
    # delivered in the heartbeat response, so this caps cancel latency.
    heartbeat_throttle_seconds: float = 2.0
    # API
    query_timeout_seconds: float = 5.0
    strict_operations: bool = True  # reject unknown operation names at submit time

    @staticmethod
    def from_env() -> "Settings":
        return Settings(
            temporal_address=os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
            temporal_namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
            task_queue=os.environ.get("TASK_QUEUE", "json-workflows"),
            log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
            log_format=os.environ.get("LOG_FORMAT", "json"),
            max_concurrent_activities=_int("WORKER_MAX_CONCURRENT_ACTIVITIES", 100),
            max_concurrent_workflow_tasks=_int("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", 100),
            graceful_shutdown_seconds=_int("GRACEFUL_SHUTDOWN_SECONDS", 30),
            heartbeat_throttle_seconds=float(os.environ.get("HEARTBEAT_THROTTLE_SECONDS", 2)),
            query_timeout_seconds=float(os.environ.get("QUERY_TIMEOUT_SECONDS", 5)),
            strict_operations=os.environ.get("STRICT_OPERATIONS", "1") not in ("0", "false", "False"),
        )
