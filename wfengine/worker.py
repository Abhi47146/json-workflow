"""Standalone worker process: ``python -m wfengine.worker``.

Independent of the API. Scale horizontally by running more copies against the same task
queue (``docker compose up --scale worker=3``); Temporal load-balances tasks across pollers.
Run several differently-named queues by starting workers with different TASK_QUEUE values
(or TASK_QUEUES=a,b to poll several from one process).
"""
import asyncio
import logging
import os
import signal
from datetime import timedelta

from temporalio.worker import Worker

from wfengine.activities import execute_operation
from wfengine.client import connect
from wfengine.config import Settings
from wfengine.logging_setup import configure_logging
from wfengine.workflows import JsonWorkflow

log = logging.getLogger("wfengine.worker")


async def run_worker(settings: Settings, queues: list) -> None:
    client = await connect(settings)
    workers = [
        Worker(
            client,
            task_queue=q,
            workflows=[JsonWorkflow],
            activities=[execute_operation],
            max_concurrent_activities=settings.max_concurrent_activities,
            max_concurrent_workflow_tasks=settings.max_concurrent_workflow_tasks,
            # On SIGTERM: stop polling, give in-flight activities this long to finish, then cancel
            # them (they will be retried elsewhere; idempotency keys make that safe).
            graceful_shutdown_timeout=timedelta(seconds=settings.graceful_shutdown_seconds),
            max_heartbeat_throttle_interval=timedelta(seconds=settings.heartbeat_throttle_seconds),
            default_heartbeat_throttle_interval=timedelta(seconds=settings.heartbeat_throttle_seconds),
        )
        for q in queues
    ]

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    log.info("worker starting", extra={"task_queues": queues, "address": settings.temporal_address, "pid": os.getpid()})
    started = []
    try:
        for w in workers:
            await w.__aenter__()
            started.append(w)
        log.info("worker ready", extra={"task_queues": queues})
        await stop.wait()
        log.info("shutdown signal received; draining", extra={"grace_seconds": settings.graceful_shutdown_seconds})
    finally:
        await asyncio.gather(*(w.__aexit__(None, None, None) for w in started), return_exceptions=True)
        log.info("worker stopped")


def main() -> None:
    settings = Settings.from_env()
    configure_logging(settings.log_level, settings.log_format)
    queues = [q.strip() for q in os.environ.get("TASK_QUEUES", settings.task_queue).split(",") if q.strip()]
    asyncio.run(run_worker(settings, queues))


if __name__ == "__main__":
    main()
