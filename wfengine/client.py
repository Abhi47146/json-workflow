"""Temporal client helper with connection retry (containers start in arbitrary order)."""
import asyncio
import logging

from temporalio.client import Client

from wfengine.config import Settings

log = logging.getLogger("wfengine.client")


async def connect(settings: Settings, attempts: int = 30, delay: float = 2.0) -> Client:
    last: Exception = RuntimeError("no attempt made")
    for i in range(1, attempts + 1):
        try:
            return await Client.connect(settings.temporal_address, namespace=settings.temporal_namespace)
        except Exception as e:  # noqa: BLE001 - any connect failure is retried
            last = e
            log.warning("temporal not reachable yet", extra={"attempt": i, "address": settings.temporal_address, "error": str(e)})
            await asyncio.sleep(delay)
    raise last
