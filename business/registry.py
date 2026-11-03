"""Operation registry: the seam between orchestration and business logic.

The engine only ever calls ``invoke(name, ctx, input)``. Adding a capability means adding a
function decorated with ``@operation("name")``; no workflow code changes and no redeploy of
workflow definitions.
"""
import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Optional

from business import storage


class BusinessError(Exception):
    """Domain failure. ``retryable=False`` means retrying can never help (bad input, declined)."""

    def __init__(self, message: str, code: str = "BUSINESS_ERROR", retryable: bool = True):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass
class OpContext:
    idempotency_key: str  # stable across retries, worker restarts and reruns of the same workflow_id
    workflow_id: str
    step_id: str
    attempt: int  # 1-based
    heartbeat: Callable[..., None] = lambda *a, **k: None


Operation = Callable[[OpContext, dict], Awaitable[Any]]
_REGISTRY: Dict[str, Operation] = {}


def operation(name: str):
    def deco(fn: Operation) -> Operation:
        if name in _REGISTRY:
            raise RuntimeError(f"operation '{name}' registered twice")
        _REGISTRY[name] = fn
        return fn

    return deco


def known_operations() -> frozenset:
    return frozenset(_REGISTRY)


def get_operation(name: str) -> Operation:
    try:
        return _REGISTRY[name]
    except KeyError:
        raise BusinessError(f"unknown operation '{name}'", code="UNKNOWN_OPERATION", retryable=False)


async def invoke(name: str, ctx: OpContext, inp: Optional[dict]) -> Any:
    op = get_operation(name)
    await asyncio.to_thread(storage.record_invocation, ctx.idempotency_key, name, ctx.attempt)
    return await op(ctx, inp or {})
