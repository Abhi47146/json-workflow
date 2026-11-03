"""Plain data types shared by workflow, activities, API and worker.

Kept free of side effects so it can be imported inside the Temporal workflow sandbox.
"""
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class StepActivityInput:
    """Argument of the generic ``execute_operation`` activity."""

    step_id: str
    operation: str
    input: dict = field(default_factory=dict)


@dataclass
class ExternalEvent:
    """Payload of the ``send_event`` signal.

    ``event_id`` makes signal delivery idempotent: a repeated id is ignored.
    """

    name: str
    payload: Any = None
    event_id: Optional[str] = None
