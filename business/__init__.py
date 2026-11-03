"""Business logic. Knows nothing about Temporal: operations take an ``OpContext`` and a
JSON-like input and return a JSON-like result, or raise ``BusinessError``.

Importing this package registers every operation in ``business.operations``.
"""
from business import operations  # noqa: F401  (registers operations)
from business.registry import BusinessError, OpContext, get_operation, invoke, known_operations

__all__ = ["BusinessError", "OpContext", "get_operation", "invoke", "known_operations"]
