"""Deterministic helpers used by the interpreter workflow: condition evaluation and
``${...}`` template resolution.

Pure functions only. No ``eval``: conditions are a small JSON DSL, so a definition can
never execute arbitrary code, and replay always yields the same decisions.
"""
import json
import re
from typing import Any

TEMPLATE_RE = re.compile(r"\$\{([^{}]+)\}")
FULL_TEMPLATE_RE = re.compile(r"^\$\{([^{}]+)\}$")

_MISSING = object()


class TemplateError(Exception):
    """A ``${...}`` reference could not be resolved at run time."""


def get_path(obj: Any, parts: list) -> Any:
    cur = obj
    for seg in parts:
        if isinstance(cur, dict):
            if seg not in cur:
                return _MISSING
            cur = cur[seg]
        elif isinstance(cur, list):
            try:
                cur = cur[int(seg)]
            except (ValueError, IndexError):
                return _MISSING
        else:
            return _MISSING
    return cur


def _split(path: Any) -> list:
    if path in (None, ""):
        return []
    return str(path).split(".")


def resolve_expr(expr: str, ctx: dict) -> Any:
    value = get_path(ctx, expr.strip().split("."))
    if value is _MISSING:
        raise TemplateError(f"cannot resolve ${{{expr}}}")
    return value


def resolve_templates(value: Any, ctx: dict) -> Any:
    """Recursively resolve ``${steps.<id>.output.<path>}`` / ``${input.<path>}``.

    A string that is exactly one reference keeps the referenced value's type; a reference
    embedded in a longer string is stringified.
    """
    if isinstance(value, str):
        full = FULL_TEMPLATE_RE.match(value)
        if full:
            return resolve_expr(full.group(1), ctx)

        def sub(m: "re.Match") -> str:
            v = resolve_expr(m.group(1), ctx)
            return json.dumps(v) if isinstance(v, (dict, list)) else str(v)

        return TEMPLATE_RE.sub(sub, value)
    if isinstance(value, dict):
        return {k: resolve_templates(v, ctx) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_templates(v, ctx) for v in value]
    return value


def _compare(op: str, actual: Any, expected: Any) -> bool:
    try:
        if op == "eq":
            return actual == expected
        if op == "ne":
            return actual != expected
        if op == "gt":
            return actual > expected
        if op == "gte":
            return actual >= expected
        if op == "lt":
            return actual < expected
        if op == "lte":
            return actual <= expected
        if op == "in":
            return actual in expected
        if op == "contains":
            return expected in actual
        if op == "truthy":
            return bool(actual)
    except TypeError:
        return False
    return False


def evaluate_condition(cond: dict, ctx: dict) -> bool:
    """Evaluate the condition DSL against ``ctx`` = {"input": {...}, "steps": {id: {"status","output"}}}.

    A leaf that points at a missing value is False for every operator (including ``ne``);
    wrap it in ``not`` if the opposite is wanted.
    """
    if "all" in cond:
        return all(evaluate_condition(c, ctx) for c in cond["all"])
    if "any" in cond:
        return any(evaluate_condition(c, ctx) for c in cond["any"])
    if "not" in cond:
        return not evaluate_condition(cond["not"], ctx)

    sub = _split(cond.get("path"))
    if "step" in cond:
        field = cond.get("field", "output")
        actual = get_path(ctx, ["steps", cond["step"], field, *sub])
    else:
        actual = get_path(ctx, ["input", *_split(cond["input"]), *sub])
    op = cond["op"]
    if op == "exists":
        return actual is not _MISSING and actual is not None
    if actual is _MISSING:
        return False
    return _compare(op, actual, cond.get("value"))
