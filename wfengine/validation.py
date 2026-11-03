"""Validation and normalisation of JSON workflow definitions.

Pure and deterministic (no I/O, clocks or randomness) so that it can run both in the
API (reject before anything starts) and inside the workflow (defence in depth for
definitions started through other clients such as the Temporal CLI).

``validate_definition`` returns a normalised ``WorkflowSpec`` or raises
``DefinitionValidationError`` carrying *all* issues found, not just the first.
"""
import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Optional

STEP_TYPES = ("activity", "wait_signal", "delay")
FAILURE_POLICIES = ("fail_fast", "wait_all")
ON_FAILURE = ("fail", "continue")
TRIGGER_RULES = ("all_success", "all_done")
CONDITION_OPS = ("eq", "ne", "gt", "gte", "lt", "lte", "in", "contains", "exists", "truthy")
MAX_STEPS = 500

ID_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
WORKFLOW_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,199}$")
QUEUE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
TEMPLATE_RE = re.compile(r"\$\{([^{}]+)\}")

TOP_LEVEL_KEYS = {
    "workflow_id", "version", "description", "input", "failure_policy",
    "task_queue", "defaults", "steps",
}
DEFAULTS_KEYS = {"retry", "timeouts", "task_queue"}
STEP_KEYS = {
    "id", "type", "name", "description", "input", "depends_on", "condition", "retry",
    "timeouts", "on_failure", "trigger_rule", "task_queue", "signal", "timeout_seconds",
    "seconds",
}
RETRY_KEYS = {
    "initial_interval_seconds", "backoff_coefficient", "maximum_interval_seconds",
    "maximum_attempts", "non_retryable_error_types",
}
TIMEOUT_KEYS = {"start_to_close_seconds", "schedule_to_close_seconds", "heartbeat_seconds"}


# --------------------------------------------------------------------------- models
@dataclass(frozen=True)
class RetrySpec:
    initial_interval_seconds: float = 1.0
    backoff_coefficient: float = 2.0
    maximum_interval_seconds: Optional[float] = None
    maximum_attempts: int = 3  # explicit default: Temporal's own default is *unlimited*
    non_retryable_error_types: tuple = ()


@dataclass(frozen=True)
class TimeoutSpec:
    start_to_close_seconds: float = 60.0
    schedule_to_close_seconds: Optional[float] = None
    heartbeat_seconds: Optional[float] = 30.0  # lets a crashed worker be detected quickly


@dataclass(frozen=True)
class Step:
    id: str
    type: str
    name: Optional[str] = None  # operation name for type=activity
    description: Optional[str] = None
    input: Optional[dict] = None
    depends_on: tuple = ()
    condition: Optional[dict] = None
    retry: RetrySpec = field(default_factory=RetrySpec)
    timeouts: TimeoutSpec = field(default_factory=TimeoutSpec)
    on_failure: str = "fail"
    trigger_rule: str = "all_success"
    task_queue: Optional[str] = None
    signal: Optional[str] = None  # type=wait_signal
    timeout_seconds: Optional[float] = None  # type=wait_signal (None = wait forever)
    seconds: Optional[float] = None  # type=delay


@dataclass(frozen=True)
class WorkflowSpec:
    workflow_id: str
    steps: dict  # id -> Step, in topological order
    order: tuple  # topological order of step ids
    failure_policy: str = "fail_fast"
    input: dict = field(default_factory=dict)
    version: Optional[str] = None
    task_queue: Optional[str] = None


# -------------------------------------------------------------------------- errors
@dataclass(frozen=True)
class ValidationIssue:
    code: str
    path: str
    message: str

    def to_dict(self) -> dict:
        return {"code": self.code, "path": self.path, "message": self.message}


class DefinitionValidationError(Exception):
    def __init__(self, issues: Iterable[ValidationIssue]):
        self.issues = list(issues)
        summary = "; ".join(f"[{i.code}] {i.path}: {i.message}" for i in self.issues[:5])
        more = f" (+{len(self.issues) - 5} more)" if len(self.issues) > 5 else ""
        super().__init__(f"invalid workflow definition: {summary}{more}")

    def to_list(self) -> list:
        return [i.to_dict() for i in self.issues]


# ------------------------------------------------------------------------ helpers
def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


class _Collector:
    def __init__(self) -> None:
        self.issues: list = []

    def add(self, code: str, path: str, message: str) -> None:
        self.issues.append(ValidationIssue(code, path, message))

    def unknown_keys(self, obj: dict, allowed: set, path: str) -> None:
        for k in sorted(set(obj) - allowed, key=str):
            self.add("UNKNOWN_FIELD", f"{path}.{k}" if path else str(k), f"unknown field '{k}'")


def _positive(c: _Collector, obj: dict, key: str, path: str, allow_zero: bool = False) -> Optional[float]:
    if key not in obj or obj[key] is None:
        return None
    v = obj[key]
    if not _is_num(v) or v < 0 or (v == 0 and not allow_zero):
        c.add("INVALID_VALUE", f"{path}.{key}", f"must be a {'non-negative' if allow_zero else 'positive'} number")
        return None
    return float(v)


def _parse_retry(c: _Collector, raw: Any, path: str, base: RetrySpec) -> RetrySpec:
    if raw is None:
        return base
    if not isinstance(raw, dict):
        c.add("INVALID_TYPE", path, "retry must be an object")
        return base
    c.unknown_keys(raw, RETRY_KEYS, path)
    initial = _positive(c, raw, "initial_interval_seconds", path)
    maximum = _positive(c, raw, "maximum_interval_seconds", path)
    coeff = base.backoff_coefficient
    if "backoff_coefficient" in raw:
        v = raw["backoff_coefficient"]
        if not _is_num(v) or v < 1:
            c.add("INVALID_VALUE", f"{path}.backoff_coefficient", "must be a number >= 1")
        else:
            coeff = float(v)
    attempts = base.maximum_attempts
    if "maximum_attempts" in raw:
        v = raw["maximum_attempts"]
        if not _is_int(v) or v < 0:
            c.add("INVALID_VALUE", f"{path}.maximum_attempts", "must be an integer >= 0 (0 = unlimited)")
        else:
            attempts = v
    nr = base.non_retryable_error_types
    if "non_retryable_error_types" in raw:
        v = raw["non_retryable_error_types"]
        if not isinstance(v, list) or not all(isinstance(x, str) for x in v):
            c.add("INVALID_TYPE", f"{path}.non_retryable_error_types", "must be an array of strings")
        else:
            nr = tuple(v)
    init_v = initial if initial is not None else base.initial_interval_seconds
    max_v = maximum if maximum is not None else base.maximum_interval_seconds
    if max_v is not None and max_v < init_v:
        c.add("INVALID_VALUE", f"{path}.maximum_interval_seconds", "must be >= initial_interval_seconds")
    return RetrySpec(init_v, coeff, max_v, attempts, nr)


def _parse_timeouts(c: _Collector, raw: Any, path: str, base: TimeoutSpec) -> TimeoutSpec:
    if raw is None:
        return base
    if not isinstance(raw, dict):
        c.add("INVALID_TYPE", path, "timeouts must be an object")
        return base
    c.unknown_keys(raw, TIMEOUT_KEYS, path)
    s2c = _positive(c, raw, "start_to_close_seconds", path)
    sch = _positive(c, raw, "schedule_to_close_seconds", path)
    hb = _positive(c, raw, "heartbeat_seconds", path)
    return TimeoutSpec(
        s2c if s2c is not None else base.start_to_close_seconds,
        sch if sch is not None else base.schedule_to_close_seconds,
        hb if hb is not None else base.heartbeat_seconds,
    )


def iter_templates(value: Any) -> Iterable[str]:
    """Yield every ``${expr}`` expression found in a JSON-like value."""
    if isinstance(value, str):
        for m in TEMPLATE_RE.finditer(value):
            yield m.group(1).strip()
    elif isinstance(value, dict):
        for v in value.values():
            yield from iter_templates(v)
    elif isinstance(value, list):
        for v in value:
            yield from iter_templates(v)


def _validate_condition(c: _Collector, cond: Any, path: str, step_ids: set, ancestors: set) -> None:
    if not isinstance(cond, dict):
        c.add("INVALID_CONDITION", path, "condition must be an object")
        return
    composite = [k for k in ("all", "any", "not") if k in cond]
    if composite:
        if len(cond) != 1:
            c.add("INVALID_CONDITION", path, f"'{composite[0]}' cannot be combined with other keys")
            return
        key = composite[0]
        if key == "not":
            _validate_condition(c, cond["not"], f"{path}.not", step_ids, ancestors)
        else:
            items = cond[key]
            if not isinstance(items, list) or not items:
                c.add("INVALID_CONDITION", f"{path}.{key}", "must be a non-empty array")
                return
            for i, sub in enumerate(items):
                _validate_condition(c, sub, f"{path}.{key}[{i}]", step_ids, ancestors)
        return
    allowed = {"step", "input", "field", "path", "op", "value"}
    for k in sorted(set(cond) - allowed):
        c.add("INVALID_CONDITION", f"{path}.{k}", f"unknown condition key '{k}'")
    has_step, has_input = "step" in cond, "input" in cond
    if has_step == has_input:
        c.add("INVALID_CONDITION", path, "exactly one of 'step' or 'input' is required")
    if has_step:
        sid = cond["step"]
        if not isinstance(sid, str) or sid not in step_ids:
            c.add("INVALID_CONDITION", f"{path}.step", f"references unknown step {sid!r}")
        elif ancestors is not None and sid not in ancestors:
            c.add("INVALID_REFERENCE", f"{path}.step", f"step '{sid}' must be listed in depends_on (directly or transitively)")
    if has_input and not isinstance(cond["input"], str):
        c.add("INVALID_CONDITION", f"{path}.input", "must be a string path into the workflow input")
    if "path" in cond and not isinstance(cond["path"], str):
        c.add("INVALID_CONDITION", f"{path}.path", "must be a string")
    if "field" in cond and cond["field"] not in ("output", "status"):
        c.add("INVALID_CONDITION", f"{path}.field", "must be 'output' or 'status'")
    op = cond.get("op")
    if op not in CONDITION_OPS:
        c.add("INVALID_CONDITION", f"{path}.op", f"op must be one of {list(CONDITION_OPS)}")
    elif op not in ("exists", "truthy") and "value" not in cond:
        c.add("INVALID_CONDITION", f"{path}.value", f"op '{op}' requires a 'value'")


def _find_cycles(graph: dict) -> list:
    """Return a list of cycles (each a list of ids, first == last). Iterative DFS."""
    WHITE, GREY, BLACK = 0, 1, 2
    color = {n: WHITE for n in graph}
    cycles, seen = [], set()
    for root in graph:
        if color[root] != WHITE:
            continue
        stack = [(root, iter(graph[root]))]
        path = [root]
        color[root] = GREY
        while stack:
            node, it = stack[-1]
            advanced = False
            for nxt in it:
                if nxt not in graph:
                    continue  # missing dependency reported separately
                if color[nxt] == WHITE:
                    color[nxt] = GREY
                    stack.append((nxt, iter(graph[nxt])))
                    path.append(nxt)
                    advanced = True
                    break
                if color[nxt] == GREY:
                    cyc = path[path.index(nxt):] + [nxt]
                    key = frozenset(cyc)
                    if key not in seen:
                        seen.add(key)
                        cycles.append(cyc)
            if not advanced:
                color[node] = BLACK
                stack.pop()
                path.pop()
    return cycles


def _topological(order_hint: list, deps: dict) -> list:
    """Kahn's algorithm, stable w.r.t. declaration order."""
    indeg = {n: len(deps[n]) for n in order_hint}
    children: dict = {n: [] for n in order_hint}
    for n in order_hint:
        for d in deps[n]:
            children[d].append(n)
    ready = [n for n in order_hint if indeg[n] == 0]
    out = []
    while ready:
        n = ready.pop(0)
        out.append(n)
        for ch in children[n]:
            indeg[ch] -= 1
            if indeg[ch] == 0:
                ready.append(ch)
        ready.sort(key=order_hint.index)
    return out


def _ancestors(deps: dict) -> dict:
    memo: dict = {}

    def visit(n: str) -> set:
        if n in memo:
            return memo[n]
        acc: set = set()
        for d in deps[n]:
            acc.add(d)
            acc |= visit(d)
        memo[n] = acc
        return acc

    for n in deps:
        visit(n)
    return memo


# ---------------------------------------------------------------------------- main
def validate_definition(raw: Any, known_operations: Optional[Iterable[str]] = None) -> WorkflowSpec:
    """Validate ``raw`` (already-parsed JSON). Raise DefinitionValidationError listing all problems.

    ``known_operations``: if given, activity ``name`` values must be in this set.
    """
    c = _Collector()
    if not isinstance(raw, dict):
        c.add("INVALID_TYPE", "$", "workflow definition must be a JSON object")
        raise DefinitionValidationError(c.issues)
    c.unknown_keys(raw, TOP_LEVEL_KEYS, "")

    wid = raw.get("workflow_id")
    if wid is None:
        c.add("MISSING_FIELD", "workflow_id", "workflow_id is required")
    elif not isinstance(wid, str) or not WORKFLOW_ID_RE.match(wid):
        c.add("INVALID_VALUE", "workflow_id", "must be a non-empty string of letters, digits, '.', '_', '-', ':' (max 200)")

    version = raw.get("version")
    if version is not None and (isinstance(version, bool) or not isinstance(version, (str, int))):
        c.add("INVALID_TYPE", "version", "must be a string or integer")

    policy = raw.get("failure_policy", "fail_fast")
    if policy not in FAILURE_POLICIES:
        c.add("INVALID_VALUE", "failure_policy", f"must be one of {list(FAILURE_POLICIES)}")
        policy = "fail_fast"

    wf_input = raw.get("input", {})
    if not isinstance(wf_input, dict):
        c.add("INVALID_TYPE", "input", "must be an object")
        wf_input = {}

    top_queue = raw.get("task_queue")
    if top_queue is not None and (not isinstance(top_queue, str) or not QUEUE_RE.match(top_queue)):
        c.add("INVALID_VALUE", "task_queue", "invalid task queue name")

    base_retry, base_timeouts, default_queue = RetrySpec(), TimeoutSpec(), None
    defaults = raw.get("defaults")
    if defaults is not None:
        if not isinstance(defaults, dict):
            c.add("INVALID_TYPE", "defaults", "must be an object")
        else:
            c.unknown_keys(defaults, DEFAULTS_KEYS, "defaults")
            base_retry = _parse_retry(c, defaults.get("retry"), "defaults.retry", base_retry)
            base_timeouts = _parse_timeouts(c, defaults.get("timeouts"), "defaults.timeouts", base_timeouts)
            default_queue = defaults.get("task_queue")
            if default_queue is not None and (not isinstance(default_queue, str) or not QUEUE_RE.match(default_queue)):
                c.add("INVALID_VALUE", "defaults.task_queue", "invalid task queue name")
                default_queue = None

    raw_steps = raw.get("steps")
    if raw_steps is None:
        c.add("MISSING_FIELD", "steps", "steps is required")
        raw_steps = []
    elif not isinstance(raw_steps, list):
        c.add("INVALID_TYPE", "steps", "steps must be an array")
        raw_steps = []
    elif not raw_steps:
        c.add("EMPTY_WORKFLOW", "steps", "at least one step is required")
    elif len(raw_steps) > MAX_STEPS:
        c.add("TOO_MANY_STEPS", "steps", f"at most {MAX_STEPS} steps are supported")

    known = set(known_operations) if known_operations is not None else None
    parsed: dict = {}
    declared_order: list = []

    # Pass 1: structure of each step, duplicates
    for idx, rs in enumerate(raw_steps):
        p = f"steps[{idx}]"
        if not isinstance(rs, dict):
            c.add("INVALID_TYPE", p, "step must be an object")
            continue
        c.unknown_keys(rs, STEP_KEYS, p)
        sid = rs.get("id")
        if sid is None:
            c.add("MISSING_FIELD", f"{p}.id", "step id is required")
            continue
        if not isinstance(sid, str) or not ID_RE.match(sid):
            c.add("INVALID_VALUE", f"{p}.id", "id must match ^[A-Za-z][A-Za-z0-9_-]{0,63}$")
            continue
        if sid in parsed:
            c.add("DUPLICATE_STEP_ID", f"{p}.id", f"duplicate step id '{sid}'")
            continue
        stype = rs.get("type")
        if stype is None:
            c.add("MISSING_FIELD", f"{p}.type", "step type is required")
            continue
        if stype not in STEP_TYPES:
            c.add("UNSUPPORTED_STEP_TYPE", f"{p}.type", f"unsupported step type {stype!r}; supported: {list(STEP_TYPES)}")
            continue

        name = rs.get("name")
        signal = rs.get("signal")
        seconds = rs.get("seconds")
        timeout_seconds = rs.get("timeout_seconds")
        if stype == "activity":
            if not isinstance(name, str) or not name:
                c.add("MISSING_FIELD", f"{p}.name", "activity steps require 'name' (the operation to run)")
                name = None
            elif known is not None and name not in known:
                c.add("UNSUPPORTED_OPERATION", f"{p}.name", f"unknown operation '{name}'")
            for bad in ("signal", "seconds", "timeout_seconds"):
                if bad in rs:
                    c.add("INVALID_FIELD_FOR_TYPE", f"{p}.{bad}", f"'{bad}' is not valid for activity steps")
        elif stype == "wait_signal":
            if not isinstance(signal, str) or not signal:
                c.add("MISSING_FIELD", f"{p}.signal", "wait_signal steps require 'signal' (event name)")
                signal = None
            if timeout_seconds is not None:
                timeout_seconds = _positive(c, rs, "timeout_seconds", p)
            for bad in ("name", "seconds", "retry", "timeouts"):
                if bad in rs:
                    c.add("INVALID_FIELD_FOR_TYPE", f"{p}.{bad}", f"'{bad}' is not valid for wait_signal steps")
        else:  # delay
            seconds = _positive(c, rs, "seconds", p, allow_zero=True)
            if seconds is None and "seconds" not in rs:
                c.add("MISSING_FIELD", f"{p}.seconds", "delay steps require 'seconds'")
            for bad in ("name", "signal", "timeout_seconds", "retry", "timeouts"):
                if bad in rs:
                    c.add("INVALID_FIELD_FOR_TYPE", f"{p}.{bad}", f"'{bad}' is not valid for delay steps")

        inp = rs.get("input")
        if inp is not None and not isinstance(inp, dict):
            c.add("INVALID_TYPE", f"{p}.input", "input must be an object")
            inp = None

        deps_raw = rs.get("depends_on", [])
        deps: tuple = ()
        if not isinstance(deps_raw, list) or not all(isinstance(d, str) for d in deps_raw):
            c.add("INVALID_TYPE", f"{p}.depends_on", "depends_on must be an array of step ids")
        else:
            if len(set(deps_raw)) != len(deps_raw):
                c.add("DUPLICATE_DEPENDENCY", f"{p}.depends_on", "depends_on contains duplicates")
            deps = tuple(dict.fromkeys(deps_raw))

        on_failure = rs.get("on_failure", "fail")
        if on_failure not in ON_FAILURE:
            c.add("INVALID_VALUE", f"{p}.on_failure", f"must be one of {list(ON_FAILURE)}")
            on_failure = "fail"
        trigger = rs.get("trigger_rule", "all_success")
        if trigger not in TRIGGER_RULES:
            c.add("INVALID_VALUE", f"{p}.trigger_rule", f"must be one of {list(TRIGGER_RULES)}")
            trigger = "all_success"
        queue = rs.get("task_queue", default_queue)
        if queue is not None and (not isinstance(queue, str) or not QUEUE_RE.match(queue)):
            c.add("INVALID_VALUE", f"{p}.task_queue", "invalid task queue name")
            queue = None

        retry = _parse_retry(c, rs.get("retry"), f"{p}.retry", base_retry)
        timeouts = _parse_timeouts(c, rs.get("timeouts"), f"{p}.timeouts", base_timeouts)
        if timeouts.schedule_to_close_seconds is not None and timeouts.schedule_to_close_seconds < timeouts.start_to_close_seconds:
            c.add("INVALID_VALUE", f"{p}.timeouts.schedule_to_close_seconds", "must be >= start_to_close_seconds")

        parsed[sid] = Step(
            id=sid, type=stype, name=name if stype == "activity" else None,
            description=rs.get("description") if isinstance(rs.get("description"), str) else None,
            input=inp, depends_on=deps, condition=rs.get("condition"), retry=retry, timeouts=timeouts,
            on_failure=on_failure, trigger_rule=trigger, task_queue=queue,
            signal=signal if stype == "wait_signal" else None,
            timeout_seconds=timeout_seconds if stype == "wait_signal" else None,
            seconds=seconds if stype == "delay" else None,
        )
        declared_order.append(sid)

    # Pass 2: graph integrity
    for sid, st in parsed.items():
        for d in st.depends_on:
            if d == sid:
                c.add("SELF_DEPENDENCY", f"steps.{sid}.depends_on", f"step '{sid}' depends on itself")
            elif d not in parsed:
                c.add("MISSING_DEPENDENCY", f"steps.{sid}.depends_on", f"step '{sid}' depends on unknown step '{d}'")

    graph = {sid: [d for d in st.depends_on if d in parsed] for sid, st in parsed.items()}
    cycles = _find_cycles(graph)
    for cyc in cycles:
        c.add("CIRCULAR_DEPENDENCY", "steps", "circular dependency: " + " -> ".join(cyc))

    # Pass 3: references (needs an acyclic graph to define 'ancestor')
    order: list = []
    if not cycles:
        order = _topological(declared_order, graph)
        anc = _ancestors(graph)
        ids = set(parsed)
        for sid, st in parsed.items():
            for expr in iter_templates(st.input):
                parts = expr.split(".")
                if parts[0] == "input":
                    continue
                if parts[0] != "steps" or len(parts) < 3 or parts[2] not in ("output", "status"):
                    c.add("INVALID_REFERENCE", f"steps.{sid}.input", f"bad reference '${{{expr}}}'; use steps.<id>.output.<path> or input.<path>")
                elif parts[1] not in ids:
                    c.add("INVALID_REFERENCE", f"steps.{sid}.input", f"reference '${{{expr}}}' points to unknown step '{parts[1]}'")
                elif parts[1] not in anc[sid]:
                    c.add("INVALID_REFERENCE", f"steps.{sid}.input", f"reference '${{{expr}}}' requires '{parts[1]}' in depends_on")
            if st.condition is not None:
                _validate_condition(c, st.condition, f"steps.{sid}.condition", ids, anc[sid])
    else:
        for sid, st in parsed.items():
            if st.condition is not None:
                _validate_condition(c, st.condition, f"steps.{sid}.condition", set(parsed), None)

    if c.issues:
        raise DefinitionValidationError(c.issues)

    return WorkflowSpec(
        workflow_id=wid, steps={sid: parsed[sid] for sid in order}, order=tuple(order),
        failure_policy=policy, input=wf_input, version=None if version is None else str(version),
        task_queue=top_queue,
    )
