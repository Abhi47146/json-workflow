"""Condition DSL and template resolution: pure, deterministic. No server needed."""
import pytest

from wfengine.dag import TemplateError, evaluate_condition, resolve_templates

CTX = {
    "input": {"region": "EU", "limit": 5},
    "steps": {
        "risk": {"status": "completed", "output": {"score": 82, "tags": ["new", "vip"], "decision": {"approved": True}}},
        "slow": {"status": "failed", "output": None},
    },
}


def test_template_keeps_type_for_whole_string():
    assert resolve_templates("${steps.risk.output.score}", CTX) == 82
    assert resolve_templates("${steps.risk.output.decision}", CTX) == {"approved": True}


def test_template_interpolation_and_nesting():
    out = resolve_templates({"a": ["x-${input.region}", {"b": "${steps.risk.output.tags.1}"}], "n": 3}, CTX)
    assert out == {"a": ["x-EU", {"b": "vip"}], "n": 3}


def test_unresolvable_template_raises():
    with pytest.raises(TemplateError):
        resolve_templates("${steps.risk.output.nope}", CTX)
    with pytest.raises(TemplateError):
        resolve_templates("${steps.ghost.output.x}", CTX)


@pytest.mark.parametrize("cond,expected", [
    ({"step": "risk", "path": "score", "op": "gte", "value": 80}, True),
    ({"step": "risk", "path": "score", "op": "lt", "value": 80}, False),
    ({"step": "risk", "path": "decision.approved", "op": "eq", "value": True}, True),
    ({"step": "risk", "path": "tags", "op": "contains", "value": "vip"}, True),
    ({"step": "risk", "path": "score", "op": "in", "value": [1, 82]}, True),
    ({"step": "slow", "field": "status", "op": "eq", "value": "failed"}, True),
    ({"input": "region", "op": "eq", "value": "EU"}, True),
    ({"step": "risk", "path": "missing", "op": "exists"}, False),
    ({"step": "risk", "path": "score", "op": "exists"}, True),
    ({"step": "risk", "path": "missing", "op": "ne", "value": 1}, False),  # missing never satisfies a comparison
    ({"step": "risk", "path": "score", "op": "gt", "value": "abc"}, False),  # type mismatch is False, not an error
    ({"all": [{"input": "region", "op": "eq", "value": "EU"}, {"step": "risk", "path": "score", "op": "gt", "value": 50}]}, True),
    ({"any": [{"input": "region", "op": "eq", "value": "US"}, {"step": "risk", "path": "score", "op": "gt", "value": 50}]}, True),
    ({"not": {"step": "risk", "path": "score", "op": "gt", "value": 90}}, True),
])
def test_conditions(cond, expected):
    assert evaluate_condition(cond, CTX) is expected
