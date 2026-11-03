"""Challenge D: dynamic JSON validation. No Temporal server needed."""
import json
from pathlib import Path

import jsonschema
import pytest

from business import known_operations
from wfengine.validation import DefinitionValidationError, validate_definition

SCHEMA = json.loads((Path(__file__).parent.parent / "wfengine/schema/workflow.schema.json").read_text())

VALID = {
    "workflow_id": "order-processing-001",
    "steps": [
        {"id": "validate", "type": "activity", "name": "validate_order", "input": {"order_id": "ORD-1001"}},
        {"id": "payment", "type": "activity", "name": "process_payment", "depends_on": ["validate"], "retry": {"maximum_attempts": 3}},
        {"id": "notification", "type": "activity", "name": "send_notification", "depends_on": ["payment"]},
    ],
}


def step(id, deps=(), **kw):
    return {"id": id, "type": "activity", "name": "echo", "depends_on": list(deps), **kw}


def codes(defn, **kw):
    with pytest.raises(DefinitionValidationError) as ei:
        validate_definition(defn, **kw)
    return {i.code for i in ei.value.issues}


def test_assignment_example_is_valid_and_ordered():
    spec = validate_definition(VALID, known_operations=known_operations())
    assert spec.order == ("validate", "payment", "notification")
    assert spec.steps["payment"].retry.maximum_attempts == 3


def test_topological_order_is_stable_and_respects_dependencies():
    spec = validate_definition({"workflow_id": "w", "steps": [step("c", ["a", "b"]), step("a"), step("b")]})
    assert spec.order == ("a", "b", "c")


@pytest.mark.parametrize("bad", [[], "text", 5, None])
def test_non_object_rejected(bad):
    assert "INVALID_TYPE" in codes(bad)


def test_missing_required_fields():
    assert {"MISSING_FIELD"} <= codes({"steps": [step("a")]})
    assert {"MISSING_FIELD"} <= codes({"workflow_id": "w"})
    assert "EMPTY_WORKFLOW" in codes({"workflow_id": "w", "steps": []})


def test_duplicate_step_ids():
    assert "DUPLICATE_STEP_ID" in codes({"workflow_id": "w", "steps": [step("a"), step("a")]})


def test_missing_dependency():
    assert "MISSING_DEPENDENCY" in codes({"workflow_id": "w", "steps": [step("a", ["ghost"])]})


def test_self_dependency():
    assert "SELF_DEPENDENCY" in codes({"workflow_id": "w", "steps": [step("a", ["a"])]})


def test_circular_dependency_reports_the_cycle():
    with pytest.raises(DefinitionValidationError) as ei:
        validate_definition({"workflow_id": "w", "steps": [step("a", ["c"]), step("b", ["a"]), step("c", ["b"])]})
    msgs = [i.message for i in ei.value.issues if i.code == "CIRCULAR_DEPENDENCY"]
    assert msgs and all(x in msgs[0] for x in ("a", "b", "c"))


def test_unsupported_step_type():
    assert "UNSUPPORTED_STEP_TYPE" in codes({"workflow_id": "w", "steps": [{"id": "a", "type": "teleport", "name": "x"}]})


def test_unsupported_operation_when_registry_given():
    assert "UNSUPPORTED_OPERATION" in codes({"workflow_id": "w", "steps": [step("a", name="nope")]},
                                            known_operations=known_operations())


def test_unknown_fields_rejected():
    assert "UNKNOWN_FIELD" in codes({"workflow_id": "w", "steps": [step("a")], "surprise": 1})
    assert "UNKNOWN_FIELD" in codes({"workflow_id": "w", "steps": [step("a", bogus=True)]})


@pytest.mark.parametrize("retry", [{"maximum_attempts": -1}, {"backoff_coefficient": 0.5}, {"initial_interval_seconds": 0},
                                   {"maximum_attempts": "3"}, {"initial_interval_seconds": 5, "maximum_interval_seconds": 1}])
def test_invalid_retry(retry):
    assert "INVALID_VALUE" in codes({"workflow_id": "w", "steps": [step("a", retry=retry)]})


def test_invalid_timeouts():
    assert "INVALID_VALUE" in codes({"workflow_id": "w", "steps": [step("a", timeouts={"start_to_close_seconds": -3})]})
    assert "INVALID_VALUE" in codes({"workflow_id": "w", "steps": [step("a", timeouts={"start_to_close_seconds": 10, "schedule_to_close_seconds": 5})]})


def test_type_specific_fields():
    assert "MISSING_FIELD" in codes({"workflow_id": "w", "steps": [{"id": "a", "type": "wait_signal"}]})
    assert "MISSING_FIELD" in codes({"workflow_id": "w", "steps": [{"id": "a", "type": "delay"}]})
    assert "INVALID_FIELD_FOR_TYPE" in codes({"workflow_id": "w", "steps": [step("a", signal="x")]})


def test_reference_must_target_an_ancestor():
    d = {"workflow_id": "w", "steps": [step("a"), step("b", input={"x": "${steps.a.output.v}"})]}  # no depends_on
    assert "INVALID_REFERENCE" in codes(d)
    d["steps"][1]["depends_on"] = ["a"]
    validate_definition(d)


def test_reference_to_unknown_step_and_bad_syntax():
    assert "INVALID_REFERENCE" in codes({"workflow_id": "w", "steps": [step("a", input={"x": "${steps.zzz.output.v}"})]})
    assert "INVALID_REFERENCE" in codes({"workflow_id": "w", "steps": [step("a", input={"x": "${bogus}"})]})


def test_condition_validation():
    base = [step("a")]
    ok = {"step": "a", "path": "v", "op": "eq", "value": 1}
    validate_definition({"workflow_id": "w", "steps": base + [step("b", ["a"], condition=ok)]})
    for bad in ({"step": "a", "op": "xx", "value": 1}, {"op": "eq", "value": 1}, {"step": "a", "op": "eq"},
                {"all": []}, "nope", {"step": "ghost", "op": "exists"}):
        assert codes({"workflow_id": "w", "steps": base + [step("b", ["a"], condition=bad)]}) & {"INVALID_CONDITION"}
    # condition on a step that is not an ancestor
    assert "INVALID_REFERENCE" in codes({"workflow_id": "w", "steps": base + [step("b", condition=ok)]})


def test_all_issues_reported_together():
    d = {"workflow_id": "w", "steps": [step("a", ["x"]), step("a"), {"id": "b", "type": "bad"}]}
    assert {"MISSING_DEPENDENCY", "DUPLICATE_STEP_ID", "UNSUPPORTED_STEP_TYPE"} <= codes(d)


def test_defaults_are_inherited_and_overridable():
    spec = validate_definition({"workflow_id": "w", "defaults": {"retry": {"maximum_attempts": 7}, "timeouts": {"start_to_close_seconds": 9}},
                                "steps": [step("a"), step("b", retry={"maximum_attempts": 2})]})
    assert spec.steps["a"].retry.maximum_attempts == 7
    assert spec.steps["b"].retry.maximum_attempts == 2
    assert spec.steps["a"].timeouts.start_to_close_seconds == 9


# --- the published JSON Schema must agree with the validator on structure --------------------
STRUCTURAL_CASES = [
    (VALID, True),
    ({"workflow_id": "w", "steps": [step("a")], "surprise": 1}, False),
    ({"workflow_id": "w", "steps": []}, False),
    ({"steps": [step("a")]}, False),
    ({"workflow_id": "w", "steps": [{"id": "a", "type": "teleport"}]}, False),
    ({"workflow_id": "w", "steps": [{"id": "a", "type": "activity"}]}, False),
    ({"workflow_id": "w", "steps": [step("a", retry={"maximum_attempts": -1})]}, False),
    ({"workflow_id": "w", "steps": [{"id": "s", "type": "wait_signal", "signal": "go", "timeout_seconds": 30}]}, True),
    ({"workflow_id": "w", "steps": [{"id": "d", "type": "delay", "seconds": 5}]}, True),
]


@pytest.mark.parametrize("doc,ok", STRUCTURAL_CASES)
def test_schema_and_validator_agree(doc, ok):
    schema_ok = jsonschema.Draft202012Validator(SCHEMA).is_valid(doc)
    try:
        validate_definition(doc)
        validator_ok = True
    except DefinitionValidationError:
        validator_ok = False
    assert schema_ok == validator_ok == ok
