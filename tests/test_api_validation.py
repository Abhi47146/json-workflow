"""API-level rejection: invalid input never reaches Temporal. Uses a stub client, no server."""
import json

import httpx
import pytest

from wfengine.api import create_app
from wfengine.config import Settings


class StubClient:
    def __init__(self):
        self.started = []

    async def start_workflow(self, *a, **kw):  # must never be reached for invalid input
        self.started.append((a, kw))
        raise AssertionError("start_workflow must not be called for an invalid definition")

    def get_workflow_handle(self, *_a, **_k):
        raise AssertionError("no Temporal lookup expected before validation passes")


@pytest.fixture
async def http():
    stub = StubClient()
    app = create_app(client=stub, settings=Settings())
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        yield c, stub


def step(id, deps=(), name="echo"):
    return {"id": id, "type": "activity", "name": name, "depends_on": list(deps)}


async def test_malformed_json_is_rejected_with_400(http):
    c, stub = http
    r = await c.post("/workflows", content=b'{"workflow_id": "x", "steps": [', headers={"content-type": "application/json"})
    assert r.status_code == 400 and r.json()["error"]["code"] == "MALFORMED_JSON"
    assert stub.started == []


async def test_circular_dependency_is_rejected_with_422(http):
    c, stub = http
    r = await c.post("/workflows", content=json.dumps({"workflow_id": "x", "steps": [step("a", ["b"]), step("b", ["a"])]}))
    assert r.status_code == 422
    issues = r.json()["error"]["issues"]
    assert any(i["code"] == "CIRCULAR_DEPENDENCY" for i in issues)
    assert stub.started == []


@pytest.mark.parametrize("definition,code", [
    ({"workflow_id": "x", "steps": [step("a", ["ghost"])]}, "MISSING_DEPENDENCY"),
    ({"workflow_id": "x", "steps": [step("a"), step("a")]}, "DUPLICATE_STEP_ID"),
    ({"workflow_id": "x", "steps": [{"id": "a", "type": "teleport", "name": "x"}]}, "UNSUPPORTED_STEP_TYPE"),
    ({"workflow_id": "x", "steps": [step("a", name="not_a_real_operation")]}, "UNSUPPORTED_OPERATION"),
    ({"steps": [step("a")]}, "MISSING_FIELD"),
])
async def test_semantic_rejections(http, definition, code):
    c, stub = http
    r = await c.post("/workflows", content=json.dumps(definition))
    assert r.status_code == 422
    assert code in {i["code"] for i in r.json()["error"]["issues"]}
    assert stub.started == []


async def test_dry_run_validate_endpoint_returns_execution_order(http):
    c, _ = http
    r = await c.post("/workflows/validate", content=json.dumps({"workflow_id": "x", "steps": [step("b", ["a"]), step("a")]}))
    assert r.status_code == 200 and r.json()["execution_order"] == ["a", "b"]


async def test_schema_and_operations_endpoints(http):
    c, _ = http
    assert (await c.get("/schema")).json()["title"] == "JSON Workflow Definition"
    assert "process_payment" in (await c.get("/operations")).json()["operations"]
    assert (await c.get("/health")).json() == {"status": "ok"}
