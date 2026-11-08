# JSON Workflow Execution Engine on Temporal

Submit a JSON workflow definition over HTTP; it is validated, then interpreted and executed durably by Temporal workers (dependencies, parallel branches, conditions, retries, timeouts, signals, pause/resume, cancellation, crash recovery, idempotent side effects).

* Design, diagrams, challenge write-ups, trade-offs: **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)**
* JSON schema: [wfengine/schema/workflow.schema.json](wfengine/schema/workflow.schema.json) (also served at `GET /schema`)

## Layout

```
wfengine/   validation.py  dag.py  workflows.py  activities.py  worker.py  api.py  config.py  logging_setup.py
business/   registry.py  operations.py  storage.py        # Temporal-agnostic business logic
tests/      unit (no server) + integration (Temporal server) + real worker-crash test
examples/   order-processing.json  parallel-and-conditional.json  crash-demo.json  invalid-circular.json
```

## Run locally with Docker

```bash
docker compose up --build -d              # Temporal (UI :8233), API (:8000), 1 worker
docker compose up -d --scale worker=3     # optional: more workers
curl -s -X POST localhost:8000/workflows -H 'content-type: application/json' --data @examples/order-processing.json | jq
curl -s localhost:8000/workflows/order-processing-001 | jq
```

Without Docker: run a Temporal dev server (`temporal server start-dev`), then
`pip install -r requirements-dev.txt`, `make worker` and `make api` in two terminals.

## API

| Method & path | Purpose |
|---|---|
| `POST /workflows` | Submit. `202` started, `200 duplicate:true` (same id and definition), `400` malformed JSON, `422` invalid definition (all issues listed), `409` same id with a different definition |
| `POST /workflows/validate` | Dry-run validation; returns execution order |
| `GET /workflows/{id}` | Status: Temporal status, per-step state/output/error, pending activity attempts |
| `POST /workflows/{id}/cancel` | Cancel (running steps are cancelled, not-started steps marked cancelled) |
| `POST /workflows/{id}/pause` / `resume` | Stop/allow *starting* new steps (running steps finish) |
| `POST /workflows/{id}/signal/{event}` | Body `{"payload": ..., "event_id": "..."}`; resumes `wait_signal` steps |
| `GET /operations`, `GET /schema`, `GET /health` | Introspection |

## Workflow format (short)

Step types: `activity` (`name` = registered operation), `wait_signal` (`signal`, optional `timeout_seconds`), `delay` (`seconds`).
Per step: `depends_on`, `input` (supports `${steps.<id>.output.<path>}` and `${input.<path>}`), `condition` (JSON DSL: `eq ne gt gte lt lte in contains exists truthy`, combinable with `all/any/not`), `retry`, `timeouts`, `on_failure` (`fail|continue`), `trigger_rule` (`all_success|all_done`), `task_queue`.
Workflow level: `failure_policy` (`fail_fast|wait_all`), `input`, `defaults`, `task_queue`, `version`. See `examples/parallel-and-conditional.json`.

## Tests

```bash
pip install -r requirements-dev.txt
make test-unit          # validator, DAG logic, idempotency, API rejection. No server needed
make test-integration   # needs Temporal: auto-starts the CLI dev server (downloads it on first run),
                        # or: TEMPORAL_TEST_ADDRESS=localhost:7233 pytest -m integration (e.g. against `docker compose up temporal`)
```

| Required scenario | Test |
|---|---|
| Valid JSON executes | `test_assignment_example_executes_in_dependency_order` |
| Invalid JSON rejected | `test_api_validation.py::test_malformed_json...`, `test_semantic_rejections` |
| Circular dependency | `test_validation.py::test_circular_dependency...`, `test_api_validation.py::test_circular...` |
| Activity failure / retry policy | `test_retry_policy_applies_then_succeeds`, `test_retries_are_bounded...`, `test_non_retryable...`, `test_activity_timeout...` |
| Worker crash recovers | `test_worker_crash.py` (real process kill) |
| Duplicate submission | `test_api_integration.py::test_duplicate_submission_executes_business_logic_once` |
| Parallel steps | `test_independent_steps_run_concurrently`, `test_parallel_branch_failure_*` |
| Cancellation | `test_cancellation_stops_running_work` |
| External signal | `test_external_signal_resumes_waiting_workflow` |
| State survives worker restart | `test_waiting_workflow_survives_worker_restart` |
| Deterministic replay | `test_history_replays_deterministically` |

## Demonstrating failure and recovery

```bash
docker compose up --build -d && ./scripts/demo_crash.sh
```
The `payment` step kills its worker process right after the (fake) provider charge succeeds. The script shows the workflow still `RUNNING`, a worker coming back, completion, and that `validate` ran once, `payment` twice, with exactly one charge. You can also `docker compose kill worker` during any `sleep` step and watch the heartbeat timeout and retry in the UI at http://localhost:8233.

## Configuration (env)

`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TASK_QUEUE` / `TASK_QUEUES`, `WORKER_MAX_CONCURRENT_ACTIVITIES`, `WORKER_MAX_CONCURRENT_WORKFLOW_TASKS`, `GRACEFUL_SHUTDOWN_SECONDS`, `LOG_LEVEL`, `LOG_FORMAT` (`json|console`), `QUERY_TIMEOUT_SECONDS`, `STRICT_OPERATIONS`, `DATA_DIR` / `BUSINESS_DB_PATH`, `ENABLE_FAULT_INJECTION` (demo/tests only).

Logs are one JSON object per line with `workflow_id`, `run_id`, `activity_id`, `attempt`, plus event fields.
