# Video walkthrough script (target 8 to 10 minutes)

The video itself has to be recorded by you; this is the shot list. Screen-record with `docker compose up` already running, Temporal UI (http://localhost:8233) in one window, terminal in another.

| Time | Show | Say |
|---|---|---|
| 0:00 | `docs/ARCHITECTURE.md` diagram | Three deployables: API, workers, Temporal. One generic interpreter workflow; JSON is data, not code. Business logic lives behind a registry and knows nothing about Temporal. |
| 1:00 | `examples/order-processing.json`, `POST /workflows`, UI event history | Validation first, then `start_workflow`. Each step is an activity scheduled by the interpreter in dependency order; show the history. |
| 2:30 | `POST` `examples/invalid-circular.json`, a malformed body | 422 with all issues and the cycle path; 400 for bad JSON; nothing appears in Temporal. |
| 3:30 | `parallel-and-conditional.json` in the UI | Parallel branches overlap in the timeline; condition skips; `wait_signal` shows WAITING_SIGNAL; send the signal via the API. |
| 5:00 | `./scripts/demo_crash.sh` | Worker dies after the charge succeeded but before recording. Workflow stays RUNNING, worker returns, heartbeat timeout, retry. `validate` ran once; payment attempted twice, charged once: stable idempotency key + provider dedupe + ledger. |
| 6:30 | `workflows.py` | Determinism: only Temporal APIs, no I/O, stable order, pure condition DSL; what is in workflow vs activity. Replay test. |
| 7:30 | Duplicate submission, cancel, pause/resume | Same id twice returns the same run; cancel propagates to running activities. |
| 8:30 | Final-review answers | Replay rebuilds state; versioning via definition-as-input and `patched`/worker versioning; scaling by replicas and queues; limits (no sagas yet, SQLite stand-in, payload size). |
