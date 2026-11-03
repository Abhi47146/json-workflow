"""Tiny SQLite persistence for the demo business domain.

Stands in for "the systems of record": an idempotency ledger, a fake payment provider and
a notification outbox. SQLite in WAL mode lets several worker processes on one host (or a
shared docker volume) share it; production would use Postgres.
"""
import json
import os
import sqlite3
import time
import uuid
from contextlib import contextmanager
from typing import Any, Optional

_SCHEMA = """
CREATE TABLE IF NOT EXISTS invocations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    idempotency_key TEXT NOT NULL, operation TEXT NOT NULL, attempt INTEGER NOT NULL, started_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS operation_results (
    idempotency_key TEXT PRIMARY KEY, operation TEXT NOT NULL, result_json TEXT NOT NULL, recorded_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS provider_charges (
    idempotency_key TEXT PRIMARY KEY, reference TEXT NOT NULL, amount REAL, charge_id TEXT NOT NULL, created_at REAL NOT NULL);
CREATE TABLE IF NOT EXISTS notifications (
    idempotency_key TEXT PRIMARY KEY, recipient TEXT, body TEXT, created_at REAL NOT NULL);
"""


def db_path() -> str:
    explicit = os.environ.get("BUSINESS_DB_PATH")
    if explicit:
        return explicit
    return os.path.join(os.environ.get("DATA_DIR", "./data"), "business.db")


@contextmanager
def connect():
    path = os.path.abspath(db_path())
    os.makedirs(os.path.dirname(path), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, isolation_level=None)
    try:
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(_SCHEMA)
        yield conn
    finally:
        conn.close()


# ------------------------------------------------------------------ audit / ledger
def record_invocation(key: str, operation: str, attempt: int) -> None:
    with connect() as c:
        c.execute("INSERT INTO invocations(idempotency_key, operation, attempt, started_at) VALUES (?,?,?,?)",
                  (key, operation, attempt, time.time()))


def invocation_count(key: str) -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM invocations WHERE idempotency_key=?", (key,)).fetchone()[0]


def invocation_count_prefix(prefix: str) -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM invocations WHERE idempotency_key LIKE ?", (prefix + "%",)).fetchone()[0]


def ledger_get(key: str) -> Optional[dict]:
    with connect() as c:
        row = c.execute("SELECT result_json FROM operation_results WHERE idempotency_key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def ledger_record(key: str, operation: str, result: Any) -> dict:
    """First writer wins; always returns the stored result (so concurrent duplicates agree)."""
    with connect() as c:
        c.execute("INSERT OR IGNORE INTO operation_results(idempotency_key, operation, result_json, recorded_at) VALUES (?,?,?,?)",
                  (key, operation, json.dumps(result), time.time()))
        row = c.execute("SELECT result_json FROM operation_results WHERE idempotency_key=?", (key,)).fetchone()
    return json.loads(row[0])


# ------------------------------------------------------------ fake external systems
def provider_charge(key: str, reference: str, amount: Optional[float]) -> dict:
    """Fake payment provider that honours an Idempotency-Key like Stripe does: a repeated key
    returns the ORIGINAL charge and never charges twice."""
    with connect() as c:
        c.execute("INSERT OR IGNORE INTO provider_charges(idempotency_key, reference, amount, charge_id, created_at) VALUES (?,?,?,?,?)",
                  (key, reference, amount, "ch_" + uuid.uuid4().hex[:14], time.time()))
        row = c.execute("SELECT charge_id, reference, amount FROM provider_charges WHERE idempotency_key=?", (key,)).fetchone()
    return {"charge_id": row[0], "reference": row[1], "amount": row[2]}


def provider_charge_count(reference: str) -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM provider_charges WHERE reference=?", (reference,)).fetchone()[0]


def send_notification_once(key: str, recipient: str, body: str) -> bool:
    """Outbox with a unique key. Returns True if this call actually sent it."""
    with connect() as c:
        cur = c.execute("INSERT OR IGNORE INTO notifications(idempotency_key, recipient, body, created_at) VALUES (?,?,?,?)",
                        (key, recipient, body, time.time()))
        return cur.rowcount == 1


def notification_count(recipient: str) -> int:
    with connect() as c:
        return c.execute("SELECT COUNT(*) FROM notifications WHERE recipient=?", (recipient,)).fetchone()[0]
