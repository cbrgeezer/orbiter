from __future__ import annotations

import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Protocol

from orbiter.core.state import DagRunState, TaskState


SCHEMA_PATH = Path(__file__).parent / "schema.sql"


class StateStore(Protocol):
    """Minimal interface the scheduler needs from the state store."""

    def register_dag(self, fingerprint: str, name: str, definition: dict[str, Any]) -> None: ...
    def create_dag_run(self, dag_fingerprint: str, params: dict[str, Any] | None = None) -> str: ...
    def create_task_run(
        self,
        dag_run_id: str,
        task_id: str,
        attempt: int,
        idempotency_key: str,
    ) -> str | None: ...
    def set_task_state(
        self,
        task_run_id: str,
        state: TaskState,
        *,
        error: str | None = None,
        output: Any = None,
    ) -> None: ...
    def set_dag_state(self, dag_run_id: str, state: DagRunState) -> None: ...
    def checkpoint(self, dag_run_id: str, task_id: str, key: str, value: Any) -> None: ...
    def task_run_by_idem_key(self, key: str) -> dict[str, Any] | None: ...
    def enqueue(self, task_run_id: str, *, available_at: float | None = None) -> None: ...
    def lease(self, worker_id: str, lease_seconds: float) -> dict[str, Any] | None: ...
    def ack(self, queue_item_id: int) -> None: ...
    def reclaim_expired_leases(self, now: float | None = None) -> int: ...


class SQLiteStateStore:
    """SQLite-backed state store. Single-node. WAL mode."""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def _init_schema(self) -> None:
        sql = SCHEMA_PATH.read_text()
        self._conn.executescript(sql)

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self._conn.execute("BEGIN IMMEDIATE")
        try:
            yield self._conn
            self._conn.execute("COMMIT")
        except Exception:
            self._conn.execute("ROLLBACK")
            raise

    def register_dag(
        self, fingerprint: str, name: str, definition: dict[str, Any]
    ) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT OR IGNORE INTO dags(fingerprint, name, definition, created_at) "
                "VALUES (?,?,?,?)",
                (fingerprint, name, json.dumps(definition), time.time()),
            )

    def create_dag_run(
        self, dag_fingerprint: str, params: dict[str, Any] | None = None
    ) -> str:
        run_id = str(uuid.uuid4())
        with self._tx() as c:
            c.execute(
                "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, params_json) "
                "VALUES (?,?,?,?,?)",
                (
                    run_id,
                    dag_fingerprint,
                    DagRunState.PENDING.value,
                    time.time(),
                    json.dumps(params or {}),
                ),
            )
        return run_id

    def create_task_run(
        self,
        dag_run_id: str,
        task_id: str,
        attempt: int,
        idempotency_key: str,
    ) -> str | None:
        """Create a task_run row. Returns the id, or None if it already existed
        (dedup by idempotency_key). The caller should treat None as a duplicate
        redelivery and skip execution.
        """
        tr_id = str(uuid.uuid4())
        try:
            with self._tx() as c:
                c.execute(
                    "INSERT INTO task_runs(id, dag_run_id, task_id, attempt, state, "
                    "idempotency_key, scheduled_at) VALUES (?,?,?,?,?,?,?)",
                    (
                        tr_id,
                        dag_run_id,
                        task_id,
                        attempt,
                        TaskState.PENDING.value,
                        idempotency_key,
                        time.time(),
                    ),
                )
        except sqlite3.IntegrityError:
            return None
        return tr_id

    def set_task_state(
        self,
        task_run_id: str,
        state: TaskState,
        *,
        error: str | None = None,
        output: Any = None,
    ) -> None:
        now = time.time()
        with self._tx() as c:
            if state == TaskState.RUNNING:
                c.execute(
                    "UPDATE task_runs SET state=?, started_at=? WHERE id=?",
                    (state.value, now, task_run_id),
                )
            elif state in {
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.DEAD_LETTER,
                TaskState.SKIPPED,
            }:
                c.execute(
                    "UPDATE task_runs SET state=?, finished_at=?, error=?, output_json=? "
                    "WHERE id=?",
                    (
                        state.value,
                        now,
                        error,
                        json.dumps(output) if output is not None else None,
                        task_run_id,
                    ),
                )
            else:
                c.execute(
                    "UPDATE task_runs SET state=? WHERE id=?",
                    (state.value, task_run_id),
                )

    def set_dag_state(self, dag_run_id: str, state: DagRunState) -> None:
        with self._tx() as c:
            if state in {
                DagRunState.SUCCEEDED,
                DagRunState.FAILED,
                DagRunState.CANCELLED,
            }:
                c.execute(
                    "UPDATE dag_runs SET state=?, finished_at=? WHERE id=?",
                    (state.value, time.time(), dag_run_id),
                )
            else:
                c.execute(
                    "UPDATE dag_runs SET state=? WHERE id=?",
                    (state.value, dag_run_id),
                )

    def checkpoint(
        self, dag_run_id: str, task_id: str, key: str, value: Any
    ) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO checkpoints(dag_run_id, task_id, key, value_json, written_at) "
                "VALUES (?,?,?,?,?) "
                "ON CONFLICT(dag_run_id, task_id, key) DO UPDATE SET "
                "value_json=excluded.value_json, written_at=excluded.written_at",
                (dag_run_id, task_id, key, json.dumps(value), time.time()),
            )

    def get_checkpoint(
        self, dag_run_id: str, task_id: str, key: str
    ) -> Any:
        row = self._conn.execute(
            "SELECT value_json FROM checkpoints WHERE dag_run_id=? AND task_id=? AND key=?",
            (dag_run_id, task_id, key),
        ).fetchone()
        if row is None:
            return None
        return json.loads(row["value_json"])

    def task_run_by_idem_key(self, key: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM task_runs WHERE idempotency_key=?", (key,)
        ).fetchone()
        return dict(row) if row else None

    def task_runs_for_dag_run(self, dag_run_id: str) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM task_runs WHERE dag_run_id=? ORDER BY scheduled_at",
            (dag_run_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def dag_run(self, dag_run_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM dag_runs WHERE id=?", (dag_run_id,)
        ).fetchone()
        return dict(row) if row else None

    # ---- queue side of the same store ---------------------------------------

    def enqueue(
        self, task_run_id: str, *, available_at: float | None = None
    ) -> None:
        with self._tx() as c:
            c.execute(
                "INSERT INTO queue_items(task_run_id, enqueued_at, available_at) "
                "VALUES (?,?,?)",
                (task_run_id, time.time(), available_at or time.time()),
            )

    def lease(
        self, worker_id: str, lease_seconds: float
    ) -> dict[str, Any] | None:
        now = time.time()
        with self._tx() as c:
            row = c.execute(
                "SELECT q.id AS queue_id, q.task_run_id, t.task_id, t.dag_run_id, "
                "t.attempt, t.idempotency_key "
                "FROM queue_items q JOIN task_runs t ON t.id = q.task_run_id "
                "WHERE q.available_at <= ? AND (q.lease_until IS NULL OR q.lease_until < ?) "
                "ORDER BY q.available_at LIMIT 1",
                (now, now),
            ).fetchone()
            if row is None:
                return None
            c.execute(
                "UPDATE queue_items SET lease_until=?, leased_by=? WHERE id=?",
                (now + lease_seconds, worker_id, row["queue_id"]),
            )
            return dict(row)

    def ack(self, queue_item_id: int) -> None:
        with self._tx() as c:
            c.execute("DELETE FROM queue_items WHERE id=?", (queue_item_id,))

    def reclaim_expired_leases(self, now: float | None = None) -> int:
        """Release queue items whose lease has expired. Returns count reclaimed."""
        now = now or time.time()
        with self._tx() as c:
            cur = c.execute(
                "UPDATE queue_items SET lease_until=NULL, leased_by=NULL "
                "WHERE lease_until IS NOT NULL AND lease_until < ?",
                (now,),
            )
            return cur.rowcount

    def close(self) -> None:
        self._conn.close()
