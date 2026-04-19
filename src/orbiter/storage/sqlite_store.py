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
    def create_dag_run(
        self,
        dag_fingerprint: str,
        params: dict[str, Any] | None = None,
        *,
        trigger: str | None = None,
    ) -> str: ...
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
    def get_checkpoint(self, dag_run_id: str, task_id: str, key: str) -> Any: ...
    def task_run_by_idem_key(self, key: str) -> dict[str, Any] | None: ...
    def task_run(self, task_run_id: str) -> dict[str, Any] | None: ...
    def dag_run(self, dag_run_id: str) -> dict[str, Any] | None: ...
    def list_dag_runs(self, *, limit: int = 50) -> list[dict[str, Any]]: ...
    def list_active_dag_runs(self, *, limit: int = 200) -> list[dict[str, Any]]: ...
    def cancel_dag_run(self, dag_run_id: str) -> bool: ...
    def state_counts(self) -> dict[str, int]: ...
    def record_activity(
        self,
        event_type: str,
        summary: str,
        *,
        actor: str = "system",
        dag_run_id: str | None = None,
        schedule_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str: ...
    def list_activity(self, *, limit: int = 100) -> list[dict[str, Any]]: ...
    def create_schedule(
        self,
        dag_fingerprint: str,
        name: str,
        interval_seconds: int,
        *,
        overlap_policy: str = "allow",
        params: dict[str, Any] | None = None,
        start_at: float | None = None,
    ) -> str: ...
    def list_schedules(self, *, limit: int = 100) -> list[dict[str, Any]]: ...
    def schedule(self, schedule_id: str) -> dict[str, Any] | None: ...
    def pause_schedule(self, schedule_id: str) -> bool: ...
    def resume_schedule(self, schedule_id: str) -> bool: ...
    def trigger_schedule_now(self, schedule_id: str) -> str | None: ...
    def dispatch_due_schedules(self, *, limit: int = 10, now: float | None = None) -> list[dict[str, Any]]: ...
    def enqueue(self, task_run_id: str, *, available_at: float | None = None) -> None: ...
    def lease(self, worker_id: str, lease_seconds: float) -> dict[str, Any] | None: ...
    def ack(self, queue_item_id: int) -> None: ...
    def reclaim_expired_leases(self, now: float | None = None) -> int: ...
    def close(self) -> None: ...


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
        cols = {
            row["name"]
            for row in self._conn.execute("PRAGMA table_info(schedules)").fetchall()
        }
        if "overlap_policy" not in cols:
            self._conn.execute(
                "ALTER TABLE schedules ADD COLUMN overlap_policy TEXT NOT NULL DEFAULT 'allow'"
            )

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
        self,
        dag_fingerprint: str,
        params: dict[str, Any] | None = None,
        *,
        trigger: str | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        with self._tx() as c:
            c.execute(
                "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, trigger, params_json) "
                "VALUES (?,?,?,?,?,?)",
                (
                    run_id,
                    dag_fingerprint,
                    DagRunState.PENDING.value,
                    time.time(),
                    trigger,
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

    def task_run(self, task_run_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM task_runs WHERE id=?", (task_run_id,)
        ).fetchone()
        return dict(row) if row else None

    def dag_run(self, dag_run_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM dag_runs WHERE id=?", (dag_run_id,)
        ).fetchone()
        return dict(row) if row else None

    def list_dag_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM dag_runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_active_dag_runs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM dag_runs WHERE state IN (?, ?) ORDER BY started_at ASC LIMIT ?",
            (DagRunState.PENDING.value, DagRunState.RUNNING.value, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def cancel_dag_run(self, dag_run_id: str) -> bool:
        with self._tx() as c:
            cur = c.execute(
                "UPDATE dag_runs SET state=?, finished_at=? "
                "WHERE id=? AND state IN (?,?)",
                (
                    DagRunState.CANCELLED.value,
                    time.time(),
                    dag_run_id,
                    DagRunState.PENDING.value,
                    DagRunState.RUNNING.value,
                ),
            )
            return cur.rowcount > 0

    def state_counts(self) -> dict[str, int]:
        dag_rows = self._conn.execute(
            "SELECT state, COUNT(*) AS n FROM dag_runs GROUP BY state"
        ).fetchall()
        task_rows = self._conn.execute(
            "SELECT state, COUNT(*) AS n FROM task_runs GROUP BY state"
        ).fetchall()
        counts: dict[str, int] = {}
        for row in dag_rows:
            counts[f"dag_runs_{row['state']}"] = row["n"]
        for row in task_rows:
            counts[f"task_runs_{row['state']}"] = row["n"]
        return counts

    def record_activity(
        self,
        event_type: str,
        summary: str,
        *,
        actor: str = "system",
        dag_run_id: str | None = None,
        schedule_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        event_id = str(uuid.uuid4())
        with self._tx() as c:
            c.execute(
                "INSERT INTO activity_events(id, event_type, summary, actor, dag_run_id, schedule_id, metadata_json, created_at) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (
                    event_id,
                    event_type,
                    summary,
                    actor,
                    dag_run_id,
                    schedule_id,
                    json.dumps(metadata or {}),
                    time.time(),
                ),
            )
        return event_id

    def list_activity(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM activity_events ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        events = [dict(r) for r in rows]
        for event in events:
            event["metadata"] = json.loads(event.pop("metadata_json") or "{}")
        return events

    def create_schedule(
        self,
        dag_fingerprint: str,
        name: str,
        interval_seconds: int,
        *,
        overlap_policy: str = "allow",
        params: dict[str, Any] | None = None,
        start_at: float | None = None,
    ) -> str:
        schedule_id = str(uuid.uuid4())
        now = time.time()
        first_run_at = start_at if start_at is not None else now + interval_seconds
        with self._tx() as c:
            c.execute(
                "INSERT INTO schedules(id, dag_fingerprint, name, state, overlap_policy, interval_seconds, next_run_at, params_json, created_at) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    schedule_id,
                    dag_fingerprint,
                    name,
                    "active",
                    overlap_policy,
                    interval_seconds,
                    first_run_at,
                    json.dumps(params or {}),
                    now,
                ),
            )
        self.record_activity(
            "schedule.created",
            f"Created schedule {name}",
            schedule_id=schedule_id,
            metadata={"interval_seconds": interval_seconds, "overlap_policy": overlap_policy},
        )
        return schedule_id

    def list_schedules(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            "SELECT * FROM schedules ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def schedule(self, schedule_id: str) -> dict[str, Any] | None:
        row = self._conn.execute(
            "SELECT * FROM schedules WHERE id=?",
            (schedule_id,),
        ).fetchone()
        return dict(row) if row else None

    def pause_schedule(self, schedule_id: str) -> bool:
        with self._tx() as c:
            cur = c.execute(
                "UPDATE schedules SET state='paused' WHERE id=? AND state='active'",
                (schedule_id,),
            )
            changed = cur.rowcount > 0
        if changed:
            self.record_activity(
                "schedule.paused",
                f"Paused schedule {schedule_id}",
                schedule_id=schedule_id,
            )
        return changed

    def resume_schedule(self, schedule_id: str) -> bool:
        with self._tx() as c:
            cur = c.execute(
                "UPDATE schedules SET state='active' WHERE id=? AND state='paused'",
                (schedule_id,),
            )
            changed = cur.rowcount > 0
        if changed:
            self.record_activity(
                "schedule.resumed",
                f"Resumed schedule {schedule_id}",
                schedule_id=schedule_id,
            )
        return changed

    def trigger_schedule_now(self, schedule_id: str) -> str | None:
        now = time.time()
        with self._tx() as c:
            row = c.execute(
                "SELECT * FROM schedules WHERE id=?",
                (schedule_id,),
            ).fetchone()
            if row is None:
                return None
            run_id = str(uuid.uuid4())
            c.execute(
                "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, trigger, params_json) "
                "VALUES (?,?,?,?,?,?)",
                (
                    run_id,
                    row["dag_fingerprint"],
                    DagRunState.PENDING.value,
                    now,
                    f"schedule:{schedule_id}:manual",
                    row["params_json"],
                ),
            )
            c.execute(
                "UPDATE schedules SET last_run_at=?, last_run_id=? WHERE id=?",
                (now, run_id, schedule_id),
            )
        self.record_activity(
            "schedule.run_now",
            f"Triggered schedule {schedule_id} for immediate execution",
            dag_run_id=run_id,
            schedule_id=schedule_id,
        )
        return run_id

    def dispatch_due_schedules(
        self, *, limit: int = 10, now: float | None = None
    ) -> list[dict[str, Any]]:
        now = now or time.time()
        dispatched: list[dict[str, Any]] = []
        activity_events: list[dict[str, Any]] = []
        with self._tx() as c:
            rows = c.execute(
                "SELECT * FROM schedules "
                "WHERE state='active' AND next_run_at <= ? "
                "ORDER BY next_run_at ASC LIMIT ?",
                (now, limit),
            ).fetchall()
            for row in rows:
                active_run_ids = [
                    r["id"]
                    for r in c.execute(
                        "SELECT id FROM dag_runs "
                        "WHERE trigger LIKE ? AND state IN (?,?)",
                        (
                            f"schedule:{row['id']}%",
                            DagRunState.PENDING.value,
                            DagRunState.RUNNING.value,
                        ),
                    ).fetchall()
                ]
                if row["overlap_policy"] == "forbid" and active_run_ids:
                    c.execute(
                        "UPDATE schedules SET next_run_at=? WHERE id=?",
                        (now + row["interval_seconds"], row["id"]),
                    )
                    continue
                if row["overlap_policy"] == "replace" and active_run_ids:
                    c.execute(
                        "UPDATE dag_runs SET state=?, finished_at=? "
                        "WHERE id IN ({})".format(",".join("?" for _ in active_run_ids)),
                        [DagRunState.CANCELLED.value, now, *active_run_ids],
                    )
                run_id = str(uuid.uuid4())
                c.execute(
                    "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, trigger, params_json) "
                    "VALUES (?,?,?,?,?,?)",
                    (
                        run_id,
                        row["dag_fingerprint"],
                        DagRunState.PENDING.value,
                        now,
                        f"schedule:{row['id']}",
                        row["params_json"],
                    ),
                )
                c.execute(
                    "UPDATE schedules SET next_run_at=?, last_run_at=?, last_run_id=? WHERE id=?",
                    (
                        now + row["interval_seconds"],
                        now,
                        run_id,
                        row["id"],
                    ),
                )
                dispatched.append(
                    {
                        "schedule_id": row["id"],
                        "dag_run_id": run_id,
                    }
                )
                activity_events.append(
                    {
                        "event_type": "schedule.dispatched",
                        "summary": f"Dispatched scheduled run for {row['name']}",
                        "dag_run_id": run_id,
                        "schedule_id": row["id"],
                        "metadata": {"trigger": f"schedule:{row['id']}"},
                    }
                )
        for event in activity_events:
            self.record_activity(
                event["event_type"],
                event["summary"],
                dag_run_id=event["dag_run_id"],
                schedule_id=event["schedule_id"],
                metadata=event["metadata"],
            )
        return dispatched

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
