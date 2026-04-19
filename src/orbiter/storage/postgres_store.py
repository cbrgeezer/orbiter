from __future__ import annotations

import time
import uuid
from pathlib import Path
from typing import Any

import psycopg
from psycopg.errors import UniqueViolation
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from orbiter.core.state import DagRunState, TaskState


SCHEMA_PATH = Path(__file__).parent / "postgres_schema.sql"


class PostgresStateStore:
    """PostgreSQL backed state store for multi process runtimes."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn = psycopg.connect(dsn, autocommit=True, row_factory=dict_row)
        self._init_schema()

    def _init_schema(self) -> None:
        sql = SCHEMA_PATH.read_text()
        with self._conn.cursor() as cur:
            cur.execute(sql)
            cur.execute(
                "ALTER TABLE schedules ADD COLUMN IF NOT EXISTS overlap_policy TEXT NOT NULL DEFAULT 'allow'"
            )

    def register_dag(
        self, fingerprint: str, name: str, definition: dict[str, Any]
    ) -> None:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dags(fingerprint, name, definition, created_at) "
                "VALUES (%s,%s,%s,%s) "
                "ON CONFLICT (fingerprint) DO NOTHING",
                (fingerprint, name, Jsonb(definition), time.time()),
            )

    def create_dag_run(
        self,
        dag_fingerprint: str,
        params: dict[str, Any] | None = None,
        *,
        trigger: str | None = None,
    ) -> str:
        run_id = str(uuid.uuid4())
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, trigger, params_json) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    run_id,
                    dag_fingerprint,
                    DagRunState.PENDING.value,
                    time.time(),
                    trigger,
                    Jsonb(params or {}),
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
        task_run_id = str(uuid.uuid4())
        try:
            with self._conn.transaction(), self._conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO task_runs(id, dag_run_id, task_id, attempt, state, idempotency_key, scheduled_at) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                    (
                        task_run_id,
                        dag_run_id,
                        task_id,
                        attempt,
                        TaskState.PENDING.value,
                        idempotency_key,
                        time.time(),
                    ),
                )
        except UniqueViolation:
            return None
        return task_run_id

    def set_task_state(
        self,
        task_run_id: str,
        state: TaskState,
        *,
        error: str | None = None,
        output: Any = None,
    ) -> None:
        now = time.time()
        with self._conn.transaction(), self._conn.cursor() as cur:
            if state == TaskState.RUNNING:
                cur.execute(
                    "UPDATE task_runs SET state=%s, started_at=%s WHERE id=%s",
                    (state.value, now, task_run_id),
                )
            elif state in {
                TaskState.SUCCEEDED,
                TaskState.FAILED,
                TaskState.DEAD_LETTER,
                TaskState.SKIPPED,
            }:
                cur.execute(
                    "UPDATE task_runs SET state=%s, finished_at=%s, error=%s, output_json=%s WHERE id=%s",
                    (
                        state.value,
                        now,
                        error,
                        Jsonb(output) if output is not None else None,
                        task_run_id,
                    ),
                )
            else:
                cur.execute(
                    "UPDATE task_runs SET state=%s WHERE id=%s",
                    (state.value, task_run_id),
                )

    def set_dag_state(self, dag_run_id: str, state: DagRunState) -> None:
        with self._conn.transaction(), self._conn.cursor() as cur:
            if state in {
                DagRunState.SUCCEEDED,
                DagRunState.FAILED,
                DagRunState.CANCELLED,
            }:
                cur.execute(
                    "UPDATE dag_runs SET state=%s, finished_at=%s WHERE id=%s",
                    (state.value, time.time(), dag_run_id),
                )
            else:
                cur.execute(
                    "UPDATE dag_runs SET state=%s WHERE id=%s",
                    (state.value, dag_run_id),
                )

    def checkpoint(self, dag_run_id: str, task_id: str, key: str, value: Any) -> None:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO checkpoints(dag_run_id, task_id, key, value_json, written_at) "
                "VALUES (%s,%s,%s,%s,%s) "
                "ON CONFLICT (dag_run_id, task_id, key) DO UPDATE SET "
                "value_json=EXCLUDED.value_json, written_at=EXCLUDED.written_at",
                (dag_run_id, task_id, key, Jsonb(value), time.time()),
            )

    def get_checkpoint(self, dag_run_id: str, task_id: str, key: str) -> Any:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT value_json FROM checkpoints WHERE dag_run_id=%s AND task_id=%s AND key=%s",
                (dag_run_id, task_id, key),
            )
            row = cur.fetchone()
        if row is None:
            return None
        return row["value_json"]

    def task_run_by_idem_key(self, key: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM task_runs WHERE idempotency_key=%s", (key,))
            return cur.fetchone()

    def task_runs_for_dag_run(self, dag_run_id: str) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM task_runs WHERE dag_run_id=%s ORDER BY scheduled_at",
                (dag_run_id,),
            )
            rows = cur.fetchall()
        return rows

    def task_run(self, task_run_id: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM task_runs WHERE id=%s", (task_run_id,))
            return cur.fetchone()

    def dag_run(self, dag_run_id: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM dag_runs WHERE id=%s", (dag_run_id,))
            return cur.fetchone()

    def list_dag_runs(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM dag_runs ORDER BY started_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()

    def list_active_dag_runs(self, *, limit: int = 200) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM dag_runs WHERE state IN (%s, %s) ORDER BY started_at ASC LIMIT %s",
                (DagRunState.PENDING.value, DagRunState.RUNNING.value, limit),
            )
            return cur.fetchall()

    def cancel_dag_run(self, dag_run_id: str) -> bool:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "UPDATE dag_runs SET state=%s, finished_at=%s "
                "WHERE id=%s AND state IN (%s,%s)",
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
        counts: dict[str, int] = {}
        with self._conn.cursor() as cur:
            cur.execute("SELECT state, COUNT(*) AS n FROM dag_runs GROUP BY state")
            for row in cur.fetchall():
                counts[f"dag_runs_{row['state']}"] = row["n"]
            cur.execute("SELECT state, COUNT(*) AS n FROM task_runs GROUP BY state")
            for row in cur.fetchall():
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
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO activity_events(id, event_type, summary, actor, dag_run_id, schedule_id, metadata_json, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    event_id,
                    event_type,
                    summary,
                    actor,
                    dag_run_id,
                    schedule_id,
                    Jsonb(metadata or {}),
                    time.time(),
                ),
            )
        return event_id

    def list_activity(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM activity_events ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()

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
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO schedules(id, dag_fingerprint, name, state, overlap_policy, interval_seconds, next_run_at, params_json, created_at) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    schedule_id,
                    dag_fingerprint,
                    name,
                    "active",
                    overlap_policy,
                    interval_seconds,
                    first_run_at,
                    Jsonb(params or {}),
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
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM schedules ORDER BY created_at DESC LIMIT %s",
                (limit,),
            )
            return cur.fetchall()

    def schedule(self, schedule_id: str) -> dict[str, Any] | None:
        with self._conn.cursor() as cur:
            cur.execute("SELECT * FROM schedules WHERE id=%s", (schedule_id,))
            return cur.fetchone()

    def pause_schedule(self, schedule_id: str) -> bool:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "UPDATE schedules SET state='paused' WHERE id=%s AND state='active'",
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
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "UPDATE schedules SET state='active' WHERE id=%s AND state='paused'",
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
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM schedules WHERE id=%s FOR UPDATE",
                (schedule_id,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            run_id = str(uuid.uuid4())
            cur.execute(
                "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, trigger, params_json) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    run_id,
                    row["dag_fingerprint"],
                    DagRunState.PENDING.value,
                    now,
                    f"schedule:{schedule_id}:manual",
                    row["params_json"],
                ),
            )
            cur.execute(
                "UPDATE schedules SET last_run_at=%s, last_run_id=%s WHERE id=%s",
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
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM schedules "
                "WHERE state='active' AND next_run_at <= %s "
                "ORDER BY next_run_at ASC "
                "FOR UPDATE SKIP LOCKED LIMIT %s",
                (now, limit),
            )
            rows = cur.fetchall()
            for row in rows:
                cur.execute(
                    "SELECT id FROM dag_runs "
                    "WHERE trigger LIKE %s AND state IN (%s,%s)",
                    (
                        f"schedule:{row['id']}%",
                        DagRunState.PENDING.value,
                        DagRunState.RUNNING.value,
                    ),
                )
                active_run_ids = [r["id"] for r in cur.fetchall()]
                if row["overlap_policy"] == "forbid" and active_run_ids:
                    cur.execute(
                        "UPDATE schedules SET next_run_at=%s WHERE id=%s",
                        (now + row["interval_seconds"], row["id"]),
                    )
                    continue
                if row["overlap_policy"] == "replace" and active_run_ids:
                    cur.execute(
                        "UPDATE dag_runs SET state=%s, finished_at=%s WHERE id = ANY(%s)",
                        (DagRunState.CANCELLED.value, now, active_run_ids),
                    )
                run_id = str(uuid.uuid4())
                cur.execute(
                    "INSERT INTO dag_runs(id, dag_fingerprint, state, started_at, trigger, params_json) "
                    "VALUES (%s,%s,%s,%s,%s,%s)",
                    (
                        run_id,
                        row["dag_fingerprint"],
                        DagRunState.PENDING.value,
                        now,
                        f"schedule:{row['id']}",
                        row["params_json"],
                    ),
                )
                cur.execute(
                    "UPDATE schedules SET next_run_at=%s, last_run_at=%s, last_run_id=%s WHERE id=%s",
                    (
                        now + row["interval_seconds"],
                        now,
                        run_id,
                        row["id"],
                    ),
                )
                dispatched.append({"schedule_id": row["id"], "dag_run_id": run_id})
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

    def enqueue(self, task_run_id: str, *, available_at: float | None = None) -> None:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO queue_items(task_run_id, enqueued_at, available_at) VALUES (%s,%s,%s)",
                (task_run_id, time.time(), available_at or time.time()),
            )

    def lease(self, worker_id: str, lease_seconds: float) -> dict[str, Any] | None:
        now = time.time()
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "SELECT q.id AS queue_id, q.task_run_id, t.task_id, t.dag_run_id, t.attempt, t.idempotency_key "
                "FROM queue_items q "
                "JOIN task_runs t ON t.id = q.task_run_id "
                "WHERE q.available_at <= %s AND (q.lease_until IS NULL OR q.lease_until < %s) "
                "ORDER BY q.available_at "
                "FOR UPDATE SKIP LOCKED LIMIT 1",
                (now, now),
            )
            row = cur.fetchone()
            if row is None:
                return None
            cur.execute(
                "UPDATE queue_items SET lease_until=%s, leased_by=%s WHERE id=%s",
                (now + lease_seconds, worker_id, row["queue_id"]),
            )
            return row

    def ack(self, queue_item_id: int) -> None:
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute("DELETE FROM queue_items WHERE id=%s", (queue_item_id,))

    def reclaim_expired_leases(self, now: float | None = None) -> int:
        now = now or time.time()
        with self._conn.transaction(), self._conn.cursor() as cur:
            cur.execute(
                "UPDATE queue_items SET lease_until=NULL, leased_by=NULL "
                "WHERE lease_until IS NOT NULL AND lease_until < %s",
                (now,),
            )
            return cur.rowcount

    def close(self) -> None:
        self._conn.close()
