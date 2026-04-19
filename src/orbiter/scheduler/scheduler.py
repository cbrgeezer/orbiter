from __future__ import annotations

import asyncio
import logging
from typing import Any

from orbiter.core.dag import DAG
from orbiter.core.state import DagRunState, TaskState, TERMINAL_TASK_STATES
from orbiter.queue.queue import Queue, QueueMessage
from orbiter.storage.sqlite_store import StateStore


log = logging.getLogger("orbiter.scheduler")


class Scheduler:
    """Owns the dispatch loop for a DAG.

    The scheduler reads task_run state from the store, decides which tasks
    are ready, and pushes QueueMessages to the queue. It does not execute
    user code — that is the worker's job. This separation means you can run
    scheduler and workers on different hosts.
    """

    def __init__(
        self,
        dag: DAG,
        queue: Queue,
        store: StateStore,
        *,
        poll_interval: float = 0.1,
    ) -> None:
        self.dag = dag.finalize() if not getattr(dag, "_finalized", False) else dag
        self.queue = queue
        self.store = store
        self.poll_interval = poll_interval
        self._stop = asyncio.Event()

        # Wire worker-driven retries back through the scheduler so the store
        # stays authoritative about (task, attempt, idempotency_key).
        setattr(self.queue, "_retry_sink", self._retry_sink)

    async def submit(self, params: dict[str, Any] | None = None) -> str:
        self.store.register_dag(
            self.dag.fingerprint(), self.dag.name, self.dag.to_dict()
        )
        dag_run_id = self.store.create_dag_run(
            self.dag.fingerprint(), params or {}
        )
        self.store.set_dag_state(dag_run_id, DagRunState.RUNNING)
        return dag_run_id

    async def run_until_complete(
        self, dag_run_id: str, *, timeout: float | None = None
    ) -> DagRunState:
        """Drive the DAG forward until it reaches a terminal state."""
        row = self.store.dag_run(dag_run_id)
        if row is not None and row["state"] == DagRunState.PENDING.value:
            self.store.set_dag_state(dag_run_id, DagRunState.RUNNING)
        await self._dispatch_ready(dag_run_id)
        deadline = None if timeout is None else asyncio.get_event_loop().time() + timeout
        while True:
            final = self._terminal_state(dag_run_id)
            if final is not None:
                return final
            if deadline is not None and asyncio.get_event_loop().time() > deadline:
                self.store.set_dag_state(dag_run_id, DagRunState.FAILED)
                return DagRunState.FAILED
            await asyncio.sleep(self.poll_interval)
            await self._dispatch_ready(dag_run_id)

    def stop(self) -> None:
        self._stop.set()

    async def _dispatch_ready(self, dag_run_id: str) -> None:
        dag_row = self.store.dag_run(dag_run_id)
        if dag_row is None:
            return
        if dag_row["state"] == DagRunState.CANCELLED.value:
            return
        rows = self.store.task_runs_for_dag_run(dag_run_id)
        # latest state per task_id
        latest: dict[str, dict[str, Any]] = {}
        for r in rows:
            cur = latest.get(r["task_id"])
            if cur is None or r["attempt"] > cur["attempt"]:
                latest[r["task_id"]] = r
        completed = {
            tid for tid, r in latest.items() if r["state"] == TaskState.SUCCEEDED.value
        }

        # If any task is dead-lettered or permanently failed, fail the DAG.
        for tid, r in latest.items():
            if r["state"] in {
                TaskState.DEAD_LETTER.value,
                TaskState.FAILED.value,
            }:
                self.store.set_dag_state(dag_run_id, DagRunState.FAILED)
                return

        for tid in self.dag.ready_tasks(completed):
            cur = latest.get(tid)
            if cur is not None and cur["state"] not in {
                TaskState.PENDING.value,
                TaskState.RETRYING.value,
            }:
                # already queued/running/terminal — don't double-dispatch
                if cur["state"] not in TERMINAL_TASK_STATES and cur["state"] != TaskState.RETRYING.value:
                    continue
                if cur["state"] in {TaskState.SUCCEEDED.value, TaskState.SKIPPED.value}:
                    continue
            attempt = 1 if cur is None else cur["attempt"] + (
                1 if cur["state"] == TaskState.RETRYING.value else 0
            )
            if attempt == 0:
                attempt = 1
            await self._dispatch(dag_run_id, tid, attempt)

        # If every task is terminal-success, succeed the DAG.
        if all(r["state"] == TaskState.SUCCEEDED.value for r in latest.values()) and (
            set(latest) == set(self.dag.tasks)
        ):
            self.store.set_dag_state(dag_run_id, DagRunState.SUCCEEDED)

    async def _dispatch(
        self, dag_run_id: str, task_id: str, attempt: int
    ) -> None:
        task = self.dag.tasks[task_id]
        idem = task.compute_idempotency_key(dag_run_id, {"attempt": attempt})
        existing = self.store.task_run_by_idem_key(idem)
        if existing is not None:
            # A redelivery for this exact attempt. If it already finished
            # successfully, we are done. Otherwise let the in-flight run proceed.
            return
        tr_id = self.store.create_task_run(dag_run_id, task_id, attempt, idem)
        if tr_id is None:
            # Lost the race with another scheduler; that's fine.
            return
        self.store.set_task_state(tr_id, TaskState.QUEUED)
        msg = QueueMessage(
            task_run_id=tr_id,
            task_id=task_id,
            dag_run_id=dag_run_id,
            attempt=attempt,
            idempotency_key=idem,
        )
        await self.queue.put(msg)

    async def _retry_sink(self, msg: QueueMessage, delay: float) -> None:
        """Called by the worker when it wants to schedule a retry."""
        task = self.dag.tasks[msg.task_id]
        idem = task.compute_idempotency_key(
            msg.dag_run_id, {"attempt": msg.attempt}
        )
        existing = self.store.task_run_by_idem_key(idem)
        if existing is not None:
            return
        tr_id = self.store.create_task_run(
            msg.dag_run_id, msg.task_id, msg.attempt, idem
        )
        if tr_id is None:
            return
        self.store.set_task_state(tr_id, TaskState.QUEUED)
        retry_msg = QueueMessage(
            task_run_id=tr_id,
            task_id=msg.task_id,
            dag_run_id=msg.dag_run_id,
            attempt=msg.attempt,
            idempotency_key=idem,
        )
        await self.queue.put(retry_msg, delay_seconds=delay)

    def _terminal_state(self, dag_run_id: str) -> DagRunState | None:
        row = self.store.dag_run(dag_run_id)
        if row is None:
            return None
        s = row["state"]
        if s in {
            DagRunState.SUCCEEDED.value,
            DagRunState.FAILED.value,
            DagRunState.CANCELLED.value,
        }:
            return DagRunState(s)
        return None
