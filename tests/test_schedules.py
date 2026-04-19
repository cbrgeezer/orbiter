import asyncio

import pytest

from orbiter import DAG
from orbiter.core.state import DagRunState
from orbiter.executor.executor import Executor
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.scheduler.service import ScheduleService
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_due_schedule_dispatches_run_and_updates_schedule():
    dag = DAG("scheduled")
    seen = {"calls": 0}

    @dag.task()
    async def task():
        seen["calls"] += 1
        return "ok"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    store.register_dag(dag.fingerprint(), dag.name, dag.to_dict())
    schedule_id = store.create_schedule(
        dag.fingerprint(),
        "every-second",
        60,
        params={"source": "schedule"},
        start_at=0,
    )
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)
    service = ScheduleService(scheduler, store, poll_interval=0.02, run_timeout=5)

    await executor.start()
    await service.start()
    try:
        await asyncio.sleep(0.15)
        runs = store.list_dag_runs()
        assert len(runs) == 1
        final = scheduler._terminal_state(runs[0]["id"])
        assert final == DagRunState.SUCCEEDED
        assert seen["calls"] == 1
        row = store.schedule(schedule_id)
        assert row is not None
        assert row["last_run_id"] == runs[0]["id"]
        assert row["state"] == "active"
    finally:
        await service.stop()
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_scheduler_service_drives_manual_pending_runs():
    dag = DAG("manual-service")
    seen = {"calls": 0}

    @dag.task()
    async def task():
        seen["calls"] += 1
        return "ok"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    store.register_dag(dag.fingerprint(), dag.name, dag.to_dict())
    run_id = store.create_dag_run(dag.fingerprint(), {"source": "manual"})
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)
    service = ScheduleService(scheduler, store, poll_interval=0.02, run_timeout=5)

    await executor.start()
    await service.start()
    try:
        await asyncio.sleep(0.15)
        final = scheduler._terminal_state(run_id)
        assert final == DagRunState.SUCCEEDED
        assert seen["calls"] == 1
    finally:
        await service.stop()
        await executor.stop()
        store.close()


def test_schedule_pause_resume_and_manual_trigger():
    store = SQLiteStateStore(":memory:")
    try:
        store.register_dag("fp", "demo", {})
        schedule_id = store.create_schedule("fp", "manual", 30, params={"x": 1}, start_at=9999999999)
        assert store.pause_schedule(schedule_id) is True
        row = store.schedule(schedule_id)
        assert row is not None and row["state"] == "paused"
        assert store.resume_schedule(schedule_id) is True
        row = store.schedule(schedule_id)
        assert row is not None and row["state"] == "active"
        run_id = store.trigger_schedule_now(schedule_id)
        assert run_id is not None
        run = store.dag_run(run_id)
        assert run is not None
        assert run["trigger"] == f"schedule:{schedule_id}:manual"
    finally:
        store.close()


def test_schedule_forbid_skips_when_active_run_exists():
    store = SQLiteStateStore(":memory:")
    try:
        store.register_dag("fp", "demo", {})
        schedule_id = store.create_schedule(
            "fp",
            "forbid",
            30,
            overlap_policy="forbid",
            start_at=0,
        )
        active_run_id = store.create_dag_run("fp", {}, trigger=f"schedule:{schedule_id}")
        store.set_dag_state(active_run_id, DagRunState.RUNNING)
        dispatched = store.dispatch_due_schedules(now=1)
        assert dispatched == []
        runs = store.list_dag_runs()
        assert len(runs) == 1
        row = store.schedule(schedule_id)
        assert row is not None
        assert row["next_run_at"] == 31
    finally:
        store.close()


def test_schedule_replace_cancels_active_run_and_creates_new_one():
    store = SQLiteStateStore(":memory:")
    try:
        store.register_dag("fp", "demo", {})
        schedule_id = store.create_schedule(
            "fp",
            "replace",
            30,
            overlap_policy="replace",
            start_at=0,
        )
        active_run_id = store.create_dag_run("fp", {}, trigger=f"schedule:{schedule_id}")
        store.set_dag_state(active_run_id, DagRunState.RUNNING)
        dispatched = store.dispatch_due_schedules(now=1)
        assert len(dispatched) == 1
        old_run = store.dag_run(active_run_id)
        assert old_run is not None
        assert old_run["state"] == DagRunState.CANCELLED.value
        new_run = store.dag_run(dispatched[0]["dag_run_id"])
        assert new_run is not None
        assert new_run["trigger"] == f"schedule:{schedule_id}"
    finally:
        store.close()
