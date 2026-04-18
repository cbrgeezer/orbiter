import asyncio

import pytest

from orbiter import DAG, RetryPolicy
from orbiter.core.state import DagRunState, TaskState
from orbiter.executor.executor import Executor
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_end_to_end_happy_path():
    dag = DAG("pipe")

    @dag.task()
    async def a():
        return "a"

    @dag.task(depends_on=["a"])
    async def b():
        return "b"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=2)

    await executor.start()
    try:
        run_id = await scheduler.submit()
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        rows = store.task_runs_for_dag_run(run_id)
        assert all(r["state"] == TaskState.SUCCEEDED.value for r in rows)
        assert {r["task_id"] for r in rows} == {"a", "b"}
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_retry_then_succeed():
    attempts = {"n": 0}
    dag = DAG("retry_dag")

    @dag.task(retry=RetryPolicy(max_attempts=3, backoff="fixed", base_seconds=0.01, jitter=0))
    async def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("flaky")
        return "ok"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)

    await executor.start()
    try:
        run_id = await scheduler.submit()
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        assert attempts["n"] == 3
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_dead_letter_after_exhausting_retries():
    dag = DAG("dl")

    @dag.task(retry=RetryPolicy(max_attempts=2, backoff="fixed", base_seconds=0.01, jitter=0))
    async def always_fail():
        raise RuntimeError("no")

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)

    await executor.start()
    try:
        run_id = await scheduler.submit()
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.FAILED
        rows = store.task_runs_for_dag_run(run_id)
        # last attempt should be dead-lettered
        last = sorted(rows, key=lambda r: r["attempt"])[-1]
        assert last["state"] == TaskState.DEAD_LETTER.value
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_parallel_independent_branches():
    order = []
    dag = DAG("par")

    @dag.task()
    async def root():
        order.append("root")
        return 1

    @dag.task(depends_on=["root"])
    async def left():
        await asyncio.sleep(0.02)
        order.append("left")

    @dag.task(depends_on=["root"])
    async def right():
        await asyncio.sleep(0.02)
        order.append("right")

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=4)
    await executor.start()
    try:
        run_id = await scheduler.submit()
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        assert order[0] == "root"
        assert set(order[1:]) == {"left", "right"}
    finally:
        await executor.stop()
        store.close()
