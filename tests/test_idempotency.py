import pytest

from orbiter import DAG
from orbiter.core.state import DagRunState
from orbiter.executor.executor import Executor
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_duplicate_idempotency_key_is_skipped():
    """If the scheduler sees a task_run row already exists for a given
    idempotency_key, it must not create a second one."""
    dag = DAG("idem")
    counter = {"n": 0}

    @dag.task()
    async def t1():
        counter["n"] += 1

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
        assert counter["n"] == 1

        # Now simulate a redelivery attempt: calling _dispatch a second
        # time with same (dag_run, task, attempt) must not insert a new row
        # because the idempotency_key is UNIQUE in the store.
        await scheduler._dispatch(run_id, "t1", attempt=1)
        rows = [r for r in store.task_runs_for_dag_run(run_id) if r["task_id"] == "t1"]
        assert len(rows) == 1
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_idem_key_differs_between_runs():
    dag = DAG("idem2")

    @dag.task()
    async def t():
        return 1

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)

    await executor.start()
    try:
        r1 = await scheduler.submit()
        await scheduler.run_until_complete(r1, timeout=5)
        r2 = await scheduler.submit()
        await scheduler.run_until_complete(r2, timeout=5)
        assert r1 != r2
    finally:
        await executor.stop()
        store.close()
