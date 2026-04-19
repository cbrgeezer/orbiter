import pytest

from orbiter import DAG
from orbiter.core.state import DagRunState, TaskState
from orbiter.core.validation import validation_warnings
from orbiter.executor.executor import Executor
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_cancelling_run_skips_queued_tasks():
    dag = DAG("cancel")

    @dag.task()
    async def root():
        return "root"

    @dag.task(depends_on=["root"])
    async def downstream():
        return "downstream"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)

    await executor.start()
    try:
        run_id = await scheduler.submit()
        assert store.cancel_dag_run(run_id) is True
        final = await scheduler.run_until_complete(run_id, timeout=1)
        assert final == DagRunState.CANCELLED
        rows = store.task_runs_for_dag_run(run_id)
        assert rows == []
    finally:
        await executor.stop()
        store.close()


def test_validation_warns_about_missing_timeouts():
    dag = DAG("validate")

    @dag.task()
    async def no_timeout():
        return "ok"

    @dag.task(timeout_seconds=5)
    async def with_timeout():
        return "ok"

    dag.finalize()
    warnings = validation_warnings(dag)
    assert len(warnings) == 1
    assert "no timeout" in warnings[0]


def test_state_counts_include_dag_and_task_states():
    store = SQLiteStateStore(":memory:")
    try:
        store.register_dag("fp", "x", {})
        dag_run_id = store.create_dag_run("fp", {})
        store.set_dag_state(dag_run_id, DagRunState.RUNNING)
        tr_id = store.create_task_run(dag_run_id, "t1", 1, "idem-1")
        assert tr_id is not None
        store.set_task_state(tr_id, TaskState.RUNNING)
        counts = store.state_counts()
        assert counts["dag_runs_running"] == 1
        assert counts["task_runs_running"] == 1
    finally:
        store.close()
