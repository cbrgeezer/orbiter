import pytest

from orbiter import DAG, TaskContext
from orbiter.core.state import DagRunState, TaskState
from orbiter.executor.executor import Executor
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_task_context_exposes_params_and_checkpoints():
    dag = DAG("ctx")
    seen: dict[str, object] = {}

    @dag.task()
    async def first(context: TaskContext) -> str:
        seen["attempt"] = context.attempt
        seen["user_id"] = context.param("user_id")
        context.checkpoint("cursor", {"step": 1})
        context.logger.info("checkpoint written")
        return "ok"

    @dag.task(depends_on=["first"])
    async def second(context: TaskContext) -> str:
        seen["checkpoint"] = context.get_checkpoint("cursor", task_id="first")
        seen["dag_run_id"] = context.dag_run_id
        return "done"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)

    await executor.start()
    try:
        run_id = await scheduler.submit({"user_id": "u-123"})
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        assert seen["attempt"] == 1
        assert seen["user_id"] == "u-123"
        assert seen["checkpoint"] == {"step": 1}
        assert seen["dag_run_id"] == run_id
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_task_params_can_bind_by_name():
    dag = DAG("ctx_named")
    seen: dict[str, object] = {}

    @dag.task()
    async def greet(name: str, retries: int = 0) -> str:
        seen["name"] = name
        seen["retries"] = retries
        return "hello"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    executor = Executor(dag, queue, store, concurrency=1)

    await executor.start()
    try:
        run_id = await scheduler.submit({"name": "Orbiter", "retries": 3})
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        assert seen == {"name": "Orbiter", "retries": 3}
        rows = store.task_runs_for_dag_run(run_id)
        assert rows[0]["state"] == TaskState.SUCCEEDED.value
    finally:
        await executor.stop()
        store.close()
