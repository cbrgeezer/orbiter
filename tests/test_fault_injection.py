"""Fault-injection tests.

These simulate worker crashes at two critical moments:
1. before the task body runs,
2. after the task body runs but before the ack + state write.

Case (1) is an at-least-once-safe drop; the retry mechanism picks it up.
Case (2) is the classic at-least-once hazard: the task *did run*, but the
system does not yet know it. On restart the scheduler re-dispatches.
"""

import pytest

from orbiter import DAG, RetryPolicy
from orbiter.core.state import DagRunState, TaskState
from orbiter.executor.executor import Executor
from orbiter.executor.worker import FaultInjector
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_crash_before_task_recovers_via_retry():
    dag = DAG("fi1")
    calls = {"n": 0}

    @dag.task(retry=RetryPolicy(max_attempts=3, backoff="fixed", base_seconds=0.01, jitter=0))
    async def t():
        calls["n"] += 1
        return "ok"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    fault = FaultInjector()
    fault.crash_before_task = True
    executor = Executor(dag, queue, store, concurrency=1, fault_injector=fault)

    await executor.start()
    try:
        run_id = await scheduler.submit()
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        # The injector crashed once; the retry path should have run the body.
        assert calls["n"] >= 1
        assert fault.crashes and fault.crashes[0].startswith("pre:")
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_crash_after_task_before_ack_is_at_least_once():
    """User code saw the side effect once, but the state transition was lost.
    Without idempotency the work re-runs — that's the at-least-once semantics
    we document. The test asserts the task ran at least once and the DAG
    ultimately succeeds via retry.
    """
    dag = DAG("fi2")
    calls = {"n": 0}

    @dag.task(retry=RetryPolicy(max_attempts=3, backoff="fixed", base_seconds=0.01, jitter=0))
    async def t():
        calls["n"] += 1
        return "ok"

    dag.finalize()

    store = SQLiteStateStore(":memory:")
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store, poll_interval=0.02)
    fault = FaultInjector()
    fault.crash_before_ack = True
    executor = Executor(dag, queue, store, concurrency=1, fault_injector=fault)

    await executor.start()
    try:
        run_id = await scheduler.submit()
        final = await scheduler.run_until_complete(run_id, timeout=5)
        assert final == DagRunState.SUCCEEDED
        assert calls["n"] >= 1
        assert fault.crashes and fault.crashes[0].startswith("post:")
    finally:
        await executor.stop()
        store.close()


@pytest.mark.asyncio
async def test_expired_leases_reclaimed():
    """Queue items whose lease expired should be eligible for another worker."""
    store = SQLiteStateStore(":memory:")
    # Register a minimal dag so FK on task_runs is satisfied.
    store.register_dag("fp", "x", {})
    dag_run_id = store.create_dag_run("fp", {})
    tr_id = store.create_task_run(dag_run_id, "t1", 1, "idem-1")
    assert tr_id is not None
    store.enqueue(tr_id)

    # Lease it.
    msg = store.lease("w1", lease_seconds=-1)  # already expired
    assert msg is not None

    reclaimed = store.reclaim_expired_leases()
    assert reclaimed == 1

    # Now another worker should be able to pick it up.
    msg2 = store.lease("w2", lease_seconds=10)
    assert msg2 is not None
    assert msg2["task_run_id"] == tr_id
