import asyncio

import pytest

from orbiter.queue.factory import create_queue
from orbiter.queue.queue import QueueMessage, StoreBackedQueue
from orbiter.storage.sqlite_store import SQLiteStateStore


@pytest.mark.asyncio
async def test_store_backed_queue_put_get_ack_round_trip():
    store = SQLiteStateStore(":memory:")
    try:
        store.register_dag("fp", "demo", {})
        run_id = store.create_dag_run("fp", {})
        task_run_id = store.create_task_run(run_id, "t1", 1, "idem-1")
        assert task_run_id is not None

        queue = StoreBackedQueue(store, worker_id="w1", lease_seconds=5)
        msg = QueueMessage(
            task_run_id=task_run_id,
            task_id="t1",
            dag_run_id=run_id,
            attempt=1,
            idempotency_key="idem-1",
        )
        await queue.put(msg)
        leased = await queue.get(timeout=0.2)
        assert leased is not None
        assert leased.task_run_id == task_run_id
        assert leased.queue_id > 0
        await queue.ack(leased)
        assert await queue.get(timeout=0.05) is None
    finally:
        store.close()


@pytest.mark.asyncio
async def test_store_backed_queue_nack_requeues_with_delay():
    store = SQLiteStateStore(":memory:")
    try:
        store.register_dag("fp", "demo", {})
        run_id = store.create_dag_run("fp", {})
        task_run_id = store.create_task_run(run_id, "t1", 1, "idem-1")
        assert task_run_id is not None

        queue = StoreBackedQueue(store, worker_id="w1", lease_seconds=5)
        msg = QueueMessage(
            task_run_id=task_run_id,
            task_id="t1",
            dag_run_id=run_id,
            attempt=1,
            idempotency_key="idem-1",
        )
        await queue.put(msg)
        leased = await queue.get(timeout=0.2)
        assert leased is not None
        await queue.nack(leased, delay_seconds=0.05)
        assert await queue.get(timeout=0.01) is None
        await asyncio.sleep(0.06)
        again = await queue.get(timeout=0.2)
        assert again is not None
        assert again.task_run_id == task_run_id
    finally:
        store.close()


def test_queue_factory_auto_selects_store_for_persistent_targets():
    store = SQLiteStateStore(":memory:")
    try:
        q1 = create_queue(store, backend="auto", db_target=":memory:")
        q2 = create_queue(store, backend="auto", db_target="orbiter.db")
        assert q1.__class__.__name__ == "InMemoryQueue"
        assert q2.__class__.__name__ == "StoreBackedQueue"
    finally:
        store.close()
