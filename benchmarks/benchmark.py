"""Simple throughput benchmark.

Spins up a DAG with N independent no-op tasks and measures wall-clock time
to drain the queue with M workers. Intended to validate relative gains from
concurrency and to catch regressions, not to produce publishable numbers.

Usage:
    python benchmarks/benchmark.py --tasks 2000 --workers 64
"""

from __future__ import annotations

import argparse
import asyncio
import time

from hoynatski import DAG
from hoynatski.executor.executor import Executor
from hoynatski.queue.queue import InMemoryQueue
from hoynatski.scheduler.scheduler import Scheduler
from hoynatski.storage.sqlite_store import SQLiteStateStore


def build_fanout_dag(n: int) -> DAG:
    dag = DAG(name=f"bench_fanout_{n}")

    async def _noop() -> int:
        return 1

    for i in range(n):
        async def fn(_i: int = i) -> int:  # capture per-task
            return _i

        fn.__name__ = f"t_{i}"
        dag.add_task_fn = None  # type: ignore[attr-defined]
        # Use decorator-style to get proper wrapping:
        dag.task(id=f"t_{i}")(fn)
    return dag.finalize()


async def main_async(n: int, workers: int, db: str) -> None:
    dag = build_fanout_dag(n)
    store = SQLiteStateStore(db)
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=workers)
    await executor.start()
    try:
        run_id = await scheduler.submit()
        t0 = time.perf_counter()
        final = await scheduler.run_until_complete(run_id, timeout=120)
        elapsed = time.perf_counter() - t0
        rate = n / elapsed if elapsed > 0 else float("inf")
        print(
            f"final={final.value} tasks={n} workers={workers} "
            f"elapsed={elapsed:.3f}s rate={rate:.1f}/s"
        )
    finally:
        await executor.stop()
        store.close()


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--tasks", type=int, default=500)
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--db", type=str, default=":memory:")
    args = p.parse_args()
    asyncio.run(main_async(args.tasks, args.workers, args.db))


if __name__ == "__main__":
    main()
