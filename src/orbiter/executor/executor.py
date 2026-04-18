from __future__ import annotations

import asyncio
import logging
from typing import Any

from orbiter.core.dag import DAG
from orbiter.executor.worker import Worker
from orbiter.queue.queue import Queue
from orbiter.storage.sqlite_store import StateStore


log = logging.getLogger("orbiter.executor")


class Executor:
    """A worker pool. Spawns N workers, owns their lifecycle."""

    def __init__(
        self,
        dag: DAG,
        queue: Queue,
        store: StateStore,
        *,
        concurrency: int = 4,
        fault_injector: Any = None,
    ) -> None:
        self.dag = dag
        self.queue = queue
        self.store = store
        self.concurrency = concurrency
        self.fault = fault_injector
        self._workers: list[Worker] = []
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        for _ in range(self.concurrency):
            w = Worker(
                self.dag, self.queue, self.store, fault_injector=self.fault
            )
            self._workers.append(w)
            self._tasks.append(asyncio.create_task(w.run()))

    async def stop(self) -> None:
        for w in self._workers:
            w.stop()
        for t in self._tasks:
            try:
                await asyncio.wait_for(t, timeout=5)
            except asyncio.TimeoutError:
                t.cancel()

    @property
    def total_processed(self) -> int:
        return sum(w.processed for w in self._workers)
