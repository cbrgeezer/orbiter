from __future__ import annotations

import asyncio
import logging

from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import StateStore


log = logging.getLogger("orbiter.schedule_service")


class ScheduleService:
    """Background loop that drives schedules and active DAG runs."""

    def __init__(
        self,
        scheduler: Scheduler,
        store: StateStore,
        *,
        poll_interval: float = 1.0,
        run_timeout: float = 600.0,
        batch_size: int = 10,
    ) -> None:
        self.scheduler = scheduler
        self.store = store
        self.poll_interval = poll_interval
        self.run_timeout = run_timeout
        self.batch_size = batch_size
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self.run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task

    async def run(self) -> None:
        while not self._stop.is_set():
            due_runs = self.store.dispatch_due_schedules(limit=self.batch_size)
            for item in due_runs:
                run_id = item["dag_run_id"]
                log.info("registered scheduled dag run %s for schedule %s", run_id, item["schedule_id"])
            for row in self.store.list_active_dag_runs(limit=self.batch_size * 20):
                await self.scheduler._dispatch_ready(row["id"])
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_interval)
            except asyncio.TimeoutError:
                continue
