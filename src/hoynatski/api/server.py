from __future__ import annotations

from typing import Any

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from hoynatski.core.dag import DAG
from hoynatski.executor.executor import Executor
from hoynatski.queue.queue import InMemoryQueue
from hoynatski.scheduler.scheduler import Scheduler
from hoynatski.storage.sqlite_store import SQLiteStateStore


class SubmitRequest(BaseModel):
    params: dict[str, Any] | None = None


def build_app(dag: DAG, *, db_path: str = ":memory:", concurrency: int = 4) -> FastAPI:
    """Build a FastAPI app serving a single DAG.

    In a production deployment you would serve many DAGs from one API, backed
    by shared Postgres. This skeleton keeps it to one DAG and in-memory SQLite
    to make the surface obvious.
    """
    app = FastAPI(title="Hoynatski", version="0.1.0")
    store = SQLiteStateStore(db_path)
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=concurrency)

    @app.on_event("startup")
    async def _startup() -> None:
        await executor.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await executor.stop()
        store.close()

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/dag")
    async def get_dag() -> dict[str, Any]:
        return dag.to_dict()

    @app.post("/runs")
    async def submit(req: SubmitRequest) -> dict[str, Any]:
        run_id = await scheduler.submit(req.params or {})
        # Fire-and-forget drive. In production this is a background worker loop.
        import asyncio

        asyncio.create_task(scheduler.run_until_complete(run_id, timeout=600))
        return {"dag_run_id": run_id}

    @app.get("/runs/{run_id}")
    async def get_run(run_id: str) -> dict[str, Any]:
        row = store.dag_run(run_id)
        if row is None:
            raise HTTPException(404, "dag run not found")
        row["tasks"] = store.task_runs_for_dag_run(run_id)
        return row

    return app
