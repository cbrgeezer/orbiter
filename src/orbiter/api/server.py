from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from orbiter.core.dag import DAG
from orbiter.executor.executor import Executor
from orbiter.queue.factory import QueueBackend, create_queue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.scheduler.service import ScheduleService
from orbiter.storage.factory import create_state_store
from orbiter.storage.sqlite_store import StateStore


class SubmitRequest(BaseModel):
    params: dict[str, Any] | None = None


class ScheduleRequest(BaseModel):
    name: str
    interval_seconds: int
    overlap_policy: str = "allow"
    params: dict[str, Any] | None = None
    start_at: float | None = None


def _prometheus_metrics(store: StateStore) -> str:
    lines = [
        "# HELP orbiter_dag_runs_total Total DAG runs by state",
        "# TYPE orbiter_dag_runs_total gauge",
    ]
    counts = store.state_counts()
    for state in ["pending", "running", "succeeded", "failed", "cancelled"]:
        lines.append(
            f'orbiter_dag_runs_total{{state="{state}"}} {counts.get(f"dag_runs_{state}", 0)}'
        )
    lines.extend(
        [
            "# HELP orbiter_task_runs_total Total task runs by state",
            "# TYPE orbiter_task_runs_total gauge",
        ]
    )
    for state in [
        "pending",
        "queued",
        "running",
        "succeeded",
        "failed",
        "retrying",
        "dead_letter",
        "skipped",
    ]:
        lines.append(
            f'orbiter_task_runs_total{{state="{state}"}} {counts.get(f"task_runs_{state}", 0)}'
        )
    return "\n".join(lines) + "\n"


def build_app(
    dag: DAG,
    *,
    db_path: str = ":memory:",
    concurrency: int = 4,
    queue_backend: QueueBackend = "auto",
) -> FastAPI:
    """Build a FastAPI app serving a single DAG.

    In a production deployment you would serve many DAGs from one API, backed
    by shared Postgres. This skeleton keeps it to one DAG and in-memory SQLite
    to make the surface obvious.
    """
    app = FastAPI(title="Orbiter", version="0.1.0")
    ui_dir = Path(__file__).with_name("ui")
    store = create_state_store(db_path)
    queue = create_queue(store, backend=queue_backend, db_target=db_path)
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=concurrency)
    schedule_service = ScheduleService(scheduler, store)

    @app.on_event("startup")
    async def _startup() -> None:
        await executor.start()
        await schedule_service.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        await schedule_service.stop()
        await executor.stop()
        store.close()

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/ui/")

    @app.get("/metrics", response_model=None)
    async def metrics() -> str:
        return _prometheus_metrics(store)

    @app.get("/dag")
    async def get_dag() -> dict[str, Any]:
        return dag.to_dict()

    @app.get("/runs")
    async def list_runs(limit: int = 20) -> dict[str, Any]:
        return {"runs": store.list_dag_runs(limit=max(1, min(limit, 200)))}

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

    @app.post("/runs/{run_id}/cancel")
    async def cancel_run(run_id: str) -> dict[str, Any]:
        if not store.cancel_dag_run(run_id):
            row = store.dag_run(run_id)
            if row is None:
                raise HTTPException(404, "dag run not found")
        row = store.dag_run(run_id)
        assert row is not None
        return {"dag_run_id": run_id, "state": row["state"]}

    @app.get("/schedules")
    async def list_schedules(limit: int = 100) -> dict[str, Any]:
        return {"schedules": store.list_schedules(limit=max(1, min(limit, 200)))}

    @app.post("/schedules")
    async def create_schedule(req: ScheduleRequest) -> dict[str, Any]:
        if req.interval_seconds <= 0:
            raise HTTPException(400, "interval_seconds must be positive")
        if req.overlap_policy not in {"allow", "forbid", "replace"}:
            raise HTTPException(400, "overlap_policy must be allow, forbid, or replace")
        schedule_id = store.create_schedule(
            dag.fingerprint(),
            req.name,
            req.interval_seconds,
            overlap_policy=req.overlap_policy,
            params=req.params or {},
            start_at=req.start_at,
        )
        return {"schedule_id": schedule_id}

    @app.post("/schedules/{schedule_id}/pause")
    async def pause_schedule(schedule_id: str) -> dict[str, Any]:
        if not store.pause_schedule(schedule_id):
            row = store.schedule(schedule_id)
            if row is None:
                raise HTTPException(404, "schedule not found")
        row = store.schedule(schedule_id)
        assert row is not None
        return {"schedule_id": schedule_id, "state": row["state"]}

    @app.post("/schedules/{schedule_id}/resume")
    async def resume_schedule(schedule_id: str) -> dict[str, Any]:
        if not store.resume_schedule(schedule_id):
            row = store.schedule(schedule_id)
            if row is None:
                raise HTTPException(404, "schedule not found")
        row = store.schedule(schedule_id)
        assert row is not None
        return {"schedule_id": schedule_id, "state": row["state"]}

    @app.post("/schedules/{schedule_id}/run")
    async def run_schedule_now(schedule_id: str) -> dict[str, Any]:
        run_id = store.trigger_schedule_now(schedule_id)
        if run_id is None:
            raise HTTPException(404, "schedule not found")
        import asyncio

        asyncio.create_task(scheduler.run_until_complete(run_id, timeout=600))
        return {"schedule_id": schedule_id, "dag_run_id": run_id}

    app.mount("/ui", StaticFiles(directory=ui_dir, html=True), name="ui")

    return app
