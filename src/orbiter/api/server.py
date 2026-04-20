from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from orbiter.api.auth import AuthConfig, is_authorised
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


def _activity_actor(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    remote = forwarded.split(",")[0].strip() if forwarded else None
    if not remote and request.client is not None:
        remote = request.client.host
    return f"operator@{remote}" if remote else "operator"


def _record_operator_activity(
    store: StateStore,
    request: Request,
    event_type: str,
    summary: str,
    *,
    dag_run_id: str | None = None,
    schedule_id: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    store.record_activity(
        event_type,
        summary,
        actor=_activity_actor(request),
        dag_run_id=dag_run_id,
        schedule_id=schedule_id,
        metadata={
            "path": request.url.path,
            **(metadata or {}),
        },
    )


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
    enable_workers: bool = True,
    enable_scheduler_service: bool = True,
    auto_drive_submissions: bool = True,
    api_key: str | None = None,
) -> FastAPI:
    """Build a FastAPI app serving a single DAG.

    In a production deployment you would serve many DAGs from one API, backed
    by shared Postgres. This skeleton keeps it to one DAG and in-memory SQLite
    to make the surface obvious.
    """
    app = FastAPI(title="Orbiter", version="0.1.0")
    ui_dir = Path(__file__).with_name("ui")
    auth = AuthConfig(api_key=api_key or os.getenv("ORBITER_API_KEY"))
    store = create_state_store(db_path)
    store.register_dag(dag.fingerprint(), dag.name, dag.to_dict())
    queue = create_queue(store, backend=queue_backend, db_target=db_path)
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=concurrency)
    schedule_service = ScheduleService(scheduler, store)

    @app.on_event("startup")
    async def _startup() -> None:
        if enable_workers:
            await executor.start()
        if enable_scheduler_service:
            await schedule_service.start()

    @app.on_event("shutdown")
    async def _shutdown() -> None:
        if enable_scheduler_service:
            await schedule_service.stop()
        if enable_workers:
            await executor.stop()
        store.close()

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse(url="/ui/")

    @app.middleware("http")
    async def require_api_key(request: Request, call_next):  # type: ignore[no-untyped-def]
        if not is_authorised(auth, request.headers, request.url.path):
            return JSONResponse(
                status_code=401,
                content={"detail": "unauthorised"},
                headers={"WWW-Authenticate": "Bearer"},
            )
        return await call_next(request)

    @app.get("/metrics", response_model=None)
    async def metrics() -> str:
        return _prometheus_metrics(store)

    @app.get("/auth/config")
    async def auth_config() -> dict[str, bool]:
        return {"enabled": auth.enabled}

    @app.get("/activity")
    async def activity(limit: int = 50) -> dict[str, Any]:
        return {"events": store.list_activity(limit=max(1, min(limit, 200)))}

    @app.get("/dag")
    async def get_dag() -> dict[str, Any]:
        return dag.to_dict()

    @app.get("/runs")
    async def list_runs(limit: int = 20) -> dict[str, Any]:
        return {"runs": store.list_dag_runs(limit=max(1, min(limit, 200)))}

    @app.post("/runs")
    async def submit(req: SubmitRequest, request: Request) -> dict[str, Any]:
        run_id = await scheduler.submit(req.params or {})
        _record_operator_activity(
            store,
            request,
            "run.submitted",
            "Submitted a DAG run",
            dag_run_id=run_id,
            metadata={"trigger": "manual", "params": req.params or {}},
        )
        if auto_drive_submissions:
            # Fire-and-forget drive. In production this belongs to a scheduler process.
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
    async def cancel_run(run_id: str, request: Request) -> dict[str, Any]:
        if not store.cancel_dag_run(run_id):
            row = store.dag_run(run_id)
            if row is None:
                raise HTTPException(404, "dag run not found")
        else:
            _record_operator_activity(
                store,
                request,
                "run.cancelled",
                f"Cancelled DAG run {run_id}",
                dag_run_id=run_id,
            )
        row = store.dag_run(run_id)
        assert row is not None
        return {"dag_run_id": run_id, "state": row["state"]}

    @app.get("/schedules")
    async def list_schedules(limit: int = 100) -> dict[str, Any]:
        return {"schedules": store.list_schedules(limit=max(1, min(limit, 200)))}

    @app.post("/schedules")
    async def create_schedule(req: ScheduleRequest, request: Request) -> dict[str, Any]:
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
        _record_operator_activity(
            store,
            request,
            "schedule.created.operator",
            f"Created schedule {req.name}",
            schedule_id=schedule_id,
            metadata={
                "interval_seconds": req.interval_seconds,
                "overlap_policy": req.overlap_policy,
            },
        )
        return {"schedule_id": schedule_id}

    @app.post("/schedules/{schedule_id}/pause")
    async def pause_schedule(schedule_id: str, request: Request) -> dict[str, Any]:
        if not store.pause_schedule(schedule_id):
            row = store.schedule(schedule_id)
            if row is None:
                raise HTTPException(404, "schedule not found")
        else:
            _record_operator_activity(
                store,
                request,
                "schedule.paused.operator",
                f"Paused schedule {schedule_id}",
                schedule_id=schedule_id,
            )
        row = store.schedule(schedule_id)
        assert row is not None
        return {"schedule_id": schedule_id, "state": row["state"]}

    @app.post("/schedules/{schedule_id}/resume")
    async def resume_schedule(schedule_id: str, request: Request) -> dict[str, Any]:
        if not store.resume_schedule(schedule_id):
            row = store.schedule(schedule_id)
            if row is None:
                raise HTTPException(404, "schedule not found")
        else:
            _record_operator_activity(
                store,
                request,
                "schedule.resumed.operator",
                f"Resumed schedule {schedule_id}",
                schedule_id=schedule_id,
            )
        row = store.schedule(schedule_id)
        assert row is not None
        return {"schedule_id": schedule_id, "state": row["state"]}

    @app.post("/schedules/{schedule_id}/run")
    async def run_schedule_now(schedule_id: str, request: Request) -> dict[str, Any]:
        run_id = store.trigger_schedule_now(schedule_id)
        if run_id is None:
            raise HTTPException(404, "schedule not found")
        _record_operator_activity(
            store,
            request,
            "schedule.run_now.operator",
            f"Triggered schedule {schedule_id} immediately",
            dag_run_id=run_id,
            schedule_id=schedule_id,
        )
        if auto_drive_submissions:
            import asyncio

            asyncio.create_task(scheduler.run_until_complete(run_id, timeout=600))
        return {"schedule_id": schedule_id, "dag_run_id": run_id}

    app.mount("/ui", StaticFiles(directory=ui_dir, html=True), name="ui")

    return app
