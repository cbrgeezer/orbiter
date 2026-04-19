from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from orbiter.core.dag import DAG
from orbiter.core.validation import validation_warnings
from orbiter.executor.executor import Executor
from orbiter.queue.factory import create_queue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.scheduler.service import ScheduleService
from orbiter.storage.factory import create_state_store


app = typer.Typer(help="Orbiter workflow engine")
console = Console()


def _load_dag(path: Path, symbol: str = "dag") -> DAG:
    """Load a DAG object from a Python file.

    The file must define a module-level `dag: DAG` (or a custom symbol name
    via --symbol). We do an explicit spec_from_file_location rather than
    polluting sys.path.
    """
    if not path.exists():
        raise typer.BadParameter(f"file not found: {path}")
    spec = importlib.util.spec_from_file_location("orbiter_user_dag", path)
    if spec is None or spec.loader is None:
        raise typer.BadParameter(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["orbiter_user_dag"] = module
    spec.loader.exec_module(module)
    dag = getattr(module, symbol, None)
    if not isinstance(dag, DAG):
        raise typer.BadParameter(
            f"{path} does not export a DAG as `{symbol}`"
        )
    return dag.finalize() if not getattr(dag, "_finalized", False) else dag


@app.command()
def validate(
    file: Path = typer.Argument(..., help="Path to a Python file that defines `dag`"),
    symbol: str = typer.Option("dag", help="Name of the DAG symbol"),
) -> None:
    """Parse and validate a DAG definition."""
    dag = _load_dag(file, symbol)
    console.print(f"[green]OK[/green] DAG '{dag.name}' — {len(dag.tasks)} tasks")
    warnings = validation_warnings(dag)
    table = Table("id", "depends_on", "retries", "tags")
    for t in dag.tasks.values():
        table.add_row(
            t.id,
            ",".join(t.depends_on) or "-",
            str(t.retry.max_attempts),
            ",".join(t.tags) or "-",
        )
    console.print(table)
    if warnings:
        console.print("[yellow]Warnings[/yellow]")
        for warning in warnings:
            console.print(f"  - {warning}")


@app.command()
def describe(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
) -> None:
    """Print the DAG as JSON."""
    dag = _load_dag(file, symbol)
    console.print_json(data=dag.to_dict())


@app.command()
def run(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
    concurrency: int = typer.Option(4, help="Worker pool size"),
    db: str = typer.Option(":memory:", help="SQLite DB path (:memory: for ephemeral)"),
    queue_backend: str = typer.Option("auto", help="Queue backend: auto, memory, or store"),
    params: str = typer.Option("{}", help="JSON object passed to task params/context"),
) -> None:
    """Run a DAG to completion, blocking until done."""
    dag = _load_dag(file, symbol)
    try:
        parsed = json.loads(params)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"invalid JSON for --params: {exc}") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter("--params must decode to a JSON object")
    asyncio.run(
        _run(
            dag,
            concurrency=concurrency,
            db=db,
            params=parsed,
            queue_backend=queue_backend,
        )
    )


async def _run(
    dag: DAG,
    *,
    concurrency: int,
    db: str,
    params: dict[str, object],
    queue_backend: str,
) -> None:
    store = create_state_store(db)
    queue = create_queue(store, backend=queue_backend, db_target=db)
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=concurrency)
    await executor.start()
    try:
        run_id = await scheduler.submit(params)
        final = await scheduler.run_until_complete(run_id, timeout=300)
        console.print(f"[bold]run {run_id}[/bold] -> {final.value}")
        rows = store.task_runs_for_dag_run(run_id)
        table = Table("task", "attempt", "state", "error")
        for r in rows:
            table.add_row(
                r["task_id"], str(r["attempt"]), r["state"],
                (r["error"] or "").splitlines()[0] if r["error"] else "",
            )
        console.print(table)
    finally:
        await executor.stop()
        store.close()


async def _wait_until_cancelled(label: str) -> None:
    console.print(f"[green]{label} started[/green]. Press Ctrl+C to stop.")
    stop = asyncio.Event()
    try:
        await stop.wait()
    except (asyncio.CancelledError, KeyboardInterrupt):
        return


@app.command()
def serve(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
    concurrency: int = typer.Option(4),
    db: str = typer.Option(":memory:"),
    queue_backend: str = typer.Option("auto", help="Queue backend: auto, memory, or store"),
) -> None:
    """Serve the REST API for a DAG."""
    import uvicorn

    from orbiter.api.server import build_app

    dag = _load_dag(file, symbol)
    uvicorn.run(
        build_app(
            dag,
            db_path=db,
            concurrency=concurrency,
            queue_backend=queue_backend,
            enable_workers=True,
            enable_scheduler_service=True,
            auto_drive_submissions=True,
        ),
        host=host,
        port=port,
    )


@app.command()
def serve_api(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
    db: str = typer.Option(":memory:"),
    queue_backend: str = typer.Option("auto", help="Queue backend: auto, memory, or store"),
) -> None:
    """Serve the API without local scheduler or workers."""
    import uvicorn

    from orbiter.api.server import build_app

    dag = _load_dag(file, symbol)
    uvicorn.run(
        build_app(
            dag,
            db_path=db,
            concurrency=0,
            queue_backend=queue_backend,
            enable_workers=False,
            enable_scheduler_service=False,
            auto_drive_submissions=False,
        ),
        host=host,
        port=port,
    )


@app.command()
def run_scheduler(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
    db: str = typer.Option(":memory:"),
    queue_backend: str = typer.Option("auto", help="Queue backend: auto, memory, or store"),
    poll_interval: float = typer.Option(1.0, help="Scheduler service poll interval"),
) -> None:
    """Run the scheduler service only."""
    dag = _load_dag(file, symbol)
    asyncio.run(
        _run_scheduler_service(
            dag,
            db=db,
            queue_backend=queue_backend,
            poll_interval=poll_interval,
        )
    )


async def _run_scheduler_service(
    dag: DAG,
    *,
    db: str,
    queue_backend: str,
    poll_interval: float,
) -> None:
    store = create_state_store(db)
    queue = create_queue(store, backend=queue_backend, db_target=db)
    scheduler = Scheduler(dag, queue, store)
    service = ScheduleService(scheduler, store, poll_interval=poll_interval)
    try:
        await service.start()
        await _wait_until_cancelled("Scheduler service")
    finally:
        await service.stop()
        store.close()


@app.command()
def run_worker(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
    db: str = typer.Option(":memory:"),
    queue_backend: str = typer.Option("auto", help="Queue backend: auto, memory, or store"),
    concurrency: int = typer.Option(4, help="Worker pool size"),
) -> None:
    """Run the worker pool only."""
    dag = _load_dag(file, symbol)
    asyncio.run(
        _run_worker_service(
            dag,
            db=db,
            queue_backend=queue_backend,
            concurrency=concurrency,
        )
    )


async def _run_worker_service(
    dag: DAG,
    *,
    db: str,
    queue_backend: str,
    concurrency: int,
) -> None:
    store = create_state_store(db)
    queue = create_queue(store, backend=queue_backend, db_target=db)
    executor = Executor(dag, queue, store, concurrency=concurrency)
    try:
        await executor.start()
        await _wait_until_cancelled("Worker service")
    finally:
        await executor.stop()
        store.close()


@app.command()
def inspect_run(
    db: str = typer.Argument(..., help="SQLite DB path"),
    run_id: str = typer.Argument(...),
) -> None:
    """Show state of a single DAG run."""
    store = create_state_store(db)
    row = store.dag_run(run_id)
    if row is None:
        console.print("[red]run not found[/red]")
        raise typer.Exit(1)
    console.print_json(data={"run": row, "tasks": store.task_runs_for_dag_run(run_id)})


@app.command()
def cancel_run(
    db: str = typer.Argument(..., help="SQLite DB path"),
    run_id: str = typer.Argument(...),
) -> None:
    """Cancel a pending or running DAG run."""
    store = create_state_store(db)
    try:
        if not store.cancel_dag_run(run_id):
            row = store.dag_run(run_id)
            if row is None:
                console.print("[red]run not found[/red]")
                raise typer.Exit(1)
        row = store.dag_run(run_id)
        console.print(f"[yellow]run {run_id}[/yellow] -> {row['state']}")
    finally:
        store.close()


@app.command()
def create_schedule(
    file: Path = typer.Argument(...),
    name: str = typer.Option(..., help="Human readable schedule name"),
    every_seconds: int = typer.Option(..., help="Interval between runs in seconds"),
    overlap_policy: str = typer.Option("allow", help="Overlap policy: allow, forbid, or replace"),
    symbol: str = typer.Option("dag"),
    db: str = typer.Option(":memory:", help="SQLite DB path"),
    params: str = typer.Option("{}", help="JSON object passed to scheduled runs"),
    start_in_seconds: int = typer.Option(0, help="Delay before the first run"),
) -> None:
    """Create a recurring schedule for a DAG."""
    dag = _load_dag(file, symbol)
    store = create_state_store(db)
    try:
        store.register_dag(dag.fingerprint(), dag.name, dag.to_dict())
    try:
        parsed = json.loads(params)
    except json.JSONDecodeError as exc:
        raise typer.BadParameter(f"invalid JSON for --params: {exc}") from exc
    if not isinstance(parsed, dict):
        raise typer.BadParameter("--params must decode to a JSON object")
    if overlap_policy not in {"allow", "forbid", "replace"}:
        raise typer.BadParameter("--overlap-policy must be allow, forbid, or replace")
    schedule_id = store.create_schedule(
        dag.fingerprint(),
        name,
        every_seconds,
        overlap_policy=overlap_policy,
        params=parsed,
        start_at=None if start_in_seconds <= 0 else time.time() + start_in_seconds,
    )
        console.print(f"[green]schedule created[/green] {schedule_id}")
    finally:
        store.close()


@app.command()
def list_schedules(
    db: str = typer.Argument(..., help="SQLite DB path"),
) -> None:
    """List recurring schedules."""
    store = create_state_store(db)
    try:
        rows = store.list_schedules()
        table = Table("id", "name", "state", "overlap", "interval_s", "next_run_at", "last_run_id")
        for row in rows:
            table.add_row(
                row["id"],
                row["name"],
                row["state"],
                row["overlap_policy"],
                str(row["interval_seconds"]),
                f"{row['next_run_at']:.3f}",
                row["last_run_id"] or "",
            )
        console.print(table)
    finally:
        store.close()


@app.command()
def pause_schedule(
    db: str = typer.Argument(..., help="SQLite DB path"),
    schedule_id: str = typer.Argument(...),
) -> None:
    """Pause a recurring schedule."""
    store = create_state_store(db)
    try:
        if not store.pause_schedule(schedule_id):
            row = store.schedule(schedule_id)
            if row is None:
                console.print("[red]schedule not found[/red]")
                raise typer.Exit(1)
        row = store.schedule(schedule_id)
        console.print(f"[yellow]schedule {schedule_id}[/yellow] -> {row['state']}")
    finally:
        store.close()


@app.command()
def resume_schedule(
    db: str = typer.Argument(..., help="SQLite DB path"),
    schedule_id: str = typer.Argument(...),
) -> None:
    """Resume a paused schedule."""
    store = create_state_store(db)
    try:
        if not store.resume_schedule(schedule_id):
            row = store.schedule(schedule_id)
            if row is None:
                console.print("[red]schedule not found[/red]")
                raise typer.Exit(1)
        row = store.schedule(schedule_id)
        console.print(f"[green]schedule {schedule_id}[/green] -> {row['state']}")
    finally:
        store.close()


@app.command()
def run_schedule(
    db: str = typer.Argument(..., help="SQLite DB path"),
    schedule_id: str = typer.Argument(...),
    file: Path = typer.Argument(..., help="Path to the DAG file for immediate execution"),
    symbol: str = typer.Option("dag"),
    concurrency: int = typer.Option(4, help="Worker pool size"),
) -> None:
    """Trigger a schedule immediately and run it to completion."""
    dag = _load_dag(file, symbol)
    asyncio.run(_run_schedule_now(dag, db=db, schedule_id=schedule_id, concurrency=concurrency))


async def _run_schedule_now(dag: DAG, *, db: str, schedule_id: str, concurrency: int) -> None:
    store = create_state_store(db)
    queue = create_queue(store, backend="auto", db_target=db)
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=concurrency)
    await executor.start()
    try:
        run_id = store.trigger_schedule_now(schedule_id)
        if run_id is None:
            console.print("[red]schedule not found[/red]")
            raise typer.Exit(1)
        final = await scheduler.run_until_complete(run_id, timeout=300)
        console.print(f"[bold]scheduled run {run_id}[/bold] -> {final.value}")
    finally:
        await executor.stop()
        store.close()


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
