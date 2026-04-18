from __future__ import annotations

import asyncio
import importlib.util
import json
import sys
from pathlib import Path

import typer
from rich.console import Console
from rich.table import Table

from orbiter.core.dag import DAG
from orbiter.executor.executor import Executor
from orbiter.queue.queue import InMemoryQueue
from orbiter.scheduler.scheduler import Scheduler
from orbiter.storage.sqlite_store import SQLiteStateStore


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
    table = Table("id", "depends_on", "retries", "tags")
    for t in dag.tasks.values():
        table.add_row(
            t.id,
            ",".join(t.depends_on) or "-",
            str(t.retry.max_attempts),
            ",".join(t.tags) or "-",
        )
    console.print(table)


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
) -> None:
    """Run a DAG to completion, blocking until done."""
    dag = _load_dag(file, symbol)
    asyncio.run(_run(dag, concurrency=concurrency, db=db))


async def _run(dag: DAG, *, concurrency: int, db: str) -> None:
    store = SQLiteStateStore(db)
    queue = InMemoryQueue()
    scheduler = Scheduler(dag, queue, store)
    executor = Executor(dag, queue, store, concurrency=concurrency)
    await executor.start()
    try:
        run_id = await scheduler.submit()
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


@app.command()
def serve(
    file: Path = typer.Argument(...),
    symbol: str = typer.Option("dag"),
    host: str = typer.Option("127.0.0.1"),
    port: int = typer.Option(8000),
    concurrency: int = typer.Option(4),
    db: str = typer.Option(":memory:"),
) -> None:
    """Serve the REST API for a DAG."""
    import uvicorn

    from orbiter.api.server import build_app

    dag = _load_dag(file, symbol)
    uvicorn.run(build_app(dag, db_path=db, concurrency=concurrency), host=host, port=port)


@app.command()
def inspect_run(
    db: str = typer.Argument(..., help="SQLite DB path"),
    run_id: str = typer.Argument(...),
) -> None:
    """Show state of a single DAG run."""
    store = SQLiteStateStore(db)
    row = store.dag_run(run_id)
    if row is None:
        console.print("[red]run not found[/red]")
        raise typer.Exit(1)
    console.print_json(data={"run": row, "tasks": store.task_runs_for_dag_run(run_id)})


def main() -> None:  # pragma: no cover
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
