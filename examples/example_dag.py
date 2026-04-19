"""A small example DAG.

Shape:

    fetch -> transform -> load
                      \\-> notify
"""

from __future__ import annotations

import asyncio
import random

from orbiter import DAG, RetryPolicy, TaskContext


dag = DAG(name="etl_example", description="A tiny ETL pipeline")


@dag.task(retry=RetryPolicy(max_attempts=3, backoff="exponential", base_seconds=0.2))
async def fetch(context: TaskContext) -> dict:
    await asyncio.sleep(0.05)
    rows = context.param("rows", 42)
    context.checkpoint("source", {"rows": rows})
    context.logger.info("fetched input rows")
    return {"rows": rows}


@dag.task(depends_on=["fetch"], retry=RetryPolicy(max_attempts=2))
async def transform(context: TaskContext) -> int:
    # Simulate a flaky dependency to exercise retries in local runs.
    if random.random() < 0.15:
        raise RuntimeError("transient network blip")
    await asyncio.sleep(0.05)
    source = context.get_checkpoint("source", task_id="fetch", default={"rows": 42})
    rows = int(source["rows"])
    context.checkpoint("transformed_rows", rows * 2)
    return rows * 2


@dag.task(depends_on=["transform"])
async def load(context: TaskContext) -> str:
    await asyncio.sleep(0.05)
    transformed = context.get_checkpoint("transformed_rows", task_id="transform", default=0)
    return f"loaded {transformed} rows"


@dag.task(depends_on=["transform"])
async def notify(context: TaskContext) -> str:
    transformed = context.get_checkpoint("transformed_rows", task_id="transform", default=0)
    return f"notification sent for {transformed} rows"


dag.finalize()
