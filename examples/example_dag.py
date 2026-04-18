"""A small example DAG.

Shape:

    fetch -> transform -> load
                      \\-> notify
"""

from __future__ import annotations

import asyncio
import random

from orbiter import DAG, RetryPolicy


dag = DAG(name="etl_example", description="A tiny ETL pipeline")


@dag.task(retry=RetryPolicy(max_attempts=3, backoff="exponential", base_seconds=0.2))
async def fetch() -> dict:
    await asyncio.sleep(0.05)
    return {"rows": 42}


@dag.task(depends_on=["fetch"], retry=RetryPolicy(max_attempts=2))
async def transform() -> int:
    # Simulate a flaky dependency to exercise retries in local runs.
    if random.random() < 0.15:
        raise RuntimeError("transient network blip")
    await asyncio.sleep(0.05)
    return 42 * 2


@dag.task(depends_on=["transform"])
async def load() -> str:
    await asyncio.sleep(0.05)
    return "loaded"


@dag.task(depends_on=["transform"])
async def notify() -> str:
    return "slack message sent"


dag.finalize()
