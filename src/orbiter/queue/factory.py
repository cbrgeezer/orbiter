from __future__ import annotations

from pathlib import Path
from typing import Literal

from orbiter.queue.queue import InMemoryQueue, StoreBackedQueue
from orbiter.storage.sqlite_store import StateStore


QueueBackend = Literal["memory", "store", "auto"]


def create_queue(
    store: StateStore,
    *,
    backend: QueueBackend = "auto",
    db_target: str | Path = ":memory:",
) -> InMemoryQueue | StoreBackedQueue:
    target = str(db_target)
    if backend not in {"auto", "memory", "store"}:
        raise ValueError(f"unsupported queue backend: {backend}")
    if backend == "memory":
        return InMemoryQueue()
    if backend == "store":
        return StoreBackedQueue(store)
    if target == ":memory:":
        return InMemoryQueue()
    return StoreBackedQueue(store)
