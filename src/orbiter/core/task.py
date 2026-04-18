from __future__ import annotations

import hashlib
import inspect
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Union

from orbiter.retry.backoff import RetryPolicy


TaskFn = Union[Callable[..., Any], Callable[..., Awaitable[Any]]]


@dataclass
class Task:
    """A single unit of work inside a DAG.

    Tasks are pure Python callables, sync or async, wrapped with metadata:
    retry policy, dependencies, and an optional idempotency key generator.
    """

    id: str
    fn: TaskFn
    depends_on: list[str] = field(default_factory=list)
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    timeout_seconds: float | None = None
    idempotency_key: str | None = None
    tags: list[str] = field(default_factory=list)

    def is_async(self) -> bool:
        return inspect.iscoroutinefunction(self.fn)

    def fingerprint(self) -> str:
        """Stable hash of the task definition. Used for DAG versioning.

        We hash the id, the qualified function name, and the source if
        available. Source hashing gives a content-version so the state store
        can detect that a redeployed DAG changed a task.
        """
        h = hashlib.sha256()
        h.update(self.id.encode())
        h.update(b"|")
        h.update(f"{self.fn.__module__}.{self.fn.__qualname__}".encode())
        try:
            src = inspect.getsource(self.fn)
            h.update(b"|")
            h.update(src.encode())
        except (OSError, TypeError):
            pass
        return h.hexdigest()

    def compute_idempotency_key(self, dag_run_id: str, inputs: dict[str, Any]) -> str:
        """Stable key for this (task, dag_run, inputs) triple.

        The scheduler uses this to detect redeliveries and skip re-executing a
        task that already succeeded within the same logical run.
        """
        if self.idempotency_key is not None:
            base = self.idempotency_key
        else:
            base = f"{dag_run_id}:{self.id}"
        h = hashlib.sha256(base.encode())
        for k in sorted(inputs):
            h.update(b"|")
            h.update(f"{k}={inputs[k]!r}".encode())
        return h.hexdigest()
