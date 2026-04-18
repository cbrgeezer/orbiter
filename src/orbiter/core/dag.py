from __future__ import annotations

import hashlib
from collections import defaultdict, deque
from typing import Any, Callable

from orbiter.core.exceptions import DAGValidationError
from orbiter.core.task import Task, TaskFn
from orbiter.retry.backoff import RetryPolicy


class DAG:
    """A directed acyclic graph of tasks.

    Construct one and register tasks on it with the `@dag.task()` decorator.
    The DAG is validated on `finalize()` — this is where cycles and
    missing-dependency errors are caught.
    """

    def __init__(self, name: str, *, description: str = "") -> None:
        self.name = name
        self.description = description
        self.tasks: dict[str, Task] = {}
        self._finalized = False

    def task(
        self,
        *,
        id: str | None = None,
        depends_on: list[str] | None = None,
        retry: RetryPolicy | None = None,
        timeout_seconds: float | None = None,
        idempotency_key: str | None = None,
        tags: list[str] | None = None,
    ) -> Callable[[TaskFn], TaskFn]:
        def decorator(fn: TaskFn) -> TaskFn:
            if self._finalized:
                raise DAGValidationError("cannot add task to a finalized DAG")
            tid = id or fn.__name__
            if tid in self.tasks:
                raise DAGValidationError(f"duplicate task id: {tid}")
            self.tasks[tid] = Task(
                id=tid,
                fn=fn,
                depends_on=list(depends_on or []),
                retry=retry or RetryPolicy(),
                timeout_seconds=timeout_seconds,
                idempotency_key=idempotency_key,
                tags=list(tags or []),
            )
            return fn

        return decorator

    def add_task(self, task: Task) -> None:
        if self._finalized:
            raise DAGValidationError("cannot add task to a finalized DAG")
        if task.id in self.tasks:
            raise DAGValidationError(f"duplicate task id: {task.id}")
        self.tasks[task.id] = task

    def finalize(self) -> "DAG":
        self._validate()
        self._finalized = True
        return self

    def _validate(self) -> None:
        for t in self.tasks.values():
            for dep in t.depends_on:
                if dep not in self.tasks:
                    raise DAGValidationError(
                        f"task {t.id!r} depends on unknown task {dep!r}"
                    )
        # cycle detection via Kahn's algorithm
        indegree: dict[str, int] = {tid: 0 for tid in self.tasks}
        for t in self.tasks.values():
            for dep in t.depends_on:
                indegree[t.id] += 1
        queue = deque(tid for tid, d in indegree.items() if d == 0)
        visited = 0
        adj: dict[str, list[str]] = defaultdict(list)
        for t in self.tasks.values():
            for dep in t.depends_on:
                adj[dep].append(t.id)
        while queue:
            tid = queue.popleft()
            visited += 1
            for child in adj[tid]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        if visited != len(self.tasks):
            raise DAGValidationError("cycle detected in DAG")

    def topological_order(self) -> list[str]:
        """Return a topologically sorted list of task ids."""
        if not self._finalized:
            self._validate()
        indegree: dict[str, int] = {tid: 0 for tid in self.tasks}
        adj: dict[str, list[str]] = defaultdict(list)
        for t in self.tasks.values():
            for dep in t.depends_on:
                indegree[t.id] += 1
                adj[dep].append(t.id)
        order: list[str] = []
        queue = deque(sorted(tid for tid, d in indegree.items() if d == 0))
        while queue:
            tid = queue.popleft()
            order.append(tid)
            for child in sorted(adj[tid]):
                indegree[child] -= 1
                if indegree[child] == 0:
                    queue.append(child)
        return order

    def ready_tasks(self, completed: set[str]) -> list[str]:
        """Return tasks whose dependencies are all in `completed`."""
        ready = []
        for t in self.tasks.values():
            if t.id in completed:
                continue
            if all(dep in completed for dep in t.depends_on):
                ready.append(t.id)
        return sorted(ready)

    def fingerprint(self) -> str:
        """Content hash of the DAG, used for versioning in the state store."""
        h = hashlib.sha256()
        h.update(self.name.encode())
        for tid in sorted(self.tasks):
            h.update(b"|")
            h.update(tid.encode())
            h.update(b"=")
            h.update(self.tasks[tid].fingerprint().encode())
            for dep in sorted(self.tasks[tid].depends_on):
                h.update(b"@")
                h.update(dep.encode())
        return h.hexdigest()

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "description": self.description,
            "tasks": [
                {
                    "id": t.id,
                    "depends_on": list(t.depends_on),
                    "retry": {
                        "max_attempts": t.retry.max_attempts,
                        "backoff": t.retry.backoff,
                        "base_seconds": t.retry.base_seconds,
                        "max_seconds": t.retry.max_seconds,
                    },
                    "timeout_seconds": t.timeout_seconds,
                    "tags": list(t.tags),
                }
                for t in self.tasks.values()
            ],
            "fingerprint": self.fingerprint(),
        }


# `task` is re-exported at package level for cases where the user wants to
# build a DAG imperatively without a decorator-owning instance.
def task(*args: Any, **kwargs: Any) -> Any:  # pragma: no cover - thin re-export
    raise RuntimeError(
        "Use `dag.task(...)` on a DAG instance. The module-level `task` "
        "symbol exists only for type hints."
    )
