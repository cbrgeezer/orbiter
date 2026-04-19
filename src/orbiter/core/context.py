from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Protocol


class ContextStore(Protocol):
    def checkpoint(self, dag_run_id: str, task_id: str, key: str, value: Any) -> None: ...
    def get_checkpoint(self, dag_run_id: str, task_id: str, key: str) -> Any: ...
    def dag_run(self, dag_run_id: str) -> dict[str, Any] | None: ...


class BoundLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg: str, kwargs: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        extra = dict(kwargs.get("extra", {}))
        extra.update(self.extra)
        kwargs["extra"] = extra
        return msg, kwargs


@dataclass
class TaskContext:
    dag_run_id: str
    task_id: str
    task_run_id: str
    attempt: int
    params: dict[str, Any]
    store: ContextStore
    logger: logging.LoggerAdapter

    def checkpoint(self, key: str, value: Any) -> None:
        self.store.checkpoint(self.dag_run_id, self.task_id, key, value)

    def get_checkpoint(
        self,
        key: str,
        default: Any = None,
        *,
        task_id: str | None = None,
    ) -> Any:
        value = self.store.get_checkpoint(
            self.dag_run_id,
            task_id or self.task_id,
            key,
        )
        return default if value is None else value

    def param(self, key: str, default: Any = None) -> Any:
        return self.params.get(key, default)

    @property
    def is_cancelled(self) -> bool:
        row = self.store.dag_run(self.dag_run_id)
        return row is not None and row["state"] == "cancelled"


def build_task_logger(
    base_logger: logging.Logger,
    *,
    dag_run_id: str,
    task_id: str,
    task_run_id: str,
    attempt: int,
) -> logging.LoggerAdapter:
    return BoundLoggerAdapter(
        base_logger,
        {
            "dag_run_id": dag_run_id,
            "task_id": task_id,
            "task_run_id": task_run_id,
            "attempt": attempt,
        },
    )
