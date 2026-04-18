from __future__ import annotations

import importlib
from typing import Any, Callable, Protocol


class Operator(Protocol):
    """An Operator is a reusable task body.

    Plugins register operators by name. User DAGs reference them by name
    and pass parameters. The operator instance is constructed at DAG parse
    time and its `execute` method is called by the worker.
    """

    name: str

    def execute(self, **params: Any) -> Any: ...


class PluginRegistry:
    """In-process registry for operators and queue backends."""

    def __init__(self) -> None:
        self._operators: dict[str, type[Operator]] = {}
        self._queue_backends: dict[str, Callable[..., Any]] = {}

    def register_operator(self, name: str, op_cls: type[Operator]) -> None:
        if name in self._operators:
            raise ValueError(f"operator already registered: {name}")
        self._operators[name] = op_cls

    def get_operator(self, name: str) -> type[Operator]:
        if name not in self._operators:
            raise KeyError(f"unknown operator: {name}")
        return self._operators[name]

    def register_queue_backend(
        self, name: str, factory: Callable[..., Any]
    ) -> None:
        self._queue_backends[name] = factory

    def get_queue_backend(self, name: str) -> Callable[..., Any]:
        if name not in self._queue_backends:
            raise KeyError(f"unknown queue backend: {name}")
        return self._queue_backends[name]

    def load_module(self, dotted: str) -> None:
        """Load a plugin module. The module is expected to call register_*
        functions at import time."""
        importlib.import_module(dotted)


REGISTRY = PluginRegistry()
