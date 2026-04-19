from __future__ import annotations

import asyncio
import inspect
import logging
import time
import traceback
import uuid
from typing import Any

from orbiter.core.context import TaskContext, build_task_logger
from orbiter.core.dag import DAG
from orbiter.core.exceptions import PermanentTaskError
from orbiter.core.state import DagRunState, TaskState
from orbiter.queue.queue import Queue, QueueMessage
from orbiter.retry.backoff import compute_delay
from orbiter.storage.sqlite_store import StateStore


log = logging.getLogger("orbiter.worker")


class Worker:
    """Pulls messages from a queue and executes the matching task.

    Workers are cheap: create as many as you want. Each worker holds a
    reference to the DAG (for task lookup) and the state store.
    """

    def __init__(
        self,
        dag: DAG,
        queue: Queue,
        store: StateStore,
        *,
        worker_id: str | None = None,
        fault_injector: "FaultInjector | None" = None,
    ) -> None:
        self.dag = dag
        self.queue = queue
        self.store = store
        self.worker_id = worker_id or f"w-{uuid.uuid4().hex[:8]}"
        self.fault = fault_injector
        self._stop = asyncio.Event()
        self.processed = 0

    async def run(self) -> None:
        log.info("worker %s starting", self.worker_id)
        while not self._stop.is_set():
            msg = await self.queue.get(timeout=0.25)
            if msg is None:
                continue
            await self._handle(msg)
        log.info("worker %s stopped", self.worker_id)

    def stop(self) -> None:
        self._stop.set()

    async def _handle(self, msg: QueueMessage) -> None:
        task = self.dag.tasks.get(msg.task_id)
        if task is None:
            log.error("unknown task %s; dropping", msg.task_id)
            await self.queue.ack(msg)
            return

        dag_run = self.store.dag_run(msg.dag_run_id)
        if dag_run is not None and dag_run["state"] == DagRunState.CANCELLED.value:
            self.store.set_task_state(
                msg.task_run_id, TaskState.SKIPPED, error="dag run cancelled"
            )
            await self.queue.ack(msg)
            return

        self.store.set_task_state(msg.task_run_id, TaskState.RUNNING)
        task_params = self._load_params(dag_run)
        context = TaskContext(
            dag_run_id=msg.dag_run_id,
            task_id=msg.task_id,
            task_run_id=msg.task_run_id,
            attempt=msg.attempt,
            params=task_params,
            store=self.store,
            logger=build_task_logger(
                log,
                dag_run_id=msg.dag_run_id,
                task_id=msg.task_id,
                task_run_id=msg.task_run_id,
                attempt=msg.attempt,
            ),
        )
        try:
            if self.fault:
                self.fault.before_task(msg)
            output = await self._invoke(task.fn, task.timeout_seconds, context)
            if self.fault:
                self.fault.after_task_before_ack(msg)
            self.store.set_task_state(
                msg.task_run_id, TaskState.SUCCEEDED, output=output
            )
            await self.queue.ack(msg)
            self.processed += 1
        except PermanentTaskError as e:
            self.store.set_task_state(
                msg.task_run_id, TaskState.FAILED, error=str(e)
            )
            await self.queue.ack(msg)
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}\n{traceback.format_exc()}"
            if task.retry.should_retry(msg.attempt):
                delay = compute_delay(task.retry, msg.attempt)
                self.store.set_task_state(
                    msg.task_run_id, TaskState.RETRYING, error=err
                )
                retry_msg = QueueMessage(
                    task_run_id="",  # will be created by scheduler
                    task_id=msg.task_id,
                    dag_run_id=msg.dag_run_id,
                    attempt=msg.attempt + 1,
                    idempotency_key="",  # scheduler recomputes
                )
                # We signal via the state; the scheduler picks it up.
                await self.queue.ack(msg)
                # enqueue the retry by re-entering via the scheduler side door:
                # callers wire in a `retry_sink` if they want worker-driven retries.
                if hasattr(self.queue, "_retry_sink") and callable(
                    getattr(self.queue, "_retry_sink")
                ):
                    await self.queue._retry_sink(retry_msg, delay)  # type: ignore[attr-defined]
            else:
                self.store.set_task_state(
                    msg.task_run_id, TaskState.DEAD_LETTER, error=err
                )
                await self.queue.ack(msg)

    def _load_params(self, dag_run: dict[str, Any] | None) -> dict[str, Any]:
        if dag_run is None:
            return {}
        raw = dag_run.get("params_json")
        if not raw:
            return {}
        if isinstance(raw, str):
            import json

            loaded = json.loads(raw)
            return loaded if isinstance(loaded, dict) else {}
        return {}

    async def _invoke(self, fn: Any, timeout: float | None, context: TaskContext) -> Any:
        call = self._bind_call(fn, context)
        if asyncio.iscoroutinefunction(fn):
            coro = call()
        else:
            loop = asyncio.get_running_loop()
            coro = loop.run_in_executor(None, call)
        if timeout is not None:
            return await asyncio.wait_for(coro, timeout=timeout)
        return await coro

    def _bind_call(self, fn: Any, context: TaskContext) -> Any:
        sig = inspect.signature(fn)
        if not sig.parameters:
            return fn
        kwargs: dict[str, Any] = {}
        positional: list[Any] = []
        for param in sig.parameters.values():
            if param.kind in {
                inspect.Parameter.VAR_POSITIONAL,
                inspect.Parameter.VAR_KEYWORD,
            }:
                continue
            if param.name == "context" or param.annotation is TaskContext:
                if param.kind in {
                    inspect.Parameter.POSITIONAL_ONLY,
                    inspect.Parameter.POSITIONAL_OR_KEYWORD,
                } and not kwargs:
                    positional.append(context)
                else:
                    kwargs[param.name] = context
                continue
            if param.name == "params":
                kwargs[param.name] = context.params
                continue
            if param.name in context.params:
                kwargs[param.name] = context.params[param.name]
                continue
            if param.default is inspect._empty:
                raise TypeError(
                    f"task function {fn.__name__} expects unsupported parameter {param.name!r}"
                )
        if positional and kwargs:
            return lambda: fn(*positional, **kwargs)
        if positional:
            return lambda: fn(*positional)
        if kwargs:
            return lambda: fn(**kwargs)
        return fn


class FaultInjector:
    """Test-only hooks for exercising recovery paths.

    Callers set the `crash_before_ack` or `crash_before_task` flags between
    task submissions to simulate worker death at precise moments.
    """

    def __init__(self) -> None:
        self.crash_before_task = False
        self.crash_before_ack = False
        self.crashes: list[str] = []

    def before_task(self, msg: QueueMessage) -> None:
        if self.crash_before_task:
            self.crash_before_task = False
            self.crashes.append(f"pre:{msg.task_id}")
            raise _WorkerCrash("simulated crash before task")

    def after_task_before_ack(self, msg: QueueMessage) -> None:
        if self.crash_before_ack:
            self.crash_before_ack = False
            self.crashes.append(f"post:{msg.task_id}")
            raise _WorkerCrash("simulated crash after task, before ack")


class _WorkerCrash(RuntimeError):
    """Distinguished from user errors so tests can assert on recovery path."""


def now() -> float:  # pragma: no cover - trivial
    return time.time()
