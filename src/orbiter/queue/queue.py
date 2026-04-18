from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from dataclasses import dataclass, field
from typing import Protocol


@dataclass
class QueueMessage:
    task_run_id: str
    task_id: str
    dag_run_id: str
    attempt: int
    idempotency_key: str
    queue_id: int = 0


class Queue(Protocol):
    async def put(self, msg: QueueMessage, *, delay_seconds: float = 0.0) -> None: ...
    async def get(self, *, timeout: float | None = None) -> QueueMessage | None: ...
    async def ack(self, msg: QueueMessage) -> None: ...
    async def nack(self, msg: QueueMessage, *, delay_seconds: float = 0.0) -> None: ...


@dataclass(order=True)
class _Entry:
    available_at: float
    seq: int
    msg: QueueMessage = field(compare=False)


class InMemoryQueue:
    """Event-driven in-memory priority queue with delayed delivery.

    This is the queue the unit tests and the single-process runtime use. It
    wakes consumers via an asyncio.Event rather than polling — see the
    architecture doc for the event-driven vs polling discussion.
    """

    def __init__(self) -> None:
        self._heap: list[_Entry] = []
        self._counter = itertools.count()
        self._event = asyncio.Event()
        self._lock = asyncio.Lock()

    async def put(self, msg: QueueMessage, *, delay_seconds: float = 0.0) -> None:
        async with self._lock:
            entry = _Entry(
                available_at=time.time() + max(0.0, delay_seconds),
                seq=next(self._counter),
                msg=msg,
            )
            heapq.heappush(self._heap, entry)
            self._event.set()

    async def get(self, *, timeout: float | None = None) -> QueueMessage | None:
        deadline = None if timeout is None else time.time() + timeout
        while True:
            async with self._lock:
                now = time.time()
                if self._heap and self._heap[0].available_at <= now:
                    entry = heapq.heappop(self._heap)
                    if not self._heap:
                        self._event.clear()
                    return entry.msg
                wait = None
                if self._heap:
                    wait = max(0.0, self._heap[0].available_at - now)
                if deadline is not None:
                    remaining = deadline - now
                    wait = remaining if wait is None else min(wait, remaining)
                self._event.clear()
            try:
                if wait is None:
                    await self._event.wait()
                else:
                    await asyncio.wait_for(self._event.wait(), timeout=wait)
            except asyncio.TimeoutError:
                if deadline is not None and time.time() >= deadline:
                    return None
                # otherwise loop: the front-of-queue item might be ready now

    async def ack(self, msg: QueueMessage) -> None:
        # no-op: at-least-once semantics; we remove on get()
        return None

    async def nack(self, msg: QueueMessage, *, delay_seconds: float = 0.0) -> None:
        await self.put(msg, delay_seconds=delay_seconds)

    def __len__(self) -> int:
        return len(self._heap)
