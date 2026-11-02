"""Admission control: the bounded queue between the gateway and the models.

This is the least glamorous and most important component. At every scale of the
platform, the difference between "slow" and "down" is whether the queue is bounded.
An unbounded queue turns overload into a latency collapse for everybody; a bounded
queue turns it into an honest ``503`` for a few.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncIterator, Dict, Optional

from .contracts import Priority
from .errors import NoCapacity, Overloaded


@dataclass(order=True)
class _Waiter:
    # Lower sort key = scheduled first, so priority is negated.
    sort_key: int
    seq: int
    future: asyncio.Future = field(compare=False)
    enqueued_at: float = field(compare=False, default_factory=time.monotonic)


class AdmissionController:
    """Bounded in-flight window plus a priority queue.

    ``max_inflight`` is concurrency, not throughput. For streaming services that is
    the number you actually run out of, because a stream holds its slot for its whole
    duration, not just until the first byte.
    """

    def __init__(
        self,
        max_inflight: int = 32,
        max_queue: int = 256,
        queue_timeout_s: float = 30.0,
    ) -> None:
        self.max_inflight = max_inflight
        self.max_queue = max_queue
        self.queue_timeout_s = queue_timeout_s
        self._inflight = 0
        self._seq = 0
        self._queue: "asyncio.PriorityQueue[_Waiter]" = asyncio.PriorityQueue()
        self._wakeup = asyncio.Event()
        self._dispatcher: Optional[asyncio.Task] = None
        self._closed = False
        self._metrics: Dict[str, int] = {
            "admitted": 0,
            "rejected": 0,
            "timed_out": 0,
            "peak_inflight": 0,
        }

    # -- lifecycle ---------------------------------------------------------

    def _ensure_running(self) -> None:
        if self._dispatcher is None or self._dispatcher.done():
            self._dispatcher = asyncio.ensure_future(self._dispatch())

    async def _dispatch(self) -> None:
        while not self._closed:
            await self._wakeup.wait()
            self._wakeup.clear()
            while self._inflight < self.max_inflight:
                try:
                    waiter = self._queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                if waiter.future.done():
                    continue
                self._inflight += 1
                self._metrics["peak_inflight"] = max(
                    self._metrics["peak_inflight"], self._inflight
                )
                waiter.future.set_result(None)

    async def close(self) -> None:
        self._closed = True
        self._wakeup.set()
        if self._dispatcher is not None:
            self._dispatcher.cancel()
            try:
                await self._dispatcher
            except (asyncio.CancelledError, Exception):
                pass

    # -- admission ---------------------------------------------------------

    async def _acquire(self, priority: Priority, timeout_s: float) -> None:
        self._ensure_running()

        # Fast path: free capacity and nobody already waiting (no queue jumping).
        if self._inflight < self.max_inflight and self._queue.empty():
            self._inflight += 1
            self._metrics["admitted"] += 1
            self._metrics["peak_inflight"] = max(
                self._metrics["peak_inflight"], self._inflight
            )
            return

        if self._queue.qsize() >= self.max_queue:
            self._metrics["rejected"] += 1
            raise NoCapacity(
                "server is at capacity; retry later",
                retry_after=1.0,
                detail={"queue_depth": self._queue.qsize()},
            )

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._seq += 1
        self._queue.put_nowait(_Waiter(-int(priority), self._seq, fut))
        self._wakeup.set()

        try:
            await asyncio.wait_for(fut, timeout=timeout_s)
        except asyncio.TimeoutError:
            self._metrics["timed_out"] += 1
            raise Overloaded(
                "request waited too long for capacity",
                retry_after=1.0,
                detail={"queue_depth": self._queue.qsize()},
            )
        self._metrics["admitted"] += 1

    def _release(self) -> None:
        self._inflight = max(0, self._inflight - 1)
        self._wakeup.set()

    @asynccontextmanager
    async def slot(
        self, priority: Priority = Priority.STANDARD, timeout_s: Optional[float] = None
    ) -> AsyncIterator[None]:
        await self._acquire(priority, timeout_s or self.queue_timeout_s)
        try:
            yield
        finally:
            self._release()

    # -- introspection -----------------------------------------------------

    def stats(self) -> Dict[str, Any]:
        return {
            "inflight": self._inflight,
            "max_inflight": self.max_inflight,
            "queued": self._queue.qsize(),
            "max_queue": self.max_queue,
            **self._metrics,
        }


class NullAdmission:
    """Unlimited admission. Fine for a laptop demo; never for production."""

    @asynccontextmanager
    async def slot(self, priority: Priority = Priority.STANDARD, timeout_s: Optional[float] = None):
        yield

    def stats(self) -> Dict[str, Any]:
        return {"mode": "unlimited"}
