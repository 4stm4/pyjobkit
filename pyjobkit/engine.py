"""Engine facade exposed to library users."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Iterable
from uuid import UUID

from .contracts import EventBus, ExecContext, Executor, LogRecord, LogSink, QueueBackend
from .events.local import LocalEventBus
from .logging.memory import MemoryLogSink

PROGRESS_TOPIC_TEMPLATE = "job.{job_id}.progress"


@dataclass(slots=True)
class _Ctx(ExecContext):  # type: ignore[misc]
    job_id: UUID
    log_sink: LogSink
    event_bus: EventBus
    backend: QueueBackend

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:  # type: ignore[override]
        await self.log_sink.write(LogRecord(self.job_id, stream, message))

    async def is_cancelled(self) -> bool:  # type: ignore[override]
        return await self.backend.is_cancelled(self.job_id)

    async def set_progress(self, value: float, /, **meta):  # type: ignore[override]
        await self.event_bus.publish(
            PROGRESS_TOPIC_TEMPLATE.format(job_id=self.job_id),
            {"value": value, **meta},
        )


class Engine:
    """Front-end for enqueueing and introspecting jobs."""

    def __init__(
        self,
        *,
        backend: QueueBackend,
        executors: Iterable[Executor],
        log_sink: LogSink | None = None,
        event_bus: EventBus | None = None,
        max_queue_size: int | None = None,
        enqueue_timeout_s: float | None = None,
        enqueue_check_interval_s: float = 0.1,
    ) -> None:
        self.backend = backend
        self.executors = {e.kind: e for e in executors}
        self.log_sink = log_sink or MemoryLogSink()
        self.event_bus = event_bus or LocalEventBus()
        self.max_queue_size = max_queue_size
        self.enqueue_timeout_s = enqueue_timeout_s
        self._enqueue_check_interval_s = enqueue_check_interval_s

    async def enqueue(
        self,
        *,
        kind: str,
        payload: dict,
        priority: int = 100,
        scheduled_for: datetime | None = None,
        max_attempts: int = 3,
        idempotency_key: str | None = None,
        timeout_s: int | None = None,
    ) -> UUID:
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", kind):
            raise ValueError("kind must contain only alphanumerics, dash, underscore, or dot")
        await self._wait_for_capacity()
        return await self.backend.enqueue(
            kind=kind,
            payload=payload,
            priority=priority,
            scheduled_for=scheduled_for,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            timeout_s=timeout_s,
        )

    async def get(self, job_id: UUID) -> dict:
        return await self.backend.get(job_id)

    async def cancel(self, job_id: UUID) -> None:
        await self.backend.cancel(job_id)

    def executor_for(self, kind: str) -> Executor | None:
        return self.executors.get(kind)

    def make_ctx(self, job_id: UUID) -> ExecContext:
        return _Ctx(job_id, self.log_sink, self.event_bus, self.backend)

    async def _wait_for_capacity(self) -> None:
        if self.max_queue_size is None:
            return

        deadline = (
            datetime.now(timezone.utc) + timedelta(seconds=self.enqueue_timeout_s)
            if self.enqueue_timeout_s is not None
            else None
        )

        while True:
            depth = await self.backend.queue_depth()
            if depth < self.max_queue_size:
                return
            if deadline and datetime.now(timezone.utc) >= deadline:
                raise TimeoutError("enqueue timed out waiting for queue capacity")
            await asyncio.sleep(self._enqueue_check_interval_s)

    def __repr__(self) -> str:
        return (
            f"Engine(max_queue_size={self.max_queue_size}, "
            f"enqueue_timeout_s={self.enqueue_timeout_s}, executors={list(self.executors)})"
        )
