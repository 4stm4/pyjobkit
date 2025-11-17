"""Engine facade exposed to library users."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
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

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:  # type: ignore[override]
        await self.log_sink.write(LogRecord(self.job_id, stream, message))

    async def is_cancelled(self) -> bool:  # type: ignore[override]
        return False  # TODO: expose cancellation from backend

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
    ) -> None:
        self.backend = backend
        self.executors = {e.kind: e for e in executors}
        self.log_sink = log_sink or MemoryLogSink()
        self.event_bus = event_bus or LocalEventBus()

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
        return _Ctx(job_id, self.log_sink, self.event_bus)
