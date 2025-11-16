"""Core contracts used by Pyjobkit components."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID


class ExecContext(Protocol):
    """Execution context passed to executors."""

    async def log(self, message: str, /, *, stream: str = "stdout") -> None: ...

    async def is_cancelled(self) -> bool: ...

    async def set_progress(self, value: float, /, **meta: Any) -> None: ...


class Executor(Protocol):
    """Protocol for executor implementations."""

    kind: str

    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict: ...


class QueueBackend(Protocol):
    """Interface for queue backends."""

    async def enqueue(
        self,
        *,
        kind: str,
        payload: dict,
        priority: int = 100,
        scheduled_for: datetime | None = None,
        max_attempts: int = 3,
        idempotency_key: str | None = None,
        timeout_s: int | None = None,) -> UUID: ...

    async def get(self, job_id: UUID) -> dict: ...

    async def cancel(self, job_id: UUID) -> None: ...

    async def claim_batch(self, worker_id: UUID, *, limit: int = 1) -> list[dict]: ...

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None: ...

    async def extend_lease(self, job_id: UUID, worker_id: UUID, ttl_s: int) -> None: ...

    async def succeed(self, job_id: UUID, result: dict) -> None: ...

    async def fail(self, job_id: UUID, reason: dict) -> None: ...

    async def timeout(self, job_id: UUID) -> None: ...


@dataclass(slots=True)
class LogRecord:
    job_id: UUID
    stream: str
    message: str


class LogSink(Protocol):
    async def write(self, record: LogRecord) -> None: ...


class EventBus(Protocol):
    async def publish(self, topic: str, payload: dict) -> None: ...
