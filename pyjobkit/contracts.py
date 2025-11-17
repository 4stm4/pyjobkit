"""Core contracts used by Pyjobkit components."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol, TypedDict
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

    class ClaimedJob(TypedDict):
        """Claimed job payload returned by :meth:`claim_batch`.

        Attributes:
            id: Unique job identifier.
            kind: Executor kind requested by the job author.
            payload: Job payload passed through to the executor.
            timeout_s: Optional timeout for the job run in seconds.
            lease_until: Backend-specific timestamp for the current lease.
        """

        id: UUID | str
        kind: str
        payload: dict
        timeout_s: int | None
        lease_until: datetime | None

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

    async def is_cancelled(self, job_id: UUID) -> bool: ...

    async def claim_batch(self, worker_id: UUID, *, limit: int = 1) -> list[ClaimedJob]: ...

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None: ...

    async def extend_lease(
        self, job_id: UUID, worker_id: UUID, ttl_s: int, *, expected_version: int | None = None
    ) -> None: ...

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None: ...

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None: ...

    async def timeout(self, job_id: UUID, *, expected_version: int | None = None) -> None: ...

    async def retry(self, job_id: UUID, *, delay: float) -> None: ...

    async def reap_expired(self) -> int: ...

    async def queue_depth(self) -> int: ...

    async def check_connection(self) -> None: ...


@dataclass(slots=True)
class LogRecord:
    job_id: UUID
    stream: str
    message: str


class LogSink(Protocol):
    async def write(self, record: LogRecord) -> None: ...


class EventBus(Protocol):
    async def publish(self, topic: str, payload: dict) -> None: ...
