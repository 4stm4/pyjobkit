"""Core contracts used by Pyjobkit components.

This module exposes explicit abstract base classes rather than ``typing.Protocol``
interfaces. Subclassing these contracts forces implementations to provide the
full API at definition time instead of relying solely on structural typing that
would otherwise be enforced only by optional type checking tools.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypedDict
from uuid import UUID


class ExecContext(ABC):
    """Execution context passed to executors."""

    @abstractmethod
    async def log(self, message: str, /, *, stream: str = "stdout") -> None:
        """Persist a log line for the running job."""

    @abstractmethod
    async def is_cancelled(self) -> bool:
        """Return ``True`` if cancellation was requested for the job."""

    @abstractmethod
    async def set_progress(self, value: float, /, **meta: Any) -> None:
        """Publish a progress update with optional metadata."""


class Executor(ABC):
    """Abstract base class for executor implementations."""

    kind: str

    @abstractmethod
    async def run(self, *, job_id: UUID, payload: dict, ctx: ExecContext) -> dict:
        """Execute a job and return the structured result."""


class OptimisticLockError(RuntimeError):
    """Raised when an expected_version constraint no longer matches."""


class QueueBackend(ABC):
    """Interface for queue backends."""

    class ClaimedJob(TypedDict):
        """Claimed job payload returned by :meth:`claim_batch`.

        Implementations **must** return a mapping containing at least ``id``,
        ``kind``, ``payload``, ``timeout_s``, ``lease_until`` and ``version``
        (for optimistic locking). Additional backend specific keys are allowed
        and simply passed through to workers.
        """

        id: UUID | str
        kind: str
        payload: dict
        timeout_s: int | None
        lease_until: datetime | None
        version: int | None

    @abstractmethod
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
        """Persist a job in the backend and return its identifier."""

    @abstractmethod
    async def get(self, job_id: UUID) -> dict:
        """Return the backend specific representation for ``job_id``."""

    @abstractmethod
    async def cancel(self, job_id: UUID) -> None:
        """Request cancellation for ``job_id``."""

    @abstractmethod
    async def is_cancelled(self, job_id: UUID) -> bool:
        """Return ``True`` if cancellation has been requested."""

    @abstractmethod
    async def claim_batch(self, worker_id: UUID, *, limit: int = 1) -> list[ClaimedJob]:
        """Claim up to ``limit`` ready jobs for ``worker_id``."""

    @abstractmethod
    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        """Mark ``job_id`` as running for ``worker_id``."""

    @abstractmethod
    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        """Extend the lease for ``job_id``.

        ``expected_version`` enables optimistic concurrency; implementations
        should only update the lease when the stored version matches and report
        failure silently otherwise.
        """

    @abstractmethod
    async def succeed(
        self,
        job_id: UUID,
        result: dict,
        *,
        expected_version: int | None = None,
    ) -> None:
        """Record a successful completion for ``job_id``."""

    @abstractmethod
    async def fail(
        self,
        job_id: UUID,
        reason: dict,
        *,
        expected_version: int | None = None,
    ) -> None:
        """Record a failure for ``job_id`` with structured ``reason``."""

    @abstractmethod
    async def timeout(
        self,
        job_id: UUID,
        *,
        expected_version: int | None = None,
    ) -> None:
        """Record a timeout outcome for ``job_id``."""

    @abstractmethod
    async def retry(self, job_id: UUID, *, delay: float) -> None:
        """Requeue ``job_id`` after ``delay`` seconds."""

    @abstractmethod
    async def reap_expired(self) -> int:
        """Return the number of jobs whose leases were reclaimed."""

    @abstractmethod
    async def queue_depth(self) -> int:
        """Return the count of runnable jobs in the backend."""

    @abstractmethod
    async def check_connection(self) -> None:
        """Raise if the backend cannot be reached."""


@dataclass(slots=True)
class LogRecord:
    job_id: UUID
    stream: str
    message: str


class LogSink(ABC):
    """Destination for log records produced by executors."""

    @abstractmethod
    async def write(self, record: LogRecord) -> None:
        """Persist ``record`` in the sink."""


class EventBus(ABC):
    """Publish/subscribe mechanism used for progress updates."""

    @abstractmethod
    async def publish(self, topic: str, payload: dict) -> None:
        """Broadcast ``payload`` to subscribers listening on ``topic``."""
