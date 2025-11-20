"""Engine facade exposed to library users."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Any, Iterable, Protocol
from uuid import UUID

from .contracts import EventBus, ExecContext, Executor, LogRecord, LogSink, QueueBackend
from .events.local import LocalEventBus
from .logging.memory import MemoryLogSink

PROGRESS_TOPIC_TEMPLATE = "job.{job_id}.progress"
KIND_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")
logger = logging.getLogger(__name__)


class ExecContextFactory(Protocol):
    """Factory for creating execution contexts."""

    def __call__(
        self,
        job_id: UUID,
        /,
        *,
        log_sink: LogSink,
        event_bus: EventBus,
        backend: QueueBackend,
    ) -> ExecContext: ...


@dataclass(slots=True)
class DefaultExecContext(ExecContext):
    job_id: UUID
    log_sink: LogSink
    event_bus: EventBus
    backend: QueueBackend

    async def log(self, message: str, /, *, stream: str = "stdout") -> None:
        await self.log_sink.write(LogRecord(self.job_id, stream, message))

    async def is_cancelled(self) -> bool:
        return await self.backend.is_cancelled(self.job_id)

    async def set_progress(self, value: float, /, **meta: Any) -> None:
        await self.event_bus.publish(
            PROGRESS_TOPIC_TEMPLATE.format(job_id=self.job_id),
            {"value": value, **meta},
        )


class Engine:
    """Front-end for enqueueing and introspecting jobs.

    Parameters:
        backend: Queue storage implementation responsible for persistence and state.
        executors: Iterable of executor implementations to register immediately.
        log_sink: Destination for log messages; defaults to in-memory sink.
        event_bus: Event bus for progress updates; defaults to in-process bus.
        max_queue_size: Optional cap on queued jobs before enqueue blocks.
        enqueue_timeout_s: Optional timeout while waiting for queue capacity.
        enqueue_check_interval_s: Polling interval for queue depth checks.
        exec_context_factory: Factory for creating ``ExecContext`` instances.
    """

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
        exec_context_factory: ExecContextFactory | None = None,
    ) -> None:
        self.backend = backend
        self.executors: dict[str, Executor] = {}
        for executor in executors:
            self.register_executor(executor)
        self.log_sink = log_sink or MemoryLogSink()
        self.event_bus = event_bus or LocalEventBus()
        self.max_queue_size = max_queue_size
        self.enqueue_timeout_s = enqueue_timeout_s
        self._enqueue_check_interval_s = enqueue_check_interval_s
        self._exec_context_factory = exec_context_factory or DefaultExecContext

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
        """Enqueue a job for processing and return its identifier.

        The job is validated against registered ``kind`` patterns and then queued
        in the configured backend. If ``max_queue_size`` is set, the method waits
        for capacity for up to ``enqueue_timeout_s`` seconds before raising
        :class:`TimeoutError`.

        Args:
            kind: Identifier of the executor that should handle the job.
            payload: Arbitrary JSON-serializable payload passed to the executor.
            priority: Lower values are executed sooner (backend-specific).
            scheduled_for: Optional time to delay execution until.
            max_attempts: Maximum retry attempts for the job.
            idempotency_key: Optional key used to de-duplicate enqueues.
            timeout_s: Optional time limit for job execution.

        Returns:
            UUID: The identifier of the enqueued job.
        """

        if not KIND_PATTERN.fullmatch(kind):
            raise ValueError("kind must contain only alphanumerics, dash, underscore, or dot")
        logger.info(
            "enqueue requested: kind=%s priority=%s scheduled_for=%s timeout_s=%s idempotency_key=%s",
            kind,
            priority,
            scheduled_for,
            timeout_s,
            idempotency_key,
        )
        await self._wait_for_capacity()
        job_id = await self.backend.enqueue(
            kind=kind,
            payload=payload,
            priority=priority,
            scheduled_for=scheduled_for,
            max_attempts=max_attempts,
            idempotency_key=idempotency_key,
            timeout_s=timeout_s,
        )
        logger.info("enqueue accepted: job_id=%s kind=%s priority=%s", job_id, kind, priority)
        return job_id

    async def get(self, job_id: UUID) -> dict:
        """Retrieve job metadata from the backend."""

        logger.debug("get requested: job_id=%s", job_id)
        return await self.backend.get(job_id)

    async def cancel(self, job_id: UUID) -> None:
        """Request cancellation of a job in the backend."""

        logger.info("cancel requested: job_id=%s", job_id)
        await self.backend.cancel(job_id)

    def executor_for(self, kind: str) -> Executor | None:
        return self.executors.get(kind)

    def register_executor(self, executor: Executor) -> None:
        """Register a new executor at runtime.

        Raises:
            ValueError: If an executor with the same ``kind`` is already registered
                or the kind contains invalid characters.
        """

        if not KIND_PATTERN.fullmatch(executor.kind):
            raise ValueError(
                "executor kind must contain only alphanumerics, dash, underscore, or dot"
            )
        if executor.kind in self.executors:
            raise ValueError(f"duplicate executor kind registered: {executor.kind}")
        self.executors[executor.kind] = executor

    def make_ctx(self, job_id: UUID) -> ExecContext:
        return self._exec_context_factory(
            job_id,
            log_sink=self.log_sink,
            event_bus=self.event_bus,
            backend=self.backend,
        )

    async def claim_batch(self, worker_id: UUID, *, limit: int = 1) -> list[QueueBackend.ClaimedJob]:
        return await self.backend.claim_batch(worker_id, limit=limit)

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
        await self.backend.mark_running(job_id, worker_id)

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:
        await self.backend.extend_lease(
            job_id, worker_id, ttl_s, expected_version=expected_version
        )

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None:
        await self.backend.succeed(job_id, result, expected_version=expected_version)

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None:
        await self.backend.fail(job_id, reason, expected_version=expected_version)

    async def timeout(self, job_id: UUID, *, expected_version: int | None = None) -> None:
        await self.backend.timeout(job_id, expected_version=expected_version)

    async def retry(self, job_id: UUID, *, delay: float) -> None:
        await self.backend.retry(job_id, delay=delay)

    async def reap_expired(self) -> int:
        return await self.backend.reap_expired()

    async def queue_depth(self) -> int:
        return await self.backend.queue_depth()

    async def check_connection(self) -> None:
        await self.backend.check_connection()

    async def _wait_for_capacity(self) -> None:
        if self.max_queue_size is None:
            return

        deadline = (
            datetime.now(timezone.utc) + timedelta(seconds=self.enqueue_timeout_s)
            if self.enqueue_timeout_s is not None
            else None
        )

        last_log_time = datetime.now(timezone.utc)

        waited_for_capacity = False

        try:
            while True:
                depth = await self.queue_depth()
                if depth < self.max_queue_size:
                    if deadline is not None and waited_for_capacity:
                        logger.info(
                            "enqueue capacity available after waiting: depth=%s max_queue_size=%s",
                            depth,
                            self.max_queue_size,
                        )
                    return
                waited_for_capacity = True
                if deadline is None:
                    logger.error(
                        "enqueue capacity reached without timeout: depth=%s max_queue_size=%s",
                        depth,
                        self.max_queue_size,
                    )
                    raise TimeoutError(
                        "queue capacity reached and enqueue_timeout_s is not set; "
                        "provide a timeout to wait for capacity or increase max_queue_size"
                    )
                now = datetime.now(timezone.utc)
                if now >= deadline:
                    logger.error(
                        "enqueue timed out waiting for queue capacity: depth=%s max_queue_size=%s timeout_s=%s",
                        depth,
                        self.max_queue_size,
                        self.enqueue_timeout_s,
                    )
                    raise TimeoutError("enqueue timed out waiting for queue capacity")

                if (now - last_log_time).total_seconds() >= 1:
                    logger.warning(
                        "enqueue waiting for capacity: depth=%s max_queue_size=%s timeout_s=%s",
                        depth,
                        self.max_queue_size,
                        self.enqueue_timeout_s,
                    )
                    last_log_time = now

                sleep_time = min(
                    self._enqueue_check_interval_s,
                    (deadline - now).total_seconds() if deadline else self._enqueue_check_interval_s,
                )
                await asyncio.sleep(sleep_time)
        except asyncio.CancelledError:
            logger.info("enqueue wait cancelled; aborting capacity wait")
            raise

    def __repr__(self) -> str:
        executors_repr = {kind: executor.__class__.__name__ for kind, executor in self.executors.items()}
        return (
            f"Engine(max_queue_size={self.max_queue_size}, "
            f"enqueue_timeout_s={self.enqueue_timeout_s}, executors={executors_repr})"
        )

    async def close(self) -> None:
        """Release held resources for backend, log sink, or event bus when supported."""

        for component in (self.backend, self.log_sink, self.event_bus):
            close = getattr(component, "close", None)
            if callable(close):
                result = close()
                if asyncio.iscoroutine(result):
                    await result

