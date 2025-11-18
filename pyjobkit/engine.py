"""Engine facade exposed to library users."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import re
from typing import Iterable, Protocol
from uuid import UUID

from .contracts import EventBus, ExecContext, Executor, LogRecord, LogSink, QueueBackend
from .events.local import LocalEventBus
from .logging.memory import MemoryLogSink

PROGRESS_TOPIC_TEMPLATE = "job.{job_id}.progress"
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
class DefaultExecContext(ExecContext):  # type: ignore[misc]
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
        exec_context_factory: ExecContextFactory | None = None,
    ) -> None:
        self.backend = backend
        self.executors = self._build_executor_map(executors)
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

        while True:
            depth = await self.queue_depth()
            if depth < self.max_queue_size:
                return
            if deadline is None:
                raise TimeoutError(
                    "queue capacity reached and enqueue_timeout_s is not set; "
                    "provide a timeout to wait for capacity or increase max_queue_size"
                )
            if deadline and datetime.now(timezone.utc) >= deadline:
                raise TimeoutError("enqueue timed out waiting for queue capacity")

            now = datetime.now(timezone.utc)
            if (now - last_log_time).total_seconds() >= 1:
                logger.warning(
                    "enqueue waiting for capacity: depth=%s max_queue_size=%s timeout_s=%s",
                    depth,
                    self.max_queue_size,
                    self.enqueue_timeout_s,
                )
                last_log_time = now
            await asyncio.sleep(self._enqueue_check_interval_s)

    def __repr__(self) -> str:
        return (
            f"Engine(max_queue_size={self.max_queue_size}, "
            f"enqueue_timeout_s={self.enqueue_timeout_s}, executors={list(self.executors)})"
        )

    @staticmethod
    def _build_executor_map(executors: Iterable[Executor]) -> dict[str, Executor]:
        mapping: dict[str, Executor] = {}
        for executor in executors:
            if executor.kind in mapping:
                raise ValueError(f"duplicate executor kind registered: {executor.kind}")
            mapping[executor.kind] = executor
        return mapping
