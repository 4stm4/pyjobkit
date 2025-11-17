"""In-memory backend implementation."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
from uuid import UUID, uuid4

from ..contracts import QueueBackend


UTC = timezone.utc


@dataclass
class _Job:
    id: UUID
    created_at: datetime
    scheduled_for: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    status: str = "queued"
    attempts: int = 0
    max_attempts: int = 3
    priority: int = 100
    kind: str = ""
    payload: dict = field(default_factory=dict)
    result: dict | None = None
    idempotency_key: str | None = None
    cancel_requested: bool = False
    leased_by: UUID | None = None
    lease_until: datetime | None = None
    version: int = 0
    timeout_s: int | None = None


class MemoryBackend(QueueBackend):
    def __init__(self, *, lease_ttl_s: int = 30) -> None:
        self._jobs: Dict[UUID, _Job] = {}
        self._lock = asyncio.Lock()
        self.lease_ttl_s = lease_ttl_s

    async def enqueue(self, **kwargs):  # type: ignore[override]
        async with self._lock:
            idempotency_key = kwargs.get("idempotency_key")
            if idempotency_key:
                for job in self._jobs.values():
                    if job.idempotency_key == idempotency_key:
                        return job.id
            job_id = uuid4()
            now = datetime.now(UTC)
            job = _Job(
                id=job_id,
                created_at=now,
                scheduled_for=kwargs.get("scheduled_for") or now,
                max_attempts=kwargs.get("max_attempts", 3),
                priority=kwargs.get("priority", 100),
                kind=kwargs["kind"],
                payload=kwargs.get("payload", {}),
                idempotency_key=idempotency_key,
                timeout_s=kwargs.get("timeout_s"),
            )
            self._jobs[job_id] = job
            return job_id

    async def get(self, job_id: UUID) -> dict:  # type: ignore[override]
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(job_id)
            return self._job_to_dict(job)

    async def cancel(self, job_id: UUID) -> None:  # type: ignore[override]
        async with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.cancel_requested = True

    async def is_cancelled(self, job_id: UUID) -> bool:  # type: ignore[override]
        async with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.cancel_requested)

    async def claim_batch(self, worker_id: UUID, *, limit: int = 1) -> List[dict]:  # type: ignore[override]
        async with self._lock:
            now = datetime.now(UTC)
            candidates = sorted(
                (
                    job
                    for job in self._jobs.values()
                    if job.status == "queued"
                    and job.scheduled_for <= now
                    and (job.lease_until is None or job.lease_until <= now)
                ),
                key=lambda j: (j.priority, j.created_at),
            )
            claimed: List[dict] = []
            for job in candidates[:limit]:
                job.lease_until = now + timedelta(seconds=self.lease_ttl_s)
                job.leased_by = worker_id
                job.version += 1
                claimed.append(self._job_to_dict(job))
            return claimed

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:  # type: ignore[override]
        async with self._lock:
            job = self._jobs[job_id]
            job.status = "running"
            job.started_at = datetime.now(UTC)
            job.attempts += 1

    async def extend_lease(
        self,
        job_id: UUID,
        worker_id: UUID,
        ttl_s: int,
        *,
        expected_version: int | None = None,
    ) -> None:  # type: ignore[override]
        async with self._lock:
            job = self._jobs[job_id]
            if job.leased_by == worker_id and (
                expected_version is None or job.version == expected_version
            ):
                job.lease_until = datetime.now(UTC) + timedelta(seconds=ttl_s)

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None:  # type: ignore[override]
        await self._finish(job_id, "success", result, expected_version=expected_version)

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None:  # type: ignore[override]
        await self._finish(job_id, "failed", reason, expected_version=expected_version)

    async def timeout(
        self, job_id: UUID, *, expected_version: int | None = None
    ) -> None:  # type: ignore[override]
        await self._finish(
            job_id, "timeout", {"error": "timeout"}, expected_version=expected_version
        )

    async def retry(self, job_id: UUID, *, delay: float) -> None:  # type: ignore[override]
        async with self._lock:
            job = self._jobs[job_id]
            job.status = "queued"
            job.scheduled_for = datetime.now(UTC) + timedelta(seconds=delay)
            job.lease_until = None
            job.leased_by = None
            job.version += 1

    async def reap_expired(self) -> int:  # type: ignore[override]
        async with self._lock:
            now = datetime.now(UTC)
            expired = [
                job
                for job in self._jobs.values()
                if job.leased_by is not None
                and job.lease_until is not None
                and job.lease_until <= now
                and job.status in {"queued", "running"}
            ]
            for job in expired:
                job.status = "failed"
                job.finished_at = now
                job.result = {"error": "lease_expired"}
                job.lease_until = None
                job.leased_by = None
                job.version += 1
            return len(expired)

    async def queue_depth(self) -> int:  # type: ignore[override]
        async with self._lock:
            return sum(1 for job in self._jobs.values() if job.status == "queued")

    async def check_connection(self) -> None:  # type: ignore[override]
        return None

    async def _finish(
        self, job_id: UUID, status: str, result: dict, *, expected_version: int | None
    ) -> None:
        async with self._lock:
            job = self._jobs[job_id]
            if expected_version is not None and job.version != expected_version:
                return
            job.status = status
            job.finished_at = datetime.now(UTC)
            job.result = result
            job.lease_until = None
            job.leased_by = None
            job.version += 1

    @staticmethod
    def _job_to_dict(job: _Job) -> dict:
        data = asdict(job)
        data["id"] = job.id
        return data
