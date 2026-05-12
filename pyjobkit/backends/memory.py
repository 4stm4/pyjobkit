"""In-memory backend implementation.

This backend keeps the entire queue inside Python data structures
(``dict`` + :class:`asyncio.Lock`) and ships with the library by default
- no external services required. It is suitable for:

* Unit tests and integration tests that want a real :class:`QueueBackend`
  without spinning up Postgres.
* Local prototyping / debugging.
* Demos and tutorials.

It is **not** durable - all state is lost when the process exits - and
holds the global lock for the duration of every operation, so it is not
intended for production workloads.
"""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from uuid import UUID, uuid4

from ..contracts import OptimisticLockError, QueueBackend


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
        async with self._lock:
            if idempotency_key:
                for job in self._jobs.values():
                    if job.idempotency_key == idempotency_key:
                        return job.id
            job_id = uuid4()
            now = datetime.now(UTC)
            job = _Job(
                id=job_id,
                created_at=now,
                scheduled_for=scheduled_for or now,
                max_attempts=max_attempts,
                priority=priority,
                kind=kind,
                payload=payload,
                idempotency_key=idempotency_key,
                timeout_s=timeout_s,
            )
            self._jobs[job_id] = job
            return job_id

    async def enqueue_many(self, jobs) -> list[UUID]:
        """Bulk enqueue under a single lock acquisition."""

        specs = list(jobs)
        seen_keys: set[str] = set()
        for idx, spec in enumerate(specs):
            if "kind" not in spec or not spec["kind"]:
                raise ValueError(f"enqueue_many[{idx}]: 'kind' is required")
            key = spec.get("idempotency_key")
            if key is not None:
                if key in seen_keys:
                    raise ValueError(
                        f"enqueue_many: duplicate idempotency_key {key!r} in batch"
                    )
                seen_keys.add(key)

        out: list[UUID] = []
        async with self._lock:
            for spec in specs:
                idem = spec.get("idempotency_key")
                if idem:
                    existing = next(
                        (j for j in self._jobs.values() if j.idempotency_key == idem),
                        None,
                    )
                    if existing is not None:
                        out.append(existing.id)
                        continue
                job_id = uuid4()
                now = datetime.now(UTC)
                job = _Job(
                    id=job_id,
                    created_at=now,
                    scheduled_for=spec.get("scheduled_for") or now,
                    max_attempts=spec.get("max_attempts", 3),
                    priority=spec.get("priority", 100),
                    kind=spec["kind"],
                    payload=spec.get("payload", {}),
                    idempotency_key=idem,
                    timeout_s=spec.get("timeout_s"),
                )
                self._jobs[job_id] = job
                out.append(job_id)
        return out

    async def get(self, job_id: UUID) -> dict:
        async with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(job_id)
            return self._job_to_dict(job)

    async def cancel(self, job_id: UUID) -> None:
        async with self._lock:
            job = self._jobs.get(job_id)
            if job:
                job.cancel_requested = True
                if job.status in {"queued", "running"}:
                    job.status = "cancelled"
                    job.finished_at = datetime.now(UTC)
                    job.result = {"error": "cancelled"}
                    job.lease_until = None
                    job.leased_by = None
                    job.version += 1

    async def is_cancelled(self, job_id: UUID) -> bool:
        async with self._lock:
            job = self._jobs.get(job_id)
            return bool(job and job.cancel_requested)

    async def claim_batch(
        self, worker_id: UUID, *, limit: int = 1
    ) -> List[QueueBackend.ClaimedJob]:
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
            claimed: List[QueueBackend.ClaimedJob] = []
            for job in candidates[:limit]:
                job.lease_until = now + timedelta(seconds=self.lease_ttl_s)
                job.leased_by = worker_id
                job.version += 1
                claimed.append(self._job_to_dict(job))
            return claimed

    async def mark_running(self, job_id: UUID, worker_id: UUID) -> None:
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
    ) -> None:
        async with self._lock:
            job = self._jobs[job_id]
            if (
                expected_version is not None
                and job.version != expected_version
            ):
                raise OptimisticLockError(
                    f"extend_lease failed for {job_id} due to version mismatch"
                )
            if job.leased_by == worker_id:
                job.lease_until = datetime.now(UTC) + timedelta(seconds=ttl_s)

    async def succeed(
        self, job_id: UUID, result: dict, *, expected_version: int | None = None
    ) -> None:
        await self._finish(job_id, "success", result, expected_version=expected_version)

    async def fail(
        self, job_id: UUID, reason: dict, *, expected_version: int | None = None
    ) -> None:
        await self._finish(job_id, "failed", reason, expected_version=expected_version)

    async def timeout(
        self, job_id: UUID, *, expected_version: int | None = None
    ) -> None:
        await self._finish(
            job_id, "timeout", {"error": "timeout"}, expected_version=expected_version
        )

    async def retry(self, job_id: UUID, *, delay: float) -> None:
        async with self._lock:
            job = self._jobs[job_id]
            job.status = "queued"
            job.scheduled_for = datetime.now(UTC) + timedelta(seconds=delay)
            job.lease_until = None
            job.leased_by = None
            job.version += 1

    async def reap_expired(self) -> int:
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

    async def queue_depth(self) -> int:
        async with self._lock:
            return sum(1 for job in self._jobs.values() if job.status == "queued")

    async def check_connection(self) -> None:
        return None

    # ---- Debug / introspection helpers (not part of QueueBackend) ----

    async def count(self, status: str | None = None) -> int:
        """Return the number of stored jobs, optionally filtered by ``status``."""

        async with self._lock:
            if status is None:
                return len(self._jobs)
            return sum(1 for job in self._jobs.values() if job.status == status)

    async def all_jobs(
        self,
        *,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict]:
        """Return a snapshot of stored jobs, optionally filtered.

        ``status`` matches the job's terminal/in-flight state; ``limit``
        caps the returned list. Both filters are applied inside the lock
        so callers avoid materialising the full table.
        """

        async with self._lock:
            out: list[dict] = []
            for job in self._jobs.values():
                if status is not None and job.status != status:
                    continue
                out.append(self._job_to_dict(job))
                if limit is not None and len(out) >= limit:
                    break
            return out

    async def clear(self) -> None:
        """Drop every stored job. Useful between test cases."""

        async with self._lock:
            self._jobs.clear()

    async def purge_finished(
        self,
        *,
        older_than: timedelta | None = None,
        statuses: tuple[str, ...] = ("success", "failed", "timeout", "cancelled"),
    ) -> int:
        """Delete terminal jobs older than ``older_than`` and return the count.

        ``older_than`` is matched against ``finished_at`` (falling back
        to ``created_at`` if a job has no recorded finish). When
        ``older_than`` is ``None`` every job whose status matches
        ``statuses`` is purged.
        """

        async with self._lock:
            cutoff = datetime.now(UTC) - older_than if older_than else None
            to_delete = []
            for jid, job in self._jobs.items():
                if job.status not in statuses:
                    continue
                ref = job.finished_at or job.created_at
                if cutoff is not None and ref > cutoff:
                    continue
                to_delete.append(jid)
            for jid in to_delete:
                del self._jobs[jid]
            return len(to_delete)

    async def _finish(
        self,
        job_id: UUID,
        status: str,
        result: dict,
        *,
        expected_version: int | None,
    ) -> None:
        async with self._lock:
            job = self._jobs[job_id]
            if expected_version is not None and job.version != expected_version:
                raise OptimisticLockError(
                    f"finish failed for {job_id} due to version mismatch"
                )
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
