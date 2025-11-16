"""Tests for the in-memory backend implementation."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from jobkit.backends.memory import MemoryBackend


UTC = timezone.utc


async def _populate_backend() -> tuple[MemoryBackend, list[UUID]]:
    backend = MemoryBackend(lease_ttl_s=1)
    now = datetime.now(UTC)
    job_ids = [
        await backend.enqueue(kind="alpha", payload={"idx": 0}, priority=20, scheduled_for=now),
        await backend.enqueue(kind="alpha", payload={"idx": 1}, priority=10, scheduled_for=now),
        await backend.enqueue(
            kind="alpha",
            payload={"idx": 2},
            priority=5,
            scheduled_for=now + timedelta(seconds=5),
        ),
    ]
    idem = await backend.enqueue(
        kind="alpha",
        payload={"idx": 99},
        idempotency_key="dup",
        scheduled_for=now + timedelta(seconds=10),
    )
    assert await backend.enqueue(kind="alpha", payload={}, idempotency_key="dup") == idem
    return backend, job_ids


def test_memory_backend_full_lifecycle() -> None:
    async def _run() -> None:
        backend, job_ids = await _populate_backend()
        missing_id = uuid4()
        with pytest.raises(KeyError):
            await backend.get(missing_id)

        await backend.cancel(job_ids[0])
        await backend.cancel(missing_id)  # silently ignored
        cancelled = await backend.get(job_ids[0])
        assert cancelled["cancel_requested"] is True

        worker_id = uuid4()
        rows = await backend.claim_batch(worker_id, limit=5)
        assert [row["payload"]["idx"] for row in rows] == [1, 0]
        assert rows[0]["lease_until"] is not None

        await backend.mark_running(rows[0]["id"], worker_id)
        await backend.extend_lease(rows[0]["id"], worker_id, ttl_s=2)
        await backend.succeed(rows[0]["id"], {"ok": True})
        await backend.fail(rows[1]["id"], {"error": "boom"})

        timeout_job = await backend.enqueue(kind="beta", payload={})
        await backend.timeout(timeout_job)
        success = await backend.get(rows[0]["id"])
        failed = await backend.get(rows[1]["id"])
        timed_out = await backend.get(timeout_job)
        assert success["status"] == "success" and success["result"] == {"ok": True}
        assert failed["status"] == "failed" and failed["result"] == {"error": "boom"}
        assert timed_out["status"] == "timeout"

    asyncio.run(_run())
