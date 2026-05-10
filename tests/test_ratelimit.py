"""Tests for the per-kind rate limiter (#73)."""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.ratelimit import TokenBucket, parse_rate_limits
from pyjobkit.worker import Worker


def test_token_bucket_rejects_invalid_inputs() -> None:
    with pytest.raises(ValueError):
        TokenBucket(0)
    with pytest.raises(ValueError):
        TokenBucket(1, burst=0)


def test_token_bucket_allows_burst_then_throttles() -> None:
    async def _run() -> None:
        bucket = TokenBucket(max_per_second=10.0, burst=2)
        t0 = time.perf_counter()
        # First two acquires consume the initial burst.
        await bucket.acquire()
        await bucket.acquire()
        # Third acquire must wait for refill (~100ms at rate=10).
        await bucket.acquire()
        elapsed = time.perf_counter() - t0
        assert 0.08 <= elapsed <= 0.5, elapsed

    asyncio.run(_run())


def test_token_bucket_rejects_oversized_request() -> None:
    bucket = TokenBucket(1.0, burst=1)
    with pytest.raises(ValueError):
        asyncio.run(bucket.acquire(5))


def test_parse_rate_limits_accepts_multiple_shapes() -> None:
    result = parse_rate_limits(
        {
            "http": {"max_per_second": 5, "burst": 10},
            "email": 2,
            "sms": (3, 4),
        }
    )
    assert result == {
        "http": (5.0, 10.0),
        "email": (2.0, 2.0),
        "sms": (3.0, 4.0),
    }


def test_parse_rate_limits_rejects_missing_rate() -> None:
    with pytest.raises(ValueError):
        parse_rate_limits({"http": {"burst": 10}})


class _Noop(Executor):
    kind = "limited"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return {}


def test_worker_builds_buckets_from_rate_limits() -> None:
    engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
    worker = Worker(
        engine,
        rate_limits={"limited": {"max_per_second": 5, "burst": 10}},
    )
    bucket = worker._buckets["limited"]
    assert bucket.rate == 5.0
    assert bucket.capacity == 10.0


def test_worker_throttles_execute_row_after_burst_exhausted() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        worker = Worker(
            engine,
            rate_limits={"limited": {"max_per_second": 10, "burst": 1}},
        )
        # Drain the initial burst token directly.
        await worker._buckets["limited"].acquire()

        job_id = await engine.enqueue(kind="limited", payload={})
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        t0 = time.perf_counter()
        await worker._execute_row(dict(claimed[0]))
        elapsed = time.perf_counter() - t0
        # Refill is 10/s, so we should wait ~100 ms.
        assert 0.08 <= elapsed <= 0.5, elapsed

        rec = await backend.get(job_id)
        assert rec["status"] == "success"

    asyncio.run(_run())
