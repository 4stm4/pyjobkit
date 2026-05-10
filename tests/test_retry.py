"""Tests for ``pyjobkit.retry`` (retry policies, #52)."""

from __future__ import annotations

import asyncio
import random
from uuid import UUID

import pytest

from pyjobkit.backends.memory import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.retry import (
    DEFAULT_RETRY_POLICY,
    ExponentialBackoff,
    FixedDelay,
    JitteredExponentialBackoff,
    RETRY_POLICY_PAYLOAD_KEY,
    RetryPolicy,
    parse_policy,
)
from pyjobkit.worker import Worker


def test_fixed_delay_constant() -> None:
    p = FixedDelay(2.5)
    assert p.delay(1) == 2.5
    assert p.delay(7) == 2.5


def test_fixed_delay_rejects_negative() -> None:
    with pytest.raises(ValueError):
        FixedDelay(-1)


def test_exponential_backoff_grows() -> None:
    p = ExponentialBackoff(base=1.0, factor=2.0)
    assert p.delay(1) == 1.0
    assert p.delay(2) == 2.0
    assert p.delay(3) == 4.0
    assert p.delay(5) == 16.0


def test_exponential_backoff_clamps_at_max() -> None:
    p = ExponentialBackoff(base=1.0, factor=2.0, max_delay_s=10)
    assert p.delay(10) == 10


def test_exponential_backoff_rejects_invalid_factor() -> None:
    with pytest.raises(ValueError):
        ExponentialBackoff(factor=0.5)


def test_jittered_exponential_within_band() -> None:
    rng = random.Random(0)
    p = JitteredExponentialBackoff(base=1.0, factor=2.0, jitter=0.2, rng=rng)
    for attempt in range(1, 6):
        d = p.delay(attempt)
        base = 1.0 * (2.0 ** (attempt - 1))
        assert 0.8 * base <= d <= 1.2 * base


def test_jittered_exponential_jitter_validation() -> None:
    with pytest.raises(ValueError):
        JitteredExponentialBackoff(jitter=1.5)


def test_parse_policy_strings() -> None:
    assert isinstance(parse_policy("fixed"), FixedDelay)
    assert parse_policy("fixed:3").delay(1) == 3.0
    assert isinstance(parse_policy("exponential:1:3"), ExponentialBackoff)
    assert parse_policy("exponential:1:3").delay(2) == 3.0
    p = parse_policy("exponential_jitter:1:2:10:0.1")
    assert isinstance(p, JitteredExponentialBackoff)
    assert p.max_delay_s == 10


def test_parse_policy_passes_through_instance() -> None:
    inst = FixedDelay(1)
    assert parse_policy(inst) is inst


def test_parse_policy_rejects_unknown() -> None:
    with pytest.raises(ValueError):
        parse_policy("squared:2:3")


def test_parse_policy_rejects_empty() -> None:
    with pytest.raises(ValueError):
        parse_policy("")


def test_default_policy_matches_legacy_schedule() -> None:
    # The pre-existing worker used 2 ** (attempts - 1).
    for attempts in range(1, 7):
        assert DEFAULT_RETRY_POLICY.delay(attempts) == 2 ** (attempts - 1)


# Integration: per-job retry policy via Engine.enqueue ------------------------


class _BoomExecutor(Executor):
    kind = "boom"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        raise RuntimeError("boom")


def test_engine_enqueue_stores_per_job_policy() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_BoomExecutor()])
        job_id = await engine.enqueue(
            kind="boom",
            payload={"x": 1},
            max_attempts=5,
            retry_policy="fixed:0.25",
        )
        rec = await backend.get(job_id)
        assert rec["payload"][RETRY_POLICY_PAYLOAD_KEY] == "fixed:0.25"

    asyncio.run(_run())


def test_engine_rejects_invalid_per_job_policy() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_BoomExecutor()])
        with pytest.raises(ValueError):
            await engine.enqueue(
                kind="boom",
                payload={},
                retry_policy="bogus:1",
            )

    asyncio.run(_run())


def test_engine_rejects_instance_retry_policy() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_BoomExecutor()])
        with pytest.raises(TypeError):
            await engine.enqueue(
                kind="boom",
                payload={},
                retry_policy=FixedDelay(1),  # instance is not serializable
            )

    asyncio.run(_run())


def test_worker_uses_per_job_policy_for_retry_delay() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_BoomExecutor()])
        worker = Worker(
            engine, retry_policy=ExponentialBackoff(base=100, factor=100)
        )
        job_id = await engine.enqueue(
            kind="boom",
            payload={},
            max_attempts=5,
            retry_policy="fixed:0.42",
        )
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        await worker._execute_row(dict(claimed[0]))
        rec = await backend.get(job_id)
        # After retry, the job is back to queued; scheduled_for ~ now + 0.42s,
        # which is far smaller than the worker's default (100s).
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        delay = (rec["scheduled_for"] - now).total_seconds()
        assert -0.5 <= delay <= 1.5, delay

    asyncio.run(_run())


def test_worker_constructor_accepts_spec_string() -> None:
    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=[_BoomExecutor()])
    w = Worker(engine, retry_policy="fixed:0.5")
    assert isinstance(w.retry_policy, FixedDelay)
    assert w.retry_policy.delay(1) == 0.5
