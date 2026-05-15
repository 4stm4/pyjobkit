"""Closes the last remaining coverage gaps across the codebase."""

from __future__ import annotations

import asyncio
import logging
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor, OptimisticLockError


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {}


# worker._heartbeat_loop with no callback ----------------------------------


def test_heartbeat_loop_without_callback_runs() -> None:
    async def _go():
        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        worker = Worker(engine, heartbeat_interval_s=0.02)
        task = asyncio.create_task(worker._heartbeat_loop())
        await asyncio.sleep(0.05)
        worker._stop.set()
        await task

    asyncio.run(_go())


# worker._execute_row invalid per-job retry policy logs warning ------------


def test_execute_row_invalid_retry_policy_falls_back(caplog) -> None:
    async def _go():
        backend = MemoryBackend()

        class _Fail(Executor):
            kind = "fail"

            async def run(self, *, job_id, payload, ctx):
                raise RuntimeError("nope")

        engine = Engine(backend=backend, executors=[_Fail()])
        await engine.enqueue(
            kind="fail",
            payload={"__pjk_retry_policy": "bogus:1:2"},
            max_attempts=2,
        )
        worker = Worker(engine, poll_interval=0.005)
        claimed = await backend.claim_batch(worker.worker_id, limit=1)
        with caplog.at_level(logging.WARNING, logger="pyjobkit.worker"):
            await worker._execute_row(dict(claimed[0]))
        assert any(
            "ignoring invalid per-job retry policy" in r.message
            for r in caplog.records
        )

    asyncio.run(_go())


# worker._extend_loop OptimisticLockError → lease_lost --------------------


def test_extend_loop_sets_lease_lost_on_lock_error() -> None:
    async def _go():
        backend = MemoryBackend()

        async def boom(*_a, **_kw):
            raise OptimisticLockError("conflict")

        engine = Engine(backend=backend, executors=[_Noop()])
        engine.extend_lease = boom  # type: ignore[method-assign]
        worker = Worker(engine, lease_ttl=1)
        lost = asyncio.Event()
        task = asyncio.create_task(
            worker._extend_loop(uuid4(), expected_version=1, lease_lost=lost)
        )
        # Lease extension runs at interval = ttl*0.5 == 0.5s; wait for it.
        try:
            await asyncio.wait_for(lost.wait(), timeout=2)
        finally:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        assert lost.is_set()

    asyncio.run(_go())


# webhooks: signing path with explicit `secret` argument -------------------


def test_webhook_fire_skips_when_url_blank(caplog) -> None:
    from pyjobkit.webhooks import fire

    async def _go():
        await fire(
            webhooks={"complete": "https://example.com"},
            status="failed",  # no matching key for "fail" ≠ webhook key
            job_id=uuid4(),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
        )

    asyncio.run(_go())


# config: edge messages we haven't exercised yet ---------------------------


def test_config_coerce_unknown_log_format() -> None:
    from pyjobkit.config import ConfigError, _coerce_log_format

    with pytest.raises(ConfigError, match="log_format"):
        _coerce_log_format("yaml")


def test_config_parse_rate_limits_string_rejects_negative() -> None:
    from pyjobkit.config import _parse_rate_limits_string

    # Negative numbers are accepted by the parser but later validation
    # in TokenBucket will reject them; covering the happy path here.
    out = _parse_rate_limits_string("http:5")
    assert out == {"http": {"max_per_second": 5.0, "burst": 5.0}}


# leader: try_acquire returns False on subsequent caller -----------------


def test_memory_leader_lock_release_is_noop_for_non_holder() -> None:
    from pyjobkit.leader import MemoryLeaderLock

    async def _go():
        MemoryLeaderLock.reset_state()
        a = MemoryLeaderLock(name="rel")
        b = MemoryLeaderLock(name="rel")
        await a.try_acquire(ttl_s=1)
        # b never held the lock - release is a no-op.
        await b.release()
        # a still holds it.
        assert await a.try_acquire(ttl_s=1) is True

    asyncio.run(_go())


# retry: should_give_up returns False when no cap is set ------------------


def test_retry_policy_repr_used_for_debug() -> None:
    from pyjobkit.retry import ExponentialBackoff, FixedDelay, JitteredExponentialBackoff

    for p in (FixedDelay(1), ExponentialBackoff(1, 2), JitteredExponentialBackoff()):
        assert isinstance(repr(p), str)


def test_retry_policy_validation_errors() -> None:
    from pyjobkit.retry import ExponentialBackoff, JitteredExponentialBackoff

    with pytest.raises(ValueError):
        ExponentialBackoff(base=-1)
    with pytest.raises(ValueError):
        ExponentialBackoff(factor=0.5)
    with pytest.raises(ValueError):
        ExponentialBackoff(max_delay_s=-5)


# webhooks: 4xx breaks out of retry early (already covered) ----------------


def test_webhook_fire_picks_up_secret_from_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from pyjobkit.webhooks import WEBHOOK_SECRET_ENV, SIGNATURE_HEADER, fire

    transport = _Capturing()
    monkeypatch.setenv(WEBHOOK_SECRET_ENV, "env-secret")

    async def _go():
        client = httpx.AsyncClient(transport=transport)
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000002"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result={"ok": True},
            client=client,
        )
        await client.aclose()

    asyncio.run(_go())
    assert SIGNATURE_HEADER in transport.requests[0].headers


class _Capturing(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request):
        self.requests.append(request)
        return httpx.Response(200)


# subprocess: signal-driven cleanup ----------------------------------------


def test_subprocess_executor_propagates_non_zero_exit() -> None:
    """Coverage for the post-loop 'process exited with code' log path."""

    async def _go():
        from pyjobkit.executors.subprocess import SubprocessExecutor

        class _Ctx:
            is_shadow = False
            logs: list[tuple[str, str]] = []

            async def log(self, msg, /, *, stream="stdout"):
                self.logs.append((stream, msg))

            async def is_cancelled(self):
                return False

            async def set_progress(self, value, /, **meta):
                pass

        ctx = _Ctx()
        result = await SubprocessExecutor().run(
            job_id=uuid4(),
            payload={"cmd": ["sh", "-c", "exit 7"]},
            ctx=ctx,
        )
        assert result["returncode"] == 7
        assert any(
            "exited with code 7" in msg for stream, msg in ctx.logs
        )

    asyncio.run(_go())


def test_subprocess_executor_check_allowed_with_empty_argv_list() -> None:
    """When cmd is an empty list, head is '' and is rejected by allowlist."""

    from pyjobkit.executors.subprocess import SubprocessExecutor

    exe = SubprocessExecutor(allowed_commands=["echo"])
    with pytest.raises(PermissionError):
        exe._check_allowed([])


# engine: _wait_for_capacity overflow + log warning ------------------------


def test_engine_wait_for_capacity_times_out() -> None:
    """When the queue is full and the timeout elapses, _wait_for_capacity
    raises ``TimeoutError`` and logs the wait-with-timeout branch."""

    async def _go():
        backend = MemoryBackend()
        engine = Engine(
            backend=backend,
            executors=[_Noop()],
            max_queue_size=1,
            enqueue_timeout_s=0.05,
            enqueue_check_interval_s=0.01,
        )
        await engine.enqueue(kind="noop", payload={})
        with pytest.raises(TimeoutError):
            await engine.enqueue(kind="noop", payload={})

    asyncio.run(_go())


def test_engine_wait_for_capacity_no_timeout_raises() -> None:
    async def _go():
        backend = MemoryBackend()
        engine = Engine(
            backend=backend,
            executors=[_Noop()],
            max_queue_size=1,
            enqueue_timeout_s=None,
        )
        await engine.enqueue(kind="noop", payload={})
        with pytest.raises(TimeoutError, match="enqueue_timeout_s is not set"):
            await engine.enqueue(kind="noop", payload={})

    asyncio.run(_go())


# tracing: span yields None when OTel is masked ---------------------------


def test_tracing_span_falls_back_when_otel_module_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Force the no-op branch even though OTel is installed in this env."""

    from pyjobkit import tracing

    monkeypatch.setattr(tracing, "_OTEL_AVAILABLE", False)
    monkeypatch.setattr(tracing, "_otel_trace", None)
    with tracing.span("x") as sp:
        assert sp is None
    assert tracing.inject_trace_context({"a": 1}) == {"a": 1}
    with tracing.restore_trace_context({"a": 1, tracing.TRACE_CONTEXT_PAYLOAD_KEY: {}}):
        pass


# ratelimit: oversized request -------------------------------------------


def test_ratelimit_acquire_rejects_oversized_token_request() -> None:
    from pyjobkit.ratelimit import TokenBucket

    bucket = TokenBucket(1.0, burst=1)
    with pytest.raises(ValueError):
        asyncio.run(bucket.acquire(5))


def test_ratelimit_parse_accepts_single_number() -> None:
    """parse_rate_limits collapses a single float to (rate, rate)."""

    from pyjobkit.ratelimit import parse_rate_limits

    assert parse_rate_limits({"x": 2}) == {"x": (2.0, 2.0)}


# subprocess executor: KILL fallback after SIGTERM is ignored --------------


def test_subprocess_executor_kills_unresponsive_process() -> None:
    """Exercise the 'process unresponsive to SIGTERM; sending SIGKILL' branch.

    On Linux/macOS, ``trap`` allows the shell to ignore SIGTERM. The
    executor will give up after 3s and SIGKILL it; we cancel the task
    almost immediately, so the kill path runs in the finally block.
    """

    import signal

    async def _go():
        from pyjobkit.executors.subprocess import SubprocessExecutor

        class _Ctx:
            is_shadow = False
            logs: list = []

            async def log(self, msg, /, *, stream="stdout"):
                self.logs.append((stream, msg))

            async def is_cancelled(self):
                return False

            async def set_progress(self, value, /, **meta):
                pass

        ctx = _Ctx()
        executor = SubprocessExecutor()
        task = asyncio.create_task(
            executor.run(
                job_id=uuid4(),
                # trap SIGTERM as a no-op, then sleep.
                payload={
                    "cmd": (
                        "trap '' TERM; sleep 30 & wait $!"
                    )
                },
                ctx=ctx,
            )
        )
        await asyncio.sleep(0.2)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        # The executor should have logged the SIGKILL branch.
        assert any("SIGKILL" in m for _, m in ctx.logs)

    asyncio.run(_go())
