"""Targeted tests that close the remaining coverage gaps across modules.

The cases here are intentionally compact; each closes one or two
otherwise-unreachable branches reported by ``pytest --cov-report=term-missing``.
"""

from __future__ import annotations

import asyncio
import importlib
import logging
import sys
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import httpx
import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor


# backends/__init__.py lazy import paths -----------------------------------


def test_backends_lazy_imports_redis() -> None:
    import pyjobkit.backends as mod

    cls = mod.RedisBackend  # noqa: F841 - exercises __getattr__
    with pytest.raises(AttributeError):
        mod.NotARealBackend


def test_executors_lazy_imports_docker() -> None:
    import pyjobkit.executors as mod

    cls = mod.DockerExecutor  # noqa: F841 - exercises __getattr__
    with pytest.raises(AttributeError):
        mod.NotAnExecutor


# tracing.py edges ----------------------------------------------------------


def test_tracing_strip_returns_payload_unchanged_when_no_marker() -> None:
    from pyjobkit.tracing import strip_trace_context

    assert strip_trace_context({"x": 1}) == {"x": 1}


def test_tracing_restore_yields_when_payload_has_no_marker() -> None:
    """restore_trace_context is a sync contextmanager; entering it without
    a payload marker is a no-op and must not raise."""

    from pyjobkit.tracing import restore_trace_context

    with restore_trace_context({"plain": True}):
        pass


def test_tracing_inject_returns_payload_unchanged_outside_span() -> None:
    from pyjobkit.tracing import inject_trace_context

    # Outside any active OTel span the inject helper returns the
    # caller's payload as-is.
    assert inject_trace_context({"x": 1}) == {"x": 1}


# ratelimit.py edges --------------------------------------------------------


def test_ratelimit_acquire_zero_tokens_is_noop() -> None:
    from pyjobkit.ratelimit import TokenBucket

    async def _go():
        bucket = TokenBucket(1.0, burst=1)
        await bucket.acquire(0)
        await bucket.acquire(-1)  # negative also returns immediately

    asyncio.run(_go())


def test_parse_rate_limits_accepts_string_form() -> None:
    from pyjobkit.ratelimit import parse_rate_limits

    out = parse_rate_limits({"http": [5, 10]})
    assert out == {"http": (5.0, 10.0)}


# retry.py edges ------------------------------------------------------------


def test_parse_policy_rejects_non_numeric_keyword() -> None:
    from pyjobkit.retry import parse_policy

    with pytest.raises(ValueError):
        parse_policy("fixed:age=abc")


def test_parse_policy_passthrough_for_instance() -> None:
    from pyjobkit.retry import FixedDelay, parse_policy

    inst = FixedDelay(1)
    assert parse_policy(inst) is inst


def test_retry_policy_repr_shows_name() -> None:
    from pyjobkit.retry import FixedDelay

    assert "FixedDelay" in repr(FixedDelay(1))


# memory backend missing lines ---------------------------------------------


def test_memory_backend_cancel_unknown_is_noop_and_get_raises_keyerror() -> None:
    async def _go():
        backend = MemoryBackend()
        await backend.cancel(uuid4())  # no-op, must not raise
        with pytest.raises(KeyError):
            await backend.get(uuid4())

    asyncio.run(_go())


def test_memory_backend_extend_lease_silent_on_unknown_worker() -> None:
    async def _go():
        backend = MemoryBackend()
        jid = await backend.enqueue(kind="x", payload={})
        # No claim was made, so leased_by is None; extend_lease must
        # silently no-op rather than raising.
        await backend.extend_lease(jid, uuid4(), ttl_s=5)

    asyncio.run(_go())


# scheduler edges ----------------------------------------------------------


def test_scheduler_remove_and_entries_listing() -> None:
    backend = MemoryBackend()

    class _Noop(Executor):
        kind = "noop"

        async def run(self, *, job_id, payload, ctx):
            return {}

    from pyjobkit.scheduler import Scheduler

    sched = Scheduler(Engine(backend=backend, executors=[_Noop()]))
    sched.every("5s", name="a", kind="noop")
    sched.every("5s", name="b", kind="noop")
    assert sorted(sched.entries()) == ["a", "b"]
    sched.remove("a")
    assert sched.entries() == ["b"]


# logging/structured edges -------------------------------------------------


def test_json_formatter_serialises_dataclasses_via_repr() -> None:
    from dataclasses import dataclass
    import json

    from pyjobkit.logging import JsonFormatter

    @dataclass
    class _Thing:
        x: int

    record = logging.LogRecord(
        name="t", level=logging.INFO, pathname=__file__, lineno=1,
        msg="hi", args=(), exc_info=None,
    )
    record.thing = _Thing(7)
    out = json.loads(JsonFormatter().format(record))
    # Dataclass falls through to repr; payload should be a string blob.
    assert "x=7" in out["thing"]


# leader edges -------------------------------------------------------------


def test_leader_loop_rejects_bad_cancel_grace() -> None:
    from pyjobkit.leader import MemoryLeaderLock, leader_loop

    async def _go():
        MemoryLeaderLock.reset_state()
        lock = MemoryLeaderLock(name="bad")
        with pytest.raises(ValueError):
            await leader_loop(
                lock, ttl_s=1, on_leader=lambda: asyncio.sleep(0), cancel_grace_s=0
            )

    asyncio.run(_go())


def test_leader_loop_swaps_dead_on_leader_with_fresh() -> None:
    from pyjobkit.leader import MemoryLeaderLock, leader_loop

    async def _go():
        MemoryLeaderLock.reset_state()
        lock = MemoryLeaderLock(name="swap")
        calls = {"n": 0}

        async def runs_once_then_raises():
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("first instance died")
            await asyncio.sleep(0.05)

        stop = asyncio.Event()

        async def stopper():
            await asyncio.sleep(0.2)
            stop.set()

        await asyncio.gather(
            leader_loop(
                lock,
                ttl_s=0.5,
                renew_every_s=0.02,
                on_leader=runs_once_then_raises,
                stop_event=stop,
                cancel_grace_s=0.1,
            ),
            stopper(),
        )
        assert calls["n"] >= 2  # at least one restart

    asyncio.run(_go())


# webhooks edges -----------------------------------------------------------


def test_normalize_webhooks_rejects_bad_key() -> None:
    from pyjobkit.webhooks import normalize_webhooks

    with pytest.raises(ValueError):
        normalize_webhooks({"bogus": "https://x"})
    with pytest.raises(ValueError):
        normalize_webhooks({"complete": ""})


def test_normalize_webhooks_empty_returns_none() -> None:
    from pyjobkit.webhooks import normalize_webhooks

    assert normalize_webhooks(None) is None
    assert normalize_webhooks({}) is None


def test_fire_returns_early_when_no_webhooks() -> None:
    from pyjobkit.webhooks import fire

    async def _go():
        await fire(
            webhooks=None,
            status="success",
            job_id=uuid4(),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
        )
        # status with no matching webhook key is also a no-op.
        await fire(
            webhooks={"complete": "https://x"},
            status="success",
            job_id=uuid4(),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
            client=httpx.AsyncClient(transport=_DummyOk()),
        )

    asyncio.run(_go())


class _DummyOk(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request):
        return httpx.Response(200)


def test_fire_skips_unsupported_status() -> None:
    """Statuses outside complete / fail / timeout silently no-op."""

    from pyjobkit.webhooks import fire

    async def _go():
        await fire(
            webhooks={"complete": "https://x"},
            status="some-other-thing",
            job_id=uuid4(),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
        )

    asyncio.run(_go())


# config edges -------------------------------------------------------------


def test_config_find_file_returns_path_when_present(tmp_path) -> None:
    from pyjobkit.config import find_config_file

    target = tmp_path / ".pyjobkit.toml"
    target.write_text("[pyjobkit]\n")
    assert find_config_file(tmp_path) == target


def test_config_load_with_explicit_missing_path_raises(tmp_path) -> None:
    from pyjobkit.config import ConfigError, load_config

    with pytest.raises(ConfigError):
        load_config(config_path=tmp_path / "missing.toml", env={})


# tracing.py span helper when otel is installed ----------------------------


def test_tracing_span_emits_attributes_when_otel_present() -> None:
    """Walk the attribute-set branches; OTel is installed in this env."""

    from pyjobkit.tracing import span

    with span("smoke", kind="x", attempts=1, none_value=None):
        pass


# memory backend reap path (with non-running jobs) -------------------------


def test_memory_reap_skips_unleased_jobs() -> None:
    async def _go():
        backend = MemoryBackend()
        await backend.enqueue(kind="x", payload={})  # never leased
        reaped = await backend.reap_expired()
        assert reaped == 0

    asyncio.run(_go())
