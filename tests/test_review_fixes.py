"""Regression tests for the high-severity code-review fixes."""

from __future__ import annotations

import asyncio
import logging
from uuid import UUID, uuid4

import httpx
import pytest

from pyjobkit import Engine, MemoryBackend, Worker
from pyjobkit.contracts import Executor
from pyjobkit.engine import (
    INTERNAL_PAYLOAD_KEYS,
    strip_internal_payload,
)
from pyjobkit.executors.subprocess import SubprocessExecutor
from pyjobkit.webhooks import fire


# #1 chain-broken event ------------------------------------------------------


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id, payload, ctx):
        return {"ok": True}


def test_chain_broken_emits_event_and_metric(
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])

        original_enqueue = engine.enqueue

        # Seed a chain head (with one queued tail step) using the real
        # enqueue, then swap in a flaky enqueue so the worker's
        # follow-up call after head success raises.
        head_id = await original_enqueue(
            kind="noop",
            payload={"x": 1, "__pjk_chain": [{"kind": "noop", "payload": {"y": 2}}]},
        )

        async def flaky(**kwargs):
            raise RuntimeError("backend down")

        engine.enqueue = flaky  # type: ignore[method-assign]

        worker = Worker(engine, poll_interval=0.005)
        with caplog.at_level(logging.INFO, logger="pyjobkit.worker"):
            await asyncio.wait_for(worker.run(once=True), timeout=2)

        records = [
            r for r in caplog.records if getattr(r, "event", None) == "chain.broken"
        ]
        assert records, "chain.broken event should be emitted"
        assert records[0].remaining == 1

        from pyjobkit import metrics

        # The counter exposes _value.get() under prometheus_client; for
        # the no-op stub the attribute is missing, so guard.
        value = getattr(metrics.chain_broken_total, "_value", None)
        if value is not None and hasattr(value, "get"):
            assert value.get() >= 1

    asyncio.run(_run())


# #2 subprocess allowlist refuses shell strings ----------------------------


class _NullCtx:
    is_shadow = False

    async def log(self, message, /, *, stream="stdout"):
        pass

    async def is_cancelled(self):
        return False

    async def set_progress(self, value, /, **meta):
        pass


def test_subprocess_allowlist_refuses_shell_string() -> None:
    async def _run() -> None:
        executor = SubprocessExecutor(allowed_commands=["bash"])
        with pytest.raises(PermissionError, match="must be a list"):
            await executor.run(
                job_id=uuid4(),
                payload={"cmd": "bash -c 'echo owned'"},
                ctx=_NullCtx(),  # type: ignore[arg-type]
            )

    asyncio.run(_run())


# #3 webhook retries only on 5xx / network errors ---------------------------


def test_webhook_does_not_retry_on_4xx() -> None:
    async def _run() -> None:
        attempts: list[int] = []

        class _4xxTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                attempts.append(1)
                return httpx.Response(400)

        client = httpx.AsyncClient(transport=_4xxTransport())
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
            client=client,
            max_attempts=5,
            initial_delay_s=0.001,
        )
        await client.aclose()
        assert len(attempts) == 1, "must not retry 4xx"

    asyncio.run(_run())


def test_webhook_retries_on_5xx() -> None:
    async def _run() -> None:
        attempts: list[int] = []

        class _5xxTransport(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                attempts.append(1)
                return httpx.Response(503)

        client = httpx.AsyncClient(transport=_5xxTransport())
        await fire(
            webhooks={"complete": "https://example.com"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            kind="x",
            attempts=1,
            duration_ms=1.0,
            result=None,
            client=client,
            max_attempts=3,
            initial_delay_s=0.001,
        )
        await client.aclose()
        assert len(attempts) == 3, "must retry full max_attempts on 5xx"

    asyncio.run(_run())


# #4 enqueue_at / enqueue_in honour default_max_attempts --------------------


def test_enqueue_at_uses_engine_default_max_attempts() -> None:
    async def _run() -> None:
        from datetime import datetime, timedelta, timezone

        backend = MemoryBackend()
        engine = Engine(
            backend=backend, executors=[_Noop()], default_max_attempts=7
        )
        jid = await engine.enqueue_at(
            datetime.now(timezone.utc) + timedelta(seconds=60),
            kind="noop",
            payload={},
        )
        rec = await backend.get(jid)
        assert rec["max_attempts"] == 7

    asyncio.run(_run())


def test_enqueue_in_uses_engine_default_max_attempts() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(
            backend=backend, executors=[_Noop()], default_max_attempts=9
        )
        jid = await engine.enqueue_in(30, kind="noop", payload={})
        rec = await backend.get(jid)
        assert rec["max_attempts"] == 9

    asyncio.run(_run())


# #5 strip_internal_payload drops marker keys --------------------------------


def test_strip_internal_payload_removes_all_markers() -> None:
    raw = {
        "user_id": 42,
        "__pjk_shadow": True,
        "__pjk_tags": ["x"],
        "__pjk_chain": [{}],
        "__pjk_retry_policy": "fixed:1",
        "__pjk_webhooks": {"complete": "https://x"},
        "__pjk_trace_context": {"traceparent": "..."},
    }
    cleaned = strip_internal_payload(raw)
    assert cleaned == {"user_id": 42}
    # And every marker should be in the canonical set.
    for key in raw:
        if key.startswith("__pjk_"):
            assert key in INTERNAL_PAYLOAD_KEYS


def test_strip_internal_payload_is_safe_for_none() -> None:
    assert strip_internal_payload(None) == {}
    assert strip_internal_payload("not a dict") == {}  # type: ignore[arg-type]
