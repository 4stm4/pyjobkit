"""Tests for terminal-state webhook notifications (#56)."""

from __future__ import annotations

import asyncio
import json
from typing import Any
from uuid import UUID

import httpx
import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine
from pyjobkit.webhooks import (
    WEBHOOK_PAYLOAD_KEY,
    fire,
    normalize_webhooks,
)
from pyjobkit.worker import Worker


def test_normalize_webhooks_rejects_unknown_key() -> None:
    with pytest.raises(ValueError):
        normalize_webhooks({"on_other": "https://example.com"})


def test_normalize_webhooks_returns_none_for_empty() -> None:
    assert normalize_webhooks(None) is None
    assert normalize_webhooks({}) is None


def test_normalize_webhooks_strips_and_lowercases() -> None:
    out = normalize_webhooks(
        {"Complete": " https://example.com/ok ", "fail": "https://example.com/bad"}
    )
    assert out == {
        "complete": "https://example.com/ok",
        "fail": "https://example.com/bad",
    }


def test_engine_enqueue_stores_webhook_marker() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[])

        class _Noop(Executor):
            kind = "noop"

            async def run(self, *, job_id, payload, ctx):  # pragma: no cover
                return {}

        engine.register_executor(_Noop())
        job_id = await engine.enqueue(
            kind="noop",
            payload={"x": 1},
            webhooks={"complete": "https://example.com/ok"},
        )
        rec = await backend.get(job_id)
        assert rec["payload"][WEBHOOK_PAYLOAD_KEY] == {
            "complete": "https://example.com/ok"
        }

    asyncio.run(_run())


class _RecordingTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.requests: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        return httpx.Response(200)


def test_fire_sends_post_with_expected_body() -> None:
    async def _run() -> None:
        transport = _RecordingTransport()
        client = httpx.AsyncClient(transport=transport)
        await fire(
            webhooks={"complete": "https://example.com/ok"},
            status="success",
            job_id=UUID("00000000-0000-0000-0000-000000000001"),
            kind="noop",
            attempts=1,
            duration_ms=12.5,
            result={"ok": True},
            client=client,
        )
        await client.aclose()
        assert len(transport.requests) == 1
        req = transport.requests[0]
        body = json.loads(req.content)
        assert body == {
            "job_id": "00000000-0000-0000-0000-000000000001",
            "kind": "noop",
            "status": "success",
            "attempts": 1,
            "duration_ms": 12.5,
            "result": {"ok": True},
        }

    asyncio.run(_run())


def test_fire_silently_logs_on_http_error(caplog) -> None:
    async def _run() -> None:
        class _Bad(httpx.AsyncBaseTransport):
            async def handle_async_request(self, request):
                return httpx.Response(500)

        client = httpx.AsyncClient(transport=_Bad())
        import logging

        with caplog.at_level(logging.WARNING):
            await fire(
                webhooks={"complete": "https://example.com/ok"},
                status="success",
                job_id=UUID("00000000-0000-0000-0000-000000000001"),
                kind="noop",
                attempts=1,
                duration_ms=1.0,
                result=None,
                client=client,
            )
        await client.aclose()
        assert any("webhook" in r.message.lower() for r in caplog.records)

    asyncio.run(_run())


def test_worker_fires_webhook_on_success(monkeypatch) -> None:
    async def _run() -> None:
        backend = MemoryBackend()

        class _Noop(Executor):
            kind = "noop"

            async def run(self, *, job_id, payload, ctx):
                return {"ok": True}

        engine = Engine(backend=backend, executors=[_Noop()])
        worker = Worker(engine, poll_interval=0.01)

        fired: list[dict[str, Any]] = []

        async def fake_fire(**kwargs):
            fired.append(kwargs)

        monkeypatch.setattr("pyjobkit.webhooks.fire", fake_fire)

        await engine.enqueue(
            kind="noop",
            payload={},
            webhooks={"complete": "https://example.com/ok"},
        )
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        assert fired and fired[0]["status"] == "success"
        assert fired[0]["webhooks"] == {"complete": "https://example.com/ok"}

    asyncio.run(_run())
