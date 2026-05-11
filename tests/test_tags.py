"""Tests for job tags + worker filtering (#53)."""

from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from pyjobkit import MemoryBackend
from pyjobkit.contracts import Executor
from pyjobkit.engine import Engine, TAGS_PAYLOAD_KEY
from pyjobkit.worker import Worker


class _Noop(Executor):
    kind = "noop"

    async def run(self, *, job_id: UUID, payload: dict, ctx) -> dict:
        return dict(payload)


def test_enqueue_stores_normalized_tags() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        job_id = await engine.enqueue(
            kind="noop",
            payload={"x": 1},
            tags=["b", "a", " a "],
        )
        rec = await backend.get(job_id)
        assert rec["payload"][TAGS_PAYLOAD_KEY] == ["a", "b"]

    asyncio.run(_run())


def test_enqueue_skips_empty_tag_list() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        job_id = await engine.enqueue(kind="noop", payload={}, tags=[])
        rec = await backend.get(job_id)
        assert TAGS_PAYLOAD_KEY not in rec["payload"]

    asyncio.run(_run())


def test_worker_runs_only_tagged_jobs_and_releases_others() -> None:
    async def _run() -> None:
        backend = MemoryBackend()
        engine = Engine(backend=backend, executors=[_Noop()])
        tagged = await engine.enqueue(
            kind="noop", payload={"m": 1}, tags=["high-priority"]
        )
        untagged = await engine.enqueue(kind="noop", payload={"m": 2})
        wrong_tag = await engine.enqueue(
            kind="noop", payload={"m": 3}, tags=["low-priority"]
        )

        worker = Worker(engine, tags=["high-priority"], poll_interval=0.01)
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        assert (await backend.get(tagged))["status"] == "success"
        # Both non-matching jobs remain queued.
        assert (await backend.get(untagged))["status"] == "queued"
        assert (await backend.get(wrong_tag))["status"] == "queued"

    asyncio.run(_run())


def test_worker_filters_combine_kind_and_tags() -> None:
    async def _run() -> None:
        backend = MemoryBackend()

        class _Other(Executor):
            kind = "other"

            async def run(self, *, job_id, payload, ctx):
                return {}

        engine = Engine(backend=backend, executors=[_Noop(), _Other()])
        match = await engine.enqueue(
            kind="noop", payload={}, tags=["hi"]
        )
        wrong_kind = await engine.enqueue(
            kind="other", payload={}, tags=["hi"]
        )
        wrong_tag = await engine.enqueue(
            kind="noop", payload={}, tags=["lo"]
        )

        worker = Worker(
            engine, kinds=["noop"], tags=["hi"], poll_interval=0.01
        )
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        assert (await backend.get(match))["status"] == "success"
        assert (await backend.get(wrong_kind))["status"] == "queued"
        assert (await backend.get(wrong_tag))["status"] == "queued"

    asyncio.run(_run())


def test_tags_marker_stripped_from_executor_payload() -> None:
    async def _run() -> None:
        backend = MemoryBackend()

        seen_payloads: list[dict] = []

        class _Capture(Executor):
            kind = "cap"

            async def run(self, *, job_id, payload, ctx):
                seen_payloads.append(payload)
                return {}

        engine = Engine(backend=backend, executors=[_Capture()])
        await engine.enqueue(kind="cap", payload={"x": 1}, tags=["hi"])
        worker = Worker(engine, poll_interval=0.01)
        await asyncio.wait_for(worker.run(once=True), timeout=5)

        assert seen_payloads == [{"x": 1}]

    asyncio.run(_run())
