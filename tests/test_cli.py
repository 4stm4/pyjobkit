"""Tests for the jobkit CLI entry point."""

from __future__ import annotations

import argparse
import asyncio
import runpy
import sys

from jobkit import cli


def test_cli_main_invokes_async_entrypoint(monkeypatch) -> None:
    ran: list[argparse.Namespace] = []

    async def fake_run(args: argparse.Namespace) -> None:
        ran.append(args)

    monkeypatch.setattr(cli, "_run_worker", fake_run)
    monkeypatch.setattr(sys, "argv", ["jobkit-worker", "--dsn", "sqlite://", "--batch", "2"])
    cli.main()
    assert ran and ran[0].dsn == "sqlite://"


def test_run_worker_builds_components(monkeypatch) -> None:
    async def _run() -> None:
        created: dict[str, object] = {}

        def fake_create_engine(dsn: str):
            created["dsn"] = dsn
            return f"engine:{dsn}"

        class FakeBackend:
            def __init__(self, engine, *, prefer_pg_skip_locked: bool, lease_ttl_s: int) -> None:
                created["backend"] = (engine, prefer_pg_skip_locked, lease_ttl_s)

        class FakeEngine:
            def __init__(self, *, backend, executors):
                created["engine"] = (backend, tuple(type(e).__name__ for e in executors))

        class FakeWorker:
            def __init__(self, eng, *, max_concurrency, batch, poll_interval, lease_ttl):
                created["worker_args"] = (max_concurrency, batch, poll_interval, lease_ttl)
                self.eng = eng

            async def run(self):
                created["worker_run"] = True

        class DummyExecutor:
            def __init__(self):
                created.setdefault("executors", []).append(type(self).__name__)

        monkeypatch.setattr(cli, "create_async_engine", fake_create_engine)
        monkeypatch.setattr(cli, "SQLBackend", FakeBackend)
        monkeypatch.setattr(cli, "Engine", FakeEngine)
        monkeypatch.setattr(cli, "Worker", FakeWorker)
        monkeypatch.setattr(cli, "SubprocessExecutor", DummyExecutor)
        monkeypatch.setattr(cli, "HttpExecutor", DummyExecutor)

        args = argparse.Namespace(
            dsn="sqlite://", concurrency=3, batch=2, poll_interval=0.1, lease_ttl=5, disable_skip_locked=True
        )
        await cli._run_worker(args)
        assert created["dsn"] == "sqlite://"
        assert created["backend"][1] is False  # skip locked disabled
        assert created["worker_run"] is True

    asyncio.run(_run())


def test_cli_main_handles_keyboard_interrupt(monkeypatch) -> None:
    async def fake_run(args):
        raise AssertionError("should not run")

    def fake_asyncio_run(coro):
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_run_worker", fake_run)
    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(sys, "argv", ["jobkit-worker", "--dsn", "sqlite://"])
    cli.main()


def test_cli_module_entrypoint(monkeypatch) -> None:
    created: dict[str, object] = {}

    def fake_create_engine(dsn: str):
        created["dsn"] = dsn
        return f"engine:{dsn}"

    class FakeBackend:
        def __init__(self, engine, *, prefer_pg_skip_locked: bool, lease_ttl_s: int) -> None:
            created["backend"] = (engine, prefer_pg_skip_locked, lease_ttl_s)

    class FakeEngine:
        def __init__(self, *, backend, executors):
            created["engine"] = backend

    class FakeWorker:
        def __init__(self, eng, *, max_concurrency, batch, poll_interval, lease_ttl):
            self.eng = eng

        async def run(self):
            created["worker_run"] = True

    class DummyExecutor:
        def __init__(self):
            created.setdefault("executors", 0)
            created["executors"] += 1

    def fake_asyncio_run(coro):
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(coro)
        finally:
            loop.close()

    monkeypatch.setattr("sqlalchemy.ext.asyncio.create_async_engine", fake_create_engine)
    monkeypatch.setattr("jobkit.backends.sql.backend.SQLBackend", FakeBackend)
    monkeypatch.setattr("jobkit.engine.Engine", FakeEngine)
    monkeypatch.setattr("jobkit.worker.Worker", FakeWorker)
    monkeypatch.setattr("jobkit.executors.subprocess.SubprocessExecutor", DummyExecutor)
    monkeypatch.setattr("jobkit.executors.http.HttpExecutor", DummyExecutor)
    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(sys, "argv", ["jobkit-worker", "--dsn", "sqlite://"])
    existing_cli = sys.modules.pop("jobkit.cli", None)
    try:
        runpy.run_module("jobkit.cli", run_name="__main__")
    finally:
        if existing_cli is not None:
            sys.modules["jobkit.cli"] = existing_cli
    assert created["worker_run"]
