"""Tests for the Pyjobkit CLI entry point."""

from __future__ import annotations

import argparse
import asyncio
import runpy
import sys
import types

import pytest

from pyjobkit import cli


def test_cli_main_invokes_async_entrypoint(monkeypatch) -> None:
    ran: list[argparse.Namespace] = []

    async def fake_run(args: argparse.Namespace) -> None:
        ran.append(args)

    monkeypatch.setattr(cli, "_run_worker", fake_run)
    monkeypatch.setattr(sys, "argv", ["pyjobkit", "--dsn", "sqlite://", "--batch", "2"])
    cli.main()
    assert ran and ran[0].dsn == "sqlite://"


def test_run_worker_builds_components(monkeypatch) -> None:
    async def _run() -> None:
        created: dict[str, object] = {}

        class FakeAsyncEngine:
            def __init__(self, dsn: str):
                self.dsn = dsn

            async def dispose(self) -> None:
                created["disposed"] = True

        def fake_create_engine(dsn: str):
            created["dsn"] = dsn
            return FakeAsyncEngine(dsn)

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

            def request_stop(self) -> None:
                created["request_stop_called"] = True

            async def wait_stopped(self) -> None:
                created["wait_stopped"] = True

        class DummyExecutor:
            def __init__(self):
                created.setdefault("executors", []).append(type(self).__name__)

        monkeypatch.setattr(cli.logging, "basicConfig", lambda **_: None)
        monkeypatch.setattr(cli, "create_async_engine", fake_create_engine)
        monkeypatch.setattr(cli, "SQLBackend", FakeBackend)
        monkeypatch.setattr(cli, "Engine", FakeEngine)
        monkeypatch.setattr(cli, "Worker", FakeWorker)
        monkeypatch.setattr(cli, "SubprocessExecutor", DummyExecutor)
        monkeypatch.setattr(cli, "HttpExecutor", DummyExecutor)

        args = argparse.Namespace(
            dsn="sqlite://",
            concurrency=3,
            batch=2,
            poll_interval=0.1,
            lease_ttl=5,
            disable_skip_locked=True,
            executor=None,
            log_level="INFO",
        )
        await cli._run_worker(args)
        assert created["dsn"] == "sqlite://"
        assert created["backend"][1] is False  # skip locked disabled
        assert created["worker_run"] is True
        assert created["disposed"] is True

    asyncio.run(_run())


def test_cli_main_handles_keyboard_interrupt(monkeypatch, capsys) -> None:
    async def fake_run(args):
        raise AssertionError("should not run")

    def fake_asyncio_run(coro):
        coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "_run_worker", fake_run)
    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(sys, "argv", ["pyjobkit", "--dsn", "sqlite://"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 130
    captured = capsys.readouterr()
    assert "requesting worker shutdown" in captured.err


def test_cli_module_entrypoint(monkeypatch) -> None:
    created: dict[str, object] = {}

    class FakeAsyncEngine:
        def __init__(self, dsn: str):
            self.dsn = dsn

        async def dispose(self) -> None:
            created["disposed"] = True

    def fake_create_engine(dsn: str):
        created["dsn"] = dsn
        return FakeAsyncEngine(dsn)

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

        def request_stop(self) -> None:
            pass

        async def wait_stopped(self) -> None:
            pass

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
    monkeypatch.setattr("pyjobkit.backends.sql.backend.SQLBackend", FakeBackend)
    monkeypatch.setattr("pyjobkit.engine.Engine", FakeEngine)
    monkeypatch.setattr("pyjobkit.worker.Worker", FakeWorker)
    monkeypatch.setattr("pyjobkit.executors.subprocess.SubprocessExecutor", DummyExecutor)
    monkeypatch.setattr("pyjobkit.executors.http.HttpExecutor", DummyExecutor)
    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(sys, "argv", ["pyjobkit", "--dsn", "sqlite://"])
    existing_cli = sys.modules.pop("pyjobkit.cli", None)
    try:
        runpy.run_module("pyjobkit.cli", run_name="__main__")
    finally:
        if existing_cli is not None:
            sys.modules["pyjobkit.cli"] = existing_cli
    assert created["worker_run"]


def test_run_worker_requests_stop_on_cancel(monkeypatch) -> None:
    async def _run() -> None:
        events: list[str] = []

        class FakeEngine:
            def __init__(self, dsn: str):
                self.dsn = dsn

            async def dispose(self) -> None:
                events.append("disposed")

        def fake_create_engine(dsn: str):
            return FakeEngine(dsn)

        class FakeBackend:
            def __init__(self, engine, *, prefer_pg_skip_locked: bool, lease_ttl_s: int) -> None:
                events.append("backend")

        class FakeEngineWrapper:
            def __init__(self, *, backend, executors):
                events.append("engine")

        class FakeWorker:
            def __init__(self, *_args, **_kwargs):
                pass

            async def run(self):
                events.append("run")
                raise asyncio.CancelledError

            def request_stop(self) -> None:
                events.append("request_stop")

            async def wait_stopped(self) -> None:
                events.append("wait_stopped")

        class DummyExecutor:
            pass

        monkeypatch.setattr(cli.logging, "basicConfig", lambda **_: None)
        monkeypatch.setattr(cli, "create_async_engine", fake_create_engine)
        monkeypatch.setattr(cli, "SQLBackend", FakeBackend)
        monkeypatch.setattr(cli, "Engine", FakeEngineWrapper)
        monkeypatch.setattr(cli, "Worker", FakeWorker)
        monkeypatch.setattr(cli, "SubprocessExecutor", DummyExecutor)
        monkeypatch.setattr(cli, "HttpExecutor", DummyExecutor)

        args = argparse.Namespace(
            dsn="sqlite://",
            concurrency=1,
            batch=1,
            poll_interval=0.1,
            lease_ttl=5,
            disable_skip_locked=False,
            executor=None,
            log_level="INFO",
        )
        with pytest.raises(asyncio.CancelledError):
            await cli._run_worker(args)
        assert events == [
            "backend",
            "engine",
            "run",
            "request_stop",
            "wait_stopped",
            "disposed",
        ]

    asyncio.run(_run())


def test_main_reports_cli_error(monkeypatch, capsys) -> None:
    def fake_asyncio_run(coro):
        coro.close()
        raise cli.CLIError("boom")

    monkeypatch.setattr(cli.asyncio, "run", fake_asyncio_run)
    monkeypatch.setattr(sys, "argv", ["pyjobkit", "--dsn", "sqlite://"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    assert "boom" in capsys.readouterr().err


def test_load_executor_errors(monkeypatch) -> None:
    with pytest.raises(cli.CLIError):
        cli._load_executor("missing")

    fake_module = types.ModuleType("tests.fake_executor")

    class DummyExecutor:
        pass

    fake_module.Factory = lambda: DummyExecutor()
    monkeypatch.setitem(sys.modules, fake_module.__name__, fake_module)
    executor = cli._load_executor("tests.fake_executor:Factory")
    assert isinstance(executor, DummyExecutor)
