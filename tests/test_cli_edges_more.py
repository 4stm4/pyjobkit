"""Additional CLI coverage: argparse validators, plugin path, signal logs."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

import pytest

from pyjobkit import cli


def test_positive_int_validator_rejects_non_positive() -> None:
    validate = cli._positive_int("concurrency")
    with pytest.raises(argparse.ArgumentTypeError):
        validate("0")
    with pytest.raises(argparse.ArgumentTypeError):
        validate("-5")
    assert validate("3") == 3


def test_positive_float_validator_rejects_non_positive() -> None:
    validate = cli._positive_float("poll-interval")
    with pytest.raises(argparse.ArgumentTypeError):
        validate("0")
    with pytest.raises(argparse.ArgumentTypeError):
        validate("-0.5")
    assert validate("0.5") == 0.5


def test_load_executor_unknown_module() -> None:
    with pytest.raises(cli.CLIError, match="Cannot import"):
        cli._load_executor("nonexistent.module.path:fn")


def test_resolve_config_wraps_config_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """If load_config raises ConfigError, the wrapper re-raises CLIError."""

    def bad_loader(**_kw):
        raise cli.ConfigError("invalid")

    monkeypatch.setattr(cli, "load_config", bad_loader)
    args = argparse.Namespace(
        config=None, dsn="x", concurrency=None, batch=None, poll_interval=None,
        lease_ttl=None, max_attempts=None, default_executor=None,
        disable_skip_locked=False, enable_plugins=False, executor=None,
        log_level=None, log_format=None, retry_policy=None,
        watchdog_interval=None, rate_limit=None,
    )
    with pytest.raises(cli.CLIError):
        cli._resolve_config(args)


def test_run_worker_logs_loaded_plugins(monkeypatch: pytest.MonkeyPatch, caplog) -> None:
    """When enable_plugins is set, the success log line fires when at
    least one plugin is registered."""

    async def _go() -> None:
        from pyjobkit import Engine, MemoryBackend
        from pyjobkit.contracts import Executor

        class _PluginExec(Executor):
            kind = "plugin"

            async def run(self, *, job_id, payload, ctx):
                return {}

        # Fake DB engine + backend so we don't touch sqlalchemy.
        class _FakeBackend:
            def __init__(self, *_a, **_kw):
                pass

        class _FakeAsync:
            async def dispose(self): pass

        def fake_create(_dsn):
            return _FakeAsync()

        class _FakeEngine(Engine):
            def __init__(self, **kw):
                super().__init__(backend=MemoryBackend(), executors=kw["executors"])

            def register_plugins(self):
                return [_PluginExec()]

        class _FakeWorker:
            def __init__(self, *_a, **_kw): pass
            async def run(self, *, once=False): pass
            def request_stop(self): pass
            async def wait_stopped(self): pass

        monkeypatch.setattr(
            cli,
            "load_config",
            lambda **_kw: cli.Config(dsn="sqlite://", enable_plugins=True),
        )
        monkeypatch.setattr(cli, "create_async_engine", fake_create)
        monkeypatch.setattr(cli, "SQLBackend", _FakeBackend)
        monkeypatch.setattr(cli, "Engine", _FakeEngine)
        monkeypatch.setattr(cli, "Worker", _FakeWorker)
        monkeypatch.setattr(cli, "_configure_logging", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli, "_install_signal_handlers", lambda *_: None)

        args = argparse.Namespace(
            config=None, dsn=None, concurrency=None, batch=None,
            poll_interval=None, lease_ttl=None, max_attempts=None,
            default_executor=None, disable_skip_locked=False,
            enable_plugins=True, executor=None, log_level=None,
            log_format=None, retry_policy=None, watchdog_interval=None,
            rate_limit=None, kind=None, once=False, metrics_port=None,
            metrics_host="0.0.0.0",
        )
        with caplog.at_level(logging.INFO, logger="pyjobkit.cli"):
            await cli._run_worker(args)
        assert any("Loaded 1 executor plugin" in r.message for r in caplog.records)

    asyncio.run(_go())


def test_run_worker_with_metrics_port_starts_server(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The metrics_port branch wires the Prometheus exporter."""

    called: list = []

    def fake_start(port, host):
        called.append((port, host))

    async def _go():
        class _FakeBackend:
            def __init__(self, *_a, **_kw): pass

        class _FakeAsync:
            async def dispose(self): pass

        class _FakeEngine:
            def __init__(self, **_kw): pass
            def register_plugins(self): return []

        class _FakeWorker:
            def __init__(self, *_a, **_kw): pass
            async def run(self, *, once=False): pass
            def request_stop(self): pass
            async def wait_stopped(self): pass

        monkeypatch.setattr(cli, "start_metrics_server", fake_start)
        monkeypatch.setattr(
            cli, "load_config", lambda **_kw: cli.Config(dsn="sqlite://")
        )
        monkeypatch.setattr(cli, "create_async_engine", lambda _dsn: _FakeAsync())
        monkeypatch.setattr(cli, "SQLBackend", _FakeBackend)
        monkeypatch.setattr(cli, "Engine", _FakeEngine)
        monkeypatch.setattr(cli, "Worker", _FakeWorker)
        monkeypatch.setattr(cli, "_configure_logging", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli, "_install_signal_handlers", lambda *_: None)

        args = argparse.Namespace(
            config=None, dsn=None, concurrency=None, batch=None,
            poll_interval=None, lease_ttl=None, max_attempts=None,
            default_executor=None, disable_skip_locked=False,
            enable_plugins=False, executor=None, log_level=None,
            log_format=None, retry_policy=None, watchdog_interval=None,
            rate_limit=None, kind=None, once=False,
            metrics_port=9100, metrics_host="127.0.0.1",
        )
        await cli._run_worker(args)

    asyncio.run(_go())
    assert called == [(9100, "127.0.0.1")]


def test_run_worker_keyboard_interrupt_propagates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _go():
        class _FakeBackend:
            def __init__(self, *_a, **_kw): pass

        class _FakeAsync:
            async def dispose(self): pass

        class _FakeEngine:
            def __init__(self, **_kw): pass
            def register_plugins(self): return []

        class _FakeWorker:
            def __init__(self, *_a, **_kw): pass

            async def run(self, *, once=False):
                raise KeyboardInterrupt

            def request_stop(self): pass

            async def wait_stopped(self): pass

        monkeypatch.setattr(
            cli, "load_config", lambda **_kw: cli.Config(dsn="sqlite://")
        )
        monkeypatch.setattr(cli, "create_async_engine", lambda _dsn: _FakeAsync())
        monkeypatch.setattr(cli, "SQLBackend", _FakeBackend)
        monkeypatch.setattr(cli, "Engine", _FakeEngine)
        monkeypatch.setattr(cli, "Worker", _FakeWorker)
        monkeypatch.setattr(cli, "_configure_logging", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli, "_install_signal_handlers", lambda *_: None)

        args = argparse.Namespace(
            config=None, dsn=None, concurrency=None, batch=None,
            poll_interval=None, lease_ttl=None, max_attempts=None,
            default_executor=None, disable_skip_locked=False,
            enable_plugins=False, executor=None, log_level=None,
            log_format=None, retry_policy=None, watchdog_interval=None,
            rate_limit=None, kind=None, once=False, metrics_port=None,
            metrics_host="0.0.0.0",
        )
        with pytest.raises(KeyboardInterrupt):
            await cli._run_worker(args)

    asyncio.run(_go())


def test_run_worker_unknown_exception_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _go():
        class _FakeBackend:
            def __init__(self, *_a, **_kw): pass

        class _FakeAsync:
            async def dispose(self): pass

        class _FakeEngine:
            def __init__(self, **_kw): pass
            def register_plugins(self): return []

        class _FakeWorker:
            def __init__(self, *_a, **_kw): pass

            async def run(self, *, once=False):
                raise RuntimeError("crashed")

            def request_stop(self): pass

            async def wait_stopped(self): pass

        monkeypatch.setattr(
            cli, "load_config", lambda **_kw: cli.Config(dsn="sqlite://")
        )
        monkeypatch.setattr(cli, "create_async_engine", lambda _dsn: _FakeAsync())
        monkeypatch.setattr(cli, "SQLBackend", _FakeBackend)
        monkeypatch.setattr(cli, "Engine", _FakeEngine)
        monkeypatch.setattr(cli, "Worker", _FakeWorker)
        monkeypatch.setattr(cli, "_configure_logging", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli, "_install_signal_handlers", lambda *_: None)

        args = argparse.Namespace(
            config=None, dsn=None, concurrency=None, batch=None,
            poll_interval=None, lease_ttl=None, max_attempts=None,
            default_executor=None, disable_skip_locked=False,
            enable_plugins=False, executor=None, log_level=None,
            log_format=None, retry_policy=None, watchdog_interval=None,
            rate_limit=None, kind=None, once=False, metrics_port=None,
            metrics_host="0.0.0.0",
        )
        with pytest.raises(cli.CLIError, match="terminated with an unexpected"):
            await cli._run_worker(args)

    asyncio.run(_go())
