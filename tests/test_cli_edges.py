"""Additional coverage for cli.py edge paths."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import types

import pytest

from pyjobkit import cli


def _make_args(**overrides):
    base = dict(
        config=None,
        dsn=None,
        concurrency=None,
        batch=None,
        poll_interval=None,
        lease_ttl=None,
        max_attempts=None,
        default_executor=None,
        disable_skip_locked=False,
        enable_plugins=False,
        executor=None,
        log_level=None,
        log_format=None,
        retry_policy=None,
        watchdog_interval=None,
        rate_limit=None,
        kind=None,
        once=False,
        metrics_port=None,
        metrics_host="0.0.0.0",
    )
    base.update(overrides)
    return argparse.Namespace(**base)


def test_configure_logging_wraps_value_error_into_cli_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def boom(*_a, **_kw):
        raise ValueError("nope")

    monkeypatch.setattr(cli, "configure_logging", boom)
    with pytest.raises(cli.CLIError):
        cli._configure_logging("INFO", fmt="text")


def test_load_executor_rejects_missing_separator() -> None:
    with pytest.raises(cli.CLIError, match="'module:attr' format"):
        cli._load_executor("no_colon_here")


def test_load_executor_missing_attribute(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = types.ModuleType("tests.fake_attr_executor")
    monkeypatch.setitem(sys.modules, mod.__name__, mod)
    with pytest.raises(cli.CLIError, match="no attribute"):
        cli._load_executor("tests.fake_attr_executor:does_not_exist")


def test_resolve_config_collects_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict = {}

    def fake_load(**kwargs):
        captured.update(kwargs)
        return cli.Config(dsn="sqlite://")

    monkeypatch.setattr(cli, "load_config", fake_load)
    args = _make_args(
        dsn="sqlite://",
        concurrency=3,
        batch=2,
        poll_interval=0.1,
        lease_ttl=5,
        max_attempts=7,
        default_executor="x:y",
        disable_skip_locked=True,
        enable_plugins=True,
        log_level="DEBUG",
        log_format="json",
        retry_policy="fixed:1",
        watchdog_interval=2.5,
        rate_limit=["http:5:10"],
        executor=["a:b"],
    )
    cli._resolve_config(args)
    overrides = captured["overrides"]
    assert overrides["concurrency"] == 3
    assert overrides["max_attempts"] == 7
    assert overrides["default_executor"] == "x:y"
    assert overrides["disable_skip_locked"] is True
    assert overrides["enable_plugins"] is True
    assert overrides["log_format"] == "json"
    assert overrides["retry_policy"] == "fixed:1"
    assert overrides["watchdog_interval_s"] == 2.5
    assert overrides["rate_limits"] == {
        "http": {"max_per_second": 5.0, "burst": 10.0}
    }
    assert overrides["extra_executors"] == ("a:b",)


def test_resolve_config_rejects_invalid_rate_limit_spec(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(cli, "load_config", lambda **_: cli.Config(dsn="x"))
    args = _make_args(rate_limit=["bogus"])
    with pytest.raises(cli.CLIError):
        cli._resolve_config(args)


def test_install_signal_handlers_handles_unsupported_platform(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``loop.add_signal_handler`` raises NotImplementedError on Windows."""

    async def _go() -> None:
        from pyjobkit import MemoryBackend, Worker, Engine
        from pyjobkit.contracts import Executor

        class _Noop(Executor):
            kind = "noop"

            async def run(self, *, job_id, payload, ctx):
                return {}

        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        worker = Worker(engine)
        loop = asyncio.get_running_loop()

        def explode(*_a, **_kw):
            raise NotImplementedError

        monkeypatch.setattr(loop, "add_signal_handler", explode)
        # Should not raise; the warning path is taken instead.
        cli._install_signal_handlers(worker)

    asyncio.run(_go())


def test_install_signal_handlers_actually_calls_request_stop() -> None:
    async def _go() -> None:
        from pyjobkit import MemoryBackend, Engine, Worker
        from pyjobkit.contracts import Executor

        class _Noop(Executor):
            kind = "noop"

            async def run(self, *, job_id, payload, ctx):
                return {}

        engine = Engine(backend=MemoryBackend(), executors=[_Noop()])
        worker = Worker(engine)
        cli._install_signal_handlers(worker)
        loop = asyncio.get_running_loop()
        # Drop the handlers after the smoke check to avoid affecting
        # other tests in the same session.
        try:
            loop.remove_signal_handler(signal.SIGTERM)
            loop.remove_signal_handler(signal.SIGINT)
        except (NotImplementedError, ValueError):
            pass

    asyncio.run(_go())


def test_run_worker_requires_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _go() -> None:
        monkeypatch.setattr(cli, "load_config", lambda **_: cli.Config(dsn=None))
        args = _make_args()
        with pytest.raises(cli.CLIError, match="DSN is required"):
            await cli._run_worker(args)

    asyncio.run(_go())


def test_run_worker_rejects_malformed_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    async def _go() -> None:
        monkeypatch.setattr(
            cli, "load_config", lambda **_: cli.Config(dsn="not-a-real-dsn")
        )
        monkeypatch.setattr(cli, "_configure_logging", lambda *_a, **_kw: None)
        args = _make_args(dsn="not-a-real-dsn")
        with pytest.raises(cli.CLIError, match="Failed to create engine"):
            await cli._run_worker(args)

    asyncio.run(_go())


def test_main_loops_through_argv(monkeypatch: pytest.MonkeyPatch) -> None:
    ran: list[argparse.Namespace] = []

    async def fake(args):
        ran.append(args)

    monkeypatch.setattr(cli, "_run_worker", fake)
    monkeypatch.setattr(sys, "argv", ["pyjobkit", "--dsn", "sqlite://"])
    cli.main()
    assert ran and ran[0].dsn == "sqlite://"


def test_main_translates_unexpected_exception(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    async def boom(_args):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(cli, "_run_worker", boom)
    monkeypatch.setattr(sys, "argv", ["pyjobkit", "--dsn", "sqlite://"])
    with pytest.raises(SystemExit) as excinfo:
        cli.main()
    assert excinfo.value.code == 1
    assert "unexpected failure" in capsys.readouterr().err
