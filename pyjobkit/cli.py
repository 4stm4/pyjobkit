"""Entry-point for the ``pyjobkit`` console script."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
from contextlib import suppress
import sys
from typing import Callable

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from .backends.sql import SQLBackend
from .config import Config, ConfigError, LOG_FORMATS, LOG_LEVELS, load_config
from .engine import Engine
from .executors import HttpExecutor, SubprocessExecutor
from .logging import configure_logging
from .metrics import start_metrics_server
from .worker import Worker


logger = logging.getLogger(__name__)


class CLIError(RuntimeError):
    """Raised when the CLI fails to start or configure the worker."""


def _positive_int(name: str) -> Callable[[str], int]:
    def _validate(value: str) -> int:
        try:
            converted = int(value)
        except ValueError as exc:  # pragma: no cover - argparse already reports
            raise argparse.ArgumentTypeError(f"{name} must be an integer") from exc
        if converted <= 0:
            raise argparse.ArgumentTypeError(f"{name} must be greater than 0")
        return converted

    return _validate


def _positive_float(name: str) -> Callable[[str], float]:
    def _validate(value: str) -> float:
        try:
            converted = float(value)
        except ValueError as exc:  # pragma: no cover - argparse already reports
            raise argparse.ArgumentTypeError(f"{name} must be a number") from exc
        if converted <= 0:
            raise argparse.ArgumentTypeError(f"{name} must be greater than 0")
        return converted

    return _validate


def _configure_logging(level_name: str, fmt: str = "text") -> None:
    try:
        configure_logging(level_name, fmt=fmt)
    except ValueError as exc:
        raise CLIError(str(exc)) from exc


def _load_executor(dotted_path: str):
    module_name, sep, attr = dotted_path.rpartition(":")
    if not module_name or not sep:
        raise CLIError("Executor path must be in 'module:attr' format")
    try:
        module = importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        raise CLIError(f"Cannot import executor module '{module_name}': {exc}") from exc
    try:
        factory = getattr(module, attr)
    except AttributeError as exc:
        raise CLIError(f"Module '{module_name}' has no attribute '{attr}'") from exc
    try:
        return factory()
    except Exception as exc:  # pragma: no cover - defensive
        raise CLIError(f"Executor factory '{dotted_path}' raised: {exc}") from exc


def _resolve_config(args: argparse.Namespace) -> Config:
    overrides: dict[str, object] = {}
    if args.dsn is not None:
        overrides["dsn"] = args.dsn
    if args.concurrency is not None:
        overrides["concurrency"] = args.concurrency
    if args.batch is not None:
        overrides["batch"] = args.batch
    if args.lease_ttl is not None:
        overrides["lease_ttl"] = args.lease_ttl
    if args.poll_interval is not None:
        overrides["poll_interval"] = args.poll_interval
    if args.max_attempts is not None:
        overrides["max_attempts"] = args.max_attempts
    if args.default_executor is not None:
        overrides["default_executor"] = args.default_executor
    if args.disable_skip_locked:
        overrides["disable_skip_locked"] = True
    if args.enable_plugins:
        overrides["enable_plugins"] = True
    if args.log_level is not None:
        overrides["log_level"] = args.log_level
    if args.log_format is not None:
        overrides["log_format"] = args.log_format
    if args.retry_policy is not None:
        overrides["retry_policy"] = args.retry_policy
    if args.watchdog_interval is not None:
        overrides["watchdog_interval_s"] = args.watchdog_interval
    if args.rate_limit:
        merged: dict[str, dict[str, float]] = {}
        for entry in args.rate_limit:
            try:
                from .config import _parse_rate_limits_string

                merged.update(_parse_rate_limits_string(entry))
            except ConfigError as exc:
                raise CLIError(str(exc)) from exc
        overrides["rate_limits"] = merged
    if args.executor:
        overrides["extra_executors"] = tuple(args.executor)

    try:
        return load_config(config_path=args.config, overrides=overrides)
    except ConfigError as exc:
        raise CLIError(str(exc)) from exc


async def _run_worker(args: argparse.Namespace) -> None:
    config = _resolve_config(args)
    if not config.dsn:
        raise CLIError(
            "DSN is required: pass --dsn, set PYJOBKIT_DSN, or configure 'dsn' in .pyjobkit.toml"
        )

    _configure_logging(config.log_level, fmt=config.log_format)
    if args.metrics_port:
        start_metrics_server(args.metrics_port, host=args.metrics_host)
    worker: Worker | None = None
    stopped = False
    engine = None
    try:
        engine = create_async_engine(config.dsn)
    except SQLAlchemyError as exc:
        raise CLIError(f"Failed to create engine for DSN {config.dsn!r}: {exc}") from exc
    except Exception as exc:  # malformed DSN, missing driver, etc.
        raise CLIError(f"Failed to create engine for DSN {config.dsn!r}: {exc}") from exc

    try:
        backend = SQLBackend(
            engine,
            prefer_pg_skip_locked=not config.disable_skip_locked,
            lease_ttl_s=config.lease_ttl,
        )
        executors = [SubprocessExecutor(), HttpExecutor()]
        if config.default_executor:
            executors.append(_load_executor(config.default_executor))
        for dotted_path in config.extra_executors:
            executors.append(_load_executor(dotted_path))
        eng = Engine(backend=backend, executors=executors)
        if config.enable_plugins:
            registered = eng.register_plugins()
            if registered:
                logger.info(
                    "Loaded %d executor plugin(s): %s",
                    len(registered),
                    ", ".join(sorted(e.kind for e in registered)),
                )
        worker = Worker(
            eng,
            max_concurrency=config.concurrency,
            batch=config.batch,
            poll_interval=config.poll_interval,
            lease_ttl=config.lease_ttl,
            retry_policy=config.retry_policy,
            watchdog_interval_s=config.watchdog_interval_s,
            rate_limits=config.rate_limits,
            kinds=args.kind or None,
        )
        try:
            await worker.run(once=args.once)
        except asyncio.CancelledError:
            worker.request_stop()
            await worker.wait_stopped()
            stopped = True
            raise
        except KeyboardInterrupt:
            worker.request_stop()
            await worker.wait_stopped()
            stopped = True
            raise
        except Exception as exc:
            raise CLIError(f"Worker terminated with an unexpected error: {exc}") from exc
    finally:
        if worker is not None and not stopped:
            worker.request_stop()
            with suppress(Exception):
                await worker.wait_stopped()
        if engine is not None:
            with suppress(Exception):
                await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Pyjobkit worker loop")
    parser.add_argument(
        "--config",
        default=None,
        help="Path to a TOML config file (default: ./.pyjobkit.toml if present)",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="SQLAlchemy async DSN (overrides config / PYJOBKIT_DSN)",
    )
    parser.add_argument("--concurrency", type=_positive_int("concurrency"), default=None)
    parser.add_argument("--batch", type=_positive_int("batch"), default=None)
    parser.add_argument("--lease-ttl", type=_positive_int("lease-ttl"), default=None)
    parser.add_argument("--poll-interval", type=_positive_float("poll-interval"), default=None)
    parser.add_argument(
        "--max-attempts",
        type=_positive_int("max-attempts"),
        default=None,
        help="Default max_attempts used when enqueueing jobs without an override",
    )
    parser.add_argument(
        "--default-executor",
        default=None,
        help="Dotted-path 'module:attr' factory for the default extra executor",
    )
    parser.add_argument(
        "--disable-skip-locked",
        action="store_true",
        help="Disable Postgres SKIP LOCKED optimization",
    )
    parser.add_argument(
        "--executor",
        action="append",
        help="Additional executor in the form 'module:attr' to register with the worker",
    )
    parser.add_argument(
        "--enable-plugins",
        action="store_true",
        help="Discover and register executors from 'pyjobkit.executors' entry points",
    )
    parser.add_argument(
        "--metrics-port",
        type=int,
        default=None,
        help=(
            "Expose Prometheus /metrics on this port. Requires the "
            "'prometheus_client' package to be installed."
        ),
    )
    parser.add_argument(
        "--metrics-host",
        default="0.0.0.0",
        help="Bind address for the Prometheus /metrics server (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Drain available jobs once then exit (no infinite poll loop)",
    )
    parser.add_argument(
        "--kind",
        action="append",
        default=None,
        help=(
            "Restrict the worker to jobs whose 'kind' is in this set. "
            "Repeat to allow multiple kinds. Rejected claims are released "
            "back to the queue."
        ),
    )
    parser.add_argument(
        "--rate-limit",
        action="append",
        default=None,
        help=(
            "Per-kind rate limit, e.g. 'http:5:10' (5 jobs/s, burst 10) or "
            "'email:2'. Repeat the flag for multiple kinds."
        ),
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=list(LOG_LEVELS),
        help="Root logging level for the worker",
    )
    parser.add_argument(
        "--log-format",
        default=None,
        choices=list(LOG_FORMATS),
        help="Log format: 'text' (human) or 'json' (structured)",
    )
    parser.add_argument(
        "--retry-policy",
        default=None,
        help=(
            "Default retry policy spec, e.g. 'exponential:1:2', "
            "'exponential_jitter:1:2:30:0.1', or 'fixed:5'"
        ),
    )
    parser.add_argument(
        "--watchdog-interval",
        type=_positive_float("watchdog-interval"),
        default=None,
        help=(
            "Seconds between watchdog sweeps for expired leases "
            "(defaults to --lease-ttl)"
        ),
    )
    args = parser.parse_args()
    try:
        asyncio.run(_run_worker(args))
    except KeyboardInterrupt:  # pragma: no cover - CLI convenience
        print("Received Ctrl+C, requesting worker shutdown...", file=sys.stderr)
        raise SystemExit(130)
    except CLIError as exc:
        print(f"pyjobkit: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    except Exception as exc:  # pragma: no cover - defensive
        print(f"pyjobkit: unexpected failure: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":  # pragma: no cover
    main()
