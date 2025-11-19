"""Entry-point for the ``pyjobkit`` console script."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import sys
from typing import Callable

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import create_async_engine

from .backends.sql import SQLBackend
from .engine import Engine
from .executors import HttpExecutor, SubprocessExecutor
from .worker import Worker


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


def _configure_logging(level_name: str) -> None:
    numeric = logging.getLevelName(level_name.upper())
    if not isinstance(numeric, int):  # pragma: no cover - guarded by argparse choices
        raise CLIError(f"Unknown log level: {level_name}")
    logging.basicConfig(level=numeric, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")


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


async def _run_worker(args: argparse.Namespace) -> None:
    _configure_logging(args.log_level)
    try:
        engine = create_async_engine(args.dsn)
    except SQLAlchemyError as exc:
        raise CLIError(f"Failed to create engine for DSN {args.dsn!r}: {exc}") from exc

    try:
        backend = SQLBackend(
            engine,
            prefer_pg_skip_locked=not args.disable_skip_locked,
            lease_ttl_s=args.lease_ttl,
        )
        executors = [SubprocessExecutor(), HttpExecutor()]
        for dotted_path in getattr(args, "executor", None) or []:
            executors.append(_load_executor(dotted_path))
        eng = Engine(backend=backend, executors=executors)
        worker = Worker(
            eng,
            max_concurrency=args.concurrency,
            batch=args.batch,
            poll_interval=args.poll_interval,
            lease_ttl=args.lease_ttl,
        )
        try:
            await worker.run()
        except asyncio.CancelledError:
            worker.request_stop()
            await worker.wait_stopped()
            raise
        except Exception as exc:
            raise CLIError(f"Worker terminated with an unexpected error: {exc}") from exc
    finally:
        await engine.dispose()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Pyjobkit worker loop")
    parser.add_argument("--dsn", required=True, help="SQLAlchemy async DSN")
    parser.add_argument("--concurrency", type=_positive_int("concurrency"), default=8)
    parser.add_argument("--batch", type=_positive_int("batch"), default=1)
    parser.add_argument("--lease-ttl", type=_positive_int("lease-ttl"), default=30)
    parser.add_argument("--poll-interval", type=_positive_float("poll-interval"), default=0.5)
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
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="Root logging level for the worker",
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
