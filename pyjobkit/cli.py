"""Entry-point for the ``pyjobkit`` console script."""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging

from sqlalchemy.ext.asyncio import create_async_engine

from .backends.sql import SQLBackend
from .engine import Engine
from .executors import HttpExecutor, SubprocessExecutor
from .worker import Worker


async def _run_worker(args: argparse.Namespace) -> None:
    logging.basicConfig(level=logging.INFO)
    engine = create_async_engine(args.dsn)
    backend = SQLBackend(
        engine,
        prefer_pg_skip_locked=not args.disable_skip_locked,
        lease_ttl_s=args.lease_ttl,
    )
    executors = [SubprocessExecutor(), HttpExecutor()]
    for dotted_path in getattr(args, "executor", None) or []:
        module_name, _, attr = dotted_path.rpartition(":")
        if not module_name:
            raise ValueError("Executor path must be in 'module:attr' format")
        module = importlib.import_module(module_name)
        executors.append(getattr(module, attr)())
    eng = Engine(backend=backend, executors=executors)
    worker = Worker(
        eng,
        max_concurrency=args.concurrency,
        batch=args.batch,
        poll_interval=args.poll_interval,
        lease_ttl=args.lease_ttl,
    )
    await worker.run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Pyjobkit worker loop")
    parser.add_argument("--dsn", required=True, help="SQLAlchemy async DSN")
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--batch", type=int, default=1)
    parser.add_argument("--lease-ttl", type=int, default=30)
    parser.add_argument("--poll-interval", type=float, default=0.5)
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
    args = parser.parse_args()
    try:
        asyncio.run(_run_worker(args))
    except KeyboardInterrupt:  # pragma: no cover - CLI convenience
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
