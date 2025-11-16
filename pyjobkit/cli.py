"""Entry-point for the ``pyjobkit`` console script."""

from __future__ import annotations

import argparse
import asyncio

from sqlalchemy.ext.asyncio import create_async_engine

from .backends.sql import SQLBackend
from .engine import Engine
from .executors import HttpExecutor, SubprocessExecutor
from .worker import Worker


async def _run_worker(args: argparse.Namespace) -> None:
    engine = create_async_engine(args.dsn)
    backend = SQLBackend(
        engine,
        prefer_pg_skip_locked=not args.disable_skip_locked,
        lease_ttl_s=args.lease_ttl,
    )
    eng = Engine(backend=backend, executors=[SubprocessExecutor(), HttpExecutor()])
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
    args = parser.parse_args()
    try:
        asyncio.run(_run_worker(args))
    except KeyboardInterrupt:  # pragma: no cover - CLI convenience
        pass


if __name__ == "__main__":  # pragma: no cover
    main()
