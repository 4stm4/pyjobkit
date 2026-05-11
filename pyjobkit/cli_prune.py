"""`pyjobkit-prune` console script - delete old terminal jobs.

Without this command the SQL backend accumulates successful / failed /
timed-out rows forever. Run on a schedule (cron, systemd timer, etc.)::

    pyjobkit-prune --older-than 30d
    pyjobkit-prune --older-than 7d --statuses failed,timeout

``--older-than`` accepts ``Nm`` (minutes), ``Nh`` (hours), ``Nd``
(days), or a bare number (seconds).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
from datetime import timedelta
from typing import Sequence

from sqlalchemy.ext.asyncio import create_async_engine

from .backends.sql import SQLBackend
from .config import load_config
from .engine import Engine

logger = logging.getLogger(__name__)


_AGE_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhd]?)$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "": 1}


def _parse_age(value: str) -> timedelta:
    m = _AGE_RE.match(value.strip())
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid --older-than value: {value!r} (expected e.g. '30d', '2h')"
        )
    return timedelta(seconds=float(m["value"]) * _UNITS[m["unit"]])


def _parse_statuses(value: str) -> tuple[str, ...]:
    parts = tuple(s.strip() for s in value.split(",") if s.strip())
    allowed = {"success", "failed", "timeout", "cancelled"}
    bad = [p for p in parts if p not in allowed]
    if bad:
        raise argparse.ArgumentTypeError(
            f"unknown status(es): {bad}; choose from {sorted(allowed)}"
        )
    return parts


async def _run(args: argparse.Namespace) -> int:
    overrides = {}
    if args.dsn:
        overrides["dsn"] = args.dsn
    cfg = load_config(overrides=overrides)
    if not cfg.dsn:
        raise SystemExit(
            "DSN required: pass --dsn, set PYJOBKIT_DSN, or configure .pyjobkit.toml"
        )

    db = create_async_engine(cfg.dsn)
    try:
        backend = SQLBackend(db)
        engine = Engine(backend=backend, executors=[])
        count = await engine.purge_finished(
            older_than=args.older_than, statuses=args.statuses
        )
        logger.info("pruned %d job(s)", count)
        print(count)
        return 0
    finally:
        await db.dispose()


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="pyjobkit-prune",
        description="Delete terminal jobs from the Pyjobkit job_tasks table.",
    )
    parser.add_argument("--dsn", default=None, help="SQLAlchemy async DSN")
    parser.add_argument(
        "--older-than",
        type=_parse_age,
        default=None,
        help=(
            "Only delete jobs whose finished_at (or created_at) is older "
            "than this duration. e.g. '30d', '24h', '90m'"
        ),
    )
    parser.add_argument(
        "--statuses",
        type=_parse_statuses,
        default=("success", "failed", "timeout", "cancelled"),
        help="Comma-separated list of statuses to delete (default: all terminal)",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=args.log_level, format="%(message)s")
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":  # pragma: no cover
    main()
