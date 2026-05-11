"""`pyjobkit-migrate` console script - run Alembic against the SQL backend.

The command ships the alembic.ini and migrations bundled with the
library and runs them against the DSN passed in via ``--dsn``,
``PYJOBKIT_DSN``, or a ``.pyjobkit.toml`` file.

Usage::

    pyjobkit-migrate up           # alembic upgrade head
    pyjobkit-migrate down 1       # alembic downgrade -1
    pyjobkit-migrate current      # alembic current
    pyjobkit-migrate history      # alembic history
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Sequence

from .config import load_config

logger = logging.getLogger(__name__)


def _alembic_config(dsn: str):  # type: ignore[no-untyped-def]
    try:
        from alembic.config import Config
    except ImportError as exc:
        raise SystemExit(
            "pyjobkit-migrate requires 'alembic'; install with: pip install alembic"
        ) from exc

    sql_dir = Path(__file__).resolve().parent / "backends" / "sql"
    cfg = Config(str(sql_dir / "alembic.ini"))
    cfg.set_main_option("script_location", str(sql_dir / "migrations"))
    cfg.set_main_option("sqlalchemy.url", dsn)
    return cfg


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="pyjobkit-migrate",
        description="Run Alembic migrations against the Pyjobkit SQL backend.",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="SQLAlchemy DSN (defaults to PYJOBKIT_DSN / .pyjobkit.toml)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up", help="alembic upgrade head")
    down = sub.add_parser("down", help="alembic downgrade")
    down.add_argument("steps", default="1", nargs="?", help="number of steps or revision")
    sub.add_parser("current", help="show current revision")
    sub.add_parser("history", help="show migration history")

    args = parser.parse_args(argv)

    overrides = {}
    if args.dsn:
        overrides["dsn"] = args.dsn
    cfg = load_config(overrides=overrides)
    if not cfg.dsn:
        raise SystemExit(
            "DSN required: use --dsn, PYJOBKIT_DSN, or .pyjobkit.toml"
        )

    # Alembic's synchronous engine needs a non-async DSN. Strip the
    # +asyncpg / +aiomysql / +aiosqlite driver hint so it falls back to
    # the sync driver of the same dialect.
    sync_dsn = cfg.dsn
    for prefix, replacement in (
        ("postgresql+asyncpg", "postgresql+psycopg2"),
        ("mysql+aiomysql", "mysql+pymysql"),
        ("sqlite+aiosqlite", "sqlite"),
    ):
        if sync_dsn.startswith(prefix + ":"):
            sync_dsn = sync_dsn.replace(prefix, replacement, 1)
            break

    alembic_cfg = _alembic_config(sync_dsn)

    from alembic import command

    if args.command == "up":
        command.upgrade(alembic_cfg, "head")
    elif args.command == "down":
        target = args.steps
        if target.lstrip("-").isdigit():
            target = f"-{abs(int(target))}"
        command.downgrade(alembic_cfg, target)
    elif args.command == "current":
        command.current(alembic_cfg)
    elif args.command == "history":
        command.history(alembic_cfg)


if __name__ == "__main__":  # pragma: no cover
    main()
