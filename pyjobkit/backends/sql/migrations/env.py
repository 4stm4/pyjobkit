"""Alembic env entry-point for the Pyjobkit SQL backend.

Designed to be driven by the ``pyjobkit-migrate`` console script, which
sets ``sqlalchemy.url`` programmatically. Running ``alembic upgrade``
directly also works as long as the URL is provided via env variable or
config file.
"""

from __future__ import annotations

from alembic import context
from sqlalchemy import engine_from_config, pool

from pyjobkit.backends.sql.schema import metadata as target_metadata

config = context.config
# Intentionally do not call ``fileConfig`` here. Loading the alembic.ini
# logging section would replace the host application's root-logger
# handlers (and pytest's caplog handler in tests). Operators that want
# alembic's chatty INFO output can pass ``--log-level`` to the
# ``pyjobkit-migrate`` CLI or configure logging in their own
# entry-point before calling ``alembic upgrade`` directly.


def run_migrations_offline() -> None:  # pragma: no cover - alembic offline mode
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    cfg = config.get_section(config.config_ini_section) or {}
    connectable = engine_from_config(
        cfg, prefix="sqlalchemy.", poolclass=pool.NullPool
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():  # pragma: no cover - alembic offline mode
    run_migrations_offline()
else:
    run_migrations_online()
