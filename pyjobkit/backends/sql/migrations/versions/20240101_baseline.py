"""Baseline migration: create the job_tasks table.

The schema is sourced from :mod:`pyjobkit.backends.sql.schema`, so the
shape always matches what the SQLBackend expects at runtime.
"""

from __future__ import annotations

from alembic import op

from pyjobkit.backends.sql.schema import metadata as pyjobkit_metadata

revision = "20240101_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    pyjobkit_metadata.create_all(bind, checkfirst=True)


def downgrade() -> None:
    bind = op.get_bind()
    pyjobkit_metadata.drop_all(bind, checkfirst=True)
