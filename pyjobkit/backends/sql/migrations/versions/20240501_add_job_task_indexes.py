"""Add indexes for scheduling and leasing lookups."""

from __future__ import annotations

from alembic import op

revision = "20240501_add_job_task_indexes"
down_revision = "20240101_baseline"
branch_labels = None
depends_on = None


def _index_exists(bind, name: str) -> bool:
    insp = op.inspect(bind)
    try:
        return any(idx["name"] == name for idx in insp.get_indexes("job_tasks"))
    except Exception:
        return False


def upgrade() -> None:
    bind = op.get_bind()
    # The baseline migration calls metadata.create_all(), which already
    # registers the same indexes - skip if they're present.
    if not _index_exists(bind, "idx_jobs_status_scheduled"):
        op.create_index(
            "idx_jobs_status_scheduled",
            "job_tasks",
            ["status", "scheduled_for", "priority"],
        )
    if not _index_exists(bind, "idx_jobs_leased"):
        op.create_index(
            "idx_jobs_leased",
            "job_tasks",
            ["lease_until", "leased_by"],
        )


def downgrade() -> None:
    op.drop_index("idx_jobs_leased", table_name="job_tasks")
    op.drop_index("idx_jobs_status_scheduled", table_name="job_tasks")
