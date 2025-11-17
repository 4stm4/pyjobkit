"""Add indexes for scheduling and leasing lookups."""

from __future__ import annotations

from alembic import op

revision = "20240501_add_job_task_indexes"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "idx_jobs_status_scheduled",
        "job_tasks",
        ["status", "scheduled_for", "priority"],
    )
    op.create_index(
        "idx_jobs_leased",
        "job_tasks",
        ["lease_until", "leased_by"],
    )


def downgrade() -> None:
    op.drop_index("idx_jobs_leased", table_name="job_tasks")
    op.drop_index("idx_jobs_status_scheduled", table_name="job_tasks")
