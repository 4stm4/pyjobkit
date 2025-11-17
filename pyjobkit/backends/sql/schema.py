"""SQLAlchemy Core schema for job tasks."""

from __future__ import annotations

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    Column,
    DateTime,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    func,
)
from sqlalchemy.dialects.mysql import JSON as MYSQL_JSON
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.sqlite import JSON as SQLITE_JSON

metadata = MetaData()

JobTasks = Table(
    "job_tasks",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()),
    Column("scheduled_for", DateTime(timezone=True), nullable=False, server_default=func.current_timestamp()),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("status", Text, nullable=False, server_default="queued"),
    Column("attempts", Integer, nullable=False, server_default="0"),
    Column("max_attempts", Integer, nullable=False, server_default="3"),
    Column("priority", Integer, nullable=False, server_default="100"),
    Column("kind", Text, nullable=False),
    Column(
        "payload",
        JSON().with_variant(JSONB, "postgresql")
        .with_variant(MYSQL_JSON, "mysql")
        .with_variant(SQLITE_JSON, "sqlite"),
        nullable=False,
    ),
    Column(
        "result",
        JSON().with_variant(JSONB, "postgresql")
        .with_variant(MYSQL_JSON, "mysql")
        .with_variant(SQLITE_JSON, "sqlite"),
    ),
    Column("idempotency_key", Text, unique=True),
    Column("cancel_requested", Boolean, nullable=False, server_default="0"),
    Column("leased_by", String(36)),
    Column("lease_until", DateTime(timezone=True)),
    Column("version", Integer, nullable=False, server_default="0"),
    Column("timeout_s", Integer),
    CheckConstraint(
        "status IN ('queued','running','success','failed','cancelled','timeout')",
        name="job_status_chk",
    ),
)

Index(
    "idx_jobs_status_scheduled",
    JobTasks.c.status,
    JobTasks.c.scheduled_for,
    JobTasks.c.priority,
)

Index(
    "idx_jobs_leased",
    JobTasks.c.lease_until,
    JobTasks.c.leased_by,
)

__all__ = ["JobTasks", "metadata"]
