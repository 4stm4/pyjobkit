"""Tests for the public typed API surface (PEP 561)."""

from __future__ import annotations

import importlib.resources
from typing import get_args, get_type_hints
from uuid import uuid4

import pyjobkit
from pyjobkit.engine import Engine
from pyjobkit.types import (
    FailureReason,
    JobRecord,
    JobResult,
    JobStatus,
    LogStream,
)


def test_py_typed_marker_ships_with_package() -> None:
    files = importlib.resources.files("pyjobkit")
    assert (files / "py.typed").is_file()


def test_status_literal_values() -> None:
    assert set(get_args(JobStatus)) == {
        "queued",
        "running",
        "success",
        "failed",
        "cancelled",
        "timeout",
    }


def test_log_stream_literal_values() -> None:
    assert set(get_args(LogStream)) == {"stdout", "stderr"}


def test_typed_dicts_accept_partial_payloads() -> None:
    record: JobRecord = {"id": uuid4(), "status": "running"}
    result: JobResult = {"ok": True}
    reason: FailureReason = {"error": "timeout", "message": "deadline exceeded"}
    assert record["status"] == "running"
    assert result["ok"] is True
    assert reason["error"] == "timeout"


def test_public_reexports() -> None:
    assert pyjobkit.JobRecord is JobRecord
    assert pyjobkit.JobResult is JobResult
    assert pyjobkit.FailureReason is FailureReason
    assert pyjobkit.JobStatus is JobStatus
    assert pyjobkit.LogStream is LogStream
    for name in ("JobRecord", "JobResult", "FailureReason", "JobStatus", "LogStream"):
        assert name in pyjobkit.__all__


def test_engine_get_annotation_is_job_record() -> None:
    hints = get_type_hints(Engine.get)
    assert hints["return"] is JobRecord
