"""Tests for ``pyjobkit.logging.structured``."""

from __future__ import annotations

import io
import json
import logging
from uuid import UUID, uuid4

import pytest

from pyjobkit.logging import JsonFormatter, configure_logging


def _format(record: logging.LogRecord, formatter: JsonFormatter | None = None) -> dict:
    formatter = formatter or JsonFormatter()
    return json.loads(formatter.format(record))


def _make_record(msg: str = "hello", level: int = logging.INFO, **extra) -> logging.LogRecord:
    record = logging.LogRecord(
        name="pyjobkit.test",
        level=level,
        pathname=__file__,
        lineno=1,
        msg=msg,
        args=(),
        exc_info=None,
    )
    for k, v in extra.items():
        setattr(record, k, v)
    return record


def test_base_fields_present() -> None:
    payload = _format(_make_record("hi"))
    assert payload["msg"] == "hi"
    assert payload["level"] == "INFO"
    assert payload["logger"] == "pyjobkit.test"
    assert "ts" in payload


def test_extras_are_merged_with_priority_order() -> None:
    record = _make_record(
        "state change",
        event="job.succeeded",
        job_id="abc",
        worker_id="w1",
        status="success",
        duration_ms=12.5,
        extra_field="ok",
    )
    payload = _format(record)
    keys = list(payload.keys())
    # Priority keys come first in the documented order.
    assert keys[:9] == [
        "ts",
        "level",
        "logger",
        "msg",
        "event",
        "job_id",
        "worker_id",
        "status",
        "duration_ms",
    ]
    assert payload["extra_field"] == "ok"


def test_uuid_and_other_objects_are_repr_or_stringified() -> None:
    jid = uuid4()
    payload = _format(_make_record(job_id=jid))
    # UUID falls through to repr() via _jsonify
    assert isinstance(payload["job_id"], str)
    assert str(jid) in payload["job_id"]


def test_exception_info_is_captured() -> None:
    try:
        raise RuntimeError("boom")
    except RuntimeError:
        import sys

        record = logging.LogRecord(
            name="pyjobkit.test",
            level=logging.ERROR,
            pathname=__file__,
            lineno=1,
            msg="oh no",
            args=(),
            exc_info=sys.exc_info(),
        )
    payload = _format(record)
    assert "exc_info" in payload
    assert "RuntimeError" in payload["exc_info"]


def test_configure_logging_json_emits_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("DEBUG", fmt="json")
    log = logging.getLogger("pyjobkit.test.json")
    log.info("hello", extra={"event": "demo", "job_id": "j1"})
    captured = capsys.readouterr()
    # Single JSON object per line.
    line = captured.err.strip().splitlines()[-1]
    payload = json.loads(line)
    assert payload["msg"] == "hello"
    assert payload["event"] == "demo"
    assert payload["job_id"] == "j1"


def test_configure_logging_text_does_not_emit_json(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("INFO", fmt="text")
    log = logging.getLogger("pyjobkit.test.text")
    log.info("plain message")
    captured = capsys.readouterr()
    assert "plain message" in captured.err
    with pytest.raises(json.JSONDecodeError):
        json.loads(captured.err.strip().splitlines()[-1])


def test_configure_logging_rejects_unknown_format() -> None:
    with pytest.raises(ValueError):
        configure_logging("INFO", fmt="yaml")  # type: ignore[arg-type]


def test_configure_logging_rejects_unknown_level() -> None:
    with pytest.raises(ValueError):
        configure_logging("LOUD", fmt="text")


def test_configure_logging_is_idempotent() -> None:
    configure_logging("INFO", fmt="json")
    handlers_after_first = list(logging.getLogger().handlers)
    configure_logging("INFO", fmt="json")
    handlers_after_second = list(logging.getLogger().handlers)
    assert len(handlers_after_second) == len(handlers_after_first) == 1
