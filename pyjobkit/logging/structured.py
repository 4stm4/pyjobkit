"""Structured JSON log formatter for Pyjobkit.

The formatter emits one JSON object per ``logging.LogRecord``. The base
shape is::

    {"ts": "...", "level": "INFO", "logger": "pyjobkit.worker", "msg": "..."}

Additional fields placed on the record via ``logger.<level>(..., extra={...})``
are merged into the same object. The standard keys ``event``, ``job_id``,
``worker_id``, ``status`` and ``duration_ms`` are surfaced first to make the
output greppable; other keys follow in insertion order.

Use :func:`configure_logging` from :mod:`pyjobkit.logging` (re-exported from
this module) to wire the formatter to the root logger.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Iterable

__all__ = ["JsonFormatter", "configure_logging"]

# Keys that should appear first in the JSON output (in this order).
_PRIORITY_KEYS = ("ts", "level", "logger", "msg", "event", "job_id", "worker_id", "status", "duration_ms")

# Fields present on every LogRecord that we do not want to forward as extras.
_RESERVED_RECORD_ATTRS = frozenset(
    {
        "name",
        "msg",
        "args",
        "levelname",
        "levelno",
        "pathname",
        "filename",
        "module",
        "exc_info",
        "exc_text",
        "stack_info",
        "lineno",
        "funcName",
        "created",
        "msecs",
        "relativeCreated",
        "thread",
        "threadName",
        "processName",
        "process",
        "taskName",
        "asctime",
        "message",
    }
)


def _jsonify(value: Any) -> Any:
    """Convert ``value`` to a JSON-serializable form, falling back to ``repr``."""

    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (list, tuple)):
        return [_jsonify(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _jsonify(v) for k, v in value.items()}
    return repr(value)


class JsonFormatter(logging.Formatter):
    """``logging.Formatter`` that renders records as single-line JSON objects."""

    def __init__(self, *, ensure_ascii: bool = False) -> None:
        super().__init__()
        self._ensure_ascii = ensure_ascii

    def format(self, record: logging.LogRecord) -> str:
        base: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extras: dict[str, Any] = {}
        for key, value in record.__dict__.items():
            if key in _RESERVED_RECORD_ATTRS or key.startswith("_"):
                continue
            extras[key] = _jsonify(value)

        if record.exc_info:
            base["exc_info"] = self.formatException(record.exc_info)
        if record.stack_info:
            base["stack_info"] = self.formatStack(record.stack_info)

        ordered: dict[str, Any] = {}
        for key in _PRIORITY_KEYS:
            if key in base:
                ordered[key] = base[key]
            elif key in extras:
                ordered[key] = extras.pop(key)
        # Remaining base + extra fields, preserving insertion order.
        for key, value in base.items():
            if key not in ordered:
                ordered[key] = value
        for key, value in extras.items():
            if key not in ordered:
                ordered[key] = value

        return json.dumps(ordered, ensure_ascii=self._ensure_ascii, default=str)


_HANDLER_MARKER = "_pyjobkit_handler"


def configure_logging(level: str | int = "INFO", *, fmt: str = "text") -> None:
    """Configure the root logger for Pyjobkit's worker process.

    ``fmt`` selects between the human-readable ``"text"`` format and the
    machine-readable ``"json"`` format. Repeated calls are idempotent:
    only handlers installed by this function are replaced, so external
    handlers (pytest's caplog, Sentry, etc.) are left in place.
    """

    if fmt not in ("text", "json"):
        raise ValueError(f"unsupported log format: {fmt!r} (expected 'text' or 'json')")
    if isinstance(level, str):
        numeric: int | None = logging.getLevelNamesMapping().get(level.upper())
        if numeric is None:
            raise ValueError(f"unknown log level: {level!r}")
        level = numeric

    handler = logging.StreamHandler()
    handler.set_name("pyjobkit")
    setattr(handler, _HANDLER_MARKER, True)
    if fmt == "json":
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
        )

    root = logging.getLogger()
    _replace_pyjobkit_handlers(root, handler)
    root.setLevel(level)


def _replace_pyjobkit_handlers(
    logger: logging.Logger, replacement: logging.Handler
) -> None:
    """Swap out previously-installed pyjobkit handlers without touching others."""

    for existing in list(logger.handlers):
        if getattr(existing, _HANDLER_MARKER, False):
            logger.removeHandler(existing)
    logger.addHandler(replacement)
