"""`pyjobkit simulate` console script - run jobs from a JSON file locally.

The command spins up an in-memory `Engine` + `Worker`, registers a set
of executors (the built-in subprocess / HTTP plus any factory paths
given via ``--executor``), enqueues every job described in the input
file, and waits for the queue to drain. It is intended for local
debugging and CI smoke tests where running against a real database
would be overkill.

Input file format (JSON)::

    {
      "jobs": [
        {"kind": "echo", "payload": {"x": 1}},
        {"kind": "echo", "payload": {"y": 2}, "max_attempts": 5,
         "shadow": false, "retry_policy": "fixed:0.1"}
      ]
    }

YAML is supported automatically when PyYAML is installed.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import sys
from pathlib import Path
from typing import Any, Sequence

from .backends.memory import MemoryBackend
from .contracts import Executor
from .engine import Engine
from .executors import HttpExecutor, SubprocessExecutor
from .logging import configure_logging
from .worker import Worker

logger = logging.getLogger(__name__)


class SimulateError(RuntimeError):
    """Raised on invalid simulate inputs."""


def _load_input(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() in {".yaml", ".yml"}:
        try:
            import yaml  # type: ignore[import-not-found]
        except ImportError as exc:
            raise SimulateError(
                "YAML input requires PyYAML; install it or use a .json file"
            ) from exc
        data = yaml.safe_load(text)
    else:
        data = json.loads(text)
    if not isinstance(data, dict) or "jobs" not in data:
        raise SimulateError(
            "Input must be a JSON / YAML mapping with a top-level 'jobs' list"
        )
    if not isinstance(data["jobs"], list):
        raise SimulateError("'jobs' must be a list")
    return data


def _load_executor_factory(dotted: str) -> Executor:
    module_name, sep, attr = dotted.rpartition(":")
    if not module_name or not sep:
        raise SimulateError(
            f"executor spec {dotted!r} must be 'module:attr'"
        )
    module = importlib.import_module(module_name)
    factory = getattr(module, attr)
    return factory()


async def _simulate(args: argparse.Namespace) -> int:
    configure_logging(args.log_level, fmt=args.log_format)
    data = _load_input(Path(args.input))
    executors: list[Executor] = [SubprocessExecutor(), HttpExecutor()]
    for dotted in args.executor or []:
        executors.append(_load_executor_factory(dotted))

    backend = MemoryBackend()
    engine = Engine(backend=backend, executors=executors)
    worker = Worker(
        engine,
        max_concurrency=args.concurrency,
        poll_interval=0.01,
        watchdog_interval_s=0.2,
    )

    enqueued: list[str] = []
    for spec in data["jobs"]:
        if not isinstance(spec, dict) or "kind" not in spec:
            raise SimulateError(f"each job must define 'kind'; got {spec!r}")
        kwargs: dict[str, Any] = {
            "kind": spec["kind"],
            "payload": spec.get("payload", {}),
            "priority": spec.get("priority", 100),
            "max_attempts": spec.get("max_attempts", 3),
            "timeout_s": spec.get("timeout_s"),
            "idempotency_key": spec.get("idempotency_key"),
            "shadow": bool(spec.get("shadow", False)),
        }
        if "retry_policy" in spec:
            kwargs["retry_policy"] = spec["retry_policy"]
        job_id = await engine.enqueue(**kwargs)
        enqueued.append(str(job_id))

    await asyncio.wait_for(worker.run(once=True), timeout=args.timeout)

    summary: dict[str, list[dict[str, Any]]] = {
        "success": [],
        "failed": [],
        "timeout": [],
        "cancelled": [],
        "queued": [],
        "running": [],
    }
    for job_id_str in enqueued:
        from uuid import UUID

        rec = await backend.get(UUID(job_id_str))
        bucket = summary.setdefault(str(rec["status"]), [])
        bucket.append(
            {"id": job_id_str, "kind": rec["kind"], "result": rec.get("result")}
        )

    json.dump({"summary": summary}, sys.stdout, default=str, indent=2)
    sys.stdout.write("\n")

    return 0 if not (summary["failed"] or summary["timeout"]) else 1


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="pyjobkit-simulate",
        description="Run Pyjobkit jobs from a JSON / YAML file against an in-memory backend",
    )
    parser.add_argument("input", help="Path to .json / .yaml / .yml jobs file")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Maximum simultaneous executors (default: 4)",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=60.0,
        help="Overall timeout in seconds for queue drain (default: 60)",
    )
    parser.add_argument(
        "--executor",
        action="append",
        help="Additional executor factory 'module:attr' to register",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
    )
    parser.add_argument(
        "--log-format",
        default="text",
        choices=["text", "json"],
    )
    args = parser.parse_args(argv)
    try:
        code = asyncio.run(_simulate(args))
    except SimulateError as exc:
        print(f"pyjobkit-simulate: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    except FileNotFoundError as exc:
        print(f"pyjobkit-simulate: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc
    raise SystemExit(code)


if __name__ == "__main__":  # pragma: no cover
    main()
