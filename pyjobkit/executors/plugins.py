"""Entry-point based discovery of third-party executors.

Plugins register an executor *factory* under the ``pyjobkit.executors``
group::

    # pyproject.toml of the plugin package
    [project.entry-points."pyjobkit.executors"]
    s3 = "myplugin.executors:make_s3_executor"

A factory is any callable that takes no required arguments and returns
an instance implementing :class:`pyjobkit.contracts.Executor`. Plugins
are loaded lazily by :func:`discover_executors`; failures in a single
plugin (import error, factory raising, returning a non-executor) are
logged and the loader continues with the rest.
"""

from __future__ import annotations

import importlib.metadata
import logging
from typing import Iterable

from ..contracts import Executor

__all__ = ["EXECUTOR_ENTRY_POINT_GROUP", "discover_executors"]

EXECUTOR_ENTRY_POINT_GROUP = "pyjobkit.executors"

logger = logging.getLogger(__name__)


def discover_executors(
    *,
    group: str = EXECUTOR_ENTRY_POINT_GROUP,
    only: Iterable[str] | None = None,
) -> list[Executor]:
    """Load and instantiate every executor advertised under ``group``.

    Parameters:
        group: The entry-point group to inspect. Override only for tests.
        only: Optional iterable of entry-point names; when provided,
            entries whose name is not in this set are skipped. Useful
            when an application wants to opt in to a subset.

    Returns:
        A list of successfully constructed executor instances, in the
        order returned by :func:`importlib.metadata.entry_points`.
    """

    allow: set[str] | None = set(only) if only is not None else None
    instances: list[Executor] = []
    try:
        entries = importlib.metadata.entry_points(group=group)
    except TypeError:  # pragma: no cover - Python 3.9 fallback
        # Older importlib.metadata (Python 3.9) does not accept group=.
        entries = importlib.metadata.entry_points().get(group, [])  # type: ignore[assignment]

    for entry in entries:
        if allow is not None and entry.name not in allow:
            continue
        try:
            factory = entry.load()
        except Exception as exc:  # pragma: no cover - rare import failure
            logger.warning(
                "failed to load executor plugin %r: %s", entry.name, exc, exc_info=True
            )
            continue
        try:
            instance = factory()
        except Exception as exc:
            logger.warning(
                "executor plugin %r raised during factory call: %s",
                entry.name,
                exc,
                exc_info=True,
            )
            continue
        if not isinstance(instance, Executor):
            logger.warning(
                "executor plugin %r did not return an Executor (got %r); skipping",
                entry.name,
                type(instance).__name__,
            )
            continue
        instances.append(instance)
    return instances
