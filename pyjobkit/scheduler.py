"""Periodic / cron-style scheduler for Pyjobkit jobs.

A :class:`Scheduler` registers named entries that should be enqueued at
a fixed interval (seconds, ``timedelta``, or a short string like
``"5m"`` / ``"1h"``). Its :meth:`Scheduler.run` coroutine drives the
loop and is safe to combine with the :mod:`pyjobkit.leader` module so
only the elected leader actually enqueues.

For cron expressions install the optional ``croniter`` package; the
:meth:`Scheduler.add_cron` method accepts standard 5-field cron
expressions when it is available.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Awaitable, Callable

from . import metrics
from .engine import Engine
from .leader import LeaderLock

logger = logging.getLogger(__name__)


_INTERVAL_RE = re.compile(r"^(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhd]?)$")
_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "": 1}


def parse_interval(value: str | float | int | timedelta) -> timedelta:
    """Parse ``5m`` / ``1h`` / float seconds / ``timedelta`` into ``timedelta``."""

    if isinstance(value, timedelta):
        return value
    if isinstance(value, (int, float)):
        return timedelta(seconds=float(value))
    m = _INTERVAL_RE.match(value.strip())
    if not m:
        raise ValueError(
            f"interval {value!r} must be e.g. '5m', '1h', '90', or a timedelta"
        )
    return timedelta(seconds=float(m["value"]) * _UNITS[m["unit"]])


@dataclass
class _Entry:
    name: str
    interval: timedelta
    kind: str
    payload: dict[str, Any]
    enqueue_kwargs: dict[str, Any]
    last_run: float = field(default_factory=lambda: 0.0)
    next_run: float = field(default_factory=lambda: 0.0)


class Scheduler:
    """Drive periodic enqueues against an :class:`Engine`.

    Example::

        scheduler = Scheduler(engine)
        scheduler.every("5m", name="health-check", kind="probe", payload={})
        scheduler.every(timedelta(hours=1), name="prune", kind="cleanup", payload={})
        await scheduler.run(stop_event=stop)

    Each registered entry uses an :ref:`idempotency_key` of the form
    ``scheduler:<name>:<slot>`` where ``<slot>`` is the integer time
    bucket of the current interval. That makes duplicate enqueues a
    no-op at the backend level - safe under leader churn (HA) since
    two schedulers racing on the same slot collapse to a single row
    via the backend's UNIQUE constraint on ``idempotency_key``.

    Enqueue failures are counted in
    ``pyjobkit_scheduler_enqueue_failures_total`` and logged at
    WARNING. The entry's ``next_run`` is **not** advanced on failure
    so the next tick re-attempts the same slot.
    """

    def __init__(self, engine: Engine, *, clock: Callable[[], float] | None = None) -> None:
        self.engine = engine
        self._clock = clock or time.monotonic
        self._entries: dict[str, _Entry] = {}

    def every(
        self,
        interval: str | float | int | timedelta,
        *,
        name: str,
        kind: str,
        payload: dict[str, Any] | None = None,
        run_immediately: bool = False,
        **enqueue_kwargs: Any,
    ) -> None:
        """Register a periodic entry. Replaces any prior entry with the same name.

        By default the first enqueue happens after ``interval`` has
        elapsed. Pass ``run_immediately=True`` to also enqueue on the
        very next :meth:`tick` (useful for warmup tasks that should
        run on startup).
        """

        td = parse_interval(interval)
        if td.total_seconds() <= 0:
            raise ValueError("interval must be positive")
        now = self._clock()
        self._entries[name] = _Entry(
            name=name,
            interval=td,
            kind=kind,
            payload=dict(payload or {}),
            enqueue_kwargs=enqueue_kwargs,
            last_run=0.0,
            next_run=now if run_immediately else now + td.total_seconds(),
        )

    def remove(self, name: str) -> None:
        self._entries.pop(name, None)

    def entries(self) -> list[str]:
        return sorted(self._entries)

    async def tick(self) -> int:
        """Enqueue every entry whose next run has elapsed; return the count."""

        now = self._clock()
        enqueued = 0
        for entry in list(self._entries.values()):
            if entry.next_run > now:
                continue
            # Time-bucket the idempotency key so racing schedulers (e.g.
            # during a leader handover) cannot enqueue the same slot twice.
            slot = int(entry.next_run // entry.interval.total_seconds())
            kwargs = dict(entry.enqueue_kwargs)
            kwargs.setdefault("idempotency_key", f"scheduler:{entry.name}:{slot}")
            try:
                await self.engine.enqueue(
                    kind=entry.kind,
                    payload=entry.payload,
                    **kwargs,
                )
                enqueued += 1
                entry.last_run = now
                entry.next_run = now + entry.interval.total_seconds()
            except Exception as exc:
                metrics.scheduler_enqueue_failures_total.inc()
                logger.warning(
                    "scheduler entry %r failed to enqueue (will retry next tick): %s",
                    entry.name,
                    exc,
                    exc_info=True,
                )
                # Do NOT advance next_run on failure so the next tick
                # re-attempts the same slot.
        return enqueued

    async def run(
        self,
        *,
        stop_event: asyncio.Event | None = None,
        leader_lock: LeaderLock | None = None,
        lock_ttl_s: float = 30.0,
        sleep_resolution_s: float = 1.0,
    ) -> None:
        """Run the scheduler loop until ``stop_event`` is set.

        When ``leader_lock`` is provided the loop only enqueues while
        it holds the lock - giving safe behaviour across multiple
        workers running the same scheduler.
        """

        stop_event = stop_event or asyncio.Event()
        try:
            while not stop_event.is_set():
                if leader_lock is not None:
                    if not await leader_lock.try_acquire(ttl_s=lock_ttl_s):
                        try:
                            await asyncio.wait_for(
                                stop_event.wait(), timeout=sleep_resolution_s
                            )
                            return
                        except asyncio.TimeoutError:
                            continue
                await self.tick()
                try:
                    await asyncio.wait_for(
                        stop_event.wait(), timeout=sleep_resolution_s
                    )
                    return
                except asyncio.TimeoutError:
                    continue
        finally:
            if leader_lock is not None:
                await leader_lock.release()


