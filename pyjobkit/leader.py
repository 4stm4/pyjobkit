"""Cooperative leader election primitives (#61).

The library ships a small `LeaderLock` ABC plus an in-process
`MemoryLeaderLock` reference implementation. Production deployments
typically back the lock with a row in the SQL backend (a single-row
table with an `expires_at` column) or with Redis SETNX; the contract
makes the rest of the application backend-agnostic.

`leader_loop` runs a coroutine while the lock is held, automatically
acquiring and re-acquiring as needed and yielding back to the caller's
event loop when the lock has been lost.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Awaitable, Callable
from uuid import UUID, uuid4

__all__ = ["LeaderLock", "MemoryLeaderLock", "leader_loop"]

logger = logging.getLogger(__name__)


class LeaderLock(ABC):
    """Backend-agnostic distributed lock for "only one worker does X" tasks."""

    @abstractmethod
    async def try_acquire(self, *, ttl_s: float) -> bool:
        """Atomically take or extend the lock for ``ttl_s`` seconds.

        Returns ``True`` when the lock is now held by this caller. The
        implementation must guarantee that two callers cannot both
        observe ``True`` before the previous holder's ``ttl_s`` lapses.
        """

    @abstractmethod
    async def release(self) -> None:
        """Release the lock if held by this caller."""


class MemoryLeaderLock(LeaderLock):
    """In-process LeaderLock.

    Multiple ``MemoryLeaderLock`` instances created with the same
    ``name`` share a single owner field at the class level, so they
    compete for the same lock - mirroring how SQL / Redis backed
    implementations behave in production.
    """

    _state: dict[str, dict[str, object]] = {}
    _state_lock = threading.Lock()

    def __init__(self, name: str = "default") -> None:
        self.name = name
        self.holder_id = uuid4()
        with self._state_lock:
            self._state.setdefault(name, {"owner": None, "expires_at": 0.0})

    async def try_acquire(self, *, ttl_s: float) -> bool:
        with self._state_lock:
            now = time.monotonic()
            s = self._state[self.name]
            if (
                s["owner"] == self.holder_id
                or s["owner"] is None
                or now >= float(s["expires_at"])  # type: ignore[arg-type]
            ):
                s["owner"] = self.holder_id
                s["expires_at"] = now + ttl_s
                return True
            return False

    async def release(self) -> None:
        with self._state_lock:
            s = self._state[self.name]
            if s["owner"] == self.holder_id:
                s["owner"] = None
                s["expires_at"] = 0.0

    @classmethod
    def reset_state(cls) -> None:
        """Clear all shared lock state - intended for test isolation."""

        cls._state.clear()


async def leader_loop(
    lock: LeaderLock,
    *,
    ttl_s: float = 30.0,
    renew_every_s: float | None = None,
    on_leader: Callable[[], Awaitable[None]],
    stop_event: asyncio.Event | None = None,
    cancel_grace_s: float = 5.0,
) -> None:
    """Drive a coroutine while ``lock`` is held.

    Acquires the lock with ``ttl_s`` seconds, runs ``on_leader()``
    concurrently, and renews the lock every ``renew_every_s`` (defaults
    to ``ttl_s / 2``). If the lock is lost the ``on_leader`` coroutine
    is cancelled and the loop returns to polling for the lock.

    ``cancel_grace_s`` bounds how long the loop will wait for
    ``on_leader`` to honour the cancellation before giving up; if the
    coroutine swallows ``CancelledError`` the loop logs a warning and
    moves on, releasing the lock so a replacement leader can take over.

    Set ``stop_event`` to cooperatively exit the loop. Any exception
    raised by ``on_leader`` is logged and the loop continues.
    """

    if ttl_s <= 0:
        raise ValueError("ttl_s must be > 0")
    if renew_every_s is None:
        renew_every_s = ttl_s / 2.0
    if renew_every_s <= 0:
        raise ValueError("renew_every_s must be > 0")
    if cancel_grace_s <= 0:
        raise ValueError("cancel_grace_s must be > 0")
    stop_event = stop_event or asyncio.Event()
    poll_interval = min(1.0, ttl_s / 3.0)

    while not stop_event.is_set():
        if not await lock.try_acquire(ttl_s=ttl_s):
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=poll_interval)
                return
            except asyncio.TimeoutError:
                continue

        leader_task = asyncio.create_task(on_leader())
        try:
            while not stop_event.is_set():
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=renew_every_s)
                    break
                except asyncio.TimeoutError:
                    pass
                if not await lock.try_acquire(ttl_s=ttl_s):
                    logger.info("leader: lock lost; cancelling on_leader task")
                    break
                if leader_task.done():
                    try:
                        leader_task.result()
                    except Exception as exc:
                        logger.warning(
                            "leader: on_leader raised: %s", exc, exc_info=True
                        )
                    leader_task = asyncio.create_task(on_leader())
        finally:
            leader_task.cancel()
            try:
                await asyncio.wait_for(leader_task, timeout=cancel_grace_s)
            except asyncio.TimeoutError:
                logger.warning(
                    "leader: on_leader did not honour cancellation within %.1fs; "
                    "releasing the lock anyway",
                    cancel_grace_s,
                )
            except (asyncio.CancelledError, Exception):
                pass
            await lock.release()
