"""Minimal in-process event bus."""

from __future__ import annotations

import asyncio
from collections import defaultdict
import logging
from typing import Awaitable, Callable, DefaultDict, List

from ..contracts import EventBus


class LocalEventBus(EventBus):
    """Fire-and-forget bus storing subscribers in-memory."""

    def __init__(self) -> None:
        self._subs: DefaultDict[str, List[Callable[[dict], Awaitable[None]]]] = defaultdict(list)

    def subscribe(self, topic: str, handler: Callable[[dict], Awaitable[None]]) -> None:
        self._subs[topic].append(handler)

    async def publish(self, topic: str, payload: dict) -> None:  # type: ignore[override]
        results = await asyncio.gather(
            *(handler(payload) for handler in self._subs.get(topic, ())),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, Exception):
                logging.error("Event handler failed", exc_info=result)
