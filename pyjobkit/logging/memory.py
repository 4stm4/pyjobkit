"""In-process log sink useful for tests and demos."""

from __future__ import annotations

import asyncio
import threading
from collections import defaultdict, deque
from typing import Deque, Dict, List
from uuid import UUID

from ..contracts import LogRecord, LogSink


class MemoryLogSink(LogSink):
    """Simple log sink storing lines per job."""

    def __init__(self, *, max_items: int = 1000) -> None:
        self._max_items = max_items
        self._logs: Dict[UUID, Deque[LogRecord]] = defaultdict(deque)
        self._lock = asyncio.Lock()
        self._thread_lock = threading.Lock()

    async def write(self, record: LogRecord) -> None:  # type: ignore[override]
        async with self._lock:
            with self._thread_lock:
                log = self._logs[record.job_id]
                if len(log) >= self._max_items:
                    log.popleft()
                log.append(record)

    async def get(self, job_id: UUID) -> List[LogRecord]:
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            with self._thread_lock:
                return list(self._logs.get(job_id, ()))

        async with self._lock:
            with self._thread_lock:
                return list(self._logs.get(job_id, ()))
