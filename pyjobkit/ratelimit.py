"""Token-bucket rate limiting for executor dispatch.

The :class:`TokenBucket` is a small standalone primitive; the worker
maintains one bucket per ``kind`` so that jobs of the same kind are
throttled together regardless of which worker process picks them up.

Limits are expressed as ``(max_per_second, burst)``:

* ``max_per_second`` is the steady-state rate at which tokens refill
  (one token corresponds to one job execution).
* ``burst`` is the maximum number of tokens that can accumulate; it
  bounds how many jobs may run back-to-back after an idle period. When
  omitted it defaults to ``max_per_second``.
"""

from __future__ import annotations

import asyncio
from time import monotonic
from typing import Mapping

__all__ = ["TokenBucket", "RateLimitSpec", "parse_rate_limits"]


# (max_per_second, burst)
RateLimitSpec = tuple[float, float]


class TokenBucket:
    """Async token-bucket allowing at most ``burst`` calls per quiet window."""

    def __init__(self, max_per_second: float, burst: float | None = None) -> None:
        if max_per_second <= 0:
            raise ValueError("max_per_second must be > 0")
        capacity = float(burst) if burst is not None else float(max_per_second)
        if capacity <= 0:
            raise ValueError("burst must be > 0")
        self.rate = float(max_per_second)
        self.capacity = capacity
        self._tokens = capacity
        self._last = monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = monotonic()
        elapsed = now - self._last
        if elapsed > 0:
            self._tokens = min(self.capacity, self._tokens + elapsed * self.rate)
            self._last = now

    async def acquire(self, tokens: float = 1.0) -> None:
        """Block until ``tokens`` tokens are available, then consume them."""

        if tokens <= 0:
            return
        if tokens > self.capacity:
            raise ValueError(
                f"requested {tokens} tokens but bucket capacity is {self.capacity}"
            )
        async with self._lock:
            while True:
                self._refill()
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return
                deficit = tokens - self._tokens
                wait_s = deficit / self.rate
                # Sleep with the lock held: we are the next-in-line consumer.
                await asyncio.sleep(wait_s)


def parse_rate_limits(
    spec: Mapping[str, Mapping[str, float] | RateLimitSpec | float] | None,
) -> dict[str, RateLimitSpec]:
    """Normalise a user-supplied rate-limit configuration.

    Accepted per-kind shapes:

    * ``{"max_per_second": 5, "burst": 10}``
    * ``(5, 10)`` / ``[5, 10]``
    * a single number, treated as ``max_per_second`` with default burst.
    """

    if not spec:
        return {}
    out: dict[str, RateLimitSpec] = {}
    for kind, value in spec.items():
        if isinstance(value, (int, float)):
            rate = float(value)
            burst = rate
        elif isinstance(value, Mapping):
            if "max_per_second" not in value:
                raise ValueError(
                    f"rate limit for {kind!r} must define 'max_per_second'"
                )
            rate = float(value["max_per_second"])
            burst = float(value.get("burst", rate))
        else:
            try:
                rate, burst = (float(x) for x in value)  # type: ignore[misc]
            except (TypeError, ValueError) as exc:
                raise ValueError(
                    f"rate limit for {kind!r} must be a number, mapping, or "
                    f"(rate, burst) tuple"
                ) from exc
        if rate <= 0:
            raise ValueError(f"rate limit for {kind!r}: max_per_second must be > 0")
        if burst <= 0:
            raise ValueError(f"rate limit for {kind!r}: burst must be > 0")
        out[kind] = (rate, burst)
    return out
