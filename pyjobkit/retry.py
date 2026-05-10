"""Pluggable retry policies for failed / timed-out jobs.

A :class:`RetryPolicy` decides how long the worker should wait before
requeueing a job that did not finish on this attempt. Built-in policies
cover the most common shapes (fixed delay, exponential backoff,
exponential with random jitter); callers may also implement the
:class:`RetryPolicy` protocol to supply their own strategy.

A policy can be selected either by passing an instance to the
:class:`~pyjobkit.worker.Worker` constructor, by spelling it as a
``"name:arg:arg"`` string in configuration, or per-job through
``Engine.enqueue(..., retry_policy=...)`` (the spec is propagated via a
payload marker and resolved by the worker on dequeue).
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod

__all__ = [
    "RetryPolicy",
    "FixedDelay",
    "ExponentialBackoff",
    "JitteredExponentialBackoff",
    "parse_policy",
    "RETRY_POLICY_PAYLOAD_KEY",
    "DEFAULT_RETRY_POLICY",
]


RETRY_POLICY_PAYLOAD_KEY = "__pjk_retry_policy"
"""Payload key used to carry a per-job retry policy spec."""


class RetryPolicy(ABC):
    """Strategy that decides the delay before the next retry attempt."""

    @abstractmethod
    def delay(self, attempt: int) -> float:
        """Return the delay in seconds before retrying attempt ``attempt``.

        ``attempt`` is 1-indexed and represents the attempt that just
        failed (so the *next* attempt is ``attempt + 1``). Implementations
        should return a non-negative float; the worker is free to clamp
        the value to a reasonable range.
        """

    def __repr__(self) -> str:  # pragma: no cover - cosmetic
        return f"{type(self).__name__}()"


class FixedDelay(RetryPolicy):
    """Always wait the same number of seconds between attempts."""

    def __init__(self, delay_s: float = 1.0) -> None:
        if delay_s < 0:
            raise ValueError("delay_s must be non-negative")
        self.delay_s = float(delay_s)

    def delay(self, attempt: int) -> float:  # noqa: D401 - protocol method
        return self.delay_s


class ExponentialBackoff(RetryPolicy):
    """Classic exponential backoff: ``base * factor ** (attempt - 1)``.

    With the defaults ``base=1.0`` and ``factor=2.0`` the delays are
    ``1, 2, 4, 8, ...`` seconds, matching Pyjobkit's previous hardcoded
    behaviour. ``max_delay_s`` clamps the result to bound runaway waits.
    """

    def __init__(
        self,
        base: float = 1.0,
        factor: float = 2.0,
        max_delay_s: float | None = None,
    ) -> None:
        if base < 0:
            raise ValueError("base must be non-negative")
        if factor < 1:
            raise ValueError("factor must be >= 1")
        if max_delay_s is not None and max_delay_s < 0:
            raise ValueError("max_delay_s must be non-negative")
        self.base = float(base)
        self.factor = float(factor)
        self.max_delay_s = max_delay_s

    def delay(self, attempt: int) -> float:
        n = max(1, attempt)
        value = self.base * (self.factor ** (n - 1))
        if self.max_delay_s is not None:
            value = min(value, self.max_delay_s)
        return value


class JitteredExponentialBackoff(ExponentialBackoff):
    """Exponential backoff with multiplicative jitter on top.

    The returned delay is ``exp * uniform(1 - jitter, 1 + jitter)`` and
    clamped to ``[0, max_delay_s]`` when ``max_delay_s`` is set. ``jitter``
    must be in ``[0, 1]``.
    """

    def __init__(
        self,
        base: float = 1.0,
        factor: float = 2.0,
        max_delay_s: float | None = None,
        jitter: float = 0.1,
        rng: random.Random | None = None,
    ) -> None:
        super().__init__(base=base, factor=factor, max_delay_s=max_delay_s)
        if not 0 <= jitter <= 1:
            raise ValueError("jitter must be in [0, 1]")
        self.jitter = float(jitter)
        self._rng = rng or random.Random()

    def delay(self, attempt: int) -> float:
        base_value = super().delay(attempt)
        spread = self._rng.uniform(1.0 - self.jitter, 1.0 + self.jitter)
        value = max(0.0, base_value * spread)
        if self.max_delay_s is not None:
            value = min(value, self.max_delay_s)
        return value


DEFAULT_RETRY_POLICY: RetryPolicy = ExponentialBackoff(base=1.0, factor=2.0)
"""Default policy preserving Pyjobkit's pre-existing ``2 ** (attempt-1)`` schedule."""


def _parse_floats(parts: list[str]) -> list[float]:
    values: list[float] = []
    for part in parts:
        try:
            values.append(float(part))
        except ValueError as exc:
            raise ValueError(f"retry policy argument must be a number, got {part!r}") from exc
    return values


def parse_policy(spec: str | RetryPolicy) -> RetryPolicy:
    """Resolve a policy spec into a :class:`RetryPolicy` instance.

    Accepted spec strings:

    * ``"fixed"`` / ``"fixed:1.5"`` - :class:`FixedDelay`
    * ``"exponential"`` / ``"exponential:1:2"`` / ``"exponential:1:2:30"``
      - :class:`ExponentialBackoff` ``(base, factor, max_delay_s)``
    * ``"exponential_jitter"`` / ``"exponential_jitter:1:2:30:0.2"``
      - :class:`JitteredExponentialBackoff`
    """

    if isinstance(spec, RetryPolicy):
        return spec
    if not isinstance(spec, str) or not spec:
        raise ValueError(f"retry policy spec must be a non-empty string, got {spec!r}")

    name, *parts = spec.split(":")
    name = name.strip().lower()
    args = _parse_floats(parts)

    if name == "fixed":
        return FixedDelay(*args) if args else FixedDelay()
    if name == "exponential":
        return ExponentialBackoff(*args)
    if name == "exponential_jitter":
        return JitteredExponentialBackoff(*args)
    raise ValueError(f"unknown retry policy: {name!r}")
