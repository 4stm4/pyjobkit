# Retry policies

A retry policy tells the worker how long to wait before requeueing a
job that failed (or timed out) and when to give up. Policies live in
`pyjobkit.retry`; the three built-ins cover virtually every case:

| Class | `delay(attempt)` |
| --- | --- |
| `FixedDelay(d)` | `d` (constant) |
| `ExponentialBackoff(base, factor, max_delay_s)` | `base * factor**(attempt-1)` clamped to `max_delay_s` |
| `JitteredExponentialBackoff(base, factor, max_delay_s, jitter)` | exponential * `uniform(1-j, 1+j)` |

`attempt` is 1-indexed and corresponds to the attempt that just
failed; the next try is `attempt + 1`.

## Per-worker default

```python
from pyjobkit import (
    Worker,
    FixedDelay,
    ExponentialBackoff,
    JitteredExponentialBackoff,
)

worker = Worker(engine, retry_policy=ExponentialBackoff(base=1, factor=2))
worker = Worker(engine, retry_policy=JitteredExponentialBackoff(1, 2, 60, 0.1))
worker = Worker(engine, retry_policy="exponential:1:2")            # spec string
worker = Worker(engine, retry_policy="exponential_jitter:1:2:60:0.1")
```

`Worker(retry_policy=None)` uses `ExponentialBackoff(1, 2)` to match
Pyjobkit's pre-1.0 hardcoded behaviour.

## Per-job override

Spec strings only - retry policies cannot be pickled into the queue.

```python
await engine.enqueue(
    kind="email",
    payload={"to": "..."},
    retry_policy="fixed:5",
)
```

The marker rides as the `__pjk_retry_policy` payload key and is
stripped before the executor sees the payload. An invalid spec is
logged and the worker falls back to its default.

## Wall-clock cap (give_up_after_age_s)

Sometimes `max_attempts` is not enough - `exponential:1:2` with 20
attempts stretches across nearly a week. Set `give_up_after_age_s`
to bound the *time* a job spends being retried:

```python
ExponentialBackoff(base=1, factor=2, max_delay_s=300,
                   give_up_after_age_s=3600)            # 1 hour

# or:
parse_policy("exponential:1:2:300:give_up_after_age_s=3600")
```

The worker computes the job's age from `row["created_at"]`. When the
policy's `should_give_up(age)` returns True the worker finalises the
job as `failed` (or `timeout`, depending on which exception kicked
the retry path) with `"reason": "age_exhausted"` in the log event.

## Spec syntax

Used by:

- `Worker(retry_policy="...")`,
- `Engine.enqueue(retry_policy="...")`,
- `.pyjobkit.toml`'s `retry_policy = "..."`,
- the CLI's `--retry-policy "..."`.

Grammar:

```
<name>(":"<arg>)*
```

Positional arguments map onto the class signature in order. A
suffix of the form `key=value` becomes a keyword argument.

Examples:

```
fixed                 # FixedDelay()           (default 1 second)
fixed:0.5             # FixedDelay(0.5)
exponential           # ExponentialBackoff()   (1, 2)
exponential:1:2:30    # ExponentialBackoff(1, 2, max_delay_s=30)
exponential_jitter:1:2:30:0.1   # JitteredExponentialBackoff(1, 2, 30, 0.1)
exponential:1:2:give_up_after_age_s=3600
```

`parse_policy` accepts either a string or a pre-built instance and
returns the resolved object.

## Writing a custom policy

Subclass `RetryPolicy` and implement `delay(attempt) -> float`.
Optionally set `give_up_after_age_s` as a class or instance
attribute; the default `should_give_up` honours it automatically.

```python
from pyjobkit.retry import RetryPolicy

class CircuitBreaker(RetryPolicy):
    give_up_after_age_s = 600

    def delay(self, attempt: int) -> float:
        if attempt > 5:
            return 60        # cooldown
        return 1 + attempt * 0.5

worker = Worker(engine, retry_policy=CircuitBreaker())
```

Custom policies cannot be used per-job (they would not survive
serialisation into the queue). Configure them on the worker.
