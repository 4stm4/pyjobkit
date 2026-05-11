# Cancellation semantics

`Engine.cancel(job_id)` is **best-effort and asynchronous**. The
contract is:

- If the job is still `queued`, it transitions to `cancelled` and the
  worker will never run it.
- If the job is already `running`, the backend marks
  ``cancel_requested = true``. The worker polls
  ``ctx.is_cancelled()`` once per ``poll_interval`` seconds (default
  500 ms). When the flag is observed the worker cancels the executor
  task. Executors are encouraged to call ``ctx.is_cancelled()`` at
  natural checkpoints (between iterations, between HTTP calls).
- If the job has already finished (``success`` / ``failed`` /
  ``timeout``), the cancel call is a no-op and the stored state is
  unchanged.

In practice this means there is a small window - bounded by
``poll_interval`` plus the time the executor takes to reach its next
checkpoint - during which cancellation has been requested but the job
is still executing. Two real consequences:

1. **Side effects may still happen.** A job that submits a payment and
   is cancelled milliseconds later may still complete the payment. If
   the executor needs strong cancellation guarantees, make the work
   idempotent or wrap it in a transaction it can roll back from
   ``CancelledError``.
2. **The terminal state may be ``success`` rather than ``cancelled``.**
   If the executor returns a result before the cancel signal is
   observed, the worker takes the success path; afterwards it sees the
   cancel flag and converts the state to ``cancelled`` via
   ``Engine.cancel``. The stored ``result`` in that case is
   ``{"error": "cancelled"}``.

## Cooperative cancellation pattern

```python
class LongRunningExecutor(Executor):
    kind = "long_running"

    async def run(self, *, job_id, payload, ctx):
        for i, item in enumerate(payload["items"]):
            if await ctx.is_cancelled():
                # Persist partial state so retries can resume.
                return {"processed": i, "cancelled": True}
            await process(item)
        return {"processed": len(payload["items"])}
```

## Hard cancellation

To force-cancel a running job - bypassing the cooperative dance -
revoke its lease and let the watchdog reclaim it:

```python
# Mark cancellation requested.
await engine.cancel(job_id)
# Optional: revoke the lease so the watchdog finalises it as failed
# even if the executor never checks ctx.is_cancelled().
await engine.backend.fail(job_id, {"error": "force_cancelled"})
```

The cooperative path is strongly preferred for normal use.
