# Rate limiting

Per-`kind` rate limits are enforced inside the worker via a token
bucket. They run *after* a job has been claimed (so other workers
remain free to handle other kinds) and *before* `mark_running`, so
the throttle does not consume executor concurrency slots.

## Configuration

Via worker constructor:

```python
worker = Worker(engine, rate_limits={
    "http":  {"max_per_second": 5, "burst": 10},
    "email": {"max_per_second": 1},                # burst defaults to rate
})
```

Via TOML:

```toml
[pyjobkit.rate_limits]
http  = { max_per_second = 5,  burst = 10 }
email = { max_per_second = 1 }
```

Via env / CLI (comma-separated, `kind:rate[:burst]` per entry):

```bash
export PYJOBKIT_RATE_LIMITS="http:5:10,email:2"
# or, repeatable:
pyjobkit --rate-limit http:5:10 --rate-limit email:2
```

`parse_rate_limits` accepts a few input shapes for convenience:

```python
parse_rate_limits({"http": 5})                 # rate=5, burst=5
parse_rate_limits({"http": (5, 10)})           # rate=5, burst=10
parse_rate_limits({"http": [5, 10]})           # rate=5, burst=10
parse_rate_limits({"http": {"max_per_second": 5, "burst": 10}})
```

## Behaviour

The bucket starts full (`burst` tokens). Each job execution consumes
one token; tokens regenerate at `max_per_second / sec`. When the
bucket is empty, the worker `await`s the wait under the bucket's
internal lock - jobs queue up in FIFO order behind the bucket
without blocking the rest of the worker.

```python
from pyjobkit.ratelimit import TokenBucket

bucket = TokenBucket(max_per_second=10, burst=5)
await bucket.acquire()        # consumes one token, blocks if empty
await bucket.acquire(2)       # consumes two tokens at once
```

`bucket.acquire(tokens)` raises `ValueError` when `tokens >
capacity` - the request would otherwise wait forever.

## Reading limits from the worker

```python
worker.rate_limits    # {"http": (5.0, 10.0), ...}  - normalised
worker._buckets       # internal, bucket objects per kind
```

`rate_limits` is the normalised dict that `parse_rate_limits`
produced.

## When **not** to use this

Rate limiting at the executor side caps **throughput**, not
**latency**. A long-running job still ties up an executor slot. If
your goal is "don't hammer the downstream API more than 5 RPS but
finish quickly when you can", rate limiting is correct. If the goal
is "never wait more than 2 seconds in the queue", look at
`Worker(queue_capacity=...)` plus monitoring instead.

For *global* (cross-worker) rate limits use a coordinated bucket
backed by Redis - the in-process bucket here is per-worker. With
N workers and `max_per_second=5` you get up to `5 * N` per second
collectively.
