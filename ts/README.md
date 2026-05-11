# Pyjobkit TypeScript / JavaScript client

This folder ships type declarations and a tiny client for the
[Pyjobkit REST API](../docs/api.md). They are intended for two
audiences:

1. **Application integrations** - drop `pyjobkit.d.ts` next to your
   own fetch wrappers to get full IntelliSense for the request /
   response shapes.
2. **Web UI** - the [bundled dashboard](../pyjobkit/integrations/ui.py)
   uses the same shapes; this folder mirrors them in TypeScript for
   anyone who wants to build their own UI on top of the REST router.

## Files

| File | Purpose |
| --- | --- |
| `pyjobkit.d.ts` | Type declarations for `JobRecord`, `EnqueueRequest`, `PyjobkitClient`, etc. |
| `pyjobkit.js` | Minimal `fetch`-based client implementing the declared interface. |

## Usage

```ts
import { createClient, JobStatus } from "./pyjobkit";

const client = createClient("/api/v1");

const { job_id } = await client.enqueue({
  kind: "echo",
  payload: { greeting: "hi" },
  max_attempts: 3,
});

const status: JobStatus | undefined = (await client.get(job_id)).status;
```

The client has no runtime dependencies and works in browsers, modern
Node, and Deno.
