// TypeScript declarations for the Pyjobkit REST API (#71).
//
// These types mirror the shapes returned by the FastAPI router exposed
// at `pyjobkit.integrations.fastapi.make_router`. Use them with the
// generated OpenAPI client of your choice, or hand-roll fetch wrappers
// like the example below.

export type JobStatus =
  | "queued"
  | "running"
  | "success"
  | "failed"
  | "cancelled"
  | "timeout";

export interface JobRecord {
  id: string;
  kind?: string;
  status?: JobStatus;
  payload?: Record<string, unknown>;
  result?: Record<string, unknown> | null;
  attempts?: number;
  max_attempts?: number;
  priority?: number;
  scheduled_for?: string | null;
  created_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
}

export interface EnqueueRequest {
  kind: string;
  payload?: Record<string, unknown>;
  priority?: number;
  max_attempts?: number;
  scheduled_for?: string | null;
  timeout_s?: number | null;
  idempotency_key?: string | null;
  tags?: string[] | null;
  shadow?: boolean;
  retry_policy?: string | null;
  webhooks?: {
    complete?: string;
    fail?: string;
    timeout?: string;
  };
}

export interface EnqueueResponse {
  job_id: string;
}

export interface PyjobkitClient {
  enqueue(req: EnqueueRequest): Promise<EnqueueResponse>;
  get(jobId: string): Promise<JobRecord>;
  list(opts?: { status?: JobStatus; limit?: number }): Promise<JobRecord[]>;
  cancel(jobId: string): Promise<void>;
  healthz(): Promise<{ status: "ok" }>;
}

/**
 * Minimal Pyjobkit REST client built on the global `fetch`.
 *
 * @example
 *   import { createClient } from "./pyjobkit";
 *   const pyjobkit = createClient("/api/v1");
 *   const { job_id } = await pyjobkit.enqueue({ kind: "echo", payload: { x: 1 } });
 */
export declare function createClient(baseUrl: string): PyjobkitClient;
