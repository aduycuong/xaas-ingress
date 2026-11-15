// POST /callbacks/:nodeRunId/:attempt: an async node (or the service it handed the URL to) reports its result.
// The ingress only records what came and wakes the run; the runner reads it and ends the node run.

import { Client } from "@upstash/workflow";
import { validCallbackSignature } from "./callback-url";
import type { Sql } from "./db";
import type { Bindings } from "./runs";

/** Largest callback body kept; a bigger one is refused with 413. */
export const MAX_CALLBACK_BYTES = 256 * 1024;
/** Headers never kept with a callback. */
const DROPPED_HEADERS = new Set(["authorization", "cookie", "proxy-authorization", "cf-connecting-ip", "x-real-ip", "x-forwarded-for", "true-client-ip"]);

export type CallbackRequest = { nodeRunId: string; attempt: number; exp: number; sig: string; body: string; contentType: string; headers: Headers };
export type CallbackAnswer = { status: 200 | 400 | 401 | 410 | 413 | 503; body: Record<string, unknown> };

/** The event the runner waits on for one attempt (`waitForEvent` in runner/src/async-node.ts). */
export const callbackEventId = (nodeRunId: string, attempt: number) => `callback-${nodeRunId}-${attempt}`;

function keptHeaders(headers: Headers): Record<string, string> {
  const kept: Record<string, string> = {};
  let count = 0;
  headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (DROPPED_HEADERS.has(lower) || count >= 50) return;
    kept[lower] = value.slice(0, 2_000);
    count++;
  });
  return kept;
}

function parsedBody(raw: string, contentType: string): unknown {
  if (!raw) return null;
  if (contentType.includes("json") || /^\s*[[{]/.test(raw)) {
    try {
      return JSON.parse(raw);
    } catch {
      return raw;
    }
  }
  return raw;
}

export async function handleCallback(sql: Sql, env: Bindings, request: CallbackRequest): Promise<CallbackAnswer> {
  if (!env.CALLBACK_SIGNING_KEY) return { status: 503, body: { error: "callbacks_not_configured" } };
  if (!(await validCallbackSignature(env.CALLBACK_SIGNING_KEY, request.nodeRunId, request.attempt, request.exp, request.sig))) {
    return { status: 401, body: { error: "invalid_signature" } };
  }
  if (request.exp * 1000 < Date.now()) return { status: 410, body: { error: "expired" } };
  if (request.body.length > MAX_CALLBACK_BYTES) return { status: 413, body: { error: "payload_too_large" } };

  const received = { body: parsedBody(request.body, request.contentType), headers: keptHeaders(request.headers) };
  // Recorded once, and only for the attempt the run is on: not ended, the latest of a running node run. Not only after the
  // 202: a node may call back before the runner has recorded its answer (the attempt row exists before the node is called).
  const [row] = await sql`
    with target as (
      select a.id, a.callback_at, r.id as run_id, r.engine_run_id
      from attempts a
      join node_runs nr on nr.id = a.node_run_id
      join runs r on r.id = nr.run_id
      where a.node_run_id = ${request.nodeRunId} and a.attempt = ${request.attempt}
        and a.finished_at is null
        and nr.status = 'running' and nr.attempt_count = a.attempt and r.status = 'running'
    ), written as (
      update attempts set callback_at = now(), callback = ${JSON.stringify(received)}::jsonb
      where id = (select id from target) and callback_at is null
      returning id
    )
    select t.run_id, t.engine_run_id, (select id from written) is not null as written from target t`;
  if (!row) return { status: 200, body: { ignored: "not_waiting" } };

  // Wakes the run. With `workflowRunId` Upstash keeps the event for the run even if it is not waiting yet.
  try {
    const client = new Client({ token: env.QSTASH_TOKEN, baseUrl: env.QSTASH_URL || undefined });
    await client.notify({ eventId: callbackEventId(request.nodeRunId, request.attempt), eventData: { attempt: request.attempt }, workflowRunId: row.engine_run_id ?? row.run_id });
  } catch (error) {
    console.warn("[callbacks] notify failed", error instanceof Error ? error.message : error);
    // The callback is recorded: a retry of the same call only wakes the run again.
    return { status: 503, body: { error: "notify_failed" } };
  }
  return { status: 200, body: row.written ? { received: true } : { received: true, duplicate: true } };
}
