// The result of a run as the caller of an API trigger sees it: the answer of `GET /wh/:token/runs/:runId` and the body of
// the result callback (`run_callbacks` of the app), POSTed through QStash when the run ends, signed with the trigger's secret.

import { Client } from "@upstash/qstash";
import { signBody } from "./crypto";
import { triggerSecret } from "./fire";
import type { Bindings, Sql } from "./runs";

export const ENDED_STATUSES = ["succeeded", "failed", "cancelled"] as const;

/** Attempts QStash makes after the first one; with its backoff they span a few minutes. */
export const CALLBACK_RETRIES = 3;

/** Largest output sent inside a callback; a larger one is left out (`outputOmitted`) and read with `statusUrl`. */
export const MAX_CALLBACK_OUTPUT_BYTES = 64 * 1024;

/** How long one delivery waits for the receiver to answer. */
const CALLBACK_TIMEOUT_MS = 15_000;

/**
 * One node the run reached. `id` is the node's id in the graph of the run's version (`definition.nodes[].id`, as the
 * integration guide of that version lists it), `label` its name there. Nodes the run has not reached (or never will, on a
 * branch not taken) are not listed. No output: only the run's own `output` leaves XAAS.
 */
export type RunNode = {
  id: string;
  label: string | null;
  type: string;
  status: "running" | "succeeded" | "warning" | "failed" | "skipped" | "cancelled";
  startedAt: string;
  finishedAt: string | null;
  error: { code: string; message: string } | null;
};

/** A node the run is at now: running (several when branches run side by side). */
export type CurrentNode = { id: string; label: string | null; key: string | null; since: string };

/** What a running run waits for: a person (Ask human) or an async node (a service that reports back). */
export type RunWaiting = { kind: "human" | "async"; node: string } | null;

export type RunResult = {
  runId: string;
  no: number;
  /** Version of the workflow the run uses: its integration guide names the same node ids. */
  version: number;
  status: "queued" | "running" | "succeeded" | "failed" | "cancelled";
  env: string;
  createdAt: string;
  startedAt: string | null;
  finishedAt: string | null;
  /** What the workflow returned; null until it succeeds. */
  output: unknown;
  error: { code: string; message: string } | null;
  nodes: RunNode[];
  /** The nodes running now; empty once the run has ended (or before it starts). No progress or percent: loops make them wrong. */
  current: CurrentNode[];
  /** What the run waits for now, or null. */
  waiting: RunWaiting;
  statusUrl: string;
};

type StoredError = { code?: string; message?: string } | null;
type RunRow = { id: string; no: number; version: number; status: RunResult["status"]; env: string; created_at: Date | string; started_at: Date | string | null; finished_at: Date | string | null; output: unknown; error: StoredError };
type NodeRow = { node_id: string; label: string | null; node_type: string; status: RunNode["status"]; started_at: Date | string; finished_at: Date | string | null; error: StoredError };

const publicError = (error: StoredError) => (error ? { code: error.code ?? "ERROR", message: error.message ?? "" } : null);

export function toRunNode(row: NodeRow): RunNode {
  return { id: row.node_id, label: row.label, type: row.node_type, status: row.status, startedAt: iso(row.started_at) as string, finishedAt: iso(row.finished_at), error: publicError(row.error) };
}

/** The nodes a run has reached, in the order they started. */
async function loadRunNodes(sql: Sql, runId: string): Promise<RunNode[]> {
  const rows = await sql`select node_id, label, node_type, status, started_at, finished_at, error from node_runs where run_id = ${runId}::uuid order by started_at, node_id`;
  return (rows as NodeRow[]).map(toRunNode);
}

type RunningRow = { node_id: string; label: string | null; started_at: Date | string; key: string | null; mode: string | null };

/**
 * Where a run is now, from its running node runs: `current` lists them; `waiting` is the first one of an async node
 * (`node_versions.mode`), a person for `human.*` nodes. Pure, so the rules are testable.
 */
export function whereRunIs(running: RunningRow[]): { current: CurrentNode[]; waiting: RunWaiting } {
  const current = running.map((row) => ({ id: row.node_id, label: row.label, key: row.key, since: iso(row.started_at) as string }));
  const waits = running.find((row) => row.mode === "async");
  return { current, waiting: waits ? { kind: waits.key?.startsWith("human.") ? "human" : "async", node: waits.node_id } : null };
}

async function loadWhereRunIs(sql: Sql, runId: string): Promise<{ current: CurrentNode[]; waiting: RunWaiting }> {
  const rows = await sql`
    select nr.node_id, nr.label, nr.started_at, n.key, nv.mode
    from node_runs nr left join node_versions nv on nv.id = nr.node_version_id left join nodes n on n.id = nv.node_id
    where nr.run_id = ${runId}::uuid and nr.status = 'running' order by nr.started_at, nr.node_id`;
  return whereRunIs(rows as RunningRow[]);
}

const iso = (value: Date | string | null) => (value === null ? null : new Date(value).toISOString());

export const isEnded = (status: string) => (ENDED_STATUSES as readonly string[]).includes(status);

export const statusUrlOf = (origin: string, token: string, runId: string) => `${origin.replace(/\/$/, "")}/wh/${token}/runs/${runId}`;

export function toRunResult(row: RunRow, nodes: RunNode[], statusUrl: string, now: { current: CurrentNode[]; waiting: RunWaiting } = { current: [], waiting: null }): RunResult {
  const running = row.status === "running";
  return {
    runId: row.id,
    no: Number(row.no),
    version: Number(row.version),
    status: row.status,
    env: row.env,
    createdAt: iso(row.created_at) as string,
    startedAt: iso(row.started_at),
    finishedAt: iso(row.finished_at),
    output: row.status === "succeeded" ? (row.output ?? null) : null,
    error: publicError(row.error),
    nodes,
    current: running ? now.current : [],
    waiting: running ? now.waiting : null,
    statusUrl,
  };
}

/** The body of a result callback. An output larger than `MAX_CALLBACK_OUTPUT_BYTES` is left out. */
export function callbackBody(result: RunResult, event: string): string {
  const full = JSON.stringify({ event, ...result });
  if (new TextEncoder().encode(JSON.stringify(result.output)).length <= MAX_CALLBACK_OUTPUT_BYTES) return full;
  return JSON.stringify({ event, ...result, output: null, outputOmitted: true });
}

/** A real run of this trigger (test runs and runs of other triggers are not found), as its caller sees it. */
export async function loadRunResult(sql: Sql, params: { triggerId: string; runId: string; statusUrl: string }): Promise<RunResult | null> {
  const [row] = await sql`
    select r.id, r.no, v.version, r.status, r.env, r.created_at, r.started_at, r.finished_at, r.output, r.error
    from runs r join workflow_versions v on v.id = r.workflow_version_id
    where r.id = ${params.runId}::uuid and r.trigger_id = ${params.triggerId}::uuid and r.test is null`;
  if (!row) return null;
  const [nodes, now] = await Promise.all([loadRunNodes(sql, params.runId), row.status === "running" ? loadWhereRunIs(sql, params.runId) : Promise.resolve({ current: [], waiting: null })]);
  return toRunResult(row as RunRow, nodes, params.statusUrl, now);
}

/**
 * Waits until the run has ended or `seconds` passed, reading its status once a second, then answers its result.
 * `seconds` 0 reads once.
 */
export async function waitForRunResult(sql: Sql, params: { triggerId: string; runId: string; statusUrl: string; seconds: number }): Promise<RunResult | null> {
  const deadline = Date.now() + params.seconds * 1000;
  while (Date.now() + 1000 <= deadline) {
    const [row] = await sql`select status from runs where id = ${params.runId}::uuid and trigger_id = ${params.triggerId}::uuid and test is null`;
    if (!row || isEnded(String(row.status))) break;
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
  return loadRunResult(sql, params);
}

/**
 * The run has ended: hands its result callback to QStash, once (`waiting` → `sending`). Nothing when the run has no
 * callback, has not ended, or was already handed over. Returns whether a callback was sent.
 */
export async function scheduleCallback(sql: Sql, env: Bindings, params: { runId: string; ingressOrigin: string }): Promise<boolean> {
  const [claimed] = await sql`
    update run_callbacks c set status = 'sending', event = 'run.' || r.status
    from runs r
    where c.run_id = ${params.runId}::uuid and r.id = c.run_id and c.status = 'waiting' and r.status in ('succeeded', 'failed', 'cancelled')
    returning c.id`;
  if (!claimed) return false;
  try {
    await new Client({ token: env.QSTASH_TOKEN, baseUrl: env.QSTASH_URL || undefined }).publishJSON({
      url: `${params.ingressOrigin.replace(/\/$/, "")}/internal/callbacks/deliver`,
      body: { callbackId: claimed.id },
      retries: CALLBACK_RETRIES,
      deduplicationId: `run-callback-${claimed.id}`,
    });
  } catch (error) {
    // Back to waiting, so the retry of the run-finished message (or the next one) hands it over again.
    await sql`update run_callbacks set status = 'waiting', event = null where id = ${claimed.id}::uuid`;
    throw error;
  }
  return true;
}

export type DeliveryAnswer = { status: 200 | 502; body: Record<string, unknown> };

/**
 * One attempt to POST a result to its URL, signed afresh (so a retry minutes later still passes the receiver's
 * timestamp check). A 2xx answer delivers it; otherwise 502 makes QStash try again, until its last retry (`retried`).
 */
export async function deliverCallback(sql: Sql, env: Bindings, params: { callbackId: string; ingressOrigin: string; retried: number; now?: number }): Promise<DeliveryAnswer> {
  const [row] = await sql`
    select c.id, c.url, c.status as callback_status, c.event, t.auth_secret, t.webhook_token,
      r.id as run_id, r.no, v.version, r.status, r.env, r.created_at, r.started_at, r.finished_at, r.output, r.error
    from run_callbacks c join runs r on r.id = c.run_id join workflow_versions v on v.id = r.workflow_version_id
      left join triggers t on t.id = c.trigger_id
    where c.id = ${params.callbackId}::uuid`;
  if (!row || row.callback_status !== "sending") return { status: 200, body: { skipped: row ? row.callback_status : "not_found" } };

  const finalAttempt = params.retried >= CALLBACK_RETRIES;
  const record = async (outcome: { ok: boolean; statusCode: number | null; error: string | null }) => {
    const status = outcome.ok ? "delivered" : finalAttempt ? "failed" : "sending";
    await sql`
      update run_callbacks set attempts = attempts + 1, status = ${status}, last_status_code = ${outcome.statusCode}, last_error = ${outcome.error},
        delivered_at = case when ${outcome.ok} then now() else delivered_at end
      where id = ${params.callbackId}::uuid`;
    return status;
  };

  const secret = row.auth_secret ? await triggerSecret(env, { auth_secret: row.auth_secret as string }) : null;
  if (!secret || !row.webhook_token) {
    // The trigger was deleted or lost its secret: an unsigned result would not be trusted, so it is not sent.
    await sql`update run_callbacks set status = 'failed', last_error = 'The trigger no longer exists or has no secret.' where id = ${params.callbackId}::uuid`;
    return { status: 200, body: { status: "failed" } };
  }

  const result = toRunResult({ ...row, id: row.run_id } as RunRow, await loadRunNodes(sql, row.run_id), statusUrlOf(params.ingressOrigin, row.webhook_token, row.run_id));
  const body = callbackBody(result, row.event ?? `run.${row.status}`);
  const timestamp = String(Math.floor((params.now ?? Date.now()) / 1000));
  const headers = {
    "Content-Type": "application/json",
    "User-Agent": "XAAS-Workflows-Callback/1",
    "X-Xaas-Event": row.event ?? `run.${row.status}`,
    "X-Xaas-Delivery-Id": row.id,
    "X-Xaas-Run-Id": row.run_id,
    "X-Xaas-Timestamp": timestamp,
    "X-Xaas-Signature": await signBody(secret, `${timestamp}.${body}`),
  };

  let outcome: { ok: boolean; statusCode: number | null; error: string | null };
  try {
    // No redirects: the URL was checked against the allowed origins, a redirect could lead anywhere.
    const response = await fetch(row.url, { method: "POST", headers, body, redirect: "manual", signal: AbortSignal.timeout(CALLBACK_TIMEOUT_MS) });
    const ok = response.status >= 200 && response.status < 300;
    outcome = { ok, statusCode: response.status, error: ok ? null : `The URL answered ${response.status}.` };
    await response.body?.cancel();
  } catch (error) {
    outcome = { ok: false, statusCode: null, error: error instanceof Error ? error.message.slice(0, 300) : "The URL could not be reached." };
  }
  const status = await record(outcome);
  return status === "sending" ? { status: 502, body: { status, error: outcome.error } } : { status: 200, body: { status } };
}
