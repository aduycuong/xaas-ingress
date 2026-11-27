// The result of a run as its caller sees it: the answer of `GET /wh/:token/runs/:runId`, and the `run` of the events
// status callbacks receive (`deliveries.ts`).

import type { Sql } from "./runs";

export const ENDED_STATUSES = ["succeeded", "failed", "cancelled"] as const;

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
  /** `GET` URL of the run for API and webhook triggers (`/wh/<token>/runs/<id>`); null for runs of other triggers. */
  statusUrl: string | null;
};

type StoredError = { code?: string; message?: string } | null;
export type RunRow = { id: string; no: number; version: number; status: RunResult["status"]; env: string; created_at: Date | string; started_at: Date | string | null; finished_at: Date | string | null; output: unknown; error: StoredError };
type NodeRow = { node_id: string; label: string | null; node_type: string; status: RunNode["status"]; started_at: Date | string; finished_at: Date | string | null; error: StoredError };

const publicError = (error: StoredError) => (error ? { code: error.code ?? "ERROR", message: error.message ?? "" } : null);

export function toRunNode(row: NodeRow): RunNode {
  return { id: row.node_id, label: row.label, type: row.node_type, status: row.status, startedAt: iso(row.started_at) as string, finishedAt: iso(row.finished_at), error: publicError(row.error) };
}

/** The nodes a run has reached, in the order they started. */
export async function loadRunNodes(sql: Sql, runId: string): Promise<RunNode[]> {
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

export async function loadWhereRunIs(sql: Sql, runId: string): Promise<{ current: CurrentNode[]; waiting: RunWaiting }> {
  const rows = await sql`
    select nr.node_id, nr.label, nr.started_at, n.key, nv.mode
    from node_runs nr left join node_versions nv on nv.id = nr.node_version_id left join nodes n on n.id = nv.node_id
    where nr.run_id = ${runId}::uuid and nr.status = 'running' order by nr.started_at, nr.node_id`;
  return whereRunIs(rows as RunningRow[]);
}

const iso = (value: Date | string | null) => (value === null ? null : new Date(value).toISOString());

export const isEnded = (status: string) => (ENDED_STATUSES as readonly string[]).includes(status);

export const statusUrlOf = (origin: string, token: string, runId: string) => `${origin.replace(/\/$/, "")}/wh/${token}/runs/${runId}`;

export function toRunResult(row: RunRow, nodes: RunNode[], statusUrl: string | null, now: { current: CurrentNode[]; waiting: RunWaiting } = { current: [], waiting: null }): RunResult {
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
