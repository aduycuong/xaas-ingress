// Creating runs and handing them to the runner. A trigger's concurrency policy is applied here,
// under a per-trigger advisory lock, so two calls at the same moment cannot both take the last slot.

import { type NeonQueryFunction } from "@neondatabase/serverless";
import { Client } from "@upstash/workflow";

export type Sql = NeonQueryFunction<false, false>;

export type Bindings = {
  DATABASE_URL: string;
  QSTASH_TOKEN: string;
  QSTASH_URL?: string;
  QSTASH_CURRENT_SIGNING_KEY?: string;
  QSTASH_NEXT_SIGNING_KEY?: string;
  /** Public URL of the runner Worker, without a trailing slash. */
  RUNNER_URL: string;
  /** Shared with the xaas-workflows app for `POST /runs` and the internal calls the app makes. */
  INGRESS_API_TOKEN?: string;
  /** Optional second token, accepted next to the first while the token is rotated: set it, switch the app, then move it into INGRESS_API_TOKEN. */
  INGRESS_API_TOKEN_NEXT?: string;
  /** Key that decrypts the secrets of webhook triggers (same value as the app's TRIGGERS_ENCRYPTION_KEY). */
  TRIGGERS_ENCRYPTION_KEY?: string;
};

/** Route of the runner. A runner change that reorders or renames steps comes with a new route. */
const RUNNER_ROUTE = "/run/v3";

export function uuidv7(now: number = Date.now()): string {
  const bytes = new Uint8Array(16);
  crypto.getRandomValues(bytes);
  bytes[0] = Math.floor(now / 0x10000000000) & 0xff;
  bytes[1] = Math.floor(now / 0x100000000) & 0xff;
  bytes[2] = Math.floor(now / 0x1000000) & 0xff;
  bytes[3] = Math.floor(now / 0x10000) & 0xff;
  bytes[4] = Math.floor(now / 0x100) & 0xff;
  bytes[5] = now & 0xff;
  bytes[6] = (bytes[6] & 0x0f) | 0x70;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, "0")).join("");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export type NewRun = {
  workspaceId: string;
  workflowId: string;
  workflowVersionId: string;
  triggerId: string | null;
  triggerType: "cron" | "webhook" | "manual" | "event" | "retry";
  input: Record<string, unknown>;
  triggeredBy?: string | null;
  retryOf?: string | null;
  causeRunId?: string | null;
  env?: "production" | "staging";
  /** The trigger's policy. Without it (a run started from the app) the run takes no slot and starts at once. */
  concurrency?: { triggerId: string; policy: "parallel" | "skip" | "queue"; max: number };
};

export type InsertedRun = { runId: string; no: number; dispatched: boolean };

/**
 * Creates the run row. With a concurrency policy the slot is taken in the same statement: a run
 * that finds the trigger full is dropped (`skip`, returns null) or created waiting (`dispatched`
 * false, no engine call yet). The run number comes from the workspace counter in the same statement.
 */
export async function insertRun(sql: Sql, run: NewRun): Promise<InsertedRun | null> {
  const runId = uuidv7();
  const policy = run.concurrency?.policy ?? "parallel";
  const max = run.concurrency?.max ?? 1_000_000;
  const triggerId = run.concurrency?.triggerId ?? run.triggerId;

  const [, inserted] = await sql.transaction([
    // Held until the transaction ends: the count below and the insert cannot interleave with another call of this trigger.
    sql`select pg_advisory_xact_lock(hashtext(${triggerId ?? runId}))`,
    sql`
      with slots as (
        select count(*)::int as c from runs
        where trigger_id = ${triggerId}::uuid and dispatched_at is not null and status in ('queued', 'running')
      ), ok as (
        select 1 where ${policy} <> 'skip' or (select c from slots) < ${max}::int
      ), counter as (
        update workspaces set run_counter = run_counter + 1 where id = ${run.workspaceId} and exists (select 1 from ok) returning run_counter
      )
      insert into runs (id, no, workspace_id, workflow_id, workflow_version_id, trigger_id, trigger_type, input, triggered_by, retry_of, cause_run_id, env, dispatched_at)
      select ${runId}, counter.run_counter, ${run.workspaceId}, ${run.workflowId}, ${run.workflowVersionId}, ${run.triggerId}::uuid,
        ${run.triggerType}, ${JSON.stringify(run.input)}::jsonb, ${run.triggeredBy ?? null}::uuid, ${run.retryOf ?? null}::uuid, ${run.causeRunId ?? null}::uuid,
        ${run.env ?? "production"}, case when (select c from slots) < ${max}::int then now() end
      from counter
      returning id, no, dispatched_at`,
  ]);
  const row = inserted[0] as { id: string; no: number; dispatched_at: string | null } | undefined;
  return row ? { runId: row.id, no: Number(row.no), dispatched: row.dispatched_at !== null } : null;
}

/** Starts the run on Upstash. If that fails the run is marked failed and the error is thrown. */
export async function handToRunner(sql: Sql, env: Bindings, runId: string): Promise<void> {
  try {
    const client = new Client({ token: env.QSTASH_TOKEN, baseUrl: env.QSTASH_URL || undefined });
    const { workflowRunId } = await client.trigger({
      url: `${env.RUNNER_URL.replace(/\/$/, "")}${RUNNER_ROUTE}`,
      workflowRunId: runId,
      body: { runId },
    });
    await sql`update runs set engine_run_id = ${workflowRunId} where id = ${runId}`;
  } catch (error) {
    const failure = JSON.stringify({ code: "TRIGGER_FAILED", message: "The run could not be handed to the runner." });
    await sql`update runs set status = 'failed', error = ${failure}::jsonb, finished_at = now() where id = ${runId}`;
    throw error;
  }
}

/** Creates a run and, when it got a slot, hands it to the runner. */
export async function startRun(sql: Sql, env: Bindings, run: NewRun): Promise<InsertedRun | null> {
  const inserted = await insertRun(sql, run);
  if (inserted?.dispatched) await handToRunner(sql, env, inserted.runId);
  return inserted;
}

/**
 * Gives free slots of a trigger to the runs waiting for them, oldest first. Called when a run of
 * the trigger ends and by a sweep that catches what a missed call left behind. Returns how many started.
 */
export async function dispatchQueued(sql: Sql, env: Bindings, triggerId: string): Promise<number> {
  let started = 0;
  for (let round = 0; round < 50; round++) {
    const [, claimed] = await sql.transaction([
      sql`select pg_advisory_xact_lock(hashtext(${triggerId}))`,
      sql`
        with next as (
          select id from runs where trigger_id = ${triggerId}::uuid and status = 'queued' and dispatched_at is null
          order by created_at asc limit 1 for update skip locked
        ), slots as (
          select count(*)::int as c from runs where trigger_id = ${triggerId}::uuid and dispatched_at is not null and status in ('queued', 'running')
        )
        update runs set dispatched_at = now()
        where id = (select id from next) and (select c from slots) < (select concurrency_max from triggers where id = ${triggerId}::uuid)
        returning id`,
    ]);
    const row = claimed[0] as { id: string } | undefined;
    if (!row) break;
    await handToRunner(sql, env, row.id);
    started++;
  }
  return started;
}
