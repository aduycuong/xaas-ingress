// Creating runs and handing them to the runner. A workflow's concurrency (milestone 19 of the app: `max_active_runs`,
// `queue_cap`, and the trigger's `when_full`) is applied here, under a per-workflow advisory lock, so two calls at the
// same moment cannot both take the last slot. Runs of every trigger and of the Run button share the slots; editor tests do not.

import { Client } from "@upstash/workflow";
import type { Sql } from "./db";

export type { Sql };

export type Bindings = {
  /** Direct database URL; not needed when the HYPERDRIVE binding is set. */
  DATABASE_URL?: string;
  /** Optional Hyperdrive in front of the database: warm connections near the Worker. */
  HYPERDRIVE?: Hyperdrive;
  QSTASH_TOKEN: string;
  QSTASH_URL?: string;
  QSTASH_CURRENT_SIGNING_KEY?: string;
  QSTASH_NEXT_SIGNING_KEY?: string;
  /** Signs the callback URLs of async nodes (the runner makes them, `POST /callbacks/...` checks them); same value as the runner. */
  CALLBACK_SIGNING_KEY?: string;
  /** Public URL of the runner Worker, without a trailing slash. */
  RUNNER_URL: string;
  /** Shared with the xaas-workflows app for `POST /runs` and the internal calls the app makes. */
  INGRESS_API_TOKEN?: string;
  /** Optional second token, accepted next to the first while the token is rotated: set it, switch the app, then move it into INGRESS_API_TOKEN. */
  INGRESS_API_TOKEN_NEXT?: string;
  /** Key that decrypts the secrets of webhook triggers (same value as the app's TRIGGERS_ENCRYPTION_KEY). */
  TRIGGERS_ENCRYPTION_KEY?: string;
  /** Facebook app of the xaas-workflows app (same values): checks `X-Hub-Signature-256` of Page webhooks, tells XAAS's own Messenger echoes. */
  FACEBOOK_APP_ID?: string;
  FACEBOOK_APP_SECRET?: string;
  /** What the Webhooks product of the Facebook app sends to verify `GET /hooks/facebook` (set the same text there). */
  FACEBOOK_WEBHOOK_VERIFY_TOKEN?: string;
};

/** Route of the runner. A runner change that reorders or renames steps comes with a new route. */
const RUNNER_ROUTE = "/run/v4";

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
  triggerType: "cron" | "webhook" | "api" | "event" | "manual" | "retry";
  input: Record<string, unknown>;
  /** The raw call that started the run (`{{ trigger… }}`); none for a run started from the app. */
  triggerData?: Record<string, unknown> | null;
  triggeredBy?: string | null;
  retryOf?: string | null;
  causeRunId?: string | null;
  /** The workspace file that fired it (`file.*` trigger). */
  causeFileId?: string | null;
  /** An environment of the workspace (`environmentExists`); `production` when left out. */
  env?: string;
  /** A run started from the editor on the draft (`runs.test`). */
  test?: { nodeId?: string; outputs?: Record<string, unknown> } | null;
  /**
   * The workflow's limit and what this run does when it is full. Without it (an editor test) the run takes no slot and
   * starts at once. `max` null is no limit.
   */
  concurrency?: { workflowId: string; whenFull: "queue" | "skip"; max: number | null; queueCap: number };
  /** The authentication the call came with (API and webhook triggers). */
  authenticationId?: string | null;
  /** An API trigger with callbacks on: where the result is POSTed when the run ends (a `run_callbacks` row, made with the run). */
  callbackUrl?: string | null;
};

export type InsertedRun = { runId: string; no: number; dispatched: boolean };

/** Environments every workspace has without an `environments` row (`builtInEnvironments` in the app's db/schema.ts). */
export const BUILT_IN_ENVIRONMENTS = ["production", "staging"];
const ENVIRONMENT_NAME = /^[a-z][a-z0-9-]{0,31}$/;

/** True when `name` is an environment of the workspace: a built-in one, or one its members added. */
export async function environmentExists(sql: Sql, workspaceId: string, name: string): Promise<boolean> {
  if (BUILT_IN_ENVIRONMENTS.includes(name)) return true;
  if (!ENVIRONMENT_NAME.test(name)) return false;
  const [row] = await sql`select 1 as ok from environments where workspace_id = ${workspaceId}::uuid and name = ${name}`;
  return Boolean(row);
}

/**
 * Creates the run row. With a concurrency limit the slot is taken in the same statement: a run that finds the workflow
 * full is created waiting (`dispatched` false, no engine call yet) when it queues and the queue has room, and not created
 * at all (null) when it skips or the queue is full. The run number comes from the workspace counter in the same statement.
 */
export async function insertRun(sql: Sql, run: NewRun): Promise<InsertedRun | null> {
  const runId = uuidv7();
  const limit = run.concurrency;
  const workflowId = limit?.workflowId ?? run.workflowId;
  const max = limit?.max ?? null;
  const whenFull = limit?.whenFull ?? "queue";
  const queueCap = limit?.queueCap ?? 1_000_000;

  const [, inserted] = await sql.transaction([
    // Held until the transaction ends: the counts below and the insert cannot interleave with another run of this workflow.
    sql`select pg_advisory_xact_lock(hashtext(${limit ? workflowId : runId}))`,
    sql`
      with active as (
        select count(*)::int as c from runs
        where workflow_id = ${workflowId}::uuid and test is null and dispatched_at is not null and status in ('queued', 'running')
      ), waiting as (
        select count(*)::int as c from runs
        where workflow_id = ${workflowId}::uuid and test is null and dispatched_at is null and status = 'queued'
      ), full_ as (
        select (${limit ? 1 : 0}::int = 1 and ${max}::int is not null and (select c from active) >= ${max}::int) as f
      ), ok as (
        select 1 where not (select f from full_) or (${whenFull} = 'queue' and (select c from waiting) < ${queueCap}::int)
      ), counter as (
        update workspaces set run_counter = run_counter + 1 where id = ${run.workspaceId} and exists (select 1 from ok) returning run_counter
      )
      insert into runs (id, no, workspace_id, workflow_id, workflow_version_id, trigger_id, trigger_type, authentication_id, input, trigger_data, triggered_by, retry_of, cause_run_id, cause_file_id, env, test, dispatched_at)
      select ${runId}, counter.run_counter, ${run.workspaceId}, ${run.workflowId}, ${run.workflowVersionId}, ${run.triggerId}::uuid,
        ${run.triggerType}, ${run.authenticationId ?? null}::uuid, ${JSON.stringify(run.input)}::jsonb, ${run.triggerData ? JSON.stringify(run.triggerData) : null}::jsonb, ${run.triggeredBy ?? null}::uuid, ${run.retryOf ?? null}::uuid, ${run.causeRunId ?? null}::uuid, ${run.causeFileId ?? null}::uuid,
        ${run.env ?? "production"}, ${run.test ? JSON.stringify(run.test) : null}::jsonb, case when not (select f from full_) then now() end
      from counter
      returning id, no, dispatched_at`,
    // In the same transaction, so the row exists before the run can end. Nothing when the run was not created.
    ...(run.callbackUrl
      ? [sql`insert into run_callbacks (id, run_id, workspace_id, trigger_id, url) select ${uuidv7()}, ${runId}, ${run.workspaceId}, ${run.triggerId}::uuid, ${run.callbackUrl} where exists (select 1 from runs where id = ${runId})`]
      : []),
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
 * Gives free slots of a workflow to the runs waiting for them, oldest first. Called when a run of the workflow ends and
 * by a sweep that catches what a missed call left behind. Returns how many started.
 */
export async function dispatchQueued(sql: Sql, env: Bindings, workflowId: string): Promise<number> {
  let started = 0;
  for (let round = 0; round < 50; round++) {
    const [, claimed] = await sql.transaction([
      sql`select pg_advisory_xact_lock(hashtext(${workflowId}))`,
      sql`
        with next as (
          select id from runs where workflow_id = ${workflowId}::uuid and test is null and status = 'queued' and dispatched_at is null
          order by created_at asc limit 1 for update skip locked
        ), active as (
          select count(*)::int as c from runs where workflow_id = ${workflowId}::uuid and test is null and dispatched_at is not null and status in ('queued', 'running')
        )
        update runs set dispatched_at = now()
        where id = (select id from next) and (select c from active) < coalesce((select max_active_runs from workflows where id = ${workflowId}::uuid), 2147483647)
        returning id`,
    ]);
    const row = claimed[0] as { id: string } | undefined;
    if (!row) break;
    await handToRunner(sql, env, row.id);
    started++;
  }
  return started;
}
