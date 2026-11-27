// Status callbacks (milestone 19 of the app): each event of a run (`run.started`, `run.finished`; `run.node` comes with
// the runner's reports) is POSTed to every enabled endpoint of its workflow that takes that run (`workflow_callbacks`),
// and to the URL the call named in `X-Xaas-Callback-Url`. One `run_deliveries` row per event and URL, handed to QStash,
// which retries; each attempt is signed afresh with the newest secret of the signing authentication.

import { Client } from "@upstash/qstash";
import { decryptSecret, signBody } from "./crypto";
import { loadRunNodes, loadWhereRunIs, statusUrlOf, toRunResult, type RunResult, type RunRow } from "./results";
import type { Bindings, Sql } from "./runs";

export type DeliveryEvent = "run.started" | "run.node" | "run.finished";

/** Attempts QStash makes after the first one; with its backoff they span a few minutes. */
export const DELIVERY_RETRIES = 3;

/** Largest output sent inside an event; a larger one is left out (`outputOmitted`) and read with `GET /runs/:id`. */
export const MAX_DELIVERY_OUTPUT_BYTES = 64 * 1024;

/** How long one attempt waits for the receiver to answer. */
const DELIVERY_TIMEOUT_MS = 15_000;

/** Where QStash calls back for each attempt: the ingress's own public URL. */
const deliverUrl = (origin: string) => `${origin.replace(/\/$/, "")}/internal/callbacks/deliver`;

/**
 * Creates the deliveries of one event of a run and hands them to QStash. Idempotent for `run.started` and `run.finished`
 * (a repeated announcement adds nothing). `run.started` also releases the per-call URL the call stored with the run
 * (`waiting`); `run.finished` sends to it too, and drops a `run.started` that never went out (the run never started).
 * Editor tests send nothing. Returns how many deliveries were handed over.
 */
export async function scheduleDeliveries(sql: Sql, env: Bindings, params: { runId: string; event: DeliveryEvent; origin: string }): Promise<number> {
  const { runId, event } = params;
  const [, rows] = await sql.transaction([
    sql`select pg_advisory_xact_lock(hashtext(${`deliveries:${runId}`}))`,
    sql`
      with r as (
        select id, workspace_id, workflow_id, authentication_id from runs where id = ${runId}::uuid and test is null
      ), s as (
        select coalesce(max(seq), 0) + 1 as seq from run_deliveries where run_id = ${runId}::uuid and status <> 'waiting'
      ), endpoints as (
        insert into run_deliveries (id, run_id, workspace_id, callback_id, url, event, seq, status)
        select gen_random_uuid(), r.id, r.workspace_id, c.id, c.url, ${event}, (select seq from s), 'sending'
        from r join workflow_callbacks c on c.workflow_id = r.workflow_id
        where c.enabled
          and (c.only_authentication_id is null or c.only_authentication_id = r.authentication_id)
          and (${event} <> 'run.node' or c.level = 'nodes')
          and (${event} = 'run.node' or not exists (select 1 from run_deliveries x where x.run_id = r.id and x.callback_id = c.id and x.event = ${event}))
        returning id
      ), released as (
        update run_deliveries set status = 'sending', seq = (select seq from s)
        where ${event} = 'run.started' and run_id = ${runId}::uuid and callback_id is null and event = 'run.started' and status = 'waiting'
        returning id
      ), never_started as (
        update run_deliveries set status = 'failed', last_error = 'The run ended without starting.'
        where ${event} = 'run.finished' and run_id = ${runId}::uuid and callback_id is null and event = 'run.started' and status = 'waiting'
        returning id
      ), per_call as (
        insert into run_deliveries (id, run_id, workspace_id, callback_id, url, event, seq, status)
        select gen_random_uuid(), d.run_id, d.workspace_id, null, d.url, ${event}, (select seq from s), 'sending'
        from (select run_id, workspace_id, url from run_deliveries where run_id = ${runId}::uuid and callback_id is null and event = 'run.started' order by created_at limit 1) d
        where ${event} <> 'run.started'
          and (${event} = 'run.node' or not exists (select 1 from run_deliveries x where x.run_id = d.run_id and x.callback_id is null and x.event = ${event}))
        returning id
      )
      select id from endpoints union all select id from released union all select id from per_call`,
  ]);
  const ids = (rows as { id: string }[]).map((row) => row.id);
  if (ids.length === 0) return 0;
  const client = new Client({ token: env.QSTASH_TOKEN, baseUrl: env.QSTASH_URL || undefined });
  try {
    await Promise.all(ids.map((deliveryId) => client.publishJSON({ url: deliverUrl(params.origin), body: { deliveryId }, retries: DELIVERY_RETRIES, deduplicationId: `run-delivery-${deliveryId}` })));
  } catch (error) {
    // Not handed over: removed again (a released per-call URL goes back to waiting), so the retry of the message that
    // announced the event creates them anew.
    await sql`delete from run_deliveries where id = any(${ids}::uuid[]) and not (callback_id is null and event = 'run.started')`;
    await sql`update run_deliveries set status = 'waiting' where id = any(${ids}::uuid[]) and callback_id is null and event = 'run.started' and attempts = 0`;
    throw error;
  }
  return ids.length;
}

/** What an event says (design-system §6.4): which event of which run, and the run as `GET /runs/:id` shows it. */
export type DeliveryBody = {
  event: DeliveryEvent;
  seq: number;
  deliveryId: string;
  occurredAt: string;
  /** The status callback it was sent to; null for the URL the call named. */
  endpoint: string | null;
  workflow: { id: string; slug: string };
  triggerId: string | null;
  run: RunResult & { outputOmitted?: true };
};

/** The body of an event. An output larger than `MAX_DELIVERY_OUTPUT_BYTES` is left out (`run.outputOmitted`). Pure. */
export function deliveryBody(meta: Omit<DeliveryBody, "run">, run: RunResult): string {
  const fits = new TextEncoder().encode(JSON.stringify(run.output ?? null)).length <= MAX_DELIVERY_OUTPUT_BYTES;
  return JSON.stringify({ ...meta, run: fits ? run : { ...run, output: null, outputOmitted: true } });
}

export type DeliveryAnswer = { status: 200 | 502; body: Record<string, unknown> };

/**
 * One attempt to POST an event to its URL, signed afresh (so a retry minutes later still passes the receiver's
 * timestamp check): `X-Xaas-Key-Id`, `X-Xaas-Timestamp`, `X-Xaas-Signature: sha256=hex(hmac(secret, "<ts>.<body>"))`.
 * A 2xx answer delivers it; otherwise 502 makes QStash try again, until its last retry (`retried`).
 */
export async function deliverRunEvent(sql: Sql, env: Bindings, params: { deliveryId: string; ingressOrigin: string; retried: number; now?: number }): Promise<DeliveryAnswer> {
  const [row] = await sql`
    select d.id, d.url, d.status as delivery_status, d.event, d.seq, d.callback_id, d.created_at as occurred_at,
      coalesce(c.sign_authentication_id, case when d.callback_id is null then r.authentication_id end) as signer,
      r.id as run_id, r.no, v.version, r.status, r.env, r.created_at, r.started_at, r.finished_at, r.output, r.error, r.trigger_id,
      t.webhook_token, t.type as trigger_type, w.id as workflow_id, w.slug as workflow_slug
    from run_deliveries d
      join runs r on r.id = d.run_id
      join workflow_versions v on v.id = r.workflow_version_id
      join workflows w on w.id = r.workflow_id
      left join workflow_callbacks c on c.id = d.callback_id
      left join triggers t on t.id = r.trigger_id
    where d.id = ${params.deliveryId}::uuid`;
  if (!row || row.delivery_status !== "sending") return { status: 200, body: { skipped: row ? row.delivery_status : "not_found" } };

  const finalAttempt = params.retried >= DELIVERY_RETRIES;
  const record = async (outcome: { ok: boolean; statusCode: number | null; error: string | null }) => {
    const status = outcome.ok ? "delivered" : finalAttempt ? "failed" : "sending";
    await sql`
      update run_deliveries set attempts = attempts + 1, status = ${status}, last_status_code = ${outcome.statusCode}, last_error = ${outcome.error},
        delivered_at = case when ${outcome.ok} then now() else delivered_at end
      where id = ${params.deliveryId}::uuid`;
    return status;
  };

  const key = row.signer ? await signingKey(sql, env, String(row.signer)) : null;
  if (!key) {
    // Nothing to sign with (the authentication lost its last secret, or the callback was removed): an unsigned event
    // would not be trusted, so it is not sent.
    await sql`update run_deliveries set status = 'failed', last_error = 'No secret to sign the event with.' where id = ${params.deliveryId}::uuid`;
    return { status: 200, body: { status: "failed" } };
  }

  const running = row.status === "running";
  const [nodes, now] = await Promise.all([loadRunNodes(sql, row.run_id), running ? loadWhereRunIs(sql, row.run_id) : Promise.resolve({ current: [], waiting: null })]);
  const statusUrl = row.webhook_token && (row.trigger_type === "api" || row.trigger_type === "webhook") ? statusUrlOf(params.ingressOrigin, row.webhook_token, row.run_id) : null;
  const run = toRunResult({ ...row, id: row.run_id } as RunRow, nodes, statusUrl, now);
  const body = deliveryBody(
    { event: row.event, seq: Number(row.seq), deliveryId: row.id, occurredAt: new Date(row.occurred_at).toISOString(), endpoint: row.callback_id ?? null, workflow: { id: row.workflow_id, slug: row.workflow_slug }, triggerId: row.trigger_id ?? null },
    run,
  );
  const timestamp = String(Math.floor((params.now ?? Date.now()) / 1000));
  const headers = {
    "Content-Type": "application/json",
    "User-Agent": "XAAS-Workflows-Callback/1",
    "X-Xaas-Event": row.event,
    "X-Xaas-Delivery-Id": row.id,
    "X-Xaas-Run-Id": row.run_id,
    "X-Xaas-Key-Id": key.keyId,
    "X-Xaas-Timestamp": timestamp,
    "X-Xaas-Signature": await signBody(key.secret, `${timestamp}.${body}`),
  };

  let outcome: { ok: boolean; statusCode: number | null; error: string | null };
  try {
    // No redirects: a redirect could lead anywhere.
    const response = await fetch(row.url, { method: "POST", headers, body, redirect: "manual", signal: AbortSignal.timeout(DELIVERY_TIMEOUT_MS) });
    const ok = response.status >= 200 && response.status < 300;
    outcome = { ok, statusCode: response.status, error: ok ? null : `The URL answered ${response.status}.` };
    await response.body?.cancel();
  } catch (error) {
    outcome = { ok: false, statusCode: null, error: error instanceof Error ? error.message.slice(0, 300) : "The URL could not be reached." };
  }
  const status = await record(outcome);
  return status === "sending" ? { status: 502, body: { status, error: outcome.error } } : { status: 200, body: { status } };
}

/** The newest secret of an authentication that is not revoked, decrypted. */
async function signingKey(sql: Sql, env: Bindings, authenticationId: string): Promise<{ keyId: string; secret: string } | null> {
  if (!env.TRIGGERS_ENCRYPTION_KEY) return null;
  const [row] = await sql`select key_id, secret from authentication_secrets where authentication_id = ${authenticationId}::uuid and revoked_at is null order by created_at desc limit 1`;
  if (!row) return null;
  try {
    return { keyId: String(row.key_id), secret: await decryptSecret(String(row.secret), env.TRIGGERS_ENCRYPTION_KEY) };
  } catch {
    return null;
  }
}

/** Hands the `run.started` events over, best effort: the run itself never waits on its callbacks. */
export async function announceRunStarted(sql: Sql, env: Bindings, runId: string): Promise<void> {
  if (!env.PUBLIC_URL) return;
  try {
    await scheduleDeliveries(sql, env, { runId, event: "run.started", origin: env.PUBLIC_URL });
  } catch (error) {
    console.warn("[deliveries] run.started not handed over", error instanceof Error ? error.message : error);
  }
}
