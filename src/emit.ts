// A run emitted a custom event (node `core.emit_event`, milestone 20): start every `custom` trigger of its workspace that
// listens to the name, in the run's environment, and answer with what each one did; the runner makes that the node's
// output. The runner signs the call with CALLBACK_SIGNING_KEY; only the name and the payload come from the workflow, the
// rest (workspace, environment, chain of runs) is read here by run and node run.
//
// The node run is the emit's id, the same for every attempt: a trigger that already started a run for it is reported, not
// fired again. A workflow already in the chain of runs that led here (the emitting one included) is not started again.

import { sameSecret } from "./crypto";
import { causeChain, chainBlocks } from "./events";
import { type FireOutcome, type TriggerRow, fireTrigger, logTriggerEvent } from "./fire";
import type { Bindings, Sql } from "./runs";

/** How far the timestamp of a signed call may be from now. */
const MAX_SKEW_SECONDS = 300;

const encoder = new TextEncoder();

function base64Url(bytes: ArrayBuffer): string {
  let text = "";
  for (const byte of new Uint8Array(bytes)) text += String.fromCharCode(byte);
  return btoa(text).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** `HMAC-SHA256(key, "<timestamp>.<body>")`, base64url: what the runner sends (`runner/src/emit-event.ts`, same code). */
export async function signEmit(key: string, timestamp: number, body: string): Promise<string> {
  const cryptoKey = await crypto.subtle.importKey("raw", encoder.encode(key), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
  return base64Url(await crypto.subtle.sign("HMAC", cryptoKey, encoder.encode(`${timestamp}.${body}`)));
}

/** Whether a call to `/internal/events/emit` comes from the runner: a recent timestamp and its signature. */
export async function emitSignatureValid(key: string, timestamp: string | undefined, signature: string | undefined, body: string, now = Date.now()): Promise<boolean> {
  const seconds = Number(timestamp);
  if (!timestamp || !signature || !Number.isInteger(seconds) || Math.abs(now / 1000 - seconds) > MAX_SKEW_SECONDS) return false;
  return sameSecret(await signEmit(key, seconds, body), signature);
}

export type EmitMessage = { runId: string; nodeRunId: string; event: string; payload: Record<string, unknown> };

type SourceRow = { run_id: string; run_no: number; test: unknown; workspace_id: string; workflow_id: string; env: string; workflow_slug: string; workflow_name: string; node_id: string; node_label: string | null };

/** What one listening trigger did with the event: the node's output lists one per trigger. */
export type EmitResult = {
  trigger: { id: string; name: string };
  workflow: { id: string; slug: string; name: string };
  outcome: "fired" | "rejected" | "skipped";
  reason: string | null;
  run: { id: string; no: number; queued: boolean } | null;
  problems?: unknown;
};

export type EmitAnswer = { event: string; id: string; env: string; started: number; triggers: EmitResult[] };

/** What a run started by the event receives as `{{ trigger.body… }}` (the app shows it: `lib/triggers/utils/custom-event-sample.ts`). */
export function emitBody(message: EmitMessage, source: SourceRow, emittedAt: string) {
  return {
    event: message.event,
    id: message.nodeRunId,
    emitted_at: emittedAt,
    env: source.env,
    payload: message.payload,
    source: {
      run: { id: source.run_id, no: Number(source.run_no), test: source.test != null },
      workflow: { id: source.workflow_id, slug: source.workflow_slug, name: source.workflow_name },
      node: { id: source.node_id, label: source.node_label },
    },
  };
}

/** The result of firing one trigger, as the node reports it. Pure. */
export function resultOf(outcome: FireOutcome): Pick<EmitResult, "outcome" | "reason" | "run" | "problems"> {
  if (outcome.kind === "started") return { outcome: "fired", reason: outcome.queued ? "queued" : null, run: { id: outcome.runId, no: outcome.no, queued: outcome.queued } };
  if (outcome.kind === "rejected") return { outcome: "rejected", reason: outcome.reason, run: null, ...(outcome.reason === "invalid_input" ? { problems: outcome.problems } : {}) };
  return { outcome: "skipped", reason: outcome.reason, run: null };
}

/** Fires the triggers listening to the event. Null when the run or node run is not found (or they do not belong together). */
export async function handleEmit(sql: Sql, env: Bindings, message: EmitMessage): Promise<EmitAnswer | null> {
  const [source] = (await sql`
    select r.id as run_id, r.no as run_no, r.test, r.workspace_id, r.workflow_id, r.env, w.slug as workflow_slug, w.name as workflow_name, nr.node_id, nr.label as node_label
    from node_runs nr join runs r on r.id = nr.run_id join workflows w on w.id = r.workflow_id
    where nr.id = ${message.nodeRunId}::uuid and r.id = ${message.runId}::uuid`) as unknown as SourceRow[];
  if (!source) return null;

  const triggers = (await sql`
    select t.id, t.name, t.type, t.enabled, t.workspace_id, t.workflow_id, t.filter, t.input_mapping,
      t.env, t.event_type, w.status as workflow_status, w.current_version_id, w.slug as workflow_slug, w.name as workflow_name,
      t.when_full, w.max_active_runs, w.queue_cap
    from triggers t join workflows w on w.id = t.workflow_id
    where t.workspace_id = ${source.workspace_id}::uuid and t.type = 'event' and t.enabled and t.event_type = 'custom'
      and t.event_name = ${message.event} and coalesce(t.env, 'production') = ${source.env}
    order by t.created_at`) as unknown as (TriggerRow & { name: string; workflow_slug: string; workflow_name: string })[];

  const answer: EmitAnswer = { event: message.event, id: message.nodeRunId, env: source.env, started: 0, triggers: [] };
  if (!triggers.length) return answer;

  const chain = await causeChain(sql, source.run_id);
  const body = emitBody(message, source, new Date().toISOString());
  for (const trigger of triggers) {
    const about = { trigger: { id: trigger.id, name: trigger.name }, workflow: { id: trigger.workflow_id, slug: trigger.workflow_slug, name: trigger.workflow_name } };
    if (chainBlocks(chain, trigger.workflow_id)) {
      await logTriggerEvent(sql, trigger, "skipped", "loop_detected", null, { payload: { body, headers: {}, query: {} } });
      answer.triggers.push({ ...about, outcome: "skipped", reason: "loop_detected", run: null });
      continue;
    }
    // An attempt of the node that ran again (its answer was lost): report the run it already started.
    const [already] = await sql`
      select id, no, dispatched_at from runs where cause_run_id = ${source.run_id}::uuid and trigger_id = ${trigger.id}::uuid
        and (trigger_data #>> '{body,id}') = ${message.nodeRunId} limit 1`;
    if (already) {
      answer.triggers.push({ ...about, outcome: "fired", reason: "already_started", run: { id: String(already.id), no: Number(already.no), queued: already.dispatched_at == null } });
      answer.started++;
      continue;
    }
    const outcome = await fireTrigger(sql, env, trigger, { body, source: "event", causeRunId: source.run_id });
    const result = resultOf(outcome);
    if (result.outcome === "fired") answer.started++;
    answer.triggers.push({ ...about, ...result });
  }
  return answer;
}
