// Firing a trigger: the checks every call goes through, in order, and a record of what happened.

import { decryptSecret } from "./crypto";
import { type Bindings, type Sql, startRun, uuidv7 } from "./runs";
import { buildInput, callScope, checkAuth, passesFilter, safeHeaders, type AuthMode, type CallParts, type Filter } from "./webhook";

export type TriggerRow = {
  id: string;
  type: "cron" | "webhook" | "manual" | "event";
  enabled: boolean;
  workspace_id: string;
  workflow_id: string;
  auth_mode: AuthMode;
  auth_secret: string | null;
  filter: Filter | null;
  input_mapping: Record<string, unknown> | null;
  concurrency_policy: "parallel" | "skip" | "queue";
  concurrency_max: number;
  rate_limit_per_minute: number | null;
  workflow_status: string;
  current_version_id: string | null;
};

export type FireOutcome =
  | { kind: "started"; runId: string; no: number; queued: boolean }
  | { kind: "rejected"; reason: "invalid_signature" | "missing_auth" | "filter_not_matched" | "rate_limited"; status: 401 | 422 | 429 }
  | { kind: "skipped"; reason: "trigger_disabled" | "workflow_not_active" | "already_running" | "loop_detected" };

export async function loadTriggerById(sql: Sql, id: string): Promise<TriggerRow | null> {
  const [row] = await sql`
    select t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.auth_mode, t.auth_secret, t.filter, t.input_mapping,
      t.concurrency_policy, t.concurrency_max, t.rate_limit_per_minute, w.status as workflow_status, w.current_version_id
    from triggers t join workflows w on w.id = t.workflow_id where t.id = ${id}::uuid`;
  return (row as TriggerRow | undefined) ?? null;
}

export async function loadTriggerByToken(sql: Sql, token: string): Promise<TriggerRow | null> {
  const [row] = await sql`
    select t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.auth_mode, t.auth_secret, t.filter, t.input_mapping,
      t.concurrency_policy, t.concurrency_max, t.rate_limit_per_minute, w.status as workflow_status, w.current_version_id
    from triggers t join workflows w on w.id = t.workflow_id
    where t.webhook_token = ${token} and t.type in ('webhook', 'manual')`;
  return (row as TriggerRow | undefined) ?? null;
}

/** Records one call of a trigger: the run it started, or why it did not. Never throws: the record is not worth failing a call over. */
export async function logTriggerEvent(sql: Sql, trigger: Pick<TriggerRow, "id" | "workspace_id">, outcome: "fired" | "rejected" | "skipped", reason: string | null, runId: string | null) {
  try {
    await sql`insert into trigger_events (id, trigger_id, workspace_id, outcome, reason, run_id) values (${uuidv7()}, ${trigger.id}, ${trigger.workspace_id}, ${outcome}, ${reason}, ${runId}::uuid)`;
  } catch (error) {
    console.warn("[ingress] could not record a trigger event", error);
  }
}

/** Checks the signature or bearer token of a webhook call against the trigger's stored secret. */
export async function authenticate(env: Bindings, trigger: TriggerRow, rawBody: string, headers: Headers) {
  if (trigger.auth_mode === "none") return null;
  let secret: string | null = null;
  if (trigger.auth_secret && env.TRIGGERS_ENCRYPTION_KEY) {
    try {
      secret = await decryptSecret(trigger.auth_secret, env.TRIGGERS_ENCRYPTION_KEY);
    } catch {
      secret = null;
    }
  }
  return checkAuth({ mode: trigger.auth_mode, secret, rawBody, headers });
}

export type Call = {
  body: Record<string, unknown>;
  headers?: Headers;
  query?: Record<string, string>;
  /** `api` is "Fire now" from the app: it skips the filter and the rate limit. */
  source: "webhook" | "schedule" | "api" | "event";
  triggeredBy?: string | null;
  causeRunId?: string | null;
};

/**
 * Everything after authentication: enabled, workflow active, rate limit, filter, input
 * mapping, then the run under the concurrency policy. Every outcome is recorded.
 */
export async function fireTrigger(sql: Sql, env: Bindings, trigger: TriggerRow, call: Call): Promise<FireOutcome> {
  const skip = async (reason: Extract<FireOutcome, { kind: "skipped" }>["reason"]): Promise<FireOutcome> => {
    await logTriggerEvent(sql, trigger, "skipped", reason, null);
    return { kind: "skipped", reason };
  };
  const reject = async (reason: Extract<FireOutcome, { kind: "rejected" }>["reason"], status: 401 | 422 | 429): Promise<FireOutcome> => {
    await logTriggerEvent(sql, trigger, "rejected", reason, null);
    return { kind: "rejected", reason, status };
  };

  if (!trigger.enabled) return skip("trigger_disabled");
  if (trigger.workflow_status !== "active" || !trigger.current_version_id) return skip("workflow_not_active");

  const http = call.source === "webhook";
  if (http && trigger.rate_limit_per_minute) {
    const [recent] = await sql`
      select count(*)::int as n from trigger_events
      where trigger_id = ${trigger.id}::uuid and created_at > now() - interval '1 minute' and outcome in ('fired', 'skipped')`;
    if (Number(recent?.n ?? 0) >= trigger.rate_limit_per_minute) return reject("rate_limited", 429);
  }

  // The raw call: what `{{ trigger… }}` reads in the filter, the mapping and (stored with the run) the workflow.
  const triggerData: CallParts = { body: call.body, headers: call.headers ? safeHeaders(call.headers) : {}, query: call.query ?? {}, ...(trigger.type === "cron" ? { scheduledAt: new Date().toISOString() } : {}) };
  const scope = callScope(triggerData);
  if (call.source !== "api" && call.source !== "schedule" && !passesFilter(trigger.filter, scope)) return reject("filter_not_matched", 422);

  // The mapping makes the input for every kind of call; a schedule has no body, so only its fixed values and `scheduledAt` count.
  const input = buildInput(trigger.input_mapping, scope, call.body);
  const inserted = await startRun(sql, env, {
    workspaceId: trigger.workspace_id,
    workflowId: trigger.workflow_id,
    workflowVersionId: trigger.current_version_id,
    triggerId: trigger.id,
    triggerType: trigger.type === "cron" ? "cron" : trigger.type,
    input,
    triggerData: triggerData as unknown as Record<string, unknown>,
    triggeredBy: call.triggeredBy ?? null,
    causeRunId: call.causeRunId ?? null,
    concurrency: { triggerId: trigger.id, policy: trigger.concurrency_policy, max: trigger.concurrency_policy === "parallel" ? trigger.concurrency_max : 1 },
  });
  if (!inserted) return skip("already_running");
  await logTriggerEvent(sql, trigger, "fired", inserted.dispatched ? null : "queued", inserted.runId);
  return { kind: "started", runId: inserted.runId, no: inserted.no, queued: !inserted.dispatched };
}
