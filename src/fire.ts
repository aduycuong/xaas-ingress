// Firing a trigger: the checks every call goes through, in order, and a record of what happened.

import { decryptSecret } from "./crypto";
import { resolveCallbackTarget } from "./callback-target";
import { validateInput, type InputProblem } from "./input-schema";
import { type Bindings, type Sql, environmentExists, startRun, uuidv7 } from "./runs";
import { buildInput, callScope, checkKeyAuth, passesFilter, safeHeaders, type AuthProblem, type CallParts, type Filter, type KeyMethod } from "./webhook";

export type TriggerRow = {
  id: string;
  /** `manual` is the old name of `api`, until `scripts/migrate-api-trigger.ts` of the app has run. */
  type: "cron" | "webhook" | "api" | "manual" | "event";
  enabled: boolean;
  workspace_id: string;
  workflow_id: string;
  filter: Filter | null;
  input_mapping: Record<string, unknown> | null;
  /** Environment of the runs it starts; ignored for `api` (the caller chooses). */
  env: string | null;
  workflow_status: string;
  current_version_id: string | null;
  /** `event` triggers: what they listen to (`run.failed`, `file.created`…). */
  event_type?: string | null;
  /**
   * API and webhook triggers (milestone 19): the authentication of the workspace callers use, its methods and rate limit.
   * Always set for them (a check constraint of the app); null for schedules and events.
   */
  authentication_id?: string | null;
  auth_methods?: KeyMethod[] | null;
  auth_rate_limit?: number | null;
  /** What a call does when the workflow runs `max_active_runs` already (milestone 19). */
  when_full: "queue" | "skip";
  /** Runs of the workflow active at once (every trigger and the Run button); null is no limit. */
  max_active_runs: number | null;
  /** Runs that may wait for a slot; past it a call that would queue is rejected (429). */
  queue_cap: number;
  /** API triggers: the last segment of their URL. */
  webhook_token?: string | null;
  /** The workflow's origins a call may name in `X-Xaas-Callback-Url` (`callback-target.ts`). */
  callback_origins?: string[] | null;
};

export type FireOutcome =
  | { kind: "started"; runId: string; no: number; queued: boolean }
  | { kind: "rejected"; reason: "invalid_signature" | "missing_auth" | "stale_signature" | "filter_not_matched" | "rate_limited" | "queue_full" | "unknown_environment" | "callback_url_not_allowed" | "callback_needs_secret"; status: 401 | 422 | 429 }
  /** The input (after the mapping) does not fit the input schema of the published version. */
  | { kind: "rejected"; reason: "invalid_input"; status: 422; problems: InputProblem[] }
  | { kind: "skipped"; reason: "trigger_disabled" | "workflow_not_active" | "already_running" | "loop_detected" };

export async function loadTriggerById(sql: Sql, id: string): Promise<TriggerRow | null> {
  const [row] = await sql`
    select t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.filter, t.input_mapping,
      t.env, t.event_type, t.webhook_token,
      w.callback_allowed_origins as callback_origins, w.status as workflow_status, w.current_version_id,
      t.authentication_id, a.methods as auth_methods, a.rate_limit_per_minute as auth_rate_limit, t.when_full, w.max_active_runs, w.queue_cap
    from triggers t join workflows w on w.id = t.workflow_id left join authentications a on a.id = t.authentication_id where t.id = ${id}::uuid`;
  return (row as TriggerRow | undefined) ?? null;
}

export async function loadTriggerByToken(sql: Sql, token: string): Promise<TriggerRow | null> {
  const [row] = await sql`
    select t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.filter, t.input_mapping,
      t.env, t.webhook_token,
      w.callback_allowed_origins as callback_origins, w.status as workflow_status, w.current_version_id,
      t.authentication_id, a.methods as auth_methods, a.rate_limit_per_minute as auth_rate_limit, t.when_full, w.max_active_runs, w.queue_cap
    from triggers t join workflows w on w.id = t.workflow_id left join authentications a on a.id = t.authentication_id
    where t.webhook_token = ${token} and t.type in ('webhook', 'api', 'manual')`;
  return (row as TriggerRow | undefined) ?? null;
}

/** The input schema of a workflow version (`definition.inputSchema`), or null when it has none. */
export async function inputSchemaOf(sql: Sql, versionId: string): Promise<Record<string, unknown> | null> {
  const [row] = await sql`select definition->'inputSchema' as schema from workflow_versions where id = ${versionId}::uuid`;
  const schema = (row as { schema?: unknown } | undefined)?.schema;
  return schema && typeof schema === "object" && !Array.isArray(schema) ? (schema as Record<string, unknown>) : null;
}

/** An API trigger (`manual` is its old name): the caller sends the input and picks the environment. */
export const isApiTrigger = (trigger: Pick<TriggerRow, "type">) => trigger.type === "api" || trigger.type === "manual";

/** Records one call of a trigger: the run it started, or why it did not. Never throws: the record is not worth failing a call over. */
export async function logTriggerEvent(sql: Sql, trigger: Pick<TriggerRow, "id" | "workspace_id">, outcome: "fired" | "rejected" | "skipped", reason: string | null, runId: string | null) {
  try {
    await sql`insert into trigger_events (id, trigger_id, workspace_id, outcome, reason, run_id) values (${uuidv7()}, ${trigger.id}, ${trigger.workspace_id}, ${outcome}, ${reason}, ${runId}::uuid)`;
  } catch (error) {
    console.warn("[ingress] could not record a trigger event", error);
  }
}

/**
 * Who may call a webhook or API trigger: the methods and secrets of the authentication of the workspace it uses
 * (`checkKeyAuth`); the secret that passed gets its `last_used_at`. `method` and `path` are the request's; a read
 * (`GET`) has an empty body. A trigger without an authentication (cannot happen: the app requires one) lets no one in.
 */
export async function authenticate(sql: Sql, env: Bindings, trigger: TriggerRow, request: { method: string; path: string; rawBody: string; headers: Headers }): Promise<{ problem: AuthProblem | null; authenticationId: string | null }> {
  if (trigger.authentication_id) {
    const authenticationId = trigger.authentication_id;
    const { problem, keyId } = await checkKeyAuth({
      methods: trigger.auth_methods ?? [],
      secretOf: (keyId) => authenticationSecret(sql, env, authenticationId, keyId),
      method: request.method,
      path: request.path,
      rawBody: request.rawBody,
      headers: request.headers,
    });
    if (!problem && keyId) await touchSecret(sql, keyId);
    return { problem, authenticationId: problem ? null : authenticationId };
  }
  return { problem: "invalid_signature", authenticationId: null };
}

/** The secret of a key id of an authentication, in clear, when it is not revoked; null otherwise. */
async function authenticationSecret(sql: Sql, env: Bindings, authenticationId: string, keyId: string): Promise<string | null> {
  if (!env.TRIGGERS_ENCRYPTION_KEY) return null;
  const [row] = await sql`select secret from authentication_secrets where key_id = ${keyId} and authentication_id = ${authenticationId}::uuid and revoked_at is null`;
  if (!row?.secret) return null;
  try {
    return await decryptSecret(String(row.secret), env.TRIGGERS_ENCRYPTION_KEY);
  } catch {
    return null;
  }
}

/** "Last used" of a secret, at most once a minute (a busy key does not write on every call). Never throws. */
async function touchSecret(sql: Sql, keyId: string): Promise<void> {
  try {
    await sql`update authentication_secrets set last_used_at = now() where key_id = ${keyId} and (last_used_at is null or last_used_at < now() - interval '1 minute')`;
  } catch (error) {
    console.warn("[ingress] could not record the use of a key", error);
  }
}

export type Call = {
  body: Record<string, unknown>;
  headers?: Headers;
  query?: Record<string, string>;
  /** `api` is "Fire now" from the app: it skips the filter and the rate limit. */
  source: "webhook" | "schedule" | "api" | "event";
  triggeredBy?: string | null;
  causeRunId?: string | null;
  /** The workspace file that fired it (`file.*` trigger). */
  causeFileId?: string | null;
  /** The environment an API trigger's caller asked for (`?env=` or `env` in "Fire now"); other triggers use their own. */
  env?: string | null;
  /** API and webhook calls: where the caller asked its run's events to go (`X-Xaas-Callback-Url`); checked against the workflow's allowed origins. */
  callbackUrl?: string | null;
  /** The authentication the call came with (`authenticate`), kept on the run. */
  authenticationId?: string | null;
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
  const reject = async (reason: Exclude<Extract<FireOutcome, { kind: "rejected" }>["reason"], "invalid_input">, status: 401 | 422 | 429): Promise<FireOutcome> => {
    await logTriggerEvent(sql, trigger, "rejected", reason, null);
    return { kind: "rejected", reason, status };
  };

  if (!trigger.enabled) return skip("trigger_disabled");
  if (trigger.workflow_status !== "active" || !trigger.current_version_id) return skip("workflow_not_active");

  const http = call.source === "webhook";
  // The rate limit of the authentication counts the calls of every trigger that uses it.
  if (http && trigger.authentication_id && trigger.auth_rate_limit) {
    const [recent] = await sql`
      select count(*)::int as n from trigger_events e join triggers t on t.id = e.trigger_id
      where t.authentication_id = ${trigger.authentication_id}::uuid and e.created_at > now() - interval '1 minute' and e.outcome in ('fired', 'skipped')`;
    if (Number(recent?.n ?? 0) >= trigger.auth_rate_limit) return reject("rate_limited", 429);
  }

  // An API trigger runs where the caller asks (production by default); the others where they are set.
  const runEnv = isApiTrigger(trigger) ? (call.env || "production") : (trigger.env || "production");
  if (isApiTrigger(trigger) && call.env && !(await environmentExists(sql, trigger.workspace_id, call.env))) return reject("unknown_environment", 422);

  // The URL the call names for its run's events, decided now: a later change of the allowed origins does not move the
  // events of a run already accepted. Signed with the authentication the call came with, so it needs one with a secret.
  let callbackUrl: string | null = null;
  if (http) {
    const canSign = Boolean(call.authenticationId) && !(trigger.auth_methods ?? []).includes("none");
    const target = resolveCallbackTarget(trigger.callback_origins ?? [], call.callbackUrl, canSign);
    if (!target.ok) return reject(target.reason, 422);
    callbackUrl = target.url;
  }

  // The raw call: what `{{ trigger… }}` reads in the filter, the mapping and (stored with the run) the workflow.
  const triggerData: CallParts = { body: call.body, headers: call.headers ? safeHeaders(call.headers) : {}, query: call.query ?? {}, ...(trigger.type === "cron" ? { scheduledAt: new Date().toISOString() } : {}) };
  const scope = callScope(triggerData);
  if (call.source !== "api" && call.source !== "schedule" && !(await passesFilter(trigger.filter, scope))) return reject("filter_not_matched", 422);

  // The mapping makes the input for every kind of call; a schedule has no body, so only its fixed values and `scheduledAt` count.
  const input = await buildInput(trigger.input_mapping, scope, call.body);

  // Every trigger's input (webhook, API, event, schedule, "Fire now") must fit the input schema of the version that runs.
  const problems = validateInput(await inputSchemaOf(sql, trigger.current_version_id), input);
  if (problems.length) {
    await logTriggerEvent(sql, trigger, "rejected", "invalid_input", null);
    return { kind: "rejected", reason: "invalid_input", status: 422, problems };
  }
  const inserted = await startRun(sql, env, {
    workspaceId: trigger.workspace_id,
    workflowId: trigger.workflow_id,
    workflowVersionId: trigger.current_version_id,
    triggerId: trigger.id,
    triggerType: isApiTrigger(trigger) ? "api" : trigger.type === "cron" ? "cron" : (trigger.type as "webhook" | "event"),
    input,
    triggerData: triggerData as unknown as Record<string, unknown>,
    triggeredBy: call.triggeredBy ?? null,
    causeRunId: call.causeRunId ?? null,
    causeFileId: call.causeFileId ?? null,
    env: runEnv,
    concurrency: { workflowId: trigger.workflow_id, whenFull: trigger.when_full, max: trigger.max_active_runs, queueCap: trigger.queue_cap },
    callbackUrl,
    authenticationId: call.authenticationId ?? null,
  });
  // Not created: the workflow was full and the trigger skips, or its queue was full too.
  if (!inserted) return trigger.when_full === "skip" ? skip("already_running") : reject("queue_full", 429);
  await logTriggerEvent(sql, trigger, "fired", inserted.dispatched ? null : "queued", inserted.runId);
  return { kind: "started", runId: inserted.runId, no: inserted.no, queued: !inserted.dispatched };
}
