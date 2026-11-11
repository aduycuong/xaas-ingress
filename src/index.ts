// Ingress Worker: every way a run is started from outside goes through here.
//   POST /wh/:token                     webhook trigger (the token in the URL is the credential)
//   POST /internal/triggers/:id/fire    QStash schedule of a cron trigger (Upstash signature)
//   POST /runs                          manual run from the xaas-workflows app (bearer token)
// Database access is plain SQL: the tables are owned and migrated by the xaas-workflows app.

import { neon, type NeonQueryFunction } from "@neondatabase/serverless";
import { Receiver } from "@upstash/qstash";
import { Client } from "@upstash/workflow";
import { Hono } from "hono";

type Bindings = {
  DATABASE_URL: string;
  QSTASH_TOKEN: string;
  QSTASH_URL?: string;
  QSTASH_CURRENT_SIGNING_KEY?: string;
  QSTASH_NEXT_SIGNING_KEY?: string;
  /** Public URL of the runner Worker, without a trailing slash. */
  RUNNER_URL: string;
  /** Shared with the xaas-workflows app for `POST /runs`. */
  INGRESS_API_TOKEN?: string;
};

type Sql = NeonQueryFunction<false, false>;

/** Largest request body accepted. The body becomes the run input. */
const MAX_BODY_BYTES = 256 * 1024;

/** Route of the runner. A runner change that reorders or renames steps comes with a new route. */
const RUNNER_ROUTE = "/run/v1";

function uuidv7(now: number = Date.now()): string {
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

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

/** Parses a request body as a JSON object; an empty body is `{}`. */
type ParsedBody = { ok: true; value: Record<string, unknown> } | { ok: false; error: string; status: 400 | 413 };

function parseJsonObject(raw: string): ParsedBody {
  if (raw.length > MAX_BODY_BYTES) return { ok: false, error: "payload_too_large", status: 413 };
  if (!raw.trim()) return { ok: true, value: {} };
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return { ok: false, error: "invalid_json", status: 400 };
  }
  if (!isRecord(value)) return { ok: false, error: "body_must_be_a_json_object", status: 400 };
  return { ok: true, value };
}

type StartRun = {
  workspaceId: string;
  workflowId: string;
  workflowVersionId: string;
  triggerId: string | null;
  triggerType: "cron" | "webhook" | "manual";
  input: Record<string, unknown>;
};

/** Creates the run row and hands the run to the runner through Upstash Workflow. */
async function startRun(sql: Sql, env: Bindings, run: StartRun): Promise<string> {
  const runId = uuidv7();
  await sql`
    insert into runs (id, workspace_id, workflow_id, workflow_version_id, trigger_id, trigger_type, input)
    values (${runId}, ${run.workspaceId}, ${run.workflowId}, ${run.workflowVersionId}, ${run.triggerId},
      ${run.triggerType}, ${JSON.stringify(run.input)}::jsonb)`;

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
  return runId;
}

type FireResult =
  | { status: "started"; runId: string }
  | { status: "skipped"; reason: "trigger_disabled" | "workflow_not_active" }
  | { status: "not_found" };

/** Starts a run for a trigger, unless the trigger is off or its workflow is not active. */
async function fireTrigger(
  sql: Sql,
  env: Bindings,
  match: { triggerId: string } | { webhookToken: string },
  input: Record<string, unknown>,
): Promise<FireResult> {
  // One of the two is null; the trigger is matched by the other.
  const triggerId = "triggerId" in match ? match.triggerId : null;
  const webhookToken = "webhookToken" in match ? match.webhookToken : null;
  const [trigger] = await sql`
    select t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, w.status as workflow_status, w.current_version_id
    from triggers t join workflows w on w.id = t.workflow_id
    where (${triggerId}::uuid is not null and t.id = ${triggerId}::uuid)
       or (${webhookToken}::text is not null and t.type = 'webhook' and t.webhook_token = ${webhookToken}::text)
    limit 1`;

  if (!trigger) return { status: "not_found" };
  if (!trigger.enabled) return { status: "skipped", reason: "trigger_disabled" };
  if (trigger.workflow_status !== "active" || !trigger.current_version_id) {
    return { status: "skipped", reason: "workflow_not_active" };
  }

  const runId = await startRun(sql, env, {
    workspaceId: trigger.workspace_id,
    workflowId: trigger.workflow_id,
    workflowVersionId: trigger.current_version_id,
    triggerId: trigger.id,
    triggerType: trigger.type,
    input,
  });
  return { status: "started", runId };
}

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

const app = new Hono<{ Bindings: Bindings }>();

app.get("/health", (c) => c.json({ ok: true }));

app.onError((error, c) => {
  console.error("[ingress]", c.req.method, c.req.path, error);
  // 500 makes QStash retry a schedule delivery.
  return c.json({ error: "could_not_start_run" }, 500);
});

app.post("/wh/:token", async (c) => {
  const body = parseJsonObject(await c.req.text());
  if (!body.ok) return c.json({ error: body.error }, body.status);

  const result = await fireTrigger(neon(c.env.DATABASE_URL), c.env, { webhookToken: c.req.param("token") }, body.value);
  if (result.status === "not_found") return c.json({ error: "not_found" }, 404);
  if (result.status === "skipped") return c.json(result, 409);
  return c.json({ status: "accepted", runId: result.runId }, 202);
});

app.post("/internal/triggers/:id/fire", async (c) => {
  const { QSTASH_CURRENT_SIGNING_KEY: currentSigningKey, QSTASH_NEXT_SIGNING_KEY: nextSigningKey } = c.env;
  if (!currentSigningKey || !nextSigningKey) return c.json({ error: "qstash_not_configured" }, 503);

  const signature = c.req.header("upstash-signature");
  if (!signature) return c.json({ error: "missing_signature" }, 401);
  const body = await c.req.text();
  const valid = await new Receiver({ currentSigningKey, nextSigningKey }).verify({ signature, body }).catch(() => false);
  if (!valid) return c.json({ error: "invalid_signature" }, 401);

  const triggerId = c.req.param("id");
  if (!UUID.test(triggerId)) return c.json({ status: "not_found" });
  // A trigger that is gone or turned off answers 200: a retry would not change the outcome.
  return c.json(await fireTrigger(neon(c.env.DATABASE_URL), c.env, { triggerId }, {}));
});

app.post("/runs", async (c) => {
  const expected = c.env.INGRESS_API_TOKEN;
  if (!expected) return c.json({ error: "api_token_not_configured" }, 503);
  if (c.req.header("authorization") !== `Bearer ${expected}`) return c.json({ error: "unauthorized" }, 401);

  const body = parseJsonObject(await c.req.text());
  if (!body.ok) return c.json({ error: body.error }, body.status);

  const workflowId = body.value.workflowId;
  const input = body.value.input ?? {};
  if (typeof workflowId !== "string" || !UUID.test(workflowId)) return c.json({ error: "workflowId_must_be_a_uuid" }, 400);
  if (!isRecord(input)) return c.json({ error: "input_must_be_a_json_object" }, 400);

  const sql = neon(c.env.DATABASE_URL);
  const [workflow] = await sql`select workspace_id, current_version_id from workflows where id = ${workflowId}`;
  if (!workflow) return c.json({ error: "not_found" }, 404);
  if (!workflow.current_version_id) return c.json({ error: "workflow_not_published" }, 409);

  const runId = await startRun(sql, c.env, {
    workspaceId: workflow.workspace_id,
    workflowId,
    workflowVersionId: workflow.current_version_id,
    triggerId: null,
    triggerType: "manual",
    input,
  });
  return c.json({ status: "accepted", runId }, 202);
});

export default app;
