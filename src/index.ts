// Ingress Worker: every way a run is started from outside goes through here.
//   POST /wh/:token                          webhook or manual-API trigger (token in the URL, plus the trigger's own auth)
//   POST /internal/triggers/:id/fire         QStash schedule of a cron trigger (Upstash signature)
//   POST /internal/events/run-finished       QStash message from the runner when a run ends (Upstash signature)
//   POST /triggers/:id/fire                  "Fire now" from the xaas-workflows app (bearer token)
//   POST /dispatch-queued                    the app's sweep: start queued runs whose trigger has a free slot (bearer token)
//   POST /runs                               manual run from the xaas-workflows app (bearer token)
// Database access is plain SQL: the tables are owned and migrated by the xaas-workflows app.

import { Receiver } from "@upstash/qstash";
import { Hono } from "hono";
import { sameSecret } from "./crypto";
import { connect, databaseUrl, type Sql } from "./db";
import { handleRunFinished } from "./events";
import { fileEvents, fireWithFile, handleFileEvent, type FileEvent } from "./file-events";
import { authenticate, fireTrigger, loadTriggerById, loadTriggerByToken, logTriggerEvent, type FireOutcome } from "./fire";
import { dispatchQueued, environmentExists, startRun, type Bindings } from "./runs";

/** Largest request body accepted. The body becomes the run input. */
const MAX_BODY_BYTES = 256 * 1024;

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

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** Verifies the Upstash signature of a QStash delivery and returns the body, or a response to send back. */
async function verifiedQstashBody(c: { env: Bindings; req: { header(name: string): string | undefined; text(): Promise<string> } }): Promise<{ body: string } | { error: string; status: 401 | 503 }> {
  const { QSTASH_CURRENT_SIGNING_KEY: currentSigningKey, QSTASH_NEXT_SIGNING_KEY: nextSigningKey } = c.env;
  if (!currentSigningKey || !nextSigningKey) return { error: "qstash_not_configured", status: 503 };
  const signature = c.req.header("upstash-signature");
  if (!signature) return { error: "missing_signature", status: 401 };
  const body = await c.req.text();
  const valid = await new Receiver({ currentSigningKey, nextSigningKey }).verify({ signature, body }).catch(() => false);
  return valid ? { body } : { error: "invalid_signature", status: 401 };
}

function hasApiToken(env: Bindings, header: string | undefined): boolean {
  const tokens = [env.INGRESS_API_TOKEN, env.INGRESS_API_TOKEN_NEXT].filter((token): token is string => Boolean(token));
  return tokens.some((token) => sameSecret(header ?? "", `Bearer ${token}`));
}

/** The HTTP answer for what firing a trigger did. */
function answer(outcome: FireOutcome): { body: Record<string, unknown>; status: 200 | 202 | 401 | 409 | 422 | 429 } {
  if (outcome.kind === "started") return { body: { status: outcome.queued ? "queued" : "accepted", runId: outcome.runId, no: outcome.no }, status: 202 };
  if (outcome.kind === "rejected") {
    if (outcome.reason === "filter_not_matched") return { body: { status: "ignored", reason: outcome.reason }, status: 200 };
    return { body: { status: "rejected", reason: outcome.reason }, status: outcome.status === 422 ? 422 : outcome.status };
  }
  return { body: { status: "skipped", reason: outcome.reason }, status: 409 };
}

const app = new Hono<{ Bindings: Bindings; Variables: { sql: Sql } }>();

// One database handle per request, closed once the request is done (a Worker cannot reuse a socket across requests).
app.use(async (c, next) => {
  const sql = connect(databaseUrl(c.env));
  c.set("sql", sql);
  try {
    await next();
  } finally {
    c.executionCtx.waitUntil(sql.end());
  }
});

app.get("/health", (c) => c.json({ ok: true }));

app.onError((error, c) => {
  console.error("[ingress]", c.req.method, c.req.path, error);
  // 500 makes QStash retry a delivery.
  return c.json({ error: "could_not_start_run" }, 500);
});

app.post("/wh/:token", async (c) => {
  const sql = c.var.sql;
  const trigger = await loadTriggerByToken(sql, c.req.param("token"));
  if (!trigger) return c.json({ error: "not_found" }, 404);

  // The raw text is what a signature covers: read it once, check it, then parse.
  const raw = await c.req.text();
  if (raw.length > MAX_BODY_BYTES) return c.json({ error: "payload_too_large" }, 413);
  const problem = await authenticate(c.env, trigger, raw, c.req.raw.headers);
  if (problem) {
    await logTriggerEvent(sql, trigger, "rejected", problem, null);
    return c.json({ error: problem }, 401);
  }
  const body = parseJsonObject(raw);
  if (!body.ok) return c.json({ error: body.error }, body.status);

  const query = Object.fromEntries(new URL(c.req.url).searchParams);
  // A manual trigger may name its environment: `?env=staging`.
  const outcome = await fireTrigger(sql, c.env, trigger, { body: body.value, headers: c.req.raw.headers, query, source: "webhook", env: trigger.type === "manual" ? (query.env ?? null) : null });
  const { body: out, status } = answer(outcome);
  return c.json(out, status);
});

app.post("/internal/triggers/:id/fire", async (c) => {
  const checked = await verifiedQstashBody(c);
  if ("error" in checked) return c.json({ error: checked.error }, checked.status);

  const triggerId = c.req.param("id");
  if (!UUID.test(triggerId)) return c.json({ status: "not_found" });
  const sql = c.var.sql;
  const trigger = await loadTriggerById(sql, triggerId);
  // A trigger that is gone answers 200: a retry would not change the outcome.
  if (!trigger) return c.json({ status: "not_found" });
  return c.json(await fireTrigger(sql, c.env, trigger, { body: {}, source: "schedule" }));
});

app.post("/internal/events/run-finished", async (c) => {
  const checked = await verifiedQstashBody(c);
  if ("error" in checked) return c.json({ error: checked.error }, checked.status);
  let runId: unknown;
  try {
    runId = (JSON.parse(checked.body) as { runId?: unknown }).runId;
  } catch {
    return c.json({ error: "invalid_json" }, 400);
  }
  if (typeof runId !== "string" || !UUID.test(runId)) return c.json({ error: "runId_must_be_a_uuid" }, 400);
  return c.json(await handleRunFinished(c.var.sql, c.env, runId));
});

app.post("/internal/events/file", async (c) => {
  const checked = await verifiedQstashBody(c);
  if ("error" in checked) return c.json({ error: checked.error }, checked.status);
  const body = parseJsonObject(checked.body);
  if (!body.ok) return c.json({ error: body.error }, body.status);
  const { fileId, event, causeRunId } = body.value;
  if (typeof fileId !== "string" || !UUID.test(fileId)) return c.json({ error: "fileId_must_be_a_uuid" }, 400);
  if (typeof event !== "string" || !(fileEvents as readonly string[]).includes(event)) return c.json({ error: "unknown_event" }, 400);
  if (causeRunId != null && (typeof causeRunId !== "string" || !UUID.test(causeRunId))) return c.json({ error: "causeRunId_must_be_a_uuid" }, 400);
  return c.json(await handleFileEvent(c.var.sql, c.env, { fileId, event: event as FileEvent, causeRunId: (causeRunId as string | null) ?? null }));
});

app.post("/triggers/:id/fire-file", async (c) => {
  if (!hasApiToken(c.env, c.req.header("authorization"))) return c.json({ error: "unauthorized" }, 401);
  const triggerId = c.req.param("id");
  if (!UUID.test(triggerId)) return c.json({ error: "trigger_id_must_be_a_uuid" }, 400);
  const body = parseJsonObject(await c.req.text());
  if (!body.ok) return c.json({ error: body.error }, body.status);
  const fileId = body.value.fileId;
  if (typeof fileId !== "string" || !UUID.test(fileId)) return c.json({ error: "fileId_must_be_a_uuid" }, 400);
  const triggeredBy = typeof body.value.triggeredBy === "string" && UUID.test(body.value.triggeredBy) ? body.value.triggeredBy : null;
  const sql = c.var.sql;
  const trigger = await loadTriggerById(sql, triggerId);
  if (!trigger || trigger.type !== "event") return c.json({ error: "not_found" }, 404);
  const outcome = await fireWithFile(sql, c.env, trigger, fileId, triggeredBy);
  if (!outcome) return c.json({ error: "file_not_found" }, 404);
  const { body: out, status } = answer(outcome);
  return c.json(out, status);
});

app.post("/triggers/:id/fire", async (c) => {
  if (!hasApiToken(c.env, c.req.header("authorization"))) return c.json({ error: "unauthorized" }, 401);
  const triggerId = c.req.param("id");
  if (!UUID.test(triggerId)) return c.json({ error: "trigger_id_must_be_a_uuid" }, 400);
  const body = parseJsonObject(await c.req.text());
  if (!body.ok) return c.json({ error: body.error }, body.status);
  const triggeredBy = typeof body.value.triggeredBy === "string" && UUID.test(body.value.triggeredBy) ? body.value.triggeredBy : null;
  const input = isRecord(body.value.input) ? body.value.input : {};

  const sql = c.var.sql;
  const trigger = await loadTriggerById(sql, triggerId);
  if (!trigger) return c.json({ error: "not_found" }, 404);
  const env = typeof body.value.env === "string" && body.value.env ? body.value.env : null;
  const { body: out, status } = answer(await fireTrigger(sql, c.env, trigger, { body: input, source: "api", triggeredBy, env }));
  return c.json(out, status);
});

app.post("/dispatch-queued", async (c) => {
  if (!hasApiToken(c.env, c.req.header("authorization"))) return c.json({ error: "unauthorized" }, 401);
  const sql = c.var.sql;
  // Triggers with runs waiting for a slot.
  const waiting = (await sql`select distinct trigger_id from runs where status = 'queued' and dispatched_at is null and trigger_id is not null limit 200`) as unknown as { trigger_id: string }[];
  let started = 0;
  for (const { trigger_id } of waiting) started += await dispatchQueued(sql, c.env, trigger_id);
  return c.json({ triggers: waiting.length, started });
});

app.post("/runs", async (c) => {
  if (!c.env.INGRESS_API_TOKEN && !c.env.INGRESS_API_TOKEN_NEXT) return c.json({ error: "api_token_not_configured" }, 503);
  if (!hasApiToken(c.env, c.req.header("authorization"))) return c.json({ error: "unauthorized" }, 401);

  const body = parseJsonObject(await c.req.text());
  if (!body.ok) return c.json({ error: body.error }, body.status);

  const workflowId = body.value.workflowId;
  const input = body.value.input ?? {};
  if (typeof workflowId !== "string" || !UUID.test(workflowId)) return c.json({ error: "workflowId_must_be_a_uuid" }, 400);
  if (!isRecord(input)) return c.json({ error: "input_must_be_a_json_object" }, 400);
  // The raw call data a test run of the editor gives to `{{ trigger… }}` (a real run from the app has none); optional.
  const triggerData = body.value.triggerData ?? null;
  if (triggerData !== null && !isRecord(triggerData)) return c.json({ error: "triggerData_must_be_a_json_object" }, 400);

  // Who started it, which run it repeats, which environment and (to repeat a run) which version; all optional.
  const optionalUuid = (value: unknown) => (typeof value === "string" && UUID.test(value) ? value : value === undefined || value === null ? null : false);
  const triggeredBy = optionalUuid(body.value.triggeredBy);
  const retryOf = optionalUuid(body.value.retryOf);
  const versionId = optionalUuid(body.value.workflowVersionId);
  if (triggeredBy === false || retryOf === false || versionId === false) return c.json({ error: "ids_must_be_uuids" }, 400);
  // A test run of the draft (from the editor): `{ nodeId?, outputs? }`. It uses a draft version, so the workflow need not be published.
  const test = body.value.test ?? null;
  if (test !== null && (!isRecord(test) || (test.nodeId !== undefined && typeof test.nodeId !== "string") || (test.outputs !== undefined && !isRecord(test.outputs)))) return c.json({ error: "test_must_be_an_object" }, 400);
  if (test && !versionId) return c.json({ error: "test_needs_a_draft_version" }, 400);
  // Tests in the editor default to staging, every other run to production; any environment of the workspace may be asked for.
  const env = typeof body.value.env === "string" && body.value.env ? body.value.env : test ? "staging" : "production";

  const sql = c.var.sql;
  const [workflow] = await sql`select workspace_id, current_version_id from workflows where id = ${workflowId}`;
  if (!workflow) return c.json({ error: "not_found" }, 404);
  if (!(await environmentExists(sql, workflow.workspace_id, env))) return c.json({ error: "unknown_environment" }, 400);
  if (!workflow.current_version_id && !test) return c.json({ error: "workflow_not_published" }, 409);
  if (versionId) {
    const [version] = await sql`select 1 as ok from workflow_versions where id = ${versionId} and workflow_id = ${workflowId} and status = ${test ? "draft" : "published"}`;
    if (!version) return c.json({ error: "version_not_found" }, 404);
  }

  const inserted = await startRun(sql, c.env, {
    workspaceId: workflow.workspace_id,
    workflowId,
    workflowVersionId: versionId ?? workflow.current_version_id,
    test: test as { nodeId?: string; outputs?: Record<string, unknown> } | null,
    triggerId: null,
    triggerType: retryOf ? "retry" : "manual",
    input,
    triggerData,
    triggeredBy,
    retryOf,
    env,
  });
  return c.json({ status: "accepted", runId: inserted?.runId }, 202);
});

export default app;
