// Pure parts of receiving a call: checking who sent it, and turning it into a run input. No I/O.

import { sameSecret, signBody } from "./crypto";
import { evaluateIf, ExpressionError, resolveValue, type IfNode, type Scope } from "./graph";

export type AuthMode = "none" | "hmac" | "bearer";
export type AuthProblem = "missing_auth" | "invalid_signature" | "stale_signature";

/** How far `X-Xaas-Timestamp` may be from the ingress clock, in seconds. */
export const SIGNATURE_TOLERANCE_SECONDS = 300;

/** A timestamp header (Unix seconds) within the tolerance, or the problem with it. */
function checkTimestamp(value: string | null, now: number): AuthProblem | null {
  if (!value || !/^\d{1,12}$/.test(value.trim())) return "missing_auth";
  return Math.abs(now / 1000 - Number(value.trim())) > SIGNATURE_TOLERANCE_SECONDS ? "stale_signature" : null;
}

async function sameSignature(given: string, secret: string, signed: string): Promise<boolean> {
  return sameSecret(given.trim().toLowerCase(), await signBody(secret, signed));
}

/**
 * `null` when the call is allowed, otherwise why it is not. Bearer is the secret itself. HMAC covers the raw body, or,
 * when the caller sends `X-Xaas-Timestamp`, `<timestamp>.<raw body>` (a copied call then works for a few minutes only).
 */
export async function checkAuth(params: { mode: AuthMode; secret: string | null; rawBody: string; headers: Headers; now?: number }): Promise<AuthProblem | null> {
  if (params.mode === "none") return null;
  if (!params.secret) return "invalid_signature";
  if (params.mode === "bearer") return checkBearer(params.secret, params.headers);
  const given = params.headers.get("x-xaas-signature");
  if (!given) return "missing_auth";
  const timestamp = params.headers.get("x-xaas-timestamp");
  if (timestamp === null) return (await sameSignature(given, params.secret, params.rawBody)) ? null : "invalid_signature";
  const problem = checkTimestamp(timestamp, params.now ?? Date.now());
  if (problem) return problem;
  return (await sameSignature(given, params.secret, `${timestamp.trim()}.${params.rawBody}`)) ? null : "invalid_signature";
}

/**
 * The same check for a read (`GET /wh/:token/runs/:runId`): bearer as for a call; HMAC signs `<timestamp>.GET.<path>`, and
 * the timestamp is required (a read has no body, so without it the signature would never change).
 */
export async function checkReadAuth(params: { mode: AuthMode; secret: string | null; method: string; path: string; headers: Headers; now?: number }): Promise<AuthProblem | null> {
  if (params.mode === "none") return null;
  if (!params.secret) return "invalid_signature";
  if (params.mode === "bearer") return checkBearer(params.secret, params.headers);
  const given = params.headers.get("x-xaas-signature");
  const timestamp = params.headers.get("x-xaas-timestamp");
  if (!given || timestamp === null) return "missing_auth";
  const problem = checkTimestamp(timestamp, params.now ?? Date.now());
  if (problem) return problem;
  return (await sameSignature(given, params.secret, `${timestamp.trim()}.${params.method.toUpperCase()}.${params.path}`)) ? null : "invalid_signature";
}

function checkBearer(secret: string, headers: Headers): AuthProblem | null {
  const given = headers.get("authorization");
  if (!given) return "missing_auth";
  return sameSecret(given, `Bearer ${secret}`) ? null : "invalid_signature";
}

/** The raw data of a call, as `{{ trigger… }}` reads it. A schedule adds `scheduledAt` (the time it fired, ISO). */
export type CallParts = { body: unknown; headers: Record<string, string>; query: Record<string, string>; scheduledAt?: string };

/** What filters and mappings read: `{{ trigger.body… }}`, `{{ trigger.headers… }}`, `{{ trigger.query… }}`, `{{ trigger.scheduledAt }}`. Header names are lower case. There is no `input` yet: the mapping makes it. */
export function callScope(parts: CallParts): Scope {
  return { input: undefined, trigger: parts, run: { id: "", no: null, env: "production" }, nodes: {} };
}

export type Filter = { join: "and" | "or"; conditions: { left: unknown; op: IfNode["data"]["conditions"][number]["op"]; right?: unknown }[] };

/** Whether the call passes the trigger's filter. No filter passes everything. A value that is missing (or fails) counts as empty. */
export async function passesFilter(filter: Filter | null | undefined, scope: Scope): Promise<boolean> {
  if (!filter || !Array.isArray(filter.conditions) || filter.conditions.length === 0) return true;
  const node: IfNode = { id: "filter", type: "if", data: { join: filter.join === "or" ? "or" : "and", conditions: filter.conditions } };
  return (await evaluateIf(node, scope)) === "true";
}

/**
 * The run input: each mapped value resolved against the call; without a mapping, the body. A mapped value is a typed literal
 * (text, number, boolean or JSON, kept as stored) or, for a string with `{{ … }}`, an expression. A value that is missing, or
 * whose expression fails, is left out (the input schema then says what is wrong).
 */
export async function buildInput(mapping: Record<string, unknown> | null | undefined, scope: Scope, body: Record<string, unknown>): Promise<Record<string, unknown>> {
  if (!mapping || Object.keys(mapping).length === 0) return body;
  const out: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(mapping)) {
    let resolved: unknown;
    try {
      resolved = await resolveValue(value, scope, false);
    } catch (error) {
      if (!(error instanceof ExpressionError)) throw error;
    }
    if (resolved !== undefined) out[name] = resolved;
  }
  return out;
}

/** Headers as a plain lower-case object, without credentials. */
export function safeHeaders(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, name) => {
    const key = name.toLowerCase();
    if (!["authorization", "x-xaas-signature", "x-xaas-timestamp", "cookie"].includes(key)) out[key] = value;
  });
  return out;
}
