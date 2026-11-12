// Pure parts of receiving a call: checking who sent it, and turning it into a run input. No I/O.

import { sameSecret, signBody } from "./crypto";
import { evaluateIf, resolveValue, type IfNode, type Scope } from "./graph";

export type AuthMode = "none" | "hmac" | "bearer";
export type AuthProblem = "missing_auth" | "invalid_signature";

/** `null` when the call is allowed, otherwise why it is not. HMAC covers the raw body; bearer is the secret itself. */
export async function checkAuth(params: { mode: AuthMode; secret: string | null; rawBody: string; headers: Headers }): Promise<AuthProblem | null> {
  if (params.mode === "none") return null;
  if (!params.secret) return "invalid_signature";
  if (params.mode === "bearer") {
    const given = params.headers.get("authorization");
    if (!given) return "missing_auth";
    return sameSecret(given, `Bearer ${params.secret}`) ? null : "invalid_signature";
  }
  const given = params.headers.get("x-xaas-signature");
  if (!given) return "missing_auth";
  return sameSecret(given.trim().toLowerCase(), await signBody(params.secret, params.rawBody)) ? null : "invalid_signature";
}

/** The raw data of a call, as `{{ trigger… }}` reads it. A schedule adds `scheduledAt` (the time it fired, ISO). */
export type CallParts = { body: unknown; headers: Record<string, string>; query: Record<string, string>; scheduledAt?: string };

/** What filters and mappings read: `{{ trigger.body… }}`, `{{ trigger.headers… }}`, `{{ trigger.query… }}`, `{{ trigger.scheduledAt }}`. Header names are lower case. There is no `input` yet: the mapping makes it. */
export function callScope(parts: CallParts): Scope {
  return { input: undefined, trigger: parts, run: { id: "", no: null, env: "production" }, nodes: {} };
}

export type Filter = { join: "and" | "or"; conditions: { left: unknown; op: IfNode["data"]["conditions"][number]["op"]; right?: unknown }[] };

/** Whether the call passes the trigger's filter. No filter passes everything. A value that is missing counts as empty. */
export function passesFilter(filter: Filter | null | undefined, scope: Scope): boolean {
  if (!filter || !Array.isArray(filter.conditions) || filter.conditions.length === 0) return true;
  const node: IfNode = { id: "filter", type: "if", data: { join: filter.join === "or" ? "or" : "and", conditions: filter.conditions } };
  return evaluateIf(node, scope) === "true";
}

/**
 * The run input: each mapped value resolved against the call; without a mapping, the body. A mapped value is a typed literal
 * (text, number, boolean or JSON, kept as stored) or, for a string with `{{ … }}`, an expression.
 */
export function buildInput(mapping: Record<string, unknown> | null | undefined, scope: Scope, body: Record<string, unknown>): Record<string, unknown> {
  if (!mapping || Object.keys(mapping).length === 0) return body;
  const out: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(mapping)) {
    const resolved = resolveValue(value, scope, false);
    if (resolved !== undefined) out[name] = resolved;
  }
  return out;
}

/** Headers as a plain lower-case object, without credentials. */
export function safeHeaders(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  headers.forEach((value, name) => {
    const key = name.toLowerCase();
    if (!["authorization", "x-xaas-signature", "cookie"].includes(key)) out[key] = value;
  });
  return out;
}
