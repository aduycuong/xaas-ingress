// Pure parts of receiving a call: checking who sent it, and turning it into a run input. No I/O.

import { sameSecret, signBody } from "./crypto";
import { evaluateIf, ExpressionError, resolveValue, type IfNode, type Scope } from "./graph";

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

// ---------- Authentications of the workspace (milestone 19 of the app) ----------

export type KeyMethod = "bearer" | "hmac" | "none";

/** Public key id of a secret of an authentication: `xa_` + 6 characters. */
export const KEY_ID = /^xa_[a-z0-9]{6}$/;

/**
 * What an HMAC caller signs with the secret of an authentication. The path is in it because one key may call many
 * triggers: a call signed for one URL does not pass at another. A read (`GET`) signs an empty body.
 */
export const keySigned = (timestamp: string, method: string, path: string, rawBody: string) => `${timestamp}.${method.toUpperCase()}.${path}.${rawBody}`;

/**
 * Checks a call against the authentication its trigger uses. `none` lets anything in. With `Authorization`, the bearer
 * token is `<key id>.<secret>`; with `X-Xaas-Signature`, the caller names the key in `X-Xaas-Key-Id` and signs
 * `keySigned(...)` with a timestamp (required). `secretOf` gives the secret of a key id of this authentication (not
 * revoked), or null. Returns the problem, or null and the key id that passed.
 */
export async function checkKeyAuth(params: {
  methods: KeyMethod[];
  secretOf: (keyId: string) => Promise<string | null>;
  method: string;
  path: string;
  rawBody: string;
  headers: Headers;
  now?: number;
}): Promise<{ problem: AuthProblem | null; keyId: string | null }> {
  const { methods, headers } = params;
  if (methods.includes("none")) return { problem: null, keyId: null };
  const invalid = { problem: "invalid_signature" as const, keyId: null };

  const authorization = headers.get("authorization");
  if (authorization !== null) {
    if (!methods.includes("bearer")) return invalid;
    const match = /^Bearer\s+(xa_[a-z0-9]{6})\.(\S+)$/.exec(authorization.trim());
    if (!match) return invalid;
    const secret = await params.secretOf(match[1]);
    return secret && sameSecret(match[2], secret) ? { problem: null, keyId: match[1] } : invalid;
  }

  const signature = headers.get("x-xaas-signature");
  if (signature !== null) {
    if (!methods.includes("hmac")) return invalid;
    const keyId = headers.get("x-xaas-key-id")?.trim() ?? "";
    const timestamp = headers.get("x-xaas-timestamp");
    if (!KEY_ID.test(keyId) || timestamp === null) return { problem: "missing_auth", keyId: null };
    const problem = checkTimestamp(timestamp, params.now ?? Date.now());
    if (problem) return { problem, keyId: null };
    const secret = await params.secretOf(keyId);
    if (!secret) return invalid;
    return (await sameSignature(signature, secret, keySigned(timestamp.trim(), params.method, params.path, params.rawBody))) ? { problem: null, keyId } : invalid;
  }
  return { problem: "missing_auth", keyId: null };
}
