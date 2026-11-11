import assert from "node:assert/strict";
import { test } from "node:test";
import { decryptSecret, signBody } from "./crypto";
import { buildInput, callScope, checkAuth, passesFilter, safeHeaders } from "./webhook";

const KEY = Buffer.alloc(32, 7).toString("base64");

test("decrypts the vector made by the app (keep in sync with xaas-workflows/lib/triggers/utils/triggers-utils.test.ts)", async () => {
  assert.equal(await decryptSecret("v1:AAAAAAAAAAAAAAAA:Cb3I3pQIn5tbBT6cKRgl2cmUBR32", KEY), "hello");
  await assert.rejects(decryptSecret("v1:AAAAAAAAAAAAAAAA:Cb3I3pQIn5tbBT6cKRgl2cmUBR32", Buffer.alloc(32, 8).toString("base64")));
});

test("HMAC: the right signature passes; a wrong one, none, or a changed body does not", async () => {
  const body = '{"status":"published"}';
  const good = await signBody("s3cret", body);
  assert.match(good, /^sha256=[0-9a-f]{64}$/);
  const check = (signature: string | null, raw = body) => checkAuth({ mode: "hmac", secret: "s3cret", rawBody: raw, headers: new Headers(signature ? { "X-XaaS-Signature": signature } : {}) });
  assert.equal(await check(good), null);
  assert.equal(await check(good.toUpperCase().replace("SHA256", "sha256")), null);
  assert.equal(await check("sha256=" + "0".repeat(64)), "invalid_signature");
  assert.equal(await check(null), "missing_auth");
  assert.equal(await check(good, '{"status":"draft"}'), "invalid_signature");
});

test("bearer and none", async () => {
  const bearer = (value: string | null) => checkAuth({ mode: "bearer", secret: "tok", rawBody: "", headers: new Headers(value ? { Authorization: value } : {}) });
  assert.equal(await bearer("Bearer tok"), null);
  assert.equal(await bearer("Bearer nope"), "invalid_signature");
  assert.equal(await bearer(null), "missing_auth");
  assert.equal(await checkAuth({ mode: "none", secret: null, rawBody: "", headers: new Headers() }), null);
});

const scope = (body: unknown, headers: Record<string, string> = {}, query: Record<string, string> = {}) => callScope({ body, headers, query });

test("filter: conditions on the body, headers and query; missing counts as empty; no filter passes", () => {
  const filter = { join: "and" as const, conditions: [{ left: "{{ trigger.body.status }}", op: "equals" as const, right: "published" }, { left: "{{ trigger.headers.x-event }}", op: "is_not_empty" as const }] };
  assert.equal(passesFilter(filter, scope({ status: "published" }, { "x-event": "post" })), true);
  assert.equal(passesFilter(filter, scope({ status: "draft" }, { "x-event": "post" })), false);
  assert.equal(passesFilter(filter, scope({ status: "published" })), false);
  assert.equal(passesFilter({ join: "or", conditions: [{ left: "{{ trigger.query.kind }}", op: "equals", right: "a" }, { left: "{{ trigger.body.n }}", op: "gt", right: 3 }] }, scope({ n: 5 })), true);
  assert.equal(passesFilter(null, scope({})), true);
});

test("input mapping: values from body, headers and query; without a mapping the body is the input", () => {
  const body = { email: "a@b.c", items: [1, 2] };
  const mapped = buildInput({ email: "{{ trigger.body.email }}", count: "{{ trigger.body.items.length }}", source: "{{ trigger.query.src }}", fixed: "hello", missing: "{{ trigger.body.nope }}" }, scope(body, {}, { src: "ads" }), body);
  assert.deepEqual(mapped, { email: "a@b.c", count: 2, source: "ads", fixed: "hello" });
  assert.deepEqual(buildInput(null, scope(body), body), body);
  assert.deepEqual(buildInput({}, scope(body), body), body);
});

test("credentials never reach the filter scope", () => {
  const headers = safeHeaders(new Headers({ Authorization: "Bearer x", "X-XaaS-Signature": "sha256=1", "X-Event": "post", Cookie: "a=b" }));
  assert.deepEqual(headers, { "x-event": "post" });
});
