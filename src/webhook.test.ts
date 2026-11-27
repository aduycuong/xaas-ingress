import assert from "node:assert/strict";
import { test } from "node:test";
import { decryptSecret, signBody } from "./crypto";
import { buildInput, callScope, checkAuth, checkKeyAuth, checkReadAuth, keySigned, passesFilter, safeHeaders } from "./webhook";

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

test("filter: conditions on the body, headers and query; missing counts as empty; no filter passes", async () => {
  const filter = { join: "and" as const, conditions: [{ left: "{{ trigger.body.status }}", op: "equals" as const, right: "published" }, { left: "{{ trigger.headers.`x-event` }}", op: "is_not_empty" as const }] };
  assert.equal(await passesFilter(filter, scope({ status: "published" }, { "x-event": "post" })), true);
  assert.equal(await passesFilter(filter, scope({ status: "draft" }, { "x-event": "post" })), false);
  assert.equal(await passesFilter(filter, scope({ status: "published" })), false);
  assert.equal(await passesFilter({ join: "or", conditions: [{ left: "{{ trigger.query.kind }}", op: "equals", right: "a" }, { left: "{{ trigger.body.n }}", op: "gt", right: 3 }] }, scope({ n: 5 })), true);
  assert.equal(await passesFilter(null, scope({})), true);
});

test("input mapping: values from body, headers and query; without a mapping the body is the input", async () => {
  const body = { email: "a@b.c", items: [1, 2] };
  const mapped = await buildInput({ email: "{{ trigger.body.email }}", count: "{{ $count(trigger.body.items) }}", label: '{{ "<" & trigger.body.email & ">" }}', broken: "{{ $number(trigger.body.email) }}", source: "{{ trigger.query.src }}", fixed: "hello", missing: "{{ trigger.body.nope }}" }, scope(body, {}, { src: "ads" }), body);
  assert.deepEqual(mapped, { email: "a@b.c", count: 2, label: "<a@b.c>", source: "ads", fixed: "hello" });
  assert.deepEqual(await buildInput(null, scope(body), body), body);
  assert.deepEqual(await buildInput({}, scope(body), body), body);
});

test("input mapping: fixed values keep their type (text, number, boolean, JSON), also for a schedule with no body", async () => {
  const mapping = { page: "A", limit: 50, dry: false, tags: ["x", "y"], zero: 0, empty: "", at: "{{ trigger.scheduledAt }}", nope: "{{ trigger.body.nope }}" };
  const call = callScope({ body: {}, headers: {}, query: {}, scheduledAt: "2026-11-12T08:00:00.000Z" });
  const input = await buildInput(mapping, call, {});
  assert.deepEqual(input, { page: "A", limit: 50, dry: false, tags: ["x", "y"], zero: 0, empty: "", at: "2026-11-12T08:00:00.000Z" });
  assert.equal(typeof input.limit, "number");
  assert.equal(typeof input.dry, "boolean");
  // A schedule with no mapping has an empty input.
  assert.deepEqual(await buildInput(null, call, {}), {});
});

test("credentials never reach the filter scope", () => {
  const headers = safeHeaders(new Headers({ Authorization: "Bearer x", "X-XaaS-Signature": "sha256=1", "X-Event": "post", Cookie: "a=b" }));
  assert.deepEqual(headers, { "x-event": "post" });
});

test("HMAC with X-Xaas-Timestamp: signs `<timestamp>.<body>`, and only within 5 minutes", async () => {
  const body = '{"a":1}';
  const now = 1_764_123_456_000;
  const ts = String(now / 1000);
  const good = await signBody("s3cret", `${ts}.${body}`);
  const check = (headers: Record<string, string>, at = now) => checkAuth({ mode: "hmac", secret: "s3cret", rawBody: body, headers: new Headers(headers), now: at });
  assert.equal(await check({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": ts }), null);
  assert.equal(await check({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": ts }, now + 299_000), null);
  assert.equal(await check({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": ts }, now + 301_000), "stale_signature");
  // A changed timestamp breaks the signature; the body-only signature does not pass once a timestamp is sent.
  assert.equal(await check({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": String(now / 1000 + 1) }), "invalid_signature");
  assert.equal(await check({ "X-Xaas-Signature": await signBody("s3cret", body), "X-Xaas-Timestamp": ts }), "invalid_signature");
  assert.equal(await check({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": "soon" }), "missing_auth");
});

test("reading a run: HMAC over `<timestamp>.GET.<path>` with a required timestamp; bearer as for a call", async () => {
  const now = 1_764_123_456_000;
  const ts = String(now / 1000);
  const path = "/wh/tok123/runs/0193aaaa-0000-7000-8000-000000000000";
  const good = await signBody("s3cret", `${ts}.GET.${path}`);
  const read = (headers: Record<string, string>, at = now, p = path) => checkReadAuth({ mode: "hmac", secret: "s3cret", method: "GET", path: p, headers: new Headers(headers), now: at });
  assert.equal(await read({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": ts }), null);
  assert.equal(await read({ "X-Xaas-Signature": good }), "missing_auth");
  assert.equal(await read({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": ts }, now + 301_000), "stale_signature");
  assert.equal(await read({ "X-Xaas-Signature": good, "X-Xaas-Timestamp": ts }, now, "/wh/tok123/runs/0193bbbb-0000-7000-8000-000000000000"), "invalid_signature");
  const bearer = (value: string) => checkReadAuth({ mode: "bearer", secret: "tok", method: "GET", path, headers: new Headers({ Authorization: value }) });
  assert.equal(await bearer("Bearer tok"), null);
  assert.equal(await bearer("Bearer nope"), "invalid_signature");
});

test("the timestamp header never reaches the filter scope", () => {
  assert.deepEqual(safeHeaders(new Headers({ "X-Xaas-Timestamp": "1", "X-Other": "y" })), { "x-other": "y" });
});

// ---------- Authentications of the workspace ----------

const KEYS: Record<string, string> = { xa_abc234: "s3cret-one", xa_def567: "s3cret-two" };
const secretOf = async (keyId: string) => KEYS[keyId] ?? null;
const NOW = Date.parse("2026-11-27T09:00:00Z");
const TS = String(NOW / 1000);
const keyCall = (methods: ("bearer" | "hmac" | "none")[], headers: Record<string, string>, extra: { path?: string; body?: string; method?: string } = {}) =>
  checkKeyAuth({ methods, secretOf, method: extra.method ?? "POST", path: extra.path ?? "/wh/tok", rawBody: extra.body ?? '{"a":1}', headers: new Headers(headers), now: NOW });

test("authentication None lets any call in", async () => {
  assert.deepEqual(await keyCall(["none"], {}), { problem: null, keyId: null });
});

test("authentication bearer: <key id>.<secret> of a key of this authentication", async () => {
  assert.deepEqual(await keyCall(["bearer"], { Authorization: "Bearer xa_abc234.s3cret-one" }), { problem: null, keyId: "xa_abc234" });
  assert.deepEqual(await keyCall(["bearer", "hmac"], { Authorization: "Bearer xa_def567.s3cret-two" }), { problem: null, keyId: "xa_def567" });
  assert.equal((await keyCall(["bearer"], { Authorization: "Bearer xa_abc234.s3cret-two" })).problem, "invalid_signature");
  // A key that is not one of this authentication's (or revoked: `secretOf` gives null).
  assert.equal((await keyCall(["bearer"], { Authorization: "Bearer xa_zzz999.s3cret-one" })).problem, "invalid_signature");
  assert.equal((await keyCall(["bearer"], { Authorization: "Bearer s3cret-one" })).problem, "invalid_signature");
  assert.equal((await keyCall(["hmac"], { Authorization: "Bearer xa_abc234.s3cret-one" })).problem, "invalid_signature");
  assert.equal((await keyCall(["bearer"], {})).problem, "missing_auth");
});

test("authentication HMAC: signs <timestamp>.<METHOD>.<path>.<body> with the key named in X-Xaas-Key-Id", async () => {
  const sign = (secret: string, path = "/wh/tok", body = '{"a":1}', method = "POST", ts = TS) => signBody(secret, keySigned(ts, method, path, body));
  const ok = { "X-Xaas-Key-Id": "xa_abc234", "X-Xaas-Timestamp": TS, "X-Xaas-Signature": await sign("s3cret-one") };
  assert.deepEqual(await keyCall(["hmac"], ok), { problem: null, keyId: "xa_abc234" });
  // Another path (another trigger), another body, another key's secret, or bearer only: refused.
  assert.equal((await keyCall(["hmac"], ok, { path: "/wh/other" })).problem, "invalid_signature");
  assert.equal((await keyCall(["hmac"], ok, { body: '{"a":2}' })).problem, "invalid_signature");
  assert.equal((await keyCall(["hmac"], { ...ok, "X-Xaas-Signature": await sign("s3cret-two") })).problem, "invalid_signature");
  assert.equal((await keyCall(["bearer"], ok)).problem, "invalid_signature");
  // The timestamp and the key id are required; an old timestamp is stale.
  assert.equal((await keyCall(["hmac"], { "X-Xaas-Key-Id": "xa_abc234", "X-Xaas-Signature": ok["X-Xaas-Signature"] })).problem, "missing_auth");
  assert.equal((await keyCall(["hmac"], { "X-Xaas-Timestamp": TS, "X-Xaas-Signature": ok["X-Xaas-Signature"] })).problem, "missing_auth");
  const old = String(NOW / 1000 - 3600);
  assert.equal((await keyCall(["hmac"], { ...ok, "X-Xaas-Timestamp": old, "X-Xaas-Signature": await sign("s3cret-one", "/wh/tok", '{"a":1}', "POST", old) })).problem, "stale_signature");
  // A read signs GET and an empty body.
  const read = { "X-Xaas-Key-Id": "xa_abc234", "X-Xaas-Timestamp": TS, "X-Xaas-Signature": await sign("s3cret-one", "/wh/tok/runs/r1", "", "GET") };
  assert.deepEqual(await keyCall(["hmac"], read, { method: "GET", path: "/wh/tok/runs/r1", body: "" }), { problem: null, keyId: "xa_abc234" });
});
