import assert from "node:assert/strict";
import { test } from "node:test";
import { callbackOriginAllowed, resolveCallbackTarget } from "./callback-target";

test("an origin matches its own host and port only", () => {
  const origins = ["https://app.example.com"];
  assert.equal(callbackOriginAllowed("https://app.example.com/hooks/xaas?x=1", origins), true);
  assert.equal(callbackOriginAllowed("https://APP.example.com/hooks", origins), true);
  assert.equal(callbackOriginAllowed("http://app.example.com/hooks", origins), false);
  assert.equal(callbackOriginAllowed("https://app.example.com:8443/hooks", origins), false);
  assert.equal(callbackOriginAllowed("https://evil.app.example.com/hooks", origins), false);
  assert.equal(callbackOriginAllowed("https://app.example.com.evil.io/hooks", origins), false);
  assert.equal(callbackOriginAllowed("https://user:pass@app.example.com/hooks", origins), false);
  assert.equal(callbackOriginAllowed("not a url", origins), false);
});

test("ports: an explicit port must match; :443 is the default", () => {
  assert.equal(callbackOriginAllowed("https://api.example.com:8443/x", ["https://api.example.com:8443"]), true);
  assert.equal(callbackOriginAllowed("https://api.example.com/x", ["https://api.example.com:443"]), true);
  assert.equal(callbackOriginAllowed("https://api.example.com/x", ["https://api.example.com:8443"]), false);
});

test("a wildcard matches subdomains, not the bare domain", () => {
  const origins = ["https://*.example.com"];
  assert.equal(callbackOriginAllowed("https://a.example.com/x", origins), true);
  assert.equal(callbackOriginAllowed("https://a.b.example.com/x", origins), true);
  assert.equal(callbackOriginAllowed("https://example.com/x", origins), false);
  assert.equal(callbackOriginAllowed("https://badexample.com/x", origins), false);
});

test("resolving the URL a call names", () => {
  const origins = ["https://app.example.com"];
  assert.deepEqual(resolveCallbackTarget(origins, null, true), { ok: true, url: null });
  assert.deepEqual(resolveCallbackTarget(origins, "  ", true), { ok: true, url: null });
  assert.deepEqual(resolveCallbackTarget(origins, "https://app.example.com/r/1", true), { ok: true, url: "https://app.example.com/r/1" });
  assert.deepEqual(resolveCallbackTarget(origins, "https://other.example.com/r/1", true), { ok: false, reason: "callback_url_not_allowed" });
  assert.deepEqual(resolveCallbackTarget([], "https://app.example.com/r/1", true), { ok: false, reason: "callback_url_not_allowed" });
  // Allowed origin, but nothing to sign with (an authentication of kind None).
  assert.deepEqual(resolveCallbackTarget(origins, "https://app.example.com/r/1", false), { ok: false, reason: "callback_needs_secret" });
  assert.deepEqual(resolveCallbackTarget(origins, null, false), { ok: true, url: null });
});
