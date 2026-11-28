import assert from "node:assert/strict";
import { test } from "node:test";
import { emitBody, emitSignatureValid, resultOf, signEmit } from "./emit";

/** The same vector is in `runner/src/emit-event.test.ts`: both sides sign alike. */
const VECTOR = { key: "secret", timestamp: 1764310000, body: '{"a":1}', signature: "bW-mNGI8Bdj6V8wJPlge65DTe9JZRcb_DpfB4MLXTGY" };

test("signature: the runner's format, recent timestamps only", async () => {
  assert.equal(await signEmit(VECTOR.key, VECTOR.timestamp, VECTOR.body), VECTOR.signature);
  const now = VECTOR.timestamp * 1000;
  assert.equal(await emitSignatureValid(VECTOR.key, String(VECTOR.timestamp), VECTOR.signature, VECTOR.body, now), true);
  assert.equal(await emitSignatureValid(VECTOR.key, String(VECTOR.timestamp), VECTOR.signature, VECTOR.body, now + 299_000), true);
  assert.equal(await emitSignatureValid(VECTOR.key, String(VECTOR.timestamp), VECTOR.signature, VECTOR.body, now + 301_000), false);
  assert.equal(await emitSignatureValid(VECTOR.key, String(VECTOR.timestamp), VECTOR.signature, '{"a":2}', now), false);
  assert.equal(await emitSignatureValid("other", String(VECTOR.timestamp), VECTOR.signature, VECTOR.body, now), false);
  assert.equal(await emitSignatureValid(VECTOR.key, undefined, VECTOR.signature, VECTOR.body, now), false);
  assert.equal(await emitSignatureValid(VECTOR.key, "abc", VECTOR.signature, VECTOR.body, now), false);
});

test("body: the event, its id (the node run), the payload and where it came from", () => {
  const body = emitBody(
    { runId: "r1", nodeRunId: "nr1", event: "task.created", payload: { task: "x" } },
    { run_id: "r1", run_no: 136, test: null, workspace_id: "ws", workflow_id: "wf", env: "staging", workflow_slug: "telegram", workflow_name: "Telegram", node_id: "emit_1", node_label: "Emit" },
    "2026-11-28T06:00:00.000Z",
  );
  assert.deepEqual(body, {
    event: "task.created",
    id: "nr1",
    emitted_at: "2026-11-28T06:00:00.000Z",
    env: "staging",
    payload: { task: "x" },
    source: { run: { id: "r1", no: 136, test: false }, workflow: { id: "wf", slug: "telegram", name: "Telegram" }, node: { id: "emit_1", label: "Emit" } },
  });
});

test("result: started, refused or skipped, as the node reports it", () => {
  assert.deepEqual(resultOf({ kind: "started", runId: "r2", no: 137, queued: true }), { outcome: "fired", reason: "queued", run: { id: "r2", no: 137, queued: true } });
  assert.deepEqual(resultOf({ kind: "rejected", reason: "filter_not_matched", status: 422 }), { outcome: "rejected", reason: "filter_not_matched", run: null });
  assert.deepEqual(resultOf({ kind: "rejected", reason: "invalid_input", status: 422, problems: [] }), { outcome: "rejected", reason: "invalid_input", run: null, problems: [] });
  assert.deepEqual(resultOf({ kind: "skipped", reason: "workflow_not_active" }), { outcome: "skipped", reason: "workflow_not_active", run: null });
});
