import assert from "node:assert/strict";
import { test } from "node:test";
import { callbackBody, MAX_CALLBACK_OUTPUT_BYTES, statusUrlOf, toRunResult } from "./results";

const row = { id: "0193aaaa-0000-7000-8000-000000000000", no: 42, status: "succeeded" as const, env: "production", created_at: new Date("2026-11-26T08:00:00Z"), started_at: "2026-11-26T08:00:01Z", finished_at: new Date("2026-11-26T08:00:12Z"), output: { ok: true }, error: null };

test("a run as its caller sees it", () => {
  const url = statusUrlOf("https://ingress.xaas.im/", "tok", row.id);
  assert.equal(url, `https://ingress.xaas.im/wh/tok/runs/${row.id}`);
  assert.deepEqual(toRunResult(row, url), {
    runId: row.id, no: 42, status: "succeeded", env: "production", createdAt: "2026-11-26T08:00:00.000Z", startedAt: "2026-11-26T08:00:01.000Z", finishedAt: "2026-11-26T08:00:12.000Z", output: { ok: true }, error: null, statusUrl: url,
  });
  // Only a succeeded run has an output; an error keeps its code and message only.
  const failed = toRunResult({ ...row, status: "failed", output: { partial: 1 }, error: { code: "NODE_FAILED", message: "boom", detail: "x" } as never }, url);
  assert.equal(failed.output, null);
  assert.deepEqual(failed.error, { code: "NODE_FAILED", message: "boom" });
});

test("a callback carries the event and the result; a too large output is left out", () => {
  const result = toRunResult(row, "https://i/wh/t/runs/r");
  assert.deepEqual(JSON.parse(callbackBody(result, "run.succeeded")), { event: "run.succeeded", ...result });
  const big = toRunResult({ ...row, output: { text: "x".repeat(MAX_CALLBACK_OUTPUT_BYTES) } }, "https://i/wh/t/runs/r");
  const body = JSON.parse(callbackBody(big, "run.succeeded"));
  assert.equal(body.output, null);
  assert.equal(body.outputOmitted, true);
  assert.equal(body.statusUrl, "https://i/wh/t/runs/r");
});
