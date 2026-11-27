import assert from "node:assert/strict";
import { test } from "node:test";
import { deliveryBody, MAX_DELIVERY_OUTPUT_BYTES } from "./deliveries";
import { statusUrlOf, toRunNode, toRunResult, whereRunIs } from "./results";

const row = { id: "0193aaaa-0000-7000-8000-000000000000", no: 42, version: 3, status: "succeeded" as const, env: "production", created_at: new Date("2026-11-26T08:00:00Z"), started_at: "2026-11-26T08:00:01Z", finished_at: new Date("2026-11-26T08:00:12Z"), output: { ok: true }, error: null };

const nodes = [
  toRunNode({ node_id: "llm", label: "LLM", node_type: "action", status: "succeeded", started_at: "2026-11-26T08:00:01Z", finished_at: "2026-11-26T08:00:09Z", error: null }),
  toRunNode({ node_id: "email", label: "Send email", node_type: "action", status: "failed", started_at: new Date("2026-11-26T08:00:09Z"), finished_at: null, error: { code: "SEND_FAILED", message: "no", detail: "x" } as never }),
];

test("a node as the caller sees it: graph id, label, status, times; the error keeps code and message only", () => {
  assert.deepEqual(nodes[0], { id: "llm", label: "LLM", type: "action", status: "succeeded", startedAt: "2026-11-26T08:00:01.000Z", finishedAt: "2026-11-26T08:00:09.000Z", error: null });
  assert.deepEqual(nodes[1].error, { code: "SEND_FAILED", message: "no" });
  assert.equal(nodes[1].finishedAt, null);
});

test("a run as its caller sees it", () => {
  const url = statusUrlOf("https://ingress.xaas.im/", "tok", row.id);
  assert.equal(url, `https://ingress.xaas.im/wh/tok/runs/${row.id}`);
  assert.deepEqual(toRunResult(row, nodes, url), {
    runId: row.id, no: 42, version: 3, status: "succeeded", env: "production", createdAt: "2026-11-26T08:00:00.000Z", startedAt: "2026-11-26T08:00:01.000Z", finishedAt: "2026-11-26T08:00:12.000Z", output: { ok: true }, error: null, nodes, current: [], waiting: null, statusUrl: url,
  });
  // Only a succeeded run has an output; an error keeps its code and message only.
  const failed = toRunResult({ ...row, status: "failed", output: { partial: 1 }, error: { code: "NODE_FAILED", message: "boom", detail: "x" } as never }, [], url);
  assert.equal(failed.output, null);
  assert.deepEqual(failed.error, { code: "NODE_FAILED", message: "boom" });
});

test("an event carries its meta and the run with its nodes; a too large output is left out", () => {
  const meta = { event: "run.finished" as const, seq: 2, deliveryId: "d1", occurredAt: "2026-11-27T09:14:09.000Z", endpoint: "cb1", workflow: { id: "w1", slug: "lead-sync" }, triggerId: null };
  const result = toRunResult(row, nodes, "https://i/wh/t/runs/r");
  assert.deepEqual(JSON.parse(deliveryBody(meta, result)), { ...meta, run: result });
  const big = toRunResult({ ...row, output: { text: "x".repeat(MAX_DELIVERY_OUTPUT_BYTES) } }, nodes, null);
  const body = JSON.parse(deliveryBody(meta, big));
  assert.equal(body.run.output, null);
  assert.equal(body.run.outputOmitted, true);
  assert.equal(body.run.nodes.length, 2);
  assert.equal(body.run.statusUrl, null);
  assert.equal(body.seq, 2);
});

test("where a run is: the nodes running now, and what it waits for (an async node; a person for human.*)", () => {
  const now = whereRunIs([
    { node_id: "score", label: "Score lead", started_at: "2026-11-27T09:14:01Z", key: "ai.llm", mode: "sync" },
    { node_id: "approve", label: "Approve", started_at: new Date("2026-11-27T09:14:02Z"), key: "human.ask", mode: "async" },
  ]);
  assert.deepEqual(now.current, [
    { id: "score", label: "Score lead", key: "ai.llm", since: "2026-11-27T09:14:01.000Z" },
    { id: "approve", label: "Approve", key: "human.ask", since: "2026-11-27T09:14:02.000Z" },
  ]);
  assert.deepEqual(now.waiting, { kind: "human", node: "approve" });
  assert.deepEqual(whereRunIs([{ node_id: "ocr", label: null, started_at: "2026-11-27T09:14:01Z", key: "image.ocr", mode: "async" }]).waiting, { kind: "async", node: "ocr" });
  assert.deepEqual(whereRunIs([]), { current: [], waiting: null });
  // Only a running run says where it is.
  const ended = toRunResult(row, nodes, "u", now);
  assert.deepEqual([ended.current, ended.waiting], [[], null]);
  const running = toRunResult({ ...row, status: "running" }, nodes, "u", now);
  assert.equal(running.current.length, 2);
  assert.deepEqual(running.waiting, { kind: "human", node: "approve" });
});
