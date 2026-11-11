import assert from "node:assert/strict";
import { test } from "node:test";
import { chainBlocks, MAX_EVENT_DEPTH } from "./events";

test("a trigger whose workflow is already in the chain is refused, so a workflow cannot start itself", () => {
  assert.equal(chainBlocks([{ workflow_id: "a" }], "a"), "loop_detected");
  assert.equal(chainBlocks([{ workflow_id: "b" }, { workflow_id: "a" }], "a"), "loop_detected");
  assert.equal(chainBlocks([{ workflow_id: "b" }], "a"), null);
});

test("a chain that gets too long is cut even when no workflow repeats", () => {
  const long = Array.from({ length: MAX_EVENT_DEPTH }, (_, i) => ({ workflow_id: `w${i}` }));
  assert.equal(chainBlocks(long, "new"), "loop_detected");
  assert.equal(chainBlocks(long.slice(1), "new"), null);
});
