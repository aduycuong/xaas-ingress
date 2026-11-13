import assert from "node:assert/strict";
import { test } from "node:test";
import { matchesPathPattern } from "./path-pattern";

test("* stays inside one folder, ** crosses folders, empty matches everything", () => {
  assert.equal(matchesPathPattern("inbox/*.pdf", "inbox/a.pdf"), true);
  assert.equal(matchesPathPattern("inbox/*.pdf", "inbox/2026/a.pdf"), false);
  assert.equal(matchesPathPattern("inbox/*.pdf", "inbox/a.csv"), false);
  assert.equal(matchesPathPattern("inbox/**/*.pdf", "inbox/a.pdf"), true);
  assert.equal(matchesPathPattern("inbox/**/*.pdf", "inbox/2026/11/a.pdf"), true);
  assert.equal(matchesPathPattern("inbox/**", "inbox/x/y.txt"), true);
  assert.equal(matchesPathPattern("inbox/**", "other/y.txt"), false);
  assert.equal(matchesPathPattern("*.csv", "list.csv"), true);
  assert.equal(matchesPathPattern("*.csv", "lists/list.csv"), false);
  assert.equal(matchesPathPattern("report-?.pdf", "report-1.pdf"), true);
  assert.equal(matchesPathPattern("/inbox/*.pdf", "inbox/a.pdf"), true);
  assert.equal(matchesPathPattern("", "anything/at/all.bin"), true);
  assert.equal(matchesPathPattern(null, "a.txt"), true);
});

test("characters of a name match themselves (dots, brackets, plus)", () => {
  assert.equal(matchesPathPattern("data/report (1).csv", "data/report (1).csv"), true);
  assert.equal(matchesPathPattern("data/a+b.csv", "data/aab.csv"), false);
  assert.equal(matchesPathPattern("data/*.csv", "data/xcsv"), false);
});
