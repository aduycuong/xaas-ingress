import assert from "node:assert/strict";
import { test } from "node:test";
import { validateInput } from "./input-schema";

const schema = {
  type: "object",
  required: ["email"],
  additionalProperties: false,
  properties: {
    email: { type: "string" },
    amount: { type: "integer", minimum: 0 },
    tier: { type: "string", enum: ["hot", "warm"] },
    file: { type: "string", format: "workspace-file" },
    lead: { type: "object", required: ["name"], properties: { name: { type: "string" }, score: { type: "number" } } },
    items: { type: "array", items: { type: "object", properties: { price: { type: "number" } } } },
  },
};

test("validateInput: a good input and no schema pass", () => {
  assert.deepEqual(validateInput(schema, { email: "a@b.vn", amount: 3, tier: "hot", file: "inbox/a.pdf", lead: { name: "An" }, items: [{ price: 1 }] }), []);
  assert.deepEqual(validateInput(null, { anything: 1 }), []);
  assert.deepEqual(validateInput({}, 5), []);
});

test("validateInput: one problem per field, in words", () => {
  const problems = validateInput(schema, { amount: 1.5, tier: "cold", lead: { score: "x" }, items: [{ price: "1" }], extra: true });
  assert.deepEqual(problems, [
    { path: "email", message: "Required." },
    { path: "amount", message: "Must be a whole number." },
    { path: "tier", message: "Is not one of the allowed values." },
    { path: "lead.name", message: "Required." },
    { path: "lead.score", message: "Must be a number." },
    { path: "items.0.price", message: "Must be a number." },
    { path: "extra", message: "Is not a field of this input." },
  ]);
});

test("validateInput: a schema that cannot be used is one problem", () => {
  assert.equal(validateInput({ $ref: "#/nowhere" }, {}).length, 1);
});
