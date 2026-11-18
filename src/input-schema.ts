// Checks a run input against the workflow's input schema (`definition.inputSchema`, JSON Schema 2020-12).
// Same as `lib/workflows/utils/validate-input.ts` in the xaas-workflows app: change both together.

import { Validator, type OutputUnit } from "@cfworker/json-schema";

type JsonSchema = { type?: string; properties?: Record<string, JsonSchema>; [key: string]: unknown };

/** A problem of an input against the workflow's input schema: `path` is the field (`email`, `items.0.price`), `""` for the whole input. */
export type InputProblem = { path: string; message: string };

/** Problems of an input in one line, for a place that shows a single message: `email: Required. · amount: Must be a number.` */
export function describeProblems(problems: InputProblem[]): string {
  return problems.map((problem) => (problem.path ? `${problem.path}: ${problem.message}` : problem.message)).join(" · ");
}

// Keywords that only say "something inside failed"; the inner error says what.
const WRAPPERS = new Set(["properties", "items", "prefixItems", "allOf", "$ref", "false", "unevaluatedProperties", "dependentSchemas"]);

const toPath = (location: string) =>
  location
    .replace(/^#\/?/, "")
    .split("/")
    .filter(Boolean)
    .map((part) => decodeURIComponent(part.replace(/~1/g, "/").replace(/~0/g, "~")))
    .join(".");

function pointer(schema: unknown, location: string): unknown {
  let current = schema;
  for (const part of location.replace(/^#\/?/, "").split("/").filter(Boolean)) {
    if (current === null || typeof current !== "object") return undefined;
    current = (current as Record<string, unknown>)[decodeURIComponent(part.replace(/~1/g, "/").replace(/~0/g, "~"))];
  }
  return current;
}

function message(unit: OutputUnit): string {
  const expected = /Expected "([^"]+)"/.exec(unit.error)?.[1];
  if (unit.keyword === "type" && expected) return `Must be ${expected === "integer" ? "a whole number" : expected === "array" ? "a list" : `a ${expected}`}.`;
  if (unit.keyword === "enum" || unit.keyword === "const") return "Is not one of the allowed values.";
  if (unit.keyword === "minLength" || unit.keyword === "maxLength" || unit.keyword === "minimum" || unit.keyword === "maximum" || unit.keyword === "pattern" || unit.keyword === "minItems" || unit.keyword === "maxItems") return unit.error.replace(/^Instance /, "Value ");
  return unit.error;
}

/**
 * Checks an input against a JSON Schema (draft 2020-12). No schema: anything goes. Problems are one per field, in words a
 * person can act on. Uses no `eval`, so the same check runs in the app and in the ingress Worker.
 */
export function validateInput(schema: JsonSchema | null | undefined, input: unknown): InputProblem[] {
  if (!schema || Object.keys(schema).length === 0) return [];
  let units: OutputUnit[];
  try {
    units = new Validator(schema as object, "2020-12", false).validate(input).errors;
  } catch (error) {
    return [{ path: "", message: `The input schema cannot be used: ${(error as Error).message}` }];
  }
  const problems: InputProblem[] = [];
  const seen = new Set<string>();
  const add = (path: string, text: string) => {
    if (seen.has(path)) return;
    seen.add(path);
    problems.push({ path, message: text });
  };
  for (const unit of units) {
    if (WRAPPERS.has(unit.keyword)) continue;
    const at = toPath(unit.instanceLocation);
    if (unit.keyword === "required") {
      const name = /property "([^"]+)"/.exec(unit.error)?.[1];
      add(name ? (at ? `${at}.${name}` : name) : at, "Required.");
    } else if (unit.keyword === "additionalProperties") {
      // Reported for every property that failed, declared or not: keep the ones the schema does not declare.
      const name = /Property "([^"]+)"/.exec(unit.error)?.[1];
      const parent = pointer(schema, unit.keywordLocation.replace(/\/additionalProperties$/, "")) as JsonSchema | undefined;
      if (name && !(parent?.properties && name in parent.properties)) add(at ? `${at}.${name}` : name, "Is not a field of this input.");
    } else add(at, message(unit));
  }
  return problems;
}
