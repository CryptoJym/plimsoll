/**
 * The <UR> usage-record predicate pin (docs/lean/ARCHITECTURE.md §3.1, §10; BEADS.md B0; MIGRATION.md S1; CONTRACTS.md C8).
 * Round 2 of B0 (review-r1 should-fix 4): the pin test the plan promised in both repositories. Two guards (green today) pin the
 * fixture text and the census script to the same predicate; the pending test binds B2a's converter to the pin.
 */
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import test from "node:test";

import { fn, loadSurface, pending } from "./_pending";

/** sha256 of the predicate after comment lines are dropped, whitespace is collapsed (none inside the outer parentheses) and case is folded. Identical in plimsoll-cloud. */
export const USAGE_RECORD_PREDICATE_PIN = "e075f3989cd668f4938c4964b6634c3b20bbab1604bfc623bc1164a195f95707";
export const normalisePredicate = (text: string) =>
  text.split("\n").filter((line) => !line.trim().startsWith("--")).join(" ").replace(/\s+/g, " ").replace(/\(\s+/g, "(").replace(/\s+\)/g, ")").trim().toLowerCase();
export const pinOf = (text: string) => createHash("sha256").update(normalisePredicate(text)).digest("hex");
const fixture = () => readFileSync(new URL("./fixtures/usage_record_predicate.sql", import.meta.url), "utf8");

test("<UR> pin guard: the fixture text hashes to the pinned value", () => {
  assert.equal(pinOf(fixture()), USAGE_RECORD_PREDICATE_PIN, `pin of the fixture is ${pinOf(fixture())}`);
});

test("<UR> pin guard: the cardinality census (fixtures/host_cardinality_census.py) counts usage rows with the pinned predicate", () => {
  const census = readFileSync(new URL("./fixtures/host_cardinality_census.py", import.meta.url), "utf8");
  const match = census.match(/UR = \(("[^\n]*"\n\s*"[^\n]*")\)/);
  assert.ok(match, "the census declares UR = (\"...\" \"...\")");
  const text = match[1].split("\n").map((part) => part.trim().replace(/^"|"$/g, "")).join("");
  assert.equal(pinOf(text), USAGE_RECORD_PREDICATE_PIN, `census predicate normalises to ${normalisePredicate(text)}`);
});

test("B2a: the converter's usage-record rule is the pinned predicate (lean/usage-record.ts exports USAGE_RECORD_PREDICATE_SQL equal to the pin)", pending("B2a"), async () => {
  const surface = await loadSurface("../../../packages/collector-cli/src/lean/usage-record.ts");
  const sql = surface.USAGE_RECORD_PREDICATE_SQL;
  assert.equal(typeof sql, "string", "USAGE_RECORD_PREDICATE_SQL is exported");
  assert.equal(pinOf(sql as string), USAGE_RECORD_PREDICATE_PIN);
  const isUsage = fn(surface, "isUsageRecord") as (row: Record<string, unknown>) => boolean;
  assert.equal(isUsage({ event_type: "tool_use", input_tokens: null, output_tokens: null, cache_read_tokens: null, cache_creation_tokens: null, cost_usd: null }), false);
  assert.equal(isUsage({ event_type: "tool_use", input_tokens: 1, output_tokens: null, cache_read_tokens: null, cache_creation_tokens: null, cost_usd: null }), true, "a token amount makes a usage record regardless of event type");
  assert.equal(isUsage({ event_type: "usage_live", input_tokens: null, output_tokens: null, cache_read_tokens: null, cache_creation_tokens: null, cost_usd: null }), true);
});
