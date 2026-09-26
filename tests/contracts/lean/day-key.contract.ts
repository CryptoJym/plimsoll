/**
 * B5 / B9a (collector): the UTC day key, the census classes and the UTC-midnight window (docs/lean/ARCHITECTURE.md §3.5, §8).
 * Ports the core of fixtures/b5_lexical_boundary.py and b5_non_iso_day_facts.py (the full 3,812-string enumeration stays in
 * the Python fixture). Pending until B5 lands packages/collector-cli/src/lean/day-key.ts and DASHBOARD_SCHEMA_VERSION 3.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { fn, loadSurface, pending } from "./_pending";

test("B5: DASHBOARD_SCHEMA_VERSION is 3 (UTC-day-aligned windows)", pending("B5"), async () => {
  const projection = await loadSurface("../../../packages/collector-cli/src/dashboard-projection.ts"); // today's module, loaded at run time so a rename stays pending
  assert.equal(projection.DASHBOARD_SCHEMA_VERSION, 3);
});

test("B5: every schema-accepted timestamp has a UTC day; only a string Date.parse rejects has none", pending("B5"), async () => {
  const utcDayOf = fn(await loadSurface("../../../packages/collector-cli/src/lean/day-key.ts"), "utcDayOf") as (s: string) => string | null;
  const cases: Array<[string, string | null]> = [
    ["2026-09-25T23:30:00-02:00", "2026-09-26"], ["2026-09-26T00:00:00+00:00", "2026-09-26"], ["2026-09-26T24:00:00Z", "2026-09-27"],
    ["Sat, 26 Sep 2026 00:00:00 GMT+00:00", "2026-09-26"], ["1 Sep 2026 00:00:00 +00:00", "2026-09-01"], ["Sat, 26 Sep 2026 23:30:00 -02:00", "2026-09-27"],
    ["2026-09-26T00:00:00.123456789Z", "2026-09-26"], ["26/09/2026 00:00:00 +00:00", null],
  ];
  for (const [s, day] of cases) assert.equal(utcDayOf(s), day, s);
});

test("B9a: the census classes are computable per row: day_move, before_own_midnight, non_iso, canonical", pending("B9a"), async () => {
  const censusClass = fn(await loadSurface("../../../packages/collector-cli/src/lean/day-key.ts"), "censusClass") as (s: string) => string[];
  assert.deepEqual(censusClass("2026-09-25T23:30:00-02:00"), ["day_move"]);
  assert.deepEqual(censusClass("2026-09-26T00:00:00+00:00"), ["before_own_midnight"]);
  assert.deepEqual(censusClass("2026-09-26 12:00:00Z"), ["before_own_midnight"]);
  assert.deepEqual(censusClass("Sat, 26 Sep 2026 00:00:00 GMT+00:00"), ["non_iso"]);
  assert.deepEqual(censusClass("2026-09-26T00:00:00.000Z"), []);
  assert.deepEqual(censusClass("2026-09-26T00:00:00+02:00"), ["day_move"], "a day_move row can still agree at every cutoff; the harness decides per cutoff");
});

test("B5: the snapshot window starts at the UTC midnight of now - days, not at a millisecond cutoff", pending("B5"), async () => {
  const since = fn(await loadSurface("../../../packages/collector-cli/src/lean/day-key.ts"), "dashboardWindowSince") as (days: number, now: Date) => string;
  assert.equal(since(30, new Date("2026-09-26T13:45:12.345Z")), "2026-08-27T00:00:00.000Z");
  assert.equal(since(1825, new Date("2026-09-26T00:00:00.000Z")), "2021-09-27T00:00:00.000Z");
});
