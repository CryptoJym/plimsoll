/**
 * B1 / B10a (collector): the S1b runway with G per host from counted cardinalities, one G, and G OWED while conversion runs
 * (docs/lean/CONTRACTS.md C2; BUDGETS.md §3, §4.2, §4.3; ARCHITECTURE.md §2.4). Ports fixtures/s1b_runway_host_bound.py (round 8:
 * the reviewer's 333-session counterexample, the Studio4 census, the dual-write and post-census scenarios, the census gate and the
 * segment proxy) and s1b_runway_geometry.py. The "true runway" in test 3 is a byte-level simulation independent of holdRunway
 * (note, review r2: it reuses rule 2's growth figure as the daily loss, so it tests the owed bytes, not the growth rate). Round 3
 * of B0 (review-r2 should-fix 5): C_conv is MEASURED as the converter's own page allocation (test 6, B2a's
 * lean/converter.ts `withMeasuredWrite`).
 * Pending until B1 lands packages/collector-cli/src/lean/runway.ts (tests 1-5) and B2a lean/converter.ts (test 6).
 */
import assert from "node:assert/strict";
import test from "node:test";

import { fn, loadSurface, openTempBuffer, pending } from "./_pending";

const MB = 1_000_000, GiB = 1024 ** 3;
const F = 700 * 1.5, T = 280 * 1.3, S = 1300, SD = 260 * 1.5, DR = 260 * 1.5, RB = 400, SEG = 830, I = 125 + 2 * 92;
type Census = { measured: boolean; usageRows: number; rawRows: number; sessions: number; sessionDayRows: number; dayRows: number; rollupBuckets: number; dayTargets: number; rawBytes: number; ledgerBytes: number; segmentProxy?: number };
const STUDIO1: Census = { measured: true, usageRows: 171_840, rawRows: 276_454, sessions: 8_070, sessionDayRows: 12_105, dayRows: 1_400, rollupBuckets: 144 * 90, dayTargets: 180, rawBytes: 623_939_584, ledgerBytes: 1_342_111_744 };
const STUDIO4: Census = { measured: true, usageRows: 79_259, rawRows: 352_885, sessions: 5_141, sessionDayRows: 5_654, dayRows: 332, rollupBuckets: 455, dayTargets: 34, rawBytes: 833_286_144, ledgerBytes: 1_504_444_416 };
const REVIEWER_333: Census = { ...STUDIO1, sessions: 57_223, sessionDayRows: 85_834 };
const SCALED: Census = { ...STUDIO1, measured: false, usageRows: 174_820, rawRows: 281_249, rawBytes: 741_421_056, ledgerBytes: 1_365_389_312 };
const segments = (h: Census) => Math.max(1.2 * h.sessions, h.segmentProxy ?? 0);
const need = (h: Census) => h.usageRows * (F + T) + h.sessions * S + h.sessionDayRows * SD + h.dayRows * DR + h.rollupBuckets * RB + (segments(h) + h.rollupBuckets + h.dayTargets) * SEG + h.rawRows * I;

type RunwayInput = {
  freeBytes: number; reserveBytes: number; rebuildHeadroomBytes: number; gGateBytes: number; rawBytesAtCensus: number;
  converterWrittenBytes: number; rawUnfoldedBytesSinceCensus: number; conversionComplete: boolean; leanTableBytesNow?: number;
  grossGrowthP95PerDay: number; rawBytes: number; ledgerBytes: number;
};
type Runway = { runwayDays: number; gOwedBytes: number; holdGrowthPerDay: number; rung: "none" | "converter_paused" | "release_acked_only" | "abort" };
async function surface() {
  const mod = await loadSurface("../../../packages/collector-cli/src/lean/runway.ts");
  return {
    estimateHostG: fn(mod, "estimateHostG") as (census: Census) => { gateBytes: number; hostBytes: number; basis: string } | null,
    holdRunway: fn(mod, "holdRunway") as (input: RunwayInput) => Runway,
    censusPreflight: fn(mod, "censusPreflight") as (input: Record<string, number>) => { ok: boolean; reasons: string[] },
  };
}

test("B1 C2: G_gate bounds each measured host's real need for its own mix (Studio1, the Studio4 census, the reviewer's 333-session host), a counted segment proxy raises it, and scaled counts have no gate value", pending("B1"), async () => {
  const { estimateHostG } = await surface();
  for (const [name, host] of Object.entries({ studio1: STUDIO1, studio4: STUDIO4, reviewer333: REVIEWER_333 })) {
    const g = estimateHostG(host);
    assert.ok(g && g.gateBytes >= need(host), `${name}: gate ${g?.gateBytes} >= need ${need(host)}`);
    assert.equal(g.basis, "census");
  }
  assert.ok(need(REVIEWER_333) > 457.3 * MB, "the reviewer's host needs more than round 6's 457 MB gate");
  assert.ok(estimateHostG(STUDIO1)!.hostBytes >= 346.8 * MB, "Studio1 at or above the round-6 reviewer's 346.8 MB lower bound");
  const withProxy = { ...STUDIO4, segmentProxy: 2 * STUDIO4.sessions };
  assert.ok(estimateHostG(withProxy)!.gateBytes > estimateHostG(STUDIO4)!.gateBytes, "a census that counted sessions split at 7 days raises G; 1.2 x sessions is only the fallback");
  assert.equal(estimateHostG(SCALED), null, "scaled counts are not a census: no gate value, no hold");
});

test("B10a C2: at the gate's 63-day threshold the host really has >= 63 days; the same G_gate sizes the multiplier", pending("B10a"), async () => {
  const { estimateHostG, holdRunway } = await surface();
  const gross = 0.30 * GiB, reserve = 25 * GiB, rebuild = 1.2 * REVIEWER_333.ledgerBytes;
  const gate = estimateHostG(REVIEWER_333)!.gateBytes;
  const growth = (g: number) => gross * (1 + (g / REVIEWER_333.rawBytes) * (REVIEWER_333.rawBytes / REVIEWER_333.ledgerBytes));
  const free = reserve + rebuild + gate + 63 * growth(gate);
  const rule = holdRunway({ freeBytes: free, reserveBytes: reserve, rebuildHeadroomBytes: rebuild, gGateBytes: gate, rawBytesAtCensus: REVIEWER_333.rawBytes, converterWrittenBytes: 0, rawUnfoldedBytesSinceCensus: 0, conversionComplete: false, grossGrowthP95PerDay: gross, rawBytes: REVIEWER_333.rawBytes, ledgerBytes: REVIEWER_333.ledgerBytes });
  assert.ok(Math.abs(rule.runwayDays - 63) < 1e-6, `gate runway ${rule.runwayDays}`);
  assert.ok(Math.abs(rule.holdGrowthPerDay - growth(gate)) < 1, "multiplier uses G_gate");
  const trueDays = (free - reserve - rebuild - need(REVIEWER_333)) / growth(need(REVIEWER_333));
  assert.ok(trueDays >= 63, `the host really has ${trueDays.toFixed(2)} days`);
});

/** Truth, independent of holdRunway (fixtures/s1b_runway_host_bound.py `simulate`): Studio4 from the census instant, free space set so the gate allows 63 days then. */
function simulate(h: Census, gate: number, grossPerDay: number, daysCensusToS2: number, daysS2ToNow: number, convertedFraction: number) {
  const reserve = 25 * GiB, rebuild = 1.2 * h.ledgerBytes;
  const gTrue = need(h) / h.rawBytes, rawShare = h.rawBytes / h.ledgerBytes;
  const growth = grossPerDay * (1 + (gate / h.rawBytes) * rawShare);
  const free0 = reserve + rebuild + gate + 63 * growth;
  const rawGrowth = grossPerDay * rawShare;
  const dual = gTrue * rawGrowth * daysS2ToNow;                 // live-writer bytes since S2: growth, not conversion
  const conv = convertedFraction * need(h);                     // the converter's own bytes for census-era history
  const unfolded = rawGrowth * daysCensusToS2;                  // raw bytes admitted between the census and S2: still owed
  const freeNow = free0 - grossPerDay * (daysCensusToS2 + daysS2ToNow) - dual - conv;
  const trueOwed = (1 - convertedFraction) * need(h) + gTrue * unfolded;
  const trueDays = (freeNow - reserve - rebuild - trueOwed) / growth;
  const input: RunwayInput = { freeBytes: freeNow, reserveBytes: reserve, rebuildHeadroomBytes: rebuild, gGateBytes: gate, rawBytesAtCensus: h.rawBytes, converterWrittenBytes: conv, rawUnfoldedBytesSinceCensus: unfolded, conversionComplete: false, leanTableBytesNow: dual + conv, grossGrowthP95PerDay: grossPerDay, rawBytes: h.rawBytes, ledgerBytes: h.ledgerBytes };
  return { input, trueDays, growth, dual, conv, unfolded };
}

test("B10a C2 (round 2): G owed counts only the converter's own bytes against the census-era work, adds the rows admitted after the census, ignores dual-write pages, and never overstates the true runway (the reviewer's scenarios and rungs)", pending("B10a"), async () => {
  const { estimateHostG, holdRunway } = await surface();
  const gate = estimateHostG(STUDIO4)!.gateBytes;
  const base = simulate(STUDIO4, gate, 40 * MB, 14, 2, 0.6);
  // (a) dual-write pages are not conversion progress: the lean tables' page count changes nothing
  const withPages = holdRunway(base.input), withoutPages = holdRunway({ ...base.input, leanTableBytesNow: undefined });
  assert.equal(withPages.gOwedBytes, withoutPages.gOwedBytes, "leanTableBytesNow is disclosure only");
  // (b) the converter's bytes and the post-census unfolded raw bytes are the two terms
  assert.ok(Math.abs(withPages.gOwedBytes - ((gate - base.conv) + (gate / STUDIO4.rawBytes) * base.unfolded)) < 1, `owed ${withPages.gOwedBytes}`);
  // (c) never overstates, in every reviewer scenario (review-r1 checks/review_c2_runway.log): rule <= truth
  for (const [gross, dCensus, dS2, f] of [[0.30 * GiB, 0, 3, 0], [40 * MB, 0, 7, 0], [40 * MB, 0, 7, 0.6], [0.30 * GiB, 7, 0, 0], [40 * MB, 14, 0, 0], [40 * MB, 14, 2, 0.6]] as const) {
    const s = simulate(STUDIO4, gate, gross, dCensus, dS2, f);
    const r = holdRunway(s.input);
    assert.ok(r.runwayDays <= s.trueDays + 1e-9, `gross ${gross / MB} MB/d census->S2 ${dCensus} d S2->now ${dS2} d ${f * 100}% converted: rule ${r.runwayDays.toFixed(2)} > truth ${s.trueDays.toFixed(2)}`);
  }
  // (d) the rungs never fire late: shift free space so the truth is just under 5 and just under 2 days
  for (const [threshold, rung] of [[5, "release_acked_only"], [2, "abort"]] as const) {
    const shifted = holdRunway({ ...base.input, freeBytes: base.input.freeBytes - (base.trueDays - (threshold - 0.1)) * base.growth });
    assert.ok(shifted.runwayDays < threshold, `truth ${threshold - 0.1} d => rule ${shifted.runwayDays.toFixed(2)} d < ${threshold}`);
    assert.equal(shifted.rung, rung);
  }
  // (e) complete: nothing owed
  const done = holdRunway({ ...base.input, converterWrittenBytes: need(STUDIO4), rawUnfoldedBytesSinceCensus: 0, conversionComplete: true });
  assert.equal(done.gOwedBytes, 0);
});

test("B10a: a Studio1-shaped host with 50 GiB free and 0.30 GiB/day gross growth is refused a 42-day hold under the corrected geometry (s1b_runway_geometry)", pending("B10a"), async () => {
  const { estimateHostG, holdRunway } = await surface();
  const gate = estimateHostG(STUDIO1)!.gateBytes;
  const rule = holdRunway({ freeBytes: 50 * GiB, reserveBytes: 25 * GiB, rebuildHeadroomBytes: 1.2 * STUDIO1.ledgerBytes, gGateBytes: gate, rawBytesAtCensus: STUDIO1.rawBytes, converterWrittenBytes: 0, rawUnfoldedBytesSinceCensus: 0, conversionComplete: false, grossGrowthP95PerDay: 0.30 * GiB, rawBytes: STUDIO1.rawBytes, ledgerBytes: STUDIO1.ledgerBytes });
  assert.ok(rule.runwayDays < 63, `runway ${rule.runwayDays.toFixed(1)} d < max(30, 1.5 x 42)`);
});

test("B1 C2 (round 2): the census gives a gate value only when taken after the catch-up, at most a day before the decision, and the VACUUM INTO copy is refused without ledger + reserve free", pending("B1"), async () => {
  const { censusPreflight } = await surface();
  const day = 86_400_000, L = STUDIO4.ledgerBytes, reserve = 25 * GiB;
  const stale = censusPreflight({ censusAtMs: 0, catchUpCompleteAtMs: 3 * day, decisionAtMs: 7 * day, freeBytes: 40 * GiB, ledgerBytes: L, reserveBytes: reserve });
  assert.equal(stale.ok, false);
  assert.deepEqual([...stale.reasons].sort(), ["census_before_catchup", "census_stale"]);
  const tight = censusPreflight({ censusAtMs: 7 * day, catchUpCompleteAtMs: 3 * day, decisionAtMs: 7 * day, freeBytes: L + reserve - 1, ledgerBytes: L, reserveBytes: reserve });
  assert.deepEqual(tight, { ok: false, reasons: ["no_space_for_vacuum_into"] });
  assert.deepEqual(censusPreflight({ censusAtMs: 7 * day, catchUpCompleteAtMs: 3 * day, decisionAtMs: 7 * day, freeBytes: L + reserve, ledgerBytes: L, reserveBytes: reserve }), { ok: true, reasons: [] });
});

test("B2a C2 (round 3): C_conv is measured as the converter's own page allocation, delta(page_count - freelist_count) x page_size inside its write transaction, and excludes pages another writer allocated between its transactions", pending("B2a"), async () => {
  const withMeasuredWrite = fn(await loadSurface("../../../packages/collector-cli/src/lean/converter.ts"), "withMeasuredWrite") as (db: unknown, write: () => void) => { writtenBytes: number };
  const { buffer, close } = openTempBuffer({ workspaceId: "tenant-lean-contract", lean: { write: true } });
  try {
    const db = buffer.database;
    db.exec("create table lean_scratch_conv (id integer primary key, payload text not null)");
    const livePages = () => (db.pragma("page_count", { simple: true }) as number) - (db.pragma("freelist_count", { simple: true }) as number);
    const pageSize = db.pragma("page_size", { simple: true }) as number;
    const insert = db.prepare("insert into lean_scratch_conv (payload) values (?)");
    const write = (rows: number) => { for (let i = 0; i < rows; i += 1) insert.run("x".repeat(900)); };
    // (a) the measurement equals the independent page delta around the converter's transaction
    const before = livePages();
    const first = withMeasuredWrite(db, () => write(2_000));
    assert.equal(first.writtenBytes, (livePages() - before) * pageSize, "written bytes = the pages the transaction allocated x page_size");
    assert.ok(first.writtenBytes >= 2_000 * 900, "at least the payload bytes");
    assert.equal(db.inTransaction, false, "the chunk was committed with its measurement");
    // (b) another writer's pages between two converter transactions (the live writer's dual-write) are not counted
    const betweenBefore = livePages();
    write(1_000);                                                                   // unmeasured: the live writer
    const live = (livePages() - betweenBefore) * pageSize;
    const second = withMeasuredWrite(db, () => write(2_000));
    assert.ok(live > 0 && second.writtenBytes < live + second.writtenBytes, "the live writer's pages are outside the converter's counter");
    assert.ok(Math.abs(second.writtenBytes - first.writtenBytes) <= 2 * pageSize, "two equal chunks measure alike, whatever ran between them");
  } finally { close(); }
});
