/**
 * B1 / B10a (collector): the S1b runway with G per host from counted cardinalities, one G, and G remaining
 * (docs/lean/CONTRACTS.md C2; BUDGETS.md §3, §4.2; ARCHITECTURE.md §2.4). Ports fixtures/s1b_runway_host_bound.py (the
 * reviewer's 333-session counterexample and the Studio4 census) and s1b_runway_geometry.py. Pending until B1 lands
 * packages/collector-cli/src/lean/runway.ts.
 */
import assert from "node:assert/strict";
import test from "node:test";

import { fn, loadSurface, pending } from "./_pending";

const MB = 1_000_000, GiB = 1024 ** 3;
const F = 700 * 1.5, T = 280 * 1.3, S = 1300, SD = 260 * 1.5, DR = 260 * 1.5, RB = 400, SEG = 830, I = 125 + 2 * 92;
type Census = { measured: boolean; usageRows: number; rawRows: number; sessions: number; sessionDayRows: number; dayRows: number; rollupBuckets: number; dayTargets: number; rawBytes: number; ledgerBytes: number };
const STUDIO1: Census = { measured: true, usageRows: 171_840, rawRows: 276_454, sessions: 8_070, sessionDayRows: 12_105, dayRows: 1_400, rollupBuckets: 144 * 90, dayTargets: 180, rawBytes: 623_939_584, ledgerBytes: 1_342_111_744 };
const STUDIO4: Census = { measured: true, usageRows: 79_259, rawRows: 352_885, sessions: 5_141, sessionDayRows: 5_654, dayRows: 332, rollupBuckets: 455, dayTargets: 34, rawBytes: 833_286_144, ledgerBytes: 1_504_444_416 };
const REVIEWER_333: Census = { ...STUDIO1, sessions: 57_223, sessionDayRows: 85_834 };
const SCALED: Census = { ...STUDIO1, measured: false, usageRows: 174_820, rawRows: 281_249, rawBytes: 741_421_056, ledgerBytes: 1_365_389_312 };
const need = (h: Census) => h.usageRows * (F + T) + h.sessions * S + h.sessionDayRows * SD + h.dayRows * DR + h.rollupBuckets * RB + (1.2 * h.sessions + h.rollupBuckets + h.dayTargets) * SEG + h.rawRows * I;

async function surface() {
  const mod = await loadSurface("../../../packages/collector-cli/src/lean/runway.ts");
  return {
    estimateHostG: fn(mod, "estimateHostG") as (census: Census) => { gateBytes: number; hostBytes: number; basis: string } | null,
    holdRunway: fn(mod, "holdRunway") as (input: Record<string, number>) => { runwayDays: number; gRemainingBytes: number; holdGrowthPerDay: number; rung: string },
  };
}

test("B1 C2: G_gate bounds each measured host's real need for its own mix (Studio1, the Studio4 census, the reviewer's 333-session host) and is null for scaled counts", pending("B1"), async () => {
  const { estimateHostG } = await surface();
  for (const [name, host] of Object.entries({ studio1: STUDIO1, studio4: STUDIO4, reviewer333: REVIEWER_333 })) {
    const g = estimateHostG(host);
    assert.ok(g && g.gateBytes >= need(host), `${name}: gate ${g?.gateBytes} >= need ${need(host)}`);
    assert.equal(g.basis, "census");
  }
  assert.ok(need(REVIEWER_333) > 457.3 * MB, "the reviewer's host needs more than round 6's 457 MB gate");
  assert.ok(estimateHostG(STUDIO1)!.hostBytes >= 346.8 * MB, "Studio1 at or above the round-6 reviewer's 346.8 MB lower bound");
  assert.equal(estimateHostG(SCALED), null, "scaled counts are not a census: no gate value, no hold");
});

test("B10a C2: at the gate's 63-day threshold the host really has >= 63 days; the same G_gate sizes the multiplier", pending("B10a"), async () => {
  const { estimateHostG, holdRunway } = await surface();
  const gross = 0.30 * GiB, reserve = 25 * GiB, rebuild = 1.2 * REVIEWER_333.ledgerBytes;
  const gate = estimateHostG(REVIEWER_333)!.gateBytes;
  const growth = (g: number) => gross * (1 + (g / REVIEWER_333.rawBytes) * (REVIEWER_333.rawBytes / REVIEWER_333.ledgerBytes));
  const free = reserve + rebuild + gate + 63 * growth(gate);
  const rule = holdRunway({ freeBytes: free, reserveBytes: reserve, rebuildHeadroomBytes: rebuild, gGateBytes: gate, newTableBytesNow: 0, grossGrowthP95PerDay: gross, rawBytes: REVIEWER_333.rawBytes, ledgerBytes: REVIEWER_333.ledgerBytes });
  assert.ok(Math.abs(rule.runwayDays - 63) < 1e-6, `gate runway ${rule.runwayDays}`);
  assert.ok(Math.abs(rule.holdGrowthPerDay - growth(gate)) < 1, "multiplier uses G_gate");
  const trueDays = (free - reserve - rebuild - need(REVIEWER_333)) / growth(need(REVIEWER_333));
  assert.ok(trueDays >= 63, `the host really has ${trueDays.toFixed(2)} days`);
});

test("B10a C2: mid-conversion the runway subtracts G remaining, never full G, so the < 5-day rung fires only when the true runway is under 5", pending("B10a"), async () => {
  const { estimateHostG, holdRunway } = await surface();
  const gross = 0.30 * GiB, reserve = 25 * GiB, rebuild = 1.2 * STUDIO4.ledgerBytes;
  const gate = estimateHostG(STUDIO4)!.gateBytes;
  const growth = gross * (1 + (gate / STUDIO4.rawBytes) * (STUDIO4.rawBytes / STUDIO4.ledgerBytes));
  const free0 = reserve + rebuild + gate + 5.4 * growth;
  const converted = 0.6 * gate;
  const mid = holdRunway({ freeBytes: free0 - converted, reserveBytes: reserve, rebuildHeadroomBytes: rebuild, gGateBytes: gate, newTableBytesNow: converted, grossGrowthP95PerDay: gross, rawBytes: STUDIO4.rawBytes, ledgerBytes: STUDIO4.ledgerBytes });
  assert.ok(Math.abs(mid.gRemainingBytes - (gate - converted)) < 1);
  assert.ok(Math.abs(mid.runwayDays - 5.4) < 0.01, `runway ${mid.runwayDays}`);
  assert.equal(mid.rung, "none");
  const done = holdRunway({ freeBytes: free0 - gate, reserveBytes: reserve, rebuildHeadroomBytes: rebuild, gGateBytes: gate, newTableBytesNow: gate, grossGrowthP95PerDay: gross, rawBytes: STUDIO4.rawBytes, ledgerBytes: STUDIO4.ledgerBytes });
  assert.equal(done.gRemainingBytes, 0);
});

test("B10a: a Studio1-shaped host with 50 GiB free and 0.30 GiB/day gross growth is refused a 42-day hold under the corrected geometry (s1b_runway_geometry)", pending("B10a"), async () => {
  const { estimateHostG, holdRunway } = await surface();
  const gate = estimateHostG(STUDIO1)!.gateBytes;
  const rule = holdRunway({ freeBytes: 50 * GiB, reserveBytes: 25 * GiB, rebuildHeadroomBytes: 1.2 * STUDIO1.ledgerBytes, gGateBytes: gate, newTableBytesNow: 0, grossGrowthP95PerDay: 0.30 * GiB, rawBytes: STUDIO1.rawBytes, ledgerBytes: STUDIO1.ledgerBytes });
  assert.ok(rule.runwayDays < 63, `runway ${rule.runwayDays.toFixed(1)} d < max(30, 1.5 x 42)`);
});
