import assert from "node:assert/strict";

import { createProofCompletion } from "./lib/proof-completion";
import { appendRows, daemonCycle, drainSummary, expectedWire, fixture,
  initialUntil, sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-future-maturity", 3);

async function main() {
  const size = 50_000;
  const buffer = fixture("future-maturity.sqlite");
  try {
    const start = Date.parse(initialUntil);
    const future = new Map<number, string>();
    for (let due = 1; due <= 3; due += 1) {
      for (let segment = 0; segment < 4; segment += 1) {
        future.set(4_096 * (segment * 3 + due) + 5,
          new Date(start + due * 1_000).toISOString());
      }
    }
    appendRows(buffer, size, (index) => future.get(index) ?? null);
    const initial = await drainSummary(buffer, initialUntil, 30);
    assert.equal(initial.fullRecomputes, 0);
    const initialSync = await daemonCycle(buffer, initialUntil);
    assert.ok(initialSync.result.ok && initialSync.result.summaryComplete);
    completion.check("future_rows_initially_excluded");
    const cycles: Array<{ due: number; passes: number; rowsRead: number; fullRecomputes: number; maxRowsRead: number }> = [];
    for (let due = 1; due <= 3; due += 1) {
      const until = new Date(start + due * 1_000).toISOString();
      let rowsRead = 0;
      let fullRecomputes = 0;
      let maxRowsRead = 0;
      let complete = false;
      let passes = 0;
      for (; passes < 30; passes += 1) {
        const sync = await daemonCycle(buffer, until);
        console.log(JSON.stringify({ due, cycle: passes + 1,
          rowsRead: sync.result.summaryStats.rowsRead,
          fullRecomputes: sync.result.summaryStats.fullRecomputes,
          summaryComplete: sync.result.summaryComplete,
          pending: sync.result.pendingSummaryReasons,
          horizon: sync.state.lastSuccessfulUntil,
          sent: sync.sent.length }));
        rowsRead += sync.result.summaryStats.rowsRead;
        maxRowsRead = Math.max(maxRowsRead, sync.result.summaryStats.rowsRead);
        fullRecomputes += sync.result.summaryStats.fullRecomputes;
        assert.ok(sync.result.summaryStats.rowsRead <= 4_096,
          `due ${due} daemon cycle ${passes + 1} exceeded the scan budget`);
        if (sync.result.summaryComplete) {
          assert.equal(sync.result.ok, true);
          assert.equal(sync.state.caughtUp, true);
          assert.deepEqual(sync.sent.find((row) => row.session.id === sessionId), expectedWire(buffer, until));
          complete = true;
          passes += 1;
          break;
        }
        assert.notEqual(sync.state.lastSuccessfulUntil, until,
          "partial summary must not advance the daemon horizon");
        assert.equal(sync.sent.length, 0, "partial summary must not be sent");
      }
      assert.equal(complete, true, `due cycle ${due} did not drain`);
      assert.equal(fullRecomputes, 0, `due cycle ${due} restarted the long summary`);
      assert.ok(maxRowsRead <= 4_096, `due cycle ${due} read ${maxRowsRead} in one pass`);
      cycles.push({ due, passes, rowsRead, fullRecomputes, maxRowsRead });
    }
    completion.check("maturing_rows_never_force_full_recompute");
    completion.check("bounded_cycle_reads_and_exact_wire");
    console.log(JSON.stringify({ size, futureRows: future.size, cycles, maxRowsPerDaemonCycle: 4_096 }));
    completion.complete();
  } finally { buffer.close(); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
