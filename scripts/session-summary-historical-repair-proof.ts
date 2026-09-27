import assert from "node:assert/strict";

import { createProofCompletion } from "./lib/proof-completion";
import { appendRows, daemonCycle, drainSummary, eventId, expectedWire, fixture, scheduleCycle,
  initialUntil, sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-historical-repair", 3);

async function main() {
  const size = Number(process.argv[2] ?? 50_000);
  assert.ok([50_000, 250_000, 1_880_000].includes(size));
  const buffer = fixture(`historical-${size}.sqlite`);
  try {
    appendRows(buffer, size);
    const initial = await drainSummary(buffer, initialUntil, Math.ceil(size / 5_000) + 50);
    assert.equal(initial.fullRecomputes, 0);
    completion.check("initial_summary_built");
    const edits: Array<() => void> = [
      () => { assert.equal(buffer.database.prepare("update buffered_events set input_tokens = 17 where id = ?")
        .run(eventId(10)).changes, 1); },
      () => { assert.equal(buffer.database.prepare("delete from buffered_events where id = ?")
        .run(eventId(11)).changes, 1); },
      () => { assert.equal(buffer.database.prepare("update buffered_events set cost_usd = 1.25 where id = ?")
        .run(eventId(12)).changes, 1); },
      () => { assert.equal(buffer.database.prepare("delete from buffered_events where id = ?")
        .run(eventId(13)).changes, 1); },
    ];
    let priorHorizon = "";
    const measurements: Array<{ edit: number; rowsRead: number; passes: number; fullRecomputes: number }> = [];
    for (const [index, edit] of edits.entries()) {
      edit();
      const until = new Date(Date.parse(initialUntil) + (index + 1) * 1_000).toISOString();
      const scheduled = scheduleCycle(buffer, until);
      const repaired = await drainSummary(buffer, until, 3);
      measurements.push({ edit: index + 1, rowsRead: repaired.rowsRead,
        passes: repaired.passes, fullRecomputes: repaired.fullRecomputes });
      assert.equal(repaired.fullRecomputes, 0, "an edit must not reset the session aggregate");
      assert.ok(repaired.rowsRead <= 4_096,
        `edit ${index + 1} reread ${repaired.rowsRead} rows of ${size}`);
      assert.deepEqual(repaired.result.snapshot,
        (await import("../packages/collector-cli/src/session-sync")).collectSessionSnapshots(
          buffer.database, { sessionIds: [sessionId], until })[0]);
      const sync = await daemonCycle(buffer, until, scheduled);
      assert.ok(sync.result.ok && sync.result.summaryComplete);
      assert.deepEqual(sync.sent.find((row) => row.session.id === sessionId), expectedWire(buffer, until));
      assert.equal(sync.state.caughtUp, true);
      assert.equal(sync.state.lastSuccessfulUntil, until);
      assert.notEqual(sync.state.lastSuccessfulUntil, priorHorizon);
      priorHorizon = until;
    }
    completion.check("repeated_edits_and_erasures_read_at_most_one_segment");
    completion.check("exact_wire_and_advancing_daemon_horizon");
    console.log(JSON.stringify({ size, initialPasses: initial.passes, measurements, boundRowsPerEdit: 4_096 }));
    completion.complete();
  } finally { buffer.close(); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
