import assert from "node:assert/strict";

import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendBackdatedRow, appendRows, daemonCycle, directRead, drainSummary, eventId,
  expectedWire, fixture, initialUntil, sessionId } from "./session-summary-repair-fixture";

const only = process.argv[2];
assert.ok(only === undefined || only === "high_water" || only === "historical_cursor");
const completion = createProofCompletion("session-summary-checkpoint-erasure", only ? 1 : 3);
const size = 50_000;

async function caseRun(kind: "high_water" | "historical_cursor") {
  const buffer = fixture(`checkpoint-erasure-${kind}.sqlite`);
  try {
    appendRows(buffer, size);
    if (kind === "historical_cursor") {
      assert.equal(buffer.database.prepare("update buffered_events set observed_at = ? where id = ?")
        .run("2026-09-24T00:00:00.000Z", eventId(100)).changes, 1);
    }
    ensureSessionSummarySchema(buffer.database);
    const initial = await drainSummary(buffer, initialUntil, 20);
    const stored = buffer.database.prepare(`select high_water as highWater, checkpoint_id as checkpointId,
      accumulator_json as accumulatorJson from session_sync_summary_state where session_id = ?`)
      .get(sessionId) as { highWater: number; checkpointId: string; accumulatorJson: string };
    const cursor = JSON.parse(stored.accumulatorJson) as { cursorRowid: number; cursorId: string };
    assert.equal(stored.highWater, size);
    if (kind === "high_water") assert.equal(cursor.cursorRowid, stored.highWater);
    else assert.notEqual(cursor.cursorRowid, stored.highWater);
    const erasedId = kind === "high_water" ? stored.checkpointId : cursor.cursorId;
    assert.equal(buffer.database.prepare("delete from buffered_events where id = ?").run(erasedId).changes, 1);
    const queued = buffer.database.prepare(`select segment from session_sync_summary_repairs
      where session_id = ?`).all(sessionId) as Array<{ segment: number }>;
    assert.equal(queued.length, 1, "the delete trigger must queue one segment");

    const repaired = await drainSummary(buffer, initialUntil, 3);
    assert.equal(repaired.fullRecomputes, 0, "erasure must not restart from row zero");
    assert.ok(repaired.rowsRead <= 4_096,
      `${kind} erasure read ${repaired.rowsRead} rows of ${size}`);
    assert.deepEqual(repaired.result.snapshot,
      collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: initialUntil })[0]);
    const revisit = await updateSessionSummary(buffer.database, sessionId, initialUntil,
      { read: directRead(buffer) });
    assert.equal(revisit.complete, true, "the repaired checkpoint must remain valid");
    assert.equal(revisit.fullRecompute, false);
    assert.deepEqual(revisit.snapshot,
      collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: initialUntil })[0]);
    const sync = await daemonCycle(buffer, initialUntil);
    assert.ok(sync.result.ok && sync.result.summaryComplete);
    assert.deepEqual(sync.sent.find((row) => row.session.id === sessionId), expectedWire(buffer, initialUntil));
    assert.equal(sync.state.caughtUp, true);

    if (kind === "high_water") {
      // SQLite may reuse the erased terminal rowid. The next append must
      // still enter the queue and reach the wire once.
      appendBackdatedRow(buffer, size + 1);
      const reused = buffer.database.prepare("select rowid from buffered_events where id = ?")
        .get(eventId(size + 1)) as { rowid: number };
      assert.equal(reused.rowid, stored.highWater, "fixture must exercise terminal rowid reuse");
      const appended = await drainSummary(buffer, initialUntil, 3);
      assert.equal(appended.fullRecomputes, 0);
      assert.deepEqual(appended.result.snapshot,
        collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: initialUntil })[0]);
    }
    console.log(JSON.stringify({ kind, size, initialPasses: initial.passes,
      erasurePasses: repaired.passes, rowsRead: repaired.rowsRead,
      boundRowsPerErasure: 4_096, fullRecomputes: repaired.fullRecomputes }));
  } finally { buffer.close(); }
}

async function main() {
  if (only) {
    await caseRun(only as "high_water" | "historical_cursor");
    completion.check(`${only}_erasure_bounded_exact`);
    completion.complete();
    return;
  }
  await caseRun("high_water");
  completion.check("high_water_erasure_bounded_exact");
  await caseRun("historical_cursor");
  completion.check("historical_cursor_erasure_bounded_exact");
  completion.check("checkpoint_remains_valid_and_wire_exact");
  completion.complete();
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
