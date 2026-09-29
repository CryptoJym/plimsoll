import assert from "node:assert/strict";

import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendRows, directRead, drainSummary, fixture, initialUntil, sessionId,
} from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-checkpoint-window", 3);

async function main() {
  const buffer = fixture("checkpoint-window.sqlite");
  try {
    appendRows(buffer, 12_500);
    await drainSummary(buffer, initialUntil, 5);
    const db = buffer.database;
    assert.equal(db.prepare(`delete from buffered_events
      where session_id = ? and rowid > 3500`).run(sessionId).changes, 9_000);
    const read = directRead(buffer);
    const cursors: number[] = [];
    for (let pass = 0; pass < 2; pass += 1) {
      const result = await updateSessionSummary(db, sessionId, initialUntil, { read });
      const row = db.prepare(`select accumulator_json as accumulatorJson
        from session_sync_summary_state where session_id = ?`)
        .get(sessionId) as { accumulatorJson: string };
      const cursor = JSON.parse(row.accumulatorJson).checkpointSearchRowid as number | null;
      cursors.push(cursor ?? 0);
      assert.equal(result.complete, false);
      assert.equal(result.snapshot, null);
      assert.equal(result.fullRecompute, false);
      assert.equal(result.fallbackReason, "checkpoint_search_in_progress");
      assert.ok(cursor !== null && cursor > 0);
    }
    assert.ok(cursors[0]! > cursors[1]!, JSON.stringify(cursors));
    completion.check("missing_checkpoint_search_cursor_survives_two_cycles");

    let final: Awaited<ReturnType<typeof updateSessionSummary>> | undefined;
    let maxRowsRead = 0;
    for (let pass = 0; pass < 20; pass += 1) {
      final = await updateSessionSummary(db, sessionId, initialUntil, { read });
      maxRowsRead = Math.max(maxRowsRead, final.rowsRead);
      assert.equal(final.fullRecompute, false);
      if (final.complete) break;
      assert.equal(final.snapshot, null);
    }
    assert.ok(final?.complete);
    assert.ok(maxRowsRead <= 5_000);
    assert.deepEqual(final.snapshot,
      collectSessionSnapshots(db, { sessionIds: [sessionId], until: initialUntil })[0]);
    completion.check("erased_tail_repairs_without_session_rebuild");
    console.log(JSON.stringify({ rows: 12_500, erased: 9_000, cursors, maxRowsRead,
      events: final.snapshot?.events }));
    completion.check("checkpoint_windows_bound_each_pass");
    completion.complete();
  } finally { buffer.close(); }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
