import assert from "node:assert/strict";

import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { sessionSummaryCurrent, updateSessionSummary,
  type SessionReadQuery } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendRows, directRead, drainSummary, eventId, fixture, initialUntil,
  sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-concurrent-repair-review", 1);

async function main() {
  const buffer = fixture("concurrent-repair-review.sqlite");
  let peer: ReturnType<typeof fixture> | undefined;
  try {
    appendRows(buffer, 8_192);
    await drainSummary(buffer, initialUntil, 5);
    const db = buffer.database;
    peer = fixture("concurrent-repair-review.sqlite");
    assert.equal(db.prepare("update buffered_events set input_tokens = 17 where id = ?")
      .run(eventId(10)).changes, 1);
    assert.equal(db.prepare("update buffered_events set output_tokens = 23 where id = ?")
      .run(eventId(5_000)).changes, 1);
    assert.deepEqual((db.prepare(`select segment from session_sync_summary_repairs
      where session_id = ? order by segment`).all(sessionId) as Array<{ segment: number }>).map(r => r.segment), [0, 1]);

    const underlyingRead = directRead(buffer);
    const peerRead = directRead(peer);
    let release!: () => void;
    let signal!: () => void;
    const held = new Promise<void>(resolve => { release = resolve; });
    const reached = new Promise<void>(resolve => { signal = resolve; });
    let paused = false;
    const heldRead = async <T,>(queries: SessionReadQuery[]): Promise<T[]> => {
      const rows = await peerRead<T>(queries);
      if (!paused && queries[0]?.sql.includes("e.rowid <= @upperRowid")) {
        paused = true;
        signal();
        await held;
      }
      return rows;
    };

    // B captures the old state and reads segment 0, then pauses. A repairs
    // segment 0 and segment 1 and clears both queue entries. B commits last.
    const bPromise = updateSessionSummary(peer.database, sessionId, initialUntil, { read: heldRead });
    await reached;
    const aFirst = await updateSessionSummary(db, sessionId, initialUntil,
      { read: underlyingRead });
    const aSecond = await updateSessionSummary(db, sessionId, initialUntil,
      { read: underlyingRead });
    assert.equal(aFirst.complete, false);
    assert.equal(aSecond.complete, true);
    const expected = collectSessionSnapshots(db, { sessionIds: [sessionId], until: initialUntil })[0];
    assert.deepEqual(aSecond.snapshot, expected);
    release();
    const b = await bPromise;
    assert.equal(b.complete, true);
    assert.equal(sessionSummaryCurrent(db, sessionId, initialUntil,
      b.mutationRevision, b.highWater), true,
    "the final upload fence accepts the overwritten state");
    const stored = await updateSessionSummary(db, sessionId, initialUntil,
      { read: underlyingRead });
    console.log(JSON.stringify({ aFirst: { complete: aFirst.complete, rowsRead: aFirst.rowsRead },
      aSecond: { complete: aSecond.complete, rowsRead: aSecond.rowsRead },
      b: { complete: b.complete, rowsRead: b.rowsRead },
      expected: { inputTokens: expected?.inputTokens, outputTokens: expected?.outputTokens },
      stored: { inputTokens: stored.snapshot?.inputTokens, outputTokens: stored.snapshot?.outputTokens } }));
    assert.deepEqual(stored.snapshot, expected,
      "a stale repair pass must not overwrite a later completed segment repair");
    completion.check("concurrent_repair_preserves_both_segments");
    completion.complete();
  } finally { peer?.close(); buffer.close(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
