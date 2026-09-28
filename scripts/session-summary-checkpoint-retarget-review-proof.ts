import assert from "node:assert/strict";

import { updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendRows, directRead, drainSummary, eventId, fixture, initialUntil,
  sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-checkpoint-retarget-review", 1);

async function main() {
  const buffer = fixture("checkpoint-retarget-review.sqlite");
  try {
    const size = 50_000;
    appendRows(buffer, size);
    await drainSummary(buffer, initialUntil, 20);
    const db = buffer.database;
    const otherSession = "00000000-0000-4000-8000-000000000107";
    assert.equal(db.prepare("update buffered_events set session_id = ? where id = ?")
      .run(otherSession, eventId(size - 1)).changes, 1);
    assert.equal((db.prepare(`select count(*) as n from session_sync_summary_repairs
      where session_id = ?`).get(sessionId) as { n: number }).n, 1);
    const result = await updateSessionSummary(db, sessionId, initialUntil,
      { read: directRead(buffer) });
    console.log(JSON.stringify({ size, fullRecompute: result.fullRecompute,
      fallbackReason: result.fallbackReason, rowsRead: result.rowsRead,
      complete: result.complete }));
    assert.equal(result.fullRecompute, false,
      "retargeting a scanned checkpoint row must use its queued segment repair");
    completion.check("checkpoint_retarget_uses_bounded_repair");
    completion.complete();
  } finally { buffer.close(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
