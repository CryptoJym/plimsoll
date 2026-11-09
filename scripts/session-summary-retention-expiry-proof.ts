import assert from "node:assert/strict";

import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendRows, directRead, drainSummary, fixture, initialUntil, sessionId } from "./session-summary-repair-fixture";

// retention_expiry_aware_v1 (plimsoll N89, N105): a raw row that leaves the
// ledger because its retention window elapsed is not an edit. Its receipt is
// written before the delete, the summary keeps the row it already counted,
// and nothing is re-dirtied, re-queued or searched for. An erasure (a delete
// with no receipt) still takes the repair path proved by
// session-summary-checkpoint-erasure-proof.ts.
const completion = createProofCompletion("session-summary-retention-expiry", 2);
const size = 9_000;

async function run() {
  const buffer = fixture("retention-expiry.sqlite");
  try {
    appendRows(buffer, size);
    ensureSessionSummarySchema(buffer.database);
    const initial = await drainSummary(buffer, initialUntil, 20);
    const before = initial.result.snapshot;
    // Every 17th fixture row is a kept duplicate copy, so the summary counts fewer than `size` events.
    assert.ok(before && before.events > 0 && before.events <= size, "the fixture summary must count the eligible rows");
    const stored = buffer.database.prepare(`select high_water as highWater, checkpoint_id as checkpointId
      from session_sync_summary_state where session_id = ?`).get(sessionId) as { highWater: number; checkpointId: string };
    assert.equal(stored.highWater, size);
    const revisionBefore = (buffer.database.prepare("select mutation_revision as revision from session_sync_summary_control")
      .get() as { revision: number }).revision;

    // Expire the oldest 4,500 rows and the checkpoint row the way buffer.ts
    // retention does: receipt first, then the delete.
    const receipt = buffer.database.prepare(`insert into raw_retention_receipts
      (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason)
      select id, rowid, created_at, privacy_generation, ?, 'retention_window_elapsed' from buffered_events where rowid = ?`);
    const remove = buffer.database.prepare("delete from buffered_events where rowid = ?");
    const expired = buffer.database.transaction((rowids: number[]) => {
      let deleted = 0;
      for (const rowid of rowids) {
        assert.equal(receipt.run("2026-10-10T00:00:00.000Z", rowid).changes, 1);
        deleted += remove.run(rowid).changes;
      }
      return deleted;
    });
    const rowids = [...Array.from({ length: 4_500 }, (_, index) => index + 1), stored.highWater];
    assert.equal(expired(rowids), rowids.length);

    const dirty = buffer.database.prepare("select count(*) as n from session_sync_summary_dirty where session_id = ?")
      .get(sessionId) as { n: number };
    const repairs = buffer.database.prepare("select count(*) as n from session_sync_summary_repairs where session_id = ?")
      .get(sessionId) as { n: number };
    const revisionAfter = (buffer.database.prepare("select mutation_revision as revision from session_sync_summary_control")
      .get() as { revision: number }).revision;
    assert.equal(dirty.n, 0, "a retention expiry must not dirty the session");
    assert.equal(repairs.n, 0, "a retention expiry must not queue a segment repair");
    assert.equal(revisionAfter, revisionBefore, "a retention expiry must not advance the ledger revision");

    const revisit = await updateSessionSummary(buffer.database, sessionId, initialUntil, { read: directRead(buffer) });
    assert.equal(revisit.complete, true, "the summary stays complete when its checkpoint row expired");
    assert.equal(revisit.fullRecompute, false, "an expired checkpoint must not restart from row zero");
    assert.equal(revisit.rowsRead, 0, "an expired checkpoint reads no raw rows");
    assert.deepEqual(revisit.snapshot, before, "the summary keeps the rows it already counted");
    assert.notEqual(collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: initialUntil })[0]?.events,
      before.events, "the raw table no longer holds every row, so the summary is the record");
    completion.check("retention_expiry_keeps_summary");
  } finally {
    buffer.close();
  }
}

// A host upgraded from 0.7.58 carries summaries whose checkpoint search began
// under the old trigger: the receipt exists, the state reads complete=false
// with a search cursor. The widened rule resumes them without a search or a
// recompute.
async function runUpgradedSearch() {
  const buffer = fixture("retention-expiry-upgrade.sqlite");
  try {
    appendRows(buffer, size);
    ensureSessionSummarySchema(buffer.database);
    const initial = await drainSummary(buffer, initialUntil, 20);
    const before = initial.result.snapshot;
    assert.ok(before && before.events > 0);
    const stored = buffer.database.prepare(`select high_water as highWater, checkpoint_id as checkpointId
      from session_sync_summary_state where session_id = ?`).get(sessionId) as { highWater: number; checkpointId: string };
    // Retention expired the 4,200 rows below the checkpoint (receipt, then delete), so the
    // checkpoint's nearest surviving predecessor lies beyond one 4,096-rowid search window.
    const expireBlock = buffer.database.transaction((lo: number, hi: number) => {
      buffer.database.prepare(`insert into raw_retention_receipts (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason)
        select id, rowid, created_at, privacy_generation, '2026-10-10T00:00:00.000Z', 'retention_window_elapsed'
        from buffered_events where rowid between ? and ?`).run(lo, hi);
      return buffer.database.prepare("delete from buffered_events where rowid between ? and ?").run(lo, hi).changes;
    });
    assert.equal(expireBlock(stored.highWater - 4_200, stored.highWater - 1), 4_200);
    // The old path: an erasure-style delete of the checkpoint row (no receipt) queues the repair and starts the search.
    assert.equal(buffer.database.prepare("delete from buffered_events where rowid = ?").run(stored.highWater).changes, 1);
    const searching = await updateSessionSummary(buffer.database, sessionId, initialUntil, { read: directRead(buffer) });
    assert.equal(searching.fallbackReason, "checkpoint_search_in_progress", "the fixture must reach a search in progress");
    // The receipt retention wrote for that row is found on the next pass.
    buffer.database.prepare(`insert into raw_retention_receipts (event_id, raw_rowid, raw_created_at, raw_generation, expired_at, reason)
      values (?, ?, '2026-09-20T00:00:00.000Z', null, '2026-10-10T00:00:00.000Z', 'retention_window_elapsed')`).run(stored.checkpointId, stored.highWater);
    const resumed = await drainSummary(buffer, initialUntil, 3);
    assert.equal(resumed.fullRecomputes, 0, "a resumed search must not restart from row zero");
    assert.equal(resumed.result.complete, true);
    // The erasure's own segment repair re-reads a segment whose other rows expired, so it may
    // drop at most that segment (rows 8,193 to 9,000); nothing below it is touched.
    assert.ok(resumed.result.snapshot && resumed.result.snapshot.events >= before.events - 808,
      `resumed summary kept ${resumed.result.snapshot?.events} of ${before.events} events`);
    completion.check("upgraded_search_in_progress_resumes_as_valid");
  } finally {
    buffer.close();
  }
}

run().then(runUpgradedSearch).then(() => completion.complete()).catch((error) => { console.error(error); process.exit(1); });
