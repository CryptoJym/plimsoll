import assert from "node:assert/strict";

import { collectSessionSnapshots } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, updateSessionSummary, type SessionReadQuery } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendBackdatedRow, appendRows, daemonCycle, directRead, drainSummary, eventId,
  expectedWire, fixture, initialUntil, sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-queued-append-repair", 3);

async function main() {
  const buffer = fixture("queued-append-repair.sqlite");
  try {
    const size = 5_796;
    appendRows(buffer, size);
    ensureSessionSummarySchema(buffer.database);
    await drainSummary(buffer, initialUntil, 5);
    assert.equal(buffer.database.prepare("update buffered_events set input_tokens = 19 where id = ?")
      .run(eventId(5_000)).changes, 1);

    const underlyingRead = directRead(buffer);
    let appended = false;
    const read = async <T,>(queries: SessionReadQuery[]): Promise<T[]> => {
      const rows = await underlyingRead<T>(queries);
      // The incremental queue was read. Insert before the segment repair's
      // independent SQL read, with a rowid inside that segment's range.
      if (!appended && queries[0]?.sql.includes("select e.rowid as rowid") &&
          queries[0].sql.includes("from session_sync_summary_rows r join buffered_events e")) {
        assert.equal(rows.length, 0);
        appendBackdatedRow(buffer, size + 1);
        appended = true;
      }
      return rows;
    };
    const first = await updateSessionSummary(buffer.database, sessionId, initialUntil, { read });
    assert.equal(appended, true, "append must land after incremental read and before repair");
    assert.equal(first.complete, false, "the queued append must fence the partial snapshot");
    assert.equal(first.snapshot, null);
    completion.check("append_interleaved_between_reads");

    const final = await drainSummary(buffer, initialUntil, 3);
    assert.equal(final.fullRecomputes, 0);
    assert.deepEqual(final.result.snapshot,
      collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: initialUntil })[0]);
    completion.check("queued_append_folded_once");

    const sync = await daemonCycle(buffer, initialUntil);
    assert.ok(sync.result.ok && sync.result.summaryComplete);
    assert.deepEqual(sync.sent.find((row) => row.session.id === sessionId), expectedWire(buffer, initialUntil));
    assert.equal(sync.state.caughtUp, true);
    completion.check("exact_wire_and_caught_up");
    console.log(JSON.stringify({ size, appended, firstRowsRead: first.rowsRead,
      drainPasses: final.passes, drainRowsRead: final.rowsRead, fullRecomputes: final.fullRecomputes }));
    completion.complete();
  } finally { buffer.close(); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
