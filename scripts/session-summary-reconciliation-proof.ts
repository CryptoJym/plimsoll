import assert from "node:assert/strict";

import { ensureSessionSummarySchema, sessionSummaryCounters, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";
import { appendBackdatedRow, appendRows, daemonCycle, directRead, eventId, expectedWire, fixture, initialUntil,
  sessionId } from "./session-summary-repair-fixture";

const completion = createProofCompletion("session-summary-reconciliation", 5);
async function main() {
const buffer = fixture("reconciliation.sqlite");
try {
  const size = 5_796;
  appendRows(buffer, size);
  ensureSessionSummarySchema(buffer.database);
  let restarts = 0;
  let rowsRead = 0;
  let passes = 0;
  let final: Awaited<ReturnType<typeof updateSessionSummary>> | null = null;
  const bound = Math.ceil(size / 5_000) + 2;
  const started = performance.now();
  for (let pass = 1; pass <= bound; pass += 1) {
    final = await updateSessionSummary(buffer.database, sessionId, initialUntil, {
      read: directRead(buffer),
    });
    passes += 1;
    rowsRead += final.rowsRead;
    restarts += Number(final.fullRecompute);
    console.log(JSON.stringify({ pass, rowsRead: final.rowsRead,
      restart: final.fullRecompute, pending: final.fallbackReason }));
    if (final.complete) break;
    // Reconciliation rewrites a row already folded into the partial prefix.
    // The real trigger records raw_update and advances the session revision.
    const before = (buffer.database.prepare(`select mutation_revision as value
      from session_sync_summary_revision where session_id = ?`).get(sessionId) as { value: number } | undefined)?.value ?? 0;
    assert.equal(buffer.database.prepare("update buffered_events set output_tokens = ? where id = ?")
      .run(10 + pass, eventId(10)).changes, 1);
    const dirty = buffer.database.prepare("select reason from session_sync_summary_dirty where session_id = ?")
      .get(sessionId) as { reason: string } | undefined;
    assert.equal(dirty?.reason, "raw_update");
    const after = (buffer.database.prepare(`select mutation_revision as value
      from session_sync_summary_revision where session_id = ?`).get(sessionId) as { value: number }).value;
    assert.ok(after > before);
  }
  assert.ok(final);
  assert.ok(final.complete && passes <= bound,
    `restart loop: ${JSON.stringify({ size, passes, bound, restarts, rowsRead })}`);
  const elapsedMs = Math.round(performance.now() - started);
  assert.ok(elapsedMs < 300_000, `local repair exceeded rollout pass bound: ${elapsedMs} ms`);
  completion.check("bounded_passes_under_reconciliation");
  const sync = await daemonCycle(buffer, initialUntil);
  assert.ok(sync.result.ok && sync.result.summaryComplete);
  assert.deepEqual(sync.sent.find((row) => row.session.id === sessionId), expectedWire(buffer, initialUntil));
  completion.check("collector_eligible_wire_equals_from_scratch");
  assert.equal(sync.state.caughtUp, true);
  assert.equal(sync.state.lastSuccessfulUntil, initialUntil);
  completion.check("daemon_horizon_caught_up");
  assert.equal(buffer.database.prepare(`update buffered_events
    set usage_duplicate_reason = 'sse_usage_kept_copy' where id = ?`).run(eventId(1)).changes, 1);
  const deduplicated = await updateSessionSummary(buffer.database, sessionId, initialUntil,
    { read: directRead(buffer) });
  const expected = (await import("../packages/collector-cli/src/session-sync"))
    .collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: initialUntil })[0];
  assert.deepEqual(deduplicated.snapshot, expected,
    "eligibility changes must repair a previously included response span");
  completion.check("duplicate_eligibility_change_repaired");
  buffer.database.prepare("update buffered_events set output_tokens = 88 where id = ?")
    .run(eventId(10));
  const partialRepair = await updateSessionSummary(buffer.database, sessionId, initialUntil,
    { read: directRead(buffer), maxRows: 1_000 });
  assert.equal(partialRepair.complete, false);
  appendBackdatedRow(buffer, size + 1);
  const nextUntil = new Date(Date.parse(initialUntil) + 1_000).toISOString();
  let appended = await updateSessionSummary(buffer.database, sessionId, nextUntil,
    { read: directRead(buffer) });
  for (let pass = 0; pass < 4 && !appended.complete; pass += 1) {
    appended = await updateSessionSummary(buffer.database, sessionId, nextUntil,
      { read: directRead(buffer) });
  }
  assert.equal(appended.complete, true);
  const appendedExpected = (await import("../packages/collector-cli/src/session-sync"))
    .collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until: nextUntil })[0];
  assert.deepEqual(appended.snapshot, appendedExpected,
    "backdated append during a partial repair must enter the snapshot once");
  completion.check("backdated_append_during_repair_retained");
  console.log(JSON.stringify({ size, passes, bound, restarts, rowsRead, elapsedMs,
    fallbackRecomputes: sessionSummaryCounters(buffer.database).fallbackRecomputes }));
  completion.complete();
} finally { buffer.close(); }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
