import assert from "node:assert/strict";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { BOUNDED_SQL_READ_PREDICATE } from "../packages/collector-cli/src/bounded-sql-read";
import { emptyDaemonSessionSyncState, planDaemonSessionSync } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, listSessionSummaryPendingIds, SESSION_SUMMARY_QUEUED_ROWS_PENDING_SQL,
  updateSessionSummary, type SessionReadQuery } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-queued-session", 4);
async function main() {
  const buffer = new LocalEventBuffer(path.join(process.env.PLIMSOLL_PROOF_ROOT!, "queued-sessions.sqlite"));
  const db = buffer.database;
  const until = "2026-10-03T00:00:00.000Z";
  const future = "2026-10-03T00:01:00.000Z";
  const blocked = "00000000-0000-4000-8000-000000000001";
  const active = "00000000-0000-4000-8000-000000000002";
  const futureOnly = "00000000-0000-4000-8000-000000000003";
  try {
    ensureSessionSummarySchema(db);
    const insert = db.prepare(`insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, session_id, input_tokens, output_tokens)
      values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', '[]', ?, ?, 2, 3)`);
    let index = 0;
    const add = (session: string, createdAt = until) => {
      const id = `00000000-0000-4000-9000-${String(++index).padStart(12, "0")}`;
      insert.run(id, until, createdAt, session);
    };
    const read = async <T>(queries: SessionReadQuery[]) =>
      queries.flatMap(query => db.prepare(query.sql).all(query.params) as T[]);
    for (const session of [blocked, active, futureOnly]) {
      add(session);
      assert.equal((await updateSessionSummary(db, session, until, { read })).complete, true);
    }
    db.transaction(() => {
      for (let row = 0; row < 12_079; row++) add(blocked);
      add(active, future);
      add(active);
      add(futureOnly, future);
    }).immediate();
    const queueBefore = db.prepare("select count(*) as n from session_sync_summary_rows").get() as { n: number };
    assert.equal(queueBefore.n, 12_082);
    // A stale queue timestamp must not replace the authoritative event horizon.
    db.prepare("update session_sync_summary_rows set created_at = ? where session_id = ?")
      .run(until, futureOnly);
    let visits = 0;
    db.function("plimsoll_bounded_sync_read", () => { visits++; return 1; });
    const queued = db.prepare(SESSION_SUMMARY_QUEUED_ROWS_PENDING_SQL).all({ until }) as Array<{ sessionId: string }>;
    assert.deepEqual([...new Set(queued.map(row => row.sessionId))].sort(), [blocked, active]);
    assert.ok(visits <= 16, `queue planner performed ${visits} event probes for three queued sessions`);
    assert.equal(queued.length, 2, "one candidate per eligible queued session");
    completion.check("queued_session_probe_cost_is_independent_of_ready_row_backlog");

    const pending = listSessionSummaryPendingIds(db, until);
    assert.deepEqual(pending.sort(), [blocked, active]);
    const state = { ...emptyDaemonSessionSyncState(), caughtUp: true,
      lastSuccessfulUntil: until, blockedSessionIds: [blocked] };
    const plan = planDaemonSessionSync({ db, state, uploadedBatches: [], until, ledgerSessionIds: [] });
    assert.equal(plan.reason, "incremental");
    assert.deepEqual(plan.sessionIds, [active]);
    assert.equal(plan.state.caughtUp, true);
    assert.equal((db.prepare("select count(*) as n from session_sync_summary_rows").get() as { n: number }).n,
      queueBefore.n, "planning preserves every queued row, including blocked rows");
    completion.check("blocked_backlog_does_not_force_unrelated_session_uploads");

    const later = db.prepare(SESSION_SUMMARY_QUEUED_ROWS_PENDING_SQL).all({ until: future }) as Array<{ sessionId: string }>;
    assert.deepEqual(later.map(row => row.sessionId).sort(), [blocked, active, futureOnly]);
    completion.check("future_only_and_mixed_queues_use_authoritative_event_horizon");

    const queryPlan = (db.prepare(`explain query plan ${SESSION_SUMMARY_QUEUED_ROWS_PENDING_SQL
      .split(BOUNDED_SQL_READ_PREDICATE).join("1")}`).all({ until }) as Array<{ detail: string }>).map(row => row.detail);
    assert.ok(queryPlan.some(detail => /USING COVERING INDEX idx_session_summary_rows_session/.test(detail)),
      JSON.stringify(queryPlan));
    assert.ok(queryPlan.some(detail => /^SEARCH r USING COVERING INDEX idx_session_summary_rows_session/.test(detail)),
      JSON.stringify(queryPlan));
    assert.ok(queryPlan.some(detail => /^SEARCH e USING INTEGER PRIMARY KEY/.test(detail)), JSON.stringify(queryPlan));
    completion.check("distinct_queue_candidates_seek_the_queue_index_before_event_lookup");
    console.log(JSON.stringify({ proof: "session-summary-queued-session", queuedRows: queueBefore.n,
      sessions: 3, readySessionProbes: visits, queryPlan }));
    completion.complete();
  } finally { buffer.close(); }
}
main().catch(error => { console.error(error); process.exitCode = 1; });
