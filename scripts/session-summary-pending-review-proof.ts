import assert from "node:assert/strict";
import path from "node:path";
import Database from "better-sqlite3";

import { BOUNDED_SQL_READ_PREDICATE, BoundedSqlReadError } from "../packages/collector-cli/src/bounded-sql-read";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { emptyDaemonSessionSyncState, planDaemonSessionSync } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, listSessionSummaryPendingIds, SESSION_SUMMARY_QUEUED_ROWS_PENDING_SQL,
  updateSessionSummary, type SessionReadQuery, type SessionSummaryRead } from "../packages/collector-cli/src/session-summary";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-pending-review");
const root = process.env.PLIMSOLL_PROOF_ROOT!;

function appendOne(buffer: LocalEventBuffer, sessionId: string, eventId: string) {
  const event = aiInteractionEventSchema.parse({
    id: eventId, sessionId, source: "codex", eventType: "assistant_response",
    observedAt: new Date(Date.now() - 60_000).toISOString(), inputTokens: 3, outputTokens: 2,
  });
  assert.equal(buffer.appendMany([{ event, suppressedFields: [] }]).deduplicatedCount, 0);
}

function directRead(db: Database.Database): SessionSummaryRead {
  return async <T>(queries: SessionReadQuery[]) => queries.flatMap((query) =>
    db.prepare(query.sql).all(query.params) as T[]);
}

async function pendingReasonReplansCompleteSummary() {
  const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa41";
  const buffer = new LocalEventBuffer(path.join(root, "pending-planner.sqlite"));
  try {
    appendOne(buffer, sessionId, "00000000-0000-4000-8000-000000000141");
    ensureSessionSummarySchema(buffer.database);
    const until = new Date().toISOString();
    const initial = await updateSessionSummary(buffer.database, sessionId, until,
      { read: directRead(buffer.database) });
    assert.equal(initial.complete, true, JSON.stringify(initial));

    const interrupted = await updateSessionSummary(buffer.database, sessionId, until, {
      read: async <T>(_queries: SessionReadQuery[]): Promise<T[]> => {
        throw new Error("session_summary_read_interrupted");
      },
    });
    assert.equal(interrupted.complete, false);
    assert.equal(interrupted.fallbackReason, "checkpoint_timeout");
    const stored = buffer.database.prepare(`select complete, mutation_revision as revision
      from session_sync_summary_state where session_id = ?`).get(sessionId) as {
      complete: number; revision: number;
    };
    const pending = buffer.database.prepare(`select reason, mutation_revision as revision,
      queued_high_water as queuedHighWater from session_sync_summary_pending where session_id = ?`)
      .get(sessionId) as { reason: string; revision: number; queuedHighWater: number };
    const revision = buffer.database.prepare(`select mutation_revision as revision
      from session_sync_summary_revision where session_id = ?`).get(sessionId) as { revision: number } | undefined;
    assert.equal(stored.complete, 1);
    assert.equal(pending.reason, "checkpoint_timeout");
    assert.equal(revision?.revision ?? 0, stored.revision);
    const dirtyCount = buffer.database.prepare(`select count(*) as count from session_sync_summary_dirty`)
      .get() as { count: number };
    const queuedCount = buffer.database.prepare(`select count(*) as count from session_sync_summary_rows`)
      .get() as { count: number };
    assert.equal(dirtyCount.count, 0);
    assert.equal(queuedCount.count, 0);

    // A crash before the carry state is saved leaves no in-memory or durable carry ID.
    const plan = planDaemonSessionSync({ db: buffer.database,
      state: { ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: until },
      uploadedBatches: [], until, ledgerSessionIds: [] });
    assert.deepEqual(plan.sessionIds, [sessionId], JSON.stringify(plan));
    assert.equal(plan.skip, false);

    // The planner can return a backed-off ID; updateSessionSummary must still skip the read.
    const future = new Date(Date.now() + 60_000).toISOString();
    buffer.database.prepare(`update session_sync_summary_pending set next_retry_at = ?
      where session_id = ?`).run(future, sessionId);
    const backedOffPlan = planDaemonSessionSync({ db: buffer.database,
      state: { ...emptyDaemonSessionSyncState(), caughtUp: true, lastSuccessfulUntil: until },
      uploadedBatches: [], until, ledgerSessionIds: [] });
    assert.deepEqual(backedOffPlan.sessionIds, [sessionId]);
    let reads = 0;
    const held = await updateSessionSummary(buffer.database, sessionId, until, {
      now: () => new Date(Date.parse(future) - 1),
      read: async <T>(_queries: SessionReadQuery[]): Promise<T[]> => {
        reads += 1;
        throw new Error("backoff_read_should_not_run");
      },
    });
    assert.equal(held.complete, false);
    assert.equal(held.rowsRead, 0);
    assert.equal(reads, 0);

    buffer.database.prepare(`insert into session_sync_summary_pending
      (session_id, reason, mutation_revision, queued_high_water,
        consecutive_zero_progress, next_retry_at, updated_at)
      values (?, 'rows_read_timeout', 0, 0, 1, null, ?)`)
      .run("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa42", new Date().toISOString());
    assert.throws(() => listSessionSummaryPendingIds(buffer.database, until, 1),
      (error) => error instanceof BoundedSqlReadError && error.reason === "row_limit");
    completion.check("pending_reason_replans_complete_summary_with_bounded_backoff");
  } finally { buffer.close(); }
}

async function latePendingReasonClearsOnCompletion() {
  const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa43";
  const buffer = new LocalEventBuffer(path.join(root, "late-pending.sqlite"));
  let writer: Database.Database | null = null;
  try {
    appendOne(buffer, sessionId, "00000000-0000-4000-8000-000000000143");
    ensureSessionSummarySchema(buffer.database);
    assert.equal(buffer.database.prepare(`select 1 from session_sync_summary_pending where session_id = ?`)
      .get(sessionId), undefined);
    writer = new Database(buffer.database.name);
    let inserted = false;
    const read: SessionSummaryRead = async <T>(queries: SessionReadQuery[]) => {
      if (!inserted) {
        writer!.prepare(`insert into session_sync_summary_pending
          (session_id, reason, mutation_revision, queued_high_water,
            consecutive_zero_progress, next_retry_at, updated_at)
          values (?, 'rows_read_timeout', 0, 0, 1, null, ?)`)
          .run(sessionId, new Date().toISOString());
        inserted = true;
        assert.ok(writer!.prepare(`select 1 from session_sync_summary_pending where session_id = ?`)
          .get(sessionId));
      }
      return directRead(buffer.database)<T>(queries);
    };
    const result = await updateSessionSummary(buffer.database, sessionId, new Date().toISOString(), { read });
    assert.equal(inserted, true);
    assert.equal(result.complete, true, JSON.stringify(result));
    assert.equal(buffer.database.prepare(`select 1 from session_sync_summary_pending where session_id = ?`)
      .get(sessionId), undefined, "completion must clear a pending row inserted after entry");
    completion.check("late_pending_reason_cleared_after_complete_pass");
  } finally { writer?.close(); buffer.close(); }
}

async function plannerToleratesLedgerWithoutPendingTable() {
  const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa45";
  const buffer = new LocalEventBuffer(path.join(root, "pre-pending-ledger.sqlite"));
  try {
    appendOne(buffer, sessionId, "00000000-0000-4000-8000-000000000145");
    ensureSessionSummarySchema(buffer.database);
    // A ledger from before #412 has the summary state and dirty tables, with work waiting,
    // but no pending table: its lazy summary migrations have not run yet.
    buffer.database.prepare(`insert into session_sync_summary_dirty (session_id, reason, updated_at)
      values (?, 'raw_row_moved', ?)`).run(sessionId, new Date().toISOString());
    buffer.database.exec("drop table session_sync_summary_pending");
    const until = new Date().toISOString();
    const ids = listSessionSummaryPendingIds(buffer.database, until);
    assert.ok(ids.includes(sessionId), JSON.stringify(ids));
    completion.check("planner_tolerates_ledger_without_pending_table");
  } finally { buffer.close(); }
}

async function queuedRowsPendingReadStartsFromTheQueue() {
  const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa47";
  const buffer = new LocalEventBuffer(path.join(root, "queued-rows-plan.sqlite"));
  try {
    appendOne(buffer, sessionId, "00000000-0000-4000-8000-000000000147");
    ensureSessionSummarySchema(buffer.database);
    const initial = await updateSessionSummary(buffer.database, sessionId, new Date().toISOString(),
      { read: directRead(buffer.database) });
    assert.equal(initial.complete, true, JSON.stringify(initial));
    // A later row in a summarized session is queued by the insert trigger.
    appendOne(buffer, sessionId, "00000000-0000-4000-8000-000000000148");
    const queued = buffer.database.prepare(`select count(*) as count from session_sync_summary_rows
      where session_id = ?`).get(sessionId) as { count: number };
    assert.equal(queued.count, 1);
    const until = new Date().toISOString();
    assert.ok(listSessionSummaryPendingIds(buffer.database, until).includes(sessionId));
    // eco-6hoxj.163.135: the queue must drive the join. Walking buffered_events
    // runs the bounded-read check once per event and misses its deadline on a
    // large ledger, which sends every session on every cycle.
    const plan = (buffer.database.prepare(`explain query plan
      ${SESSION_SUMMARY_QUEUED_ROWS_PENDING_SQL.replace(BOUNDED_SQL_READ_PREDICATE, "1")}`)
      .all({ until }) as Array<{ detail: string }>).map((row) => row.detail);
    assert.match(plan[0] ?? "", /^(?:SCAN|SEARCH) r\b/, JSON.stringify(plan));
    assert.ok(plan.some((detail) => /^SEARCH e USING INTEGER PRIMARY KEY/.test(detail)), JSON.stringify(plan));
    completion.check("queued_rows_pending_read_starts_from_the_queue");
  } finally { buffer.close(); }
}

async function main() {
  const selected = process.argv.find((arg) => arg.startsWith("--case="))?.slice("--case=".length);
  if (selected && selected !== "planner" && selected !== "completion" && selected !== "upgrade" &&
      selected !== "queue-plan") throw new Error("unknown proof case");
  if (!selected || selected === "planner") await pendingReasonReplansCompleteSummary();
  if (!selected || selected === "completion") await latePendingReasonClearsOnCompletion();
  if (!selected || selected === "upgrade") await plannerToleratesLedgerWithoutPendingTable();
  if (!selected || selected === "queue-plan") await queuedRowsPendingReadStartsFromTheQueue();
  completion.complete();
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
