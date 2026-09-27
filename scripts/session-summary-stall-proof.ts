import assert from "node:assert/strict";
import path from "node:path";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { collectSessionSnapshots, commitDaemonSessionSyncFailure,
  commitDaemonSessionSyncSuccess, loadDaemonSessionSyncState, planDaemonSessionSync,
  saveDaemonSessionSyncState } from "../packages/collector-cli/src/session-sync";
import { ensureSessionSummarySchema, updateSessionSummary, type SessionReadQuery, type SessionSummaryRead } from "../packages/collector-cli/src/session-summary";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-stall");
const sessionId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa15";
const buffer = new LocalEventBuffer(path.join(process.env.PLIMSOLL_PROOF_ROOT!, "stall.sqlite"));
let clock = new Date(Date.now() + 2_000);
let reads = 0;
const interrupted: SessionSummaryRead = async <T>(_queries: SessionReadQuery[]): Promise<T[]> => {
  reads += 1;
  throw new Error("session_summary_read_interrupted");
};

function progress(targetId = sessionId) {
  return buffer.database.prepare(`select reason, consecutive_zero_progress as consecutive,
    next_retry_at as nextRetryAt from session_sync_summary_pending where session_id = ?`)
    .get(targetId) as { reason: string; consecutive: number; nextRetryAt: string } | undefined;
}

async function cycle(read: SessionSummaryRead, until: string) {
  const prior = loadDaemonSessionSyncState(buffer.database);
  const plan = planDaemonSessionSync({ db: buffer.database, state: prior, uploadedBatches: [], until });
  saveDaemonSessionSyncState(buffer.database, plan.state);
  assert.equal(plan.skip, false);
  const update = await updateSessionSummary(buffer.database, sessionId, until, {
    read, now: () => clock, maxRows: 100,
  });
  const next = update.complete
    ? commitDaemonSessionSyncSuccess(plan.state, plan.until, [])
    : commitDaemonSessionSyncFailure(plan.state, [sessionId]);
  saveDaemonSessionSyncState(buffer.database, next);
  return { update, state: loadDaemonSessionSyncState(buffer.database) };
}

async function main() {
  try {
    const event = aiInteractionEventSchema.parse({
      id: "00000000-0000-4000-8000-000000000115", sessionId,
      source: "codex", eventType: "assistant_response", observedAt: new Date().toISOString(),
      inputTokens: 7, outputTokens: 3,
    });
    assert.equal(buffer.appendMany([{ event, suppressedFields: [] }]).deduplicatedCount, 0);
    ensureSessionSummarySchema(buffer.database);
    const until = clock.toISOString();
    const first = await cycle(interrupted, until);
    const firstState = buffer.database.prepare("select updated_at as updatedAt from session_sync_summary_state where session_id = ?")
      .get(sessionId) as { updatedAt: string };
    assert.equal(first.update.complete, false);
    assert.equal(first.update.rowsRead, 0);
    assert.equal(first.update.fallbackReason, "rows_read_timeout");
    assert.equal(first.state.caughtUp, false);
    assert.equal(first.state.lastSuccessfulUntil, null);
    completion.check("first_zero_row_pass_remains_incomplete");

    if (process.argv.includes("--baseline")) {
      const second = await cycle(interrupted, until);
      assert.equal(second.update.rowsRead, 0);
      assert.equal(second.update.complete, false);
      const secondState = buffer.database.prepare("select updated_at as updatedAt from session_sync_summary_state where session_id = ?")
        .get(sessionId) as { updatedAt: string };
      const recorded = buffer.database.prepare(`select 1 from sqlite_master where type = 'table'
        and name = 'session_sync_summary_pending'`).get();
      assert.equal(recorded, undefined);
      assert.equal(secondState.updatedAt, firstState.updatedAt);
      console.log(JSON.stringify({ baseline: "3ccba2bd", passes: 2, reads,
        rowsRead: second.update.rowsRead, caughtUp: second.state.caughtUp,
        progressRecord: recorded ?? null, stateUpdatedAtUnchanged: true }));
      completion.check("base_repeats_without_durable_progress_record");
      completion.complete();
      return;
    }

    assert.deepEqual(progress() && { reason: progress()!.reason, consecutive: progress()!.consecutive },
      { reason: "rows_read_timeout", consecutive: 1 });
    completion.check("first_timeout_is_durable");
    const beforeRetry = progress()!;
    clock = new Date(Date.parse(beforeRetry.nextRetryAt) - 1);
    const held = await cycle(interrupted, until);
    assert.equal(held.update.complete, false);
    assert.equal(held.update.rowsRead, 0);
    assert.equal(reads, 1, "backoff must avoid another read before its deadline");
    assert.equal(progress()!.consecutive, 1);
    completion.check("retry_wait_is_bounded_and_does_not_claim_catch_up");

    for (const expected of [2, 3]) {
      clock = new Date(progress()!.nextRetryAt);
      const stalled = await cycle(interrupted, until);
      assert.equal(stalled.update.complete, false);
      assert.equal(stalled.update.rowsRead, 0);
      assert.equal(stalled.state.caughtUp, false);
      assert.equal(progress()!.consecutive, expected);
    }
    assert.equal(progress()!.reason, "rows_read_stuck");
    assert.equal(reads, 3);
    completion.check("third_zero_progress_read_is_durable_stuck_state");

    const independent = new Database(buffer.database.name, { readonly: true, fileMustExist: true });
    try {
      const row = independent.prepare(`select reason, consecutive_zero_progress as consecutive
        from session_sync_summary_pending where session_id = ?`).get(sessionId) as {
        reason: string; consecutive: number;
      };
      assert.deepEqual(row, { reason: "rows_read_stuck", consecutive: 3 });
    } finally { independent.close(); }
    completion.check("stuck_reason_is_readable_from_an_independent_connection");

    for (const expected of [4, 5, 6, 7]) {
      clock = new Date(progress()!.nextRetryAt);
      const stalled = await cycle(interrupted, until);
      assert.equal(stalled.update.complete, false);
      assert.equal(stalled.state.caughtUp, false);
      assert.equal(progress()!.consecutive, expected);
      assert.equal(Date.parse(progress()!.nextRetryAt) - clock.getTime(),
        Math.min(5_000, 100 * 2 ** (expected - 1)));
    }
    assert.equal(progress()!.reason, "rows_read_stuck");
    completion.check("exponential_retry_delay_caps_at_five_seconds");

    clock = new Date(progress()!.nextRetryAt);
    const recovered = await cycle(async <T>(queries: SessionReadQuery[]) => queries.flatMap((query) =>
      buffer.database.prepare(query.sql).all(query.params) as T[]), until);
    assert.equal(recovered.update.complete, true, JSON.stringify(recovered.update));
    assert.equal(recovered.state.caughtUp, true);
    assert.equal(recovered.state.lastSuccessfulUntil, until);
    assert.equal(progress(), undefined);
    assert.deepEqual(recovered.update.snapshot,
      collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until })[0]);
    completion.check("recovered_read_converges_exactly_and_clears_stuck_reason");

    const changedId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa16";
    const appendChanged = (id: string) => buffer.appendMany([{ event: aiInteractionEventSchema.parse({
      id, sessionId: changedId, source: "codex", eventType: "assistant_response",
      observedAt: new Date().toISOString(), inputTokens: 1, outputTokens: 1,
    }), suppressedFields: [] }]);
    appendChanged("00000000-0000-4000-8000-000000000116");
    clock = new Date(Date.now() + 10_000);
    const changedUntil = clock.toISOString();
    const changedFirst = await updateSessionSummary(buffer.database, changedId, changedUntil, {
      read: interrupted, now: () => clock,
    });
    assert.equal(changedFirst.complete, false);
    const changedDeadline = progress(changedId)!.nextRetryAt;
    clock = new Date(Date.parse(changedDeadline) - 1);
    appendChanged("00000000-0000-4000-8000-000000000117");
    const readsBeforeChangedRetry = reads;
    const changedRetry = await updateSessionSummary(buffer.database, changedId, changedUntil, {
      read: interrupted, now: () => clock,
    });
    assert.equal(changedRetry.complete, false);
    assert.equal(reads, readsBeforeChangedRetry + 1,
      "a new ledger revision must bypass the prior revision's retry deadline");
    assert.equal(progress(changedId)!.consecutive, 1);
    completion.check("ledger_mutation_bypasses_old_zero_progress_backoff");

    const racingId = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa17";
    const appendRacing = (id: string) => buffer.appendMany([{ event: aiInteractionEventSchema.parse({
      id, sessionId: racingId, source: "codex", eventType: "assistant_response",
      observedAt: new Date().toISOString(), inputTokens: 1, outputTokens: 1,
    }), suppressedFields: [] }]);
    appendRacing("00000000-0000-4000-8000-000000000118");
    clock = new Date(Date.now() + 20_000);
    const racingUntil = clock.toISOString();
    const racingFirst = await updateSessionSummary(buffer.database, racingId, racingUntil, {
      now: () => clock,
      read: async <T>(queries: SessionReadQuery[]) => {
        appendRacing("00000000-0000-4000-8000-000000000119");
        return interrupted<T>(queries);
      },
    });
    assert.equal(racingFirst.complete, false);
    clock = new Date(Date.parse(progress(racingId)!.nextRetryAt) - 1);
    const readsBeforeRacingRetry = reads;
    const racingRetry = await updateSessionSummary(buffer.database, racingId, racingUntil, {
      now: () => clock, read: interrupted,
    });
    assert.equal(racingRetry.complete, false);
    assert.equal(reads, readsBeforeRacingRetry + 1,
      "a row appended during a timed-out read must bypass that read's backoff");
    completion.check("concurrent_append_bypasses_old_zero_progress_backoff");
    completion.complete();
  } finally {
    buffer.close();
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
