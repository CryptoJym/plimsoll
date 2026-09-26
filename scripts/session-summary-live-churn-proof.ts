import assert from "node:assert/strict";
import path from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { readLedgerOffThread, buildSessionSyncRow, collectSessionSnapshots,
  commitDaemonSessionSyncFailure, commitDaemonSessionSyncSuccess,
  loadDaemonSessionSyncState, planDaemonSessionSync, runSessionSync,
  saveDaemonSessionSyncState } from "../packages/collector-cli/src/session-sync";
import { sessionSummaryCounters, updateSessionSummary } from "../packages/collector-cli/src/session-summary";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-live-churn");
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const tenantId = "00000000-0000-4000-8000-000000000087";
const installKey = "live-churn-proof-install";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey,
  uploadSigningSecret: "live-churn-proof-secret",
});
let sequence = 1;
const eventId = () => `00000000-0000-4000-8000-${String(sequence++).padStart(12, "0")}`;
const sessionId = (n: number) => `00000000-0000-4000-8000-${String(870000 + n).padStart(12, "0")}`;

function append(buffer: LocalEventBuffer, session: string, count: number): string[] {
  const ids: string[] = [];
  const observedAt = Date.now() + 60_000;
  for (let start = 0; start < count; start += 250) {
    const entries = Array.from({ length: Math.min(250, count - start) }, (_, offset) => {
      const id = eventId();
      ids.push(id);
      return { event: aiInteractionEventSchema.parse({
        id, sessionId: session, source: "codex", eventType: "assistant_response",
        observedAt: new Date(observedAt + start + offset).toISOString(),
        inputTokens: 2, outputTokens: 1,
      }), suppressedFields: [] };
    });
    const result = buffer.appendMany(entries);
    assert.equal(result.deduplicatedCount, 0);
    assert.equal(result.enrollmentRejectedEventCount, 0);
  }
  return ids;
}

function revision(buffer: LocalEventBuffer, session: string): number {
  return (buffer.database.prepare(`select mutation_revision as value from session_sync_summary_revision
    where session_id = ?`).get(session) as { value: number } | undefined)?.value ?? 0;
}

function activity(buffer: LocalEventBuffer, session: string): number {
  return (buffer.database.prepare(`select activity_revision as value from session_sync_summary_activity
    where session_id = ?`).get(session) as { value: number } | undefined)?.value ?? 0;
}

function dirty(buffer: LocalEventBuffer, session: string): string | null {
  return (buffer.database.prepare(`select reason from session_sync_summary_dirty where session_id = ?`)
    .get(session) as { reason: string } | undefined)?.reason ?? null;
}

function state(buffer: LocalEventBuffer, session: string) {
  return buffer.database.prepare(`select complete, mode, accumulator_json as accumulatorJson
    from session_sync_summary_state where session_id = ?`).get(session) as {
      complete: number; mode: string; accumulatorJson: string;
    };
}

function expectedWire(buffer: LocalEventBuffer, session: string, until: string) {
  const snapshot = collectSessionSnapshots(buffer.database, { sessionIds: [session], until })[0];
  assert.ok(snapshot);
  const normalized = buildSessionSyncRow(snapshot);
  assert.equal(normalized.ok, true);
  return normalized.row;
}

async function daemonCycle(buffer: LocalEventBuffer) {
  const until = new Date().toISOString();
  const prior = loadDaemonSessionSyncState(buffer.database);
  const plan = planDaemonSessionSync({ db: buffer.database, state: prior, uploadedBatches: [], until });
  saveDaemonSessionSyncState(buffer.database, plan.state);
  const sent: Array<{ session: { id: string }; [key: string]: unknown }> = [];
  if (plan.skip) return { until, plan, result: null, sent, state: loadDaemonSessionSyncState(buffer.database) };
  const fetchImpl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const raw = String(init?.body ?? "");
    sent.push(...(JSON.parse(raw).sessions ?? []));
    return new Response(JSON.stringify({
      ...acceptedFixtureDelivery(raw, installKey),
      inserted: sent.length, updated: 0, skippedStale: 0,
    }), { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  const result = await runSessionSync(config, {
    ...(plan.sessionIds !== undefined ? { sessionIds: plan.sessionIds } : {}),
    until: plan.until, ledgerDb: buffer.database, incremental: true,
    fetchImpl, sleep: async () => undefined, delayMs: 0, maxAttemptsPerBatch: 1,
    log: () => undefined,
  });
  const next = result.ok && result.summaryComplete
    ? commitDaemonSessionSyncSuccess(plan.state, plan.until, result.rejectedSessionIds)
    : commitDaemonSessionSyncFailure(plan.state,
      plan.sessionIds === undefined ? undefined : [...plan.sessionIds, ...result.pendingSummarySessionIds]);
  saveDaemonSessionSyncState(buffer.database, next);
  return { until, plan, result, sent, state: loadDaemonSessionSyncState(buffer.database) };
}

async function initialCatchUp(buffer: LocalEventBuffer, maxPasses: number) {
  for (let pass = 1; pass <= maxPasses; pass += 1) {
    const cycle = await daemonCycle(buffer);
    if (cycle.state.caughtUp && cycle.state.lastSuccessfulUntil === cycle.until) return pass;
  }
  throw new Error(`initial_catch_up_exceeded_${maxPasses}_passes`);
}

async function proveBusySession(size: number, ordinal: number) {
  const session = sessionId(ordinal);
  const buffer = new LocalEventBuffer(path.join(root, `busy-${size}.sqlite`), { workspaceId: tenantId });
  try {
    append(buffer, session, size);
    const initialPasses = await initialCatchUp(buffer, Math.ceil(size / 5_000) + 2);
    assert.equal(state(buffer, session).complete, 1);
    const baseRecomputes = sessionSummaryCounters(buffer.database).fallbackRecomputes;
    const start = Date.now();
    let lastId = "";
    let previousHorizon = loadDaemonSessionSyncState(buffer.database).lastSuccessfulUntil;
    for (let tick = 1; tick <= 6; tick += 1) {
      await sleep(1_000);
      // Two new responses every three seconds = 40 appends/minute. The
      // reconciler changes one recent response cost each second, including
      // repeated corrections before the next daemon pass.
      if ((tick - 1) % 3 === 0) lastId = append(buffer, session, 2)[1]!;
      const beforeRevision = revision(buffer, session);
      assert.equal(buffer.database.prepare(`update buffered_events set cost_usd = ? where id = ?`)
        .run(tick / 10_000, lastId).changes, 1);
      assert.equal(revision(buffer, session), beforeRevision,
        `unscanned ${size}-row edit must not invalidate the durable prefix`);
      assert.equal(dirty(buffer, session), null);
      if (tick % 3 !== 0) continue;
      const cycle = await daemonCycle(buffer);
      assert.ok(cycle.result?.ok && cycle.result.summaryComplete,
        JSON.stringify({ size, tick, pending: cycle.result?.pendingSummaryReasons }));
      assert.equal(cycle.state.caughtUp, true);
      assert.equal(cycle.state.lastSuccessfulUntil, cycle.until);
      assert.notEqual(cycle.until, previousHorizon);
      assert.deepEqual(cycle.sent.find((row) => row.session.id === session),
        expectedWire(buffer, session, cycle.until));
      assert.equal(sessionSummaryCounters(buffer.database).fallbackRecomputes, baseRecomputes);
      previousHorizon = cycle.until;
    }
    assert.ok(Date.now() - start >= 6_000);
    completion.check(`daemon_${size}_rows_1_mark_per_second_40_appends_per_minute`);
    console.log(JSON.stringify({ size, initialPasses, activeCycles: 2, activePassesPerCycle: 1,
      marks: 6, appends: 4, horizon: previousHorizon }));
  } finally { buffer.close(); }
}

async function proveHistoricalAndReadRace() {
  const session = sessionId(10);
  const buffer = new LocalEventBuffer(path.join(root, "historical.sqlite"), { workspaceId: tenantId });
  try {
    const directRead = async <T,>(queries: Array<{ sql: string; params: Record<string, unknown> }>): Promise<T[]> =>
      queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]);
    const ids = append(buffer, session, 10_000);
    await initialCatchUp(buffer, 4);
    const before = revision(buffer, session);
    buffer.database.prepare("update buffered_events set output_tokens = 7 where id = ?").run(ids[0]);
    assert.ok(revision(buffer, session) > before);
    const first = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(first.complete, false);
    assert.equal(state(buffer, session).mode, "fallback");
    const afterFirst = revision(buffer, session);
    buffer.database.prepare("update buffered_events set output_tokens = 9 where id = ?").run(ids.at(-1));
    buffer.database.prepare("delete from buffered_events where id = ?").run(ids.at(-2));
    assert.equal(revision(buffer, session), afterFirst);
    let final = first;
    for (let pass = 0; pass < 3 && !final.complete; pass += 1) {
      final = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
        read: (queries) => readLedgerOffThread(buffer.database, queries),
      });
      assert.equal(final.fullRecompute, false);
    }
    assert.equal(final.complete, true);
    assert.deepEqual(final.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: new Date().toISOString(),
    })[0]);
    completion.check("unscanned_historical_edit_and_erasure_keep_cursor_and_match_full_rebuild");

    // Moving an as-yet-unread row behind the historical cursor is different:
    // it would be missed by the seek, so this one must restart the session.
    buffer.database.prepare("update buffered_events set input_tokens = 12 where id = ?").run(ids[0]);
    const beforeMove = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(beforeMove.complete, false);
    const beforeMoveRevision = revision(buffer, session);
    buffer.database.prepare("update buffered_events set observed_at = ? where id = ?")
      .run("2026-01-01T00:00:00.000Z", ids.at(-1));
    assert.ok(revision(buffer, session) > beforeMoveRevision);
    let moved = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: directRead,
    });
    assert.equal(moved.fullRecompute, true);
    for (let pass = 0; pass < 3 && !moved.complete; pass += 1) {
      moved = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
        read: directRead,
      });
    }
    assert.equal(moved.complete, true);
    assert.deepEqual(moved.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: new Date().toISOString(),
    })[0]);
    completion.check("row_moved_behind_historical_cursor_restarts_only_that_session");

    // A write after the worker has read a slice but before its state commit
    // must discard that slice, even though the old durable cursor calls it
    // unscanned. This exercises the activity fence without a timing race.
    buffer.database.prepare("update buffered_events set input_tokens = 13 where id = ?").run(ids[0]);
    const restarted = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: directRead,
    });
    assert.equal(restarted.complete, false);
    const committedBeforeRace = JSON.parse(state(buffer, session).accumulatorJson).events;
    let injected = false;
    const beforeActivity = activity(buffer, session);
    const beforeRevision = revision(buffer, session);
    const raced = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: async <T>(queries: Parameters<typeof readLedgerOffThread>[1]) => {
        const rows = await directRead<T>(queries);
        if (!injected && rows.length > 0 &&
            typeof (rows[0] as { outputTokens?: unknown }).outputTokens === "number") {
          injected = true;
          const id = (rows[0] as { id: string }).id;
          buffer.database.prepare("update buffered_events set output_tokens = 17 where id = ?").run(id);
        }
        return rows;
      },
    });
    assert.equal(injected, true);
    assert.equal(revision(buffer, session), beforeRevision);
    assert.ok(activity(buffer, session) > beforeActivity);
    assert.equal(raced.complete, false);
    assert.equal(raced.fallbackReason, "ledger_edit_during_slice");
    assert.equal(JSON.parse(state(buffer, session).accumulatorJson).events, committedBeforeRace);
    let recovered = raced;
    for (let pass = 0; pass < 4 && !recovered.complete; pass += 1) {
      recovered = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
        read: directRead,
      });
    }
    assert.equal(recovered.complete, true);
    assert.deepEqual(recovered.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: new Date().toISOString(),
    })[0]);
    completion.check("in_flight_unscanned_edit_retries_slice_without_stale_commit");
  } finally { buffer.close(); }
}

async function proveLineageAndErasure() {
  const session = sessionId(11);
  const buffer = new LocalEventBuffer(path.join(root, "lineage.sqlite"), { workspaceId: tenantId });
  try {
    buffer.delivery.configure({ enabled: true, limits: config.delivery });
    const [firstId] = append(buffer, session, 1);
    await initialCatchUp(buffer, 2);
    const beforeRevision = revision(buffer, session);
    const beforeActivity = activity(buffer, session);
    const beforeControl = sessionSummaryCounters(buffer.database).mutationRevision;
    buffer.database.prepare("update buffered_events set payload_json = payload_json where id = ?").run(firstId);
    buffer.database.prepare("update buffered_events set cost_usd = cost_usd where id = ?").run(firstId);
    assert.equal(revision(buffer, session), beforeRevision);
    assert.equal(activity(buffer, session), beforeActivity);
    assert.equal(sessionSummaryCounters(buffer.database).mutationRevision, beforeControl);
    completion.check("irrelevant_and_noop_raw_marks_do_not_dirty_or_bump_revision");

    const matchingDelivery = (buffer.database.prepare(
      "select delivery_id as id from upload_outbox where raw_id = ?",
    ).get(firstId) as { id: string }).id;
    buffer.database.prepare("delete from upload_outbox where delivery_id = ?").run(matchingDelivery);
    assert.equal(revision(buffer, session), beforeRevision);
    assert.equal(sessionSummaryCounters(buffer.database).mutationRevision, beforeControl);
    assert.equal(dirty(buffer, session), null);
    completion.check("ordinary_matching_outbox_ack_delete_does_not_dirty_summary");

    const [secondId] = append(buffer, session, 1);
    const beforeSecond = await daemonCycle(buffer);
    assert.ok(beforeSecond.result?.summaryComplete);
    const delivery = (buffer.database.prepare("select delivery_id as id from upload_outbox where raw_id = ?")
      .get(secondId) as { id: string }).id;
    // The outbox's immutable lineage now disagrees with its raw row.
    buffer.database.prepare("update buffered_events set id = ? where id = ?")
      .run(eventId(), secondId);
    assert.equal(dirty(buffer, session), "raw_update");
    const excludedUntil = new Date().toISOString();
    const excluded = await updateSessionSummary(buffer.database, session, excludedUntil, {
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(excluded.complete, true);
    assert.deepEqual(excluded.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: excludedUntil,
    })[0]);
    assert.equal(excluded.snapshot?.events, 1);
    const beforeDelete = revision(buffer, session);
    buffer.database.prepare("delete from upload_outbox where delivery_id = ?").run(delivery);
    assert.ok(revision(buffer, session) > beforeDelete);
    const restoredUntil = new Date().toISOString();
    const restored = await updateSessionSummary(buffer.database, session, restoredUntil, {
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(restored.complete, true);
    assert.deepEqual(restored.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: restoredUntil,
    })[0]);
    assert.equal(restored.snapshot?.events, 2);
    completion.check("mismatched_lineage_delete_restores_eligibility_exactly");

    buffer.database.prepare("delete from buffered_events where id = ?").run(firstId);
    assert.equal(dirty(buffer, session), "raw_delete");
    const erasedUntil = new Date().toISOString();
    const erased = await updateSessionSummary(buffer.database, session, erasedUntil, {
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(erased.complete, true);
    assert.deepEqual(erased.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: erasedUntil,
    })[0]);
    assert.equal(erased.snapshot?.events, 1);
    completion.check("scanned_erasure_still_wins");

    const otherSession = sessionId(12);
    append(buffer, otherSession, 1);
    assert.ok((await daemonCycle(buffer)).result?.summaryComplete);
    const [movedId] = append(buffer, session, 1);
    const oldRevision = revision(buffer, session);
    const newRevision = revision(buffer, otherSession);
    buffer.database.prepare("update buffered_events set session_id = ? where id = ?")
      .run(otherSession, movedId);
    assert.equal(revision(buffer, session), oldRevision);
    assert.equal(revision(buffer, otherSession), newRevision);
    const queued = buffer.database.prepare(`select session_id as sessionId from session_sync_summary_rows
      where raw_rowid = (select rowid from buffered_events where id = ?)`)
      .get(movedId) as { sessionId: string };
    assert.equal(queued.sessionId, otherSession);
    const movedCycle = await daemonCycle(buffer);
    assert.ok(movedCycle.result?.summaryComplete);
    assert.equal(movedCycle.state.lastSuccessfulUntil, movedCycle.until);
    assert.deepEqual(movedCycle.sent.find((row) => row.session.id === otherSession),
      expectedWire(buffer, otherSession, movedCycle.until));
    const oldSummary = await updateSessionSummary(buffer.database, session, movedCycle.until, {
      read: (queries) => readLedgerOffThread(buffer.database, queries),
    });
    assert.equal(oldSummary.complete, true);
    assert.deepEqual(oldSummary.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: movedCycle.until,
    })[0]);
    completion.check("unscanned_session_transfer_moves_append_queue_without_stale_summary");
  } finally { buffer.close(); }
}

async function main() {
  await proveBusySession(5_000, 1);
  await proveBusySession(10_000, 2);
  await proveBusySession(20_000, 3);
  await proveHistoricalAndReadRace();
  await proveLineageAndErasure();
  completion.complete();
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
