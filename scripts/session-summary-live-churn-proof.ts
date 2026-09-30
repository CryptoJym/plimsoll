import assert from "node:assert/strict";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { setTimeout as sleep } from "node:timers/promises";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { buildSessionSyncRow, collectSessionSnapshots,
  commitDaemonSessionSyncFailure, commitDaemonSessionSyncSuccess,
  loadDaemonSessionSyncState, planDaemonSessionSync, runSessionSync,
  saveDaemonSessionSyncState } from "../packages/collector-cli/src/session-sync";
import { SESSION_SUMMARY_DEFAULT_MAX_MS, SESSION_SUMMARY_DEFAULT_MAX_ROWS,
  sessionSummaryCounters, updateSessionSummary, type SessionSummaryRead,
  type SessionSummaryUpdateResult } from "../packages/collector-cli/src/session-summary";
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
  return buffer.database.prepare(`select complete, mode, high_water as highWater,
    accumulator_json as accumulatorJson
    from session_sync_summary_state where session_id = ?`).get(session) as {
      complete: number; mode: string; highWater: number; accumulatorJson: string;
    };
}

function expectedWire(buffer: LocalEventBuffer, session: string, until: string) {
  const snapshot = collectSessionSnapshots(buffer.database, { sessionIds: [session], until })[0];
  assert.ok(snapshot);
  const normalized = buildSessionSyncRow(snapshot);
  assert.equal(normalized.ok, true);
  return normalized.row;
}

// The proof controls the read on its disposable ledger. Summary updates still
// keep their 5,000-row and 250 ms caps; the worker's wall-clock deadline is
// exercised separately by the worker and timeout proofs.
function proofRead(buffer: LocalEventBuffer): SessionSummaryRead {
  return async <T>(queries: Parameters<SessionSummaryRead>[0]): Promise<T[]> =>
    queries.flatMap((query) => buffer.database.prepare(query.sql).all(query.params) as T[]);
}

function drainBound(rows: number, elapsedMs: number) {
  return Math.ceil(rows / SESSION_SUMMARY_DEFAULT_MAX_ROWS) +
    Math.floor(elapsedMs / SESSION_SUMMARY_DEFAULT_MAX_MS) + 1;
}

function rowCount(buffer: LocalEventBuffer) {
  return (buffer.database.prepare("select count(*) as count from buffered_events")
    .get() as { count: number }).count;
}

// A bounded pass may take longer than a tick on a loaded host. Apply every
// missed tick before the next pass, rather than pausing the advertised churn
// while catch-up retries run. Ticks 1, 4, 7, ... each append two responses.
class ChurnSchedule {
  readonly startedAt = performance.now();
  marks = 0;
  appends = 0;
  readonly scannedCorrections: number[] = [];
  private lastId = "";

  constructor(private readonly buffer: LocalEventBuffer, private readonly session: string,
    private readonly size: number) {}

  private applyThrough(ticks: number) {
    for (let tick = this.marks + 1; tick <= ticks; tick += 1) {
      const scheduledAppend = (tick - 1) % 3 === 0;
      if (scheduledAppend) {
        this.lastId = append(this.buffer, this.session, 2)[1]!;
        this.appends += 2;
      }
      const row = this.buffer.database.prepare(`select rowid, observed_at as observedAt
        from buffered_events where id = ?`).get(this.lastId) as {
          rowid: number; observedAt: string;
        } | undefined;
      assert.ok(row, `tick ${tick} must correct the last scheduled response`);
      const summary = state(this.buffer, this.session);
      const cursor = JSON.parse(summary.accumulatorJson) as {
        scanBoundary: number; cursorObservedAt: string | null; cursorRowid: number;
      };
      const unscanned = summary.complete === 1 || summary.mode === "incremental"
        ? row.rowid > summary.highWater
        : (row.rowid > cursor.scanBoundary && row.rowid > summary.highWater) ||
          (row.rowid <= cursor.scanBoundary &&
            (cursor.cursorObservedAt === null || row.observedAt > cursor.cursorObservedAt ||
              (row.observedAt === cursor.cursorObservedAt && row.rowid > cursor.cursorRowid)));
      const beforeRevision = revision(this.buffer, this.session);
      const beforeDirty = dirty(this.buffer, this.session);
      assert.equal(this.buffer.database.prepare(`update buffered_events set cost_usd = ? where id = ?`)
        .run(tick / 10_000, this.lastId).changes, 1);
      if (unscanned) {
        assert.equal(revision(this.buffer, this.session), beforeRevision,
          `unscanned ${this.size}-row edit must not invalidate the durable prefix`);
        assert.equal(dirty(this.buffer, this.session), beforeDirty);
      } else {
        assert.ok(revision(this.buffer, this.session) > beforeRevision,
          `scanned ${this.size}-row edit must invalidate the durable prefix`);
        assert.equal(dirty(this.buffer, this.session), "raw_update");
        this.scannedCorrections.push(tick);
      }
      this.marks = tick;
      assert.equal(this.appends, 2 * Math.ceil(this.marks / 3),
        `tick ${tick} must keep the exact 40-appends-per-minute schedule`);
    }
  }

  applyDue() {
    this.applyThrough(Math.floor((performance.now() - this.startedAt) / 1_000));
  }

  async waitForTick(tick: number) {
    while (this.marks < tick) {
      const delayMs = this.startedAt + tick * 1_000 - performance.now();
      if (delayMs > 0) await sleep(Math.max(1, Math.ceil(delayMs)));
      this.applyDue();
    }
  }

  stop() {
    const elapsedMs = performance.now() - this.startedAt;
    this.applyThrough(Math.floor(elapsedMs / 1_000));
    return elapsedMs;
  }
}

async function completeSummary(buffer: LocalEventBuffer, session: string,
  first: SessionSummaryUpdateResult, onPass?: (result: SessionSummaryUpdateResult) => void) {
  let result = first;
  let elapsedMs = 0;
  const rows = rowCount(buffer);
  for (let pass = 1; ; pass += 1) {
    elapsedMs += result.durationMs;
    assert.ok(pass <= drainBound(rows, elapsedMs),
      JSON.stringify({ pass, bound: drainBound(rows, elapsedMs), rows, result }));
    if (result.complete) return result;
    result = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: proofRead(buffer),
    });
    onPass?.(result);
  }
}

async function daemonCycle(buffer: LocalEventBuffer, summaryMaxRows?: number) {
  const until = new Date().toISOString();
  const prior = loadDaemonSessionSyncState(buffer.database);
  const plan = planDaemonSessionSync({ db: buffer.database, state: prior, uploadedBatches: [], until });
  saveDaemonSessionSyncState(buffer.database, plan.state);
  const sent: Array<{ session: { id: string }; [key: string]: unknown }> = [];
  const updates = new Map<string, SessionSummaryUpdateResult>();
  if (plan.skip) return { until, plan, result: null, sent, updates,
    state: loadDaemonSessionSyncState(buffer.database) };
  const fetchImpl = (async (_input: RequestInfo | URL, init?: RequestInit) => {
    const raw = String(init?.body ?? "");
    sent.push(...(JSON.parse(raw).sessions ?? []));
    return new Response(JSON.stringify({
      ...acceptedFixtureDelivery(raw, installKey),
      inserted: sent.length, updated: 0, skippedStale: 0,
    }), { status: 200, headers: { "content-type": "application/json" } });
  }) as typeof fetch;
  // The planner can request a full walk. Its production ID pager has a
  // separate worker deadline, so enumerate this disposable fixture locally.
  // runSessionSync still performs the same bounded per-session updates.
  const sessionIds = plan.sessionIds ?? (buffer.database.prepare(`select distinct session_id as sessionId
    from buffered_events where session_id is not null order by session_id`).all() as Array<{ sessionId: string }>)
    .map((row) => row.sessionId);
  const result = await runSessionSync(config, {
    sessionIds,
    until: plan.until, ledgerDb: buffer.database, incremental: true,
    ...(summaryMaxRows === undefined ? {} : { summaryMaxRows }),
    proofSummaryHooks: {
      read: proofRead(buffer),
      onUpdate: (sessionId, update) => { updates.set(sessionId, update); },
    },
    fetchImpl, sleep: async () => undefined, delayMs: 0, maxAttemptsPerBatch: 1,
    log: () => undefined,
  });
  const next = result.ok && result.summaryComplete
    ? commitDaemonSessionSyncSuccess(plan.state, plan.until, result.rejectedSessionIds)
    : commitDaemonSessionSyncFailure(plan.state,
      plan.sessionIds === undefined ? undefined : [...plan.sessionIds, ...result.pendingSummarySessionIds]);
  saveDaemonSessionSyncState(buffer.database, next);
  return { until, plan, result, sent, updates, state: loadDaemonSessionSyncState(buffer.database) };
}

async function initialCatchUp(buffer: LocalEventBuffer, churn?: ChurnSchedule) {
  let elapsedMs = 0;
  const updates: SessionSummaryUpdateResult[] = [];
  const marksBefore = churn?.marks ?? 0;
  let marksAtPreviousPass = marksBefore;
  for (let pass = 1; ; pass += 1) {
    if (churn) {
      churn.applyDue();
      // Every retry sees at least one scheduled mark. A fast pass waits for
      // the next tick; a slow pass applies all ticks missed while it ran.
      if (pass > 1 && churn.marks === marksAtPreviousPass) {
        await churn.waitForTick(marksAtPreviousPass + 1);
      }
      marksAtPreviousPass = churn.marks;
    }
    // Make the active workload exercise a retry even on an unloaded host.
    // Later passes retain the production 5,000-row summary cap.
    const cycle = await daemonCycle(buffer, churn && pass === 1 ? 1 : undefined);
    updates.push(...cycle.updates.values());
    for (const pendingId of cycle.result?.pendingSummarySessionIds ?? []) {
      assert.ok(!cycle.sent.some((row) => row.session.id === pendingId),
        "a partial accumulator cannot be sent");
    }
    elapsedMs += [...cycle.updates.values()].reduce((sum, update) => sum + update.durationMs, 0);
    const rows = rowCount(buffer);
    const bound = drainBound(rows, elapsedMs) + (churn ? 1 : 0);
    assert.ok(pass <= bound,
      JSON.stringify({ pass, bound, rows, marks: churn?.marks,
        pending: cycle.result?.pendingSummaryReasons }));
    if (cycle.state.caughtUp && cycle.state.lastSuccessfulUntil === cycle.until) {
      return { passes: pass, cycle, updates, retryMarks: (churn?.marks ?? 0) - marksBefore };
    }
  }
}

async function proveBusySession(size: number, ordinal: number) {
  const session = sessionId(ordinal);
  const buffer = new LocalEventBuffer(path.join(root, `busy-${size}.sqlite`), { workspaceId: tenantId });
  try {
    append(buffer, session, size);
    const { passes: initialPasses } = await initialCatchUp(buffer);
    assert.equal(state(buffer, session).complete, 1);
    const baseRecomputes = sessionSummaryCounters(buffer.database).fallbackRecomputes;
    const churn = new ChurnSchedule(buffer, session, size);
    let previousHorizon = loadDaemonSessionSyncState(buffer.database).lastSuccessfulUntil;
    const activePasses: number[] = [];
    const retryMarks: number[] = [];
    const repairRows: number[] = [];
    let priorScannedCorrections = 0;
    for (const checkpoint of [3, 6]) {
      while (churn.marks < checkpoint) await churn.waitForTick(churn.marks + 1);
      const beforeFallbacks = sessionSummaryCounters(buffer.database).fallbackRecomputes;
      const { cycle, passes, updates, retryMarks: duringRetries } = await initialCatchUp(buffer, churn);
      const read = updates.reduce((sum, update) => sum + update.rowsRead, 0);
      const scannedSinceLastCycle = churn.scannedCorrections.length - priorScannedCorrections;
      assert.equal(sessionSummaryCounters(buffer.database).fallbackRecomputes, beforeFallbacks,
        "queued scanned corrections must repair segments without restarting the session");
      assert.ok(updates.every((update) => !update.fullRecompute));
      if (scannedSinceLastCycle > 0) {
        assert.ok(read > 0, "a scanned correction must cause a bounded repair read");
      }
      assert.ok(updates.every((update) => update.rowsRead <= SESSION_SUMMARY_DEFAULT_MAX_ROWS));
      priorScannedCorrections = churn.scannedCorrections.length;
      activePasses.push(passes);
      retryMarks.push(duringRetries);
      repairRows.push(read);
      assert.ok(passes >= 2 && duringRetries >= 1,
        JSON.stringify({ size, checkpoint, passes, duringRetries }));
      assert.ok(cycle.result?.ok && cycle.result.summaryComplete,
        JSON.stringify({ size, checkpoint, pending: cycle.result?.pendingSummaryReasons }));
      assert.equal(cycle.state.caughtUp, true);
      assert.equal(cycle.state.lastSuccessfulUntil, cycle.until);
      assert.notEqual(cycle.until, previousHorizon);
      assert.deepEqual(cycle.sent.find((row) => row.session.id === session),
        expectedWire(buffer, session, cycle.until));
      previousHorizon = cycle.until;
    }
    // Include ticks due during the final pass, then drain that finite tail.
    const marksBeforeStop = churn.marks;
    const activeElapsedMs = churn.stop();
    let cleanupPasses = 0;
    if (churn.marks > marksBeforeStop) {
      const cleanup = await initialCatchUp(buffer);
      cleanupPasses = cleanup.passes;
      const cleanupRead = cleanup.updates.reduce((sum, update) => sum + update.rowsRead, 0);
      const scannedDuringStop = churn.scannedCorrections.length - priorScannedCorrections;
      if (scannedDuringStop > 0) {
        assert.ok(cleanupRead > 0, "the stopped tail must repair a scanned correction");
      }
      assert.ok(cleanup.updates.every((update) => !update.fullRecompute &&
        update.rowsRead <= SESSION_SUMMARY_DEFAULT_MAX_ROWS));
      repairRows.push(cleanupRead);
      assert.ok(cleanup.cycle.result?.ok && cleanup.cycle.result.summaryComplete);
      assert.deepEqual(cleanup.cycle.sent.find((row) => row.session.id === session),
        expectedWire(buffer, session, cleanup.cycle.until));
    }
    assert.ok(churn.scannedCorrections.length > 0,
      "the fixed schedule must correct a previously scanned row");
    assert.equal(sessionSummaryCounters(buffer.database).fallbackRecomputes, baseRecomputes);
    assert.ok(activeElapsedMs >= 6_000);
    assert.ok(churn.marks >= 6 && churn.appends >= 4);
    assert.equal(churn.appends, 2 * Math.ceil(churn.marks / 3));
    completion.check(`daemon_${size}_rows_1_mark_per_second_40_appends_per_minute`);
    console.log(JSON.stringify({ size, initialPasses, activeCycles: 2, activePasses, retryMarks,
      repairRows, scannedCorrections: churn.scannedCorrections,
      marks: churn.marks, appends: churn.appends, activeElapsedMs, cleanupPasses,
      horizon: previousHorizon }));
  } finally { buffer.close(); }
}

async function proveHistoricalAndReadRace() {
  const session = sessionId(10);
  const buffer = new LocalEventBuffer(path.join(root, "historical.sqlite"), { workspaceId: tenantId });
  try {
    const directRead = proofRead(buffer);
    const ids = append(buffer, session, 10_000);
    await initialCatchUp(buffer);
    const before = revision(buffer, session);
    buffer.database.prepare("update buffered_events set output_tokens = 7 where id = ?").run(ids[0]);
    assert.ok(revision(buffer, session) > before);
    const first = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: directRead, maxRows: 1_000,
    });
    assert.equal(first.complete, false);
    assert.equal(first.snapshot, null, "a partial segment repair cannot be sent");
    assert.equal(state(buffer, session).mode, "incremental");
    const afterFirst = revision(buffer, session);
    buffer.database.prepare("update buffered_events set output_tokens = 9 where id = ?").run(ids.at(-1));
    buffer.database.prepare("delete from buffered_events where id = ?").run(ids.at(-2));
    assert.ok(revision(buffer, session) > afterFirst,
      "subsequent scanned edits must remain visible during segment repair");
    const final = await completeSummary(buffer, session, first, (result) => {
      assert.equal(result.fullRecompute, false);
    });
    assert.equal(final.complete, true);
    assert.deepEqual(final.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: new Date().toISOString(),
    })[0]);
    completion.check("unscanned_historical_edit_and_erasure_keep_cursor_and_match_full_rebuild");

    // Move a row behind the historical cursor while a different segment is
    // being repaired. The rowid repair queue must still produce an exact wire.
    buffer.database.prepare("update buffered_events set input_tokens = 12 where id = ?").run(ids[0]);
    let beforeMove = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: directRead, maxRows: 1_000,
    });
    let preMovePasses = 1;
    let preMoveElapsedMs = beforeMove.durationMs;
    while (beforeMove.rowsRead === 0) {
      assert.ok(preMovePasses <= drainBound(rowCount(buffer), preMoveElapsedMs),
        JSON.stringify({ preMovePasses, preMoveElapsedMs, beforeMove }));
      beforeMove = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
        read: directRead,
      });
      preMovePasses += 1;
      preMoveElapsedMs += beforeMove.durationMs;
    }
    assert.equal(beforeMove.complete, false);
    assert.equal(beforeMove.snapshot, null);
    const cursor = JSON.parse(state(buffer, session).accumulatorJson) as { cursorObservedAt: string | null };
    assert.ok(cursor.cursorObservedAt && cursor.cursorObservedAt > "2026-01-01T00:00:00.000Z",
      JSON.stringify({ beforeMove, cursor }));
    const beforeMoveRevision = revision(buffer, session);
    buffer.database.prepare("update buffered_events set observed_at = ? where id = ?")
      .run("2026-01-01T00:00:00.000Z", ids.at(-1));
    assert.ok(revision(buffer, session) > beforeMoveRevision);
    const firstMoved = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: directRead,
    });
    assert.equal(firstMoved.fullRecompute, false,
      "moving a scanned row repairs bounded segments without a session restart");
    const moved = await completeSummary(buffer, session, firstMoved, (update) => {
      assert.equal(update.fullRecompute, false);
    });
    assert.equal(moved.complete, true);
    assert.deepEqual(moved.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: new Date().toISOString(),
    })[0]);
    completion.check("row_moved_behind_historical_cursor_repairs_only_that_session");

    // A write after the worker has read a slice but before its state commit
    // must discard that slice, even though the old durable cursor calls it
    // unscanned. This exercises the activity fence without a timing race.
    buffer.database.prepare("update buffered_events set input_tokens = 13 where id = ?").run(ids[0]);
    const restarted = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: directRead, maxRows: 1_000,
    });
    assert.equal(restarted.complete, false);
    const committedBeforeRace = state(buffer, session).accumulatorJson;
    let injected = false;
    const beforeActivity = activity(buffer, session);
    const beforeRevision = revision(buffer, session);
    const raced = await updateSessionSummary(buffer.database, session, new Date().toISOString(), {
      read: async <T>(queries: Parameters<SessionSummaryRead>[0]) => {
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
    assert.ok(revision(buffer, session) > beforeRevision,
      "the scanned row changed during a repair read");
    assert.ok(activity(buffer, session) > beforeActivity);
    assert.equal(raced.complete, false);
    assert.equal(raced.snapshot, null, "a raced slice cannot reach the wire");
    assert.equal(raced.fallbackReason, "ledger_mutation_during_slice");
    assert.equal(state(buffer, session).accumulatorJson, committedBeforeRace);
    const recovered = await completeSummary(buffer, session, raced);
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
    await initialCatchUp(buffer);
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
    const { cycle: beforeSecond } = await initialCatchUp(buffer);
    assert.ok(beforeSecond.result?.summaryComplete);
    const delivery = (buffer.database.prepare("select delivery_id as id from upload_outbox where raw_id = ?")
      .get(secondId) as { id: string }).id;
    // The outbox's immutable lineage now belongs to a different incarnation.
    buffer.database.prepare("update buffered_events set id = ? where id = ?")
      .run(eventId(), secondId);
    assert.equal(dirty(buffer, session), "raw_update");
    const excludedUntil = new Date().toISOString();
    const excluded = await updateSessionSummary(buffer.database, session, excludedUntil, {
      read: proofRead(buffer),
    });
    assert.equal(excluded.complete, true);
    assert.deepEqual(excluded.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: excludedUntil,
    })[0]);
    assert.equal(excluded.snapshot?.events, 2);
    const beforeDelete = revision(buffer, session);
    const beforeDeleteControl = sessionSummaryCounters(buffer.database).mutationRevision;
    buffer.database.prepare("delete from upload_outbox where delivery_id = ?").run(delivery);
    assert.equal(revision(buffer, session), beforeDelete);
    assert.equal(sessionSummaryCounters(buffer.database).mutationRevision, beforeDeleteControl);
    const restoredUntil = new Date().toISOString();
    const restored = await updateSessionSummary(buffer.database, session, restoredUntil, {
      read: proofRead(buffer),
    });
    assert.equal(restored.complete, true);
    assert.deepEqual(restored.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: restoredUntil,
    })[0]);
    assert.equal(restored.snapshot?.events, 2);
    completion.check("mismatched_incarnation_never_changes_summary_eligibility");

    buffer.database.prepare("delete from buffered_events where id = ?").run(firstId);
    assert.equal(dirty(buffer, session), "raw_delete");
    const erasedUntil = new Date().toISOString();
    const erased = await updateSessionSummary(buffer.database, session, erasedUntil, {
      read: proofRead(buffer),
    });
    assert.equal(erased.complete, true);
    assert.deepEqual(erased.snapshot, collectSessionSnapshots(buffer.database, {
      sessionIds: [session], until: erasedUntil,
    })[0]);
    assert.equal(erased.snapshot?.events, 1);
    completion.check("scanned_erasure_still_wins");

    const otherSession = sessionId(12);
    append(buffer, otherSession, 1);
    assert.ok((await initialCatchUp(buffer)).cycle.result?.summaryComplete);
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
    const { cycle: movedCycle } = await initialCatchUp(buffer);
    assert.ok(movedCycle.result?.summaryComplete);
    assert.equal(movedCycle.state.lastSuccessfulUntil, movedCycle.until);
    assert.deepEqual(movedCycle.sent.find((row) => row.session.id === otherSession),
      expectedWire(buffer, otherSession, movedCycle.until));
    const oldSummary = await updateSessionSummary(buffer.database, session, movedCycle.until, {
      read: proofRead(buffer),
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
