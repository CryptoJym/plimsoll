import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { build } from "esbuild";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { buildSessionSyncRow, collectSessionSnapshots,
  commitDaemonSessionSyncFailure, commitDaemonSessionSyncSuccess,
  loadDaemonSessionSyncState, planDaemonSessionSync, runSessionSync,
  saveDaemonSessionSyncState } from "../packages/collector-cli/src/session-sync";
import { SESSION_SUMMARY_DEFAULT_MAX_MS, SESSION_SUMMARY_DEFAULT_MAX_ROWS,
  sessionSummaryCounters,
  type SessionSummaryRead, type SessionSummaryUpdateResult } from "../packages/collector-cli/src/session-summary";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-cycle-resume");
const root = process.env.PLIMSOLL_PROOF_ROOT!;
const tenantId = "00000000-0000-4000-8000-000000000086";
const installKey = "cycle-resume-proof-install";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey,
  uploadSigningSecret: "cycle-resume-proof-secret",
});
const ids = {
  studio4: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaa86",
  studio5: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbb86",
  fiveSlices: "cccccccc-cccc-4ccc-8ccc-cccccccccc86",
  control: "dddddddd-dddd-4ddd-8ddd-dddddddddd86",
  upgrade: "eeeeeeee-eeee-4eee-8eee-eeeeeeeeee86",
};
let eventNumber = 1;
const eventId = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
class ProofClock {
  constructor(private instant = Date.now()) {}
  now() { return this.instant; }
  nextUntil() { this.instant += 1_000; return new Date(this.instant).toISOString(); }
  advancePast(iso: string) { this.instant = Math.max(this.instant, Date.parse(iso) + 1); }
}
const clock = new ProofClock();
const rowReadDelayMs = Number(process.env.PLIMSOLL_PROOF_ROW_READ_DELAY_MS ?? "0");
assert.ok(Number.isFinite(rowReadDelayMs) && rowReadDelayMs >= 0 && rowReadDelayMs <= 1);

// The proof controls read speed without changing the production reader. The
// proxy delays each row when the summarizer consumes it, preserving the SQL
// result length and cursor semantics while making the 250 ms cap observable.
function proofRead(buffer: LocalEventBuffer, slow: boolean): SessionSummaryRead {
  return async <T>(queries: Parameters<SessionSummaryRead>[0]): Promise<T[]> => {
    assert.equal(queries.length, 1);
    const query = queries[0]!;
    const rows = buffer.database.prepare(query.sql).all(query.params) as T[];
    if (!slow || typeof query.params.limit !== "number") return rows;
    return new Proxy(rows, {
      get(target, property, receiver) {
        if (typeof property === "string" && /^(0|[1-9]\d*)$/.test(property)) {
          const readyAt = performance.now() + rowReadDelayMs;
          while (performance.now() < readyAt) { /* proof-only per-row read latency */ }
        }
        return Reflect.get(target, property, receiver);
      },
    });
  };
}

// LocalEventBuffer.appendMany is the collector's real event insertion path.
function insertEventRows(buffer: LocalEventBuffer, sessionId: string, count: number,
  observedAt = new Date(Date.now() + 60_000).toISOString()): string[] {
  const inserted: string[] = [];
  for (let start = 0; start < count; start += 250) {
    const entries = Array.from({ length: Math.min(250, count - start) }, (_, offset) => {
      const id = eventId(eventNumber++);
      inserted.push(id);
      return { event: aiInteractionEventSchema.parse({
        id, sessionId, source: "codex", eventType: "assistant_response",
        observedAt: new Date(Date.parse(observedAt) + start + offset).toISOString(),
        inputTokens: 2, outputTokens: 1,
      }), suppressedFields: [] };
    });
    const result = buffer.appendMany(entries);
    assert.equal(result.deduplicatedCount, 0);
    assert.equal(result.enrollmentRejectedEventCount, 0);
  }
  return inserted;
}

type Cycle = Awaited<ReturnType<typeof daemonCycle>>;
async function daemonCycle(buffer: LocalEventBuffer, slow = false, summaryMaxRows?: number) {
  const until = clock.nextUntil();
  const updates = new Map<string, SessionSummaryUpdateResult>();
  const prior = loadDaemonSessionSyncState(buffer.database);
  const plan = planDaemonSessionSync({ db: buffer.database, state: prior, uploadedBatches: [], until });
  saveDaemonSessionSyncState(buffer.database, plan.state);
  const sent: Array<{ session: { id: string }; [key: string]: unknown }> = [];
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
  const result = await runSessionSync(config, {
    // The disposable fixture already knows its session IDs. Keep the daemon
    // plan and sync path, while excluding the independent worker-census timer
    // from this deterministic summary-drain proof.
    sessionIds: plan.sessionIds ?? (buffer.database.prepare(`select distinct session_id as sessionId
      from buffered_events where session_id is not null`).all() as Array<{ sessionId: string }>)
      .map((row) => row.sessionId),
    until: plan.until, ledgerDb: buffer.database, incremental: true,
    ...(summaryMaxRows === undefined ? {} : { summaryMaxRows }),
    proofSummaryHooks: {
      read: proofRead(buffer, slow && rowReadDelayMs > 0),
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

function stateRow(buffer: LocalEventBuffer, sessionId: string) {
  return buffer.database.prepare(`select complete, mode, high_water as highWater,
    covered_until as coveredUntil, accumulator_json as accumulatorJson
    from session_sync_summary_state where session_id = ?`).get(sessionId) as {
      complete: number; mode: string; highWater: number; coveredUntil: string; accumulatorJson: string;
    };
}

function accumulator(buffer: LocalEventBuffer, sessionId: string) {
  return JSON.parse(stateRow(buffer, sessionId).accumulatorJson) as {
    scanBoundary: number; events: number; futureRows: boolean; futureCreatedAt: string | null;
  };
}

function expectedWire(buffer: LocalEventBuffer, sessionId: string, until: string) {
  const snapshot = collectSessionSnapshots(buffer.database, { sessionIds: [sessionId], until })[0];
  assert.ok(snapshot, sessionId);
  const row = buildSessionSyncRow(snapshot);
  assert.equal(row.ok, true);
  return row.row;
}

async function waitForInitialCatchUp(buffer: LocalEventBuffer, maxPasses: number) {
  const trace: unknown[] = [];
  for (let pass = 1; pass <= maxPasses; pass += 1) {
    const cycle = await daemonCycle(buffer);
    if (cycle.state.caughtUp && cycle.state.lastSuccessfulUntil === cycle.until) return cycle;
    trace.push({ pass, pending: cycle.result?.pendingSummarySessionIds,
      stats: cycle.result?.summaryStats, reasons: cycle.result?.pendingSummaryReasons });
  }
  throw new Error(`initial_session_sync_did_not_complete_within_${maxPasses}_passes ${JSON.stringify(trace)}`);
}

async function load0740Summary() {
  const commit = "92c33bf98e381ed3a601800d50d1359a86c2f8f3";
  const source = execFileSync("git", ["show", `${commit}:packages/collector-cli/src/session-summary.ts`],
    { encoding: "utf8" });
  assert.ok(source.includes("Date.parse(stored.coveredUntil) === Date.parse(until)"));
  const sha256 = createHash("sha256").update(source).digest("hex");
  const outfile = path.join(root, "session-summary-0740.mjs");
  await build({ stdin: {
    contents: source, sourcefile: "session-summary-0740.ts",
    resolveDir: path.resolve("packages/collector-cli/src"), loader: "ts",
  }, bundle: true, platform: "node", format: "esm", target: "node20",
    outfile, packages: "external", logLevel: "silent" });
  console.log(JSON.stringify({ legacySourceCommit: commit, legacySourceSha256: sha256 }));
  return await import(pathToFileURL(outfile).href) as typeof import("../packages/collector-cli/src/session-summary");
}

async function main() {
  const buffer = new LocalEventBuffer(path.join(root, "moving-horizon.sqlite"), { workspaceId: tenantId });
  try {
    const rows = new Map([
      [ids.studio4, insertEventRows(buffer, ids.studio4, 5_094)],
      [ids.studio5, insertEventRows(buffer, ids.studio5, 10_119)],
      [ids.fiveSlices, insertEventRows(buffer, ids.fiveSlices, 20_001)],
      [ids.control, insertEventRows(buffer, ids.control, 1)],
    ]);
    // Fixture insertion can take seconds; start the injected wall clock after
    // its created_at values so ordinary rows are not mistaken for future rows.
    clock.advancePast(new Date(Date.now() + 1_000).toISOString());
    const initial = await waitForInitialCatchUp(buffer, 12);
    completion.check("initial_daemon_horizon_caught_up", initial.state.caughtUp && initial.state.lastSuccessfulUntil === initial.until);
    for (const [sessionId, eventIds] of rows) {
      if (sessionId === ids.control) continue;
      // This is the collector's raw-update path, which fires the summary revision trigger.
      assert.equal(buffer.database.prepare("update buffered_events set output_tokens = 7 where id = ?")
        .run(eventIds[0]).changes, 1);
    }
    const startingHorizon = initial.state.lastSuccessfulUntil;
    const completedAt = new Map<string, number>();
    const elapsedBySession = new Map<string, number>();
    const appliedBySession = new Map<string, number>();
    const timeBoundUpdates: Array<{ sessionId: string; pass: number; rowsRead: number; durationMs: number }> = [];
    let frozenBoundary: number | null = null;
    let appendedRowid: number | null = null;
    let appendDrainCount = 0;
    let last: Cycle | null = null;
    for (let pass = 1; pass <= (rowReadDelayMs > 0 ? 40 : 12); pass += 1) {
      const previousHighWater = stateRow(buffer, ids.studio4).highWater;
      // Keep the first edited segment partial so the backdated append lands
      // during a real daemon repair, even when this host reads very quickly.
      last = await daemonCycle(buffer, rowReadDelayMs > 0, pass === 1 ? 1_000 : undefined);
      if (pass === 1) {
        const firstState = stateRow(buffer, ids.studio4);
        assert.equal(firstState.mode, "incremental");
        assert.equal(firstState.complete, 0);
        frozenBoundary = accumulator(buffer, ids.studio4).scanBoundary;
        assert.ok(frozenBoundary > 0);
        // A new row has a later rowid but an observation behind the historical cursor.
        const first = buffer.database.prepare("select observed_at as observedAt from buffered_events where id = ?")
          .get(rows.get(ids.studio4)![0]) as { observedAt: string };
        const appendedId = insertEventRows(buffer, ids.studio4, 1,
          new Date(Date.parse(first.observedAt) - 1).toISOString());
        appendedRowid = (buffer.database.prepare("select rowid from buffered_events where id = ?")
          .get(appendedId[0]) as { rowid: number }).rowid;
        assert.ok(appendedRowid > frozenBoundary);
        // The append uses real creation time. Keep the next injected daemon
        // horizon after it, even if this partial pass was slow on the host.
        clock.advancePast(new Date(Date.now() + 1_000).toISOString());
      }
      assert.equal(accumulator(buffer, ids.studio4).scanBoundary, frozenBoundary,
        `scanBoundary changed on daemon pass ${pass}`);
      for (const sessionId of [ids.studio4, ids.studio5, ids.fiveSlices]) {
        const update = last.updates.get(sessionId);
        assert.ok(update, `missing summary update for ${sessionId} on pass ${pass}`);
        if (completedAt.has(sessionId)) continue;
        assert.ok(update.complete || update.rowsRead > 0 ||
          update.durationMs >= SESSION_SUMMARY_DEFAULT_MAX_MS ||
          update.fallbackReason === "append_queue",
        JSON.stringify({ sessionId, pass, update, reason: "unaccounted_partial_pass" }));
        if (!update.complete) {
          assert.ok(!last.sent.some((row) => row.session.id === sessionId),
            "a partial segment aggregate must not be sent");
        }
        elapsedBySession.set(sessionId, (elapsedBySession.get(sessionId) ?? 0) + update.durationMs);
        appliedBySession.set(sessionId, (appliedBySession.get(sessionId) ?? 0) + update.rowsApplied);
        if (!update.complete && update.durationMs >= SESSION_SUMMARY_DEFAULT_MAX_MS &&
            update.rowsRead > 0 && update.rowsRead < SESSION_SUMMARY_DEFAULT_MAX_ROWS) {
          timeBoundUpdates.push({ sessionId, pass, rowsRead: update.rowsRead, durationMs: update.durationMs });
        }
        if (stateRow(buffer, sessionId).complete === 1) completedAt.set(sessionId, pass);
      }
      if (appendedRowid !== null && previousHighWater < appendedRowid &&
          stateRow(buffer, ids.studio4).highWater >= appendedRowid) {
        appendDrainCount += 1;
      }
      if (last.state.caughtUp && last.state.lastSuccessfulUntil === last.until) break;
    }
    assert.ok(last?.state.caughtUp && last.state.lastSuccessfulUntil === last.until,
      JSON.stringify({ completedAt: [...completedAt], last: last?.result?.summaryStats }));
    assert.equal(last.state.lastSuccessfulUntil, last.until);
    assert.notEqual(last.state.lastSuccessfulUntil, startingHorizon);
    completion.check("moving_horizon_advances_after_all_summaries_complete");
    assert.equal(appendDrainCount, 1);
    assert.equal(accumulator(buffer, ids.studio4).scanBoundary, frozenBoundary);
    assert.equal(accumulator(buffer, ids.studio4).events, 5_095);
    completion.check("backdated_append_keeps_frozen_boundary_and_drains_once");
    completion.check("partial_passes_make_progress_or_reach_time_budget");
    for (const [sessionId, count] of [[ids.studio4, 5_095], [ids.studio5, 10_119], [ids.fiveSlices, 20_001]] as const) {
      // Include one bounded segment repair, the deliberate first 1,000-row
      // slice, and the post-boundary append allowance in addition to row and
      // time caps. The wire comparison below proves all rows were included.
      const elapsedMs = elapsedBySession.get(sessionId)!;
      const bound = Math.ceil(count / SESSION_SUMMARY_DEFAULT_MAX_ROWS) +
        Math.floor(elapsedMs / SESSION_SUMMARY_DEFAULT_MAX_MS) + 3;
      assert.ok((completedAt.get(sessionId) ?? Infinity) <= bound,
        JSON.stringify({ sessionId, passes: completedAt.get(sessionId), bound, elapsedMs }));
      const sentRow: unknown = last.sent.find((row) => row.session.id === sessionId);
      assert.deepEqual(sentRow, expectedWire(buffer, sessionId, last.until));
      console.log(JSON.stringify({ drain: count, passes: completedAt.get(sessionId), bound,
        elapsedMs, applied: appliedBySession.get(sessionId) }));
      completion.check(`from_scratch_${count}_rows_with_load_aware_bound`);
    }
    if (rowReadDelayMs > 0) {
      assert.ok(timeBoundUpdates.length > 0, "injected read delay never reached the 250 ms cap");
      console.log(JSON.stringify({ rowReadDelayMs, timeBoundUpdates }));
    }
    completion.check("read_delay_exercises_time_cap_when_injected");

    // A deletion during a partial segment repair changes its revision. The
    // in-flight segment must be reread before the erased row can reach wire.
    assert.equal(buffer.database.prepare("update buffered_events set input_tokens = 77 where id = ?")
      .run(rows.get(ids.studio4)![1]).changes, 1);
    const beforeErasure = await daemonCycle(buffer, false, 1_000);
    assert.ok(beforeErasure.result?.pendingSummarySessionIds.includes(ids.studio4));
    assert.ok(!beforeErasure.sent.some((row) => row.session.id === ids.studio4));
    assert.equal(buffer.database.prepare("delete from buffered_events where id = ?")
      .run(rows.get(ids.studio4)![2]).changes, 1);
    let erased: Cycle | null = null;
    for (let pass = 0; pass < 4; pass += 1) {
      erased = await daemonCycle(buffer);
      if (erased.state.caughtUp && erased.state.lastSuccessfulUntil === erased.until) break;
    }
    assert.ok(erased?.state.caughtUp && erased.state.lastSuccessfulUntil === erased.until);
    assert.deepEqual(erased.sent.find((row) => row.session.id === ids.studio4),
      expectedWire(buffer, ids.studio4, erased.until));
    completion.check("erasure_during_rebuild_wins_and_horizon_recovers");

    // A mutation every cycle repairs the same bounded segment. The control
    // session still sends, and any sent busy-session snapshot is exact.
    let stormHorizon = erased.state.lastSuccessfulUntil;
    let lastStorm: Cycle | null = null;
    for (let pass = 0; pass < 4; pass += 1) {
      insertEventRows(buffer, ids.control, 1);
      clock.advancePast(new Date(Date.now() + 1_000).toISOString());
      assert.equal(buffer.database.prepare("update buffered_events set output_tokens = ? where id = ?")
        .run(20 + pass, rows.get(ids.studio4)![0]).changes, 1);
      const cycle = await daemonCycle(buffer);
      lastStorm = cycle;
      assert.ok(cycle.sent.some((row) => row.session.id === ids.control));
      assert.deepEqual(cycle.sent.find((row) => row.session.id === ids.control),
        expectedWire(buffer, ids.control, cycle.until));
      assert.equal(cycle.result?.summaryStats.fullRecomputes, 0);
      const busySent = cycle.sent.find((row) => row.session.id === ids.studio4);
      if (busySent) {
        assert.deepEqual(busySent, expectedWire(buffer, ids.studio4, cycle.until));
        assert.equal(cycle.state.lastSuccessfulUntil, cycle.until);
      } else {
        assert.ok(cycle.result?.pendingSummarySessionIds.includes(ids.studio4));
        assert.equal(cycle.state.lastSuccessfulUntil, stormHorizon);
      }
      stormHorizon = cycle.state.lastSuccessfulUntil;
    }
    completion.check("periodic_updates_visible_while_other_session_syncs");
    const recovered = lastStorm !== null && lastStorm.state.lastSuccessfulUntil === lastStorm.until
      ? lastStorm : await waitForInitialCatchUp(buffer, 4);
    assert.deepEqual(recovered.sent.find((row) => row.session.id === ids.studio4),
      expectedWire(buffer, ids.studio4, recovered.until));
    completion.check("periodic_update_storm_recovers_after_updates_stop");
  } finally { buffer.close(); }

  // Generate an actually stuck state with the published 0.7.40 summary
  // implementation, then switch to this build without touching its rows.
  const old = await load0740Summary();
  const upgraded = new LocalEventBuffer(path.join(root, "upgrade-0740.sqlite"), { workspaceId: tenantId });
  try {
    const inserted = insertEventRows(upgraded, ids.upgrade, 5_094);
    // This ledger is built after the first fixture. Keep its ordinary rows
    // behind the injected clock even when host load makes insertion slow.
    clock.advancePast(new Date(Date.now() + 1_000).toISOString());
    old.ensureSessionSummarySchema(upgraded.database);
    const initialUntil = clock.nextUntil();
    for (let pass = 0; pass < 4; pass += 1) {
      const result = await old.updateSessionSummary(upgraded.database, ids.upgrade, initialUntil, {
        read: proofRead(upgraded, false),
      });
      if (result.complete) break;
    }
    assert.equal(stateRow(upgraded, ids.upgrade).complete, 1);
    assert.equal(upgraded.database.prepare("update buffered_events set input_tokens = 99 where id = ?")
      .run(inserted[0]).changes, 1);
    const legacyPasses: Array<{ highWater: number; rowsRead: number; events: number }> = [];
    for (let pass = 0; pass < 3; pass += 1) {
      const result = await old.updateSessionSummary(upgraded.database, ids.upgrade, clock.nextUntil(), {
        read: proofRead(upgraded, false), maxRows: 2_500,
      });
      assert.equal(result.complete, false);
      assert.equal(result.fullRecompute, true, "the published build restarts this changed session");
      assert.ok(result.highWater <= 2_500, "each published pass restarts before the row cap");
      legacyPasses.push({ highWater: result.highWater, rowsRead: result.rowsRead,
        events: JSON.parse(stateRow(upgraded, ids.upgrade).accumulatorJson).events });
    }
    console.log(JSON.stringify({ legacyPasses }));
    assert.ok(sessionSummaryCounters(upgraded.database).fallbackRecomputes >= 3);
    completion.check("published_0740_path_creates_repeating_partial_fallback");
    let recovered: Cycle | null = null;
    for (let pass = 0; pass < 3; pass += 1) {
      recovered = await daemonCycle(upgraded);
      if (recovered.state.caughtUp && recovered.state.lastSuccessfulUntil === recovered.until) break;
    }
    assert.ok(recovered?.state.caughtUp && recovered.state.lastSuccessfulUntil === recovered.until);
    assert.equal(recovered.state.lastSuccessfulUntil, recovered.until);
    assert.deepEqual(recovered.sent.find((row) => row.session.id === ids.upgrade),
      expectedWire(upgraded, ids.upgrade, recovered.until));
    completion.check("0740_stuck_ledger_recovers_without_manual_step");

    // The injected daemon clock crosses this row's creation time while a
    // bounded segment repair is partial. No direct update or artificial until
    // bypasses planDaemonSessionSync/runSessionSync.
    const futureCreatedAt = new Date(clock.now() + 3_000).toISOString();
    assert.equal(upgraded.database.prepare("update buffered_events set created_at = ? where id = ?")
      .run(futureCreatedAt, inserted[1]).changes, 1);
    const recomputesBeforeFuture = sessionSummaryCounters(upgraded.database).fallbackRecomputes;
    const beforeMaturity = await daemonCycle(upgraded, false, 2_000);
    assert.ok(Date.parse(beforeMaturity.until) < Date.parse(futureCreatedAt));
    assert.equal(stateRow(upgraded, ids.upgrade).complete, 0);
    assert.ok(beforeMaturity.result?.pendingSummarySessionIds.includes(ids.upgrade));
    assert.ok(!beforeMaturity.sent.some((row) => row.session.id === ids.upgrade));
    assert.equal(beforeMaturity.state.lastSuccessfulUntil, recovered.until);
    assert.ok(upgraded.database.prepare(`select 1 from session_sync_summary_repairs
      where session_id = ?`).get(ids.upgrade), "the changed future row needs a durable repair");
    assert.equal(sessionSummaryCounters(upgraded.database).fallbackRecomputes, recomputesBeforeFuture);
    const stillBeforeMaturity = await daemonCycle(upgraded, false, 2_000);
    assert.ok(Date.parse(stillBeforeMaturity.until) < Date.parse(futureCreatedAt));
    assert.equal(stateRow(upgraded, ids.upgrade).complete, 0);
    assert.equal(stillBeforeMaturity.updates.get(ids.upgrade)?.fullRecompute, false);
    assert.equal(sessionSummaryCounters(upgraded.database).fallbackRecomputes, recomputesBeforeFuture);
    assert.ok(!stillBeforeMaturity.sent.some((row) => row.session.id === ids.upgrade));
    assert.equal(stillBeforeMaturity.state.lastSuccessfulUntil, recovered.until);
    completion.check("future_row_stays_unsent_while_pre_maturity_repair_is_partial");

    clock.advancePast(futureCreatedAt);
    let maturityRebuilds = 0;
    let matured: Cycle | null = null;
    for (let pass = 0; pass < 10; pass += 1) {
      matured = await daemonCycle(upgraded, false, 2_000);
      const update = matured.updates.get(ids.upgrade);
      console.log(JSON.stringify({ maturityPass: pass + 1, until: matured.until,
        fullRecompute: update?.fullRecompute, fallbackReason: update?.fallbackReason,
        rowsRead: update?.rowsRead, complete: update?.complete,
        horizon: matured.state.lastSuccessfulUntil }));
      if (update?.fullRecompute) maturityRebuilds += 1;
      if (pass === 0) {
        assert.equal(update?.fullRecompute, false);
      }
      if (stateRow(upgraded, ids.upgrade).complete === 0) {
        assert.ok(!matured.sent.some((row) => row.session.id === ids.upgrade));
        assert.equal(matured.state.lastSuccessfulUntil, recovered.until);
      }
      if (matured.state.caughtUp && matured.state.lastSuccessfulUntil === matured.until) break;
    }
    assert.equal(maturityRebuilds, 0);
    assert.equal(sessionSummaryCounters(upgraded.database).fallbackRecomputes, recomputesBeforeFuture);
    assert.ok(matured?.state.caughtUp && matured.state.lastSuccessfulUntil === matured.until);
    assert.ok(Date.parse(matured.until) > Date.parse(futureCreatedAt));
    assert.deepEqual(matured.sent.find((row) => row.session.id === ids.upgrade),
      expectedWire(upgraded, ids.upgrade, matured.until));
    assert.equal(accumulator(upgraded, ids.upgrade).events, inserted.length);
    completion.check("future_row_matures_during_partial_daemon_summary_once");
  } finally { upgraded.close(); }
  completion.complete();
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
