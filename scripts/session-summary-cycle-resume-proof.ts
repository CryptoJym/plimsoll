import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

import { build } from "esbuild";

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
let horizonNumber = 0;
const eventId = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const nextUntil = () => new Date(Date.now() + (++horizonNumber) * 1_000).toISOString();

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
async function daemonCycle(buffer: LocalEventBuffer) {
  const until = nextUntil();
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

function stateRow(buffer: LocalEventBuffer, sessionId: string) {
  return buffer.database.prepare(`select complete, mode, high_water as highWater,
    covered_until as coveredUntil, accumulator_json as accumulatorJson
    from session_sync_summary_state where session_id = ?`).get(sessionId) as {
      complete: number; mode: string; highWater: number; coveredUntil: string; accumulatorJson: string;
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
  for (let pass = 1; pass <= maxPasses; pass += 1) {
    const cycle = await daemonCycle(buffer);
    if (cycle.state.caughtUp && cycle.state.lastSuccessfulUntil === cycle.until) return cycle;
  }
  throw new Error(`initial_session_sync_did_not_complete_within_${maxPasses}_passes`);
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
    let last: Cycle | null = null;
    for (let pass = 1; pass <= 8; pass += 1) {
      last = await daemonCycle(buffer);
      if (pass === 1) {
        // A new row has a later rowid but an observation behind the historical cursor.
        const first = buffer.database.prepare("select observed_at as observedAt from buffered_events where id = ?")
          .get(rows.get(ids.studio4)![0]) as { observedAt: string };
        insertEventRows(buffer, ids.studio4, 1,
          new Date(Date.parse(first.observedAt) - 1).toISOString());
      }
      for (const sessionId of [ids.studio4, ids.studio5, ids.fiveSlices]) {
        if (!completedAt.has(sessionId) && stateRow(buffer, sessionId).complete === 1) completedAt.set(sessionId, pass);
      }
      if (last.state.caughtUp && last.state.lastSuccessfulUntil === last.until) break;
    }
    assert.ok(last?.state.caughtUp && last.state.lastSuccessfulUntil === last.until,
      JSON.stringify({ completedAt: [...completedAt], last: last?.result?.summaryStats }));
    assert.equal(last.state.lastSuccessfulUntil, last.until);
    assert.notEqual(last.state.lastSuccessfulUntil, startingHorizon);
    completion.check("moving_horizon_advances_after_all_summaries_complete");
    for (const [sessionId, count] of [[ids.studio4, 5_095], [ids.studio5, 10_119], [ids.fiveSlices, 20_001]] as const) {
      const bound = Math.ceil(count / 5_000) + 1;
      assert.ok((completedAt.get(sessionId) ?? Infinity) <= bound,
        JSON.stringify({ sessionId, passes: completedAt.get(sessionId), bound }));
      const sentRow: unknown = last.sent.find((row) => row.session.id === sessionId);
      assert.deepEqual(sentRow, expectedWire(buffer, sessionId, last.until));
      completion.check(`from_scratch_${count}_rows_within_${bound}_passes`);
    }

    // A deletion after a partial historical slice changes the revision, so
    // the next pass must rebuild and the erased row cannot reach the wire.
    assert.equal(buffer.database.prepare("update buffered_events set input_tokens = 77 where id = ?")
      .run(rows.get(ids.studio4)![1]).changes, 1);
    const beforeErasure = await daemonCycle(buffer);
    assert.ok(beforeErasure.result?.pendingSummarySessionIds.includes(ids.studio4));
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

    // A mutation every cycle can exceed rebuild throughput. The host cannot
    // claim full catch-up, but each cycle still sends an unrelated session.
    const beforeStorm = erased.state.lastSuccessfulUntil;
    for (let pass = 0; pass < 4; pass += 1) {
      insertEventRows(buffer, ids.control, 1);
      assert.equal(buffer.database.prepare("update buffered_events set output_tokens = ? where id = ?")
        .run(20 + pass, rows.get(ids.studio4)![0]).changes, 1);
      const cycle = await daemonCycle(buffer);
      assert.equal(cycle.state.lastSuccessfulUntil, beforeStorm);
      assert.ok(cycle.result?.pendingSummarySessionIds.includes(ids.studio4));
      assert.ok(cycle.sent.some((row) => row.session.id === ids.control));
      assert.ok((cycle.result?.summaryStats.fullRecomputes ?? 0) >= 1);
      assert.ok((cycle.result?.pendingSummaryReasons.ledger_mutation ?? 0) >= 1);
    }
    completion.check("periodic_updates_visible_while_other_session_syncs");
    const recovered = await waitForInitialCatchUp(buffer, 4);
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
    old.ensureSessionSummarySchema(upgraded.database);
    const initialUntil = nextUntil();
    for (let pass = 0; pass < 4; pass += 1) {
      const result = await old.updateSessionSummary(upgraded.database, ids.upgrade, initialUntil, {
        read: (queries) => readLedgerOffThread(upgraded.database, queries),
      });
      if (result.complete) break;
    }
    assert.equal(stateRow(upgraded, ids.upgrade).complete, 1);
    assert.equal(upgraded.database.prepare("update buffered_events set input_tokens = 99 where id = ?")
      .run(inserted[0]).changes, 1);
    const cursors: number[] = [];
    for (let pass = 0; pass < 3; pass += 1) {
      const result = await old.updateSessionSummary(upgraded.database, ids.upgrade, nextUntil(), {
        read: (queries) => readLedgerOffThread(upgraded.database, queries),
      });
      assert.equal(result.complete, false);
      cursors.push(JSON.parse(stateRow(upgraded, ids.upgrade).accumulatorJson).events);
    }
    assert.equal(new Set(cursors).size, 1, JSON.stringify(cursors));
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

    // A preexisting row beyond the current horizon must not restart every
    // moving-horizon slice. It is rebuilt once when that row becomes eligible.
    const futureCreatedAt = new Date(Date.now() + 3_600_000).toISOString();
    assert.equal(upgraded.database.prepare("update buffered_events set created_at = ? where id = ?")
      .run(futureCreatedAt, inserted[1]).changes, 1);
    const recomputesBeforeFuture = sessionSummaryCounters(upgraded.database).fallbackRecomputes;
    let beforeMaturity: Cycle | null = null;
    for (let pass = 0; pass < 3; pass += 1) {
      beforeMaturity = await daemonCycle(upgraded);
      if (beforeMaturity.state.lastSuccessfulUntil === beforeMaturity.until) break;
    }
    assert.ok(beforeMaturity?.state.caughtUp && beforeMaturity.state.lastSuccessfulUntil === beforeMaturity.until);
    assert.equal(sessionSummaryCounters(upgraded.database).fallbackRecomputes, recomputesBeforeFuture + 1);
    assert.deepEqual(beforeMaturity.sent.find((row) => row.session.id === ids.upgrade),
      expectedWire(upgraded, ids.upgrade, beforeMaturity.until));
    const matureUntil = new Date(Date.parse(futureCreatedAt) + 1_000).toISOString();
    let matureRebuilds = 0;
    let matured: Awaited<ReturnType<typeof updateSessionSummary>> | null = null;
    for (let pass = 0; pass < 3; pass += 1) {
      matured = await updateSessionSummary(upgraded.database, ids.upgrade, matureUntil, {
        read: (queries) => readLedgerOffThread(upgraded.database, queries),
      });
      if (matured.fullRecompute) matureRebuilds += 1;
      if (matured.complete) break;
    }
    assert.equal(matureRebuilds, 1);
    assert.ok(matured?.complete);
    assert.deepEqual(matured.snapshot, collectSessionSnapshots(upgraded.database, {
      until: matureUntil, sessionIds: [ids.upgrade],
    })[0]);
    completion.check("future_row_waits_without_restarts_then_enters_final_horizon_once");
  } finally { upgraded.close(); }
  completion.complete();
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
