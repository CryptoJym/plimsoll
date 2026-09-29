import assert from "node:assert/strict";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { ensureSessionSummarySchema } from "../packages/collector-cli/src/session-summary";
import { buildSessionSyncRow, collectSessionSnapshots, runSessionSync } from
  "../packages/collector-cli/src/session-sync";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("session-summary-v3-upgrade-gate", 5);
const tenantId = "00000000-0000-4000-8000-000000000106";
const installKey = "v3-upgrade-gate-install";
const config = collectorConfigSchema.parse({
  uploadUrl: "http://127.0.0.1:1/ingest", tenantId, installKey,
  uploadSigningSecret: "v3-upgrade-gate-secret",
});
const until = "2026-09-25T00:00:00.000Z";
const observed = "2026-09-20T00:00:00.000Z";
const sessionId = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const eventId = (n: number) => `10000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const large = sessionId(1);
const target = sessionId(2);
const sessions = [
  { id: large, rows: 1_050_000 },
  { id: target, rows: 5_784 },
  ...Array.from({ length: 24 }, (_, n) => ({ id: sessionId(n + 3), rows: 6_100 })),
];

function v3Accumulator(id: string, rows: number, highWater: number, cursorId: string,
  scanBoundary: number) {
  return {
    sessionId: id, scanBoundary, cursorObservedAt: observed, cursorRowid: highWater,
    cursorId, futureRows: false, futureCreatedAt: null,
    sourceMax: "codex", startedAt: observed, endedAt: observed,
    events: rows, inputTokens: rows * 2, outputTokens: rows * 3,
    cacheReadTokens: 0, cacheCreationTokens: 0, pricedEvents: rows,
    costUsd: rows * 0.25, costCompensation: 0,
    repoNonNull: 0, repoValue: null, repoMixed: false,
    branchNonNull: 0, branchValue: null, branchMixed: false,
    accountNonNull: 0, accountValue: null, accountMixed: false,
  };
}

async function main() {
  const buffer = new LocalEventBuffer(
    path.join(process.env.PLIMSOLL_PROOF_ROOT!, "v3-upgrade-gate.sqlite"),
    { workspaceId: tenantId },
  );
  try {
    const db = buffer.database;
    const insert = db.prepare(`with recursive seq(x) as (
      select @first union all select x + 1 from seq where x < @last
    ) insert into buffered_events
      (id, source, event_type, data_mode, observed_at, payload_json,
       suppressed_fields_json, created_at, session_id, input_tokens, output_tokens,
       cost_usd, workspace_id, privacy_generation)
      select printf('10000000-0000-4000-8000-%012d', x), 'codex',
        'assistant_response', 'metadata', @observed, '{}', '[]', @observed,
        @sessionId, 2, 3, 0.25, @tenantId, printf('generation-%d', x)
      from seq`);
    let firstEvent = 1;
    const checkpoints: Array<{ id: string; rows: number; highWater: number; cursorId: string }> = [];
    for (const session of sessions) {
      for (let start = 0; start < session.rows; start += 100_000) {
        const first = firstEvent + start;
        insert.run({ first, last: first + Math.min(100_000, session.rows - start) - 1,
          sessionId: session.id, observed, tenantId });
      }
      const highWater = firstEvent + session.rows - 1;
      checkpoints.push({ ...session, highWater, cursorId: eventId(highWater) });
      firstEvent += session.rows;
    }
    const totalRows = firstEvent - 1;
    assert.equal(totalRows, 1_202_184);
    assert.equal((db.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n,
      totalRows);
    completion.check("studio_shaped_multi_session_ledger_seeded");

    // An old, complete v3 state is installed after the source rows. The
    // production upgrade path adds the v4 columns and then revisits each ID.
    db.exec(`create table session_sync_summary_state (
      session_id text primary key, schema_version integer not null,
      high_water integer not null, checkpoint_id text, covered_until text not null,
      complete integer not null, mutation_revision integer not null,
      mode text not null, accumulator_json text not null, updated_at text not null
    )`);
    const stateInsert = db.prepare(`insert into session_sync_summary_state
      (session_id, schema_version, high_water, checkpoint_id, covered_until,
       complete, mutation_revision, mode, accumulator_json, updated_at)
      values (?, 3, ?, ?, ?, 1, 0, 'incremental', ?, ?)`);
    db.transaction(() => {
      for (const checkpoint of checkpoints) {
        stateInsert.run(checkpoint.id, checkpoint.highWater, checkpoint.cursorId, until,
          JSON.stringify(v3Accumulator(checkpoint.id, checkpoint.rows,
            checkpoint.highWater, checkpoint.cursorId, totalRows)), observed);
      }
    }).immediate();
    const upgradeStarted = performance.now();
    ensureSessionSummarySchema(db);
    const migrationMs = performance.now() - upgradeStarted;
    // A v3 completion can have an append already queued when the new binary
    // first revisits it. Conversion must keep its old totals and drain that
    // append without publishing a partial aggregate or rebuilding the prefix.
    const queuedSession = sessions.at(-1)!.id;
    insert.run({ first: totalRows + 1, last: totalRows + 1,
      sessionId: queuedSession, observed, tenantId });
    assert.ok(db.prepare(`select 1 from session_sync_summary_rows
      where session_id = ? and raw_rowid = ?`).get(queuedSession, totalRows + 1));
    const pending = new Set(checkpoints.map((checkpoint) => checkpoint.id));
    let targetSentMs: number | null = null;
    let largestCompleteMs: number | null = null;
    let targetWire: unknown = null;
    let largestWire: unknown = null;
    let queuedWire: unknown = null;
    let totalRowsRead = 0;
    let totalRecomputes = 0;
    let cycles = 0;
    const maxCycles = 300;
    while ((targetSentMs === null || largestCompleteMs === null || pending.size > 0) &&
           cycles < maxCycles) {
      cycles += 1;
      const sent: Array<{ session: { id: string }; totals: { events: number } }> = [];
      const fetchImpl = (async (_request: RequestInfo | URL, init?: RequestInit) => {
        const body = String(init?.body ?? "");
        sent.push(...(JSON.parse(body).sessions ?? []));
        return new Response(JSON.stringify({
          ...acceptedFixtureDelivery(body, installKey),
          inserted: sent.length, updated: 0, skippedStale: 0,
        }), { status: 200, headers: { "content-type": "application/json" } });
      }) as typeof fetch;
      const result = await runSessionSync(config, {
        sessionIds: [...pending], until, ledgerDb: db, incremental: true,
        fetchImpl, sleep: async () => undefined, delayMs: 0, maxAttemptsPerBatch: 1,
        log: () => undefined,
      });
      assert.equal(result.ok, true, `cycle ${cycles}: ${result.reason}`);
      totalRowsRead += result.summaryStats.rowsRead;
      totalRecomputes += result.summaryStats.fullRecomputes;
      const elapsed = performance.now() - upgradeStarted;
      for (const row of sent) {
        pending.delete(row.session.id);
        if (row.session.id === target && targetSentMs === null) {
          targetSentMs = elapsed;
          targetWire = row;
        }
        if (row.session.id === large && largestCompleteMs === null) {
          largestCompleteMs = elapsed;
          largestWire = row;
        }
        if (row.session.id === queuedSession) queuedWire = row;
      }
      if (cycles % 25 === 0 || targetSentMs !== null && cycles === 2) {
        console.log(JSON.stringify({ progressCycle: cycles, elapsedMs: Math.round(elapsed),
          pending: pending.size, totalRowsRead }));
      }
    }
    const metrics = { totalRows, sessions: sessions.length, largestRows: sessions[0]!.rows,
      targetRows: sessions[1]!.rows, migrationMs, cycles, totalRowsRead, totalRecomputes,
      targetSentMs, largestCompleteMs, pendingSessions: pending.size };
    console.log(JSON.stringify(metrics));
    assert.ok(targetSentMs !== null && targetSentMs <= 300_000,
      `v3 upgrade target missed 300 s: ${targetSentMs}`);
    completion.check("target_converged_during_upgrade_backlog_under_300_seconds");
    assert.ok(largestCompleteMs !== null, "1M+ state did not rebuild within 300 cycles");
    const targetExpected = buildSessionSyncRow(collectSessionSnapshots(db,
      { sessionIds: [target], until })[0]!);
    const largeExpected = buildSessionSyncRow(collectSessionSnapshots(db,
      { sessionIds: [large], until })[0]!);
    assert.equal(targetExpected.ok, true);
    assert.equal(largeExpected.ok, true);
    assert.deepEqual(targetWire, targetExpected.row);
    assert.deepEqual(largestWire, largeExpected.row);
    completion.check("upgraded_wire_snapshots_equal_scratch_aggregate");
    const queuedExpected = buildSessionSyncRow(collectSessionSnapshots(db,
      { sessionIds: [queuedSession], until })[0]!);
    assert.equal(queuedExpected.ok, true);
    assert.deepEqual(queuedWire, queuedExpected.row);
    assert.equal(totalRecomputes, 0);
    completion.check("queued_v3_append_drains_without_rebuilding_prefix");

    const converted = db.prepare(`select schema_version as version, accumulator_json as accumulator
      from session_sync_summary_state where session_id = ?`).get(target) as {
      version: number; accumulator: string;
    };
    assert.equal(converted.version, 4);
    assert.equal(JSON.parse(converted.accumulator).legacyHighWater, checkpoints[1]!.highWater);
    db.prepare(`update buffered_events set output_tokens = 10 where id = ?`)
      .run(eventId(sessions[0]!.rows + 1));
    let repairRecomputes = 0;
    let repairRowsRead = 0;
    let repairedWire: unknown = null;
    for (let pass = 0; pass < 120 && repairedWire === null; pass += 1) {
      const retry = db.prepare(`select next_retry_at as nextRetryAt
        from session_sync_summary_pending where session_id = ?`).get(target) as {
          nextRetryAt: string | null;
        } | undefined;
      if (retry?.nextRetryAt) {
        const delayMs = Math.max(0, Date.parse(retry.nextRetryAt) - Date.now() + 1);
        if (delayMs > 0) await new Promise<void>((resolve) => setTimeout(resolve, delayMs));
      }
      const sent: unknown[] = [];
      const fetchImpl = (async (_request: RequestInfo | URL, init?: RequestInit) => {
        const body = String(init?.body ?? "");
        sent.push(...(JSON.parse(body).sessions ?? []));
        return new Response(JSON.stringify(acceptedFixtureDelivery(body, installKey)),
          { status: 200, headers: { "content-type": "application/json" } });
      }) as typeof fetch;
      const result = await runSessionSync(config, {
        sessionIds: [target], until, ledgerDb: db, incremental: true,
        fetchImpl, sleep: async () => undefined, delayMs: 0, maxAttemptsPerBatch: 1,
        log: () => undefined,
      });
      assert.equal(result.ok, true);
      repairRowsRead += result.summaryStats.rowsRead;
      repairRecomputes += result.summaryStats.fullRecomputes;
      repairedWire = sent[0] ?? null;
      if (pass < 5 || pass % 5 === 4) console.log(JSON.stringify({ repairPass: pass + 1,
        rowsRead: result.summaryStats.rowsRead, fullRecomputes: result.summaryStats.fullRecomputes,
        pending: result.pendingSummaryReasons, sent: sent.length }));
    }
    const repairedExpected = buildSessionSyncRow(collectSessionSnapshots(db,
      { sessionIds: [target], until })[0]!);
    assert.equal(repairedExpected.ok, true);
    assert.deepEqual(repairedWire, repairedExpected.row);
    assert.equal(repairRecomputes, 1, "first historical repair replaces the legacy prefix");
    console.log(JSON.stringify({ repairRowsRead, repairRecomputes,
      repairedOutputTokens: repairedExpected.row?.totals.outputTokens }));
    completion.check("first_legacy_prefix_repair_rebuilds_once_and_sends_exact_wire");
    completion.complete();
  } finally { buffer.close(); }
}

main().catch(error => { console.error(error); process.exitCode = 1; });
