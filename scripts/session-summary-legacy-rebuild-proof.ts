/** A 0.7.39-shaped ledger upgrades while intake and session sync keep moving. */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";
import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorConfigSchema } from "../packages/collector-cli/src/config";
import { createSourceRateLimiter } from "../packages/collector-cli/src/http-boundary";
import {
  beginLegacySessionSummaryRebuild, advanceLegacySessionSummaryRebuild,
  buildSessionSyncRow, collectSessionSnapshots, runSessionSync,
} from "../packages/collector-cli/src/session-sync";
import { acceptedFixtureDelivery } from "./lib/delivery-fixture";

const home = process.env.HOME!;
const tmp = process.env.TMPDIR!;
assert.ok(tmp.startsWith(home + path.sep));
const ledger = path.join(tmp, `legacy-summary-${process.pid}.sqlite`);
const tenantId = "00000000-0000-4000-8000-000000000108";
const config = collectorConfigSchema.parse({
  tenantId, uploadUrl: "http://127.0.0.1:1/ingest", installKey: "legacy-rebuild-proof",
  uploadSigningSecret: "legacy-rebuild-proof-secret",
});
const sid = (n: number) => `00000000-0000-4000-8000-${String(100_000 + n).padStart(12, "0")}`;
const ids = Array.from({ length: 12 }, (_, n) => sid(n));
const insertedAt = "2026-09-25T00:00:00.000Z";
let event = 0;

function seed(db: Database.Database) {
  const insert = db.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, observed_at, payload_json, created_at,
     workspace_id, session_id, privacy_generation, input_tokens, output_tokens)
    values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?, ?, ?, ?, 2, 3)`);
  db.transaction(() => {
    for (const [index, sessionId] of ids.entries()) {
      for (let row = 0; row < (index === 0 ? 10_240 : 3); row++) {
        const id = sid(++event + 50_000);
        insert.run(id, insertedAt, insertedAt, tenantId, sessionId, `generation-${id}`);
      }
    }
  })();
  // A 0.7.39 ledger has a durable daemon horizon but no summary tables.
  db.prepare(`insert into maintenance_state (key, value, updated_at) values (?, ?, ?)`)
    .run("session_sync_daemon_v1", JSON.stringify({ schemaVersion: 1, caughtUp: true,
      lastSuccessfulUntil: insertedAt, pendingSessionIds: [], blockedSessionIds: [] }), insertedAt);
  assert.equal((db.prepare(`select count(*) as n from sqlite_master where name='session_sync_summary_state'`)
    .get() as { n: number }).n, 0);
}

const sent = new Map<string, unknown>();
const fetchImpl = (async (_request: RequestInfo | URL, init?: RequestInit) => {
  const body = String(init?.body ?? "");
  for (const row of JSON.parse(body).sessions ?? []) sent.set(row.session.id, row);
  await new Promise((resolve) => setTimeout(resolve, 35));
  return new Response(JSON.stringify(acceptedFixtureDelivery(body, config.installKey)), {
    status: 200, headers: { "content-type": "application/json" },
  });
}) as typeof fetch;

async function main() {
  let buffer = new LocalEventBuffer(ledger, { workspaceId: tenantId, databaseBusyTimeoutMs: 0 });
  let writer: Worker | undefined;
  try {
    seed(buffer.database);
    const initial = beginLegacySessionSummaryRebuild(buffer.database);
    assert.ok(initial && initial.phase === "scan");
    // Probe from another thread: a same-thread timer cannot observe a long
    // synchronous SQLite transaction because the event loop would be blocked.
    writer = new Worker(`
      const { parentPort, workerData } = require('node:worker_threads');
      const Database = require(workerData.sqliteModule);
      const db = new Database(workerData.ledger, { timeout: 0 });
      let attempts = 0, successes = 0, busy = 0, busySince = null, maxBusyMs = 0, stopped = false;
      let maxWriterMs = 0;
      function probe() {
        if (stopped) return;
        const start = performance.now();
        try {
          db.exec('begin immediate; rollback');
          successes++;
          if (busySince !== null) maxBusyMs = Math.max(maxBusyMs, start - busySince);
          busySince = null;
        } catch (error) {
          if (error.code !== 'SQLITE_BUSY') throw error;
          busy++;
          if (busySince === null) busySince = start;
        }
        attempts++;
        maxWriterMs = Math.max(maxWriterMs, performance.now() - start);
        setTimeout(probe, 5);
      }
      parentPort.on('message', () => {
        stopped = true;
        if (busySince !== null) maxBusyMs = Math.max(maxBusyMs, performance.now() - busySince);
        parentPort.postMessage({ attempts, successes, busy, maxBusyMs, maxWriterMs });
        db.close();
      });
      parentPort.postMessage({ ready: true });
      probe();
    `, { eval: true, workerData: { ledger,
      sqliteModule: createRequire(import.meta.url).resolve("better-sqlite3") } });
    await new Promise<void>((resolve, reject) => {
      writer!.once("message", () => resolve()); writer!.once("error", reject);
    });
    const first = await advanceLegacySessionSummaryRebuild(config, buffer.database, {
      maxSessionIds: 2, fetchImpl,
    });
    const stats = await new Promise<{ attempts: number; successes: number; busy: number;
      maxBusyMs: number; maxWriterMs: number }>((resolve, reject) => {
      writer!.once("message", resolve); writer!.once("error", reject);
      writer!.postMessage({ stop: true });
    });
    await writer.terminate(); writer = undefined;
    assert.ok(first.state && first.state.phase === "scan" && first.state.scanned === 2);
    assert.ok(stats.attempts > 0 && stats.successes > 0, "writer must run during rebuild");
    assert.ok(stats.maxBusyMs < 750, `writer was excluded for ${stats.maxBusyMs} ms`);

    // An ordinary session can still sync while the historical cursor is open.
    const live = await runSessionSync(config, { ledgerDb: buffer.database, incremental: true,
      sessionIds: [ids[11]], until: new Date().toISOString(), fetchImpl,
      delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined });
    assert.ok(live.ok && live.summaryComplete && live.acceptedSessions === 1,
      JSON.stringify({ ok: live.ok, complete: live.summaryComplete, accepted: live.acceptedSessions }));

    // Simulate interruption after one durable batch, then finish from the saved cursor.
    buffer.close();
    buffer = new LocalEventBuffer(ledger, { workspaceId: tenantId, databaseBusyTimeoutMs: 0 });
    const resumed = beginLegacySessionSummaryRebuild(buffer.database);
    assert.equal(resumed?.cursor, ids[1]);
    let last = first;
    for (let pass = 0; pass < 70 && last.state?.phase !== "done"; pass++) {
      last = await advanceLegacySessionSummaryRebuild(config, buffer.database, {
        maxSessionIds: 2, fetchImpl,
      });
    }
    assert.equal(last.state?.phase, "done", JSON.stringify(last.state));
    assert.equal(last.state?.scanned, ids.length);
    const final = await runSessionSync(config, { ledgerDb: buffer.database, incremental: true,
      sessionIds: ids, until: new Date().toISOString(), fetchImpl,
      delayMs: 0, maxAttemptsPerBatch: 1, log: () => undefined });
    assert.ok(final.ok && final.summaryComplete && final.acceptedSessions === ids.length);
    const expected = collectSessionSnapshots(buffer.database, { until: final.until, sessionIds: ids })
      .map(buildSessionSyncRow).flatMap((row) => row.ok ? [row.row] : []);
    assert.deepEqual(expected.map((row) => sent.get(row.session.id)), expected,
      "resumed summaries must equal a full recompute");

    const limiter = createSourceRateLimiter();
    for (let request = 0; request < 1_600; request++) limiter.assertAdmissible("codex");
    console.log(JSON.stringify({ result: "PASS", scanned: last.state.scanned,
      writerAttempts: stats.attempts, writerSuccesses: stats.successes,
      writerBusy: stats.busy, maxWriterBusyMs: Math.round(stats.maxBusyMs),
      syncAcceptedDuringRebuild: live.acceptedSessions, finalAccepted: final.acceptedSessions }));
  } finally {
    if (writer) await writer.terminate();
    if (buffer.database.open) buffer.close();
    for (const suffix of ["", "-wal", "-shm"]) fs.rmSync(ledger + suffix, { force: true });
  }
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
