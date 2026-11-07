import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { ensureBudgetSchema } from "../packages/collector-cli/src/budget-sampler";
import { pruneDisposableTelemetry } from "../packages/collector-cli/src/disposable-telemetry";
import { AutomaticRetentionCadence, LocalStorageRetention, walPressureDefersRetention } from "../packages/collector-cli/src/retention-cadence";
import { runDeadlineMaintenanceStages } from "../packages/collector-cli/src/maintenance-stage-primitives";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";

const completion = createProofCompletion("storage_retention", 4);
async function main() {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-storage-retention-"));
const file = path.join(root, "fixture.sqlite");
const dayMs = 86_400_000;
// Age the fixture clock, preserving the outbox's immutable raw incarnation.
const now = new Date(Date.now() + 365 * dayMs);
const buffer = new LocalEventBuffer(file, { workspaceId: "fixture", deviceId: "device",
  enrollmentNow: () => new Date("2000-01-01T00:00:00.000Z"), delivery: { enabled: true } });
const db = buffer.database;
const other = new Database(file, { timeout: 0 });
try {
  for (let n = 1; n <= 17; n++) buffer.append(aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`, source: "codex",
    eventType: "assistant_response", dataMode: "metadata", observedAt: "2000-01-01T00:00:00.000Z", inputTokens: 9 }));
  ensureBudgetSchema(db);
  db.pragma("wal_autocheckpoint=0");
  db.pragma("wal_checkpoint(TRUNCATE)");
  other.exec("begin");
  other.prepare("select count(*) from budget_samples").get();
  const insert = db.prepare("insert into budget_samples(at_ms,version,sample_json) values (?,2,'{}')");
  db.transaction(() => {
    for (let n = 0; n < 1_480; n++) insert.run(now.getTime() - 60_000);
    insert.run(now.getTime() - 2 * dayMs);
    insert.run(now.getTime() - dayMs); // cutoff equality stays
    for (const age of [8, 7, 0]) db.prepare("insert into budget_daily(day) values (?)")
      .run(new Date(now.getTime() - age * dayMs).toISOString().slice(0, 10));
  })();
  const held = (db.pragma("wal_checkpoint(PASSIVE)") as Array<{ log: number; checkpointed: number }>)[0];
  assert.ok(held.log > held.checkpointed, "a real reader pins committed frames");
  let pressure = { heldByReader: true, walBytes: fs.statSync(`${file}-wal`).size };
  const queue: { next?: { callback: () => void; delay: number } } = {};
  const runner = new LocalStorageRetention(buffer, () => 90, () => now.getTime());
  const receipts: Array<ReturnType<typeof runner.run>> = [];
  const cadence = new AutomaticRetentionCadence(request => {
    const receipt = runner.run(request); receipts.push(receipt); return receipt;
  }, { canRun: () => !walPressureDefersRetention(pressure, 1), largeBacklog: () => true,
    passRows: { base: 16, max: 16, maxWriterMs: 250 }, timer: { now: () => now.getTime(),
      setTimeout: (callback, delay) => { queue.next = { callback, delay }; return 1; },
      clearTimeout: () => { queue.next = undefined; } } });
  const nextDelay = () => queue.next?.delay;
  const fire = () => { const task = queue.next!; delete queue.next; task.callback(); return nextDelay(); };
  const changes = () => Number(db.prepare("select total_changes()").pluck().get());
  cadence.start();
  const beforeGate = changes(); fire();
  assert.equal(changes(), beforeGate); assert.equal(receipts.length, 0);
  assert.equal(cadence.status().counters.deferred, 1);
  other.exec("rollback");
  assert.equal((db.pragma("wal_checkpoint(TRUNCATE)") as Array<{ busy: number }>)[0].busy, 0);
  assert.equal(fs.statSync(`${file}-wal`).size, 0);
  pressure = { heldByReader: false, walBytes: 0 };
  completion.check("pinned_wal_defers_all_retention_and_releases_to_checkpoint");

  const protectedTables = (db.prepare("select name from sqlite_master where type='table' and name not like 'sqlite_%'")
    .all() as Array<{ name: string }>).map(r => r.name).filter(n => !["budget_samples", "budget_daily", "maintenance_state"].includes(n));
  const evidence = () => Object.fromEntries(protectedTables.map(n => [n, db.prepare(`select * from "${n}"`).all()]));
  const before = evidence(), timeout = db.pragma("busy_timeout", { simple: true });
  other.exec("begin immediate");
  const beforeBusy = changes(), started = performance.now();
  const delays = Array.from({ length: 10 }, fire);
  assert.deepEqual(delays, [200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600, 51_200, 60_000]);
  assert.ok(performance.now() - started < 2_000, "ten lock attempts must yield promptly");
  assert.equal(changes(), beforeBusy); assert.equal(cadence.status().counters.busyDeferrals, 10);
  assert.equal(db.pragma("busy_timeout", { simple: true }), timeout);
  other.exec("rollback"); fire();
  assert.equal(cadence.status().busyStreak, 0);
  assert.equal(receipts.at(-1)?.telemetry?.samplesExpired, 16);
  assert.deepEqual(evidence(), before, "disposable telemetry changes no evidence tables");
  completion.check("locked_database_backs_off_without_writes_then_recovers");

  for (let n = 0; n < 30 && cadence.status().lastPass?.hasMore; n++) fire();
  cadence.stop();
  assert.deepEqual(receipts.filter(r => !r.busy).slice(0, 3).map(r => r.stage), ["telemetry", "raw", "secondary"]);
  assert.ok(receipts.find(r => r.stage === "raw")?.pendingStages?.includes("raw"),
    "secondary must run while the first raw page still has more work");
  assert.equal(cadence.status().lastPass?.hasMore, false);
  assert.equal(cadence.status().counters.telemetrySamplesExpired, 43);
  assert.equal(cadence.status().counters.telemetryDaysExpired, 1);
  assert.equal(cadence.status().counters.migrationProtectedRows, 17);
  assert.ok(receipts.every(r => (r.telemetry?.rowsVisited ?? r.eventRowsVisited + r.metricRowsVisited) <= 16));
  assert.equal(db.prepare("select count(*) from budget_samples").pluck().get(), 1_439);
  assert.equal(db.prepare("select count(*) from budget_samples where at_ms=?").pluck().get(now.getTime() - dayMs), 1);
  assert.equal(db.prepare("select count(*) from budget_daily").pluck().get(), 2);
  assert.equal(db.prepare("select count(*) from buffered_events").pluck().get(), 17);
  assert.equal(db.prepare("select count(*) from upload_outbox").pluck().get(), 17);
  const resting = pruneDisposableTelemetry(db, { maxRows: 16, maxWriterMs: 250, now });
  assert.equal(resting.rowsVisited, 0);
  assert.equal(db.prepare("select count(*) from maintenance_state where key='disposable_telemetry_retention_v1'").pluck().get(), 1);
  completion.check("bounded_fair_sweeps_preserve_pending_usage_and_cutoff_boundaries");

  let childPrunes = 0;
  runDeadlineMaintenanceStages(db, { deadlineMs: 1_000, teardownMarginMs: 0,
    retentionDays: 90, parityReady: true, retentionScheduledExternally: true,
    prune: () => { childPrunes++; throw new Error("child_must_not_prune"); } });
  assert.equal(childPrunes, 0);
  assert.equal(db.prepare("select count(*) from buffered_events").pluck().get(), 17);
  completion.check("maintenance_child_leaves_retention_to_the_wal_guarded_cadence");
  console.log(JSON.stringify({ proof: "storage_retention", pinnedFrames: held.log - held.checkpointed,
    lockDelaysMs: delays, samplesExpired: 43, daysExpired: 1, pendingRawPreserved: 17 }));
  completion.complete();
} finally {
  if (other.inTransaction) other.exec("rollback");
  other.close(); await buffer.close(); fs.rmSync(root, { recursive: true, force: true });
}
}
main().catch(error => { console.error(error); process.exitCode = 1; });
