import assert from "node:assert/strict";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  runDeadlineMaintenanceStages,
  runEnrichmentMaintenanceJob,
  runRetentionDeletionStage,
} from "../packages/collector-cli/src/maintenance-stage-primitives";

function fixture() {
  // Keep this integration fixture on the production ledger schema, indexes, and triggers.
  return new LocalEventBuffer(":memory:", { databaseBusyTimeoutMs: 0 });
}

{
  const buffer = fixture();
  const database = buffer.database;
  const plan = database.prepare(
    `explain query plan select id from buffered_events indexed by idx_events_observed
     where observed_at < ? and uploaded_at is not null order by observed_at limit ?`,
  ).all("2026-01-01", 1) as Array<{ detail: string }>;
  assert.match(plan.map((row) => row.detail).join("\n"), /idx_events_observed/);
  const old = "2000-01-01T00:00:00.000Z";
  for (let index = 0; index < 8; index += 1) {
    database.prepare(`insert into buffered_events
      (id, source, event_type, data_mode, payload_json, created_at, uploaded_at, observed_at)
      values (?, 'codex', 'assistant_response', 'full', '{}', ?, ?, ?)`)
      .run(`old-${index}`, old, old, old);
  }
  let tick = 0;
  const first = runRetentionDeletionStage(database, {
    remainingMs: 10_000, batchSize: 8, retentionDays: 90, parityReady: true,
    now: () => tick++ === 0 ? 0 : 8_000,
    wallNow: () => Date.parse("2026-09-04T00:00:00.000Z"),
  });
  assert.equal(first.rows, 8);
  const second = runRetentionDeletionStage(database, {
    remainingMs: 10_000, batchSize: 8, retentionDays: 90, parityReady: true,
    now: (() => { let value = 0; return () => value++; })(),
    wallNow: () => Date.parse("2026-09-04T00:00:00.000Z"),
  });
  assert.equal(second.batchSize, 2, "8 rows in 8s adapts the next slice below 3s");
  buffer.close();
}

{
  const buffer = fixture();
  const database = buffer.database;
  const stages: string[] = [];
  let now = 0;
  const result = runDeadlineMaintenanceStages(database, {
    deadlineMs: 30_000,
    teardownMarginMs: 1_000,
    retentionDays: 90,
    parityReady: true,
    now: () => now++,
    onDurableCommit: (progress) => { stages.push(progress.stage); return true; },
  });
  assert.deepEqual(stages, ["wal_checkpoint", "retention", "fill_pending_event_links"]);
  assert.ok(result.remainingMs <= 29_000 && result.remainingMs >= 0);
  buffer.close();
}

{
  const buffer = fixture();
  const database = buffer.database;
  const skipped = runEnrichmentMaintenanceJob(database, { remainingMs: 4_999 });
  assert.deepEqual({ rows: skipped.rows, skipped: skipped.skipped }, { rows: 0, skipped: true });
  const recent = "2026-09-04T00:00:00.000Z";
  database.prepare(`insert into buffered_events
    (id, source, event_type, data_mode, payload_json, created_at, observed_at, session_id, input_tokens)
    values ('candidate', 'codex', 'assistant_response', 'full', '{}', ?, ?, 'session-1', 1)`).run(recent, recent);
  const ran = runEnrichmentMaintenanceJob(database, { remainingMs: 5_000 });
  assert.equal(ran.rows, 1);
  assert.equal(ran.batchSize, 1);
  buffer.close();
}

console.log(JSON.stringify({ proof: "maintenance_stage_integration", checks: 3, passed: 3 }));
