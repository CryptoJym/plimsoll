import assert from "node:assert/strict";
import Database from "better-sqlite3";
import {
  ensureMaintenanceStageSchema,
  runDeadlineMaintenanceStages,
  runEnrichmentMaintenanceJob,
  runRetentionDeletionStage,
} from "../packages/collector-cli/src/maintenance-stage-primitives";

function fixture() {
  const database = new Database(":memory:");
  database.exec(`
    create table buffered_events (
      id text primary key, created_at text not null, uploaded_at text,
      session_id text, observed_at text not null, repo_hash text,
      branch_hash text, head_sha text, input_tokens integer,
      output_tokens integer, cost_usd real, payload_json text not null default '{}'
    );
    create index idx_events_observed on buffered_events (observed_at);
    create index idx_events_session on buffered_events (session_id, observed_at);
    create index idx_events_repo_enrichment_seed on buffered_events (id)
      where session_id is not null and (
        repo_hash is not null or input_tokens is not null or
        output_tokens is not null or cost_usd is not null
      );
    create table metric_samples (id text primary key, created_at text not null);
    create index idx_metrics_observed on metric_samples (created_at);
    create table repo_context_results (
      context_id text primary key, repo_hash text not null, branch_hash text,
      head_sha text, resolved_at text not null, resolver_version text not null,
      accepted_at text not null
    );
    create table repo_context_event_links (
      event_id text primary key, context_id text not null,
      fill_pending integer not null default 1, context_conflict integer not null default 0,
      suppression_cleaned integer not null default 0
    );
    create table repo_context_suppressions (
      context_id text primary key, reason text not null, suppressed_at text not null,
      cleanup_complete integer not null default 0
    );
    create table repo_enrichment_dirty (
      session_id text primary key, cursor_rowid integer not null default 0,
      queued_at text not null, updated_at text not null
    );
    create table maintenance_state (key text primary key, value text not null, updated_at text not null);
  `);
  ensureMaintenanceStageSchema(database);
  return database;
}

{
  const database = fixture();
  const plan = database.prepare(
    `explain query plan select id from buffered_events indexed by idx_events_observed
     where observed_at < ? and uploaded_at is not null order by observed_at limit ?`,
  ).all("2026-01-01", 1) as Array<{ detail: string }>;
  assert.match(plan.map((row) => row.detail).join("\n"), /idx_events_observed/);
  const old = "2000-01-01T00:00:00.000Z";
  for (let index = 0; index < 8; index += 1) {
    database.prepare(`insert into buffered_events (id, created_at, uploaded_at, observed_at) values (?, ?, ?, ?)`)
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
  database.close();
}

{
  const database = fixture();
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
  database.close();
}

{
  const database = fixture();
  const skipped = runEnrichmentMaintenanceJob(database, { remainingMs: 4_999 });
  assert.deepEqual({ rows: skipped.rows, skipped: skipped.skipped }, { rows: 0, skipped: true });
  const recent = "2026-09-04T00:00:00.000Z";
  database.prepare(`insert into buffered_events
    (id, created_at, observed_at, session_id, input_tokens)
    values ('candidate', ?, ?, 'session-1', 1)`).run(recent, recent);
  database.prepare(`insert into repo_enrichment_dirty values ('session-1', 0, ?, ?)`)
    .run(recent, recent);
  const ran = runEnrichmentMaintenanceJob(database, { remainingMs: 5_000 });
  assert.equal(ran.rows, 1);
  assert.equal(ran.batchSize, 1);
  database.close();
}

console.log(JSON.stringify({ proof: "maintenance_stage_integration", checks: 3, passed: 3 }));
