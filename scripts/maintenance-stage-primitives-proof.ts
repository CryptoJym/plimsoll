import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import {
  ensureMaintenanceStageSchema,
  readMaintenanceStageCursor,
  runEnrichmentStage,
  runGitContextStage,
  runPendingEventLinkFillStage,
  runRetentionDeletionStage,
  runWalCheckpointStage,
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
    create table metric_samples (id text primary key, created_at text not null);
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
  `);
  ensureMaintenanceStageSchema(database);
  ensureMaintenanceStageSchema(database);
  return database;
}

const old = "2000-01-01T00:00:00.000Z";
const recent = "2099-01-01T00:00:00.000Z";

{
  const database = fixture();
  for (let index = 0; index < 7; index += 1) {
    database.prepare(`insert into buffered_events (id, created_at, uploaded_at, observed_at) values (?, ?, ?, ?)`)
      .run(`old-${index}`, old, old, old);
  }
  const first = runRetentionDeletionStage(database, {
    remainingMs: 1_000, batchSize: 3, retentionDays: 90,
    now: () => Date.parse("2026-09-03T00:00:00.000Z"), parityReady: true,
  });
  assert.equal(first.rows, 3);
  assert.equal((database.prepare(`select count(*) as n from buffered_events`).get() as { n: number }).n, 4);
  assert.equal(readMaintenanceStageCursor(database, "retention_deletion").rowsTotal, 3);
  database.close(); // simulated kill: no in-memory state survives

  const resumed = fixture();
  resumed.close();
}

{
  const database = fixture();
  for (let index = 0; index < 5; index += 1) {
    const contextId = `context-${index}`;
    const eventId = `event-${index}`;
    database.prepare(`insert into buffered_events (id, created_at, observed_at) values (?, ?, ?)`)
      .run(eventId, recent, recent);
    database.prepare(`insert into repo_context_results values (?, 'repo', null, null, ?, 'v1', ?)`)
      .run(contextId, recent, recent);
    database.prepare(`insert into repo_context_event_links values (?, ?, 1, 0, 0)`)
      .run(eventId, contextId);
  }
  const result = runPendingEventLinkFillStage(database, { remainingMs: 1_000, batchSize: 2 });
  assert.equal(result.rows, 2);
  assert.equal((database.prepare(`select count(*) as n from repo_context_event_links where fill_pending = 1`).get() as { n: number }).n, 3);
  assert.equal(readMaintenanceStageCursor(database, "pending_event_link_fill").rowsTotal, 2);
  database.close();
}

{
  const database = fixture();
  database.prepare(`insert into buffered_events (id, created_at, observed_at, session_id, input_tokens) values ('seed', ?, ?, 's1', 1)`).run(recent, recent);
  database.prepare(`insert into repo_enrichment_dirty values ('s1', 0, ?, ?)`).run(recent, recent);
  const result = runEnrichmentStage(database, { remainingMs: 0, batchSize: 4 });
  assert.deepEqual({ rows: result.rows, remaining: result.remaining }, { rows: 0, remaining: 0 });
  assert.equal((database.prepare(`select count(*) as n from repo_enrichment_dirty`).get() as { n: number }).n, 1);
  database.close();
}

{
  const directory = mkdtempSync(join(tmpdir(), "plimsoll-maint-stage-proof-"));
  const path = join(directory, "ledger.sqlite");
  let database = new Database(path);
  ensureMaintenanceStageSchema(database);
  const requests = [
    { contextId: "ctx-a", source: "codex" as const, cwd: "/tmp/a" },
    { contextId: "ctx-b", source: "claude_code" as const, cwd: "/tmp/b" },
  ];
  const queued = runGitContextStage(database, {
    remainingMs: 0, batchSize: 2, requests,
    resolve: () => { throw new Error("zero_budget_resolved"); },
    commit: () => { throw new Error("zero_budget_committed"); },
  });
  assert.equal(queued.rows, 0);
  database.close(); // simulated kill: only ledger state survives
  database = new Database(path);
  assert.deepEqual(readMaintenanceStageCursor(database, "git_context").request, requests[0]);
  const first = runGitContextStage(database, {
    remainingMs: 2, batchSize: 2,
    now: (() => { let value = 0; return () => value++; })(),
    resolve: (request) => ({ ...request, repoHash: "repo", branchHash: null, headSha: null,
      resolvedAt: recent, resolverVersion: "v1" }),
    commit: () => undefined,
  });
  assert.equal(first.rows, 1);
  assert.deepEqual(readMaintenanceStageCursor(database, "git_context").request, requests[1]);
  database.close();
  database = new Database(path);
  assert.deepEqual(readMaintenanceStageCursor(database, "git_context").request, requests[1]);
  database.close();
  rmSync(directory, { recursive: true });
}

{
  const database = fixture();
  const checkpoint = runWalCheckpointStage(database, { remainingMs: 0, batchSize: 1 });
  assert.equal(checkpoint.rows, 0);
  assert.equal(checkpoint.remaining, 0);
  assert.equal(checkpoint.passive, null);
  database.close();
}

console.log(JSON.stringify({ proof: "maintenance_stage_primitives", checks: 5, passed: 5 }));
