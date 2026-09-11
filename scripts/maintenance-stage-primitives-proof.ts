import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import Database from "better-sqlite3";
import { ensureFinanceProvenanceSchema } from "../packages/collector-cli/src/history-coverage";
import {
  ensureMaintenanceStageSchema,
  readMaintenanceStageCursor,
  runEnrichmentStage,
  runGitContextStage,
  runPendingEventLinkFillStage,
  runRetentionDeletionStage,
  runWalCheckpointStage,
} from "../packages/collector-cli/src/maintenance-stage-primitives";

function fixture(path = ":memory:") {
  const database = new Database(path, { timeout: 0 });
  if (path !== ":memory:") database.pragma("journal_mode = WAL");
  database.exec(`
    create table buffered_events (
      id text primary key, created_at text not null, uploaded_at text,
      source text not null default 'codex', workspace_id text, installation_epoch_id text,
      session_id text, observed_at text not null, repo_hash text,
      branch_hash text, head_sha text, input_tokens integer,
      output_tokens integer, cost_usd real, payload_json text not null default '{}'
    );
    create table metric_samples (id text primary key, created_at text not null);
    create index idx_events_observed on buffered_events (observed_at);
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
    create index idx_repo_context_event_links_pending_context
      on repo_context_event_links (context_id, event_id) where fill_pending = 1;
    create index idx_repo_context_results_gc
      on repo_context_results (accepted_at, context_id);
    create table repo_enrichment_dirty (
      session_id text primary key, cursor_rowid integer not null default 0,
      queued_at text not null, updated_at text not null
    );
  `);
  ensureFinanceProvenanceSchema(database);
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
  const directory = mkdtempSync(join(tmpdir(), "plimsoll-fill-unfillable-proof-"));
  const path = join(directory, "ledger.sqlite");
  const database = fixture(path);
  const insertResult = database.prepare(
    `insert into repo_context_results values (?, 'repo', null, null, ?, 'v1', ?)`,
  );
  const insertLink = database.prepare(
    `insert into repo_context_event_links values (?, ?, 1, 0, 0)`,
  );
  database.transaction(() => {
    for (let index = 0; index < 689; index += 1) {
      const contextId = `result-${index.toString().padStart(4, "0")}`;
      insertResult.run(contextId, recent, recent);
    }
    for (let index = 0; index < 196_000; index += 1) {
      insertLink.run(
        `unfillable-${index.toString().padStart(6, "0")}`,
        `missing-${index.toString().padStart(6, "0")}`,
      );
    }
  })();
  const cursorBefore = database.prepare(
    `select cursor, rows_total as rowsTotal, updated_at as updatedAt
     from maintenance_stage_cursors where stage = 'pending_event_link_fill'`,
  ).get();
  const originalPrepare = database.prepare.bind(database);
  let precheckSql: string | null = null;
  (database as any).prepare = (sql: string) => {
    if (
      sql.includes("from repo_context_results r") &&
      sql.includes("idx_repo_context_event_links_pending_context")
    ) precheckSql = sql;
    return originalPrepare(sql);
  };
  const originalTransaction = database.transaction.bind(database);
  let transactionRuns = 0;
  (database as any).transaction = (fn: (...args: any[]) => unknown) => {
    const transaction = originalTransaction(fn) as any;
    const wrap = (run: (...args: any[]) => unknown) => (...args: any[]) => {
      transactionRuns += 1;
      return run(...args);
    };
    const callable = wrap(transaction) as any;
    callable.deferred = wrap(transaction.deferred);
    callable.immediate = wrap(transaction.immediate);
    callable.exclusive = wrap(transaction.exclusive);
    return callable;
  };
  const startedAt = performance.now();
  const result = runPendingEventLinkFillStage(database, { remainingMs: 1_000, batchSize: 256 });
  const elapsedMs = performance.now() - startedAt;
  (database as any).prepare = originalPrepare;
  (database as any).transaction = originalTransaction;
  assert.equal(result.rows, 0);
  assert.equal(transactionRuns, 0, "an unfillable stage must not enter any transaction");
  assert.deepEqual(
    database.prepare(
      `select cursor, rows_total as rowsTotal, updated_at as updatedAt
       from maintenance_stage_cursors where stage = 'pending_event_link_fill'`,
    ).get(),
    cursorBefore,
    "zero work must not write the cursor",
  );
  assert.ok(precheckSql, "the fill stage must start from bounded results");
  const plan = database.prepare(`explain query plan ${precheckSql}`).all(8) as Array<{ detail: string }>;
  assert.equal(
    plan.some((row) => /\bSCAN (?:main\.)?(?:l|repo_context_event_links)\b/i.test(row.detail)),
    false,
    JSON.stringify(plan),
  );
  database.close();
  rmSync(directory, { recursive: true });
  console.log(JSON.stringify({
    check: "unfillable_196k_uses_result_precheck_without_writer",
    pendingLinks: 196_000,
    retainedResults: 689,
    rows: result.rows,
    transactionRuns,
    elapsedMs: Number(elapsedMs.toFixed(3)),
    plan: plan.map((row) => row.detail),
  }));
}

{
  const database = fixture();
  const insertEvent = database.prepare(
    `insert into buffered_events (id, created_at, observed_at, repo_hash) values (?, ?, ?, ?)`,
  );
  const insertResult = database.prepare(
    `insert into repo_context_results values (?, ?, null, null, ?, 'v1', ?)`,
  );
  const insertLink = database.prepare(
    `insert into repo_context_event_links values (?, ?, 1, ?, 0)`,
  );
  database.transaction(() => {
    for (let contextIndex = 0; contextIndex < 9; contextIndex += 1) {
      const contextId = `context-${contextIndex}`;
      insertResult.run(
        contextId,
        `repo-${contextIndex}`,
        recent,
        `2026-01-${String(contextIndex + 1).padStart(2, "0")}T00:00:00.000Z`,
      );
      for (let rowIndex = 0; rowIndex < 40; rowIndex += 1) {
        const eventId = `${contextIndex === 8 ? "000" : "100"}-${contextIndex}-${String(rowIndex).padStart(3, "0")}`;
        const isDiscoveredConflict = contextIndex === 0 && rowIndex === 0;
        const isExistingConflict = contextIndex === 1 && rowIndex === 0;
        insertEvent.run(eventId, recent, recent, isDiscoveredConflict ? "other-repo" : null);
        insertLink.run(eventId, contextId, isExistingConflict ? 1 : 0);
      }
    }
    insertResult.run("context-suppressed", "repo-suppressed", recent, "2025-01-01T00:00:00.000Z");
    insertEvent.run("suppressed-event", recent, recent, null);
    insertLink.run("suppressed-event", "context-suppressed", 0);
    database.prepare(
      `insert into repo_context_suppressions values ('context-suppressed', 'proof', ?, 0)`,
    ).run(recent);
  })();

  const result = runPendingEventLinkFillStage(database, { remainingMs: 1_000, batchSize: 1_000 });
  assert.equal(result.rows, 256);
  assert.equal(
    (database.prepare(
      `select count(*) as n from repo_context_event_links where context_id = 'context-8' and fill_pending = 0`,
    ).get() as { n: number }).n,
    0,
    "the ninth result context must wait for the next bounded slice",
  );
  assert.deepEqual(
    database.prepare(
      `select l.fill_pending as fillPending, l.context_conflict as contextConflict,
         e.repo_hash as repoHash
       from repo_context_event_links l join buffered_events e on e.id = l.event_id
       where l.event_id = 'suppressed-event'`,
    ).get(),
    { fillPending: 1, contextConflict: 0, repoHash: null },
  );
  assert.deepEqual(
    database.prepare(
      `select fill_pending as fillPending, context_conflict as contextConflict
       from repo_context_event_links where event_id = '100-1-000'`,
    ).get(),
    { fillPending: 1, contextConflict: 1 },
  );
  assert.deepEqual(
    database.prepare(
      `select fill_pending as fillPending, context_conflict as contextConflict
       from repo_context_event_links where event_id = '100-0-000'`,
    ).get(),
    { fillPending: 0, contextConflict: 1 },
  );

  const atomic = fixture();
  for (let index = 0; index < 2; index += 1) {
    atomic.prepare(`insert into buffered_events (id, created_at, observed_at) values (?, ?, ?)`)
      .run(`atomic-${index}`, recent, recent);
    atomic.prepare(`insert into repo_context_results values (?, 'repo', null, null, ?, 'v1', ?)`)
      .run(`atomic-context-${index}`, recent, recent);
    atomic.prepare(`insert into repo_context_event_links values (?, ?, 1, 0, 0)`)
      .run(`atomic-${index}`, `atomic-context-${index}`);
  }
  atomic.exec(`
    create trigger abort_fill before update on buffered_events
    when new.id = 'atomic-1' begin select raise(abort, 'proof_abort_fill'); end;
  `);
  assert.throws(
    () => runPendingEventLinkFillStage(atomic, { remainingMs: 1_000, batchSize: 256 }),
    /proof_abort_fill/,
  );
  assert.deepEqual(
    atomic.prepare(
      `select count(*) as pending,
         sum(case when e.repo_hash is not null then 1 else 0 end) as filled
       from repo_context_event_links l join buffered_events e on e.id = l.event_id`,
    ).get(),
    { pending: 2, filled: 0 },
  );
  atomic.close();
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

console.log(JSON.stringify({ proof: "maintenance_stage_primitives", checks: 7, passed: 7 }));
