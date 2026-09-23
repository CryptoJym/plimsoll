#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DashboardProjectionStore } from "../packages/collector-cli/src/dashboard-projection";

const NOW = new Date("2026-09-23T12:00:00.000Z");
const TARGET_TABLES = [
  "dashboard_session_repair_source",
  "dashboard_session_source_window",
] as const;
const TEMP_TABLE = "dashboard_token_event_max";
const TIMED_FACTS = 300_000;
const TIMED_SESSIONS = 5_000;
const MAX_MIGRATION_MS = 30_000;

type Fact = {
  source: string;
  sessionHash: string | null;
  observedAt: string;
  inputTokens: number | null;
};

type SessionRow = {
  source: string;
  sessionHash: string;
  tokenEvents: number;
};

type PlanRow = { detail: string };
type OpenStep = { step: string; durationMs: number; elapsedMs: number };

function initializeLegacyFixture(
  databasePath: string,
  facts: Fact[],
  sessions: SessionRow[],
) {
  const buffer = new LocalEventBuffer(databasePath);
  buffer.close();

  const db = new Database(databasePath);
  db.exec(TARGET_TABLES.map(
    (table) => `alter table ${table} drop column last_token_event_at`,
  ).join(";"));

  const insertFact = db.prepare(
    `insert into dashboard_event_facts
       (projection_id,raw_rowid,source,event_type,observed_at,session_hash,input_tokens)
     values (?,?,?,?,?,?,?)`,
  );
  const insertSession = new Map(TARGET_TABLES.map((table) => [table, db.prepare(
    `insert into ${table}
       (days,session_hash,source,started_at,ended_at,events,token_events,input_tokens,
        output_tokens,cache_read_tokens,cache_creation_tokens,cost_nanos)
     values (30,?,?,?, ?,1,?,0,0,0,0,0)`,
  )]));

  db.transaction(() => {
    facts.forEach((fact, index) => insertFact.run(
      `fact-${index}`,
      index + 1,
      fact.source,
      "assistant_response",
      fact.observedAt,
      fact.sessionHash,
      fact.inputTokens,
    ));
    for (const row of sessions) {
      for (const table of TARGET_TABLES) {
        insertSession.get(table)!.run(
          row.sessionHash,
          row.source,
          "2026-09-01T00:00:00.000Z",
          "2026-09-22T00:00:00.000Z",
          row.tokenEvents,
        );
      }
    }
  })();
  db.pragma("wal_checkpoint(TRUNCATE)");
  db.close();
}

function migrateWithCurrentCode(databasePath: string) {
  const traced: string[] = [];
  const db = new Database(databasePath, { verbose: (sql) => traced.push(String(sql)) });
  new DashboardProjectionStore(db, { now: NOW });
  const rows = readBackfillRows(db);
  db.close();
  return { rows, migrationSql: traced.filter(isBackfillStatement) };
}

function migrateWithLegacySql(databasePath: string) {
  const db = new Database(databasePath);
  for (const table of TARGET_TABLES) {
    db.exec(`alter table ${table} add column last_token_event_at text`);
    db.exec(`update ${table} set last_token_event_at=(
      select max(f.observed_at) from dashboard_event_facts f
      where f.session_hash=${table}.session_hash and f.source=${table}.source
        and f.input_tokens is not null) where token_events>0`);
  }
  const rows = readBackfillRows(db);
  db.close();
  return rows;
}

function readBackfillRows(db: Database.Database) {
  return Object.fromEntries(TARGET_TABLES.map((table) => [table, db.prepare(
    `select days,session_hash as sessionHash,source,token_events as tokenEvents,
       last_token_event_at as lastTokenEventAt
     from ${table} order by days,session_hash,source`,
  ).all()]));
}

function isBackfillStatement(sql: string) {
  const normalized = sql.trim().toLowerCase();
  return normalized.includes(TEMP_TABLE)
    || TARGET_TABLES.some((table) => (
      normalized.startsWith(`alter table ${table}`)
      && normalized.includes("last_token_event_at")
    ) || (
      normalized.startsWith(`update ${table}`)
      && normalized.includes("last_token_event_at")
    ));
}

function replayPlans(databasePath: string, migrationSql: string[]) {
  const db = new Database(databasePath);
  const statements: Array<{ sql: string; plan: PlanRow[] }> = [];
  for (const sql of migrationSql) {
    const plan = /\bselect\b/i.test(sql)
      ? db.prepare(`explain query plan ${sql}`).all() as PlanRow[]
      : [];
    statements.push({ sql, plan });
    db.exec(sql);
  }
  db.close();
  return statements;
}

function semanticFixture(root: string) {
  const facts: Fact[] = [
    { source: "codex", sessionHash: "shared", observedAt: "2026-09-01T01:00:00.000Z", inputTokens: 5 },
    { source: "codex", sessionHash: "shared", observedAt: "2026-09-02T01:00:00.000Z", inputTokens: 8 },
    // A later non-token fact must not move the token watermark.
    { source: "codex", sessionHash: "shared", observedAt: "2026-09-22T01:00:00.000Z", inputTokens: null },
    // The same session in another source must retain its own maximum.
    { source: "claude_code", sessionHash: "shared", observedAt: "2026-09-03T01:00:00.000Z", inputTokens: 13 },
    // A token fact for a token_events=0 row must leave the new column null.
    { source: "codex", sessionHash: "zero-count", observedAt: "2026-09-04T01:00:00.000Z", inputTokens: 21 },
    // Null-session facts never match a materialized session.
    { source: "codex", sessionHash: null, observedAt: "2026-09-23T01:00:00.000Z", inputTokens: 34 },
  ];
  const sessions: SessionRow[] = [
    { source: "codex", sessionHash: "shared", tokenEvents: 2 },
    { source: "claude_code", sessionHash: "shared", tokenEvents: 1 },
    { source: "codex", sessionHash: "zero-count", tokenEvents: 0 },
    // Its retained facts have aged out even though the summary still says it had tokens.
    { source: "codex", sessionHash: "aged-out", tokenEvents: 4 },
    { source: "grok", sessionHash: "no-token-facts", tokenEvents: 0 },
  ];
  const actualPath = path.join(root, "semantic-actual.sqlite");
  const expectedPath = path.join(root, "semantic-expected.sqlite");
  const planPath = path.join(root, "plan.sqlite");
  initializeLegacyFixture(actualPath, facts, sessions);
  initializeLegacyFixture(expectedPath, facts, sessions);
  initializeLegacyFixture(planPath, facts, sessions);

  const expected = migrateWithLegacySql(expectedPath);
  const actual = migrateWithCurrentCode(actualPath);
  assert.deepEqual(actual.rows, expected, "new backfill must exactly match the legacy correlated update");
  return { migrationSql: actual.migrationSql, planPath, rows: actual.rows };
}

function assertBoundedPlan(databasePath: string, migrationSql: string[]) {
  const planned = replayPlans(databasePath, migrationSql);
  const factReaders = planned.filter(({ sql }) => /\bfrom\s+dashboard_event_facts\b/i.test(sql));
  assert.equal(
    factReaders.length,
    1,
    `backfill must read token facts in exactly one statement: ${JSON.stringify(planned)}`,
  );

  const factPlan = factReaders[0]!.plan.map((row) => row.detail);
  assert.ok(
    factPlan.some((detail) => detail.includes("SCAN dashboard_event_facts USING INDEX idx_dashboard_facts_source_token")),
    `token facts must be scanned once through the partial token index: ${JSON.stringify(factPlan)}`,
  );
  assert.ok(
    !planned.some(({ plan }) => plan.some((row) => (
      row.detail.includes("idx_dashboard_facts_source_token (source=?)")
    ))),
    `no target row may search all token facts for one source: ${JSON.stringify(planned)}`,
  );

  const targetUpdates = planned.filter(({ sql }) => TARGET_TABLES.some(
    (table) => sql.trim().toLowerCase().startsWith(`update ${table}`),
  ));
  assert.equal(targetUpdates.length, TARGET_TABLES.length, JSON.stringify(planned));
  for (const { sql, plan } of targetUpdates) {
    assert.ok(
      plan.some((row) => row.detail.includes(TEMP_TABLE)),
      `target update must seek the one-pass aggregate: ${sql} ${JSON.stringify(plan)}`,
    );
    assert.ok(
      plan.every((row) => !row.detail.includes("dashboard_event_facts")),
      `target update must not revisit token facts: ${sql} ${JSON.stringify(plan)}`,
    );
  }
  return planned;
}

function openTimingFixture(root: string) {
  const steps: OpenStep[] = [];
  const databasePath = path.join(root, "timing-events.sqlite");
  const buffer = new LocalEventBuffer(databasePath, {
    onOpenStep: (step) => steps.push(step),
  });
  buffer.close();
  const names = steps.map((step) => step.step);
  for (const expected of [
    "ledger.sqlite_open",
    "ledger.core_schema",
    "ledger.column_migrations",
    "ledger.delivery_schema",
    "ledger.workspace_binding",
    "projection.core_schema_and_indexes",
    "projection.column_migrations",
    "projection.compact_summary_migration",
    "ledger.projection_schema",
  ]) {
    assert.ok(names.includes(expected), `missing ledger-open timing step ${expected}: ${JSON.stringify(names)}`);
  }
  assert.ok(
    steps.every((step) => Number.isFinite(step.durationMs) && step.durationMs >= 0
      && Number.isFinite(step.elapsedMs) && step.elapsedMs >= step.durationMs),
    JSON.stringify(steps),
  );
  assert.deepEqual(
    steps.map((step) => step.elapsedMs),
    steps.map((step) => step.elapsedMs).sort((left, right) => left - right),
    `open-step elapsed times must use one monotonic outer clock: ${JSON.stringify(steps)}`,
  );
  return steps;
}

function timedFixture(root: string) {
  const databasePath = path.join(root, "timed.sqlite");
  const buffer = new LocalEventBuffer(databasePath);
  buffer.close();
  const db = new Database(databasePath);
  db.exec(TARGET_TABLES.map(
    (table) => `alter table ${table} drop column last_token_event_at`,
  ).join(";"));
  db.pragma("synchronous = OFF");

  const insertFact = db.prepare(
    `insert into dashboard_event_facts
       (projection_id,raw_rowid,source,event_type,observed_at,session_hash,input_tokens)
     values (?,?,?,?,?,?,?)`,
  );
  const insertSession = new Map(TARGET_TABLES.map((table) => [table, db.prepare(
    `insert into ${table}
       (days,session_hash,source,started_at,ended_at,events,token_events,input_tokens,
        output_tokens,cache_read_tokens,cache_creation_tokens,cost_nanos)
     values (30,?,'codex','2026-09-01T00:00:00.000Z','2026-09-22T00:00:00.000Z',
       60,60,60,0,0,0,0)`,
  )]));

  db.transaction(() => {
    for (let index = 0; index < TIMED_FACTS; index += 1) {
      const sessionHash = `session-${String(index % TIMED_SESSIONS).padStart(5, "0")}`;
      insertFact.run(
        `timed-fact-${index}`,
        index + 1,
        "codex",
        "assistant_response",
        `2026-09-01T${String(index).padStart(9, "0")}Z`,
        sessionHash,
        1,
      );
    }
    for (let index = 0; index < TIMED_SESSIONS; index += 1) {
      const sessionHash = `session-${String(index).padStart(5, "0")}`;
      for (const table of TARGET_TABLES) insertSession.get(table)!.run(sessionHash);
    }
  })();
  db.pragma("wal_checkpoint(TRUNCATE)");
  db.close();

  const reopened = new Database(databasePath);
  const started = performance.now();
  new DashboardProjectionStore(reopened, { now: NOW });
  const durationMs = performance.now() - started;
  const populated = Object.fromEntries(TARGET_TABLES.map((table) => {
    const row = reopened.prepare(
      `select count(*) as n from ${table} where last_token_event_at is not null`,
    ).get() as { n: number };
    return [table, row.n];
  }));
  reopened.close();

  assert.deepEqual(
    populated,
    Object.fromEntries(TARGET_TABLES.map((table) => [table, TIMED_SESSIONS])),
  );
  assert.ok(
    durationMs < MAX_MIGRATION_MS,
    `300k-fact/5k-session first-open migration took ${durationMs.toFixed(1)}ms (bound ${MAX_MIGRATION_MS}ms)`,
  );
  return { durationMs, populated };
}

function main() {
  const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-token-backfill-")));
  try {
    const semantic = semanticFixture(root);
    const plan = assertBoundedPlan(semantic.planPath, semantic.migrationSql);
    const openTiming = openTimingFixture(root);
    const timed = timedFixture(root);
    console.log(JSON.stringify({
      check: "last_token_event_at_backfill_is_equivalent_and_single_pass",
      sqliteVersion: new Database(":memory:").prepare("select sqlite_version() as version").get(),
      equivalence: semantic.rows,
      plan,
      openTiming,
      timed: {
        facts: TIMED_FACTS,
        sessions: TIMED_SESSIONS,
        boundMs: MAX_MIGRATION_MS,
        durationMs: Number(timed.durationMs.toFixed(1)),
        populated: timed.populated,
      },
    }, null, 2));
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

main();
