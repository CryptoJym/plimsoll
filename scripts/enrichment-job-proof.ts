import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import path from "node:path";
import Database from "better-sqlite3";
import { runRepoEnrichmentMaintenance } from "../packages/collector-cli/src/maintenance";
import {
  EnrichmentProcessBoundary,
  IdleEnrichmentScheduler,
  lowerEnrichmentProcessPriority,
  type EnrichmentBoundaryChild,
} from "../packages/collector-cli/src/enrichment-job";

function fixture() {
  const database = new Database(":memory:");
  database.exec(`
    create table buffered_events (
      id text primary key, created_at text not null, uploaded_at text,
      session_id text, observed_at text not null, repo_hash text,
      branch_hash text, head_sha text, input_tokens integer,
      output_tokens integer, cost_usd real, payload_json text not null default '{}'
    );
    create index idx_events_session on buffered_events (session_id, observed_at);
    create index idx_events_repo_enrichment_seed on buffered_events (id)
      where session_id is not null and (
        repo_hash is not null or input_tokens is not null or
        output_tokens is not null or cost_usd is not null
      );
    create table repo_context_event_links (
      event_id text primary key, context_id text not null,
      fill_pending integer not null default 1, context_conflict integer not null default 0,
      suppression_cleaned integer not null default 0
    );
    create table repo_enrichment_dirty (
      session_id text primary key, cursor_rowid integer not null default 0,
      queued_at text not null, updated_at text not null
    );
    create table maintenance_state (key text primary key, value text not null, updated_at text not null);
  `);
  return database;
}

function oldOneRow(database: Database.Database, sessionId: string) {
  return database.prepare(`
    select e.id,
      (select r.repo_hash from buffered_events r
       where r.session_id = e.session_id and r.repo_hash is not null
         and not exists (select 1 from repo_context_event_links l where l.event_id = r.id)
         and r.observed_at <= e.observed_at
       order by r.observed_at desc, r.rowid desc limit 1) as repoHash
    from buffered_events e
    where e.session_id = ? and e.repo_hash is null
      and not exists (select 1 from repo_context_event_links l where l.event_id = e.id)
      and (e.input_tokens is not null or e.output_tokens is not null or e.cost_usd is not null)
    order by e.rowid limit 1
  `).get(sessionId);
}

async function main() {
const database = fixture();
const insert = database.prepare(`insert into buffered_events
  (id, created_at, session_id, observed_at, repo_hash, branch_hash, input_tokens)
  values (?, ?, 'large-session', ?, ?, ?, ?)`);
const started = Date.parse("2026-01-01T00:00:00.000Z");
database.transaction(() => {
  for (let index = 0; index < 100_000; index += 1) {
    const at = new Date(started + index).toISOString();
    insert.run(
      `event-${String(index).padStart(6, "0")}`,
      at,
      at,
      index === 49_999 ? "repo-a" : null,
      index === 49_999 ? "branch-a" : null,
      index === 50_000 ? 1 : null,
    );
  }
})();
database.prepare(`insert into repo_enrichment_dirty values ('large-session', 0, ?, ?)`).run(
  new Date(started).toISOString(), new Date(started).toISOString(),
);
const oldStarted = performance.now();
oldOneRow(database, "large-session");
const oldMs = performance.now() - oldStarted;
const boundedStarted = performance.now();
const bounded = runRepoEnrichmentMaintenance(database, {
  skipLegacyBackfill: true, sessionLimit: 1, eventLimit: 1, neighborLimit: 50,
});
const boundedMs = performance.now() - boundedStarted;
assert.equal(bounded.rowsVisited, 1);
assert.equal(bounded.backward, 1);
assert.equal(
  (database.prepare(`select repo_hash from buffered_events where id = 'event-050000'`).get() as { repo_hash: string }).repo_hash,
  "repo-a",
);
assert.ok(boundedMs < 250, `bounded one-row enrichment took ${boundedMs.toFixed(3)} ms`);
database.close();

class FakeChild extends EventEmitter implements EnrichmentBoundaryChild {
  pid = 24_001;
  connected = true;
  signals: NodeJS.Signals[] = [];
  send(raw: unknown, callback?: (error: Error | null) => void) {
    const request = raw as { type?: string; generation?: number; nonce?: string };
    callback?.(null);
    if (request.type === "run") queueMicrotask(() => this.emit("message", {
      schema: 1, type: "enrichment_job_progress", generation: request.generation,
      nonce: request.nonce, sequence: 1, rows: 1, ms: 1, remaining: 10,
    }));
    return true;
  }
  kill(signal: NodeJS.Signals) {
    this.signals.push(signal);
    if (signal === "SIGKILL") queueMicrotask(() => {
      this.connected = false;
      this.emit("close", null, signal);
    });
    return true;
  }
}

let timeoutChild!: FakeChild;
const boundary = new EnrichmentProcessBoundary({
  entryPath: "unused", deadlineMs: 20, readyDeadlineMs: 20,
  termGraceMs: 5, killGraceMs: 30,
  verifyChild: async () => true,
  spawnChild: (spawnNonce) => {
    timeoutChild = new FakeChild();
    queueMicrotask(() => timeoutChild.emit("message", { schema: 1, type: "ready", spawnNonce }));
    return timeoutChild;
  },
});
const partial = await boundary.run({ acceptPartial: true });
assert.equal(partial.outcome, "PARTIAL_OK");
assert.deepEqual(timeoutChild.signals, ["SIGTERM", "SIGKILL"]);
assert.equal(boundary.status().childPresent, false);
assert.equal(boundary.status().orphanRisk, false);
assert.equal(boundary.status().reapedChildren, 1);

const osBoundary = new EnrichmentProcessBoundary({
  entryPath: path.resolve("scripts/fixtures/enrichment-timeout-child.mjs"),
  execArgv: [], env: process.env,
  deadlineMs: 1_200, readyDeadlineMs: 1_000,
  termGraceMs: 20, killGraceMs: 500,
});
const osPartial = await osBoundary.run({ acceptPartial: true });
assert.equal(osPartial.outcome, "PARTIAL_OK");
assert.deepEqual(
  { childPresent: osBoundary.status().childPresent, orphanRisk: osBoundary.status().orphanRisk },
  { childPresent: false, orphanRisk: false },
);
assert.deepEqual(
  { term: osBoundary.status().termSignals, kill: osBoundary.status().killSignals },
  { term: 1, kill: 1 },
);

let mainIdle = false;
let scheduledRuns = 0;
const idleScheduler = new IdleEnrichmentScheduler(
  () => mainIdle,
  async () => { scheduledRuns += 1; return { outcome: "completed" as const, rows: 1, ms: 2 }; },
);
assert.equal((await idleScheduler.trigger()).outcome, "skipped_main_busy");
mainIdle = true;
assert.equal((await idleScheduler.trigger()).outcome, "completed");
assert.equal(scheduledRuns, 1);
let priorityCall: [number, number] | null = null;
assert.equal(lowerEnrichmentProcessPriority((pid, priority) => {
  priorityCall = [pid, priority];
}), true);
assert.deepEqual(priorityCall, [0, 15]);

console.log(JSON.stringify({
  proof: "enrichment_job", checks: 5, passed: 5,
  fixtureEvents: 100_000,
  oldMs: Number(oldMs.toFixed(3)), boundedMs: Number(boundedMs.toFixed(3)),
  timeoutSignals: timeoutChild.signals,
  childPresent: boundary.status().childPresent,
  orphanRisk: boundary.status().orphanRisk,
  osTimeoutSignals: { term: osBoundary.status().termSignals, kill: osBoundary.status().killSignals },
}));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
