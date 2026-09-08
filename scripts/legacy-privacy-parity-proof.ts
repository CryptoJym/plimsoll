#!/usr/bin/env node

/** Legacy generation migration must drain repairs before parity admission. */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import os from "node:os";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { DASHBOARD_WINDOWS } from "../packages/collector-cli/src/dashboard-projection";
import { LOCAL_TENANT_ID } from "../packages/shared/src/index";

const DAY_MS = 24 * 60 * 60 * 1_000;
const LEGACY_ROWS = 1_200;
// Keep the injected maintenance clock just ahead of constructor time so the
// fixture exercises parity rather than the independent clock-rollback guard.
const NOW = new Date(Date.now() + 60 * 1_000);
const OBSERVED_AT = new Date(NOW.getTime() - 60 * DAY_MS).toISOString();
const workDir = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-legacy-privacy-"));
const dbPath = path.join(workDir, "legacy.sqlite");

type Counters = {
  events: number;
  tokenEvents: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  costNanos: number;
};

type Phase = {
  label: string;
  backfill: number;
  parity: number;
  repairs: number;
  dirtySessions: number;
  parityCursor: number;
  parityComplete: boolean;
  ready: boolean;
  degradedReason: string | null;
  deltas: Record<string, number>;
};

function zeroCounters(): Counters {
  return {
    events: 0,
    tokenEvents: 0,
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheCreationTokens: 0,
    costNanos: 0,
  };
}

function readCounters(db: Database.Database, table: string, days: number): Counters {
  return db.prepare(
    `select events,token_events as tokenEvents,input_tokens as inputTokens,
       output_tokens as outputTokens,cache_read_tokens as cacheReadTokens,
       cache_creation_tokens as cacheCreationTokens,cost_nanos as costNanos
     from ${table} where days=?`,
  ).get(days) as Counters;
}

function deltas(db: Database.Database, days = 90) {
  const total = readCounters(db, "dashboard_window_totals", days);
  const parity = readCounters(db, "dashboard_parity_window", days);
  const post = readCounters(db, "dashboard_post_highwater_window", days);
  return Object.fromEntries(
    (Object.keys(total) as Array<keyof Counters>).map((key) => [
      key,
      total[key] - parity[key] - post[key],
    ]),
  );
}

function phase(buffer: LocalEventBuffer, label: string, backfill = 0, parity = 0): Phase {
  const status = buffer.projection.status();
  return {
    label,
    backfill,
    parity,
    repairs: status.backlog.repairs,
    dirtySessions: status.backlog.dirtySessions,
    parityCursor: status.backfill.parityCursor,
    parityComplete: status.backfill.parityComplete,
    ready: status.ready,
    degradedReason: status.degradedReason,
    deltas: deltas(buffer.database),
  };
}

function eventPayload(id: string) {
  return JSON.stringify({
    id,
    tenantId: LOCAL_TENANT_ID,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: OBSERVED_AT,
    actionClass: "other",
    model: "fixture-model",
    inputTokens: 2,
    outputTokens: 3,
    cacheReadTokens: 5,
    cacheCreationTokens: 1,
    costUsd: 0.000001,
    metadata: {},
  });
}

function allZero(value: Record<string, number>) {
  return Object.values(value).every((entry) => entry === 0);
}

const phases: Phase[] = [];
let buffer: LocalEventBuffer | null = null;

try {
  // Deliberately old schema: LocalEventBuffer adds columns and triggers on
  // reopen, so these historical rows retain NULL privacy_generation.
  const oldDb = new Database(dbPath);
  oldDb.exec(`
    create table buffered_events (
      id text primary key,
      source text not null,
      event_type text not null,
      data_mode text not null,
      observed_at text not null,
      payload_json text not null,
      suppressed_fields_json text not null default '[]',
      created_at text not null,
      session_id text,
      action_class text,
      model text,
      input_tokens integer,
      output_tokens integer,
      cache_read_tokens integer,
      cache_creation_tokens integer,
      cost_usd real,
      uploaded_at text,
      repo_hash text,
      branch_hash text,
      head_sha text,
      machine text,
      account_hash text
    );
  `);
  const insert = oldDb.prepare(
    `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,
        session_id,action_class,model,input_tokens,output_tokens,cache_read_tokens,
        cache_creation_tokens,cost_usd,uploaded_at,repo_hash,branch_hash,head_sha,machine,account_hash)
     values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
  );
  oldDb.transaction(() => {
    for (let index = 0; index < LEGACY_ROWS; index += 1) {
      // RFC-shaped ids keep the fixture on the normal upload path; no payload
      // or identity is printed by this script.
      const id = `00000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`;
      insert.run(
        id,
        "codex",
        "assistant_response",
        "metadata",
        OBSERVED_AT,
        eventPayload(id),
        "[]",
        NOW.toISOString(),
        null,
        "other",
        "fixture-model",
        2,
        3,
        5,
        1,
        0.000001,
        null,
        null,
        null,
        null,
        "fixture-machine",
        null,
      );
    }
  })();
  oldDb.close();

  buffer = new LocalEventBuffer(dbPath, {
    delivery: {
      enabled: true,
      now: () => NOW,
    },
  });

  const first = buffer.projection.runMaintenance(NOW);
  phases.push(phase(buffer, "legacy-backfill-first", first.backfillRowsVisited, first.parityRowsVisited));
  const second = buffer.projection.runMaintenance(NOW);
  phases.push(phase(buffer, "legacy-backfill-complete", second.backfillRowsVisited, second.parityRowsVisited));

  const beforeMigration = buffer.database.prepare(
    `select count(*) as n from buffered_events where privacy_generation is null`,
  ).get() as { n: number };
  assert.equal(beforeMigration.n, LEGACY_ROWS, "old rows must remain ineligible before migration");
  assert.equal(deltas(buffer.database)["events"], 0, "skipped legacy rows do not enter flat totals");

  const migration = buffer.delivery.migrateLegacy({
    maxRows: 5_000,
    maxBytes: 10_000_000,
    now: NOW,
  });
  const afterMigration = buffer.database.prepare(
    `select count(*) as n from buffered_events where privacy_generation is null`,
  ).get() as { n: number };
  const repairCount = buffer.database.prepare(
    `select count(*) as n from dashboard_projection_repairs`,
  ).get() as { n: number };
  assert.equal(migration.visited, LEGACY_ROWS, "migration must visit the bounded fixture");
  assert.equal(migration.complete, true, "migration should complete in one bounded slice");
  assert.equal(afterMigration.n, 0, "migration assigns every legacy generation");
  assert.equal(repairCount.n, LEGACY_ROWS, "generation updates must enqueue projection repairs");

  const firstRepair = buffer.projection.runMaintenance(NOW);
  const racePhase = phase(buffer, "post-migration-repairs", firstRepair.backfillRowsVisited, firstRepair.parityRowsVisited);
  phases.push(racePhase);
  assert.equal(racePhase.parity, 0, "parity must wait while newly eligible rows are repaired");
  assert.ok(racePhase.repairs > 0, "fixture exceeds one repair slice");

  let repairsDrained = false;
  for (let index = 0; index < 40; index += 1) {
    const receipt = buffer.projection.runMaintenance(NOW);
    const current = phase(buffer, `settle-${index + 1}`, receipt.backfillRowsVisited, receipt.parityRowsVisited);
    phases.push(current);
    if (receipt.repairRowsVisited > 0 || current.repairs > 0) {
      assert.equal(receipt.parityRowsVisited, 0, "parity must wait through the last repair slice");
    }
    if (current.repairs === 0) repairsDrained = true;
  }
  assert.equal(repairsDrained, true, "bounded repair queue must drain in the fixture");
  const finalPhase = phases.at(-1)!;
  const stableTail = phases.slice(-3);
  assert.equal(finalPhase.parityCursor, LEGACY_ROWS, "parity must reach the captured highwater");
  assert.equal(finalPhase.parityComplete, true, "parity completes naturally after repairs");
  assert.equal(finalPhase.ready, true, "settled projection publishes a ready snapshot");
  for (const days of DASHBOARD_WINDOWS) {
    assert.ok(allZero(deltas(buffer.database, days)), `window ${days} conserves every counter`);
  }
  assert.ok(stableTail.every((entry) => allZero(entry.deltas)), "clean passes preserve parity");
  const rawCount = buffer.database.prepare("select count(*) as n from buffered_events").get() as {n:number};
  assert.equal(rawCount.n, LEGACY_ROWS, "reconciliation preserves every raw row");

  const totals30 = readCounters(buffer.database, "dashboard_window_totals", 30);
  const reference30 = readCounters(buffer.database, "dashboard_parity_window", 30);
  const post30 = readCounters(buffer.database, "dashboard_post_highwater_window", 30);
  assert.deepEqual(totals30, zeroCounters(), "fixture is outside the 30-day window");
  assert.deepEqual(reference30, zeroCounters(), "30-day parity remains empty");
  assert.deepEqual(post30, zeroCounters(), "30-day post-highwater remains empty");

  console.log(JSON.stringify({
    schema: "plimsoll.legacy-privacy-parity-fixture.v1",
    status: "PASS",
    rows: LEGACY_ROWS,
    observedWindowDays: 60,
    migration: {
      visited: migration.visited,
      complete: migration.complete,
      enqueued: migration.enqueued,
      repairsAfterMigration: repairCount.n,
    },
    race: {
      deltas: racePhase.deltas,
      repairsRemaining: racePhase.repairs,
      parityCursor: racePhase.parityCursor,
      degradedReason: racePhase.degradedReason,
    },
    settledObservation: {
      passes: phases.filter((entry) => entry.label.startsWith("settle-")).length,
      final: finalPhase,
      stableTail,
      repairsDrained,
      converged: true,
    },
    windowsChecked: DASHBOARD_WINDOWS,
    liveStateTouched: false,
    providerNetworkCalled: false,
  }, null, 2));
} finally {
  buffer?.close();
  fs.rmSync(workDir, { recursive: true, force: true });
}
