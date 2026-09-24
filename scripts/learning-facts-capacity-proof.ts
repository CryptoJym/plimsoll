#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";

import Database from "better-sqlite3";

import {
  DEFAULT_LEARNING_FACT_LIMITS,
  LearningFactStore,
  deterministicToolOperationId,
} from "../packages/collector-cli/src/learning-facts";

const SCHEMA = "plimsoll.learning-facts-capacity-proof.v1" as const;
const source = fs.readFileSync("packages/collector-cli/src/learning-facts.ts", "utf8");
const serverSource = fs.readFileSync("packages/collector-cli/src/server.ts", "utf8");
const cliSource = fs.readFileSync("packages/collector-cli/src/cli.ts", "utf8");
const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function timestamp(index: number) {
  return new Date(Date.parse("2026-09-01T00:00:00.000Z") + index * 60_000).toISOString();
}

function attempt(index: number, sessionId = `capacity-session-${index}`) {
  return {
    kind: "attempt" as const,
    operationId: deterministicToolOperationId({
      source: "codex",
      sessionId,
      sourceOperationKey: `capacity-operation-${index}`,
    }),
    source: "codex" as const,
    sessionId,
    toolClass: "compute" as const,
    toolName: "shell" as const,
    startedAt: timestamp(index),
  };
}

function main() {
  // A migration may do a one-time reconciliation count, but the hot-path
  // capacity helper and all promotion code must remain count-free.
  const schemaStart = source.indexOf("private ensureSchema");
  const capacityStart = source.indexOf("private capacityPressure");
  const classEnd = source.indexOf("  attempts():", capacityStart);
  const runtimeCapacityText = source.slice(capacityStart, classEnd);
  const promotionText = source.slice(classEnd);
  check(
    "hot_path_has_no_full_table_count",
    capacityStart >= 0 && classEnd > capacityStart &&
      !/count\s*\(\s*\*\s*\)/i.test(runtimeCapacityText) &&
      !/count\s*\(\s*\*\s*\)/i.test(promotionText),
    { schemaStart, capacityStart, classEnd },
  );
  check(
    "learning_fact_limits_remain_the_size_bound",
    DEFAULT_LEARNING_FACT_LIMITS.attempts === 100_000 &&
      DEFAULT_LEARNING_FACT_LIMITS.episodes === 10_000 &&
      DEFAULT_LEARNING_FACT_LIMITS.exposures === 10_000 &&
      DEFAULT_LEARNING_FACT_LIMITS.techniqueIdentities === 256,
    { limits: DEFAULT_LEARNING_FACT_LIMITS },
  );
  check(
    "status_surfaces_report_fact_fill_and_evictions_without_a_scan",
    serverSource.includes("learningFacts: refreshControl ? buffer.learningFacts.status()") &&
      serverSource.includes("learningFacts: null") &&
      cliSource.includes("learningFacts: buffer.learningFacts.status()") &&
      !/count\s*\(\s*\*\s*\)/i.test(source.slice(source.indexOf("status():"), source.indexOf("runMaintenance("))),
    {},
  );

  const db = new Database(":memory:");
  try {
    const store = new LearningFactStore(db, {
      attempts: 3,
      episodes: 3,
      exposures: 3,
      techniqueIdentities: 3,
    });
    for (let index = 0; index < 8; index += 1) store.recordToolSignal(attempt(index));

    const before = store.status();
    check(
      "new_attempt_is_accepted_above_capacity_until_maintenance",
      before.tables.tool_attempt_facts.rowCount === 8 &&
        before.tables.tool_attempt_facts.limit === 3 &&
        before.tables.tool_attempt_facts.maintenanceNeeded,
      { before },
    );

    let maintenanceRuns = 0;
    let totalEvicted = 0;
    while (store.status().tables.tool_attempt_facts.rowCount > 3) {
      const result = store.runMaintenance(2);
      maintenanceRuns += 1;
      totalEvicted += result.evicted;
      check("maintenance_batch_is_bounded", result.evicted <= 2, { result });
      assert.ok(result.evicted > 0, "maintenance must make progress while over cap");
    }
    const attempts = store.attempts();
    const after = store.status();
    check(
      "newest_attempts_are_retained_and_oldest_are_evicted",
      attempts.length === 3 &&
        attempts.every((row, index) => row.sessionId === `capacity-session-${index + 5}`) &&
        after.tables.tool_attempt_facts.rowCount === 3 &&
        after.tables.tool_attempt_facts.evictedCount === 5 &&
        totalEvicted === 5,
      { attempts: attempts.map((row) => row.sessionId), after, maintenanceRuns },
    );
    check(
      "status_reports_all_fact_tables_without_a_scan",
      Object.keys(after.tables).sort().join(",") ===
        "technique_exposure_facts,technique_identity_registry,tool_attempt_facts,work_episode_facts" &&
        after.totalRows === 3 && after.totalLimit === 12 && after.totalEvicted === 5,
      { status: after },
    );
    check(
      "evicted_facts_are_fully_removed",
      !JSON.stringify(db.prepare(`select * from tool_attempt_facts`).all()).includes("capacity-session-0") &&
        !JSON.stringify(db.prepare(`select * from tool_attempt_facts`).all()).includes("capacity-operation-0"),
      { remainingRows: attempts.length },
    );
    check(
      "eviction_accounting_is_aggregate",
      (db.prepare(
        `select count(*) as n from sqlite_master where type = 'table' and name like '%evict%'`,
      ).get() as { n: number }).n === 0,
      { state: after.tables.tool_attempt_facts },
    );
  } finally {
    db.close();
  }

  process.stdout.write(`${JSON.stringify({
    schema: SCHEMA,
    passed: true,
    checks: checks.length,
    measurements: {
      maintenanceBatch: 2,
      attemptLimit: 3,
      retainedAttempts: 3,
      evictedAttempts: 5,
    },
    liveStateTouched: false,
    providerNetworkCalled: false,
    backgroundScansStarted: false,
    llmCalled: false,
    checksDetail: checks,
  }, null, 2)}\n`);
}

main();
