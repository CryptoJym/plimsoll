#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import { spawnSync } from "node:child_process";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import Database from "better-sqlite3";

import {
  DEFAULT_LEARNING_FACT_LIMITS,
  LearningFactStore,
  buildTechniqueExposureFact,
  buildWorkEpisodeFact,
  deterministicLearningFactId,
  deterministicToolOperationId,
} from "../packages/collector-cli/src/learning-facts";
import { CollectorMaintenance, automaticRepairServiceStatus } from "../packages/collector-cli/src/maintenance";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { runLearningMaterialization } from "../packages/collector-cli/src/learning-materializer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import {
  recordExplicitTechniqueAssignment,
  runtimeFactDropCounters,
} from "../packages/collector-cli/src/runtime-facts";

const SCHEMA = "plimsoll.learning-facts-capacity-proof.v2" as const;
const repoRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const privacySpec = fs.readFileSync(path.join(repoRoot, "docs/privacy-spec.md"), "utf8");
const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];
type TableName = keyof typeof DEFAULT_TABLE_COUNTS;
const DEFAULT_TABLE_COUNTS = { tool_attempt_facts: 0, work_episode_facts: 0,
  technique_exposure_facts: 0, technique_identity_registry: 0 };
type Counts = Record<TableName, number>;

function check(name: string, condition: unknown, detail: Record<string, unknown> = {}) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function timestamp(index: number) {
  return new Date(Date.parse("2026-09-01T00:00:00.000Z") + index * 1_000).toISOString();
}

function attempt(index: number, prefix = "capacity") {
  const sessionId = `${prefix}-session-${index}`;
  return {
    kind: "attempt" as const,
    operationId: deterministicToolOperationId({
      source: "codex",
      sessionId,
      sourceOperationKey: `${prefix}-operation-${index}`,
    }),
    source: "codex" as const,
    sessionId,
    toolClass: "compute" as const,
    toolName: "shell" as const,
    startedAt: timestamp(index),
  };
}

function rawAttemptInsert(db: Database.Database) {
  return db.prepare(
    `insert into tool_attempt_facts
      (operation_id, source, session_id, episode_id, tool_class, tool_name,
       started_at, ended_at, duration_ms, result_status, error_category,
       retry_of, created_at, updated_at)
     values (?, 'codex', ?, null, 'compute', 'shell', ?, null, null,
       'unknown', 'unknown', null, ?, ?)`,
  );
}

function insertRawAttempts(db: Database.Database, start: number, count: number, prefix: string) {
  const insert = rawAttemptInsert(db);
  db.transaction(() => {
    for (let index = start; index < start + count; index += 1) {
      const at = timestamp(index);
      insert.run(deterministicLearningFactId([prefix, String(index)]), `${prefix}-session-${index}`, at, at, at);
    }
  })();
}

function rawEpisodeInsert(db: Database.Database) {
  return db.prepare(
    `insert into work_episode_facts
      (episode_id, source, session_id, work_class, complexity_band,
       parent_episode_id, started_at, ended_at, duration_ms, created_at)
     values (?, 'codex', ?, 'review', 'medium', null, ?, ?, ?, ?)`,
  );
}

function rawExposureInsert(db: Database.Database) {
  return db.prepare(
    `insert into technique_exposure_facts
      (exposure_id, episode_id, technique_id, technique_version, content_digest,
       assignment_id, work_class, complexity_band, exposed_at, mode, assertion, created_at)
     values (?, ?, ?, ?, null, ?, 'review', 'medium', ?, 'treatment', 'exposure_only', ?)`,
  );
}

function rawRegistryInsert(db: Database.Database) {
  return db.prepare(
    `insert into technique_identity_registry
      (technique_key, technique_id, technique_version, content_digest, first_seen_at)
     values (?, ?, ?, null, ?)`,
  );
}

function actualCounts(db: Database.Database) {
  const tables = [
    "tool_attempt_facts",
    "work_episode_facts",
    "technique_exposure_facts",
    "technique_identity_registry",
  ] as const;
  return Object.fromEntries(tables.map((table) => [
    table,
    (db.prepare(`select count(*) as n from ${table}`).get() as { n: number }).n,
  ]));
}

function exerciseHotPathBound() {
  let fullCounts = 0;
  let orderedSelections = 0;
  let rowDeletes = 0;
  const statusSql: string[] = [];
  let tracingStatus = false;
  const db = new Database(":memory:", {
    verbose: (sql) => {
      if (/count\s*\(\s*\*\s*\)/i.test(String(sql))) fullCounts += 1;
      if (/^select operation_id as id\s+from tool_attempt_facts\s+order by/i.test(String(sql))) orderedSelections += 1;
      if (/^delete from tool_attempt_facts where operation_id = /i.test(String(sql))) rowDeletes += 1;
      if (tracingStatus) statusSql.push(String(sql));
    },
  });
  try {
    const store = new LearningFactStore(db);
    fullCounts = 0;
    insertRawAttempts(db, 0, DEFAULT_LEARNING_FACT_LIMITS.attempts, "seed");
    const before = store.status().tables.tool_attempt_facts;
    fullCounts = 0;
    const burstStarted = performance.now();
    const burstSize = 10_000;
    let inserted = 0;
    let peakRows = before.rowCount;
    for (let index = 0; index < burstSize; index += 1) {
      if (store.recordToolSignal(attempt(200_000 + index, "burst")).inserted) inserted += 1;
      peakRows = Math.max(peakRows, store.status().tables.tool_attempt_facts.rowCount);
    }
    const burstMs = performance.now() - burstStarted;
    const writeFullCounts = fullCounts;
    tracingStatus = true;
    const after = store.status().tables.tool_attempt_facts;
    tracingStatus = false;
    const oldestSeed = (db.prepare(
      `select count(*) as n from tool_attempt_facts where started_at < '${timestamp(burstSize)}'`,
    ).get() as { n: number }).n;
    const newestBurst = (db.prepare(
      `select count(*) as n from tool_attempt_facts where session_id like 'burst-session-%'`,
    ).get() as { n: number }).n;
    check(
      "reviewer_count_paths_head_insert_has_no_full_table_count",
      writeFullCounts === 0,
      { writeFullCounts },
    );
    check(
      "studio0_shaped_burst_accepts_newest_and_holds_the_hard_cap",
      before.rowCount === DEFAULT_LEARNING_FACT_LIMITS.attempts &&
        inserted === burstSize &&
        after.rowCount === DEFAULT_LEARNING_FACT_LIMITS.attempts &&
        !after.maintenanceNeeded &&
        after.evictedCount === burstSize &&
        oldestSeed === 0 &&
        newestBurst === burstSize && peakRows === DEFAULT_LEARNING_FACT_LIMITS.attempts,
      { before, after, inserted, burstSize, peakRows, oldestSeed, newestBurst, burstMs: Number(burstMs.toFixed(2)) },
    );
    check(
      "normal_write_cost_is_bounded_by_one_indexed_eviction",
      orderedSelections === burstSize && rowDeletes === burstSize,
      { burstSize, orderedSelections, rowDeletes, burstMs: Number(burstMs.toFixed(2)), averageMs: Number((burstMs / burstSize).toFixed(4)) },
    );
    check("status_reads_only_the_four_row_state", statusSql.length === 1 &&
      /from learning_fact_table_state/i.test(statusSql[0]) && !/count\(/i.test(statusSql[0]), { statusSql });
    assertCounts(db, "count_exact_after_10000_attempt_burst");
  } finally {
    db.close();
  }
}

function exerciseFairness() {
  const db = new Database(":memory:");
  try {
    const store = new LearningFactStore(db, {
      attempts: 1,
      episodes: 1,
      exposures: 1,
      techniqueIdentities: 1,
    });
    const episodes = Array.from({length:9}, (_, index) => buildWorkEpisodeFact({
      source: "codex",
      sessionId: `fairness-session-${index}`,
      sourceEpisodeKey: `fairness-episode-${index}`,
      workClass: "review",
      complexityBand: "medium",
      startedAt: timestamp(index * 10),
      endedAt: timestamp(200),
    }));
    const insertEpisode = rawEpisodeInsert(db);
    const insertExposure = rawExposureInsert(db);
    const insertRegistry = rawRegistryInsert(db);
    db.transaction(() => {
      for (const [index, episode] of episodes.entries()) {
        insertEpisode.run(
          episode.episodeId,
          episode.sessionId,
          episode.startedAt,
          episode.endedAt,
          episode.durationMs,
          episode.startedAt,
        );
        const exposure = buildTechniqueExposureFact({
          episodeId: episodes[8].episodeId,
          techniqueId: `fairness-technique-${index}`,
          techniqueVersion: "1.0.0",
          assignmentId: `fairness-assignment-${index}`,
          workClass: "review",
          complexityBand: "medium",
          exposedAt: timestamp(100 + index),
          mode: "treatment",
        });
        insertExposure.run(
          exposure.exposureId,
          exposure.episodeId,
          exposure.techniqueId,
          exposure.techniqueVersion,
          exposure.assignmentId,
          exposure.exposedAt,
          episode.startedAt,
        );
        insertRegistry.run(
          deterministicLearningFactId([exposure.techniqueId, exposure.techniqueVersion ?? "", ""]),
          exposure.techniqueId,
          exposure.techniqueVersion,
          episode.startedAt,
        );
      }
      insertRawAttempts(db, 300_000, 300, "fairness");
    })();
    for (let pass = 0; pass < 3; pass++) {
      const before = actualCounts(db);
      const receipt = store.runMaintenance(2);
      const after = actualCounts(db);
      check(`every_over_cap_table_progresses_under_sustained_attempt_pressure_${pass}`,
        Object.keys(before).every(table => before[table] - after[table] === 2) &&
          receipt.tables.length === 4, {before, after, receipt});
      assertCounts(db, `fair_pass_${pass}_counts_equal_rows`);
      insertRawAttempts(db, 400_000 + pass * 2, 2, "sustained");
    }
  } finally {
    db.close();
  }

}

async function exerciseProductionStage() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fact-maintenance-"));
  const buffer = new LocalEventBuffer(path.join(root, "ledger.sqlite"), {
    delivery: { enabled: false }, learningFacts: { limits: { attempts: 1 } },
  });
  const codexRoot = path.join(root, "codex");
  const claudeRoot = path.join(root, "claude");
  fs.mkdirSync(codexRoot); fs.mkdirSync(claudeRoot);
  const maintenance = new CollectorMaintenance(buffer,
    new RolloutTailer(buffer, codexRoot, () => []), new TranscriptTailer(buffer, claudeRoot));
  try {
    insertRawAttempts(buffer.database, 500_000, 300, "stage");
    const service = automaticRepairServiceStatus(buffer.database);
    service.next = 4; // Admit the real learning_facts switch arm first.
    buffer.database.prepare(`insert or replace into maintenance_state(key,value,updated_at) values(?,?,?)`)
      .run("automatic_repair_service_v1", JSON.stringify(service), timestamp(0));
    await maintenance.runRecent();
    const stage = automaticRepairServiceStatus(buffer.database).stages.learning_facts;
    check(
      "production_learning_fact_maintenance_stage_drains_legacy_overflow",
      stage.completed === 1 && stage.failures === 0 && stage.rowsVisited === 256 &&
        buffer.learningFacts.status().tables.tool_attempt_facts.rowCount === 44,
      { stage },
    );
    assertCounts(buffer.database, "production_stage_counts_equal_rows");
  } finally {
    maintenance.close(); buffer.close(); fs.rmSync(root, {recursive:true,force:true});
  }
}

function exerciseReferentialRetention() {
  const db = new Database(":memory:");
  try {
    const store = new LearningFactStore(db, {
      attempts: 10,
      episodes: 1,
      exposures: 10,
      techniqueIdentities: 10,
    });
    const first = buildWorkEpisodeFact({
      source: "codex",
      sessionId: "referential-session-0",
      sourceEpisodeKey: "referential-episode-0",
      workClass: "review",
      complexityBand: "medium",
      startedAt: timestamp(0),
    });
    const second = buildWorkEpisodeFact({
      source: "codex",
      sessionId: "referential-session-1",
      sourceEpisodeKey: "referential-episode-1",
      workClass: "review",
      complexityBand: "medium",
      startedAt: timestamp(1),
    });
    store.recordWorkEpisode(first);
    const operationId = deterministicToolOperationId({
      source: "codex",
      sessionId: first.sessionId,
      sourceOperationKey: "referential-attempt-0",
    });
    store.recordToolSignal({
      kind: "attempt",
      operationId,
      source: "codex",
      sessionId: first.sessionId,
      episodeId: first.episodeId,
      toolClass: "compute",
      toolName: "shell",
      startedAt: timestamp(0),
    });
    const exposure = buildTechniqueExposureFact({
      episodeId: first.episodeId,
      techniqueId: "referential-technique",
      techniqueVersion: "1.0.0",
      assignmentId: "referential-assignment",
      workClass: "review",
      complexityBand: "medium",
      exposedAt: timestamp(0),
      mode: "treatment",
    });
    store.recordTechniqueExposure(exposure, { outcomeObservedAt: timestamp(10) });
    const write = store.recordWorkEpisode(second);
    store.runMaintenance(); // Also exercise the reviewer's old maintenance-only eviction path.
    const orphanAttempts = (db.prepare(
      `select count(*) as n from tool_attempt_facts a
         left join work_episode_facts e on e.episode_id = a.episode_id
        where a.episode_id is not null and e.episode_id is null`,
    ).get() as { n: number }).n;
    const orphanExposures = (db.prepare(
      `select count(*) as n from technique_exposure_facts x
         left join work_episode_facts e on e.episode_id = x.episode_id
        where e.episode_id is null`,
    ).get() as { n: number }).n;
    const lateResult = store.recordToolSignal({
      kind: "result",
      operationId,
      source: "codex",
      sessionId: first.sessionId,
      endedAt: timestamp(2),
      resultStatus: "success",
    });
    const lateExposure = recordExplicitTechniqueAssignment(
      { learningFacts: store },
      {
        episodeId: first.episodeId,
        techniqueId: "late-technique",
        techniqueVersion: "1.0.0",
        assignmentId: "late-assignment",
        workClass: "review",
        complexityBand: "medium",
        exposedAt: timestamp(0),
        mode: "control",
      },
      { now: () => new Date(timestamp(10)) },
    );
    const staleCounters = runtimeFactDropCounters(db);
    check(
      "episode_eviction_removes_related_attempts_and_exposures_atomically",
      write.inserted && store.episodes().length === 1 && store.episodes()[0].episodeId === second.episodeId &&
        orphanAttempts === 0 && orphanExposures === 0 &&
        store.attempts().length === 0 && store.exposures().length === 0,
      { write, orphanAttempts, orphanExposures },
    );
    check(
      "late_result_and_exposure_for_evicted_episode_drop_without_throwing",
      lateResult.dropped === true && lateExposure.dropped === true &&
        staleCounters.some((row) => row.reason === "stale_reference" && row.droppedCount === 1) &&
        staleCounters.some((row) => row.reason === "unpaired_result" && row.droppedCount === 1),
      { lateResult, lateExposure, staleCounters },
    );
  } finally {
    db.close();
  }
}

function exerciseInterruptedUpgradeDowngrade() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-learning-facts-migration-r2-"));
  const ledgerPath = path.join(root, "ledger.sqlite");
  try {
    const seedDb = new Database(ledgerPath);
    new LearningFactStore(seedDb, { attempts: 20 });
    insertRawAttempts(seedDb, 600_000, 3, "migration-seed");
    for (const table of [
      "tool_attempt_facts",
      "work_episode_facts",
      "technique_exposure_facts",
      "technique_identity_registry",
    ]) {
      seedDb.exec(`drop trigger learning_fact_state_${table}_insert`);
      seedDb.exec(`drop trigger learning_fact_state_${table}_delete`);
    }
    seedDb.exec(`drop table learning_fact_table_state`);
    seedDb.close();

    const child = spawnSync(
      process.execPath,
      [...process.execArgv, fileURLToPath(import.meta.url), "--crash-child", ledgerPath],
      { cwd: repoRoot, encoding: "utf8", timeout: 30_000 },
    );
    const afterCrash = new Database(ledgerPath);
    try {
      const stateTables = (afterCrash.prepare(
        `select count(*) as n from sqlite_master where type='table' and name='learning_fact_table_state'`,
      ).get() as { n: number }).n;
      const triggers = (afterCrash.prepare(
        `select count(*) as n from sqlite_master where type='trigger' and name like 'learning_fact_state_%'`,
      ).get() as { n: number }).n;
      check(
        "interrupted_upgrade_rolls_back_state_and_triggers_together",
        (child.signal === "SIGKILL" || child.status === 137) && stateTables === 0 && triggers === 0,
        { child: { status: child.status, signal: child.signal }, stateTables, triggers },
      );
      insertRawAttempts(afterCrash, 700_000, 1, "downgrade-write");
    } finally {
      afterCrash.close();
    }

    const reupgradeDb = new Database(ledgerPath);
    try {
      const store = new LearningFactStore(reupgradeDb, { attempts: 20 });
      const actual = (reupgradeDb.prepare(`select count(*) as n from tool_attempt_facts`).get() as { n: number }).n;
      const status = store.status().tables.tool_attempt_facts;
      check(
        "reopen_recounts_after_interrupted_upgrade_and_downgrade_write",
        actual === 4 && status.rowCount === 4 && !status.maintenanceNeeded,
        { actual, status },
      );
    } finally {
      reupgradeDb.close();
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
}

function maintainedCounts(db: Database.Database): Counts {
  const rows = db.prepare(
    `select table_name as tableName, row_count as rowCount
       from learning_fact_table_state order by table_name`,
  ).all() as Array<{ tableName: TableName; rowCount: number }>;
  return Object.fromEntries(rows.map((row) => [row.tableName, row.rowCount])) as Counts;
}

function assertCounts(db: Database.Database, name: string) {
  const actual = actualCounts(db);
  const maintained = maintainedCounts(db);
  assert.deepEqual(maintained, actual, `${name}: maintained count diverged`);
  checks.push({ name, detail: { actual, maintained } });
  return actual;
}

function rawAttemptId(index: number) {
  return `00000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`;
}

function exerciseApiAndMutationPaths() {
  const db = new Database(":memory:");
  try {
    const store = new LearningFactStore(db, {
      attempts: 2,
      episodes: 2,
      exposures: 2,
      techniqueIdentities: 2,
    });
    assertCounts(db, "count_exact_after_schema_migration_on_empty_ledger");

    const episode = buildWorkEpisodeFact({
      source: "codex",
      sessionId: "path-session",
      sourceEpisodeKey: "path-episode",
      workClass: "review",
      complexityBand: "medium",
      startedAt: timestamp(0),
      endedAt: timestamp(10),
    });
    store.recordWorkEpisode(episode);
    assertCounts(db, "count_exact_after_episode_insert");

    const start = {
      ...attempt(1, "path"),
      sessionId: episode.sessionId,
      episodeId: episode.episodeId,
      startedAt: timestamp(1),
    };
    store.recordToolSignal(start);
    assertCounts(db, "count_exact_after_attempt_insert");

    store.recordToolSignal({
      kind: "result",
      operationId: start.operationId,
      source: start.source,
      sessionId: start.sessionId,
      endedAt: timestamp(2),
      resultStatus: "success",
    });
    assertCounts(db, "count_exact_after_attempt_update");

    const exposure = buildTechniqueExposureFact({
      episodeId: episode.episodeId,
      techniqueId: "path-technique",
      techniqueVersion: "1.0.0",
      assignmentId: "path-assignment",
      workClass: "review",
      complexityBand: "medium",
      exposedAt: timestamp(3),
      mode: "treatment",
    });
    store.recordTechniqueExposure(exposure, { outcomeObservedAt: timestamp(4) });
    assertCounts(db, "count_exact_after_exposure_and_identity_insert");

    db.prepare("update technique_exposure_facts set assignment_id = assignment_id where exposure_id = ?")
      .run(exposure.exposureId);
    assertCounts(db, "count_exact_after_non_cardinality_update");

    db.prepare("delete from technique_exposure_facts where exposure_id = ?").run(exposure.exposureId);
    assertCounts(db, "count_exact_after_direct_logical_erasure");
    db.prepare("delete from technique_identity_registry").run();
    db.prepare("delete from tool_attempt_facts").run();
    db.prepare("delete from work_episode_facts").run();
    assertCounts(db, "count_exact_after_multi_table_privacy_style_deletion");

    const beforeRollback = actualCounts(db);
    assert.throws(() => db.transaction(() => {
      const at = timestamp(7);
      rawAttemptInsert(db).run(rawAttemptId(7), "rolled-back", at, at, at);
      throw new Error("force_rollback");
    })(), /force_rollback/);
    assert.deepEqual(actualCounts(db), beforeRollback);
    assertCounts(db, "count_exact_after_explicit_transaction_rollback");
  } finally {
    db.close();
  }
}

function exerciseCrashRollback(root: string) {
  const dbPath = path.join(root, "crash.sqlite");
  const seed = new Database(dbPath);
  new LearningFactStore(seed, { attempts: 20 });
  insertRawAttempts(seed, 0, 3, "crash-seed");
  assertCounts(seed, "count_exact_before_crash_child");
  seed.close();

  const child = spawnSync(process.execPath, [
    ...process.execArgv,
    fileURLToPath(import.meta.url),
    "--write-crash-child",
    dbPath,
  ], { encoding: "utf8" });
  check(
    "crash_child_was_killed_before_commit",
    child.signal === "SIGKILL" || child.status === 137,
    { status: child.status, signal: child.signal },
  );
  const reopened = new Database(dbPath);
  try {
    new LearningFactStore(reopened, { attempts: 20 });
    const counts = assertCounts(reopened, "count_exact_after_process_crash_mid_transaction");
    check("uncommitted_crash_insert_is_absent", counts.tool_attempt_facts === 3, { counts });
  } finally {
    reopened.close();
  }
}

function exerciseRetentionPrivacyCompactionAndReadonly(root: string) {
  const ledgerPath = path.join(root, "paths.sqlite");
  const buffer = new LocalEventBuffer(ledgerPath, {
    delivery: { enabled: false },
    learningFacts: { limits: { attempts: 10, episodes: 10 } },
  });
  try {
    const event = aiInteractionEventSchema.parse({
      id: "11111111-1111-4111-8111-111111111111",
      source: "codex",
      sessionId: "retention-session",
      dataMode: "metadata",
      eventType: "tool_use",
      observedAt: "2025-01-01T00:00:00.000Z",
      actionClass: "shell",
      metadata: { call_id: "retention-operation" },
    });
    assert.equal(buffer.append(event), true);
    buffer.database.prepare("update buffered_events set created_at=? where id=?").run(
      "2025-01-01T00:00:00.000Z",
      event.id,
    );
    const promoted = assertCounts(buffer.database, "count_exact_after_transactional_runtime_promotion");
    check(
      "runtime_promotion_created_episode_and_attempt",
      promoted.tool_attempt_facts === 1 && promoted.work_episode_facts === 1,
      { promoted },
    );

    buffer.database.prepare(
      "update buffered_events set privacy_disposition='local_privacy_violation', privacy_disposed_at=? where id=?",
    ).run("2026-01-01T00:00:00.000Z", event.id);
    assertCounts(buffer.database, "count_exact_after_terminal_privacy_disposition");

    buffer.projection.runMaintenance(new Date("2026-01-02T00:00:00.000Z"));
    assertCounts(buffer.database, "count_exact_after_projection_compaction_maintenance");

    const prune = buffer.prune(1, {
      maxRows: 100,
      now: new Date("2026-01-03T00:00:00.000Z"),
    });
    assert.ok(prune.events >= 1);
    assertCounts(buffer.database, "count_exact_after_raw_retention_deletion");
  } finally {
    buffer.close();
  }

  const beforeWorker = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  const beforeActual = actualCounts(beforeWorker);
  const beforeMaintained = maintainedCounts(beforeWorker);
  beforeWorker.close();
  assert.deepEqual(beforeMaintained, beforeActual);

  const materialized = runLearningMaterialization({
    ledgerPath,
    outcomeStorePath: null,
    statePath: path.join(root, "materializer-state.sqlite"),
    outPath: null,
    workspaceRoot: root,
    until: "2026-01-04T00:00:00.000Z",
    windowDays: 30,
    maxNewUsageEvents: 100,
  });
  const afterWorker = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  try {
    const afterActual = actualCounts(afterWorker);
    const afterMaintained = maintainedCounts(afterWorker);
    check(
      "read_only_materializer_leaves_counts_and_rows_unchanged",
      JSON.stringify(beforeActual) === JSON.stringify(afterActual) &&
        JSON.stringify(beforeMaintained) === JSON.stringify(afterMaintained),
      { materializerStatus: materialized.status, beforeActual, afterActual, afterMaintained },
    );
  } finally {
    afterWorker.close();
  }

  fs.rmSync(ledgerPath, { force: true });
  fs.rmSync(`${ledgerPath}-wal`, { force: true });
  fs.rmSync(`${ledgerPath}-shm`, { force: true });
  check("whole_ledger_purge_removes_fact_rows_and_state_together", !fs.existsSync(ledgerPath), {
    ledgerExists: fs.existsSync(ledgerPath),
  });
}

function exerciseEvictionRollbackAndIndexes() {
  let fullCounts = 0;
  const db = new Database(":memory:", {verbose: sql => { if (/count\s*\(/i.test(String(sql))) fullCounts++; }});
  try {
    const store = new LearningFactStore(db, {attempts:2, episodes:2, exposures:2, techniqueIdentities:2});
    fullCounts = 0;
    for (let index = 0; index < 4; index++) {
      const episode = buildWorkEpisodeFact({ source:"codex", sessionId:`bound-${index}`,
        sourceEpisodeKey:`bound-${index}`, workClass:"review", complexityBand:"medium", startedAt:timestamp(index) });
      store.recordWorkEpisode(episode);
      store.recordToolSignal(attempt(index));
      store.recordTechniqueExposure(buildTechniqueExposureFact({episodeId:episode.episodeId,
        techniqueId:`technique-${index}`, techniqueVersion:"1", assignmentId:`assignment-${index}`,
        workClass:"review", complexityBand:"medium", exposedAt:timestamp(index), mode:"treatment"}));
    }
    check("all_sibling_write_paths_have_no_counts_and_enforce_caps", fullCounts === 0 &&
      Object.values(store.status().tables).every(row => row.rowCount === 2), {fullCounts, status:store.status()});
    assertCounts(db, "all_sibling_counts_exact_after_eviction");
    const before = store.status();
    const rows = store.attempts();
    db.exec(`create trigger abort_new_attempt before insert on tool_attempt_facts
      begin select raise(abort, 'rollback_probe'); end`);
    assert.throws(() => store.recordToolSignal(attempt(10)), /rollback_probe/);
    assert.deepEqual(store.status(), before);
    assert.deepEqual(store.attempts(), rows);
    db.exec(`drop trigger abort_new_attempt`);
    check("failed_insert_rolls_back_eviction_rows_counts_and_receipts", true, {});

    const plans = [
      `select operation_id from tool_attempt_facts order by started_at,operation_id limit 1`,
      `select episode_id from work_episode_facts order by started_at,episode_id limit 1`,
      `select exposure_id from technique_exposure_facts order by exposed_at,exposure_id limit 1`,
      `select technique_key from technique_identity_registry order by first_seen_at,technique_key limit 1`,
      `delete from tool_attempt_facts where episode_id = 'fixture'`,
      `delete from technique_exposure_facts where episode_id = 'fixture'`,
      `select episode_id from work_episode_facts where parent_episode_id = 'fixture'`,
      `select operation_id from tool_attempt_facts where retry_of = 'fixture'`,
    ].map(sql => ({sql, plan:(db.prepare(`explain query plan ${sql}`).all() as Array<{detail:string}>).map(x=>x.detail)}));
    check("retention_and_graph_queries_use_indexes_without_sort_or_full_table_scan", plans.every(({plan}) =>
      plan.every(detail => /USING (COVERING )?INDEX/i.test(detail) && !/TEMP B-TREE/i.test(detail))), {plans});
    // A retry whose parent is evicted must not survive as a dangling link.
    db.exec(`delete from tool_attempt_facts`);
    const parent = {...attempt(20), sessionId:"retry-session"};
    store.recordToolSignal(parent);
    store.recordToolSignal({...attempt(21), sessionId:parent.sessionId, retryOf:parent.operationId});
    store.recordToolSignal(attempt(22));
    check("retry_descendants_leave_with_their_evicted_parent", store.attempts().length === 1 &&
      store.attempts()[0].operationId === attempt(22).operationId, {});
  } finally { db.close(); }
}

function exerciseMissingTriggers() {
  let counts = 0;
  const db = new Database(":memory:", {verbose: sql => {if (/select count\(\*\)/i.test(String(sql))) counts++;}});
  try {
    new LearningFactStore(db);
    insertRawAttempts(db,0,3,"missing-trigger");
    db.exec(`drop trigger learning_fact_state_tool_attempt_facts_insert`);
    insertRawAttempts(db,3,1,"downgrade"); // v0.7.37-style writer has no count-state logic.
    const store = new LearningFactStore(db);
    check("existing_state_is_recounted_when_any_trigger_was_missing",
      store.status().tables.tool_attempt_facts.rowCount === 4, {status:store.status()});
    assertCounts(db, "repaired_partial_trigger_set_counts_exact");
    // A completed upgrade keeps tracking writes by the old SQL insert/delete path.
    insertRawAttempts(db,4,1,"completed-downgrade");
    db.prepare(`delete from tool_attempt_facts where operation_id = ?`).run(deterministicLearningFactId(["missing-trigger","0"]));
    counts = 0;
    const reopened = new LearningFactStore(db);
    check("normal_reopen_and_completed_downgrade_need_no_recount", counts === 0 &&
      reopened.status().tables.tool_attempt_facts.rowCount === 4, {fullCounts:counts});
    assertCounts(db, "completed_downgrade_counts_exact");
  } finally { db.close(); }
}

function exerciseLateRuntimeResult() {
  const buffer = new LocalEventBuffer(":memory:", {delivery:{enabled:false}, learningFacts:{limits:{episodes:1,attempts:10}}});
  try {
    const event = (index:number, session:string, result=false) => aiInteractionEventSchema.parse({
      id:deterministicLearningFactId(["late-runtime",String(index)]), source:"codex", sessionId:session,
      dataMode:"metadata", eventType:result ? "tool_result" : "tool_use", observedAt:timestamp(index),
      actionClass:"shell", metadata:{call_id:`operation-${session}`},
    });
    buffer.append(event(0,"evicted"));
    buffer.append(event(1,"newest"));
    buffer.append(event(2,"evicted",true));
    check("late_runtime_result_does_not_recreate_an_evicted_episode", buffer.learningFacts.episodes().length === 1 &&
      buffer.learningFacts.episodes()[0].sessionId === "newest" &&
      buffer.learningFacts.attempts().length === 1 && buffer.learningFacts.attempts()[0].sessionId === "newest" &&
      runtimeFactDropCounters(buffer.database).some(row => row.reason === "unpaired_result" && row.droppedCount === 1), {});
    assertCounts(buffer.database, "late_runtime_result_counts_exact");
  } finally { buffer.close(); }
}

function exerciseNestedEpisodeRollback() {
  const db = new Database(":memory:");
  try {
    const store = new LearningFactStore(db, {episodes:2,attempts:10,exposures:10});
    const parent = buildWorkEpisodeFact({source:"codex", sessionId:"nested", sourceEpisodeKey:"parent",
      workClass:"review", complexityBand:"medium", startedAt:timestamp(0)});
    const child = buildWorkEpisodeFact({source:"codex", sessionId:"nested", sourceEpisodeKey:"child",
      parentEpisodeId:parent.episodeId, workClass:"review", complexityBand:"medium", startedAt:timestamp(1)});
    store.recordWorkEpisode(parent); store.recordWorkEpisode(child);
    store.recordToolSignal({...attempt(2), sessionId:child.sessionId, episodeId:child.episodeId});
    store.recordTechniqueExposure(buildTechniqueExposureFact({episodeId:child.episodeId,
      techniqueId:"nested-technique", techniqueVersion:"1", assignmentId:"nested-assignment",
      workClass:"review", complexityBand:"medium", exposedAt:timestamp(2), mode:"control"}));
    const newest = buildWorkEpisodeFact({source:"codex", sessionId:"next", sourceEpisodeKey:"next",
      workClass:"review", complexityBand:"medium", startedAt:timestamp(3)});
    const before = store.status();
    db.exec(`create trigger abort_graph before delete on technique_exposure_facts
      begin select raise(abort, 'graph_rollback_probe'); end`);
    assert.throws(() => store.recordWorkEpisode(newest), /graph_rollback_probe/);
    assert.deepEqual(store.status(), before);
    assert.equal(store.attempts().length,1);
    assert.equal(store.exposures().length,1);
    assert.equal(store.episodes().length,2);
    assertCounts(db,"graph_eviction_and_counters_rollback_atomically");
    db.exec('drop trigger abort_graph');
    store.recordWorkEpisode(newest);
    check("nested_episode_graph_is_fully_erased_and_evictions_are_aggregated",
      store.episodes().length === 1 && store.episodes()[0].episodeId === newest.episodeId &&
      store.attempts().length === 0 && store.exposures().length === 0 &&
      store.status().tables.work_episode_facts.evictedCount === 2 &&
      store.status().tables.tool_attempt_facts.evictedCount === 1 &&
      store.status().tables.technique_exposure_facts.evictedCount === 1, {status:store.status()});
    assertCounts(db,"nested_graph_counts_exact");
  } finally {db.close();}
}

async function main() {
  if (process.argv[2] === "--write-crash-child") {
    const db = new Database(process.argv[3]);
    new LearningFactStore(db, { attempts: 20 });
    db.exec("begin immediate");
    insertRawAttempts(db, 900_000, 1, "uncommitted");
    process.kill(process.pid, "SIGKILL");
    throw new Error("expected write crash");
  }
  if (process.argv[2] === "--crash-child") {
    const ledgerPath = process.argv[3];
    if (!ledgerPath) throw new Error("ledger path required");
    const db = new Database(ledgerPath, {
      verbose: (sql) => {
        if (/create trigger if not exists learning_fact_state_/i.test(String(sql))) {
          process.kill(process.pid, "SIGKILL");
        }
      },
    });
    new LearningFactStore(db);
    throw new Error("expected atomic migration child to be killed");
  }

  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-fact-capacity-paths-"));
  const cases: Record<string, () => void | Promise<void>> = {
    "count-paths": () => {
      exerciseHotPathBound();
      exerciseApiAndMutationPaths();
      exerciseEvictionRollbackAndIndexes();
      exerciseCrashRollback(root);
      exerciseRetentionPrivacyCompactionAndReadonly(root);
    },
    fairness: exerciseFairness,
    referential: () => { exerciseReferentialRetention(); exerciseLateRuntimeResult(); exerciseNestedEpisodeRollback(); },
    migration: exerciseInterruptedUpgradeDowngrade,
    "migration-recount": exerciseMissingTriggers,
    production: exerciseProductionStage,
    contracts: () => {
      check("learning_fact_limits_remain_the_size_bound",
        JSON.stringify(DEFAULT_LEARNING_FACT_LIMITS) === JSON.stringify({
          attempts: 100_000, episodes: 10_000, exposures: 10_000, techniqueIdentities: 256,
        }), { limits: DEFAULT_LEARNING_FACT_LIMITS });
      check("privacy_spec_lists_promoted_facts_state_and_retention",
        privacySpec.includes("tool_attempt_facts") && privacySpec.includes("learning_fact_table_state") &&
          privacySpec.includes("evicted"), {});
    },
  };
  try {
    const selected = process.argv[2] ? [process.argv[2]] : Object.keys(cases);
    for (const name of selected) {
      assert.ok(cases[name], `unknown case ${name}`);
      await cases[name]();
    }
  } finally {
    fs.rmSync(root, {recursive:true, force:true});
  }

  process.stdout.write(`${JSON.stringify({
    schema: SCHEMA,
    passed: true,
    checks: checks.length,
    liveStateTouched: false,
    providerNetworkCalled: false,
    backgroundScansStarted: false,
    llmCalled: false,
    checksDetail: checks,
  }, null, 2)}\n`);
}

main();
