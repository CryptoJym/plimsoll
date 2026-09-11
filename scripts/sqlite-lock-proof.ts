/** Deadline maintenance versus capture writer, real WAL connections, deterministic interleaving. */
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  beginAutomaticCaptureBaseline,
  stageAutomaticCaptureBaselineObservation,
} from "../packages/collector-cli/src/capture-baseline";
import { bindCaptureInventory } from "../packages/collector-cli/src/capture-root-inventory";
import {
  ensureJsonlScanState,
  jsonlScanStateKey,
} from "../packages/collector-cli/src/jsonl-byte-tailer";
import { runDeadlineMaintenanceStages } from "../packages/collector-cli/src/maintenance-stage-primitives";
import { aiInteractionEventSchema } from "../packages/shared/src/index";

const require = createRequire(path.resolve("package.json"));
const Database = require("better-sqlite3");
const directory = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-sqlite-lock-proof-"));
const ledgerPath = path.join(directory, "ledger.sqlite");
const buffer = new LocalEventBuffer(ledgerPath, {
  databaseBusyTimeoutMs: 900,
  workspaceId: "local",
});
const maintenance = buffer.database;
const capture = new Database(ledgerPath, { timeout: 0 });
const largePendingRows = 4_096;

type InjectionStage = "retention" | "fill_pending_event_links";
type InjectionOutcome = "committed" | string;
type RemainingStage =
  | "projection"
  | "repo_context_fill"
  | "capture"
  | "baseline_observation"
  | "capture_inventory"
  | "jsonl_path_migration"
  | "workspace_binding"
  | "finance_capture_activity";

try {
  assert.equal(maintenance.pragma("journal_mode", { simple: true }), "wal");
  assert.equal(capture.pragma("journal_mode", { simple: true }), "wal");
  maintenance.exec("create table fixture_capture_writer(n integer); insert into fixture_capture_writer values(0)");

  const insertEvent = maintenance.prepare(
    `insert into buffered_events
       (id, source, event_type, data_mode, observed_at, payload_json, created_at)
     values (?, 'codex', 'assistant_response', 'metadata', ?, '{}', ?)`,
  );
  const insertContext = maintenance.prepare(
    `insert into repo_context_results
       (context_id, repo_hash, branch_hash, head_sha, resolved_at, resolver_version, accepted_at)
     values (?, 'sha256:fixture', null, null, ?, 'proof-v1', ?)`,
  );
  const insertLink = maintenance.prepare(
    `insert into repo_context_event_links (event_id, context_id) values (?, ?)`,
  );
  const recent = "2099-01-01T00:00:00.000Z";
  maintenance.transaction(() => {
    for (let index = 0; index < largePendingRows; index += 1) {
      const suffix = index.toString(16).padStart(64, "0");
      const eventId = `event-${suffix}`;
      const contextId = `repoctx:v1:${suffix}`;
      insertEvent.run(eventId, recent, recent);
      insertContext.run(contextId, recent, recent);
      insertLink.run(eventId, contextId);
    }
  })();

  // Characterize the SQLite failure independently: a deferred reader cannot
  // promote its stale WAL snapshot after a second connection commits.
  let legacyDeferredCode: string | null = null;
  try {
    maintenance.transaction(() => {
      maintenance.prepare("select n from fixture_capture_writer").get();
      capture.prepare("update fixture_capture_writer set n=n+1").run();
      maintenance.prepare("update fixture_capture_writer set n=n+1").run();
    })();
  } catch (error: any) {
    legacyDeferredCode = error.code ?? error.name;
  }
  assert.equal(legacyDeferredCode, "SQLITE_BUSY_SNAPSHOT");
  assert.equal(maintenance.inTransaction, false);
  assert.equal(capture.inTransaction, false);

  const baseline = beginAutomaticCaptureBaseline(maintenance, "codex", {
    startedAt: recent,
    filesDiscovered: 1,
    runId: "sqlite-lock-proof-baseline",
  });
  assert.ok(baseline.latestRun);
  maintenance.exec(`create table if not exists rollout_scan_state (
    file text primary key,
    size integer not null,
    scanned_at text not null
  )`);
  const legacyPath = path.join(directory, "legacy-rollout.jsonl");
  maintenance.prepare(
    `insert into rollout_scan_state (file, size, scanned_at) values (?, 0, ?)`,
  ).run(legacyPath, recent);

  const outcomes = new Map<InjectionStage, InjectionOutcome>();
  const compete = (stage: InjectionStage) => {
    if (outcomes.has(stage)) return;
    try {
      capture.prepare("update fixture_capture_writer set n=n+1").run();
      outcomes.set(stage, "committed");
    } catch (error: any) {
      outcomes.set(stage, error.code ?? error.name);
    }
  };
  const originalPrepare = maintenance.prepare.bind(maintenance);
  (maintenance as any).prepare = (sql: string) => {
    const statement = originalPrepare(sql);
    if (sql.includes("select value from maintenance_state where key=?")) {
      const get = statement.get.bind(statement);
      (statement as any).get = (...args: any[]) => {
        const row = get(...args);
        if (args[0] === "raw_retention_scan_v1") compete("retention");
        return row;
      };
    }
    if (sql.includes("from repo_context_event_links l") && sql.includes("order by l.event_id limit")) {
      const all = statement.all.bind(statement);
      (statement as any).all = (...args: any[]) => {
        const rows = all(...args);
        compete("fill_pending_event_links");
        return rows;
      };
    }
    return statement;
  };

  let deadlineError: string | null = null;
  let result: ReturnType<typeof runDeadlineMaintenanceStages> | null = null;
  try {
    result = runDeadlineMaintenanceStages(maintenance, {
      deadlineMs: 30_000,
      teardownMarginMs: 1_000,
      retentionDays: 90,
      parityReady: true,
      prune: (maxRows) => buffer.prune(90, { maxRows }),
    });
  } catch (error: any) {
    deadlineError = error.code ?? error.name;
  } finally {
    (maintenance as any).prepare = originalPrepare;
  }

  assert.equal(maintenance.inTransaction, false);
  assert.equal(capture.inTransaction, false);

  const remainingOutcomes = new Map<RemainingStage, InjectionOutcome>();
  let activeStage: RemainingStage | null = null;
  const competeRemaining = (stage: RemainingStage) => {
    if (activeStage !== stage || remainingOutcomes.has(stage)) return;
    try {
      capture.prepare("update fixture_capture_writer set n=n+1").run();
      remainingOutcomes.set(stage, "committed");
    } catch (error: any) {
      remainingOutcomes.set(stage, error.code ?? error.name);
    }
  };
  const probePrepare = maintenance.prepare.bind(maintenance);
  (maintenance as any).prepare = (sql: string) => {
    const statement = probePrepare(sql);
    if (
      sql.includes("from repo_context_results r") &&
      sql.includes("idx_repo_context_event_links_pending_context")
    ) {
      const all = statement.all.bind(statement);
      (statement as any).all = (...args: any[]) => {
        const rows = all(...args);
        competeRemaining("repo_context_fill");
        return rows;
      };
    }
    if (sql.includes("from codex_live_pins")) {
      const get = statement.get.bind(statement);
      (statement as any).get = (...args: any[]) => {
        const row = get(...args);
        competeRemaining("capture");
        return row;
      };
    }
    if (sql.includes("select schema_version as version") && sql.includes("automatic_capture_baseline_state")) {
      const get = statement.get.bind(statement);
      (statement as any).get = (...args: any[]) => {
        const row = get(...args);
        if (maintenance.inTransaction) competeRemaining("baseline_observation");
        return row;
      };
    }
    if (sql.includes("from sqlite_master") && sql.includes("automatic_capture_baseline_state")) {
      const get = statement.get.bind(statement);
      (statement as any).get = (...args: any[]) => {
        const row = get(...args);
        if (maintenance.inTransaction) competeRemaining("capture_inventory");
        return row;
      };
    }
    if (sql.includes("where length(file) != 64") && sql.includes("order by file limit")) {
      const all = statement.all.bind(statement);
      (statement as any).all = (...args: any[]) => {
        const rows = all(...args);
        if (maintenance.inTransaction) competeRemaining("jsonl_path_migration");
        return rows;
      };
    }
    if (sql.includes("select current_workspace_id as currentWorkspaceId") && sql.includes("previous_workspace_id")) {
      const get = statement.get.bind(statement);
      (statement as any).get = (...args: any[]) => {
        const row = get(...args);
        if (maintenance.inTransaction) competeRemaining("workspace_binding");
        return row;
      };
    }
    if (/from\s+collector_workspace_binding\s+where\s+singleton\s*=\s*1/.test(sql)) {
      const get = statement.get.bind(statement);
      (statement as any).get = (...args: any[]) => {
        const row = get(...args);
        if (maintenance.inTransaction) competeRemaining("finance_capture_activity");
        return row;
      };
    }
    return statement;
  };
  const projection = buffer.projection as any;
  const originalControl = projection.control.bind(projection);
  projection.control = (...args: any[]) => {
    const row = originalControl(...args);
    if (maintenance.inTransaction) competeRemaining("projection");
    return row;
  };
  const errors = new Map<RemainingStage, string | null>();
  const values = new Map<RemainingStage, unknown>();
  const probe = (stage: RemainingStage, run: () => unknown) => {
    activeStage = stage;
    try {
      values.set(stage, run());
      errors.set(stage, null);
    } catch (error: any) {
      errors.set(stage, error.code ?? error.name);
    } finally {
      activeStage = null;
    }
  };
  try {
    probe("projection", () => buffer.projection.runMaintenance(new Date(recent)));
    probe("repo_context_fill", () => buffer.drainRepoContextFills(1));
    const captureEvent = aiInteractionEventSchema.parse({
      id: "00000000-0000-4000-8000-000000000001",
      source: "codex",
      dataMode: "metadata",
      eventType: "usage_rollout",
      observedAt: recent,
      sessionId: "sqlite-lock-proof-session",
      inputTokens: 1,
      outputTokens: 1,
    });
    probe("capture", () =>
      buffer.transactionWithRepoContextHandoffs(() => buffer.append(captureEvent))
    );
    probe("baseline_observation", () =>
      stageAutomaticCaptureBaselineObservation(maintenance, "codex", {
        runId: baseline.latestRun!.runId,
        observedAt: recent,
        observation: {
          path: path.join(directory, "baseline-rollout.jsonl"),
          device: 1,
          inode: 1,
          size: 0,
          birthtimeNs: 1n,
        },
        filesDiscovered: 1,
        filesValidated: 1,
      })
    );
    probe("capture_inventory", () =>
      bindCaptureInventory(maintenance, "codex", [{
        rootId: "sqlite-lock-proof-root",
        profileId: "sqlite-lock-proof-profile",
        installationEpochId: "sqlite-lock-proof-epoch",
        source: "codex",
        directory,
      }])
    );
    probe("jsonl_path_migration", () => ensureJsonlScanState(maintenance));
    probe("workspace_binding", () => buffer.useWorkspace("local"));
    probe("finance_capture_activity", () => buffer.projection.recordCaptureActivity({
      source: "codex",
      lastActivityAt: null,
      filesToday: 0,
      discoveryEntries: 0,
      lastScanAt: new Date().toISOString(),
    }));
  } finally {
    projection.control = originalControl;
    (maintenance as any).prepare = probePrepare;
  }

  assert.deepEqual(Object.fromEntries(errors), {
    projection: null,
    repo_context_fill: null,
    capture: null,
    baseline_observation: null,
    capture_inventory: null,
    jsonl_path_migration: null,
    workspace_binding: null,
    finance_capture_activity: null,
  });
  assert.deepEqual(Object.fromEntries(remainingOutcomes), {
    projection: "SQLITE_BUSY",
    repo_context_fill: "SQLITE_BUSY",
    capture: "SQLITE_BUSY",
    baseline_observation: "SQLITE_BUSY",
    capture_inventory: "SQLITE_BUSY",
    jsonl_path_migration: "SQLITE_BUSY",
    workspace_binding: "SQLITE_BUSY",
    finance_capture_activity: "SQLITE_BUSY",
  });
  assert.equal(
    deadlineError,
    null,
    `deadline maintenance failed with ${deadlineError}: ${JSON.stringify(Object.fromEntries(outcomes))}`,
  );
  assert.deepEqual(Object.fromEntries(outcomes), {
    retention: "SQLITE_BUSY",
    fill_pending_event_links: "SQLITE_BUSY",
  });
  assert.deepEqual(result?.stages.map(({ stage }) => stage), [
    "wal_checkpoint",
    "retention",
    "fill_pending_event_links",
  ]);
  assert.equal(result?.stages.at(-1)?.rows, 256);
  assert.equal((values.get("repo_context_fill") as { rowsVisited: number }).rowsVisited, 1);
  assert.equal(values.get("capture"), true);
  assert.equal(values.get("baseline_observation"), true);
  assert.equal(values.get("capture_inventory"), true);
  assert.equal(values.get("jsonl_path_migration"), undefined);
  assert.equal(values.get("workspace_binding"), "local");
  assert.equal(
    maintenance.prepare("select 1 from rollout_scan_state where file = ?").pluck().get(
      jsonlScanStateKey(legacyPath),
    ),
    1,
  );
  assert.equal(maintenance.inTransaction, false);
  assert.equal(capture.inTransaction, false);
  capture.prepare("update fixture_capture_writer set n=n+1").run();

  console.log(JSON.stringify({
    proof: "sqlite_lock",
    passed: true,
    largePendingRows,
    legacyDeferredCode,
    deadlineStages: result?.stages.map(({ stage, rows }) => ({ stage, rows })),
    competingWritesDuringMaintenance: Object.fromEntries(outcomes),
    remainingWorkerTransactions: {
      errors: Object.fromEntries(errors),
      competingWrites: Object.fromEntries(remainingOutcomes),
    },
    checks: {
      walConnections: 2,
      staleSnapshotRaceReproduced: true,
      largeTableDeadlinePathExercised: true,
      retentionWriterClaimedBeforeRead: true,
      pendingFillWriterClaimedBeforeRead: true,
      projectionWriterClaimedBeforeRead: true,
      repoContextFillWriterClaimedBeforeRead: true,
      captureWriterClaimedBeforeRead: true,
      baselineObservationWriterClaimedBeforeRead: true,
      captureInventoryWriterClaimedBeforeRead: true,
      jsonlPathMigrationWriterClaimedBeforeRead: true,
      workspaceBindingWriterClaimedBeforeRead: true,
      financeCaptureActivityWriterClaimedBeforeRead: true,
      deadlineMaintenanceSucceeded: true,
      competingWriterSucceededAfterMaintenance: true,
    },
    scope: "Synthetic ledger only; models the collector parent as the competing WAL writer.",
  }, null, 2));
} finally {
  capture.close();
  buffer.close();
  fs.rmSync(directory, { recursive: true, force: true });
}
