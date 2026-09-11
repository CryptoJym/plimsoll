/** Deadline maintenance versus capture writer, real WAL connections, deterministic interleaving. */
import assert from "node:assert/strict";
import fs from "node:fs";
import { createRequire } from "node:module";
import os from "node:os";
import path from "node:path";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { runDeadlineMaintenanceStages } from "../packages/collector-cli/src/maintenance-stage-primitives";

const require = createRequire(path.resolve("package.json"));
const Database = require("better-sqlite3");
const directory = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-sqlite-lock-proof-"));
const ledgerPath = path.join(directory, "ledger.sqlite");
const buffer = new LocalEventBuffer(ledgerPath, { databaseBusyTimeoutMs: 900 });
const maintenance = buffer.database;
const capture = new Database(ledgerPath, { timeout: 0 });
const largePendingRows = 4_096;

type InjectionStage = "retention" | "fill_pending_event_links";
type InjectionOutcome = "committed" | string;

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
  assert.equal(maintenance.inTransaction, false);
  assert.equal(capture.inTransaction, false);
  assert.equal(capture.prepare("select n from fixture_capture_writer").pluck().get(), 1);
  capture.prepare("update fixture_capture_writer set n=n+1").run();
  assert.equal(capture.prepare("select n from fixture_capture_writer").pluck().get(), 2);

  console.log(JSON.stringify({
    proof: "sqlite_lock",
    passed: true,
    largePendingRows,
    legacyDeferredCode,
    deadlineStages: result?.stages.map(({ stage, rows }) => ({ stage, rows })),
    competingWritesDuringMaintenance: Object.fromEntries(outcomes),
    checks: {
      walConnections: 2,
      staleSnapshotRaceReproduced: true,
      largeTableDeadlinePathExercised: true,
      retentionWriterClaimedBeforeRead: true,
      pendingFillWriterClaimedBeforeRead: true,
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
