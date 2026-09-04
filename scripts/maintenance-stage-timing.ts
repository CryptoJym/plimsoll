import { execFileSync } from "node:child_process";
import { existsSync, mkdirSync, statSync, unlinkSync } from "node:fs";
import { homedir } from "node:os";
import { basename, join } from "node:path";
import Database from "better-sqlite3";
import {
  ensureMaintenanceStageSchema,
  runEnrichmentStage,
  runGitContextStage,
  runPendingEventLinkFillStage,
  runRetentionDeletionStage,
  runWalCheckpointStage,
} from "../packages/collector-cli/src/maintenance-stage-primitives";

const source = process.argv.slice(2).find((argument) => argument !== "--");
if (!source) throw new Error("usage: pnpm timing:maintenance-stages -- /absolute/path/to/work-ledger.sqlite");
const sourceBytes = statSync(source).size;
const scratch = join(homedir(), "Projects", "plimsoll-lanes", "scratch");
mkdirSync(scratch, { recursive: true });
const freeKb = Number(execFileSync("df", ["-Pk", scratch], { encoding: "utf8" }).trim().split(/\s+/).at(-3));
const freeBytes = freeKb * 1024;
const requiredRemainingBytes = 60 * 1024 ** 3;
if (!Number.isFinite(freeBytes) || freeBytes - sourceBytes < requiredRemainingBytes) {
  throw new Error(`maintenance_timing_insufficient_space:${freeBytes}:${sourceBytes}`);
}

const copy = join(scratch, `plimsoll-maintenance-timing-${process.pid}-${basename(source)}`);
// Hold one short read snapshot while `cp` captures the main file and WAL. This
// prevents a checkpoint from changing the main database between those copies;
// the connection is closed immediately afterward so it cannot pin the WAL.
const sourceReader = new Database(source, { readonly: true, fileMustExist: true });
try {
  sourceReader.pragma("query_only = on");
  sourceReader.exec("begin");
  sourceReader.prepare(`select count(*) as n from sqlite_schema`).get();
  execFileSync("cp", [source, copy], { stdio: "inherit" });
  if (existsSync(source + "-wal")) execFileSync("cp", [source + "-wal", copy + "-wal"], { stdio: "inherit" });
  sourceReader.exec("commit");
} finally {
  sourceReader.close();
}
let database: Database.Database | null = null;
try {
  database = new Database(copy);
  database.prepare(`select count(*) as n from sqlite_schema`).get();
  database.pragma("journal_mode = WAL");
  ensureMaintenanceStageSchema(database);
  const batchSize = Math.max(1, Math.min(Number(process.env.PLIMSOLL_STAGE_BATCH_SIZE ?? 256), 1_000));
  const remainingMs = Math.max(1, Math.min(Number(process.env.PLIMSOLL_STAGE_BUDGET_MS ?? 2_000), 10_000));
  const retentionDays = Math.max(1, Number(process.env.PLIMSOLL_RETENTION_DAYS ?? 90));
  const readings = {
    sourceBytes,
    batchSize,
    remainingMs,
    retention: runRetentionDeletionStage(database, {
      remainingMs, batchSize, retentionDays, parityReady: true,
    }),
    pendingEventLinkFill: runPendingEventLinkFillStage(database, { remainingMs, batchSize }),
    enrichment: runEnrichmentStage(database, { remainingMs, batchSize }),
    gitContext: runGitContextStage(database, {
      remainingMs, batchSize,
      resolve: () => { throw new Error("timing_copy_contains_unexpected_git_request"); },
      commit: () => undefined,
    }),
    walCheckpoint: runWalCheckpointStage(database, { remainingMs, batchSize }),
  };
  const measuredStages = {
    retention: readings.retention,
    pendingEventLinkFill: readings.pendingEventLinkFill,
    enrichment: readings.enrichment,
    gitContext: readings.gitContext,
    walCheckpoint: readings.walCheckpoint,
  };
  const rates = Object.fromEntries(Object.entries(measuredStages).map(([stage, reading]) => [stage, {
      activeRowsPerMinute: reading.ms > 0 ? reading.rows * 60_000 / reading.ms : null,
      cadenceRowsPerMinute: reading.rows / 5,
    }]));
  console.log(JSON.stringify({ timing: "maintenance_stage_batches", copyOnly: true, scratch, rates, ...readings }));
} finally {
  database?.close();
  for (const suffix of ["", "-wal", "-shm"]) {
    try { unlinkSync(copy + suffix); } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  }
}
