/** Physical ledger reclaim. The caller owns the listener and must prove that
 * every collector process has exited before this module opens SQLite. */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import Database from "better-sqlite3";
import { DashboardProjectionStore } from "./dashboard-projection";

/** Static B13 inventory from plan-r6-checks/writer-modules.log. A process-level
 * quiesce closes the common ledger connection used by these modules. */
export const REQUIRED_REBUILD_WRITERS = [
  "dashboard-projection", "buffer", "outbox", "session-summary",
  "codex-reconciliation", "session-context-index", "codex-live-usage-ledger",
  "learning-facts", "capture-baseline", "outcome-timeline-store",
  "repo-context-replay-state", "history-coverage", "maintenance",
  "maintenance-stage-primitives", "repo-context-link-dispositions",
  "grok-usage-tailer", "capture-frontier", "capture-root-inventory",
  "jsonl-continuation", "session-sync", "capture-fairness",
  "jsonl-byte-tailer", "otlp-spool", "transcript-tailer",
  "account-assertion", "learning-materializer", "maintenance-starvation",
  "runtime-fact-drops", "privacy-disposition", "repo-context-drain",
  "rollout-tailer",
] as const;

export type RebuildStage = "S10" | "ABORT";
export type QuiesceReceipt = {
  modules: readonly string[];
  /** True only after the parent and both worker connections are closed. */
  connectionsClosed: boolean;
};
export type RebuildInput = {
  ledgerPath: string;
  stage: RebuildStage;
  /** Operator's WAL high-water receipt; preflight also reads the last 24 hours
   * of budget samples and the current WAL, and uses the greatest value. */
  walHighWaterBytes: number;
  /** Only copy proofs may substitute a synthetic quota. */
  freeBytes?: number;
  /** Copy proofs cannot claim a migration-stage receipt. */
  copyDrill?: boolean;
};
export type RebuildRunInput = RebuildInput & {
  quiesce: () => Promise<QuiesceReceipt>;
  resume: () => Promise<void>;
  /** Fault seam for the forced reopen drill. */
  reopen?: (ledgerPath: string) => void;
  /** Fault seam for the interrupted-copy drill. */
  afterVacuum?: () => void;
  /** Fault seam for a busy checkpoint refusal. */
  checkpoint?: (db: Database.Database) => Array<{ busy: number; log: number; checkpointed: number }>;
};
type RebuildPhase = "paused" | "vacuum" | "verified" | "swapped" |
  "resume_started" | "complete" | "failed" | "recovered";
type RebuildState = {
  version: 1;
  nonce: string;
  phase: RebuildPhase;
  stage: RebuildStage;
  backupPath: string;
  targetPath: string;
  startedAt: string;
  updatedAt: string;
  reason?: string;
};

function fail(code: string): never { throw new Error(code); }
function quoteIdentifier(name: string) { return `"${name.replaceAll('"', '""')}"`; }
function statePath(ledgerPath: string) { return `${ledgerPath}.maintenance-rebuild.json`; }
function lockPath(ledgerPath: string) { return `${ledgerPath}.maintenance-rebuild.lock`; }
function targetPath(ledgerPath: string) { return `${ledgerPath}.rebuild`; }
function headroomPath(ledgerPath: string) { return `${ledgerPath}.maintenance-rebuild-headroom.json`; }
function openLeaseDirectory(ledgerPath: string) { return `${ledgerPath}.rebuild-open-leases`; }
function sqliteSidecars(ledgerPath: string) { return [ledgerPath, `${ledgerPath}-wal`, `${ledgerPath}-shm`]; }
function fsyncDirectory(file: string) {
  const directory = fs.openSync(path.dirname(file), "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
}
function regularLedger(inputPath: string) {
  if (!path.isAbsolute(inputPath)) fail("ledger_path_must_be_absolute");
  const stat = fs.lstatSync(inputPath);
  if (!stat.isFile() || stat.isSymbolicLink()) fail("ledger_not_regular_file");
  if (fs.realpathSync(inputPath) !== inputPath) fail("ledger_path_not_canonical");
  return inputPath;
}
function readState(ledgerPath: string): RebuildState | null {
  try { return JSON.parse(fs.readFileSync(statePath(ledgerPath), "utf8")) as RebuildState; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    return fail("rebuild_state_unreadable");
  }
}
function writeState(ledgerPath: string, state: RebuildState) {
  const file = statePath(ledgerPath);
  const temporary = `${file}.${state.nonce}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(state)}\n`);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, file);
  fsyncDirectory(file);
}
function advance(ledgerPath: string, state: RebuildState, phase: RebuildPhase, reason?: string) {
  const next = { ...state, phase, updatedAt: new Date().toISOString(), ...(reason ? { reason } : {}) };
  writeState(ledgerPath, next);
  return next;
}
function hasTable(db: Database.Database, name: string) {
  return Boolean(db.prepare("select 1 from sqlite_master where type='table' and name=?").get(name));
}
export function assertRebuildWriterGateOpen(ledgerPath: string) {
  if (fs.existsSync(lockPath(ledgerPath))) fail("maintenance_rebuild_paused");
}
/** A token exists before a writer may open SQLite. A rebuild creates its gate
 * before scanning tokens; thus a delayed open cannot cross the final lsof. */
export function acquireRebuildOpenToken(ledgerPath: string) {
  if (ledgerPath === ":memory:") return null;
  const directory = openLeaseDirectory(ledgerPath);
  try { fs.mkdirSync(directory, { mode: 0o700 }); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error; }
  const stat = fs.lstatSync(directory);
  if (!stat.isDirectory() || stat.isSymbolicLink()) fail("writer_lease_directory_invalid");
  const token = path.join(directory, `${process.pid}.${randomUUID()}.lease`);
  const descriptor = fs.openSync(token, "wx", 0o600);
  try { fs.writeFileSync(descriptor, `${process.pid}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  try { assertRebuildWriterGateOpen(ledgerPath); }
  catch (error) { fs.unlinkSync(token); throw error; }
  return token;
}
export function releaseRebuildOpenToken(token: string | null) {
  if (token) removeIfExists(token);
}
function assertNoOpenTokens(ledgerPath: string) {
  const directory = openLeaseDirectory(ledgerPath);
  let entries: string[];
  try { entries = fs.readdirSync(directory); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    fail("writer_lease_check_unavailable");
  }
  if (entries.some((entry) => entry.endsWith(".lease"))) fail("writer_not_quiesced");
}
export function acquireRebuildWriterLeases(db: Database.Database, ledgerPath: string) {
  assertRebuildWriterGateOpen(ledgerPath);
  const nonce = randomUUID();
  const insert = db.prepare(`insert into maintenance_state(key,value,updated_at) values (?,?,?)`);
  const at = new Date().toISOString();
  db.transaction(() => {
    for (const module of REQUIRED_REBUILD_WRITERS) {
      insert.run(`rebuild_writer_lease:${nonce}:${module}`, JSON.stringify({ pid: process.pid, module }), at);
    }
  }).immediate();
  return nonce;
}
export function releaseRebuildWriterLeases(db: Database.Database, nonce: string) {
  db.prepare("delete from maintenance_state where key like ?")
    .run(`rebuild_writer_lease:${nonce}:%`);
}
export function readActiveRebuildWriterLeases(db: Database.Database) {
  if (!hasTable(db, "maintenance_state")) return [];
  const rows = db.prepare("select value from maintenance_state where key like 'rebuild_writer_lease:%' order by key")
    .all() as Array<{ value: string }>;
  return rows.map((row) => JSON.parse(row.value) as { pid: number; module: string });
}
function integrity(db: Database.Database) {
  const rows = db.prepare("PRAGMA integrity_check").all() as Array<Record<string, unknown>>;
  if (rows.length !== 1 || Object.values(rows[0] ?? {})[0] !== "ok") fail("integrity_check_failed");
}
function stageReady(db: Database.Database, stage: RebuildStage, copyDrill: boolean) {
  if (copyDrill) return;
  if (!hasTable(db, "maintenance_state")) fail("stage_not_ready");
  const row = db.prepare("select value from maintenance_state where key='lean_rebuild_stage'").get() as
    { value: string } | undefined;
  if (row?.value !== stage) fail("stage_not_ready");
}
function availableBytes(ledgerPath: string) {
  const stats = fs.statfsSync(path.dirname(ledgerPath), { bigint: true });
  return Number(stats.bavail * stats.bsize);
}
export type RebuildHeadroomStatus = {
  state: "insufficient_headroom";
  at: string;
  reclaimableBytes: number;
  requiredFreeBytes: number;
  availableFreeBytes: number;
  shortfallBytes: number;
  message: string;
};
export function readMaintenanceRebuildHeadroomStatus(ledgerPath: string): RebuildHeadroomStatus | null {
  const file = headroomPath(ledgerPath);
  try {
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) fail("headroom_status_unreadable");
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as RebuildHeadroomStatus;
    if (value.state !== "insufficient_headroom" || !Number.isSafeInteger(value.shortfallBytes)) {
      fail("headroom_status_unreadable");
    }
    return value;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}
function writeHeadroomStatus(ledgerPath: string, status: RebuildHeadroomStatus) {
  const file = headroomPath(ledgerPath);
  const temporary = `${file}.${randomUUID()}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeFileSync(descriptor, `${JSON.stringify(status)}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, file);
  fsyncDirectory(file);
}
function assertUnused(ledgerPath: string) {
  assertNoOpenTokens(ledgerPath);
  // An unknown lsof result is never interpreted as an empty lease set.
  const result = spawnSync("/usr/sbin/lsof", ["-n", "-P", "-t", "--", ...sqliteSidecars(ledgerPath)],
    { encoding: "utf8", timeout: 15_000 });
  if (result.error || ![0, 1].includes(result.status ?? -1)) fail("writer_lease_check_unavailable");
  if (result.stdout.trim()) fail("writer_not_quiesced");
}
function outboxPending(db: Database.Database) {
  if (!hasTable(db, "upload_outbox")) return 0;
  const row = db.prepare("select count(*) as n from upload_outbox where state not in ('acknowledged','dead')")
    .get() as { n: number };
  return row.n;
}
function measuredWalHighWater(db: Database.Database, ledgerPath: string, copyDrill: boolean) {
  let sampledBytes: number | null = null;
  if (hasTable(db, "budget_samples")) {
    const rows = db.prepare("select sample_json from budget_samples where at_ms>=? order by at_ms")
      .all(Date.now() - 86_400_000) as Array<{ sample_json: string }>;
    for (const row of rows) {
      let sample: { walBytes?: unknown };
      try { sample = JSON.parse(row.sample_json) as { walBytes?: unknown }; }
      catch { fail("wal_high_water_sample_unreadable"); }
      if (Number.isSafeInteger(sample.walBytes) && Number(sample.walBytes) >= 0) {
        sampledBytes = Math.max(sampledBytes ?? 0, Number(sample.walBytes));
      }
    }
  }
  if (!copyDrill && sampledBytes === null) fail("wal_high_water_unavailable");
  const currentWalBytes = fs.statSync(`${ledgerPath}-wal`, { throwIfNoEntry: false })?.size ?? 0;
  return { sampledBytes, currentWalBytes };
}

export function preflightMaintenanceRebuild(input: RebuildInput) {
  const ledgerPath = regularLedger(input.ledgerPath);
  if (!Number.isSafeInteger(input.walHighWaterBytes) || input.walHighWaterBytes < 0) fail("wal_high_water_required");
  if (input.freeBytes !== undefined && !input.copyDrill) fail("synthetic_quota_copy_only");
  if (fs.existsSync(lockPath(ledgerPath))) fail("rebuild_lease_held");
  const prior = readState(ledgerPath);
  if (prior && !["complete", "recovered", "failed"].includes(prior.phase)) fail("rebuild_recovery_required");
  if (fs.existsSync(targetPath(ledgerPath))) fail("rebuild_target_exists");
  const db = new Database(ledgerPath, { readonly: true, fileMustExist: true, timeout: 0 });
  try {
    stageReady(db, input.stage, input.copyDrill === true);
    integrity(db);
    const pageCount = Number(db.pragma("page_count", { simple: true }));
    const freePages = Number(db.pragma("freelist_count", { simple: true }));
    const pageSize = Number(db.pragma("page_size", { simple: true }));
    const liveBytes = (pageCount - freePages) * pageSize;
    const observedWal = measuredWalHighWater(db, ledgerPath, input.copyDrill === true);
    const walHighWaterBytes = Math.max(input.walHighWaterBytes,
      observedWal.sampledBytes ?? 0, observedWal.currentWalBytes);
    const requiredBytes = Math.ceil(liveBytes * 1.2 + walHighWaterBytes);
    const freeBytes = input.freeBytes ?? availableBytes(ledgerPath);
    if (!Number.isSafeInteger(freeBytes) || freeBytes < requiredBytes) {
      const shortfallBytes = Math.max(0, requiredBytes - freeBytes);
      writeHeadroomStatus(ledgerPath, { state: "insufficient_headroom", at: new Date().toISOString(),
        reclaimableBytes: freePages * pageSize, requiredFreeBytes: requiredBytes,
        availableFreeBytes: freeBytes, shortfallBytes,
        message: `${(freePages * pageSize / 1024 ** 3).toFixed(3)} GB reclaimable; needs ` +
          `${(requiredBytes / 1024 ** 3).toFixed(3)} GB free`,
      });
      fail(`insufficient_headroom:${shortfallBytes}`);
    }
    removeIfExists(headroomPath(ledgerPath));
    return { liveBytes, requiredBytes, freeBytes, outboxPending: outboxPending(db),
      reclaimableBytes: freePages * pageSize, walHighWaterBytes,
      sampledWalHighWaterBytes: observedWal.sampledBytes };
  } finally { db.close(); }
}

function logicalInventory(db: Database.Database) {
  const schema = db.prepare("select type,name,tbl_name,sql from sqlite_master order by type,name,tbl_name")
    .all() as Array<Record<string, unknown>>;
  const schemaHash = createHash("sha256").update(JSON.stringify(schema)).digest("hex");
  const names = db.prepare("select name from sqlite_master where type='table' order by name")
    .all() as Array<{ name: string }>;
  const counts = names.map(({ name }) => {
    const row = db.prepare(`select count(*) as n from ${quoteIdentifier(name)}`).get() as { n: number };
    return [name, row.n] as const;
  });
  const windows = [30, 90, 182, 365, 1825];
  if (!hasTable(db, "dashboard_snapshots")) fail("five_snapshots_missing");
  const snapshots = db.prepare("select * from dashboard_snapshots order by days")
    .all() as Array<{ days: number; payload_json: string }>;
  if (snapshots.length !== windows.length || snapshots.some((row, index) => row.days !== windows[index])) {
    fail("five_snapshots_missing");
  }
  if (!hasTable(db, "finance_publication_control")) fail("finance_publication_missing");
  const finance = db.prepare("select * from finance_publication_control order by singleton").all();
  return { schemaHash, userVersion: db.pragma("user_version", { simple: true }), counts,
    snapshots, finance };
}
function normalizeSnapshotCounters(value: Record<string, unknown>) {
  // The published snapshot was built before these two counters were advanced.
  // They are internal work statistics, not dashboard or finance content.
  const snapshot = structuredClone(value);
  for (const holder of [snapshot.projection, (snapshot.status as Record<string, unknown>)?.projection]) {
    const counters = (holder as Record<string, unknown> | undefined)?.counters as
      Record<string, unknown> | undefined;
    if (counters) { delete counters.snapshotRowsVisited; delete counters.snapshotBuilds; }
  }
  return snapshot;
}
function verifyRegeneratedSnapshots(source: Database.Database, rebuilt: Database.Database,
  copyDrill: boolean) {
  if (!hasTable(source, "dashboard_projection_control")) {
    if (!copyDrill) fail("snapshot_regeneration_unavailable");
    return;
  }
  const rows = source.prepare("select days,generation,created_at,payload_json from dashboard_snapshots order by days")
    .all() as Array<{ days: number; generation: number; created_at: string; payload_json: string }>;
  for (const row of rows) {
    const published = normalizeSnapshotCounters(JSON.parse(row.payload_json) as Record<string, unknown>);
    const generated = DashboardProjectionStore.regenerateForRebuildVerification(rebuilt,
      row.days, row.generation, new Date(row.created_at)) as Record<string, unknown>;
    if (JSON.stringify(normalizeSnapshotCounters(generated)) !== JSON.stringify(published)) {
      fail(`snapshot_regeneration_mismatch:${row.days}`);
    }
  }
}
function verifyCopy(source: string, rebuilt: string, copyDrill: boolean) {
  const old = new Database(source, { readonly: true, fileMustExist: true, timeout: 0 });
  const next = new Database(rebuilt, { readonly: true, fileMustExist: true, timeout: 0 });
  try {
    integrity(next);
    if (JSON.stringify(logicalInventory(old)) !== JSON.stringify(logicalInventory(next))) {
      fail("rebuild_logical_mismatch");
    }
    verifyRegeneratedSnapshots(old, next, copyDrill);
  } finally { next.close(); old.close(); }
}
function removeIfExists(file: string) {
  try { fs.unlinkSync(file); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}
function removeSqliteSidecars(ledgerPath: string) {
  removeIfExists(`${ledgerPath}-wal`);
  removeIfExists(`${ledgerPath}-shm`);
}

/** Only the paused, pre-resume state allows replacing the rebuilt file. */
export function renameBackBeforeResume(ledgerPath: string) {
  const state = readState(ledgerPath);
  if (!state || !["verified", "swapped"].includes(state.phase)) fail("forward_repair_only");
  assertUnused(ledgerPath);
  if (!fs.existsSync(state.backupPath)) fail("old_file_unavailable");
  removeSqliteSidecars(ledgerPath);
  if (fs.existsSync(ledgerPath)) removeIfExists(ledgerPath);
  fs.renameSync(state.backupPath, ledgerPath);
  fsyncDirectory(ledgerPath);
  removeIfExists(state.targetPath);
  const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  try { integrity(db); } finally { db.close(); }
  advance(ledgerPath, state, "failed", "rename_back");
  removeIfExists(lockPath(ledgerPath));
}

/** Recovery after SIGKILL before any writer resumed; never rewinds a resumed file. */
export function recoverInterruptedRebuild(ledgerPath: string) {
  const state = readState(ledgerPath);
  if (!state || ["resume_started", "complete"].includes(state.phase)) fail("forward_repair_only");
  assertUnused(ledgerPath);
  if (fs.existsSync(state.backupPath)) {
    removeSqliteSidecars(ledgerPath);
    if (fs.existsSync(ledgerPath)) removeIfExists(ledgerPath);
    fs.renameSync(state.backupPath, ledgerPath);
    fsyncDirectory(ledgerPath);
  }
  removeIfExists(state.targetPath);
  const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  try { integrity(db); } finally { db.close(); }
  removeIfExists(lockPath(ledgerPath));
  advance(ledgerPath, state, "recovered", "interrupted_before_resume");
  return { status: "recovered_untouched_source" as const };
}

export async function rebuildLedger(input: RebuildRunInput) {
  preflightMaintenanceRebuild(input);
  const ledgerPath = input.ledgerPath;
  const pausedAt = performance.now();
  const receipt = await input.quiesce();
  const missing = REQUIRED_REBUILD_WRITERS.filter((name) => !receipt.modules.includes(name));
  if (!receipt.connectionsClosed || missing.length) {
    await input.resume();
    fail(`writer_not_quiesced:${missing.join(",")}`);
  }
  let preflight: ReturnType<typeof preflightMaintenanceRebuild>;
  try { preflight = preflightMaintenanceRebuild(input); }
  catch (error) { await input.resume(); throw error; }
  let state: RebuildState | null = null;
  let resumeStarted = false;
  let ownsLock = false;
  try {
    const descriptor = fs.openSync(lockPath(ledgerPath), "wx", 0o600);
    ownsLock = true;
    try {
      fs.writeFileSync(descriptor, `${process.pid}\n`);
      fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
    assertUnused(ledgerPath);
    const leaseDb = new Database(ledgerPath, { readonly: true, fileMustExist: true });
    try { if (readActiveRebuildWriterLeases(leaseDb).length > 0) fail("writer_not_quiesced"); }
    finally { leaseDb.close(); }
    const nonce = randomUUID();
    const backupPath = `${ledgerPath}.pre-lean-${new Date().toISOString().slice(0, 10)}`;
    if (fs.existsSync(backupPath)) fail("old_file_already_exists");
    state = { version: 1, nonce, phase: "paused", stage: input.stage, backupPath,
      targetPath: targetPath(ledgerPath), startedAt: new Date().toISOString(), updatedAt: new Date().toISOString() };
    writeState(ledgerPath, state);
    const db = new Database(ledgerPath, { fileMustExist: true, timeout: 0 });
    try {
      db.pragma("locking_mode = EXCLUSIVE");
      db.exec("BEGIN EXCLUSIVE; COMMIT");
      const checkpoint = input.checkpoint?.(db) ?? db.pragma("wal_checkpoint(TRUNCATE)") as
        Array<{ busy: number; log: number; checkpointed: number }>;
      if (checkpoint[0]?.busy !== 0 || fs.statSync(`${ledgerPath}-wal`, { throwIfNoEntry: false })?.size) {
        fail("wal_not_empty_after_checkpoint");
      }
      state = advance(ledgerPath, state, "vacuum");
      db.exec(`VACUUM INTO '${state.targetPath.replaceAll("'", "''")}'`);
    } finally { db.close(); }
    input.afterVacuum?.();
    const targetDescriptor = fs.openSync(state.targetPath, "r");
    try { fs.fsyncSync(targetDescriptor); } finally { fs.closeSync(targetDescriptor); }
    fsyncDirectory(state.targetPath);
    verifyCopy(ledgerPath, state.targetPath, input.copyDrill === true);
    state = advance(ledgerPath, state, "verified");
    assertUnused(ledgerPath);
    fs.renameSync(ledgerPath, backupPath);
    fs.renameSync(state.targetPath, ledgerPath);
    fsyncDirectory(ledgerPath);
    state = advance(ledgerPath, state, "swapped");
    if (input.reopen) input.reopen(ledgerPath);
    else {
      const rebuilt = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      try { integrity(rebuilt); } finally { rebuilt.close(); }
    }
    state = advance(ledgerPath, state, "resume_started");
    resumeStarted = true;
    // From here the old file is forensic evidence. Let the new daemon open
    // the rebuilt path; the durable phase already forbids rename-back.
    removeIfExists(lockPath(ledgerPath));
    await input.resume();
    state = advance(ledgerPath, state, "complete");
    return { status: "rebuilt" as const, pauseMs: performance.now() - pausedAt,
      backupPath, ...preflight };
  } catch (error) {
    if (!resumeStarted && ownsLock) {
      if (state && fs.existsSync(state.backupPath)) {
        removeSqliteSidecars(ledgerPath);
        if (fs.existsSync(ledgerPath)) removeIfExists(ledgerPath);
        fs.renameSync(state.backupPath, ledgerPath);
        fsyncDirectory(ledgerPath);
      }
      removeIfExists(targetPath(ledgerPath));
      if (state) advance(ledgerPath, state, "failed", error instanceof Error ? error.message : "unknown");
      removeIfExists(lockPath(ledgerPath));
      await input.resume();
    }
    throw error;
  } finally {
    if (ownsLock && (!state || !["vacuum", "verified", "swapped"].includes(readState(ledgerPath)?.phase ?? ""))) {
      removeIfExists(lockPath(ledgerPath));
    }
  }
}
