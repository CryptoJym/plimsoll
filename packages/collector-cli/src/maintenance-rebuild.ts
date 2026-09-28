/** Physical ledger reclaim. The caller owns the listener and must prove that
 * every collector process has exited before this module opens SQLite. */
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";
import Database from "better-sqlite3";
import { DashboardProjectionStore } from "./dashboard-projection";
import { assertNoRebuildOpenTokens, assertRebuildWriterGateOpen, rebuildLockPath,
  rebuildResumeClaimPath, openRebuildFencedDatabase, retireDeadRebuildOpenTokens } from "./rebuild-open-gate";
import { currentRebuildWriterIdentity, rebuildWriterIdentityLiveness } from "./rebuild-writer-identity";
import { withRebuildCoordination } from "./rebuild-coordination";
import { refreshMaintenanceRebuildPauseHighWater } from "./maintenance-rebuild-pause-state";
export { acquireRebuildOpenToken, releaseRebuildOpenToken } from "./rebuild-open-gate";

/** Static B13 inventory for coverage reporting. It is never a quiesce receipt. */
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
  before: ConnectionOwnership;
  after: ConnectionOwnership;
  connectionsClosed: boolean;
};
export type ConnectionOwnership = {
  openTokens: Array<{ pid: number | null; token: string }>;
  sqlitePids: number[];
  writerLeases: Array<{ pid: number; owner: string }>;
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
  /** Fault seam for a delayed independent opener after verification. */
  beforeSwap?: () => void | Promise<void>;
  /** Copy-only fault seam for a real SIGKILL between the two renames. */
  afterFirstRename?: () => void | Promise<void>;
  /** Fault seam for a busy checkpoint refusal. */
  checkpoint?: (db: Database.Database) => Array<{ busy: number; log: number; checkpointed: number }>;
  /** Copy-only seam after the coordination transaction publishes the fence. */
  afterLockPublished?: () => void | Promise<void>;
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
const lockPath = rebuildLockPath;
function targetPath(ledgerPath: string) { return `${ledgerPath}.rebuild`; }
function headroomPath(ledgerPath: string) { return `${ledgerPath}.maintenance-rebuild-headroom.json`; }
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
/** Recovery must accept a missing basename in the first-rename crash gap. */
export function canonicalRecoveryLedgerPath(inputPath: string) {
  if (!path.isAbsolute(inputPath) || path.normalize(inputPath) !== inputPath ||
    path.basename(inputPath) === "." || path.basename(inputPath) === "..") fail("ledger_path_not_canonical");
  const parent = path.dirname(inputPath);
  if (fs.realpathSync(parent) !== parent) fail("ledger_path_not_canonical");
  const inspect = (file: string, reason: string) => {
    try {
      const stat = fs.lstatSync(file);
      if (!stat.isFile() || stat.isSymbolicLink()) fail(reason);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
    }
  };
  inspect(inputPath, "ledger_not_regular_file");
  for (const sibling of [statePath(inputPath), lockPath(inputPath), targetPath(inputPath)]) {
    inspect(sibling, "rebuild_state_path_invalid");
  }
  return inputPath;
}
function validatedRecoveryState(ledgerPath: string) {
  canonicalRecoveryLedgerPath(ledgerPath);
  const state = readState(ledgerPath);
  if (!state) return null;
  const backupPrefix = `${ledgerPath}.pre-lean-`;
  const backupName = typeof state.backupPath === "string" && state.backupPath.startsWith(backupPrefix)
    ? state.backupPath.slice(backupPrefix.length) : "";
  // Old in-flight states used a date-only backup; new states bind the backup
  // basename to their durable nonce so another same-day rebuild can retain it.
  const legacyBackup = /^\d{4}-\d{2}-\d{2}$/.test(backupName);
  const nonceBackup = typeof state.startedAt === "string" &&
    backupName === `${state.startedAt.slice(0, 10)}-${state.nonce}`;
  if (state.version !== 1 || !/^[0-9a-f-]{36}$/i.test(state.nonce) ||
    state.targetPath !== targetPath(ledgerPath) ||
    !(legacyBackup || (nonceBackup && /^\d{4}-\d{2}-\d{2}-[0-9a-f-]{36}$/i.test(backupName)))) {
    fail("rebuild_state_path_invalid");
  }
  try {
    const stat = fs.lstatSync(state.backupPath);
    if (!stat.isFile() || stat.isSymbolicLink()) fail("rebuild_backup_invalid");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  return state;
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
  // A killed write can leave its temp behind. Every later state transition,
  // including recovery of the same nonce, needs an exclusive fresh name.
  const temporary = `${file}.${state.nonce}.${randomUUID()}.tmp`;
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
export function acquireRebuildWriterLeases(db: Database.Database, ledgerPath: string) {
  assertRebuildWriterGateOpen(ledgerPath);
  const nonce = randomUUID();
  db.prepare(`insert into maintenance_state(key,value,updated_at) values (?,?,?)`)
    .run(`rebuild_writer_lease:${nonce}`, JSON.stringify({ ...currentRebuildWriterIdentity(),
      owner: "local_event_buffer" }),
      new Date().toISOString());
  return nonce;
}
export function releaseRebuildWriterLeases(db: Database.Database, nonce: string) {
  // A collector connection can use timeout 0 for request admission. Shutdown
  // still has to wait briefly for an independent writer's transaction so its
  // durable owner lease is removed before the connection closes.
  db.pragma("busy_timeout = 1000");
  db.prepare("delete from maintenance_state where key=?").run(`rebuild_writer_lease:${nonce}`);
}
export function readActiveRebuildWriterLeases(db: Database.Database) {
  if (!hasTable(db, "maintenance_state")) return [];
  const rows = db.prepare("select value from maintenance_state where key like 'rebuild_writer_lease:%' order by key")
    .all() as Array<{ value: string }>;
  return rows.map((row) => {
    const value = JSON.parse(row.value) as { pid: number; owner: string };
    return { pid: value.pid, owner: value.owner };
  });
}

/** Delete only identity-confirmed dead rows, with a value comparison so a
 * concurrent replacement can never be retired by a stale observation. */
export function retireDeadRebuildWriterLeases(ledgerPath: string) {
  const db = openRebuildFencedDatabase(ledgerPath, { fileMustExist: true, timeout: 0 });
  try {
    if (!hasTable(db, "maintenance_state")) return 0;
    const rows = db.prepare(`select key,value from maintenance_state
      where key like 'rebuild_writer_lease:%' order by key`).all() as Array<{ key: string; value: string }>;
    const stale = rows.filter((row) => {
      try { return rebuildWriterIdentityLiveness(JSON.parse(row.value)) === "stale"; }
      catch { return false; }
    });
    if (stale.length === 0) return 0;
    const remove = db.prepare("delete from maintenance_state where key=? and value=?");
    return db.transaction(() => stale.reduce((count, row) =>
      count + remove.run(row.key, row.value).changes, 0))();
  } finally { db.close(); }
}
/** Native observation, captured before and after daemon unload and again
 * under the rebuild fence. No inventory name is presented as an open handle. */
export function observeRebuildConnectionOwnership(ledgerPath: string): ConnectionOwnership {
  let entries: string[];
  try { entries = fs.readdirSync(`${ledgerPath}.rebuild-open-leases`); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") fail("writer_lease_check_unavailable");
    entries = [];
  }
  const openTokens = entries.filter((entry) => entry.endsWith(".lease")).map((entry) => ({
    pid: /^\d+\./.test(entry) ? Number(entry.slice(0, entry.indexOf("."))) : null,
    token: entry,
  }));
  let writerLeases: Array<{ pid: number; owner: string }> = [];
  if (fs.existsSync(ledgerPath)) {
    const db = new Database(ledgerPath, { readonly: true, fileMustExist: true, timeout: 0 });
    try { writerLeases = readActiveRebuildWriterLeases(db); } finally { db.close(); }
  }
  const result = spawnSync("/usr/sbin/lsof", ["-n", "-P", "-t", "--", ...sqliteSidecars(ledgerPath)],
    { encoding: "utf8", timeout: 15_000 });
  if (result.error || ![0, 1].includes(result.status ?? -1)) fail("writer_lease_check_unavailable");
  const sqlitePids = [...new Set(result.stdout.trim().split(/\s+/).filter(Boolean).map(Number))];
  return { openTokens, sqlitePids, writerLeases };
}
export function connectionOwnershipClosed(observed: ConnectionOwnership) {
  return observed.openTokens.length === 0 && observed.sqlitePids.length === 0 && observed.writerLeases.length === 0;
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
  assertNoRebuildOpenTokens(ledgerPath);
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
function verifyCopy(old: Database.Database, rebuilt: string, copyDrill: boolean) {
  const next = new Database(rebuilt, { readonly: true, fileMustExist: true, timeout: 0 });
  try {
    integrity(next);
    if (JSON.stringify(logicalInventory(old)) !== JSON.stringify(logicalInventory(next))) {
      fail("rebuild_logical_mismatch");
    }
    verifyRegeneratedSnapshots(old, next, copyDrill);
  } finally { next.close(); }
}
function assertExclusiveSourceOwner(db: Database.Database, ledgerPath: string) {
  assertNoRebuildOpenTokens(ledgerPath);
  if (db.pragma("locking_mode", { simple: true }) !== "exclusive") fail("writer_not_quiesced");
  const result = spawnSync("/usr/sbin/lsof", ["-n", "-P", "-t", "--", ...sqliteSidecars(ledgerPath)],
    { encoding: "utf8", timeout: 15_000 });
  if (result.error || ![0, 1].includes(result.status ?? -1)) fail("writer_lease_check_unavailable");
  if (result.stdout.trim().split(/\s+/).filter(Boolean).some((pid) => Number(pid) !== process.pid)) {
    fail("writer_not_quiesced");
  }
}
function removeIfExists(file: string) {
  try { fs.unlinkSync(file); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}
/** The hard-linked fence is published only after its owner identity is durable.
 * A legacy PID-only lock is recoverable only if that PID no longer exists. */
type LockObservation = { raw: string; dev: number; ino: number };
function observeLock(ledgerPath: string): LockObservation {
  const file = lockPath(ledgerPath);
  let raw: string;
  let descriptor: number;
  try {
    descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") fail("rebuild_lock_missing");
    return fail("rebuild_owner_unverified");
  }
  let stat: fs.Stats;
  try {
    stat = fs.fstatSync(descriptor);
    if (!stat.isFile() || stat.size > 4096) fail("rebuild_owner_unverified");
    raw = fs.readFileSync(descriptor, "utf8");
  } finally { fs.closeSync(descriptor); }
  return { raw, dev: stat.dev, ino: stat.ino };
}
function staleRecoveryLock(ledgerPath: string): LockObservation {
  const observed = observeLock(ledgerPath);
  let identity: unknown;
  if (/^[1-9]\d*\n?$/.test(observed.raw)) identity = { pid: Number(observed.raw.trim()) };
  else {
    try { identity = JSON.parse(observed.raw); }
    catch { fail("rebuild_owner_unverified"); }
    if (!identity || typeof identity !== "object" || Array.isArray(identity) ||
      !("processStartFingerprint" in identity) ||
      !("processStartFingerprintAlgorithm" in identity)) fail("rebuild_owner_unverified");
  }
  const liveness = rebuildWriterIdentityLiveness(identity);
  if (liveness === "live") fail("rebuild_owner_active");
  if (liveness !== "stale") fail("rebuild_owner_unverified");
  return observed;
}
function sameLock(left: LockObservation, right: LockObservation) {
  return left.raw === right.raw && left.dev === right.dev && left.ino === right.ino;
}
function removeStaleRecoveryLock(ledgerPath: string, observed: LockObservation) {
  // A second recovery must not remove a lock whose owner changed while this
  // recovery verified SQLite. Revalidate inode, contents and process identity.
  if (!sameLock(staleRecoveryLock(ledgerPath), observed)) fail("rebuild_owner_changed");
  fs.unlinkSync(lockPath(ledgerPath));
  fsyncDirectory(lockPath(ledgerPath));
}
function removeOwnedRebuildLock(ledgerPath: string, observed: LockObservation) {
  // Never let a failed run's later cleanup unlink a successor's fence.
  let current: LockObservation;
  try { current = observeLock(ledgerPath); }
  catch (error) {
    if (error instanceof Error && error.message === "rebuild_lock_missing") return false;
    throw error;
  }
  if (!sameLock(current, observed)) return false;
  fs.unlinkSync(lockPath(ledgerPath));
  fsyncDirectory(lockPath(ledgerPath));
  return true;
}
function removeSqliteSidecars(ledgerPath: string) {
  removeIfExists(`${ledgerPath}-wal`);
  removeIfExists(`${ledgerPath}-shm`);
}

/** Only the paused, pre-resume state allows replacing the rebuilt file. */
export function renameBackBeforeResume(ledgerPath: string) {
  return withRebuildCoordination(ledgerPath, () => renameBackBeforeResumeLocked(ledgerPath));
}
function renameBackBeforeResumeLocked(ledgerPath: string) {
  const state = validatedRecoveryState(ledgerPath);
  if (!state || !["verified", "swapped"].includes(state.phase)) fail("forward_repair_only");
  const staleLock = staleRecoveryLock(ledgerPath);
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
  removeStaleRecoveryLock(ledgerPath, staleLock);
}

/** Recovery after SIGKILL before any writer resumed; never rewinds a resumed file. */
export function recoverInterruptedRebuild(ledgerPath: string) {
  return withRebuildCoordination(ledgerPath, () => recoverInterruptedRebuildLocked(ledgerPath));
}
function recoverInterruptedRebuildLocked(ledgerPath: string) {
  const state = validatedRecoveryState(ledgerPath);
  if (!fs.existsSync(lockPath(ledgerPath))) fail("forward_repair_only");
  const staleLock = staleRecoveryLock(ledgerPath);
  if (!state) {
    // Lock creation precedes the first durable state write. With no target or
    // state there has been no checkpoint, swap or resume to reverse.
    if (!fs.existsSync(ledgerPath) || fs.existsSync(targetPath(ledgerPath))) fail("rebuild_state_unavailable");
    assertUnused(ledgerPath);
    const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
    try { integrity(db); } finally { db.close(); }
    removeStaleRecoveryLock(ledgerPath, staleLock);
    return { status: "recovered_stale_lock" as const };
  }
  if (["complete", "failed", "recovered"].includes(state.phase)) {
    // The replacement was verified and resumed, or rollback/recovery was
    // finalized before its lock unlink. There is no rename left to perform.
    const db = new Database(ledgerPath, { readonly: true, fileMustExist: true });
    try { integrity(db); } finally { db.close(); }
    if (state.phase === "complete") removeIfExists(rebuildResumeClaimPath(ledgerPath, state.nonce));
    removeStaleRecoveryLock(ledgerPath, staleLock);
    return { status: state.phase === "complete" ? "recovered_completed_rebuild" as const :
      "recovered_untouched_source" as const };
  }
  if (state.phase === "resume_started") fail("forward_repair_only");
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
  advance(ledgerPath, state, "recovered", "interrupted_before_resume");
  removeStaleRecoveryLock(ledgerPath, staleLock);
  return { status: "recovered_untouched_source" as const };
}

export async function rebuildLedger(input: RebuildRunInput) {
  preflightMaintenanceRebuild(input);
  const ledgerPath = input.ledgerPath;
  retireDeadRebuildOpenTokens(ledgerPath);
  retireDeadRebuildWriterLeases(ledgerPath);
  const pausedAt = performance.now();
  const receipt = await input.quiesce();
  if (!receipt.connectionsClosed || !connectionOwnershipClosed(receipt.after)) {
    await input.resume();
    fail("writer_not_quiesced");
  }
  let preflight: ReturnType<typeof preflightMaintenanceRebuild>;
  try {
    preflight = preflightMaintenanceRebuild(input);
    refreshMaintenanceRebuildPauseHighWater(ledgerPath);
  }
  catch (error) { await input.resume(); throw error; }
  let state: RebuildState | null = null;
  let resumeStarted = false;
  let ownsLock = false;
  let ownedLock: LockObservation | null = null;
  const retireOwnedLock = () => {
    if (!ownedLock) return true;
    const observed = ownedLock;
    ownedLock = null; // A failed run must never make a second unlink attempt.
    return withRebuildCoordination(ledgerPath, () => removeOwnedRebuildLock(ledgerPath, observed));
  };
  try {
    const stagingLock = `${lockPath(ledgerPath)}.${randomUUID()}.tmp`;
    const descriptor = fs.openSync(stagingLock, "wx", 0o600);
    const contents = `${JSON.stringify(currentRebuildWriterIdentity())}\n`;
    let stagedLock!: LockObservation;
    try {
      fs.writeFileSync(descriptor, contents);
      fs.fsyncSync(descriptor);
      const stat = fs.fstatSync(descriptor);
      stagedLock = { raw: contents, dev: stat.dev, ino: stat.ino };
    } finally { fs.closeSync(descriptor); }
    try {
      withRebuildCoordination(ledgerPath, () => {
        fs.linkSync(stagingLock, lockPath(ledgerPath));
        ownsLock = true;
        ownedLock = stagedLock;
        fsyncDirectory(lockPath(ledgerPath));
      });
    } finally { removeIfExists(stagingLock); }
    await input.afterLockPublished?.();
    assertUnused(ledgerPath);
    const fencedOwnership = observeRebuildConnectionOwnership(ledgerPath);
    if (!connectionOwnershipClosed(fencedOwnership)) fail("writer_not_quiesced");
    const leaseDb = new Database(ledgerPath, { readonly: true, fileMustExist: true });
    try { if (readActiveRebuildWriterLeases(leaseDb).length > 0) fail("writer_not_quiesced"); }
    finally { leaseDb.close(); }
    const nonce = randomUUID();
    const startedAt = new Date().toISOString();
    const backupPath = `${ledgerPath}.pre-lean-${startedAt.slice(0, 10)}-${nonce}`;
    if (fs.existsSync(backupPath)) fail("old_file_already_exists");
    state = { version: 1, nonce, phase: "paused", stage: input.stage, backupPath,
      targetPath: targetPath(ledgerPath), startedAt, updatedAt: startedAt };
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
      input.afterVacuum?.();
      const targetDescriptor = fs.openSync(state.targetPath, "r");
      try { fs.fsyncSync(targetDescriptor); } finally { fs.closeSync(targetDescriptor); }
      fsyncDirectory(state.targetPath);
      verifyCopy(db, state.targetPath, input.copyDrill === true);
      state = advance(ledgerPath, state, "verified");
      // Keep SQLite's exclusive source lock through both renames. It also
      // rejects a raw SQLite opener that does not participate in our token
      // protocol at the reviewer's verify-to-rename boundary.
      assertExclusiveSourceOwner(db, ledgerPath);
      await input.beforeSwap?.();
      fs.renameSync(ledgerPath, backupPath);
      await input.afterFirstRename?.();
      fs.renameSync(state.targetPath, ledgerPath);
      fsyncDirectory(ledgerPath);
    } finally { db.close(); }
    state = advance(ledgerPath, state, "swapped");
    if (input.reopen) input.reopen(ledgerPath);
    else {
      const rebuilt = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      try { integrity(rebuilt); } finally { rebuilt.close(); }
    }
    state = advance(ledgerPath, state, "resume_started");
    resumeStarted = true;
    // The old file is forensic evidence. Only the resumed daemon's start
    // command may open the verified replacement while this fence remains.
    await input.resume();
    state = advance(ledgerPath, state, "complete");
    if (!retireOwnedLock()) fail("rebuild_owner_changed");
    removeIfExists(rebuildResumeClaimPath(ledgerPath, state.nonce));
    return { status: "rebuilt" as const, pauseMs: performance.now() - pausedAt,
      backupPath, quiesce: { ...receipt, fencedOwnership }, ...preflight };
  } catch (error) {
    if (!resumeStarted) {
      if (ownsLock) {
        if (state && fs.existsSync(state.backupPath)) {
          removeSqliteSidecars(ledgerPath);
          if (fs.existsSync(ledgerPath)) removeIfExists(ledgerPath);
          fs.renameSync(state.backupPath, ledgerPath);
          fsyncDirectory(ledgerPath);
        }
        removeIfExists(targetPath(ledgerPath));
        if (state) advance(ledgerPath, state, "failed", error instanceof Error ? error.message : "unknown");
        retireOwnedLock();
      }
      // Quiesce has already unloaded the daemon even when a competing rebuild
      // wins the lock race. Resume ours without touching that owner's lock.
      await input.resume();
    }
    throw error;
  }
}
