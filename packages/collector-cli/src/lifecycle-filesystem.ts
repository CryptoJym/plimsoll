import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import {
  LIFECYCLE_RETAINED_SNAPSHOTS,
  LIFECYCLE_SCHEMA_VERSION,
  LifecycleInterruption,
  PURGE_CONFIRMATION,
  immutableRuntimeRelativePath,
  parseCompletionReceipt,
  planLifecycleRetention,
  validRestoreRecord,
  type LifecycleAdapter,
  type LifecycleCloneFallback,
  type LifecycleCompletedOperation,
  type LifecycleJournal,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type LifecycleReconcileRecord,
  type LifecycleRemovedItem,
  type LifecycleRestoreRecord,
  type LifecycleRetentionInput,
  type LifecycleRetentionPlan,
  type LifecycleRetentionRecord,
  type LifecycleSnapshotInventory,
  type LifecycleSnapshotMethod,
  type LifecycleSnapshotPlan,
  type LifecycleSnapshotRecord,
  type LifecycleSupportSnapshot,
  type RuntimeArtifact,
} from "./lifecycle";
import { LifecycleMutationAuthority, type LifecycleMutationLease } from "./lifecycle-authority";
import { isStatusSummaryTempFile } from "./status-summary";

const FILE_MODE = 0o600;
const DIRECTORY_MODE = 0o700;
const EXECUTABLE_MODE = 0o700;
/** A completion receipt, including a retention record naming thousands of removals. */
const MAX_MARKER_BYTES = 4 * 1024 * 1024;
const MAX_MANIFEST_BYTES = 256 * 1024;
const MAX_TREE_ENTRIES = 200_000;
/** Trash entries are `<kind>+<name>+<nonce>`; `+` never occurs in an identifier. */
const TRASH_SEPARATOR = "+";
const CLONE_FALLBACKS: readonly LifecycleCloneFallback[] = [
  "ledger_in_use", "ledger_not_wal", "wal_not_empty", "quiescence_unproven", "clone_unsupported",
];

export type ManagedLifecyclePaths = {
  lifecycleRoot: string;
  ownershipRoot: string;
  artifactSourceRoot: string;
  collectorConfig: string;
  database: string;
  serviceManifest: string;
  ownedToolFragments: readonly string[];
  history: readonly string[];
  /** The daemon's status-summary.json (eco-6hoxj.163.34): usage counters and its run's /healthz key. */
  statusSummary: string;
};

export type LifecycleServiceAdapter = {
  activate(input: { executablePath: string; version: string }): Promise<void>;
  restore(input: { executablePath: string | null; version: string | null }): Promise<void>;
  remove(): Promise<void>;
  readiness(expectedVersion: string, input: { signal: AbortSignal; deadlineMs: number }): Promise<LifecycleReadiness>;
  supportSnapshot(): Promise<LifecycleSupportSnapshot>;
};

export type LifecycleDatabaseSnapshot = {
  present: boolean;
  method: LifecycleSnapshotMethod | null;
  quiesced: boolean;
  cloneFallback: LifecycleCloneFallback | null;
};

export type LifecycleDatabaseRestore = {
  method: "clone" | "copy";
  cloneFallback: "clone_unsupported" | null;
  databaseBytes: number;
  /** "preexisting_damage": the restored copy fails integrity_check only in ways the live ledger already does. */
  integrity?: "ok" | "preexisting_damage";
};

/**
 * The operation's mutation fence, for database steps that can outlast the
 * lease. Both throw LifecycleInterruption once the fence is lost; a caller
 * must then stop without changing anything further.
 */
export type LifecycleFenceGuard = {
  /** Extends the lease while it is still current; call at least every `keepAliveIntervalMs`. */
  keepAlive(): void;
  keepAliveIntervalMs: number;
  /** Revalidates the fence; call immediately before replacing or removing the live ledger. */
  assertCurrent(): void;
};

/** SQLite implementations must use the online backup API or an equivalent
 * quiesced snapshot. Copying a live WAL database is not a compatible backup.
 * A bare boolean result (present or absent) records no snapshot method.
 * Restore must leave the live ledger untouched unless the restored copy is
 * complete and valid; it may report how the copy was made. */
export type LifecycleDatabaseAdapter = {
  snapshot(input: { source: string; destination: string; guard?: LifecycleFenceGuard }): Promise<boolean | LifecycleDatabaseSnapshot>;
  restore(input: { source: string; destination: string; guard?: LifecycleFenceGuard }): Promise<void | LifecycleDatabaseRestore>;
  /**
   * Removes the live ledger for a snapshot taken when no ledger existed, only
   * when no other connection has it open. Adapters without it get a plain
   * removal of the ledger files.
   */
  discard?(input: { destination: string; guard?: LifecycleFenceGuard }): Promise<void>;
  /**
   * Update --preflight for a snapshot of `source` under `destination` (which
   * may not exist yet). Read-only: creates, changes and removes nothing.
   */
  plan?(input: { source: string; destination: string }): Promise<LifecycleSnapshotPlan>;
};

type SnapshotMetadata = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  currentVersion: string | null;
  currentExecutable: string | null;
  /**
   * Digest of `currentExecutable` when the snapshot was taken (from 0.7.40):
   * what a restore brings back. Older snapshots have none; releases up to
   * 0.7.39 ignore it.
   */
  currentExecutableSha256?: string | null;
  present: Record<"config" | "database" | "service", boolean>;
  createdAt?: string;
  database?: {
    method: LifecycleSnapshotMethod | null;
    quiesced: boolean;
    cloneFallback: LifecycleCloneFallback | null;
    bytes: number;
  };
};

function isBoundedIdentifier(value: unknown): value is string {
  return typeof value === "string" && /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}$/.test(value) && !value.includes("..");
}

function assertAbsoluteOwnedPath(candidate: string, root: string, label: string) {
  if (!path.isAbsolute(candidate) || !path.isAbsolute(root)) {
    throw new Error(`${label} must be absolute`);
  }
  const relative = path.relative(path.resolve(root), path.resolve(candidate));
  if (relative === "" || relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error(`${label} must be a child of the ownership root`);
  }
}

function ensureDirectory(directory: string, boundary?: string) {
  if (boundary) assertNoSymlink(directory, boundary);
  fs.mkdirSync(directory, { recursive: true, mode: DIRECTORY_MODE });
  if (boundary) assertNoSymlink(directory, boundary);
  fs.chmodSync(directory, DIRECTORY_MODE);
}

function lstatIfPresent(candidate: string) {
  try {
    return fs.lstatSync(candidate);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

function assertNoSymlink(candidate: string, stopAt: string) {
  const resolved = path.resolve(candidate);
  const stop = path.resolve(stopAt);
  const relative = path.relative(stop, resolved);
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("managed path escapes its ownership boundary");
  }
  let current = stop;
  if (lstatIfPresent(current)?.isSymbolicLink()) {
    throw new Error("managed path cannot traverse a symlink");
  }
  for (const segment of relative.split(path.sep).filter(Boolean)) {
    current = path.join(current, segment);
    if (lstatIfPresent(current)?.isSymbolicLink()) {
      throw new Error("managed path cannot traverse a symlink");
    }
  }
}

function isLifecycleJournal(value: unknown): value is LifecycleJournal {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Partial<LifecycleJournal>;
  return row.schemaVersion === 1 &&
    isBoundedIdentifier(row.operationId) &&
    (row.kind === "update" || row.kind === "rollback") &&
    (row.fromVersion === null || isBoundedIdentifier(row.fromVersion)) &&
    isBoundedIdentifier(row.toVersion) &&
    ["prepared", "snapshotted", "staged", "switched", "verified", "rollback_required", "rollback_complete"].includes(String(row.phase)) &&
    row.snapshotId === row.operationId &&
    (row.restore === undefined || validRestoreRecord(row.restore));
}

function isSnapshotMetadata(value: unknown): value is SnapshotMetadata {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Partial<SnapshotMetadata>;
  const present = row.present as Partial<SnapshotMetadata["present"]> | undefined;
  const currentPairValid =
    (row.currentVersion === null && row.currentExecutable === null) ||
    (isBoundedIdentifier(row.currentVersion) && typeof row.currentExecutable === "string" && path.isAbsolute(row.currentExecutable));
  return row.schemaVersion === 1 && currentPairValid && Boolean(present) &&
    typeof present?.config === "boolean" &&
    typeof present?.database === "boolean" &&
    typeof present?.service === "boolean";
}

/**
 * The durable completion order: the last sequence handed to a completing
 * update or rollback, and every completion marker that existed when
 * sequencing began (all older than every sequenced one).
 */
type CompletionOrderRecord = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  lastSequence: number;
  legacyOperations: string[];
  /** Written by `snapshots reconcile --keep-snapshots`; see LifecycleOrderSeal. */
  seal?: { operationId: string; sequence: number; keep: string[]; covered: string[] };
};

const MAX_COMPLETION_MARKERS = 100_000;

function identifierList(value: unknown): string[] | null {
  if (!Array.isArray(value) || value.length > MAX_COMPLETION_MARKERS || !value.every(isBoundedIdentifier) ||
      new Set(value).size !== value.length) return null;
  return [...value];
}

function parseCompletionOrder(value: unknown): CompletionOrderRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  const keys = Object.keys(row).filter((key) => key !== "seal").sort().join(",");
  if (keys !== "lastSequence,legacyOperations,schemaVersion" || row.schemaVersion !== 1) return null;
  if (typeof row.lastSequence !== "number" || !Number.isSafeInteger(row.lastSequence) || row.lastSequence < 0) return null;
  const legacy = identifierList(row.legacyOperations);
  if (!legacy) return null;
  const record: CompletionOrderRecord = { schemaVersion: 1, lastSequence: row.lastSequence, legacyOperations: legacy };
  if (!("seal" in row)) return record;
  const seal = row.seal as Record<string, unknown> | null;
  if (!seal || typeof seal !== "object" || Array.isArray(seal) ||
      Object.keys(seal).sort().join(",") !== "covered,keep,operationId,sequence") return null;
  const keep = identifierList(seal.keep);
  const covered = identifierList(seal.covered);
  if (!isBoundedIdentifier(seal.operationId) || typeof seal.sequence !== "number" || !Number.isSafeInteger(seal.sequence) ||
      seal.sequence < 0 || seal.sequence > row.lastSequence || !keep || !covered || keep.length === 0 ||
      !keep.every((id) => covered.includes(id))) return null;
  return { ...record, seal: { operationId: seal.operationId, sequence: seal.sequence, keep, covered } };
}

/**
 * Whether the receipts' sequences tell the same story as their versions: in
 * sequence order each operation starts from the version the one before it
 * left installed (its target, or its starting version when it rolled back),
 * and the last one left the version installed now.
 */
function sequencesFollowVersionChain(operations: readonly LifecycleCompletedOperation[], installedVersion: string | null) {
  const ordered = [...operations].sort((left, right) => left.sequence! - right.sequence!);
  let installed: string | null | undefined;
  for (const operation of ordered) {
    if (installed !== undefined && operation.fromVersion !== installed) return false;
    installed = operation.status === "completed" ? operation.toVersion : operation.fromVersion;
  }
  return installed === undefined || installed === installedVersion;
}

/** A marker that is plainly not an update or rollback (uninstall, purge, support bundle, prune). */
function isOtherOperationReceipt(value: unknown, operationId: string) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  return row.schemaVersion === 1 && row.operationId === operationId && row.toVersion === null &&
    row.restoredVersion === null &&
    ["uninstall", "purge", "support_bundle", "snapshots_prune", "snapshots_reconcile"].includes(String(row.operation));
}

/**
 * What one operation committed to removing, written and fsynced before any
 * of it is moved or deleted and kept until a durable receipt names every
 * item. "planned": retention chose it; "orphan": a trash entry no record
 * accounted for.
 */
type RemovalItem = LifecycleRemovedItem & { trashName: string; origin: "planned" | "orphan" };
type RemovalRecord = { schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION; operationId: string; items: RemovalItem[] };

class RetentionPlanChanged extends Error {
  constructor(reason = "retention plan changed") {
    super(`retention plan is no longer safe to apply: ${reason}`);
    // The CLI prints thrown values directly. Keep this failure reason
    // value-blind: it may name a managed item, but never include its path or
    // contents even when its caller includes an Error stack.
    this.stack = this.message;
  }
}

const TRASH_NAME = /^(snapshot|runtime_version)\+([A-Za-z0-9][A-Za-z0-9._-]{0,95})\+[0-9a-f]{12}$/;

function parseRemovalRecord(value: unknown, operationId: string): RemovalRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  if (Object.keys(row).sort().join(",") !== "items,operationId,schemaVersion" || row.schemaVersion !== 1 ||
      row.operationId !== operationId || !Array.isArray(row.items) || row.items.length > MAX_COMPLETION_MARKERS) return null;
  const items: RemovalItem[] = [];
  for (const entry of row.items as unknown[]) {
    if (!entry || typeof entry !== "object" || Array.isArray(entry)) return null;
    const item = entry as Record<string, unknown>;
    const trash = TRASH_NAME.exec(String(item.trashName));
    if (Object.keys(item).sort().join(",") !== "bytes,kind,name,origin,trashName" || !trash ||
        trash[1] !== item.kind || trash[2] !== item.name || !isBoundedIdentifier(item.name) ||
        typeof item.bytes !== "number" || !Number.isSafeInteger(item.bytes) || item.bytes < 0 ||
        (item.origin !== "planned" && item.origin !== "orphan")) return null;
    items.push({
      kind: item.kind as RemovalItem["kind"], name: item.name, bytes: item.bytes, trashName: item.trashName as string,
      origin: item.origin,
    });
  }
  return { schemaVersion: 1, operationId, items };
}

/** Whether a durable receipt's retention record names every one of `items`. */
function receiptNamesRemovals(receipt: unknown, items: readonly LifecycleRemovedItem[]) {
  const retention = receipt && typeof receipt === "object" ? (receipt as { retention?: unknown }).retention : undefined;
  if (!retention || typeof retention !== "object") return false;
  const named = new Set<string>();
  for (const list of [(retention as { removed?: unknown }).removed, (retention as { recovered?: unknown }).recovered]) {
    if (!Array.isArray(list)) return false;
    for (const item of list) {
      if (item && typeof item === "object") named.add(`${(item as { kind?: unknown }).kind}:${(item as { name?: unknown }).name}`);
    }
  }
  return items.every((item) => named.has(`${item.kind}:${item.name}`));
}

function writeJson(file: string, value: unknown, boundary?: string) {
  ensureDirectory(path.dirname(file), boundary);
  if (boundary) assertNoSymlink(file, boundary);
  const temporary = `${file}.tmp`;
  if (boundary) assertNoSymlink(temporary, boundary);
  fs.writeFileSync(temporary, `${JSON.stringify(value, null, 2)}\n`, { mode: FILE_MODE, flag: "w" });
  fs.chmodSync(temporary, FILE_MODE);
  if (boundary) assertNoSymlink(file, boundary);
  fs.renameSync(temporary, file);
}

/** writeJson whose content and rename survive a crash or power loss before anything depends on them. */
function writeJsonDurable(file: string, value: unknown, boundary?: string) {
  ensureDirectory(path.dirname(file), boundary);
  if (boundary) assertNoSymlink(file, boundary);
  const temporary = `${file}.tmp`;
  if (boundary) assertNoSymlink(temporary, boundary);
  const descriptor = fs.openSync(temporary, "w", FILE_MODE);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(value, null, 2)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.chmodSync(temporary, FILE_MODE);
  if (boundary) assertNoSymlink(file, boundary);
  fs.renameSync(temporary, file);
  fsyncDirectory(path.dirname(file));
}

function readJson<T>(file: string): T | null {
  if (!fs.existsSync(file)) return null;
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("managed JSON must be a regular file");
  try {
    return JSON.parse(fs.readFileSync(file, "utf8")) as T;
  } catch {
    throw new Error("managed JSON is malformed");
  }
}

function copyRegularFile(source: string, destination: string, mode = FILE_MODE, boundary?: string) {
  const stat = fs.lstatSync(source);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("managed source must be a regular file");
  ensureDirectory(path.dirname(destination), boundary);
  if (boundary) assertNoSymlink(destination, boundary);
  fs.copyFileSync(source, destination, fs.constants.COPYFILE_EXCL);
  fs.chmodSync(destination, mode);
}

function sha256(file: string) {
  return `sha256:${createHash("sha256").update(fs.readFileSync(file)).digest("hex")}`;
}

/** Digest of a regular file (never through a symlink), or null when there is none to read. */
function regularFileSha256(file: string) {
  try {
    return lstatIfPresent(file)?.isFile() ? sha256(file) : null;
  } catch {
    return null;
  }
}

/** Apparent bytes of the regular files under target; never follows a symlink. */
function treeBytes(target: string) {
  let total = 0;
  let visited = 0;
  const walk = (entry: string, depth: number) => {
    const stat = lstatIfPresent(entry);
    if (!stat || stat.isSymbolicLink()) return;
    if (stat.isFile()) {
      total += stat.size;
      return;
    }
    if (!stat.isDirectory() || depth > 32) return;
    for (const name of fs.readdirSync(entry)) {
      visited += 1;
      if (visited > MAX_TREE_ENTRIES) return;
      walk(path.join(entry, name), depth + 1);
    }
  };
  walk(target, 0);
  return total;
}

/** A path-free structural stamp used to catch a source changing after planning. */
function treeStamp(target: string): string | null {
  const rows: string[] = [];
  const walk = (entry: string, relative: string, depth: number): boolean => {
    const stat = lstatIfPresent(entry);
    if (!stat || stat.isSymbolicLink() || depth > 32) return false;
    if (stat.isFile()) {
      rows.push(`f ${relative} ${stat.dev} ${stat.ino} ${stat.mode} ${stat.size} ${stat.mtimeMs}`);
      return true;
    }
    if (!stat.isDirectory()) return false;
    rows.push(`d ${relative} ${stat.dev} ${stat.ino} ${stat.mode} ${stat.mtimeMs}`);
    for (const name of fs.readdirSync(entry).sort()) {
      if (!walk(path.join(entry, name), path.posix.join(relative, name), depth + 1)) return false;
    }
    return true;
  };
  if (!walk(target, ".", 0)) return null;
  return createHash("sha256").update(rows.join("\n")).digest("hex");
}

/** Makes completed renames in a directory durable before anything depends on them. */
function fsyncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
}

function snapshotRecordFrom(metadata: SnapshotMetadata, databaseBytes: number): LifecycleSnapshotRecord {
  const database = metadata.present.database ? metadata.database : undefined;
  return {
    method: database?.method === "clone" || database?.method === "online_backup" ? database.method : null,
    quiesced: database?.quiesced === true,
    cloneFallback: CLONE_FALLBACKS.find((reason) => reason === database?.cloneFallback) ?? null,
    databaseBytes,
  };
}

/** Runtime version names a service manifest references under versionsRoot. */
function versionsNamedIn(manifest: string, versionsRoots: readonly string[]) {
  const text = manifest.replace(/&(amp|lt|gt|quot|apos);/g, (_entity, name: string) =>
    ({ amp: "&", lt: "<", gt: ">", quot: "\"", apos: "'" })[name]!);
  const found = new Set<string>();
  for (const root of versionsRoots) {
    const prefix = `${root}${path.sep}`;
    for (let index = text.indexOf(prefix); index >= 0; index = text.indexOf(prefix, index + prefix.length)) {
      const name = /^[A-Za-z0-9][A-Za-z0-9._-]{0,95}/.exec(text.slice(index + prefix.length))?.[0];
      if (isBoundedIdentifier(name)) found.add(name);
    }
  }
  return [...found];
}

export class FilesystemLifecycleAdapter implements LifecycleAdapter {
  private readonly root: string;
  private readonly versionsRoot: string;
  private readonly snapshotsRoot: string;
  private readonly receiptsRoot: string;
  private readonly completedRoot: string;
  private readonly trashRoot: string;
  private readonly statePath: string;
  private readonly journalPath: string;
  private readonly lockPath: string;
  private readonly currentPath: string;
  private readonly orderPath: string;
  private readonly removalsRoot: string;
  private readonly unreadableRemovalsRoot: string;
  /**
   * Operation → the removal records its receipt accounts for and the items
   * that receipt must name before those records may be deleted.
   */
  private readonly pendingCommits = new Map<string, { records: string[]; named: LifecycleRemovedItem[] }>();
  /** Operation-ID → held mutation lease. Bounded; one process holds at most a
   * handful of concurrent lifecycle operations. */
  private readonly fences = new Map<string, LifecycleMutationLease>();

  /**
   * `--retention keep-all`: this adapter removes nothing an earlier operation
   * left. Retention only previews (recorded as skipped_by_operator), trash
   * entries stay, and display receipts are not trimmed.
   */
  private readonly keepAll: boolean;

  constructor(
    private readonly paths: ManagedLifecyclePaths,
    private readonly service: LifecycleServiceAdapter,
    private readonly database: LifecycleDatabaseAdapter,
    private readonly authority?: LifecycleMutationAuthority,
    options: { keepAll?: boolean } = {},
  ) {
    this.keepAll = options.keepAll === true;
    for (const [label, candidate] of Object.entries({
      lifecycleRoot: paths.lifecycleRoot,
      artifactSourceRoot: paths.artifactSourceRoot,
      collectorConfig: paths.collectorConfig,
      database: paths.database,
      serviceManifest: paths.serviceManifest,
      statusSummary: paths.statusSummary,
    })) {
      assertAbsoluteOwnedPath(candidate, paths.ownershipRoot, label);
    }
    for (const [index, candidate] of paths.ownedToolFragments.entries()) {
      assertAbsoluteOwnedPath(candidate, paths.ownershipRoot, `ownedToolFragments[${index}]`);
    }
    for (const [index, candidate] of paths.history.entries()) {
      assertAbsoluteOwnedPath(candidate, paths.ownershipRoot, `history[${index}]`);
    }
    this.root = path.resolve(paths.lifecycleRoot);
    this.versionsRoot = path.join(this.root, "versions");
    this.snapshotsRoot = path.join(this.root, "snapshots");
    this.receiptsRoot = path.join(this.root, "receipts");
    this.completedRoot = path.join(this.root, "completed-operations");
    this.trashRoot = path.join(this.root, "trash");
    this.statePath = path.join(this.root, "state.json");
    this.journalPath = path.join(this.root, "journal.json");
    this.lockPath = path.join(this.root, "operation.lock");
    this.currentPath = path.join(this.root, "current");
    this.orderPath = path.join(this.root, "completion-order.json");
    this.removalsRoot = path.join(this.root, "removals");
    this.unreadableRemovalsRoot = path.join(this.root, "removals-unreadable");
    if (fs.existsSync(paths.ownershipRoot) && fs.lstatSync(paths.ownershipRoot).isSymbolicLink()) {
      throw new Error("ownership root cannot be a symlink");
    }
  }

  private initialize() {
    assertNoSymlink(this.root, this.paths.ownershipRoot);
    ensureDirectory(this.root, this.paths.ownershipRoot);
    ensureDirectory(this.versionsRoot, this.root);
    ensureDirectory(this.snapshotsRoot, this.root);
    ensureDirectory(this.receiptsRoot, this.root);
    ensureDirectory(this.completedRoot, this.root);
  }

  private state() {
    const state = readJson<unknown>(this.statePath);
    if (state === null) return null;
    if (!state || typeof state !== "object" || Array.isArray(state)) throw new Error("lifecycle state is malformed");
    const row = state as { schemaVersion?: unknown; version?: unknown; executablePath?: unknown };
    if (row.schemaVersion !== 1 || !isBoundedIdentifier(row.version) || typeof row.executablePath !== "string") {
      throw new Error("lifecycle state is malformed");
    }
    assertAbsoluteOwnedPath(row.executablePath, this.versionsRoot, "installed runtime");
    assertNoSymlink(row.executablePath, this.versionsRoot);
    const stat = fs.lstatSync(row.executablePath);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("installed runtime is malformed");
    return { schemaVersion: 1 as const, version: row.version, executablePath: row.executablePath };
  }

  async acquireLock(operationId: string) {
    this.initialize();
    if (this.authority) {
      // Issue #158: the exclusive mutation lease is the shared lifecycle
      // authority's fenced revision, not a private lock domain. Busy and
      // ambiguous outcomes both refuse the operation (false); an ambiguous
      // authority never falls back to acting unlocked.
      const acquisition = this.authority.acquire();
      if (acquisition.kind !== "acquired") return false;
      if (this.fences.size >= 8 && !this.fences.has(operationId)) {
        const oldest = this.fences.keys().next();
        if (!oldest.done) {
          this.fences.get(oldest.value)?.release();
          this.fences.delete(oldest.value);
        }
      }
      this.fences.set(operationId, acquisition.lease);
      return true;
    }
    try {
      fs.mkdirSync(this.lockPath, { mode: DIRECTORY_MODE });
      writeJson(path.join(this.lockPath, "owner.json"), { schemaVersion: 1, operationId }, this.root);
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "EEXIST") {
        return false;
      }
      throw error;
    }
  }

  async releaseLock(operationId: string) {
    const lease = this.fences.get(operationId);
    if (lease) {
      // Release touches only this operation's own record. A superseded or
      // expired owner therefore cannot affect a successor.
      this.fences.delete(operationId);
      lease.release();
      return;
    }
    if (!fs.existsSync(this.lockPath)) return;
    const owner = readJson<{ operationId?: string }>(path.join(this.lockPath, "owner.json"));
    if (owner?.operationId !== operationId) return;
    fs.rmSync(this.lockPath, { recursive: true, force: true });
  }

  async assertFence(operationId: string) {
    this.assertFenceNow(operationId);
  }

  private heldLease(operationId: string) {
    const lease = this.fences.get(operationId);
    if (!lease) {
      // Invariant violation: a fenced mutating step ran without holding the
      // mutation lease. Fail closed rather than act unowned.
      throw new Error(`lifecycle fence is not held for operation ${operationId}`);
    }
    return lease;
  }

  private assertFenceNow(operationId: string) {
    if (!this.authority) return;
    const revalidation = this.heldLease(operationId).assertCurrent();
    if (!revalidation.ok) {
      throw new LifecycleInterruption(
        `lifecycle fence lost before a mutating step: ${revalidation.reason}`,
      );
    }
  }

  /** The operation's fence for long database steps: renewal plus a final check. */
  private fenceGuard(operationId: string): LifecycleFenceGuard {
    const lease = this.authority ? this.heldLease(operationId) : null;
    const keepAliveIntervalMs = lease ? Math.max(50, Math.floor(lease.durationMs / 4)) : 60_000;
    // Each renewal rescans the lease directory and rewrites and fsyncs the
    // record. A step that reports progress often (the online backup, every
    // 100 pages) renews at most twice per interval; the first call always does.
    let renewedAt = Number.NEGATIVE_INFINITY;
    return {
      keepAliveIntervalMs,
      keepAlive: () => {
        if (!lease || Date.now() - renewedAt < keepAliveIntervalMs / 2) return;
        const renewal = lease.renew();
        if (!renewal.ok) {
          throw new LifecycleInterruption(`lifecycle fence lost during a long step: ${renewal.reason}`);
        }
        renewedAt = Date.now();
      },
      assertCurrent: () => this.assertFenceNow(operationId),
    };
  }

  async readJournal() {
    const value = readJson<unknown>(this.journalPath);
    if (value !== null && !isLifecycleJournal(value)) throw new Error("managed lifecycle journal is malformed");
    return value;
  }

  async writeJournal(journal: LifecycleJournal) {
    assertNoSymlink(this.journalPath, this.root);
    writeJson(this.journalPath, journal, this.root);
  }

  async clearJournal(operationId: string) {
    const journal = await this.readJournal();
    if (journal && journal.operationId !== operationId) throw new Error("journal ownership changed");
    fs.rmSync(this.journalPath, { force: true });
  }

  async installedVersion() {
    return this.state()?.version ?? null;
  }

  async operationIdExists(operationId: string) {
    this.initialize();
    const marker = path.join(this.completedRoot, `${operationId}.json`);
    assertNoSymlink(marker, this.root);
    return fs.existsSync(marker);
  }

  async snapshot(operationId: string) {
    this.initialize();
    const snapshot = path.join(this.snapshotsRoot, operationId);
    assertNoSymlink(this.snapshotsRoot, this.root);
    assertNoSymlink(snapshot, this.snapshotsRoot);
    if (fs.existsSync(snapshot)) {
      const metadataPath = path.join(snapshot, "snapshot.json");
      if (fs.existsSync(metadataPath)) {
        const metadata = readJson<SnapshotMetadata>(metadataPath);
        if (!isSnapshotMetadata(metadata)) throw new Error("snapshot is malformed");
        return operationId;
      }
      // A process can stop after creating the private snapshot directory but
      // before committing its metadata marker. No committed snapshot refers
      // to that directory yet, so rebuilding it is the idempotent recovery.
      fs.rmSync(snapshot, { recursive: true, force: true });
    }
    ensureDirectory(snapshot, this.snapshotsRoot);
    const state = this.state();
    const entries = [
      ["config", this.paths.collectorConfig],
      ["service", this.paths.serviceManifest],
    ] as const;
    const present = { config: false, database: false, service: false };
    try {
      for (const [label, source] of entries) {
        if (!fs.existsSync(source)) continue;
        assertNoSymlink(source, this.paths.ownershipRoot);
        assertNoSymlink(snapshot, this.snapshotsRoot);
        copyRegularFile(source, path.join(snapshot, label), FILE_MODE, snapshot);
        present[label] = true;
      }
      assertNoSymlink(snapshot, this.snapshotsRoot);
      assertNoSymlink(path.join(snapshot, "database"), snapshot);
      assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
      const databaseSource = lstatIfPresent(this.paths.database);
      if (databaseSource && !databaseSource.isFile()) {
        throw new Error("managed database source must be a regular file");
      }
      const outcome = await this.database.snapshot({
        source: this.paths.database,
        destination: path.join(snapshot, "database"),
        guard: this.fenceGuard(operationId),
      });
      const database = typeof outcome === "boolean"
        ? { present: outcome, method: null, quiesced: false, cloneFallback: null }
        : outcome;
      present.database = database.present;
      assertNoSymlink(snapshot, this.snapshotsRoot);
      let databaseBytes = 0;
      if (present.database) {
        const databaseSnapshot = path.join(snapshot, "database");
        assertNoSymlink(databaseSnapshot, snapshot);
        const stat = fs.lstatSync(databaseSnapshot);
        if (!stat.isFile()) throw new Error("database snapshot must be a regular file");
        fs.chmodSync(databaseSnapshot, FILE_MODE);
        databaseBytes = stat.size;
      }
      const metadata: SnapshotMetadata = {
        schemaVersion: 1,
        currentVersion: state?.version ?? null,
        currentExecutable: state?.executablePath ?? null,
        currentExecutableSha256: state?.executablePath ? regularFileSha256(state.executablePath) : null,
        present,
        createdAt: new Date().toISOString(),
        ...(present.database
          ? {
              database: {
                method: database.method,
                quiesced: database.quiesced,
                cloneFallback: database.cloneFallback,
                bytes: databaseBytes,
              },
            }
          : {}),
      };
      writeJson(path.join(snapshot, "snapshot.json"), metadata, snapshot);
      return operationId;
    } catch (error) {
      fs.rmSync(snapshot, { recursive: true, force: true });
      throw error;
    }
  }

  async stage(artifact: RuntimeArtifact) {
    this.initialize();
    if (fs.existsSync(this.paths.artifactSourceRoot) && fs.lstatSync(this.paths.artifactSourceRoot).isSymbolicLink()) {
      throw new Error("artifact source root cannot be a symlink");
    }
    assertAbsoluteOwnedPath(artifact.sourcePath, this.paths.artifactSourceRoot, "artifact source");
    assertNoSymlink(artifact.sourcePath, this.paths.artifactSourceRoot);
    if (sha256(artifact.sourcePath) !== artifact.sha256) throw new Error("artifact digest mismatch");
    const runtimeDirectory = path.join(
      this.root,
      "versions",
      artifact.version,
      `${artifact.platform}-${artifact.architecture}`,
    );
    const target = path.join(this.root, immutableRuntimeRelativePath(artifact));
    const stagedCompanions: Array<{ destination: string }> = [];
    const companionDestinations = (artifact.files ?? []).map((file, index) => {
      const destination = path.join(runtimeDirectory, ...file.relativePath.split("/"));
      assertAbsoluteOwnedPath(destination, this.root, `companion ${index} destination`);
      assertNoSymlink(path.dirname(destination), this.root);
      return { file, destination, index };
    });

    // The executable is the immutable target for this version. Check it before
    // touching any companion so a conflicting repin cannot damage an existing
    // runtime closure before the transaction rolls back.
    assertNoSymlink(path.dirname(target), this.root);
    const targetStat = lstatIfPresent(target);
    if (targetStat && (!targetStat.isFile() || targetStat.isSymbolicLink() || sha256(target) !== artifact.sha256)) {
      throw new Error("immutable runtime target already differs");
    }

    // Existing companions are immutable too. Validate every one before
    // staging a missing companion, so a mismatch leaves the whole closure
    // untouched.
    for (const { file, destination, index } of companionDestinations) {
      const stat = lstatIfPresent(destination);
      if (!stat) continue;
      if (!stat.isFile() || stat.isSymbolicLink() || sha256(destination) !== file.sha256) {
        throw new Error(`immutable runtime companion ${file.relativePath} already differs`);
      }
    }

    try {
      for (const { file, index, destination } of companionDestinations) {
        if (lstatIfPresent(destination)) continue;
        // Companion sources live in the artifact staging area next to the
        // bundle; their absolute paths were validated when resolved.
        assertAbsoluteOwnedPath(file.sourcePath, this.paths.artifactSourceRoot, `companion ${file.relativePath} source`);
        assertNoSymlink(file.sourcePath, this.paths.artifactSourceRoot);
        ensureDirectory(path.dirname(destination), this.root);
        // Verify beside the final destination, then publish with one rename.
        // A failed copy or digest check therefore cannot leave a partial or
        // unverified companion under its immutable name.
        const staging = `${destination}+staging`;
        fs.rmSync(staging, { force: true });
        try {
          copyRegularFile(file.sourcePath, staging, FILE_MODE, this.root);
          if (sha256(staging) !== file.sha256) throw new Error(`companion ${index} digest mismatch`);
          fs.renameSync(staging, destination);
        } catch (error) {
          fs.rmSync(staging, { force: true });
          throw error;
        }
        stagedCompanions.push({ destination });
      }
      if (targetStat) {
        fs.chmodSync(target, EXECUTABLE_MODE);
        return;
      }
      ensureDirectory(path.dirname(target), this.root);
      const staging = `${target}.staging`;
      fs.rmSync(staging, { force: true });
      try {
        copyRegularFile(artifact.sourcePath, staging, EXECUTABLE_MODE, this.root);
        if (sha256(staging) !== artifact.sha256) throw new Error("staged artifact digest mismatch");
        fs.renameSync(staging, target);
      } catch (error) {
        fs.rmSync(staging, { force: true });
        throw error;
      }
    } catch (error) {
      for (const staged of stagedCompanions.reverse()) {
        fs.rmSync(staged.destination, { force: true });
      }
      throw error;
    }
  }

  private verifyStagedRuntime(artifact: RuntimeArtifact) {
    const executablePath = path.join(this.root, immutableRuntimeRelativePath(artifact));
    const stat = fs.lstatSync(executablePath);
    if (!stat.isFile() || stat.isSymbolicLink() || sha256(executablePath) !== artifact.sha256) {
      throw new Error("staged runtime is not immutable or does not match");
    }
    // Companion relative paths are rooted at the architecture directory.
    const runtimeDirectory = path.dirname(path.dirname(executablePath));
    for (const [index, file] of (artifact.files ?? []).entries()) {
      const companion = path.join(runtimeDirectory, ...file.relativePath.split("/"));
      assertNoSymlink(companion, this.root);
      const companionStat = fs.lstatSync(companion);
      if (!companionStat.isFile() || companionStat.isSymbolicLink() || sha256(companion) !== file.sha256) {
        throw new Error(`staged companion ${index} is not immutable or does not match`);
      }
    }
    return executablePath;
  }

  async switchTo(artifact: RuntimeArtifact) {
    const executablePath = this.verifyStagedRuntime(artifact);
    await this.service.activate({ executablePath, version: artifact.version });
    const targetDirectory = path.dirname(path.dirname(executablePath));
    const temporary = `${this.currentPath}.next`;
    fs.rmSync(temporary, { force: true });
    fs.symlinkSync(targetDirectory, temporary, "dir");
    fs.renameSync(temporary, this.currentPath);
    writeJson(this.statePath, {
      schemaVersion: 1,
      version: artifact.version,
      executablePath,
    }, this.root);
  }

  readiness(expectedVersion: string, input: { signal: AbortSignal; deadlineMs: number }) {
    return this.service.readiness(expectedVersion, input);
  }

  async restore(snapshotId: string, operationId = snapshotId) {
    this.initialize();
    const guard = this.fenceGuard(operationId);
    const snapshot = path.join(this.snapshotsRoot, snapshotId);
    assertNoSymlink(snapshot, this.snapshotsRoot);
    const metadataPath = path.join(snapshot, "snapshot.json");
    assertNoSymlink(metadataPath, snapshot);
    const metadata = readJson<SnapshotMetadata>(metadataPath);
    if (!isSnapshotMetadata(metadata)) throw new Error("rollback snapshot is missing");
    // The ledger goes first: its restore may refuse (another process still
    // has the ledger open, no room for a byte copy, a copy that fails its
    // integrity check), and a refusal must leave the config, runtime pointer
    // and service exactly as they were.
    const databaseSnapshot = path.join(snapshot, "database");
    assertNoSymlink(databaseSnapshot, snapshot);
    assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
    let record: LifecycleRestoreRecord | undefined = { method: "none", cloneFallback: null, databaseBytes: 0 };
    if (metadata.present.database) {
      const databaseSource = lstatIfPresent(databaseSnapshot);
      if (!databaseSource?.isFile()) throw new Error("rollback database snapshot must be a regular file");
      const outcome = await this.database.restore({
        source: databaseSnapshot,
        destination: this.paths.database,
        guard,
      });
      assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
      const restoredDatabase = lstatIfPresent(this.paths.database);
      if (!restoredDatabase?.isFile()) throw new Error("restored database must be a regular file");
      record = outcome
        ? {
            method: outcome.method,
            cloneFallback: outcome.cloneFallback,
            databaseBytes: outcome.databaseBytes,
            ...outcome.integrity ? { integrity: outcome.integrity } : {},
          }
        : undefined;
    } else if (this.database.discard) {
      await this.database.discard({ destination: this.paths.database, guard });
    } else {
      guard.assertCurrent();
      fs.rmSync(this.paths.database, { force: true });
      fs.rmSync(`${this.paths.database}-wal`, { force: true });
      fs.rmSync(`${this.paths.database}-shm`, { force: true });
    }
    // Config, runtime pointer and service follow only while the fence holds.
    guard.assertCurrent();
    const restoreFile = (label: "config" | "service", destination: string) => {
      const source = path.join(snapshot, label);
      assertNoSymlink(source, snapshot);
      assertNoSymlink(destination, this.paths.ownershipRoot);
      if (metadata.present[label]) {
        fs.rmSync(destination, { force: true });
        assertNoSymlink(destination, this.paths.ownershipRoot);
        copyRegularFile(source, destination, FILE_MODE, this.paths.ownershipRoot);
      } else {
        fs.rmSync(destination, { force: true });
      }
    };
    restoreFile("config", this.paths.collectorConfig);
    if (metadata.currentVersion && metadata.currentExecutable) {
      assertAbsoluteOwnedPath(metadata.currentExecutable, this.versionsRoot, "snapshot runtime");
      assertNoSymlink(metadata.currentExecutable, this.versionsRoot);
      const targetDirectory = path.dirname(path.dirname(metadata.currentExecutable));
      const temporary = `${this.currentPath}.restore`;
      fs.rmSync(temporary, { force: true });
      fs.symlinkSync(targetDirectory, temporary, "dir");
      fs.renameSync(temporary, this.currentPath);
      writeJson(this.statePath, {
        schemaVersion: 1,
        version: metadata.currentVersion,
        executablePath: metadata.currentExecutable,
      }, this.root);
    } else {
      fs.rmSync(this.currentPath, { force: true });
      fs.rmSync(this.statePath, { force: true });
    }
    await this.service.restore({
      executablePath: metadata.currentExecutable,
      version: metadata.currentVersion,
    });
    // Service restoration may regenerate or remove a manifest. The snapshot
    // is authoritative, including a legacy source install with no version
    // pointer and a managed install's prior Node path and environment.
    restoreFile("service", this.paths.serviceManifest);
    return record;
  }

  async persistReceipt(receipt: LifecycleReceipt) {
    // Receipt persistence must not resurrect directories that destructive
    // operations just removed (versions/, snapshots/), so only the receipt
    // roots are ensured here.
    ensureDirectory(this.receiptsRoot, this.root);
    ensureDirectory(this.completedRoot, this.root);
    if (receipt.status !== "rollback_required" && receipt.status !== "refused") {
      // The durable, unbounded marker is the operation-ID authority. Commit
      // it before the display receipt so a stop between the two can never
      // make a completed destructive operation reusable. A verified or
      // rollback-complete journal can still reopen and finish receipt writing.
      // A refusal changed nothing, so its operation ID stays usable.
      writeJsonDurable(path.join(this.completedRoot, `${receipt.operationId}.json`), receipt, this.completedRoot);
    }
    // Display receipts are never trimmed: a refused or rollback_required
    // receipt is the only record of its operation.
    writeJsonDurable(path.join(this.receiptsRoot, `${receipt.operationId}-${receipt.operation}.json`), receipt, this.receiptsRoot);
  }

  async uninstallOwned(input: { apply: boolean }) {
    const targets = ["service_manifest", "tool_config_fragments", "runtime_pointer", "runtime_versions"] as const;
    if (!input.apply) return targets;
    await this.service.remove();
    fs.rmSync(this.paths.serviceManifest, { force: true });
    for (const fragment of this.paths.ownedToolFragments) {
      if (!fs.existsSync(fragment)) continue;
      assertNoSymlink(fragment, this.paths.ownershipRoot);
      const stat = fs.lstatSync(fragment);
      if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("owned tool fragment must be a regular file");
      fs.rmSync(fragment, { force: true });
    }
    fs.rmSync(this.currentPath, { force: true });
    fs.rmSync(this.versionsRoot, { recursive: true, force: true });
    fs.rmSync(this.statePath, { force: true });
    return targets;
  }

  async purgeOwnedData(input: { apply: boolean; confirmation: string | null }) {
    const targets = [
      "collector_config", "workspace_credentials", "ledger", "history", "status_summary", "lifecycle_snapshots",
    ] as const;
    if (!input.apply) return targets;
    if (input.confirmation !== PURGE_CONFIRMATION) throw new Error("purge confirmation mismatch");
    for (const candidate of [
      this.paths.collectorConfig, this.paths.database, ...this.paths.history, ...this.statusSummaryFiles(),
    ]) {
      if (!fs.existsSync(candidate)) continue;
      assertNoSymlink(candidate, this.paths.ownershipRoot);
      const stat = fs.lstatSync(candidate);
      if (stat.isSymbolicLink()) throw new Error("purge target cannot be a symlink");
      fs.rmSync(candidate, { recursive: stat.isDirectory(), force: true });
    }
    assertNoSymlink(this.snapshotsRoot, this.root);
    fs.rmSync(this.snapshotsRoot, { recursive: true, force: true });
    // Snapshots awaiting removal still hold config and ledger copies. Their
    // removal records go with them: this purge receipt covers every snapshot.
    assertNoSymlink(this.trashRoot, this.root);
    fs.rmSync(this.trashRoot, { recursive: true, force: true });
    assertNoSymlink(this.removalsRoot, this.root);
    fs.rmSync(this.removalsRoot, { recursive: true, force: true });
    assertNoSymlink(this.unreadableRemovalsRoot, this.root);
    fs.rmSync(this.unreadableRemovalsRoot, { recursive: true, force: true });
    return targets;
  }

  supportSnapshot() {
    return this.service.supportSnapshot();
  }

  /** The status summary and any temp file an interrupted writer left beside it. */
  private statusSummaryFiles() {
    const directory = path.dirname(this.paths.statusSummary);
    let entries: string[] = [];
    try {
      entries = fs.readdirSync(directory);
    } catch {
      // No directory means no summary either.
    }
    return [
      this.paths.statusSummary,
      ...entries.filter(isStatusSummaryTempFile).map((entry) => path.join(directory, entry)),
    ];
  }

  private readSnapshotMetadata(snapshot: string): SnapshotMetadata | null {
    try {
      const metadataPath = path.join(snapshot, "snapshot.json");
      assertNoSymlink(metadataPath, this.root);
      const metadata = readJson<unknown>(metadataPath);
      return isSnapshotMetadata(metadata) ? metadata : null;
    } catch {
      return null;
    }
  }

  async snapshotRecord(snapshotId: string): Promise<LifecycleSnapshotRecord | null> {
    if (!isBoundedIdentifier(snapshotId)) return null;
    const snapshot = path.join(this.snapshotsRoot, snapshotId);
    const metadata = this.readSnapshotMetadata(snapshot);
    if (!metadata) return null;
    const database = metadata.present.database ? lstatIfPresent(path.join(snapshot, "database")) : null;
    return snapshotRecordFrom(metadata, database?.isFile() ? database.size : 0);
  }

  /** Read-only: creates nothing, not even the lifecycle root. */
  async planSnapshot(): Promise<LifecycleSnapshotPlan> {
    if (!this.database.plan) throw new Error("the database adapter cannot plan snapshots");
    assertNoSymlink(this.root, this.paths.ownershipRoot);
    assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
    return this.database.plan({ source: this.paths.database, destination: this.snapshotsRoot });
  }

  /**
   * Every completion marker: the updates and rollbacks whose full receipt
   * proves they completed or rolled back, the markers of other operations,
   * and the ones that prove nothing (unreadable or not the receipt their
   * operation wrote), which make those operations unknown.
   */
  private completionMarkers() {
    const operations: LifecycleCompletedOperation[] = [];
    const invalid: string[] = [];
    const withoutSequence: string[] = [];
    const ids: string[] = [];
    if (!lstatIfPresent(this.completedRoot)) return { operations, invalid, withoutSequence, ids };
    assertNoSymlink(this.completedRoot, this.root);
    for (const name of fs.readdirSync(this.completedRoot).sort()) {
      if (!name.endsWith(".json")) continue;
      const id = name.slice(0, -".json".length);
      if (!isBoundedIdentifier(id)) continue;
      if (ids.length >= MAX_COMPLETION_MARKERS) throw new Error("too many completion markers to order");
      ids.push(id);
      try {
        const stat = fs.lstatSync(path.join(this.completedRoot, name));
        if (!stat.isFile() || stat.size > MAX_MARKER_BYTES) throw new Error("marker is not a bounded regular file");
        const value = JSON.parse(fs.readFileSync(path.join(this.completedRoot, name), "utf8")) as unknown;
        const operation = parseCompletionReceipt(value, id);
        if (operation) operations.push(operation);
        else if (!isOtherOperationReceipt(value, id)) {
          invalid.push(id);
          // Complete but for its sequence: a command that could not read the
          // order record wrote it (0.7.38 on a sealed host, or with the record lost).
          if (parseCompletionReceipt(value, id, { sequenceOptional: true })) withoutSequence.push(id);
        }
      } catch {
        invalid.push(id);
      }
    }
    return { operations, invalid, withoutSequence, ids };
  }

  private completionOrder(): { state: "absent" } | { state: "invalid" } | { state: "valid"; record: CompletionOrderRecord } {
    try {
      assertNoSymlink(this.orderPath, this.root);
      const stat = lstatIfPresent(this.orderPath);
      if (!stat) return { state: "absent" };
      if (!stat.isFile() || stat.size > 16 * 1024 * 1024) return { state: "invalid" };
      const record = parseCompletionOrder(JSON.parse(fs.readFileSync(this.orderPath, "utf8")));
      return record ? { state: "valid", record } : { state: "invalid" };
    } catch {
      return { state: "invalid" };
    }
  }

  async assignCompletionSequence(operationId: string): Promise<number | null> {
    try {
      const markers = this.completionMarkers();
      const order = this.completionOrder();
      const sequences = markers.operations.flatMap((operation) => operation.sequence === null ? [] : [operation.sequence]);
      let record: CompletionOrderRecord;
      if (order.state === "valid") {
        record = order.record;
      } else if (order.state === "absent" && sequences.length === 0) {
        // Sequencing begins: every marker already here is older than every
        // sequenced completion, so record exactly which ones those are.
        record = { schemaVersion: 1, lastSequence: 0, legacyOperations: markers.ids.filter((id) => id !== operationId) };
      } else {
        // The order record is lost or damaged: retention stays blocked until
        // `snapshots reconcile`, which never rebuilds it from guesses. This
        // completion still gets a sequence above every receipt's, so its own
        // receipt is not left unsequenced (and its snapshot unknown) for ever.
        const fallback = Math.max(0, ...sequences) + 1;
        return Number.isSafeInteger(fallback) ? fallback : null;
      }
      const next = Math.max(record.lastSequence, ...sequences) + 1;
      if (!Number.isSafeInteger(next)) return null;
      writeJsonDurable(this.orderPath, { ...record, lastSequence: next }, this.root);
      return next;
    } catch {
      return null;
    }
  }

  private versionOf(target: string) {
    const relative = path.relative(this.versionsRoot, path.resolve(this.root, target));
    if (!relative || relative.startsWith("..") || path.isAbsolute(relative)) return null;
    const version = relative.split(path.sep)[0];
    return isBoundedIdentifier(version) ? version : null;
  }

  /** Versions the current pointer and the service manifest point at. */
  private pinnedVersions() {
    const pinned: Array<{ version: string; reason: "current" | "service_manifest" }> = [];
    if (lstatIfPresent(this.currentPath)?.isSymbolicLink()) {
      const version = this.versionOf(fs.readlinkSync(this.currentPath));
      if (version) pinned.push({ version, reason: "current" });
    }
    const manifest = lstatIfPresent(this.paths.serviceManifest);
    if (manifest?.isFile() && manifest.size <= MAX_MANIFEST_BYTES) {
      const roots = new Set([this.versionsRoot]);
      try {
        roots.add(fs.realpathSync(this.versionsRoot));
      } catch {
        // No versions directory: nothing further to resolve.
      }
      for (const version of versionsNamedIn(fs.readFileSync(this.paths.serviceManifest, "utf8"), [...roots])) {
        pinned.push({ version, reason: "service_manifest" });
      }
    }
    return pinned;
  }

  private childDirectories(parent: string) {
    if (!lstatIfPresent(parent)) return [];
    assertNoSymlink(parent, this.root);
    if (!fs.lstatSync(parent).isDirectory()) throw new Error("managed lifecycle directory is malformed");
    return fs.readdirSync(parent).sort().filter((name) => {
      if (!isBoundedIdentifier(name)) return false;
      const stat = lstatIfPresent(path.join(parent, name));
      return Boolean(stat?.isDirectory() && !stat.isSymbolicLink());
    });
  }

  /**
   * Every fact about the completion order that retention and reconcile decide
   * on. Proven only when the order record is readable (or sequencing has not
   * begun), no two completions after the last reconcile share a sequence, no
   * sequence is beyond the record, and every unsequenced receipt predates
   * sequencing.
   */
  private orderEvidence() {
    const markers = this.completionMarkers();
    const order = this.completionOrder();
    const record = order.state === "valid" ? order.record : null;
    const seal = record?.seal ?? null;
    const sequenced = markers.operations.filter((operation) => operation.sequence !== null);
    const postSeal = seal ? sequenced.filter((operation) => operation.sequence! > seal.sequence) : sequenced;
    const counts = new Map<number, number>();
    for (const operation of postSeal) counts.set(operation.sequence!, (counts.get(operation.sequence!) ?? 0) + 1);
    const duplicated = postSeal.filter((operation) => counts.get(operation.sequence!)! > 1).map((operation) => operation.id);
    const beyondRecord = record
      ? sequenced.filter((operation) => operation.sequence! > record.lastSequence).map((operation) => operation.id)
      : [];
    const predates = new Set(record?.legacyOperations ?? []);
    // A receipt without a sequence written after sequencing began (an older
    // collector ran an update or rollback later) has no provable place.
    const unsequencedAfter = order.state !== "absent" || sequenced.length > 0
      ? markers.operations.filter((operation) => operation.sequence === null && !predates.has(operation.id))
        .map((operation) => operation.id)
      : [];
    const proven = order.state !== "invalid" && !(order.state === "absent" && sequenced.length > 0) &&
      duplicated.length === 0 && beyondRecord.length === 0 && unsequencedAfter.length === 0;
    return { markers, order, record, seal, sequenced, duplicated, beyondRecord, predates, unsequencedAfter, proven };
  }

  /** Read-only view of everything retention decides over. */
  private retentionInput() {
    let blockedReason: LifecycleSnapshotInventory["blockedReason"] = null;
    const empty: LifecycleRetentionInput = {
      installedVersion: null, pinnedVersions: [], journal: null, operations: [],
      order: { proven: true, legacyChain: false }, snapshots: [], versions: [],
    };
    const unusable = new Map<string, string>();
    if (!lstatIfPresent(this.root)) return { input: empty, blockedReason, createdAt: new Map<string, string | null>(), unusable };
    assertNoSymlink(this.root, this.paths.ownershipRoot);
    let installedVersion: string | null = null;
    try {
      installedVersion = this.state()?.version ?? null;
    } catch {
      blockedReason = "lifecycle_state_unreadable";
    }
    let journal: LifecycleJournal | null = null;
    try {
      assertNoSymlink(this.journalPath, this.root);
      const value = readJson<unknown>(this.journalPath);
      if (value !== null && !isLifecycleJournal(value)) throw new Error("malformed journal");
      journal = value;
    } catch {
      blockedReason ??= "journal_unreadable";
    }
    const createdAt = new Map<string, string | null>();
    const runtimeDigests = new Map<string, string | null>();
    const snapshots = this.childDirectories(this.snapshotsRoot).map((id) => {
      const directory = path.join(this.snapshotsRoot, id);
      const metadata = this.readSnapshotMetadata(directory);
      const createdAtMs = metadata?.createdAt !== undefined && Number.isFinite(Date.parse(metadata.createdAt))
        ? Date.parse(metadata.createdAt)
        : (lstatIfPresent(path.join(directory, "snapshot.json")) ?? fs.lstatSync(directory)).mtimeMs;
      createdAt.set(id, new Date(createdAtMs).toISOString());
      const reason = metadata ? this.unrestorableReason(directory, metadata, runtimeDigests) : null;
      if (reason) unusable.set(id, reason);
      return {
        id,
        bytes: treeBytes(directory),
        metadataValid: metadata !== null,
        restoresVersion: metadata?.currentVersion ?? null,
        method: metadata ? snapshotRecordFrom(metadata, 0).method : null,
        restorable: reason === null,
      };
    });
    const versions = this.childDirectories(this.versionsRoot).map((version) => ({
      version,
      bytes: treeBytes(path.join(this.versionsRoot, version)),
    }));

    // Completion order: durable sequences, checked against the order record.
    // Missing, duplicated or contradictory order evidence keeps everything.
    const { markers, proven, predates, seal } = this.orderEvidence();
    const known = new Set(markers.operations.map((operation) => operation.id));
    const markerIds = new Set(markers.ids);
    const legacyChain = markers.invalid.length === 0 &&
      snapshots.every((snapshot) => known.has(snapshot.id) ||
        (journal !== null && (snapshot.id === journal.snapshotId || snapshot.id === journal.operationId))) &&
      [...predates].every((id) => markerIds.has(id));
    if (!proven) blockedReason ??= "completion_order_unproven";
    // A removal record that cannot be read may describe deletions no receipt
    // names yet: remove nothing more until someone looks.
    if (this.removalRecords().invalid.length > 0) blockedReason ??= "removal_record_unreadable";
    const operations = markers.operations.map((operation) => ({ ...operation, predatesSequence: predates.has(operation.id) }));
    return {
      input: {
        installedVersion, pinnedVersions: this.pinnedVersions(), journal, operations,
        receiptsWithoutSequence: markers.withoutSequence,
        order: { proven, legacyChain, seal }, snapshots, versions,
      },
      blockedReason,
      createdAt,
      unusable,
    };
  }

  /**
   * Why a snapshot could not actually be restored, or null when it can: its
   * own files are present (the database copy at its recorded size), and the
   * runtime it restores is still a regular file under versions/ that matches
   * the digest the snapshot recorded (snapshots taken before 0.7.40 recorded
   * none, so only its presence is checked). Read only; each runtime is hashed
   * once per call.
   */
  private unrestorableReason(directory: string, metadata: SnapshotMetadata, runtimeDigests: Map<string, string | null>): string | null {
    const regularFile = (file: string) => {
      const stat = lstatIfPresent(file);
      return stat?.isFile() ? stat : null;
    };
    for (const label of ["config", "service"] as const) {
      if (metadata.present[label] && !regularFile(path.join(directory, label))) return `its ${label} copy is missing`;
    }
    if (metadata.present.database) {
      const database = regularFile(path.join(directory, "database"));
      if (!database) return "its database copy is missing";
      if (metadata.database && database.size !== metadata.database.bytes) {
        return `its database copy is ${database.size} bytes, but ${metadata.database.bytes} were recorded`;
      }
    }
    if (!metadata.currentVersion || !metadata.currentExecutable) return null;
    const executable = metadata.currentExecutable;
    try {
      assertAbsoluteOwnedPath(executable, this.versionsRoot, "snapshot runtime");
      assertNoSymlink(executable, this.versionsRoot);
    } catch {
      return `runtime ${metadata.currentVersion} is not a lifecycle runtime`;
    }
    if (!regularFile(executable)) return `runtime ${metadata.currentVersion} executable is missing`;
    const recorded = metadata.currentExecutableSha256;
    if (recorded === undefined || recorded === null) return null;
    if (!runtimeDigests.has(executable)) runtimeDigests.set(executable, regularFileSha256(executable));
    return runtimeDigests.get(executable) === recorded ? null
      : `runtime ${metadata.currentVersion} executable no longer matches the digest the snapshot recorded`;
  }

  /** Entries an earlier removal renamed into the trash but had not deleted yet. */
  private trashEntries() {
    const trash = lstatIfPresent(this.trashRoot);
    if (!trash) return [];
    assertNoSymlink(this.trashRoot, this.root);
    if (!trash.isDirectory()) throw new Error("lifecycle trash must be a directory");
    // Only names this adapter gives trash entries; anything else is left alone.
    const entries: Array<{ fileName: string; item: LifecycleRemovedItem }> = [];
    for (const fileName of fs.readdirSync(this.trashRoot).sort()) {
      const match = TRASH_NAME.exec(fileName);
      if (match && isBoundedIdentifier(match[2])) {
        const kind = match[1] as LifecycleRemovedItem["kind"];
        entries.push({ fileName, item: { kind, name: match[2]!, bytes: treeBytes(path.join(this.trashRoot, fileName)) } });
      }
    }
    return entries;
  }

  /** Removal records of operations whose receipt has not yet accounted for them. */
  private removalRecords() {
    const records: RemovalRecord[] = [];
    const invalid: string[] = [];
    if (!lstatIfPresent(this.removalsRoot)) return { records, invalid };
    assertNoSymlink(this.removalsRoot, this.root);
    for (const name of fs.readdirSync(this.removalsRoot).sort()) {
      if (!name.endsWith(".json")) continue;
      const operationId = name.slice(0, -".json".length);
      try {
        const stat = fs.lstatSync(path.join(this.removalsRoot, name));
        if (!isBoundedIdentifier(operationId) || !stat.isFile() || stat.size > 64 * 1024 * 1024) throw new Error("bad record");
        const record = parseRemovalRecord(JSON.parse(fs.readFileSync(path.join(this.removalsRoot, name), "utf8")), operationId);
        if (!record) throw new Error("bad record");
        records.push(record);
      } catch {
        invalid.push(name);
      }
    }
    return { records, invalid };
  }

  /** Whether the operation's durable completion marker names every one of `items`. */
  private receiptNames(operationId: string, items: readonly LifecycleRemovedItem[]) {
    try {
      const marker = path.join(this.completedRoot, `${operationId}.json`);
      assertNoSymlink(marker, this.root);
      const stat = lstatIfPresent(marker);
      if (!stat?.isFile() || stat.size > MAX_MARKER_BYTES) return false;
      return receiptNamesRemovals(JSON.parse(fs.readFileSync(marker, "utf8")), items);
    } catch {
      return false;
    }
  }

  private removalSource(item: LifecycleRemovedItem) {
    return path.join(item.kind === "snapshot" ? this.snapshotsRoot : this.versionsRoot, item.name);
  }

  /** Deletes one trash entry (never following a link); its removal is already durably recorded. */
  private async deleteTrashEntry(operationId: string, trashName: string) {
    await this.assertFence(operationId);
    assertNoSymlink(this.trashRoot, this.root);
    try {
      fs.rmSync(path.join(this.trashRoot, trashName), { recursive: true, force: true });
    } catch {
      const error = new Error("retention trash delete failed");
      error.stack = error.message;
      throw error;
    }
  }

  async inspectSnapshots(input: { keep: number }): Promise<LifecycleSnapshotInventory> {
    const { input: retention, blockedReason, createdAt } = this.retentionInput();
    const plan = planLifecycleRetention(retention, input.keep);
    const byId = new Map(retention.snapshots.map((snapshot) => [snapshot.id, snapshot]));
    const snapshots = plan.snapshots.map((decision) => {
      const snapshot = byId.get(decision.id)!;
      return {
        id: decision.id,
        createdAt: createdAt.get(decision.id) ?? null,
        bytes: snapshot.bytes,
        method: snapshot.method ?? "unrecorded" as const,
        restoresVersion: snapshot.restoresVersion,
        operationState: decision.state,
        retention: decision.keep ? "keep" as const : "prune" as const,
        reason: decision.reason,
      };
    }).sort((left, right) => (right.createdAt ?? "").localeCompare(left.createdAt ?? "") || right.id.localeCompare(left.id));
    const versionBytes = new Map(retention.versions.map((version) => [version.version, version.bytes]));
    const versions = plan.versions.map((decision) => ({
      version: decision.version,
      bytes: versionBytes.get(decision.version) ?? 0,
      retention: decision.keep ? "keep" as const : "prune" as const,
      reason: decision.reason,
    })).sort((left, right) => right.version.localeCompare(left.version, undefined, { numeric: true }));
    const pendingRemoval = this.trashEntries().map((entry) => entry.item);
    const sum = (rows: readonly { bytes: number }[]) => rows.reduce((total, row) => total + row.bytes, 0);
    return {
      keepSnapshots: input.keep,
      installedVersion: retention.installedVersion,
      blockedReason,
      snapshots,
      versions,
      pendingRemoval,
      bytes: {
        snapshots: sum(snapshots),
        versions: sum(versions),
        prunable: blockedReason ? 0 : sum([...snapshots, ...versions].filter((row) => row.retention === "prune")),
        pendingRemoval: sum(pendingRemoval),
      },
    };
  }

  /**
   * Re-read all retention inputs immediately before a destructive step. The
   * mutation lease protects cooperating lifecycle commands, while this check
   * protects the retention plan from an uncooperating watcher changing a
   * snapshot or its rollback runtime after the removal record is written.
   */
  private assertRetentionPlanStillSafe(
    initial: LifecycleRetentionInput,
    initialPlan: LifecycleRetentionPlan,
    keep: number,
    planned: readonly RemovalItem[],
    moved: ReadonlySet<string>,
    initialStamps: ReadonlyMap<string, string | null>,
  ) {
    try {
      const fail = (reason: string): never => {
        throw new RetentionPlanChanged(reason);
      };
      const currentResult = this.retentionInput();
      if (currentResult.blockedReason) fail(`blocked:${currentResult.blockedReason}`);
      const current = currentResult.input;
      const basis = (value: LifecycleRetentionInput) => JSON.stringify({
        installedVersion: value.installedVersion,
        pinnedVersions: value.pinnedVersions,
        journal: value.journal,
        operations: value.operations,
        receiptsWithoutSequence: value.receiptsWithoutSequence ?? [],
        // `legacyChain` is derived from the visible snapshot set. A planned
        // rename removes that row from the set, so its value can change as a
        // result of this operation without any outside mutation. The durable
        // order and seal are still compared above; marker and row changes are
        // checked independently below.
        order: { proven: value.order.proven, seal: value.order.seal ?? null },
      });
      if (basis(initial) !== basis(current)) fail("basis");

      const initialSnapshots = new Map(initial.snapshots.map((row) => [`snapshot:${row.id}`, row] as const));
      const currentSnapshots = new Map(current.snapshots.map((row) => [`snapshot:${row.id}`, row] as const));
      const initialVersions = new Map(initial.versions.map((row) => [`runtime_version:${row.version}`, row] as const));
      const currentVersions = new Map(current.versions.map((row) => [`runtime_version:${row.version}`, row] as const));
      type RetentionRow = {
        bytes: number;
        metadataValid?: boolean;
        restoresVersion?: string | null;
        method?: LifecycleSnapshotMethod | null;
        restorable?: boolean;
      };
      const initialRows = new Map<string, RetentionRow>();
      const currentRows = new Map<string, RetentionRow>();
      initialSnapshots.forEach((row, rowKey) => initialRows.set(rowKey, row));
      initialVersions.forEach((row, rowKey) => initialRows.set(rowKey, row));
      currentSnapshots.forEach((row, rowKey) => currentRows.set(rowKey, row));
      currentVersions.forEach((row, rowKey) => currentRows.set(rowKey, row));
      const key = (item: LifecycleRemovedItem) => `${item.kind}:${item.name}`;
      const movedKeys = new Set(moved);
      const plannedKeys = new Set(planned.map(key));
      const rowSignature = (row: RetentionRow) => {
        if ("metadataValid" in row) {
          return JSON.stringify([
            row.bytes, row.metadataValid, row.restoresVersion, row.method, row.restorable === false ? false : true,
          ]);
        }
        return JSON.stringify([row.bytes]);
      };
      for (const [rowKey, before] of initialRows) {
        const after = currentRows.get(rowKey);
        if (movedKeys.has(rowKey)) {
          const item = planned.find((candidate) => key(candidate) === rowKey);
          if (!item) {
            fail(`moved:${rowKey}`);
          }
          continue;
        }
        if (plannedKeys.has(rowKey) && initialStamps.get(rowKey) === null) fail(`unstampable:${rowKey}`);
        if (!after || rowSignature(before) !== rowSignature(after) ||
            (plannedKeys.has(rowKey) && treeStamp(this.removalSource({ kind: rowKey.startsWith("snapshot:") ? "snapshot" : "runtime_version", name: rowKey.slice(rowKey.indexOf(":") + 1), bytes: 0 })) !== initialStamps.get(rowKey))) {
          fail(`row:${rowKey}`);
        }
      }
      for (const rowKey of currentRows.keys()) {
        if (!initialRows.has(rowKey) && !movedKeys.has(rowKey)) fail(`new-row:${rowKey}`);
      }

      const initialDecisions = new Map([
        ...initialPlan.snapshots.map((row) => [`snapshot:${row.id}`, row.keep] as const),
        ...initialPlan.versions.map((row) => [`runtime_version:${row.version}`, row.keep] as const),
      ]);
      const currentPlan = planLifecycleRetention(current, keep);
      const currentDecisions = new Map([
        ...currentPlan.snapshots.map((row) => [`snapshot:${row.id}`, row.keep] as const),
        ...currentPlan.versions.map((row) => [`runtime_version:${row.version}`, row.keep] as const),
      ]);
      for (const [rowKey, before] of initialDecisions) {
        if (movedKeys.has(rowKey)) continue;
        if (currentDecisions.get(rowKey) !== before) fail(`decision:${rowKey}`);
      }
      for (const rowKey of currentDecisions.keys()) {
        if (!initialDecisions.has(rowKey) && !movedKeys.has(rowKey)) fail(`new-decision:${rowKey}`);
      }

      // A kept snapshot that restores an earlier runtime is the way back. It
      // must still be present and restorable before any older item moves.
      for (const decision of initialPlan.snapshots.filter((row) => row.keep)) {
        const snapshot = initialSnapshots.get(`snapshot:${decision.id}`);
        if (snapshot && snapshot.metadataValid && snapshot.restoresVersion !== initial.installedVersion &&
            snapshot.restorable !== false) {
          const currentSnapshot = currentSnapshots.get(`snapshot:${decision.id}`);
          if (!currentSnapshot || currentSnapshot.restorable === false) fail(`wayback:${decision.id}`);
        }
      }
    } catch (error) {
      if (error instanceof RetentionPlanChanged) throw error;
      throw new RetentionPlanChanged();
    }
  }

  /**
   * Crash-safe, audited removal. Before anything moves, this operation's
   * removal record (every planned item and every unaccounted trash entry,
   * each with its trash name) is written and fsynced. Items are then renamed
   * into the lifecycle trash (atomic on one volume; directories fsynced) and
   * deleted. The record stays until commitRetention runs after a durable
   * receipt names every item, so a crash or a failed receipt write at any
   * point leaves a record that the next apply finishes and reports as
   * recovered. Nothing is deleted while retention is blocked.
  */
  async retainSnapshots(input: { operationId: string; keep: number; apply: boolean }): Promise<LifecycleRetentionRecord> {
    if (this.keepAll && input.apply) {
      // Removes and recovers nothing; the read-only preview only informs the receipt.
      let wouldRemove: LifecycleRemovedItem[] | undefined;
      try {
        const preview = await this.retainSnapshots({ ...input, apply: false });
        // A blocked preview removes nothing for a reason; "would remove nothing" would hide it.
        if (preview.status === "preview") wouldRemove = preview.removed;
      } catch {
        wouldRemove = undefined;
      }
      return {
        keepSnapshots: input.keep,
        status: "skipped",
        skippedReason: "skipped_by_operator",
        removed: [],
        removedBytes: 0,
        recovered: [],
        keptSnapshots: [],
        keptVersions: [],
        ...(wouldRemove ? { wouldRemove } : {}),
      };
    }
    const { input: retention, blockedReason } = this.retentionInput();
    const plan = planLifecycleRetention(retention, input.keep);
    const snapshotBytes = new Map(retention.snapshots.map((snapshot) => [snapshot.id, snapshot.bytes]));
    const versionBytes = new Map(retention.versions.map((version) => [version.version, version.bytes]));
    const removed: LifecycleRemovedItem[] = [
      ...plan.snapshots.filter((row) => !row.keep)
        .map((row) => ({ kind: "snapshot" as const, name: row.id, bytes: snapshotBytes.get(row.id) ?? 0 })),
      ...plan.versions.filter((row) => !row.keep)
        .map((row) => ({ kind: "runtime_version" as const, name: row.version, bytes: versionBytes.get(row.version) ?? 0 })),
    ];
    const record = (
      status: LifecycleRetentionRecord["status"],
      items: LifecycleRemovedItem[],
      recovered: LifecycleRemovedItem[],
      resultPlan: LifecycleRetentionPlan = plan,
      restored: LifecycleRemovedItem[] = [],
    ): LifecycleRetentionRecord => ({
      keepSnapshots: input.keep,
      status,
      skippedReason: status === "skipped" ? blockedReason : null,
      removed: items,
      removedBytes: items.reduce((total, item) => total + item.bytes, 0),
      recovered,
      ...(restored.length > 0 ? { restored } : {}),
      keptSnapshots: status === "skipped" ? [] : resultPlan.snapshots.filter((row) => row.keep).map((row) => row.id),
      keptVersions: status === "skipped" ? [] : resultPlan.versions.filter((row) => row.keep).map((row) => row.version),
    });
    if (blockedReason) return record("skipped", [], []);
    if (!input.apply) return record("preview", removed, []);
    await this.assertFence(input.operationId);
    this.removeStaleTemporaries();

    const planned: RemovalItem[] = removed.map((item) => ({
      ...item,
      trashName: [item.kind, item.name, randomBytes(6).toString("hex")].join(TRASH_SEPARATOR),
      origin: "planned" as const,
    }));
    const initialStamps = new Map<string, string | null>();
    for (const snapshot of retention.snapshots) {
      const stamp = treeStamp(this.removalSource({ kind: "snapshot", name: snapshot.id, bytes: snapshot.bytes }));
      initialStamps.set(`snapshot:${snapshot.id}`, stamp);
    }
    for (const version of retention.versions) {
      const stamp = treeStamp(this.removalSource({ kind: "runtime_version", name: version.version, bytes: version.bytes }));
      initialStamps.set(`runtime_version:${version.version}`, stamp);
    }
    const moved = new Set<string>();
    const removalRecordPath = path.join(this.removalsRoot, `${input.operationId}.json`);
    let carriedRecord = false;
    let recovered: LifecycleRemovedItem[] = [];
    const undoMoved = () => {
      let complete = true;
      for (const item of [...planned].reverse()) {
        const itemKey = `${item.kind}:${item.name}`;
        if (!moved.has(itemKey)) continue;
        const source = this.removalSource(item);
        const trash = path.join(this.trashRoot, item.trashName);
        try {
          if (lstatIfPresent(trash)) {
            if (lstatIfPresent(source)) {
              complete = false;
              continue;
            }
            fs.renameSync(trash, source);
          } else if (!lstatIfPresent(source)) {
            complete = false;
            continue;
          }
          moved.delete(itemKey);
        } catch {
          complete = false;
        }
      }
      if (complete && moved.size === 0) {
        if (lstatIfPresent(this.trashRoot)) fsyncDirectory(this.trashRoot);
        if (planned.some((item) => item.kind === "snapshot") && lstatIfPresent(this.snapshotsRoot)) fsyncDirectory(this.snapshotsRoot);
        if (planned.some((item) => item.kind === "runtime_version") && lstatIfPresent(this.versionsRoot)) fsyncDirectory(this.versionsRoot);
      }
      return complete && moved.size === 0;
    };
    const abortUnsafeRetention = (error: unknown): never => {
      this.pendingCommits.delete(input.operationId);
      // Put a partially moved item back before returning the refusal. This
      // keeps the snapshot usable as a way back and leaves no false progress
      // for a follow-up prune to delete. If undo itself fails, retain the
      // durable record so the next prune can recover it.
      const undone = undoMoved();
      // If no item was moved or recovered, this operation's record must not
      // claim work that did not happen. A carried record from an earlier
      // same-ID attempt is retained even when this retry moved nothing.
      if (undone && recovered.length === 0 && !carriedRecord) {
        fs.rmSync(removalRecordPath, { force: true });
        if (lstatIfPresent(this.removalsRoot)) fsyncDirectory(this.removalsRoot);
      }
      if (error instanceof RetentionPlanChanged) throw error;
      throw error;
    };

    // Finish what earlier operations recorded but no receipt accounts for.
    const committed: string[] = [];
    try {
      const earlier = this.removalRecords().records;
      carriedRecord = earlier.some((pending) => pending.operationId === input.operationId && pending.items.length > 0);

      // A crash can leave a snapshot in trash after the runtime of the newer
      // kept snapshot disappears. In that state the trash entry may be the
      // only usable way back. Restore the whole recorded move before any
      // cleanup, then finish this prune with a recovery receipt. The next
      // prune sees the restored rows and can make a fresh, safe decision.
      const hasUsableWayBack = retention.snapshots.some((snapshot) =>
        snapshot.metadataValid && snapshot.restoresVersion !== null &&
        snapshot.restoresVersion !== retention.installedVersion && snapshot.restorable !== false);
      const pendingWayBack = earlier.flatMap((pending) => pending.items)
        .find((item) => item.kind === "snapshot" && lstatIfPresent(path.join(this.trashRoot, item.trashName)));
      if (!hasUsableWayBack && pendingWayBack) {
        const restored: LifecycleRemovedItem[] = [];
        let complete = true;
        for (const pending of earlier) {
          for (const item of [...pending.items].reverse()) {
            const trash = path.join(this.trashRoot, item.trashName);
            const source = this.removalSource(item);
            if (lstatIfPresent(trash)) {
              await this.assertFence(input.operationId);
              if (lstatIfPresent(source)) {
                complete = false;
                continue;
              }
              try {
                fs.renameSync(trash, source);
                restored.push({ kind: item.kind, name: item.name, bytes: item.bytes });
              } catch {
                complete = false;
              }
            }
            if (lstatIfPresent(trash) || !lstatIfPresent(source)) complete = false;
          }
        }
        if (!complete || restored.length === 0) {
          throw new RetentionPlanChanged("needed_restore_incomplete");
        }
        if (lstatIfPresent(this.trashRoot)) fsyncDirectory(this.trashRoot);
        if (restored.some((item) => item.kind === "snapshot") && lstatIfPresent(this.snapshotsRoot)) fsyncDirectory(this.snapshotsRoot);
        if (restored.some((item) => item.kind === "runtime_version") && lstatIfPresent(this.versionsRoot)) fsyncDirectory(this.versionsRoot);
        const refreshed = this.retentionInput();
        const usableRestoredWayBack = refreshed.input.snapshots.some((snapshot) =>
          snapshot.metadataValid && snapshot.restoresVersion !== null &&
          snapshot.restoresVersion !== refreshed.input.installedVersion && snapshot.restorable !== false);
        if (refreshed.blockedReason || !usableRestoredWayBack) {
          throw new RetentionPlanChanged("needed_restore_unusable");
        }
        for (const pending of earlier) {
          fs.rmSync(path.join(this.removalsRoot, `${pending.operationId}.json`), { force: true });
        }
        if (lstatIfPresent(this.removalsRoot)) fsyncDirectory(this.removalsRoot);
        const refreshedPlan = planLifecycleRetention(refreshed.input, input.keep);
        return record("applied", [], [], refreshedPlan, restored);
      }
      // A refused retry must leave earlier trash untouched. Validate planned
      // removals before finishing an earlier record under the same operation ID.
      this.assertRetentionPlanStillSafe(retention, plan, input.keep, planned, moved, initialStamps);
      for (const pending of earlier) {
        const alreadyRecorded = this.receiptNames(pending.operationId, pending.items);
        for (const item of pending.items) {
          if (lstatIfPresent(path.join(this.trashRoot, item.trashName))) {
            this.assertRetentionPlanStillSafe(retention, plan, input.keep, planned, moved, initialStamps);
            await this.deleteTrashEntry(input.operationId, item.trashName);
          } else if (lstatIfPresent(this.removalSource(item))) {
            continue; // never moved: nothing was removed, and retention decides it afresh
          }
          if (!alreadyRecorded) recovered.push({ kind: item.kind, name: item.name, bytes: item.bytes });
        }
        committed.push(pending.operationId);
      }
      const accounted = new Set(earlier.flatMap((pending) => pending.items.map((item) => item.trashName)));
      const orphans: RemovalItem[] = this.trashEntries().flatMap((entry) =>
        accounted.has(entry.fileName) ? [] : [{ ...entry.item, trashName: entry.fileName, origin: "orphan" as const }]);
      // A retried operation (same ID) replaces its own earlier record, so that
      // record's items are carried over until this receipt names them.
      const carried = earlier.find((pending) => pending.operationId === input.operationId)?.items ?? [];
      const items = [...planned, ...orphans, ...carried];
      if (items.length > 0) {
        await this.assertFence(input.operationId);
        writeJsonDurable(removalRecordPath,
          { schemaVersion: 1, operationId: input.operationId, items } satisfies RemovalRecord, this.root);
        committed.push(input.operationId);
      }
      const report = (item: RemovalItem): LifecycleRemovedItem => ({ kind: item.kind, name: item.name, bytes: item.bytes });
      const reportedRecovered = [...recovered, ...orphans.map(report)];
      this.pendingCommits.set(input.operationId, { records: committed, named: [...planned.map(report), ...reportedRecovered] });
      for (const item of planned) {
        await this.assertFence(input.operationId);
        this.assertRetentionPlanStillSafe(retention, plan, input.keep, planned, moved, initialStamps);
        const source = this.removalSource(item);
        assertNoSymlink(source, this.root);
        if (!fs.lstatSync(source).isDirectory()) throw new RetentionPlanChanged();
        ensureDirectory(this.trashRoot, this.root);
        try {
          fs.renameSync(source, path.join(this.trashRoot, item.trashName));
        } catch {
          throw new RetentionPlanChanged();
        }
        moved.add(`${item.kind}:${item.name}`);
      }
      if (planned.length > 0) {
        fsyncDirectory(this.trashRoot);
        if (planned.some((item) => item.kind === "snapshot")) fsyncDirectory(this.snapshotsRoot);
        if (planned.some((item) => item.kind === "runtime_version")) fsyncDirectory(this.versionsRoot);
      }
      for (const item of items) {
        this.assertRetentionPlanStillSafe(retention, plan, input.keep, planned, moved, initialStamps);
        await this.deleteTrashEntry(input.operationId, item.trashName);
      }
      return record("applied", planned.map(report), reportedRecovered);
    } catch (error) {
      if (error instanceof RetentionPlanChanged) return abortUnsafeRetention(error);
      throw error;
    }
  }

  /**
   * Temporaries a crash can leave behind: an intent or order record written
   * but never renamed into place. Nothing ever read them, so they are
   * removed without a record.
   */
  private staleTemporaries() {
    const found: string[] = [];
    if (lstatIfPresent(this.removalsRoot)?.isDirectory()) {
      assertNoSymlink(this.removalsRoot, this.root);
      for (const name of fs.readdirSync(this.removalsRoot)) {
        if (name.endsWith(".json.tmp")) found.push(path.join(this.removalsRoot, name));
      }
    }
    if (lstatIfPresent(`${this.orderPath}.tmp`)) found.push(`${this.orderPath}.tmp`);
    return found.filter((file) => lstatIfPresent(file)?.isFile());
  }

  private removeStaleTemporaries() {
    for (const file of this.staleTemporaries()) {
      assertNoSymlink(file, this.root);
      fs.rmSync(file, { force: true });
    }
  }

  async reconcileRetention(input: { operationId: string; keep: readonly string[] | null; apply: boolean; force?: boolean }): Promise<LifecycleReconcileRecord> {
    if (!lstatIfPresent(this.root)) throw new Error("there is no lifecycle directory to reconcile");
    if (await this.readJournal()) throw new Error("lifecycle recovery is required before reconcile");
    const { input: retention, blockedReason, createdAt, unusable } = this.retentionInput();
    const evidence = this.orderEvidence();
    const removals = this.removalRecords();
    const snapshotIds = retention.snapshots.map((snapshot) => snapshot.id);
    const known = new Set(evidence.markers.operations.map((operation) => operation.id));
    const snapshotsWithoutReceipt = snapshotIds.filter((id) => !known.has(id));
    const withoutSequence = new Set(evidence.markers.withoutSequence);
    const temporaries = this.staleTemporaries();
    const quarantined = removals.invalid.map((name) => {
      const file = path.join(this.removalsRoot, name);
      // Never follows a link or reads a directory: those are only moved aside.
      if (!lstatIfPresent(file)?.isFile()) return { name, bytes: 0, sha256: "not_a_regular_file" };
      const bytes = fs.readFileSync(file);
      return { name, bytes: bytes.length, sha256: `sha256:${createHash("sha256").update(bytes).digest("hex")}` };
    });
    // Provable: the order record is lost or damaged, but every receipt is
    // readable and sequenced, no two share a sequence, every snapshot has
    // one, and the sequences agree with the version chain. The receipts' own
    // sequences are then the whole order.
    const rebuildable = evidence.order.state !== "valid" && evidence.markers.invalid.length === 0 &&
      evidence.markers.operations.every((operation) => operation.sequence !== null) &&
      evidence.duplicated.length === 0 && snapshotsWithoutReceipt.length === 0 &&
      sequencesFollowVersionChain(evidence.markers.operations, retention.installedVersion);
    // Without a keep-set: nothing, a rebuild, or only the operator can decide:
    // the order cannot be proved, or retention keeps a snapshot it cannot
    // order or whose receipt it cannot read, which would otherwise stay for ever.
    const undecided = planLifecycleRetention(retention, LIFECYCLE_RETAINED_SNAPSHOTS).snapshots
      .filter((row) => row.reason === "operation_unknown" || row.reason === "receipt_without_sequence" ||
        row.reason === "completion_order_unproven")
      .map((row) => row.id);
    const neededRepair: LifecycleReconcileRecord["neededRepair"] = !evidence.proven ? (rebuildable ? "rebuilt" : "needs_keep")
      : undecided.length > 0 ? "needs_keep"
      : "none";
    const repair: LifecycleReconcileRecord["repair"] = input.keep ? "sealed" : neededRepair;
    const keep = [...input.keep ?? []];
    const missing = keep.filter((id) => !snapshotIds.includes(id));
    if (missing.length > 0) throw new Error(`--keep-snapshots names snapshots that do not exist: ${missing.join(", ")}`);
    // A way back restores an earlier version and actually can: the first
    // install's snapshot restores none, and one whose own files or runtime are
    // gone cannot. A seal never releases every usable way back, even forced.
    const candidates = retention.snapshots.filter((snapshot) => snapshot.metadataValid && snapshot.restoresVersion !== null &&
      snapshot.restoresVersion !== retention.installedVersion);
    const unusableWaysBack = candidates.flatMap((snapshot) => unusable.has(snapshot.id)
      ? [{ id: snapshot.id, restoresVersion: snapshot.restoresVersion!, reason: unusable.get(snapshot.id)! }] : []);
    const waysBack = candidates.filter((snapshot) => !unusable.has(snapshot.id)).map((snapshot) => snapshot.id);
    if (input.keep && waysBack.length > 0 && !keep.some((id) => waysBack.includes(id))) {
      const keptButUnusable = unusableWaysBack.filter((row) => keep.includes(row.id))
        .map((row) => `${row.id} cannot restore ${row.restoresVersion}: ${row.reason}`);
      throw new Error("--keep-snapshots must keep a way back to an earlier version, a snapshot that can actually restore: " +
        `name at least one of ${waysBack.join(", ")}${keptButUnusable.length > 0 ? ` (${keptButUnusable.join("; ")})` : ""}`);
    }
    const newestWayBack = [...waysBack].sort((left, right) =>
      (createdAt.get(right) ?? "").localeCompare(createdAt.get(left) ?? ""))[0] ?? null;
    const newestWayBackReleased = input.keep !== null && newestWayBack !== null && !keep.includes(newestWayBack);
    const sequence = Math.max(evidence.record?.lastSequence ?? 0, ...evidence.sequenced.map((operation) => operation.sequence!));
    const record: LifecycleReconcileRecord = {
      status: input.apply ? "applied" : "preview",
      blockedBefore: blockedReason,
      findings: {
        orderRecord: evidence.order.state,
        duplicateSequences: evidence.duplicated,
        sequencesBeyondRecord: evidence.beyondRecord,
        unsequencedAfterSequencing: evidence.unsequencedAfter,
        unreadableReceipts: evidence.markers.invalid.filter((id) => !withoutSequence.has(id)),
        receiptsWithoutSequence: [...withoutSequence],
        snapshotsWithoutReceipt,
        undecidedSnapshots: undecided,
        unreadableRemovalRecords: removals.invalid,
        staleTemporaries: temporaries.length,
      },
      repair,
      neededRepair,
      forced: input.keep !== null && input.force === true && (neededRepair !== "needs_keep" || newestWayBackReleased),
      keep,
      released: repair === "sealed" ? snapshotIds.filter((id) => !keep.includes(id)) : [],
      newestWayBack,
      newestWayBackReleased,
      unusableWaysBack,
      quarantined,
      lastSequence: repair === "none" ? evidence.record?.lastSequence ?? null : sequence,
    };
    if (!input.apply) return record;
    if (repair === "needs_keep") {
      throw new Error("these snapshots cannot be ordered from provable facts: review `plimsoll lifecycle snapshots " +
        "reconcile` and name the snapshots to keep with --keep-snapshots ID[,ID...]");
    }
    if (repair === "sealed" && !input.force) {
      if (neededRepair !== "needs_keep") {
        throw new Error(neededRepair === "none"
          ? "nothing needs a keep-set: the completion order is proven and every snapshot has a readable receipt " +
            "(use `snapshots prune`, or pass --force to seal anyway)"
          : "nothing needs a keep-set: the completion order can be rebuilt from the receipts " +
            "(run `snapshots reconcile --apply` without --keep-snapshots, or pass --force to seal anyway)");
      }
      if (newestWayBackReleased) {
        throw new Error(`--keep-snapshots would release ${newestWayBack}, the newest snapshot that restores an earlier ` +
          "version: add it to the keep-set, or pass --force");
      }
    }
    await this.assertFence(input.operationId);
    for (const { name } of quarantined) {
      ensureDirectory(this.unreadableRemovalsRoot, this.root);
      const source = path.join(this.removalsRoot, name);
      assertNoSymlink(source, this.root);
      fs.renameSync(source, path.join(this.unreadableRemovalsRoot, `${name}.${randomBytes(6).toString("hex")}`));
    }
    if (quarantined.length > 0) {
      fsyncDirectory(this.unreadableRemovalsRoot);
      fsyncDirectory(this.removalsRoot);
    }
    this.removeStaleTemporaries();
    if (repair === "rebuilt") {
      await this.assertFence(input.operationId);
      writeJsonDurable(this.orderPath, { schemaVersion: 1, lastSequence: sequence, legacyOperations: [] } satisfies CompletionOrderRecord, this.root);
    } else if (repair === "sealed") {
      // Every receipt present now predates every completion sequenced from here on.
      await this.assertFence(input.operationId);
      writeJsonDurable(this.orderPath, {
        schemaVersion: 1,
        lastSequence: sequence,
        legacyOperations: evidence.markers.ids,
        seal: { operationId: input.operationId, sequence, keep, covered: snapshotIds },
      } satisfies CompletionOrderRecord, this.root);
    }
    return record;
  }

  /**
   * Called after the receipt of a retention apply is persisted. Only when that
   * durable receipt names every removed and recovered item are the removal
   * records it accounts for deleted; otherwise they stay for the next apply.
   */
  async commitRetention(operationId: string) {
    const pending = this.pendingCommits.get(operationId);
    this.pendingCommits.delete(operationId);
    if (!pending || !this.receiptNames(operationId, pending.named)) return;
    for (const id of pending.records) {
      const file = path.join(this.removalsRoot, `${id}.json`);
      assertNoSymlink(file, this.root);
      fs.rmSync(file, { force: true });
    }
  }
}
