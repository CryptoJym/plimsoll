import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import {
  LIFECYCLE_SCHEMA_VERSION,
  LifecycleInterruption,
  PURGE_CONFIRMATION,
  immutableRuntimeRelativePath,
  parseCompletionReceipt,
  planLifecycleRetention,
  type LifecycleAdapter,
  type LifecycleCloneFallback,
  type LifecycleCompletedOperation,
  type LifecycleJournal,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type LifecycleRemovedItem,
  type LifecycleRestoreRecord,
  type LifecycleRetentionInput,
  type LifecycleRetentionRecord,
  type LifecycleSnapshotInventory,
  type LifecycleSnapshotMethod,
  type LifecycleSnapshotPlan,
  type LifecycleSnapshotRecord,
  type LifecycleSupportSnapshot,
  type RuntimeArtifact,
} from "./lifecycle";
import { LifecycleMutationAuthority, type LifecycleMutationLease } from "./lifecycle-authority";

const FILE_MODE = 0o600;
const DIRECTORY_MODE = 0o700;
const EXECUTABLE_MODE = 0o700;
const MAX_RECEIPTS = 32;
const MAX_MARKER_BYTES = 64 * 1024;
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
};

/** SQLite implementations must use the online backup API or an equivalent
 * quiesced snapshot. Copying a live WAL database is not a compatible backup.
 * A bare boolean result (present or absent) records no snapshot method.
 * Restore must leave the live ledger untouched unless the restored copy is
 * complete and valid; it may report how the copy was made. */
export type LifecycleDatabaseAdapter = {
  snapshot(input: { source: string; destination: string }): Promise<boolean | LifecycleDatabaseSnapshot>;
  restore(input: { source: string; destination: string }): Promise<void | LifecycleDatabaseRestore>;
  /**
   * Removes the live ledger for a snapshot taken when no ledger existed, only
   * when no other connection has it open. Adapters without it get a plain
   * removal of the ledger files.
   */
  discard?(input: { destination: string }): Promise<void>;
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
    row.snapshotId === row.operationId;
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
};

const MAX_COMPLETION_MARKERS = 100_000;

function parseCompletionOrder(value: unknown): CompletionOrderRecord | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Record<string, unknown>;
  const keys = Object.keys(row).sort().join(",");
  if (keys !== "lastSequence,legacyOperations,schemaVersion" || row.schemaVersion !== 1) return null;
  if (typeof row.lastSequence !== "number" || !Number.isSafeInteger(row.lastSequence) || row.lastSequence < 0) return null;
  const legacy = row.legacyOperations;
  if (!Array.isArray(legacy) || legacy.length > MAX_COMPLETION_MARKERS || !legacy.every(isBoundedIdentifier) ||
      new Set(legacy).size !== legacy.length) return null;
  return { schemaVersion: 1, lastSequence: row.lastSequence, legacyOperations: [...legacy] };
}

/** A marker that is plainly not an update or rollback (uninstall, purge, support bundle, prune). */
function isOtherOperationReceipt(value: unknown, operationId: string) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return false;
  const row = value as Record<string, unknown>;
  return row.schemaVersion === 1 && row.operationId === operationId && row.toVersion === null &&
    row.restoredVersion === null &&
    ["uninstall", "purge", "support_bundle", "snapshots_prune"].includes(String(row.operation));
}

/**
 * What one operation committed to removing, written and fsynced before any
 * of it is moved or deleted and kept until a durable receipt names every
 * item. "planned": retention chose it; "orphan": a trash entry no record
 * accounted for.
 */
type RemovalItem = LifecycleRemovedItem & { trashName: string; origin: "planned" | "orphan" };
type RemovalRecord = { schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION; operationId: string; items: RemovalItem[] };

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
  /**
   * Operation → the removal records its receipt accounts for and the items
   * that receipt must name before those records may be deleted.
   */
  private readonly pendingCommits = new Map<string, { records: string[]; named: LifecycleRemovedItem[] }>();
  /** Operation-ID → held mutation lease. Bounded; one process holds at most a
   * handful of concurrent lifecycle operations. */
  private readonly fences = new Map<string, LifecycleMutationLease>();

  constructor(
    private readonly paths: ManagedLifecyclePaths,
    private readonly service: LifecycleServiceAdapter,
    private readonly database: LifecycleDatabaseAdapter,
    private readonly authority?: LifecycleMutationAuthority,
  ) {
    for (const [label, candidate] of Object.entries({
      lifecycleRoot: paths.lifecycleRoot,
      artifactSourceRoot: paths.artifactSourceRoot,
      collectorConfig: paths.collectorConfig,
      database: paths.database,
      serviceManifest: paths.serviceManifest,
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
    if (!this.authority) return;
    const lease = this.fences.get(operationId);
    if (!lease) {
      // Invariant violation: a fenced mutating step ran without holding the
      // mutation lease. Fail closed rather than act unowned.
      throw new Error(`lifecycle fence is not held for operation ${operationId}`);
    }
    const revalidation = lease.assertCurrent();
    if (!revalidation.ok) {
      throw new LifecycleInterruption(
        `lifecycle fence lost before a mutating step: ${revalidation.reason}`,
      );
    }
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
    try {
      for (const [index, file] of (artifact.files ?? []).entries()) {
        const destination = path.join(runtimeDirectory, ...file.relativePath.split("/"));
        assertAbsoluteOwnedPath(destination, this.root, `companion ${index} destination`);
        assertNoSymlink(path.dirname(destination), this.root);
        ensureDirectory(path.dirname(destination), this.root);
        if (fs.existsSync(destination)) {
          fs.rmSync(destination, { force: true });
        }
        // Companion sources live in the artifact staging area next to the
        // bundle; their absolute paths were validated when resolved.
        assertAbsoluteOwnedPath(file.sourcePath, this.paths.artifactSourceRoot, `companion ${index} source`);
        assertNoSymlink(file.sourcePath, this.paths.artifactSourceRoot);
        copyRegularFile(file.sourcePath, destination, FILE_MODE, this.root);
        if (sha256(destination) !== file.sha256) throw new Error(`companion ${index} digest mismatch`);
        stagedCompanions.push({ destination });
      }
      assertNoSymlink(path.dirname(target), this.root);
      ensureDirectory(path.dirname(target), this.root);
      if (fs.existsSync(target)) {
        const stat = fs.lstatSync(target);
        if (!stat.isFile() || stat.isSymbolicLink() || sha256(target) !== artifact.sha256) {
          throw new Error("immutable runtime target already differs");
        }
        fs.chmodSync(target, EXECUTABLE_MODE);
        return;
      }
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

  async restore(snapshotId: string) {
    this.initialize();
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
      });
      assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
      const restoredDatabase = lstatIfPresent(this.paths.database);
      if (!restoredDatabase?.isFile()) throw new Error("restored database must be a regular file");
      record = outcome
        ? { method: outcome.method, cloneFallback: outcome.cloneFallback, databaseBytes: outcome.databaseBytes }
        : undefined;
    } else if (this.database.discard) {
      await this.database.discard({ destination: this.paths.database });
    } else {
      fs.rmSync(this.paths.database, { force: true });
      fs.rmSync(`${this.paths.database}-wal`, { force: true });
      fs.rmSync(`${this.paths.database}-shm`, { force: true });
    }
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
      // it before the bounded display receipt so a stop between the two can
      // never make a completed destructive operation reusable. A verified or
      // rollback-complete journal can still reopen and finish receipt writing.
      // A refusal changed nothing, so its operation ID stays usable.
      writeJsonDurable(path.join(this.completedRoot, `${receipt.operationId}.json`), receipt, this.completedRoot);
    }
    writeJsonDurable(path.join(this.receiptsRoot, `${receipt.operationId}-${receipt.operation}.json`), receipt, this.receiptsRoot);
    const receipts = fs.readdirSync(this.receiptsRoot)
      .filter((entry) => entry.endsWith(".json"))
      .sort((left, right) => left.localeCompare(right));
    for (const stale of receipts.slice(0, Math.max(0, receipts.length - MAX_RECEIPTS))) {
      fs.rmSync(path.join(this.receiptsRoot, stale), { force: true });
    }
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
    const targets = ["collector_config", "workspace_credentials", "ledger", "history", "lifecycle_snapshots"] as const;
    if (!input.apply) return targets;
    if (input.confirmation !== PURGE_CONFIRMATION) throw new Error("purge confirmation mismatch");
    for (const candidate of [this.paths.collectorConfig, this.paths.database, ...this.paths.history]) {
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
    return targets;
  }

  supportSnapshot() {
    return this.service.supportSnapshot();
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
    const ids: string[] = [];
    if (!lstatIfPresent(this.completedRoot)) return { operations, invalid, ids };
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
        else if (!isOtherOperationReceipt(value, id)) invalid.push(id);
      } catch {
        invalid.push(id);
      }
    }
    return { operations, invalid, ids };
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
        // A lost or damaged order record is never rebuilt from guesses.
        return null;
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

  /** Read-only view of everything retention decides over. */
  private retentionInput() {
    let blockedReason: LifecycleSnapshotInventory["blockedReason"] = null;
    const empty: LifecycleRetentionInput = {
      installedVersion: null, pinnedVersions: [], journal: null, operations: [],
      order: { proven: true, legacyChain: false }, snapshots: [], versions: [],
    };
    if (!lstatIfPresent(this.root)) return { input: empty, blockedReason, createdAt: new Map<string, string | null>() };
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
    const snapshots = this.childDirectories(this.snapshotsRoot).map((id) => {
      const directory = path.join(this.snapshotsRoot, id);
      const metadata = this.readSnapshotMetadata(directory);
      const createdAtMs = metadata?.createdAt !== undefined && Number.isFinite(Date.parse(metadata.createdAt))
        ? Date.parse(metadata.createdAt)
        : (lstatIfPresent(path.join(directory, "snapshot.json")) ?? fs.lstatSync(directory)).mtimeMs;
      createdAt.set(id, new Date(createdAtMs).toISOString());
      return {
        id,
        bytes: treeBytes(directory),
        metadataValid: metadata !== null,
        restoresVersion: metadata?.currentVersion ?? null,
        method: metadata ? snapshotRecordFrom(metadata, 0).method : null,
      };
    });
    const versions = this.childDirectories(this.versionsRoot).map((version) => ({
      version,
      bytes: treeBytes(path.join(this.versionsRoot, version)),
    }));

    // Completion order: durable sequences, checked against the order record.
    // Missing, duplicated or contradictory order evidence keeps everything.
    const markers = this.completionMarkers();
    const order = this.completionOrder();
    const sequences = markers.operations.flatMap((operation) => operation.sequence === null ? [] : [operation.sequence]);
    const predates = new Set(order.state === "valid" ? order.record.legacyOperations : []);
    let proven = new Set(sequences).size === sequences.length;
    if (order.state === "invalid" || (order.state === "absent" && sequences.length > 0)) proven = false;
    if (order.state === "valid") {
      if (sequences.some((sequence) => sequence > order.record.lastSequence)) proven = false;
      // A pre-sequencing receipt that appeared after sequencing began (an
      // older collector ran an update later) has no provable place.
      if (markers.operations.some((operation) => operation.sequence === null && !predates.has(operation.id))) proven = false;
    }
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
        order: { proven, legacyChain }, snapshots, versions,
      },
      blockedReason,
      createdAt,
    };
  }

  /** Entries an earlier removal renamed into the trash but had not deleted yet. */
  private trashEntries() {
    const trash = lstatIfPresent(this.trashRoot);
    if (!trash) return [];
    assertNoSymlink(this.trashRoot, this.root);
    if (!trash.isDirectory()) throw new Error("lifecycle trash must be a directory");
    const entries: Array<{ fileName: string; item: LifecycleRemovedItem }> = [];
    for (const fileName of fs.readdirSync(this.trashRoot).sort()) {
      const [kind, name] = fileName.split(TRASH_SEPARATOR);
      if ((kind === "snapshot" || kind === "runtime_version") && isBoundedIdentifier(name)) {
        entries.push({ fileName, item: { kind, name, bytes: treeBytes(path.join(this.trashRoot, fileName)) } });
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
    fs.rmSync(path.join(this.trashRoot, trashName), { recursive: true, force: true });
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
    ): LifecycleRetentionRecord => ({
      keepSnapshots: input.keep,
      status,
      skippedReason: status === "skipped" ? blockedReason : null,
      removed: items,
      removedBytes: items.reduce((total, item) => total + item.bytes, 0),
      recovered,
      keptSnapshots: status === "skipped" ? [] : plan.snapshots.filter((row) => row.keep).map((row) => row.id),
      keptVersions: status === "skipped" ? [] : plan.versions.filter((row) => row.keep).map((row) => row.version),
    });
    if (blockedReason) return record("skipped", [], []);
    if (!input.apply) return record("preview", removed, []);

    // Finish what earlier operations recorded but no receipt accounts for.
    const recovered: LifecycleRemovedItem[] = [];
    const committed: string[] = [];
    const earlier = this.removalRecords().records;
    for (const pending of earlier) {
      const alreadyRecorded = this.receiptNames(pending.operationId, pending.items);
      for (const item of pending.items) {
        if (lstatIfPresent(path.join(this.trashRoot, item.trashName))) {
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
    const planned: RemovalItem[] = removed.map((item) => ({
      ...item,
      trashName: [item.kind, item.name, randomBytes(6).toString("hex")].join(TRASH_SEPARATOR),
      origin: "planned" as const,
    }));
    // A retried operation (same ID) replaces its own earlier record, so that
    // record's items are carried over until this receipt names them.
    const carried = earlier.find((pending) => pending.operationId === input.operationId)?.items ?? [];
    const items = [...planned, ...orphans, ...carried];
    if (items.length > 0) {
      await this.assertFence(input.operationId);
      writeJsonDurable(path.join(this.removalsRoot, `${input.operationId}.json`),
        { schemaVersion: 1, operationId: input.operationId, items } satisfies RemovalRecord, this.root);
      committed.push(input.operationId);
    }
    const report = (item: RemovalItem): LifecycleRemovedItem => ({ kind: item.kind, name: item.name, bytes: item.bytes });
    const reportedRecovered = [...recovered, ...orphans.map(report)];
    this.pendingCommits.set(input.operationId, { records: committed, named: [...planned.map(report), ...reportedRecovered] });
    for (const item of planned) {
      await this.assertFence(input.operationId);
      const source = this.removalSource(item);
      assertNoSymlink(source, this.root);
      if (!fs.lstatSync(source).isDirectory()) throw new Error("retention target must be a directory");
      ensureDirectory(this.trashRoot, this.root);
      fs.renameSync(source, path.join(this.trashRoot, item.trashName));
    }
    if (planned.length > 0) {
      fsyncDirectory(this.trashRoot);
      if (planned.some((item) => item.kind === "snapshot")) fsyncDirectory(this.snapshotsRoot);
      if (planned.some((item) => item.kind === "runtime_version")) fsyncDirectory(this.versionsRoot);
    }
    for (const item of items) await this.deleteTrashEntry(input.operationId, item.trashName);
    return record("applied", planned.map(report), reportedRecovered);
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
