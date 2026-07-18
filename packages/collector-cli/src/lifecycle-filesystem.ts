import { createHash } from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import {
  LIFECYCLE_SCHEMA_VERSION,
  PURGE_CONFIRMATION,
  immutableRuntimeRelativePath,
  type LifecycleAdapter,
  type LifecycleJournal,
  type LifecycleReadiness,
  type LifecycleReceipt,
  type LifecycleSupportSnapshot,
  type RuntimeArtifact,
} from "./lifecycle";

const FILE_MODE = 0o600;
const DIRECTORY_MODE = 0o700;
const EXECUTABLE_MODE = 0o700;
const MAX_RECEIPTS = 32;

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

/** SQLite implementations must use the online backup API or an equivalent
 * quiesced snapshot. Copying a live WAL database is not a compatible backup. */
export type LifecycleDatabaseAdapter = {
  snapshot(input: { source: string; destination: string }): Promise<boolean>;
  restore(input: { source: string; destination: string }): Promise<void>;
};

type SnapshotMetadata = {
  schemaVersion: typeof LIFECYCLE_SCHEMA_VERSION;
  currentVersion: string | null;
  currentExecutable: string | null;
  present: Record<"config" | "database" | "service", boolean>;
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

export class FilesystemLifecycleAdapter implements LifecycleAdapter {
  private readonly root: string;
  private readonly versionsRoot: string;
  private readonly snapshotsRoot: string;
  private readonly receiptsRoot: string;
  private readonly completedRoot: string;
  private readonly statePath: string;
  private readonly journalPath: string;
  private readonly lockPath: string;
  private readonly currentPath: string;

  constructor(
    private readonly paths: ManagedLifecyclePaths,
    private readonly service: LifecycleServiceAdapter,
    private readonly database: LifecycleDatabaseAdapter,
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
    this.statePath = path.join(this.root, "state.json");
    this.journalPath = path.join(this.root, "journal.json");
    this.lockPath = path.join(this.root, "operation.lock");
    this.currentPath = path.join(this.root, "current");
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
    if (!fs.existsSync(this.lockPath)) return;
    const owner = readJson<{ operationId?: string }>(path.join(this.lockPath, "owner.json"));
    if (owner?.operationId !== operationId) return;
    fs.rmSync(this.lockPath, { recursive: true, force: true });
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
      present.database = await this.database.snapshot({
        source: this.paths.database,
        destination: path.join(snapshot, "database"),
      });
      assertNoSymlink(snapshot, this.snapshotsRoot);
      if (present.database) {
        const databaseSnapshot = path.join(snapshot, "database");
        assertNoSymlink(databaseSnapshot, snapshot);
        if (!fs.lstatSync(databaseSnapshot).isFile()) throw new Error("database snapshot must be a regular file");
        fs.chmodSync(databaseSnapshot, FILE_MODE);
      }
      const metadata: SnapshotMetadata = {
        schemaVersion: 1,
        currentVersion: state?.version ?? null,
        currentExecutable: state?.executablePath ?? null,
        present,
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
    const target = path.join(this.root, immutableRuntimeRelativePath(artifact));
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
  }

  async switchTo(artifact: RuntimeArtifact) {
    const executablePath = path.join(this.root, immutableRuntimeRelativePath(artifact));
    const stat = fs.lstatSync(executablePath);
    if (!stat.isFile() || stat.isSymbolicLink() || sha256(executablePath) !== artifact.sha256) {
      throw new Error("staged runtime is not immutable or does not match");
    }
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
    const entries = [
      ["config", this.paths.collectorConfig],
      ["service", this.paths.serviceManifest],
    ] as const;
    for (const [label, destination] of entries) {
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
    }
    const databaseSnapshot = path.join(snapshot, "database");
    assertNoSymlink(databaseSnapshot, snapshot);
    assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
    if (metadata.present.database) {
      const databaseSource = lstatIfPresent(databaseSnapshot);
      if (!databaseSource?.isFile()) throw new Error("rollback database snapshot must be a regular file");
      await this.database.restore({
        source: databaseSnapshot,
        destination: this.paths.database,
      });
      assertNoSymlink(this.paths.database, this.paths.ownershipRoot);
      const restoredDatabase = lstatIfPresent(this.paths.database);
      if (!restoredDatabase?.isFile()) throw new Error("restored database must be a regular file");
    } else {
      fs.rmSync(this.paths.database, { force: true });
      fs.rmSync(`${this.paths.database}-wal`, { force: true });
      fs.rmSync(`${this.paths.database}-shm`, { force: true });
    }
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
  }

  async persistReceipt(receipt: LifecycleReceipt) {
    this.initialize();
    if (receipt.status !== "rollback_required") {
      // The durable, unbounded marker is the operation-ID authority. Commit
      // it before the bounded display receipt so a stop between the two can
      // never make a completed destructive operation reusable. A verified or
      // rollback-complete journal can still reopen and finish receipt writing.
      writeJson(path.join(this.completedRoot, `${receipt.operationId}.json`), receipt, this.completedRoot);
    }
    writeJson(path.join(this.receiptsRoot, `${receipt.operationId}-${receipt.operation}.json`), receipt, this.receiptsRoot);
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
    const targets = ["collector_config", "ledger", "history", "lifecycle_snapshots"] as const;
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
    return targets;
  }

  supportSnapshot() {
    return this.service.supportSnapshot();
  }
}
