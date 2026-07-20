import { spawnSync } from "node:child_process";
import { createHash, randomUUID } from "node:crypto";
import fs from "node:fs";
import net from "node:net";
import path from "node:path";

// Long enough for the existing synchronous ledger open/prune path, but finite:
// launchd's throttled retry can recover an abandoned lock after two minutes.
export const START_LOCK_LEASE_MS = 120_000;
const OWNER_STABILITY_DELAY_MS = 100;

export type ProcessIdentity = Readonly<{
  pid: number;
  processStartFingerprint: string;
}>;

export type CollectorRuntimeIdentity = ProcessIdentity & Readonly<{
  instanceId: string;
}>;

export type CollectorListenerObservation =
  | { kind: "absent" }
  | { kind: "collector"; runtimeIdentity: CollectorRuntimeIdentity }
  | { kind: "unrelated" }
  | { kind: "indeterminate" };

export type LaunchAgentLabelObservation =
  | { kind: "reported"; processIdentity: ProcessIdentity | null }
  | { kind: "not_reported" }
  | { kind: "query_failed" };

export type LaunchAgentUnloadPriorState = Readonly<{
  label: LaunchAgentLabelObservation;
  listener: CollectorListenerObservation;
  listenerRuntimeIdentity: CollectorRuntimeIdentity | null;
  ownership: "consistent" | "ambiguous" | "unproven" | "unrelated";
  pidRecordKind: CollectorPidFileRead["kind"];
  pidRuntimeIdentity: CollectorRuntimeIdentity | null;
  pidCleanupState: CollectorPidCleanupState;
  pidCleanupReconciliation: CollectorPidCleanupReconciliationResult;
}>;

export type LaunchAgentUnloadState =
  | "stopped"
  | "still_stopping"
  | "live_conflict"
  | "stale_owned_record"
  | "unrelated_listener"
  | "ambiguous_prior_owner"
  | "indeterminate";

export type LaunchAgentUnloadObservation = Readonly<{
  state: LaunchAgentUnloadState;
  stopped: boolean;
  labelState: LaunchAgentLabelObservation["kind"];
  listenerState: CollectorListenerObservation["kind"];
  pidRecordState: CollectorPidFileRead["kind"];
  pidCleaned: boolean;
  removedPidFile: boolean;
  pidCleanupAmbiguous: boolean;
  pidCleanupQuarantined: boolean;
  pidCleanupMarkerState: CollectorPidCleanupState["markerState"];
  pidCleanupClaimCount: number;
  pidCleanupQuarantineCount: number;
  pidCleanupInventoryTruncated: boolean;
  priorRuntimeLive: boolean;
  priorRuntimeCount: number;
  currentListenerMatchesPrior: boolean;
  currentPidMatchesPrior: boolean;
  currentListenerRuntimeIdentity: CollectorRuntimeIdentity | null;
  currentPidRuntimeIdentity: CollectorRuntimeIdentity | null;
}>;

export type LaunchAgentUnloadOutcome = Readonly<{
  stopped: boolean;
  state: LaunchAgentUnloadState;
  pidCleaned: boolean;
  removedPidFile: boolean;
  pidCleanupAmbiguous: boolean;
  pidCleanupQuarantined: boolean;
  final: LaunchAgentUnloadObservation;
  timing: {
    timeoutMs: number;
    pollIntervalMs: number;
    elapsedMs: number;
    observations: number;
    deadlineCrossed: boolean;
    finalObservationPerformed: true;
  };
}>;

export type CollectorPidRecord = CollectorRuntimeIdentity & {
  command: string[];
  cwd: string;
  label: string;
  startedAt: string;
  version: 2;
};

export type CollectorPidFileRead =
  | { kind: "current"; fileIdentity: PidFileIdentity; raw: string; record: CollectorPidRecord }
  | { kind: "invalid"; fileIdentity: PidFileIdentity; raw: string }
  | { kind: "legacy"; fileIdentity: PidFileIdentity; pid: number; raw: string }
  | { kind: "unsafe"; reason: PidFileUnsafeReason }
  | { kind: "missing" };

export type PidFileIdentity = Readonly<{
  device: number;
  inode: number;
  mode: number;
  uid: number;
  gid: number;
  links: number;
  size: number;
  modifiedMs: number;
  changedMs: number;
}>;

export type PidFileUnsafeReason =
  | "ancestor_symlink"
  | "leaf_symlink"
  | "nonregular"
  | "link_count"
  | "owner"
  | "mode"
  | "identity_changed"
  | "unreadable";

export type CollectorPidCleanupResult = Readonly<{
  removed: boolean;
  ambiguous: boolean;
  quarantined: boolean;
  persistent: CollectorPidCleanupState;
  disposition:
    | "removed"
    | "not_owned"
    | "persistent_ambiguity"
    | "preclaim_changed"
    | "mismatch_quarantined"
    | "claim_missing"
    | "claim_changed"
    | "destination_reappeared"
    | "marker_create_failed"
    | "marker_clear_failed"
    | "operation_failed";
}>;

export type CollectorPidCleanupState = Readonly<{
  ambiguous: boolean;
  markerState: "missing" | "present" | "unsafe";
  claimCount: number;
  quarantineCount: number;
  inventoryTruncated: boolean;
  unsafeArtifactCount: number;
}>;

export type CollectorPidCleanupReconciliationResult = Readonly<{
  reconciled: boolean;
  eligible: boolean;
  disposition:
    | "clean"
    | "eligible_dead_actor"
    | "actor_live"
    | "actor_indeterminate"
    | "pid_claim_present"
    | "quarantine_present"
    | "unsafe_artifact"
    | "multiple_marker_slots"
    | "marker_cleared"
    | "marker_claim_cleared"
    | "clear_race";
  before: CollectorPidCleanupState;
  after: CollectorPidCleanupState;
}>;

export type CollectorPidCleanupReconciliationHooks = Readonly<{
  beforeMarkerClaim?: () => void;
  beforeClear?: () => void;
}>;

export type CollectorPidCleanupHooks = Readonly<{
  beforeClaim?: () => void;
  afterPreClaimCheck?: () => void;
  beforeMismatchRestore?: () => void;
  afterClaim?: () => void;
}>;

type StartLockRecord = CollectorRuntimeIdentity & {
  createdAt: string;
  label: string;
  version: 2;
};

export type CollectorStartOwnership =
  | {
      kind: "already_running";
      pidPath: string;
      port: number;
      runtimeIdentity: CollectorRuntimeIdentity;
    }
  | {
      kind: "owner";
      lockPath: string;
      pidPath: string;
      release: () => void;
      writePidFile: (record: CollectorPidRecord) => void;
    };

export class CollectorStartOwnershipError extends Error {
  constructor(
    readonly code:
      | "process_fingerprint_unavailable"
      | "pid_cleanup_ambiguous"
      | "start_in_progress",
    message: string,
  ) {
    super(message);
    this.name = "CollectorStartOwnershipError";
  }
}

function processIsRunning(pid: number) {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

export function readProcessStartFingerprint(pid: number) {
  if (!Number.isInteger(pid) || pid <= 0) return null;
  const result = spawnSync("ps", ["-p", String(pid), "-o", "lstart="], {
    encoding: "utf8",
    env: { ...process.env, LANG: "C", LC_ALL: "C" },
    stdio: ["ignore", "pipe", "pipe"],
    timeout: 1_000,
  });
  const startedAt = result.status === 0 ? result.stdout.trim().replace(/\s+/g, " ") : "";
  if (!startedAt) return null;
  return (
    "sha256:" +
    createHash("sha256")
      .update("plimsoll-process-start-v1\0" + pid + "\0" + startedAt)
      .digest("hex")
  );
}

export function createCollectorRuntimeIdentity(): CollectorRuntimeIdentity {
  const processStartFingerprint = readProcessStartFingerprint(process.pid);
  if (!processStartFingerprint) {
    throw new CollectorStartOwnershipError(
      "process_fingerprint_unavailable",
      "Could not read the collector process start fingerprint.",
    );
  }
  return Object.freeze({
    instanceId: randomUUID(),
    pid: process.pid,
    processStartFingerprint,
  });
}

function parseCurrentPidRecord(raw: string, label: string): CollectorPidRecord | null {
  try {
    const parsed = JSON.parse(raw) as Partial<CollectorPidRecord>;
    if (
      parsed.version === 2 &&
      parsed.label === label &&
      Number.isInteger(parsed.pid) &&
      (parsed.pid ?? 0) > 0 &&
      typeof parsed.instanceId === "string" &&
      parsed.instanceId.length >= 32 &&
      typeof parsed.processStartFingerprint === "string" &&
      parsed.processStartFingerprint.startsWith("sha256:") &&
      Array.isArray(parsed.command) &&
      typeof parsed.cwd === "string" &&
      typeof parsed.startedAt === "string"
    ) {
      return parsed as CollectorPidRecord;
    }
  } catch {
    // Invalid ownership records never become authority.
  }
  return null;
}

type PidPathIdentity = PidFileIdentity & Readonly<{ path: string }>;

type InspectedPidFile =
  | { kind: "safe"; fileIdentity: PidFileIdentity; raw: string }
  | { kind: "unsafe"; reason: PidFileUnsafeReason }
  | { kind: "missing" };

function currentUid() {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

function pidFileIdentity(stat: fs.Stats): PidFileIdentity {
  return {
    device: stat.dev,
    inode: stat.ino,
    mode: stat.mode,
    uid: stat.uid,
    gid: stat.gid,
    links: stat.nlink,
    size: stat.size,
    modifiedMs: stat.mtimeMs,
    changedMs: stat.ctimeMs,
  };
}

function samePidFileIdentity(left: PidFileIdentity, right: PidFileIdentity) {
  return left.device === right.device &&
    left.inode === right.inode &&
    left.mode === right.mode &&
    left.uid === right.uid &&
    left.gid === right.gid &&
    left.links === right.links &&
    left.size === right.size &&
    left.modifiedMs === right.modifiedMs &&
    left.changedMs === right.changedMs;
}

function samePidFileObject(left: PidFileIdentity, right: PidFileIdentity) {
  return left.device === right.device && left.inode === right.inode;
}

function lstatPidPath(file: string) {
  try {
    return fs.lstatSync(file);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

function inspectPidAncestors(file: string):
  | { kind: "safe"; identities: PidPathIdentity[] }
  | { kind: "unsafe"; reason: PidFileUnsafeReason }
  | { kind: "missing" } {
  const absolutePath = path.resolve(file);
  const parsed = path.parse(absolutePath);
  const segments = absolutePath.slice(parsed.root.length).split(path.sep).filter(Boolean);
  const identities: PidPathIdentity[] = [];
  let cursor = parsed.root;
  let missing = false;
  try {
    for (const segment of segments.slice(0, -1)) {
      cursor = path.join(cursor, segment);
      if (missing) continue;
      const stat = lstatPidPath(cursor);
      if (!stat) {
        missing = true;
        continue;
      }
      if (stat.isSymbolicLink()) {
        // macOS exposes root-owned top-level compatibility aliases such as
        // /var and /tmp. No user-owned or nested alias is trusted.
        if (path.dirname(cursor) === parsed.root && stat.uid === 0) continue;
        return { kind: "unsafe", reason: "ancestor_symlink" };
      }
      if (!stat.isDirectory()) return { kind: "unsafe", reason: "nonregular" };
      const uid = currentUid();
      if (uid !== undefined && stat.uid !== uid && stat.uid !== 0) {
        return { kind: "unsafe", reason: "owner" };
      }
      const permissions = stat.mode & 0o7777;
      const trustedStickyRoot = stat.uid === 0 && (permissions & 0o1000) !== 0;
      if ((permissions & 0o022) !== 0 && !trustedStickyRoot) {
        return { kind: "unsafe", reason: "mode" };
      }
      identities.push({ path: cursor, ...pidFileIdentity(stat) });
    }
  } catch {
    return { kind: "unsafe", reason: "unreadable" };
  }
  return missing ? { kind: "missing" } : { kind: "safe", identities };
}

function samePidAncestors(left: readonly PidPathIdentity[], right: readonly PidPathIdentity[]) {
  return left.length === right.length && left.every((entry, index) => {
    const other = right[index];
    return Boolean(other && entry.path === other.path && samePidFileIdentity(entry, other));
  });
}

function validatePidLeaf(stat: fs.Stats): PidFileUnsafeReason | null {
  if (stat.isSymbolicLink()) return "leaf_symlink";
  if (!stat.isFile()) return "nonregular";
  if (stat.nlink !== 1) return "link_count";
  const uid = currentUid();
  if (uid !== undefined && stat.uid !== uid) return "owner";
  if ((stat.mode & 0o7777) !== 0o600) return "mode";
  return null;
}

function readPidDescriptor(descriptor: number) {
  const chunks: Buffer[] = [];
  let position = 0;
  while (true) {
    const chunk = Buffer.allocUnsafe(64 * 1024);
    const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, position);
    if (bytesRead === 0) break;
    chunks.push(chunk.subarray(0, bytesRead));
    position += bytesRead;
  }
  return Buffer.concat(chunks).toString("utf8");
}

function inspectCollectorPidFile(pidPath: string): InspectedPidFile {
  const absolutePath = path.resolve(pidPath);
  const ancestors = inspectPidAncestors(absolutePath);
  if (ancestors.kind !== "safe") return ancestors;
  let initialStat: fs.Stats;
  try {
    const stat = lstatPidPath(absolutePath);
    if (!stat) return { kind: "missing" };
    const unsafeReason = validatePidLeaf(stat);
    if (unsafeReason) return { kind: "unsafe", reason: unsafeReason };
    initialStat = stat;
  } catch {
    return { kind: "unsafe", reason: "unreadable" };
  }
  const initialIdentity = pidFileIdentity(initialStat);
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(absolutePath, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const boundBefore = fs.fstatSync(descriptor);
    if (validatePidLeaf(boundBefore)) return { kind: "unsafe", reason: "identity_changed" };
    const boundBeforeIdentity = pidFileIdentity(boundBefore);
    if (!samePidFileIdentity(initialIdentity, boundBeforeIdentity)) {
      return { kind: "unsafe", reason: "identity_changed" };
    }
    const raw = readPidDescriptor(descriptor);
    const boundAfterIdentity = pidFileIdentity(fs.fstatSync(descriptor));
    if (!samePidFileIdentity(boundBeforeIdentity, boundAfterIdentity)) {
      return { kind: "unsafe", reason: "identity_changed" };
    }
    const finalStat = lstatPidPath(absolutePath);
    const finalAncestors = inspectPidAncestors(absolutePath);
    if (
      !finalStat ||
      finalAncestors.kind !== "safe" ||
      !samePidAncestors(ancestors.identities, finalAncestors.identities) ||
      !samePidFileIdentity(boundAfterIdentity, pidFileIdentity(finalStat))
    ) {
      return { kind: "unsafe", reason: "identity_changed" };
    }
    return { kind: "safe", fileIdentity: boundAfterIdentity, raw };
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    return {
      kind: "unsafe",
      reason: code === "ELOOP" || code === "EMLINK" ? "leaf_symlink" : "unreadable",
    };
  } finally {
    if (descriptor !== undefined) fs.closeSync(descriptor);
  }
}

export function readCollectorPidFile(pidPath: string, label: string): CollectorPidFileRead {
  const inspected = inspectCollectorPidFile(pidPath);
  if (inspected.kind !== "safe") return inspected;
  const { fileIdentity, raw } = inspected;
  const trimmed = raw.trim();
  const legacyPid = Number(trimmed);
  if (Number.isInteger(legacyPid) && legacyPid > 0) {
    return { kind: "legacy", fileIdentity, pid: legacyPid, raw };
  }
  const current = parseCurrentPidRecord(trimmed, label);
  if (current) return { kind: "current", fileIdentity, raw, record: current };
  try {
    const previous = JSON.parse(trimmed) as { label?: unknown; pid?: unknown; version?: unknown };
    if (
      previous.version === 1 &&
      previous.label === label &&
      Number.isInteger(previous.pid) &&
      Number(previous.pid) > 0
    ) {
      return { kind: "legacy", fileIdentity, pid: Number(previous.pid), raw };
    }
  } catch {
    // Not a legacy JSON record.
  }
  return { kind: "invalid", fileIdentity, raw };
}

export function runtimeIdentityMatches(
  left: CollectorRuntimeIdentity | null | undefined,
  right: CollectorRuntimeIdentity | null | undefined,
) {
  return Boolean(
    left &&
      right &&
      left.pid === right.pid &&
      left.instanceId === right.instanceId &&
      left.processStartFingerprint === right.processStartFingerprint,
  );
}

export function processIdentityIsLive(identity: ProcessIdentity) {
  return (
    processIsRunning(identity.pid) &&
    readProcessStartFingerprint(identity.pid) === identity.processStartFingerprint
  );
}

function removeFileIfUnchanged(filePath: string, expected: string) {
  try {
    if (fs.readFileSync(filePath, "utf8") === expected) {
      fs.unlinkSync(filePath);
      return true;
    }
  } catch {
    // Another process already changed or removed the file.
  }
  return false;
}

export function removeCollectorPidFileIfOwned(
  pidPath: string,
  identity: CollectorRuntimeIdentity,
  label: string,
  hooks: CollectorPidCleanupHooks = {},
): CollectorPidCleanupResult {
  return removeCollectorPidFileIfOwnedDetailed(pidPath, identity, label, hooks);
}

const CLEANUP_ARTIFACT_MAX_BYTES = 256 * 1024;

type CleanupArtifact = Readonly<{
  path: string;
  raw: string;
  fileIdentity: PidFileIdentity;
}>;

type CleanupMarkerRecord = Readonly<{
  actor: ProcessIdentity;
  label: string;
  schema: "plimsoll.collector-pid-cleanup.v2";
  state: "in_progress";
  target: CollectorRuntimeIdentity;
  transactionId: string;
}>;

type CleanupMarkerInspection =
  | { kind: "missing" }
  | { kind: "unsafe" }
  | ({ kind: "present"; record: CleanupMarkerRecord } & CleanupArtifact);

type CleanupSlotInspection =
  | { kind: "missing" }
  | { kind: "unsafe" }
  | ({ kind: "present" } & CleanupArtifact);

function cleanupArtifactNames(pidPath: string) {
  const absolutePath = path.resolve(pidPath);
  const directory = path.dirname(absolutePath);
  const basename = path.basename(absolutePath);
  return {
    absolutePath,
    markerPath: path.join(directory, `.${basename}.plimsoll-cleanup-marker`),
    pidClaimPath: path.join(directory, `.${basename}.plimsoll-remove-claim`),
    markerClaimPath: path.join(directory, `.${basename}.plimsoll-cleanup-marker-claim`),
    quarantinePath: path.join(directory, `.${basename}.plimsoll-quarantine`),
  };
}

function inspectCleanupSlot(slotPath: string): CleanupSlotInspection {
  const inspected = inspectCollectorPidFile(slotPath);
  if (inspected.kind === "missing") return inspected;
  if (inspected.kind !== "safe" || inspected.fileIdentity.size > CLEANUP_ARTIFACT_MAX_BYTES) {
    return { kind: "unsafe" };
  }
  return {
    kind: "present",
    path: slotPath,
    raw: inspected.raw,
    fileIdentity: inspected.fileIdentity,
  };
}

function validProcessIdentity(value: unknown): value is ProcessIdentity {
  const identity = value as Partial<ProcessIdentity> | null | undefined;
  return Boolean(
    identity &&
      Number.isInteger(identity.pid) &&
      (identity.pid ?? 0) > 0 &&
      typeof identity.processStartFingerprint === "string" &&
      identity.processStartFingerprint.startsWith("sha256:"),
  );
}

function inspectCleanupMarker(slotPath: string, label: string): CleanupMarkerInspection {
  const inspected = inspectCleanupSlot(slotPath);
  if (inspected.kind !== "present") return inspected;
  try {
    const parsed = JSON.parse(inspected.raw) as Partial<CleanupMarkerRecord>;
    if (
      Object.keys(parsed).sort().join("|") !==
        "actor|label|schema|state|target|transactionId" ||
      parsed.schema !== "plimsoll.collector-pid-cleanup.v2" ||
      parsed.state !== "in_progress" ||
      parsed.label !== label ||
      typeof parsed.transactionId !== "string" ||
      !/^[0-9a-f-]{36}$/.test(parsed.transactionId) ||
      !validProcessIdentity(parsed.actor) ||
      !validRuntimeIdentity(parsed.target)
    ) {
      return { kind: "unsafe" };
    }
    return { ...inspected, record: parsed as CleanupMarkerRecord };
  } catch {
    return { kind: "unsafe" };
  }
}

function inspectCleanupArtifacts(pidPath: string, label: string) {
  const names = cleanupArtifactNames(pidPath);
  const marker = inspectCleanupMarker(names.markerPath, label);
  const pidClaim = inspectCleanupSlot(names.pidClaimPath);
  const markerClaim = inspectCleanupMarker(names.markerClaimPath, label);
  const quarantine = inspectCleanupSlot(names.quarantinePath);
  const markerState = marker.kind === "missing"
    ? "missing" as const
    : marker.kind === "present"
      ? "present" as const
      : "unsafe" as const;
  const claimCount = Number(pidClaim.kind !== "missing") + Number(markerClaim.kind !== "missing");
  const quarantineCount = Number(quarantine.kind !== "missing");
  const unsafeArtifactCount = [marker, pidClaim, markerClaim, quarantine]
    .filter((artifact) => artifact.kind === "unsafe").length;
  const state: CollectorPidCleanupState = Object.freeze({
    ambiguous:
      markerState !== "missing" || claimCount > 0 || quarantineCount > 0,
    markerState,
    claimCount,
    quarantineCount,
    inventoryTruncated: false,
    unsafeArtifactCount,
  });
  return { names, marker, pidClaim, markerClaim, quarantine, state };
}

export function readCollectorPidCleanupState(
  pidPath: string,
  label: string,
): CollectorPidCleanupState {
  return inspectCleanupArtifacts(pidPath, label).state;
}

function createCleanupMarker(
  pidPath: string,
  label: string,
  target: CollectorRuntimeIdentity,
): CleanupArtifact | null {
  const actorFingerprint = readProcessStartFingerprint(process.pid);
  if (!actorFingerprint) return null;
  const names = cleanupArtifactNames(pidPath);
  const raw = `${JSON.stringify({
    actor: { pid: process.pid, processStartFingerprint: actorFingerprint },
    label,
    schema: "plimsoll.collector-pid-cleanup.v2",
    state: "in_progress",
    target,
    transactionId: randomUUID(),
  } satisfies CleanupMarkerRecord)}\n`;
  try {
    fs.writeFileSync(names.markerPath, raw, { flag: "wx", mode: 0o600 });
    const inspected = inspectCleanupMarker(names.markerPath, label);
    if (inspected.kind !== "present" || inspected.raw !== raw) return null;
    return inspected;
  } catch {
    return null;
  }
}

function claimExactArtifact(
  source: CleanupArtifact,
  destinationPath: string,
): CleanupArtifact | null {
  try {
    const before = inspectCleanupSlot(source.path);
    if (
      before.kind !== "present" ||
      !samePidFileIdentity(source.fileIdentity, before.fileIdentity) ||
      before.raw !== source.raw ||
      lstatPidPath(destinationPath)
    ) {
      return null;
    }
    fs.writeFileSync(destinationPath, source.raw, { flag: "wx", mode: 0o600 });
    const destination = inspectCleanupSlot(destinationPath);
    const sourceBeforeUnlink = inspectCleanupSlot(source.path);
    if (
      destination.kind !== "present" ||
      destination.raw !== source.raw ||
      sourceBeforeUnlink.kind !== "present" ||
      !samePidFileIdentity(source.fileIdentity, sourceBeforeUnlink.fileIdentity) ||
      sourceBeforeUnlink.raw !== source.raw
    ) {
      return null;
    }
    fs.unlinkSync(source.path);
    const claimed = inspectCleanupSlot(destinationPath);
    return !lstatPidPath(source.path) &&
      claimed.kind === "present" &&
      claimed.raw === source.raw
      ? claimed
      : null;
  } catch {
    return null;
  }
}

function clearExactArtifact(artifact: CleanupArtifact) {
  try {
    const current = inspectCleanupSlot(artifact.path);
    if (
      current.kind !== "present" ||
      !samePidFileIdentity(artifact.fileIdentity, current.fileIdentity) ||
      current.raw !== artifact.raw
    ) {
      return false;
    }
    fs.unlinkSync(artifact.path);
    return !lstatPidPath(artifact.path);
  } catch {
    return false;
  }
}

function processIdentityState(identity: ProcessIdentity): "live" | "not_live" | "indeterminate" {
  try {
    process.kill(identity.pid, 0);
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ESRCH" ? "not_live" : "indeterminate";
  }
  const fingerprint = readProcessStartFingerprint(identity.pid);
  if (!fingerprint) return "indeterminate";
  return fingerprint === identity.processStartFingerprint ? "live" : "not_live";
}

function reconciliationResult(
  pidPath: string,
  label: string,
  disposition: CollectorPidCleanupReconciliationResult["disposition"],
  before: CollectorPidCleanupState,
  options: { eligible?: boolean; reconciled?: boolean } = {},
): CollectorPidCleanupReconciliationResult {
  return Object.freeze({
    reconciled: options.reconciled ?? false,
    eligible: options.eligible ?? false,
    disposition,
    before,
    after: readCollectorPidCleanupState(pidPath, label),
  });
}

export function reconcileCollectorPidCleanupState(
  pidPath: string,
  label: string,
  options: {
    apply?: boolean;
    hooks?: CollectorPidCleanupReconciliationHooks;
  } = {},
): CollectorPidCleanupReconciliationResult {
  const artifacts = inspectCleanupArtifacts(pidPath, label);
  const before = artifacts.state;
  if (!before.ambiguous) return reconciliationResult(pidPath, label, "clean", before);
  if (before.unsafeArtifactCount > 0) {
    return reconciliationResult(pidPath, label, "unsafe_artifact", before);
  }
  if (artifacts.pidClaim.kind !== "missing") {
    return reconciliationResult(pidPath, label, "pid_claim_present", before);
  }
  if (artifacts.quarantine.kind !== "missing") {
    return reconciliationResult(pidPath, label, "quarantine_present", before);
  }
  const markerSlots = [artifacts.marker, artifacts.markerClaim]
    .filter((artifact): artifact is Extract<CleanupMarkerInspection, { kind: "present" }> =>
      artifact.kind === "present");
  if (markerSlots.length !== 1) {
    return reconciliationResult(pidPath, label, "multiple_marker_slots", before);
  }
  const marker = markerSlots[0];
  const actorState = processIdentityState(marker.record.actor);
  if (actorState === "live") {
    return reconciliationResult(pidPath, label, "actor_live", before);
  }
  if (actorState === "indeterminate") {
    return reconciliationResult(pidPath, label, "actor_indeterminate", before);
  }
  if (options.apply === false) {
    return reconciliationResult(pidPath, label, "eligible_dead_actor", before, {
      eligible: true,
    });
  }

  let claimed: CleanupArtifact | null = marker;
  if (marker.path === artifacts.names.markerPath) {
    options.hooks?.beforeMarkerClaim?.();
    claimed = claimExactArtifact(marker, artifacts.names.markerClaimPath);
  }
  if (!claimed) return reconciliationResult(pidPath, label, "clear_race", before);
  options.hooks?.beforeClear?.();
  if (!clearExactArtifact(claimed)) {
    return reconciliationResult(pidPath, label, "clear_race", before);
  }
  return reconciliationResult(
    pidPath,
    label,
    marker.path === artifacts.names.markerPath ? "marker_cleared" : "marker_claim_cleared",
    before,
    { eligible: true, reconciled: true },
  );
}

export function removeCollectorPidFileIfOwnedDetailed(
  pidPath: string,
  identity: CollectorRuntimeIdentity,
  label: string,
  hooks: CollectorPidCleanupHooks = {},
): CollectorPidCleanupResult {
  const priorCleanup = reconcileCollectorPidCleanupState(pidPath, label).after;
  if (priorCleanup.ambiguous) {
    return cleanupResult(pidPath, label, "persistent_ambiguity", {
      ambiguous: true,
      quarantined: priorCleanup.quarantineCount > 0,
    });
  }
  const current = readCollectorPidFile(pidPath, label);
  if (current.kind !== "current" || !runtimeIdentityMatches(current.record, identity)) {
    return cleanupResult(pidPath, label, "not_owned");
  }
  const marker = createCleanupMarker(pidPath, label, identity);
  if (!marker) {
    return cleanupResult(pidPath, label, "marker_create_failed", { ambiguous: true });
  }
  const names = cleanupArtifactNames(pidPath);
  const absolutePath = names.absolutePath;
  let claim: CleanupArtifact | null = null;
  try {
    hooks.beforeClaim?.();
    const preClaim = lstatPidPath(absolutePath);
    if (!preClaim || !samePidFileIdentity(current.fileIdentity, pidFileIdentity(preClaim))) {
      return cleanupResult(pidPath, label, "preclaim_changed", { ambiguous: true });
    }
    hooks.afterPreClaimCheck?.();
    const markerBeforeClaim = inspectCleanupMarker(marker.path, label);
    if (
      markerBeforeClaim.kind !== "present" ||
      !samePidFileIdentity(marker.fileIdentity, markerBeforeClaim.fileIdentity) ||
      markerBeforeClaim.raw !== marker.raw
    ) {
      return cleanupResult(pidPath, label, "marker_create_failed", { ambiguous: true });
    }
    claim = claimExactArtifact(
      { path: absolutePath, raw: current.raw, fileIdentity: current.fileIdentity },
      names.pidClaimPath,
    );
    if (!claim) {
      const afterClaimFailure = readCollectorPidFile(absolutePath, label);
      return cleanupResult(
        pidPath,
        label,
        afterClaimFailure.kind === "current" &&
          runtimeIdentityMatches(afterClaimFailure.record, current.record) &&
          samePidFileIdentity(afterClaimFailure.fileIdentity, current.fileIdentity)
          ? "claim_missing"
          : "preclaim_changed",
        { ambiguous: true },
      );
    }
    hooks.afterClaim?.();
    const verified = inspectCleanupSlot(claim.path);
    if (lstatPidPath(absolutePath)) {
      const quarantined = Boolean(claimExactArtifact(claim, names.quarantinePath));
      return cleanupResult(pidPath, label, "destination_reappeared", { ambiguous: true, quarantined });
    }
    if (
      verified.kind !== "present" ||
      !samePidFileIdentity(claim.fileIdentity, verified.fileIdentity) ||
      verified.raw !== current.raw
    ) {
      const quarantined = Boolean(claimExactArtifact(claim, names.quarantinePath));
      return cleanupResult(pidPath, label, "claim_changed", { ambiguous: true, quarantined });
    }
    const markerClaim = claimExactArtifact(marker, names.markerClaimPath);
    if (!markerClaim) {
      const quarantined = Boolean(claimExactArtifact(claim, names.quarantinePath));
      return cleanupResult(pidPath, label, "marker_clear_failed", {
        ambiguous: true,
        quarantined,
      });
    }
    if (!clearExactArtifact(claim)) {
      return cleanupResult(pidPath, label, "claim_changed", {
        ambiguous: true,
        quarantined: false,
      });
    }
    const destinationReappeared = Boolean(lstatPidPath(absolutePath));
    if (destinationReappeared) {
      return cleanupResult(
        pidPath,
        label,
        "destination_reappeared",
        { ambiguous: true, quarantined: false },
      );
    }
    if (!clearExactArtifact(markerClaim)) {
      return cleanupResult(pidPath, label, "marker_clear_failed", {
        removed: true,
        ambiguous: true,
      });
    }
    return cleanupResult(pidPath, label, "removed", { removed: true });
  } catch {
    const quarantined = claim ? Boolean(claimExactArtifact(claim, names.quarantinePath)) : false;
    return cleanupResult(pidPath, label, "operation_failed", { ambiguous: true, quarantined });
  }
}

function cleanupResult(
  pidPath: string,
  label: string,
  disposition: CollectorPidCleanupResult["disposition"],
  overrides: Partial<Pick<CollectorPidCleanupResult, "removed" | "ambiguous" | "quarantined">> = {},
): CollectorPidCleanupResult {
  const persistent = readCollectorPidCleanupState(pidPath, label);
  return Object.freeze({
    removed: overrides.removed ?? false,
    ambiguous: overrides.ambiguous ?? persistent.ambiguous,
    quarantined: overrides.quarantined ?? persistent.quarantineCount > 0,
    persistent,
    disposition,
  });
}

function sleep(milliseconds: number) {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function validRuntimeIdentity(value: unknown): value is CollectorRuntimeIdentity {
  const identity = value as Partial<CollectorRuntimeIdentity> | null | undefined;
  return Boolean(
    identity &&
      Number.isInteger(identity.pid) &&
      (identity.pid ?? 0) > 0 &&
      typeof identity.instanceId === "string" &&
      identity.instanceId.length >= 32 &&
      typeof identity.processStartFingerprint === "string" &&
      identity.processStartFingerprint.startsWith("sha256:"),
  );
}

function probeLoopbackPort(port: number, timeoutMs: number) {
  return new Promise<"open" | "closed" | "indeterminate">((resolve) => {
    const socket = net.createConnection({ host: "127.0.0.1", port });
    let settled = false;
    const finish = (state: "open" | "closed" | "indeterminate") => {
      if (settled) return;
      settled = true;
      socket.destroy();
      resolve(state);
    };
    socket.once("connect", () => finish("open"));
    socket.once("timeout", () => finish("indeterminate"));
    socket.once("error", (error: NodeJS.ErrnoException) => {
      finish(error.code === "ECONNREFUSED" ? "closed" : "indeterminate");
    });
    socket.setTimeout(timeoutMs);
  });
}

export async function observeCollectorListener(
  port: number,
  options: { probeTimeoutMs?: number } = {},
): Promise<CollectorListenerObservation> {
  const timeoutMs = Math.max(1, options.probeTimeoutMs ?? 250);
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch("http://127.0.0.1:" + port + "/status", {
      signal: controller.signal,
    });
    if (!response.ok) return { kind: "unrelated" };
    const body = (await response.json()) as { runtimeIdentity?: unknown };
    if (!validRuntimeIdentity(body.runtimeIdentity)) return { kind: "unrelated" };
    return { kind: "collector", runtimeIdentity: body.runtimeIdentity };
  } catch {
    const portState = await probeLoopbackPort(port, timeoutMs);
    return portState === "open"
      ? { kind: "unrelated" }
      : portState === "closed"
        ? { kind: "absent" }
        : { kind: "indeterminate" };
  } finally {
    clearTimeout(timer);
  }
}

async function statusRuntimeIdentity(port: number, timeoutMs: number) {
  const listener = await observeCollectorListener(port, { probeTimeoutMs: timeoutMs });
  return listener.kind === "collector" ? listener.runtimeIdentity : null;
}

function identityKey(identity: ProcessIdentity) {
  return [identity.pid, identity.processStartFingerprint].join("|");
}

function pidRuntimeIdentity(read: CollectorPidFileRead): CollectorRuntimeIdentity | null {
  return read.kind === "current"
    ? {
        instanceId: read.record.instanceId,
        pid: read.record.pid,
        processStartFingerprint: read.record.processStartFingerprint,
      }
    : null;
}

function priorRuntimeIdentities(prior: LaunchAgentUnloadPriorState) {
  const candidates = [
    prior.label.kind === "reported" ? prior.label.processIdentity : null,
    prior.pidRuntimeIdentity,
    prior.listenerRuntimeIdentity,
  ].filter((identity): identity is ProcessIdentity | CollectorRuntimeIdentity => Boolean(identity));
  return [...new Map(candidates.map((identity) => [identityKey(identity), identity])).values()];
}

function processIdentityMatches(left: ProcessIdentity, right: ProcessIdentity) {
  return left.pid === right.pid &&
    left.processStartFingerprint === right.processStartFingerprint;
}

function matchesAnyPrior(
  identity: CollectorRuntimeIdentity | null,
  identities: readonly ProcessIdentity[],
) {
  return Boolean(identity && identities.some((candidate) => processIdentityMatches(identity, candidate)));
}

export async function captureLaunchAgentUnloadPriorState(options: {
  label: string;
  pidPath: string;
  port: number;
  observeLabel: () => LaunchAgentLabelObservation | Promise<LaunchAgentLabelObservation>;
  observeListener?: () => Promise<CollectorListenerObservation>;
  readPidFile?: (pidPath: string, label: string) => CollectorPidFileRead;
  readCleanupState?: (pidPath: string, label: string) => CollectorPidCleanupState;
  reconcileCleanupState?: (
    pidPath: string,
    label: string,
  ) => CollectorPidCleanupReconciliationResult;
}): Promise<LaunchAgentUnloadPriorState> {
  const readPidFile = options.readPidFile ?? readCollectorPidFile;
  const readCleanupState = options.readCleanupState ?? readCollectorPidCleanupState;
  const pidCleanupReconciliation = (
    options.reconcileCleanupState ?? reconcileCollectorPidCleanupState
  )(options.pidPath, options.label);
  const [label, listener] = await Promise.all([
    options.observeLabel(),
    options.observeListener?.() ?? observeCollectorListener(options.port),
  ]);
  const pidRead = readPidFile(options.pidPath, options.label);
  const pidCleanupState = readCleanupState(options.pidPath, options.label);
  const pidIdentity = pidRuntimeIdentity(pidRead);
  const listenerIdentity = listener.kind === "collector" ? listener.runtimeIdentity : null;
  const ownership = listener.kind === "unrelated"
    ? "unrelated" as const
    : pidRead.kind === "legacy" ||
        pidRead.kind === "invalid" ||
        pidRead.kind === "unsafe" ||
        pidCleanupState.ambiguous ||
        listener.kind === "indeterminate"
      ? "unproven" as const
      : pidIdentity && listenerIdentity && !runtimeIdentityMatches(pidIdentity, listenerIdentity)
        ? "ambiguous" as const
        : "consistent" as const;
  return Object.freeze({
    label,
    listener,
    listenerRuntimeIdentity: listenerIdentity,
    ownership,
    pidRecordKind: pidRead.kind,
    pidRuntimeIdentity: pidIdentity,
    pidCleanupState,
    pidCleanupReconciliation,
  });
}

async function observeLaunchAgentUnloadOnce(options: {
  label: string;
  pidPath: string;
  prior: LaunchAgentUnloadPriorState;
  observeLabel: () => LaunchAgentLabelObservation | Promise<LaunchAgentLabelObservation>;
  observeListener: () => Promise<CollectorListenerObservation>;
  processIsLive: (identity: ProcessIdentity) => boolean;
  readPidFile: (pidPath: string, label: string) => CollectorPidFileRead;
  readCleanupState: (pidPath: string, label: string) => CollectorPidCleanupState;
  reconcileCleanupState: (
    pidPath: string,
    label: string,
  ) => CollectorPidCleanupReconciliationResult;
  removePidFile: (
    pidPath: string,
    identity: CollectorRuntimeIdentity,
    label: string,
  ) => CollectorPidCleanupResult;
  removedPidFile: boolean;
  pidCleanupAmbiguous: boolean;
  pidCleanupQuarantined: boolean;
}): Promise<LaunchAgentUnloadObservation> {
  const priorIdentities = priorRuntimeIdentities(options.prior);
  const [labelState, listenerState] = await Promise.all([
    options.observeLabel(),
    options.observeListener(),
  ]);
  const priorRuntimeLive = priorIdentities.some(options.processIsLive);
  const listenerIdentity = listenerState.kind === "collector"
    ? listenerState.runtimeIdentity
    : null;
  const currentListenerMatchesPrior = matchesAnyPrior(listenerIdentity, priorIdentities);

  let pidRead = options.readPidFile(options.pidPath, options.label);
  options.reconcileCleanupState(options.pidPath, options.label);
  let persistentCleanup = options.readCleanupState(options.pidPath, options.label);
  let currentPidIdentity = pidRuntimeIdentity(pidRead);
  let currentPidMatchesPrior = matchesAnyPrior(currentPidIdentity, priorIdentities);
  let removedPidFile = options.removedPidFile;
  let pidCleanupAmbiguous =
    options.pidCleanupAmbiguous || options.prior.pidCleanupState.ambiguous || persistentCleanup.ambiguous;
  let pidCleanupQuarantined =
    options.pidCleanupQuarantined ||
    options.prior.pidCleanupState.quarantineCount > 0 ||
    persistentCleanup.quarantineCount > 0;
  if (
    !pidCleanupAmbiguous &&
    currentPidIdentity &&
    currentPidMatchesPrior &&
    options.prior.ownership === "consistent" &&
    !options.processIsLive(currentPidIdentity)
  ) {
    const attemptedIdentity = currentPidIdentity;
    const cleanup = options.removePidFile(
      options.pidPath,
      currentPidIdentity,
      options.label,
    );
    const cleanupRemoved = cleanup.removed;
    removedPidFile = cleanupRemoved || removedPidFile;
    pidCleanupAmbiguous = cleanup.ambiguous || pidCleanupAmbiguous;
    pidCleanupQuarantined = cleanup.quarantined || pidCleanupQuarantined;
    pidRead = options.readPidFile(options.pidPath, options.label);
    persistentCleanup = options.readCleanupState(options.pidPath, options.label);
    pidCleanupAmbiguous = persistentCleanup.ambiguous || pidCleanupAmbiguous;
    pidCleanupQuarantined =
      persistentCleanup.quarantineCount > 0 || pidCleanupQuarantined;
    currentPidIdentity = pidRuntimeIdentity(pidRead);
    currentPidMatchesPrior = matchesAnyPrior(currentPidIdentity, priorIdentities);
    if (!cleanupRemoved && !runtimeIdentityMatches(attemptedIdentity, currentPidIdentity)) {
      pidCleanupAmbiguous = true;
    }
  }

  const pidCleaned =
    pidRead.kind === "missing" && !pidCleanupAmbiguous && !persistentCleanup.ambiguous;
  let state: LaunchAgentUnloadState;
  const currentPidMatchesListener = runtimeIdentityMatches(currentPidIdentity, listenerIdentity);
  if (
    pidRead.kind === "current" &&
    !currentPidMatchesPrior &&
    listenerState.kind === "collector" &&
    currentPidMatchesListener
  ) {
    state = "live_conflict";
  } else if (
    options.prior.ownership === "ambiguous" ||
    (pidRead.kind === "current" && !currentPidMatchesPrior)
  ) {
    state = "ambiguous_prior_owner";
  } else if (listenerState.kind === "unrelated" || options.prior.ownership === "unrelated") {
    state = "unrelated_listener";
  } else if (
    labelState.kind === "query_failed" ||
    pidCleanupAmbiguous ||
    pidRead.kind === "unsafe" ||
    listenerState.kind === "indeterminate" ||
    (options.prior.ownership === "unproven" &&
      options.prior.pidRecordKind !== "legacy" &&
      options.prior.pidRecordKind !== "invalid")
  ) {
    state = "indeterminate";
  } else if (
    options.prior.ownership === "unproven" &&
    (options.prior.pidRecordKind === "legacy" || options.prior.pidRecordKind === "invalid")
  ) {
    state = "stale_owned_record";
  } else if (pidRead.kind === "legacy" || pidRead.kind === "invalid") {
    state = "stale_owned_record";
  } else if (
    listenerState.kind === "collector" &&
    !currentListenerMatchesPrior
  ) {
    state = "live_conflict";
  } else if (
    labelState.kind === "reported" ||
    priorRuntimeLive ||
    listenerState.kind === "collector" ||
    pidRead.kind === "current"
  ) {
    state = labelState.kind === "not_reported" && priorRuntimeLive
      ? "live_conflict"
      : "still_stopping";
  } else {
    state = "stopped";
  }

  return Object.freeze({
    state,
    stopped: state === "stopped",
    labelState: labelState.kind,
    listenerState: listenerState.kind,
    pidRecordState: pidRead.kind,
    pidCleaned,
    removedPidFile,
    pidCleanupAmbiguous,
    pidCleanupQuarantined,
    pidCleanupMarkerState: persistentCleanup.markerState,
    pidCleanupClaimCount: persistentCleanup.claimCount,
    pidCleanupQuarantineCount: persistentCleanup.quarantineCount,
    pidCleanupInventoryTruncated: persistentCleanup.inventoryTruncated,
    priorRuntimeLive,
    priorRuntimeCount: priorIdentities.length,
    currentListenerMatchesPrior,
    currentPidMatchesPrior,
    currentListenerRuntimeIdentity: listenerIdentity,
    currentPidRuntimeIdentity: currentPidIdentity,
  });
}

export async function observeLaunchAgentUnloadTerminalState(options: {
  label: string;
  pidPath: string;
  port: number;
  prior: LaunchAgentUnloadPriorState;
  timeoutMs?: number;
  pollIntervalMs?: number;
  observeLabel: () => LaunchAgentLabelObservation | Promise<LaunchAgentLabelObservation>;
  observeListener?: () => Promise<CollectorListenerObservation>;
  processIsLive?: (identity: ProcessIdentity) => boolean;
  readPidFile?: (pidPath: string, label: string) => CollectorPidFileRead;
  readCleanupState?: (pidPath: string, label: string) => CollectorPidCleanupState;
  reconcileCleanupState?: (
    pidPath: string,
    label: string,
  ) => CollectorPidCleanupReconciliationResult;
  removePidFile?: (
    pidPath: string,
    identity: CollectorRuntimeIdentity,
    label: string,
  ) => CollectorPidCleanupResult;
  now?: () => number;
  poll?: (milliseconds: number) => Promise<void>;
}): Promise<LaunchAgentUnloadOutcome> {
  const timeoutMs = Math.max(0, options.timeoutMs ?? 4_000);
  const pollIntervalMs = Math.max(1, options.pollIntervalMs ?? 50);
  const now = options.now ?? Date.now;
  const poll = options.poll ?? sleep;
  const startedAt = now();
  const deadline = startedAt + timeoutMs;
  const observeListener = options.observeListener ?? (() => observeCollectorListener(options.port));
  const processIsLive = options.processIsLive ?? processIdentityIsLive;
  const readPidFile = options.readPidFile ?? readCollectorPidFile;
  const readCleanupState = options.readCleanupState ?? readCollectorPidCleanupState;
  const reconcileCleanupState =
    options.reconcileCleanupState ?? reconcileCollectorPidCleanupState;
  const removePidFile = options.removePidFile ?? removeCollectorPidFileIfOwnedDetailed;
  let removedPidFile = false;
  let pidCleanupAmbiguous = false;
  let pidCleanupQuarantined = false;
  let observations = 0;
  let deadlineCrossed = false;
  let final: LaunchAgentUnloadObservation;

  while (true) {
    final = await observeLaunchAgentUnloadOnce({
      label: options.label,
      pidPath: options.pidPath,
      prior: options.prior,
      observeLabel: options.observeLabel,
      observeListener,
      processIsLive,
      readPidFile,
      readCleanupState,
      reconcileCleanupState,
      removePidFile,
      removedPidFile,
      pidCleanupAmbiguous,
      pidCleanupQuarantined,
    });
    observations += 1;
    removedPidFile = final.removedPidFile;
    pidCleanupAmbiguous = final.pidCleanupAmbiguous;
    pidCleanupQuarantined = final.pidCleanupQuarantined;
    if (final.stopped) break;

    const observedAt = now();
    if (observedAt >= deadline) {
      deadlineCrossed = true;
      break;
    }
    await poll(Math.min(pollIntervalMs, Math.max(0, deadline - observedAt)));
  }

  // Freeze the receipt clock before the mandatory final aggregate read. No
  // injected clock runs after this observation, so a clock hook cannot mutate
  // ownership between the final truth read and serialization.
  const receiptBoundaryAt = now();
  deadlineCrossed = deadlineCrossed || receiptBoundaryAt >= deadline;
  final = await observeLaunchAgentUnloadOnce({
    label: options.label,
    pidPath: options.pidPath,
    prior: options.prior,
    observeLabel: options.observeLabel,
    observeListener,
    processIsLive,
    readPidFile,
    readCleanupState,
    reconcileCleanupState,
    removePidFile,
    removedPidFile,
    pidCleanupAmbiguous,
    pidCleanupQuarantined,
  });
  observations += 1;
  removedPidFile = final.removedPidFile;
  pidCleanupAmbiguous = final.pidCleanupAmbiguous;
  pidCleanupQuarantined = final.pidCleanupQuarantined;
  const elapsedMs = Math.max(0, receiptBoundaryAt - startedAt);
  return Object.freeze({
    stopped: final.stopped,
    state: final.state,
    pidCleaned: final.pidCleaned,
    removedPidFile,
    pidCleanupAmbiguous,
    pidCleanupQuarantined,
    final,
    timing: {
      timeoutMs,
      pollIntervalMs,
      elapsedMs,
      observations,
      deadlineCrossed,
      finalObservationPerformed: true as const,
    },
  });
}

export async function verifyCollectorRuntimeIdentity(
  port: number,
  expected: CollectorRuntimeIdentity,
  options: {
    probeCount?: number;
    probeTimeoutMs?: number;
    stabilityDelayMs?: number;
  } = {},
) {
  const probeCount = Math.max(1, options.probeCount ?? 2);
  const probeTimeoutMs = options.probeTimeoutMs ?? 5_000;
  const stabilityDelayMs = options.stabilityDelayMs ?? OWNER_STABILITY_DELAY_MS;

  for (let probe = 0; probe < probeCount; probe += 1) {
    if (!processIdentityIsLive(expected)) return false;
    const observed = await statusRuntimeIdentity(port, probeTimeoutMs);
    if (!runtimeIdentityMatches(observed, expected)) return false;
    if (!processIdentityIsLive(expected)) return false;
    if (probe + 1 < probeCount) await sleep(stabilityDelayMs);
  }

  return processIdentityIsLive(expected);
}

function parseStartLock(raw: string, label: string): StartLockRecord | null {
  try {
    const parsed = JSON.parse(raw) as Partial<StartLockRecord>;
    if (
      parsed.version === 2 &&
      parsed.label === label &&
      Number.isInteger(parsed.pid) &&
      (parsed.pid ?? 0) > 0 &&
      typeof parsed.instanceId === "string" &&
      parsed.instanceId.length >= 32 &&
      typeof parsed.processStartFingerprint === "string" &&
      parsed.processStartFingerprint.startsWith("sha256:") &&
      typeof parsed.createdAt === "string" &&
      Number.isFinite(Date.parse(parsed.createdAt))
    ) {
      return parsed as StartLockRecord;
    }
  } catch {
    // Invalid lock records are recoverable stale state.
  }
  return null;
}

function startLockIsCurrent(lock: StartLockRecord, now: number) {
  const ageMs = now - Date.parse(lock.createdAt);
  return (
    ageMs >= 0 &&
    ageMs <= START_LOCK_LEASE_MS &&
    processIdentityIsLive(lock)
  );
}

function releaseLock(lockPath: string, serializedLock: string) {
  removeFileIfUnchanged(lockPath, serializedLock);
}

function pidRecordKey(record: CollectorPidRecord) {
  return [
    record.pid,
    record.instanceId,
    record.processStartFingerprint,
  ].join("|");
}

export async function acquireCollectorStartOwnership(options: {
  candidateIdentity: CollectorRuntimeIdentity;
  label: string;
  pidPath: string;
  port: number;
  probeTimeoutMs?: number;
  waitTimeoutMs?: number;
}): Promise<CollectorStartOwnership> {
  if (!processIdentityIsLive(options.candidateIdentity)) {
    throw new CollectorStartOwnershipError(
      "process_fingerprint_unavailable",
      "The candidate collector process identity is not live.",
    );
  }

  const probeTimeoutMs = options.probeTimeoutMs ?? 5_000;
  const waitTimeoutMs = options.waitTimeoutMs ?? 10_000;
  const lockPath = options.pidPath + ".start.lock";
  const deadline = Date.now() + waitTimeoutMs;
  let knownUnhealthyRecord = "";
  fs.mkdirSync(path.dirname(options.pidPath), { recursive: true, mode: 0o700 });

  while (Date.now() <= deadline) {
    if (reconcileCollectorPidCleanupState(options.pidPath, options.label).after.ambiguous) {
      throw new CollectorStartOwnershipError(
        "pid_cleanup_ambiguous",
        "Collector PID cleanup has unresolved private ownership artifacts.",
      );
    }
    const existing = readCollectorPidFile(options.pidPath, options.label);
    if (existing.kind === "current" && knownUnhealthyRecord !== pidRecordKey(existing.record)) {
      if (
        await verifyCollectorRuntimeIdentity(options.port, existing.record, {
          probeCount: 2,
          probeTimeoutMs,
        })
      ) {
        return {
          kind: "already_running",
          pidPath: options.pidPath,
          port: options.port,
          runtimeIdentity: {
            instanceId: existing.record.instanceId,
            pid: existing.record.pid,
            processStartFingerprint: existing.record.processStartFingerprint,
          },
        };
      }
      knownUnhealthyRecord = pidRecordKey(existing.record);
    }

    const serializedLock =
      JSON.stringify({
        createdAt: new Date().toISOString(),
        instanceId: options.candidateIdentity.instanceId,
        label: options.label,
        pid: options.candidateIdentity.pid,
        processStartFingerprint: options.candidateIdentity.processStartFingerprint,
        version: 2,
      } satisfies StartLockRecord) + "\n";

    try {
      fs.writeFileSync(lockPath, serializedLock, { flag: "wx", mode: 0o600 });

      if (reconcileCollectorPidCleanupState(options.pidPath, options.label).after.ambiguous) {
        releaseLock(lockPath, serializedLock);
        throw new CollectorStartOwnershipError(
          "pid_cleanup_ambiguous",
          "Collector PID cleanup became ambiguous while start ownership was acquired.",
        );
      }

      const rechecked = readCollectorPidFile(options.pidPath, options.label);
      if (
        rechecked.kind === "current" &&
        knownUnhealthyRecord !== pidRecordKey(rechecked.record)
      ) {
        if (
          await verifyCollectorRuntimeIdentity(options.port, rechecked.record, {
            probeCount: 2,
            probeTimeoutMs,
          })
        ) {
          releaseLock(lockPath, serializedLock);
          return {
            kind: "already_running",
            pidPath: options.pidPath,
            port: options.port,
            runtimeIdentity: {
              instanceId: rechecked.record.instanceId,
              pid: rechecked.record.pid,
              processStartFingerprint: rechecked.record.processStartFingerprint,
            },
          };
        }
        knownUnhealthyRecord = pidRecordKey(rechecked.record);
      }

      if (
        rechecked.kind === "current" ||
        rechecked.kind === "legacy" ||
        rechecked.kind === "invalid"
      ) {
        removeFileIfUnchanged(options.pidPath, rechecked.raw);
      }

      let released = false;
      return {
        kind: "owner",
        lockPath,
        pidPath: options.pidPath,
        release: () => {
          if (released) return;
          released = true;
          releaseLock(lockPath, serializedLock);
        },
        writePidFile: (record) => {
          if (!runtimeIdentityMatches(record, options.candidateIdentity)) {
            throw new Error("PID record does not match the candidate runtime identity.");
          }
          fs.writeFileSync(options.pidPath, JSON.stringify(record, null, 2) + "\n", {
            flag: "wx",
            mode: 0o600,
          });
        },
      };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      try {
        const raw = fs.readFileSync(lockPath, "utf8");
        const currentLock = parseStartLock(raw, options.label);
        if (!currentLock || !startLockIsCurrent(currentLock, Date.now())) {
          removeFileIfUnchanged(lockPath, raw);
          continue;
        }
      } catch {
        continue;
      }
      await sleep(100);
    }
  }

  throw new CollectorStartOwnershipError(
    "start_in_progress",
    "Another collector start still owns " + lockPath + " after " + waitTimeoutMs + "ms.",
  );
}
