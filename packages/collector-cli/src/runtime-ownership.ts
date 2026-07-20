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
  final: LaunchAgentUnloadObservation;
  timing: {
    timeoutMs: number;
    pollIntervalMs: number;
    elapsedMs: number;
    observations: number;
    deadlineCrossed: boolean;
    finalObservation: true;
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
    readonly code: "process_fingerprint_unavailable" | "start_in_progress",
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
  hooks: { beforeClaim?: () => void; afterClaim?: () => void } = {},
) {
  const current = readCollectorPidFile(pidPath, label);
  if (current.kind !== "current" || !runtimeIdentityMatches(current.record, identity)) {
    return false;
  }
  const absolutePath = path.resolve(pidPath);
  const claimPath = path.join(
    path.dirname(absolutePath),
    `.${path.basename(absolutePath)}.plimsoll-remove-${randomUUID()}`,
  );
  let claimIdentity: PidFileIdentity | null = null;
  try {
    hooks.beforeClaim?.();
    fs.renameSync(absolutePath, claimPath);
    const claimStat = lstatPidPath(claimPath);
    if (!claimStat) return false;
    claimIdentity = pidFileIdentity(claimStat);
    if (!samePidFileObject(current.fileIdentity, claimIdentity)) {
      restorePidClaim(claimPath, absolutePath, claimIdentity);
      return false;
    }
    const claimed = inspectCollectorPidFile(claimPath);
    if (
      claimed.kind !== "safe" ||
      !samePidFileObject(claimIdentity, claimed.fileIdentity) ||
      claimed.raw !== current.raw
    ) {
      restorePidClaim(claimPath, absolutePath, claimIdentity);
      return false;
    }
    claimIdentity = claimed.fileIdentity;
    hooks.afterClaim?.();
    const verified = inspectCollectorPidFile(claimPath);
    if (
      verified.kind !== "safe" ||
      !samePidFileIdentity(claimIdentity, verified.fileIdentity) ||
      verified.raw !== current.raw ||
      lstatPidPath(absolutePath)
    ) {
      restorePidClaim(claimPath, absolutePath, claimIdentity);
      return false;
    }
    const beforeUnlink = lstatPidPath(claimPath);
    if (!beforeUnlink || !samePidFileIdentity(claimIdentity, pidFileIdentity(beforeUnlink))) {
      return false;
    }
    fs.unlinkSync(claimPath);
    return !lstatPidPath(claimPath) && !lstatPidPath(absolutePath);
  } catch {
    if (claimIdentity) restorePidClaim(claimPath, absolutePath, claimIdentity);
    return false;
  }
}

function restorePidClaim(
  claimPath: string,
  destination: string,
  expectedClaim: PidFileIdentity,
) {
  try {
    if (lstatPidPath(destination)) return false;
    const claim = lstatPidPath(claimPath);
    if (
      !claim ||
      claim.isSymbolicLink() ||
      !claim.isFile() ||
      !samePidFileObject(expectedClaim, pidFileIdentity(claim))
    ) return false;
    fs.linkSync(claimPath, destination);
    fs.unlinkSync(claimPath);
    const restored = lstatPidPath(destination);
    return Boolean(restored && samePidFileObject(expectedClaim, pidFileIdentity(restored)));
  } catch {
    return false;
  }
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
}): Promise<LaunchAgentUnloadPriorState> {
  const readPidFile = options.readPidFile ?? readCollectorPidFile;
  const [label, listener] = await Promise.all([
    options.observeLabel(),
    options.observeListener?.() ?? observeCollectorListener(options.port),
  ]);
  const pidRead = readPidFile(options.pidPath, options.label);
  const pidIdentity = pidRuntimeIdentity(pidRead);
  const listenerIdentity = listener.kind === "collector" ? listener.runtimeIdentity : null;
  const ownership = listener.kind === "unrelated"
    ? "unrelated" as const
    : pidRead.kind === "legacy" ||
        pidRead.kind === "invalid" ||
        pidRead.kind === "unsafe" ||
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
  removePidFile: (
    pidPath: string,
    identity: CollectorRuntimeIdentity,
    label: string,
  ) => boolean;
  removedPidFile: boolean;
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
  let currentPidIdentity = pidRuntimeIdentity(pidRead);
  let currentPidMatchesPrior = matchesAnyPrior(currentPidIdentity, priorIdentities);
  let removedPidFile = options.removedPidFile;
  if (
    currentPidIdentity &&
    currentPidMatchesPrior &&
    options.prior.ownership === "consistent" &&
    !options.processIsLive(currentPidIdentity)
  ) {
    removedPidFile = options.removePidFile(
      options.pidPath,
      currentPidIdentity,
      options.label,
    ) || removedPidFile;
    pidRead = options.readPidFile(options.pidPath, options.label);
    currentPidIdentity = pidRuntimeIdentity(pidRead);
    currentPidMatchesPrior = matchesAnyPrior(currentPidIdentity, priorIdentities);
  }

  const pidCleaned = pidRead.kind === "missing";
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
    options.prior.ownership === "unproven" &&
    (options.prior.pidRecordKind === "legacy" || options.prior.pidRecordKind === "invalid")
  ) {
    state = "stale_owned_record";
  } else if (
    labelState.kind === "query_failed" ||
    pidRead.kind === "unsafe" ||
    listenerState.kind === "indeterminate" ||
    options.prior.ownership === "unproven"
  ) {
    state = "indeterminate";
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
  removePidFile?: (
    pidPath: string,
    identity: CollectorRuntimeIdentity,
    label: string,
  ) => boolean;
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
  const removePidFile = options.removePidFile ?? removeCollectorPidFileIfOwned;
  let removedPidFile = false;
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
      removePidFile,
      removedPidFile,
    });
    observations += 1;
    removedPidFile = final.removedPidFile;
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
    removePidFile,
    removedPidFile,
  });
  observations += 1;
  removedPidFile = final.removedPidFile;
  const elapsedMs = Math.max(0, receiptBoundaryAt - startedAt);
  return Object.freeze({
    stopped: final.stopped,
    state: final.state,
    pidCleaned: final.pidCleaned,
    removedPidFile,
    final,
    timing: {
      timeoutMs,
      pollIntervalMs,
      elapsedMs,
      observations,
      deadlineCrossed,
      finalObservation: true as const,
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
