import { randomUUID } from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { collectorHome } from "./config";

/**
 * Issue #158: the single cross-process lifecycle mutation authority.
 *
 * Every mutating step over the collector LaunchAgent (load, unload, install,
 * uninstall, update, rollback, owned-PID cleanup) must hold one lease from
 * THIS authority domain. The lease carries a durable, monotonically
 * increasing fencing revision plus an expiring owner identity, so a slow or
 * crashed predecessor can never authorize work against a successor: once a
 * newer revision exists, every older revision revalidates as superseded and
 * authorizes nothing.
 *
 * Properties enforced here:
 * - durable: revisions are files under the authority root, allocated by an
 *   atomic create-if-absent link, never renamed or removed;
 * - private: directories 0700, records 0600, user-owned;
 * - no-follow: every path segment is lstat-checked and record reads bind
 *   O_NOFOLLOW descriptors with before/after identity comparison;
 * - literal states: busy/expired/superseded/released outcomes are enumerated
 *   codes containing no filesystem paths;
 * - ambiguous authorizes nothing: any unexpected entry, malformed record, or
 *   unsafe object fails closed without destructive cleanup.
 *
 * No daemon, watcher, polling loop, credential movement, or hosted control
 * surface exists here: it is a plain synchronous library over the local
 * filesystem.
 */

export const LIFECYCLE_AUTHORITY_SCHEMA_VERSION = 1 as const;
export const LIFECYCLE_MUTATION_OPERATION = "launch-agent-mutation" as const;

/** One mutation domain for the whole estate: every LaunchAgent/runtime/PID
 * mutation acquires from the same canonical root. */
export function defaultLifecycleAuthorityRoot(homeDir = os.homedir()) {
  return path.join(collectorHome(homeDir), "lifecycle-authority");
}

const LEASE_DIRECTORY_MODE = 0o700;
const LEASE_FILE_MODE = 0o600;
const REVISION_WIDTH = 20;
const REVISION_NAME = /^(\d{20})\.json$/;
const TEMPORARY_PREFIX = ".tmp-";
const MAX_RECORD_BYTES = 64 * 1024;
const MAX_LEASE_MS = 24 * 60 * 60 * 1000;
const DEFAULT_LEASE_MS = 10 * 60_000;
const OWNER_TOKEN_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export type LifecycleAuthorityAcquisition =
  | { kind: "acquired"; lease: LifecycleMutationLease }
  | { kind: "busy"; reason: "held_by_current_owner"; currentRevision: number; busyUntilMs: number }
  | { kind: "ambiguous"; reason: LifecycleAmbiguityReason };

export type LifecycleAmbiguityReason =
  | "authority_root_unsafe"
  | "lease_directory_unsafe"
  | "unexpected_entry"
  | "revision_name_invalid"
  | "record_unreadable"
  | "record_malformed";

export type LifecycleStalenessReason =
  | "superseded"
  | "released"
  | "expired"
  | "missing"
  | "ambiguous";

export type LifecycleRevalidation =
  | { ok: true; revision: number }
  | { ok: false; reason: LifecycleStalenessReason };

export type LifecycleReleaseOutcome = "released" | "not_owner" | "changed" | "missing" | "ambiguous";

type LeaseRecord = {
  schemaVersion: typeof LIFECYCLE_AUTHORITY_SCHEMA_VERSION;
  revision: number;
  ownerToken: string;
  operation: typeof LIFECYCLE_MUTATION_OPERATION;
  state: "held" | "released";
  ownerPid: number;
  acquiredAt: string;
  expiresAtMs: number;
};

type LeaseEntry = {
  revision: number;
  record: LeaseRecord;
  raw: string;
  device: number;
  inode: number;
};

export type LifecycleMutationLease = Readonly<{
  revision: number;
  ownerToken: string;
  expiresAtMs: number;
  /** Revalidates the exact fence; call immediately before every mutating step. */
  assertCurrent: () => LifecycleRevalidation;
  /** Marks only this lease's own record released. Never touches successors. */
  release: () => LifecycleReleaseOutcome;
}>;

function leaseName(revision: number) {
  return `${String(revision).padStart(REVISION_WIDTH, "0")}.json`;
}

function isSafeIntegerBounded(value: unknown, minimum: number, maximum: number): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= minimum && value <= maximum;
}

function parseLeaseRecord(raw: string, expectedRevision: number): LeaseRecord | null {
  let value: unknown;
  try {
    value = JSON.parse(raw);
  } catch {
    return null;
  }
  if (!value || typeof value !== "object" || Array.isArray(value)) return null;
  const row = value as Partial<LeaseRecord>;
  if (
    Object.keys(row).sort().join("|") !==
      "acquiredAt|expiresAtMs|operation|ownerPid|ownerToken|revision|schemaVersion|state" ||
    row.schemaVersion !== LIFECYCLE_AUTHORITY_SCHEMA_VERSION ||
    row.revision !== expectedRevision ||
    typeof row.ownerToken !== "string" ||
    !OWNER_TOKEN_PATTERN.test(row.ownerToken) ||
    row.operation !== LIFECYCLE_MUTATION_OPERATION ||
    (row.state !== "held" && row.state !== "released") ||
    !isSafeIntegerBounded(row.ownerPid, 1, Number.MAX_SAFE_INTEGER) ||
    typeof row.acquiredAt !== "string" ||
    !Number.isFinite(Date.parse(row.acquiredAt)) ||
    !isSafeIntegerBounded(row.expiresAtMs, 0, Number.MAX_SAFE_INTEGER)
  ) {
    return null;
  }
  return row as LeaseRecord;
}

function lstatIfPresent(candidate: string) {
  try {
    return fs.lstatSync(candidate);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

function uid() {
  return typeof process.getuid === "function" ? process.getuid() : undefined;
}

/** Validates candidate and every directory from stopAt down to it,
 * rejecting symlinks, non-directories, foreign owners, and any mode other
 * than 0700. Ancestors above stopAt are outside the authority boundary. */
function assertPrivateAncestors(candidate: string, stopAt: string) {
  const resolved = path.resolve(candidate);
  const stop = path.resolve(stopAt);
  const relative = path.relative(stop, resolved);
  if (relative.startsWith("..") || path.isAbsolute(relative)) {
    throw new Error("authority path escapes its root");
  }
  const segments = relative === ""
    ? [path.basename(resolved)]
    : [path.basename(stop), ...relative.split(path.sep).filter(Boolean)];
  let current = path.dirname(stop);
  for (const segment of segments) {
    current = path.join(current, segment);
    const stat = lstatIfPresent(current);
    if (!stat || stat.isSymbolicLink() || !stat.isDirectory()) {
      throw new Error("authority path is unsafe");
    }
    const owner = uid();
    if (owner !== undefined && stat.uid !== owner) throw new Error("authority path is not user-owned");
    if ((stat.mode & 0o7777) !== LEASE_DIRECTORY_MODE) throw new Error("authority path is not private");
  }
}

function ensurePrivateDirectory(directory: string, boundary: string) {
  assertPrivateAncestors(path.dirname(directory), boundary);
  const existing = lstatIfPresent(directory);
  if (!existing) {
    try {
      fs.mkdirSync(directory, { mode: LEASE_DIRECTORY_MODE });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    }
  }
  assertPrivateAncestors(directory, boundary);
}

function boundReadFile(file: string):
  | { kind: "raw"; raw: string; device: number; inode: number }
  | { kind: "unsafe" } {
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    const before = fs.fstatSync(descriptor);
    if (before.nlink !== 1 || before.size > MAX_RECORD_BYTES) return { kind: "unsafe" };
    const chunks: Buffer[] = [];
    let position = 0;
    while (position < before.size) {
      const chunk = Buffer.allocUnsafe(64 * 1024);
      const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, position);
      if (bytesRead === 0) break;
      chunks.push(chunk.subarray(0, bytesRead));
      position += bytesRead;
      if (position > MAX_RECORD_BYTES) return { kind: "unsafe" };
    }
    const after = fs.fstatSync(descriptor);
    if (after.dev !== before.dev || after.ino !== before.ino || after.size !== before.size ||
        after.mtimeMs !== before.mtimeMs || after.mode !== before.mode || after.nlink !== 1) {
      return { kind: "unsafe" };
    }
    return { kind: "raw", raw: Buffer.concat(chunks).toString("utf8"), device: before.dev, inode: before.ino };
  } catch {
    return { kind: "unsafe" };
  } finally {
    if (descriptor !== undefined) {
      try {
        fs.closeSync(descriptor);
      } catch {
        // Descriptor already closed.
      }
    }
  }
}

type DomainScan =
  | { kind: "scanned"; entries: LeaseEntry[] }
  | { kind: "ambiguous"; reason: LifecycleAmbiguityReason };

export class LifecycleMutationAuthority {
  private readonly root: string;
  private readonly leasesDirectory: string;
  private readonly defaultLeaseMs: number;

  constructor(root: string, options: { defaultLeaseMs?: number } = {}) {
    if (!path.isAbsolute(root)) throw new Error("authority root must be absolute");
    this.root = path.resolve(root);
    this.leasesDirectory = path.join(this.root, "leases");
    const leaseMs = options.defaultLeaseMs ?? DEFAULT_LEASE_MS;
    if (!isSafeIntegerBounded(leaseMs, 1, MAX_LEASE_MS)) {
      throw new Error("default lease duration must be between 1 and 86400000 milliseconds");
    }
    this.defaultLeaseMs = leaseMs;
  }

  private initialize() {
    if (!lstatIfPresent(this.root)) {
      fs.mkdirSync(this.root, { recursive: true, mode: LEASE_DIRECTORY_MODE });
    }
    assertPrivateAncestors(this.root, path.dirname(this.root));
    ensurePrivateDirectory(this.leasesDirectory, this.root);
  }

  private scan(): DomainScan {
    let names: string[];
    try {
      this.initialize();
      names = fs.readdirSync(this.leasesDirectory);
    } catch {
      return { kind: "ambiguous", reason: "lease_directory_unsafe" };
    }
    const entries: LeaseEntry[] = [];
    for (const name of names.sort()) {
      const match = name.match(REVISION_NAME);
      if (!match) return { kind: "ambiguous", reason: "unexpected_entry" };
      const revision = Number(match[1]);
      if (!isSafeIntegerBounded(revision, 1, Number.MAX_SAFE_INTEGER)) {
        return { kind: "ambiguous", reason: "revision_name_invalid" };
      }
      const read = boundReadFile(path.join(this.leasesDirectory, name));
      if (read.kind === "unsafe") return { kind: "ambiguous", reason: "record_unreadable" };
      const record = parseLeaseRecord(read.raw, revision);
      if (!record) return { kind: "ambiguous", reason: "record_malformed" };
      entries.push({ revision, record, raw: read.raw, device: read.device, inode: read.inode });
    }
    return { kind: "scanned", entries };
  }

  /**
   * Allocates the next monotonic revision. The created record is immutable:
   * it enters the domain through an atomic create-if-absent hard link and is
   * afterwards touched only by its owner's in-place release marker.
   */
  acquire(options: { leaseMs?: number; now?: () => number } = {}): LifecycleAuthorityAcquisition {
    const now = options.now ?? Date.now;
    const leaseMs = options.leaseMs ?? this.defaultLeaseMs;
    if (!isSafeIntegerBounded(leaseMs, 1, MAX_LEASE_MS)) {
      throw new Error("lease duration must be between 1 and 86400000 milliseconds");
    }
    this.initialize();
    for (let attempt = 0; attempt < 64; attempt += 1) {
      const scan = this.scan();
      if (scan.kind === "ambiguous") return scan;
      const top = [...scan.entries].sort((left, right) => left.revision - right.revision).at(-1);
      if (top && top.record.state === "held" && now() < top.record.expiresAtMs) {
        return {
          kind: "busy",
          reason: "held_by_current_owner",
          currentRevision: top.revision,
          busyUntilMs: top.record.expiresAtMs,
        };
      }
      const revision = (top?.revision ?? 0) + 1;
      const acquiredAtMs = now();
      const record: LeaseRecord = {
        schemaVersion: LIFECYCLE_AUTHORITY_SCHEMA_VERSION,
        revision,
        ownerToken: randomUUID(),
        operation: LIFECYCLE_MUTATION_OPERATION,
        state: "held",
        ownerPid: process.pid,
        acquiredAt: new Date(acquiredAtMs).toISOString(),
        expiresAtMs: acquiredAtMs + leaseMs,
      };
      const raw = `${JSON.stringify(record)}\n`;
      const temporary = path.join(this.root, `${TEMPORARY_PREFIX}${randomUUID()}`);
      let linked = false;
      try {
        fs.writeFileSync(temporary, raw, { flag: "wx", mode: LEASE_FILE_MODE });
        try {
          fs.linkSync(temporary, path.join(this.leasesDirectory, leaseName(revision)));
          linked = true;
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
          // Another acquirer claimed this revision first; re-scan and retry.
        }
      } finally {
        try {
          fs.unlinkSync(temporary);
        } catch {
          // After a successful link only the duplicate name is removed; a
          // crashed allocator may leave an inert sibling in the root, which
          // the domain scan never reads.
        }
      }
      if (!linked) continue;
      try {
        const descriptor = fs.openSync(this.leasesDirectory, fs.constants.O_RDONLY);
        try {
          fs.fsyncSync(descriptor);
        } finally {
          fs.closeSync(descriptor);
        }
      } catch {
        // Directory fsync is best-effort durability; correctness comes from
        // the immutable linked record itself.
      }
      try {
        const stat = fs.lstatSync(path.join(this.leasesDirectory, leaseName(revision)));
        if (stat.isSymbolicLink() || !stat.isFile()) {
          return { kind: "ambiguous", reason: "record_unreadable" };
        }
        return {
          kind: "acquired",
          lease: buildLease(
            this.leasesDirectory,
            this.scan.bind(this),
            { record, raw, device: stat.dev, inode: stat.ino },
          ),
        };
      } catch {
        return { kind: "ambiguous", reason: "record_unreadable" };
      }
    }
    return { kind: "ambiguous", reason: "unexpected_entry" };
  }

  /** Read-only view of the domain head; acquires nothing and mutates nothing. */
  observe(options: { now?: () => number } = {}):
    | { kind: "free" }
    | { kind: "held"; currentRevision: number; expiresAtMs: number }
    | { kind: "ambiguous"; reason: LifecycleAmbiguityReason } {
    const now = options.now ?? Date.now;
    const scan = this.scan();
    if (scan.kind === "ambiguous") return scan;
    const top = [...scan.entries].sort((left, right) => left.revision - right.revision).at(-1);
    if (!top) return { kind: "free" };
    if (top.record.state === "released" || now() >= top.record.expiresAtMs) return { kind: "free" };
    return { kind: "held", currentRevision: top.revision, expiresAtMs: top.record.expiresAtMs };
  }
}

function revalidate(
  leasesDirectory: string,
  scan: () => DomainScan,
  owned: { record: LeaseRecord; raw: string; device: number; inode: number },
): LifecycleRevalidation {
  const result = scan();
  if (result.kind === "ambiguous") return { ok: false, reason: "ambiguous" };
  if (result.entries.some((entry) => entry.revision > owned.record.revision)) {
    return { ok: false, reason: "superseded" };
  }
  const mine = result.entries.find((entry) => entry.revision === owned.record.revision);
  if (!mine) return { ok: false, reason: "missing" };
  // The exact object recorded at acquire time must still sit at this
  // revision; a swapped or hardlinked replacement is ambiguous.
  if (mine.device !== owned.device || mine.inode !== owned.inode) {
    return { ok: false, reason: "ambiguous" };
  }
  if (mine.record.ownerToken !== owned.record.ownerToken) return { ok: false, reason: "ambiguous" };
  if (
    mine.record.acquiredAt !== owned.record.acquiredAt ||
    mine.record.expiresAtMs !== owned.record.expiresAtMs ||
    mine.record.ownerPid !== owned.record.ownerPid
  ) {
    return { ok: false, reason: "ambiguous" };
  }
  // Our own in-place release marker is the only legitimate byte change.
  if (mine.record.state === "released") return { ok: false, reason: "released" };
  if (Date.now() >= mine.record.expiresAtMs) return { ok: false, reason: "expired" };
  return { ok: true, revision: owned.record.revision };
}

function releaseLease(
  leasesDirectory: string,
  owned: { record: LeaseRecord; raw: string; device: number; inode: number },
): LifecycleReleaseOutcome {
  const file = path.join(leasesDirectory, leaseName(owned.record.revision));
  let descriptor: number | undefined;
  try {
    descriptor = fs.openSync(file, fs.constants.O_RDWR | fs.constants.O_NOFOLLOW);
    const stat = fs.fstatSync(descriptor);
    if (stat.dev !== owned.device || stat.ino !== owned.inode) return "not_owner";
    const chunks: Buffer[] = [];
    let position = 0;
    while (position < owned.raw.length) {
      const chunk = Buffer.allocUnsafe(64 * 1024);
      const bytesRead = fs.readSync(descriptor, chunk, 0, chunk.length, position);
      if (bytesRead === 0) break;
      chunks.push(chunk.subarray(0, bytesRead));
      position += bytesRead;
      if (position > MAX_RECORD_BYTES) return "ambiguous";
    }
    const current = Buffer.concat(chunks).toString("utf8");
    if (current !== owned.raw) return "changed";
    if (owned.record.state === "released") return "released";
    const updated = `${JSON.stringify({ ...owned.record, state: "released" })}\n`;
    fs.ftruncateSync(descriptor, 0);
    let offset = 0;
    const content = Buffer.from(updated, "utf8");
    while (offset < content.length) {
      offset += fs.writeSync(descriptor, content, offset, content.length - offset, offset);
    }
    fs.fsyncSync(descriptor);
    return fs.readFileSync(file, "utf8") === updated ? "released" : "changed";
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return "missing";
    return "ambiguous";
  } finally {
    if (descriptor !== undefined) {
      try {
        fs.closeSync(descriptor);
      } catch {
        // Descriptor already closed.
      }
    }
  }
}

function buildLease(
  leasesDirectory: string,
  scan: () => DomainScan,
  owned: { record: LeaseRecord; raw: string; device: number; inode: number },
): LifecycleMutationLease {
  return Object.freeze({
    revision: owned.record.revision,
    ownerToken: owned.record.ownerToken,
    expiresAtMs: owned.record.expiresAtMs,
    assertCurrent: () => revalidate(leasesDirectory, scan, owned),
    release: () => releaseLease(leasesDirectory, owned),
  });
}
