import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import type Database from "better-sqlite3";

import {
  aiInteractionEventSchema,
  toolSourceSchema,
  type AiInteractionEvent,
} from "../../shared/src/index";
import type { LocalEventBuffer } from "./buffer";
import { hookSpoolEntryTrusted } from "./hook-spool";
import type { LocalProducerSource } from "./http-boundary";
import type { MetricSample } from "./otlp";
import { OTLP_DROP_REASONS, type OtlpAdmissionDrop } from "./otlp-admission";
import { peekRepoContextSidecar } from "./repo-context";
import { isSqliteContentionError } from "./sqlite-contention";

/**
 * Bead eco-6hoxj.163.17: OTLP requests the ledger could not take in time are
 * kept, not lost.
 *
 * Before this module an authenticated, validated OTLP export that met a busy
 * ledger (503 `storage_busy_retry`) or missed the 1.5 s request deadline (408
 * `request_deadline_exceeded`) was answered and forgotten: OTLP was not
 * spooled, exporter retries live only in the exporter's memory, and 408 is not
 * an OTLP-retryable status, so a spec-following exporter drops that batch.
 *
 * What is written here is NOT the request body. It is the batch the OTLP route
 * already built for `LocalEventBuffer.appendMany`: the normalized events and
 * metric samples `explodeOtlpPayload` produced after the ledger's own
 * sanitization (`sanitizeForPolicy` + the admitted-attribute allowlist), plus
 * the admission-drop counts. Each event serializes to exactly the
 * `payload_json` the ledger would store. So the spool holds nothing the ledger
 * would not hold, with one deliberate subtraction: the transient repository
 * sidecar (a raw working directory the ledger never persists) is not written,
 * and the events that carried one are counted (`repoContextDropped`) because a
 * replayed event cannot be linked to a repository.
 *
 * Writing the normalized batch also fixes each event's id and time at arrival:
 * `recordTimestamp` falls back to the clock for a record with no usable time,
 * so re-exploding a body later would mint different ids and times.
 *
 * Replay is exactly once. Each 16-row chunk is committed through the same
 * `appendMany` call the live route uses, in one immediate transaction together
 * with a cursor row in the existing `maintenance_state` table. Events dedupe by
 * id and payload digest, metric samples upsert by id, and the cursor makes the
 * non-idempotent admission-drop counters (chunk 0) commit once. A file is
 * removed only after its last chunk committed and the ledger WAL was flushed
 * with the same `F_FULLFSYNC` the spool file itself was written with.
 *
 * The daemon is the only writer of this directory, so its bounds are kept in
 * memory and checked without a directory scan on the request path.
 */

export const OTLP_SPOOL_DIRECTORY = "otlp-spool";
export const OTLP_SPOOL_REJECTED_DIRECTORY = "rejected";
export const OTLP_SPOOL_COUNTERS_FILE = ".counters.json";
/** Kill switch: `PLIMSOLL_OTLP_SPOOL=off` in the daemon's environment. */
export const OTLP_SPOOL_ENV = "PLIMSOLL_OTLP_SPOOL";
/** The live route's writer turn: at most 16 events and 16 samples per transaction. */
export const OTLP_COMMIT_CHUNK_SIZE = 16;
const CURSOR_KEY_PREFIX = "otlp_intake_spool:v1:";

export const OTLP_SPOOL_LIMITS = Object.freeze({
  /** One file per refused request, the hook spool's file ceiling. */
  maxFiles: 5_000,
  /** Normalized batches, not bodies; see REPORT for the sizing. */
  maxBytes: 256 * 1024 * 1024,
  /** Acknowledged data older than this is deleted and counted as lost. */
  maxAgeMs: 7 * 24 * 60 * 60 * 1000,
  /** Drain cadence and the wall-clock ceiling of one pass. */
  drainIntervalMs: 2_000,
  maxPassMs: 250,
  maxFilesPerPass: 64,
  /** A file whose replay fails this many times (not busy, not a storage fault) is quarantined. */
  replayFailureLimit: 5,
  maxRejectedFiles: 100,
  rejectedMaxAgeMs: 7 * 24 * 60 * 60 * 1000,
  /** Orphan temporaries: younger ones may still be mid-write. */
  temporaryOrphanMs: 60_000,
  /** Stale cursor rows and quarantine retention, every N drain ticks. */
  housekeepingEveryTicks: 150,
});

export type OtlpSpoolLimits = typeof OTLP_SPOOL_LIMITS;

export type OtlpIntakeEntry = { event: AiInteractionEvent; suppressedFields: string[] };

/** Exactly the three arguments `LocalEventBuffer.appendMany` takes. */
export type OtlpIntakeBatch = {
  events: OtlpIntakeEntry[];
  metricSamples: MetricSample[];
  admissionDrops: OtlpAdmissionDrop[];
};

export type OtlpSpoolCause = "storage_busy_retry" | "request_deadline_exceeded";

export function otlpChunkCount(batch: OtlpIntakeBatch) {
  return Math.max(
    Math.ceil(batch.events.length / OTLP_COMMIT_CHUNK_SIZE),
    Math.ceil(batch.metricSamples.length / OTLP_COMMIT_CHUNK_SIZE),
    1,
  );
}

/** Chunk `index` of a batch; admission drops ride with chunk 0 only, as they always have. */
export function otlpChunk(batch: OtlpIntakeBatch, index: number): OtlpIntakeBatch {
  const start = index * OTLP_COMMIT_CHUNK_SIZE;
  return {
    events: batch.events.slice(start, start + OTLP_COMMIT_CHUNK_SIZE),
    metricSamples: batch.metricSamples.slice(start, start + OTLP_COMMIT_CHUNK_SIZE),
    admissionDrops: index === 0 ? batch.admissionDrops : [],
  };
}

/**
 * What is still uncommitted after `committedChunks` chunks of a live request
 * committed. A chunk that threw was rolled back whole (`appendMany` is one
 * immediate transaction), so the remainder starts exactly at the failed chunk.
 */
export function otlpBatchRemainder(batch: OtlpIntakeBatch, committedChunks: number): OtlpIntakeBatch {
  const start = committedChunks * OTLP_COMMIT_CHUNK_SIZE;
  return {
    events: batch.events.slice(start),
    metricSamples: batch.metricSamples.slice(start),
    admissionDrops: committedChunks === 0 ? batch.admissionDrops : [],
  };
}

export function otlpSpoolEnabled(env: NodeJS.ProcessEnv = process.env) {
  return env[OTLP_SPOOL_ENV] !== "off";
}

export function otlpSpoolDirectory(home: string) {
  return path.join(home, OTLP_SPOOL_DIRECTORY);
}

/**
 * `<receivedAtMillis>-<pid>-<sequence>-<random8>.json`. Strict on purpose: it
 * is the only thing that makes a pending file, so the counters file, a
 * temporary and the `rejected/` directory are never listed as pending.
 */
const SPOOL_FILE_PATTERN = /^(\d{13,})-(\d+)-(\d+)-([0-9a-f]{8})\.json$/;
const REJECTED_FILE_PATTERN = /^(\d{13,})-(\d+)-(\d+)-([0-9a-f]{8})\.([a-z0-9_]+)\.json$/;
const TEMPORARY_FILE_PATTERN = /\.json\.tmp$/;

type PendingFile = {
  stem: string;
  receivedAtMs: number;
  pid: number;
  sequence: number;
  bytes: number;
  /** Row counts; null for a file found at start-up, until it is read. */
  events: number | null;
  metricSamples: number | null;
  failures: number;
};

export type OtlpSpoolCounters = {
  spooled: number;
  spooledEvents: number;
  spooledMetricSamples: number;
  replayed: number;
  replayedEvents: number;
  replayedMetricSamples: number;
  replayDeduplicated: number;
  replayCollisions: number;
  replayEnrollmentRejected: number;
  refusedFileCap: number;
  refusedByteCap: number;
  writeFailed: number;
  expired: number;
  expiredEvents: number;
  rejected: number;
  deferredPasses: number;
  repoContextDropped: number;
  lastSpooledAt: string | null;
  lastReplayedAt: string | null;
};

const EMPTY_COUNTERS: OtlpSpoolCounters = Object.freeze({
  spooled: 0,
  spooledEvents: 0,
  spooledMetricSamples: 0,
  replayed: 0,
  replayedEvents: 0,
  replayedMetricSamples: 0,
  replayDeduplicated: 0,
  replayCollisions: 0,
  replayEnrollmentRejected: 0,
  refusedFileCap: 0,
  refusedByteCap: 0,
  writeFailed: 0,
  expired: 0,
  expiredEvents: 0,
  rejected: 0,
  deferredPasses: 0,
  repoContextDropped: 0,
  lastSpooledAt: null,
  lastReplayedAt: null,
});

function counterValue(value: unknown) {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : 0;
}

export type OtlpSpoolStatus = {
  enabled: boolean;
  /** False when the directory exists but is not a private directory of this uid. */
  directoryTrusted: boolean;
  pendingFiles: number;
  pendingBytes: number;
  oldestPendingAgeSeconds: number | null;
  spooled: { requests: number; events: number; metricSamples: number; lastAt: string | null };
  replayed: {
    requests: number;
    events: number;
    metricSamples: number;
    deduplicated: number;
    collisions: number;
    /** Rows the ledger's own enrollment rule refused on replay, as it would live. */
    enrollmentRejected: number;
    lastAt: string | null;
  };
  /**
   * `fileCap`/`byteCap`: requests the spool could not hold (answered 503 with
   * Retry-After, so an exporter may still resend them). `ageCap`: files that
   * were acknowledged and then deleted unreplayed at the age limit — lost.
   */
  droppedByCap: { fileCap: number; byteCap: number; ageCap: number; ageCapEvents: number };
  writeFailed: number;
  rejectedOnReplay: number;
  deferredPasses: number;
  repoContextDropped: number;
  limits: { maxFiles: number; maxBytes: number; maxAgeSeconds: number };
  counterLifetime: "durable";
};

export type OtlpSpoolRefusal =
  | "spool_disabled"
  | "spool_untrusted"
  | "spool_bounds"
  | "spool_encode_failed"
  | "spool_write_failed";

export type OtlpSpoolWriteOutcome =
  | { ok: true; id: string; events: number; metricSamples: number }
  | { ok: false; attempted: boolean; refused: OtlpSpoolRefusal; bound?: "files" | "bytes" };

export type OtlpSpoolWriteInput = {
  source: LocalProducerSource;
  transportPath: string | undefined;
  receivedAtMs: number;
  cause: OtlpSpoolCause;
  /** Chunks the live request had already committed; informational. */
  committedChunks: number;
  batch: OtlpIntakeBatch;
};

type OtlpSpoolEnvelope = {
  v: 1;
  kind: "otlp_intake_spool";
  id: string;
  source: LocalProducerSource;
  transportPath: string | null;
  receivedAt: string;
  cause: OtlpSpoolCause;
  committedChunks: number;
  repoContextDropped: number;
  digest: string;
  batch: OtlpIntakeBatch;
};

export type OtlpSpoolDrainPass = {
  replayed: number;
  replayedEvents: number;
  replayedMetricSamples: number;
  chunks: number;
  rejected: number;
  expired: number;
  /** True when a busy ledger stopped the pass. */
  deferred: boolean;
  /** True when the pass stopped at its time or file ceiling with work left. */
  budgetExhausted: boolean;
  failed: number;
};

/**
 * Seams for the crash proofs: each fires at the exact point a crash would
 * leave the state it names. Production passes none.
 */
export type OtlpSpoolHooks = {
  afterDurableWrite?: (id: string) => void;
  afterChunkCommitted?: (id: string, chunk: number, chunks: number) => void;
  beforeRemove?: (ids: string[]) => void;
};

function spoolSha256Hex(text: string) {
  return crypto.createHash("sha256").update(text).digest("hex");
}

function errorCode(error: unknown) {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" ? code : "unknown";
}

/**
 * Storage conditions that say nothing about the file being replayed: a full
 * disk or an I/O error defers the pass like a busy ledger does and never
 * counts toward quarantine, or a bad hour on the disk would quarantine data
 * that was already acknowledged.
 */
function isTransientStorageError(code: string) {
  return /^SQLITE_(FULL|IOERR|CANTOPEN|NOMEM|READONLY|INTERRUPT|PROTOCOL)/.test(code) ||
    code === "ENOSPC" || code === "EIO";
}

async function syncPath(target: string) {
  const handle = await fs.promises.open(target, "r");
  try {
    // libuv issues F_FULLFSYNC on macOS (falling back to F_BARRIERFSYNC and
    // fsync), the same flush the hook spool relies on.
    await handle.sync();
  } finally {
    await handle.close();
  }
}

/**
 * Flush the committed ledger bytes before a replayed file is removed.
 *
 * The ledger commits with `synchronous=FULL` in WAL mode but `fullfsync=0`, so
 * on macOS its commit fsync does not flush the drive's write cache, while the
 * spool file was flushed with F_FULLFSYNC. Deleting the file on the strength of
 * the commit alone would lower durability for exactly the events that were
 * already hard to keep. F_FULLFSYNC asks the drive to flush all buffered data,
 * so one flush of the WAL (or of the database file when no WAL exists) covers
 * frames a checkpoint may already have copied into the main file.
 */
async function syncLedger(db: Database.Database) {
  if (db.memory || !db.name) return;
  for (const candidate of [`${db.name}-wal`, db.name]) {
    try {
      await syncPath(candidate);
      return;
    } catch (error) {
      if (errorCode(error) === "ENOENT") continue;
      throw error;
    }
  }
}

function readCursor(db: Database.Database, key: string) {
  const row = db.prepare(`select value from maintenance_state where key = ?`).get(key) as
    | { value: string }
    | undefined;
  if (!row) return null;
  try {
    const parsed = JSON.parse(row.value) as Record<string, unknown>;
    if (
      Number.isSafeInteger(parsed.nextChunk) &&
      Number.isSafeInteger(parsed.chunks) &&
      typeof parsed.digest === "string"
    ) {
      return {
        nextChunk: parsed.nextChunk as number,
        chunks: parsed.chunks as number,
        digest: parsed.digest,
      };
    }
  } catch {
    /* an unreadable cursor is treated as absent; replay is idempotent for rows */
  }
  return null;
}

function writeCursor(
  db: Database.Database,
  key: string,
  cursor: { nextChunk: number; chunks: number; digest: string },
) {
  db.prepare(
    `insert into maintenance_state (key, value, updated_at) values (?, ?, ?)
     on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
  ).run(key, JSON.stringify(cursor), new Date().toISOString());
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((entry) => typeof entry === "string");
}

function optionalString(value: unknown) {
  return value === undefined || typeof value === "string";
}

const DROP_REASONS = new Set<string>(OTLP_DROP_REASONS);

/**
 * Validate a batch read back from disk. The directory and file are private to
 * this uid and the digest detects a torn or edited file; the schema checks are
 * the same shape the live route produced, so a replay can never commit
 * something the route could not have.
 */
function validatedBatch(value: unknown): OtlpIntakeBatch | null {
  if (!isPlainRecord(value)) return null;
  const { events, metricSamples, admissionDrops } = value;
  if (!Array.isArray(events) || !Array.isArray(metricSamples) || !Array.isArray(admissionDrops)) return null;
  const entries: OtlpIntakeEntry[] = [];
  for (const entry of events) {
    if (!isPlainRecord(entry) || !isStringArray(entry.suppressedFields)) return null;
    const parsed = aiInteractionEventSchema.safeParse(entry.event);
    if (!parsed.success) return null;
    entries.push({ event: parsed.data, suppressedFields: entry.suppressedFields });
  }
  for (const sample of metricSamples) {
    if (
      !isPlainRecord(sample) ||
      typeof sample.id !== "string" || sample.id.length === 0 ||
      !toolSourceSchema.safeParse(sample.source).success ||
      typeof sample.metricName !== "string" ||
      typeof sample.observedAt !== "string" || Number.isNaN(Date.parse(sample.observedAt)) ||
      !optionalString(sample.sessionId) || !optionalString(sample.model) || !optionalString(sample.sampleType) ||
      typeof sample.value !== "number" || !Number.isFinite(sample.value) ||
      !isPlainRecord(sample.attrs) ||
      !isStringArray(sample.suppressedFields)
    ) {
      return null;
    }
  }
  for (const drop of admissionDrops) {
    if (
      !isPlainRecord(drop) ||
      !toolSourceSchema.safeParse(drop.source).success ||
      typeof drop.reason !== "string" || !DROP_REASONS.has(drop.reason) ||
      !Number.isSafeInteger(drop.count) || (drop.count as number) <= 0
    ) {
      return null;
    }
  }
  return {
    events: entries,
    metricSamples: metricSamples as MetricSample[],
    admissionDrops: admissionDrops as OtlpAdmissionDrop[],
  };
}

const PRODUCER_SOURCES = new Set<string>(["claude_code", "codex", "gemini_cli", "grok"]);
const CAUSES = new Set<string>(["storage_busy_retry", "request_deadline_exceeded"]);

type ReadResult =
  | { ok: true; envelope: OtlpSpoolEnvelope }
  | { ok: false; reason: "spool_untrusted" | "spool_digest_mismatch" | "spool_invalid" }
  /** A storage fault, not a verdict on the file: the pass defers. */
  | { ok: false; reason: "spool_unreadable" };

async function readEnvelope(file: string, stem: string): Promise<ReadResult> {
  if (!hookSpoolEntryTrusted(file, "file")) return { ok: false, reason: "spool_untrusted" };
  let text: string;
  try {
    text = await fs.promises.readFile(file, "utf8");
  } catch {
    return { ok: false, reason: "spool_unreadable" };
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return { ok: false, reason: "spool_invalid" };
  }
  if (!isPlainRecord(parsed)) return { ok: false, reason: "spool_invalid" };
  if (
    parsed.v !== 1 ||
    parsed.kind !== "otlp_intake_spool" ||
    parsed.id !== stem ||
    typeof parsed.source !== "string" || !PRODUCER_SOURCES.has(parsed.source) ||
    !(parsed.transportPath === null || typeof parsed.transportPath === "string") ||
    typeof parsed.receivedAt !== "string" || Number.isNaN(Date.parse(parsed.receivedAt)) ||
    typeof parsed.cause !== "string" || !CAUSES.has(parsed.cause) ||
    typeof parsed.digest !== "string"
  ) {
    return { ok: false, reason: "spool_invalid" };
  }
  // The digest is over the batch exactly as the intake serialized it; JSON
  // round-trips these plain values byte for byte.
  if (`sha256:${spoolSha256Hex(JSON.stringify(parsed.batch))}` !== parsed.digest) {
    return { ok: false, reason: "spool_digest_mismatch" };
  }
  const batch = validatedBatch(parsed.batch);
  if (!batch) return { ok: false, reason: "spool_invalid" };
  return {
    ok: true,
    envelope: {
      v: 1,
      kind: "otlp_intake_spool",
      id: stem,
      source: parsed.source as LocalProducerSource,
      transportPath: parsed.transportPath as string | null,
      receivedAt: parsed.receivedAt,
      cause: parsed.cause as OtlpSpoolCause,
      committedChunks: counterValue(parsed.committedChunks),
      repoContextDropped: counterValue(parsed.repoContextDropped),
      digest: parsed.digest,
      batch,
    },
  };
}

export class OtlpIntakeSpool {
  readonly enabled: boolean;
  readonly home: string;
  readonly directory: string;
  private readonly limits: OtlpSpoolLimits;
  private readonly nowMs: () => number;
  private readonly hooks: OtlpSpoolHooks;
  private readonly warn: (line: Record<string, unknown>) => void;
  private readonly pending = new Map<string, PendingFile>();
  private pendingBytes = 0;
  private inflightFiles = 0;
  private inflightBytes = 0;
  private counters: OtlpSpoolCounters;
  private countersDirty = false;
  private directoryReady = false;
  private directoryTrusted = true;
  private sequence = 0;
  private timer: NodeJS.Timeout | undefined;
  private draining = false;
  private stopped = false;
  private ticks = 0;
  private drainLog = {
    lastAtMs: null as number | null,
    replayed: 0,
    events: 0,
    metricSamples: 0,
    rejected: 0,
    expired: 0,
  };

  constructor(options: {
    home: string;
    env?: NodeJS.ProcessEnv;
    limits?: Partial<OtlpSpoolLimits>;
    nowMs?: () => number;
    hooks?: OtlpSpoolHooks;
    onWarning?: (line: Record<string, unknown>) => void;
  }) {
    this.enabled = otlpSpoolEnabled(options.env ?? process.env);
    this.home = options.home;
    this.directory = otlpSpoolDirectory(options.home);
    this.limits = { ...OTLP_SPOOL_LIMITS, ...(options.limits ?? {}) };
    this.nowMs = options.nowMs ?? Date.now;
    this.hooks = options.hooks ?? {};
    this.warn = options.onWarning ?? ((line) => console.warn(JSON.stringify(line)));
    this.counters = this.readCounters();
    this.scan();
  }

  private countersPath() {
    return path.join(this.directory, OTLP_SPOOL_COUNTERS_FILE);
  }

  private readCounters(): OtlpSpoolCounters {
    try {
      const parsed = JSON.parse(fs.readFileSync(this.countersPath(), "utf8")) as Record<string, unknown>;
      const counters = { ...EMPTY_COUNTERS };
      for (const key of Object.keys(EMPTY_COUNTERS) as Array<keyof OtlpSpoolCounters>) {
        if (key === "lastSpooledAt" || key === "lastReplayedAt") {
          counters[key] = typeof parsed[key] === "string" ? (parsed[key] as string) : null;
        } else {
          counters[key] = counterValue(parsed[key]);
        }
      }
      return counters;
    } catch {
      return { ...EMPTY_COUNTERS };
    }
  }

  /** Counters are observability, not acceptance: written without a flush. */
  private persistCounters() {
    if (!this.countersDirty || !this.directoryReady) return;
    const target = this.countersPath();
    const temporary = `${target}.tmp`;
    try {
      fs.writeFileSync(temporary, `${JSON.stringify(this.counters)}\n`, { mode: 0o600 });
      fs.chmodSync(temporary, 0o600);
      fs.renameSync(temporary, target);
      this.countersDirty = false;
    } catch (error) {
      this.warn({ warning: "otlp_spool_counters_write_failed", code: errorCode(error) });
    }
  }

  private bump(key: Exclude<keyof OtlpSpoolCounters, "lastSpooledAt" | "lastReplayedAt">, by = 1) {
    if (by === 0) return;
    this.counters[key] = Math.min(Number.MAX_SAFE_INTEGER, this.counters[key] + by);
    this.countersDirty = true;
  }

  /**
   * Start-up inventory: the only directory scan outside a drain pass. Every
   * later bound check reads the in-memory index this builds.
   */
  private scan() {
    let stat: fs.Stats;
    try {
      stat = fs.lstatSync(this.directory);
    } catch {
      return;
    }
    if (!this.adoptDirectory(stat)) {
      this.directoryTrusted = false;
      return;
    }
    this.directoryReady = true;
    let names: string[];
    try {
      names = fs.readdirSync(this.directory);
    } catch {
      return;
    }
    const nowMs = this.nowMs();
    for (const name of names) {
      const file = path.join(this.directory, name);
      if (TEMPORARY_FILE_PATTERN.test(name)) {
        try {
          const temporary = fs.lstatSync(file);
          if (temporary.isFile() && nowMs - temporary.mtimeMs > this.limits.temporaryOrphanMs) fs.unlinkSync(file);
        } catch {
          /* the next start-up looks again */
        }
        continue;
      }
      const match = SPOOL_FILE_PATTERN.exec(name);
      if (!match) continue;
      let fileStat: fs.Stats;
      try {
        fileStat = fs.lstatSync(file);
      } catch {
        continue;
      }
      if (!fileStat.isFile()) continue;
      this.index({
        stem: name.slice(0, -".json".length),
        receivedAtMs: Number(match[1]),
        pid: Number(match[2]),
        sequence: Number(match[3]),
        bytes: fileStat.size,
        events: null,
        metricSamples: null,
        failures: 0,
      });
    }
    this.pruneRejected();
  }

  private index(file: PendingFile) {
    this.pending.set(file.stem, file);
    this.pendingBytes += file.bytes;
  }

  private unindex(stem: string) {
    const file = this.pending.get(stem);
    if (!file) return;
    this.pending.delete(stem);
    this.pendingBytes = Math.max(0, this.pendingBytes - file.bytes);
  }

  private orderedPending() {
    return [...this.pending.values()].sort((left, right) =>
      left.receivedAtMs - right.receivedAtMs ||
      left.pid - right.pid ||
      left.sequence - right.sequence ||
      left.stem.localeCompare(right.stem),
    );
  }

  /**
   * A real directory owned by this uid, tightened to 0700. A symlink or a
   * directory someone else owns is never written to or replayed from.
   */
  private adoptDirectory(stat: fs.Stats) {
    if (stat.isSymbolicLink() || !stat.isDirectory()) return false;
    const uid = typeof process.getuid === "function" ? process.getuid() : undefined;
    if (uid !== undefined && stat.uid !== uid) return false;
    try {
      fs.chmodSync(this.directory, 0o700);
    } catch {
      return false;
    }
    return hookSpoolEntryTrusted(this.directory, "directory");
  }

  /** The directory is created on first use and must be a private directory of this uid. */
  private ensureDirectory() {
    if (this.directoryReady) return true;
    if (!this.directoryTrusted) return false;
    try {
      let existing: fs.Stats | null = null;
      try {
        existing = fs.lstatSync(this.directory);
      } catch {
        fs.mkdirSync(this.directory, { mode: 0o700 });
        existing = fs.lstatSync(this.directory);
      }
      if (!this.adoptDirectory(existing)) {
        this.directoryTrusted = false;
        return false;
      }
      // Make the new directory entry durable before any file is acknowledged in it.
      const descriptor = fs.openSync(this.home, "r");
      try {
        fs.fsyncSync(descriptor);
      } finally {
        fs.closeSync(descriptor);
      }
      this.directoryReady = true;
      return true;
    } catch {
      return false;
    }
  }

  status(): OtlpSpoolStatus {
    const nowMs = this.nowMs();
    let oldest: number | null = null;
    for (const file of this.pending.values()) {
      if (oldest === null || file.receivedAtMs < oldest) oldest = file.receivedAtMs;
    }
    const counters = this.counters;
    return {
      enabled: this.enabled,
      directoryTrusted: this.directoryTrusted,
      pendingFiles: this.pending.size,
      pendingBytes: this.pendingBytes,
      oldestPendingAgeSeconds: oldest === null ? null : Math.max(0, Math.floor((nowMs - oldest) / 1000)),
      spooled: {
        requests: counters.spooled,
        events: counters.spooledEvents,
        metricSamples: counters.spooledMetricSamples,
        lastAt: counters.lastSpooledAt,
      },
      replayed: {
        requests: counters.replayed,
        events: counters.replayedEvents,
        metricSamples: counters.replayedMetricSamples,
        deduplicated: counters.replayDeduplicated,
        collisions: counters.replayCollisions,
        enrollmentRejected: counters.replayEnrollmentRejected,
        lastAt: counters.lastReplayedAt,
      },
      droppedByCap: {
        fileCap: counters.refusedFileCap,
        byteCap: counters.refusedByteCap,
        ageCap: counters.expired,
        ageCapEvents: counters.expiredEvents,
      },
      writeFailed: counters.writeFailed,
      rejectedOnReplay: counters.rejected,
      deferredPasses: counters.deferredPasses,
      repoContextDropped: counters.repoContextDropped,
      limits: {
        maxFiles: this.limits.maxFiles,
        maxBytes: this.limits.maxBytes,
        maxAgeSeconds: Math.floor(this.limits.maxAgeMs / 1000),
      },
      counterLifetime: "durable",
    };
  }

  /**
   * Write one refused request's uncommitted batch, durably, or say why not.
   * The caller answers 2xx only after this resolved `ok`: the file and the
   * directory entry were flushed with F_FULLFSYNC before it did.
   */
  async write(input: OtlpSpoolWriteInput): Promise<OtlpSpoolWriteOutcome> {
    if (!this.enabled) return { ok: false, attempted: false, refused: "spool_disabled" };
    if (!this.directoryTrusted) return { ok: false, attempted: false, refused: "spool_untrusted" };
    const receivedAtMs = Math.max(0, Math.trunc(input.receivedAtMs));
    // Captured now: other requests advance the counter while this one awaits I/O.
    const sequence = ++this.sequence;
    const stem = `${String(receivedAtMs).padStart(13, "0")}-${process.pid}-${sequence}-${crypto.randomBytes(4).toString("hex")}`;
    let repoContextDropped = 0;
    for (const entry of input.batch.events) {
      if (peekRepoContextSidecar(entry.event)) repoContextDropped += 1;
    }
    let content: string;
    try {
      const batchText = JSON.stringify(input.batch);
      const head = JSON.stringify({
        v: 1,
        kind: "otlp_intake_spool",
        id: stem,
        source: input.source,
        transportPath: input.transportPath ?? null,
        receivedAt: new Date(receivedAtMs).toISOString(),
        cause: input.cause,
        committedChunks: input.committedChunks,
        repoContextDropped,
        digest: `sha256:${spoolSha256Hex(batchText)}`,
      });
      content = `${head.slice(0, -1)},"batch":${batchText}}`;
    } catch {
      return { ok: false, attempted: false, refused: "spool_encode_failed" };
    }
    const bytes = Buffer.byteLength(content);
    if (this.pending.size + this.inflightFiles + 1 > this.limits.maxFiles) {
      this.bump("refusedFileCap");
      return { ok: false, attempted: true, refused: "spool_bounds", bound: "files" };
    }
    if (this.pendingBytes + this.inflightBytes + bytes > this.limits.maxBytes) {
      this.bump("refusedByteCap");
      return { ok: false, attempted: true, refused: "spool_bounds", bound: "bytes" };
    }
    this.inflightFiles += 1;
    this.inflightBytes += bytes;
    const target = path.join(this.directory, `${stem}.json`);
    const temporary = `${target}.tmp`;
    let handle: fs.promises.FileHandle | undefined;
    let created = false;
    let published = false;
    try {
      if (!this.ensureDirectory()) throw new Error("otlp_spool_directory_unavailable");
      // Exclusive creation never truncates or follows an existing entry. The
      // I/O runs on the libuv pool, so a slow flush does not hold the loop.
      handle = await fs.promises.open(temporary, "wx", 0o600);
      created = true;
      await handle.writeFile(content);
      await handle.chmod(0o600);
      await handle.sync();
      await handle.close();
      handle = undefined;
      await fs.promises.rename(temporary, target);
      published = true;
      await syncPath(this.directory);
    } catch {
      if (handle) await handle.close().catch(() => undefined);
      // Not acknowledged, so not kept: hide a published file again, then drop it.
      if (published) await fs.promises.rename(target, temporary).catch(() => undefined);
      if (created) await fs.promises.unlink(temporary).catch(() => undefined);
      this.bump("writeFailed");
      return { ok: false, attempted: true, refused: "spool_write_failed" };
    } finally {
      this.inflightFiles -= 1;
      this.inflightBytes -= bytes;
    }
    this.index({
      stem,
      receivedAtMs,
      pid: process.pid,
      sequence,
      bytes,
      events: input.batch.events.length,
      metricSamples: input.batch.metricSamples.length,
      failures: 0,
    });
    this.bump("spooled");
    this.bump("spooledEvents", input.batch.events.length);
    this.bump("spooledMetricSamples", input.batch.metricSamples.length);
    this.bump("repoContextDropped", repoContextDropped);
    this.counters.lastSpooledAt = new Date(this.nowMs()).toISOString();
    this.hooks.afterDurableWrite?.(stem);
    return {
      ok: true,
      id: stem,
      events: input.batch.events.length,
      metricSamples: input.batch.metricSamples.length,
    };
  }

  /** Quarantine a file that must never be replayed, naming why. */
  private async reject(file: PendingFile, reason: string) {
    const directory = path.join(this.directory, OTLP_SPOOL_REJECTED_DIRECTORY);
    const source = path.join(this.directory, `${file.stem}.json`);
    const safeReason = /^[a-z0-9_]+$/.test(reason) ? reason : "spool_invalid";
    try {
      fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
      fs.chmodSync(directory, 0o700);
      await fs.promises.rename(source, path.join(directory, `${file.stem}.${safeReason}.json`));
    } catch {
      // An unmovable file must not block every later one behind it.
      await fs.promises.unlink(source).catch(() => undefined);
    }
    this.unindex(file.stem);
    this.bump("rejected");
    this.warn({ warning: "otlp_spool_file_rejected", reason: safeReason });
  }

  /** Bound the quarantine by count and age, oldest first. */
  private pruneRejected() {
    const directory = path.join(this.directory, OTLP_SPOOL_REJECTED_DIRECTORY);
    let names: string[];
    try {
      names = fs.readdirSync(directory);
    } catch {
      return;
    }
    const nowMs = this.nowMs();
    const entries = names
      .map((name) => ({ name, match: REJECTED_FILE_PATTERN.exec(name) }))
      .filter((entry): entry is { name: string; match: RegExpExecArray } => entry.match !== null)
      .map((entry) => ({ name: entry.name, atMs: Number(entry.match[1]) }))
      .sort((left, right) => left.atMs - right.atMs || left.name.localeCompare(right.name));
    const overflow = Math.max(0, entries.length - this.limits.maxRejectedFiles);
    entries.forEach((entry, position) => {
      if (position >= overflow && nowMs - entry.atMs <= this.limits.rejectedMaxAgeMs) return;
      try {
        fs.unlinkSync(path.join(directory, entry.name));
      } catch {
        /* next pass */
      }
    });
  }

  /** Delete acknowledged files past the age limit; runs even when the ledger is busy. */
  private async expireOld(result: OtlpSpoolDrainPass) {
    const cutoff = this.nowMs() - this.limits.maxAgeMs;
    for (const file of this.orderedPending()) {
      if (file.receivedAtMs >= cutoff) break;
      try {
        await fs.promises.unlink(path.join(this.directory, `${file.stem}.json`));
      } catch (error) {
        if (errorCode(error) !== "ENOENT") continue;
      }
      this.unindex(file.stem);
      this.bump("expired");
      if (file.events !== null) this.bump("expiredEvents", file.events);
      result.expired += 1;
    }
    if (result.expired > 0) {
      await syncPath(this.directory).catch(() => undefined);
      this.warn({ warning: "otlp_spool_expired", files: result.expired, maxAgeSeconds: Math.floor(this.limits.maxAgeMs / 1000) });
    }
  }

  /**
   * Replay one file from its durable cursor. Each chunk is one immediate
   * transaction holding the rows and the advanced cursor, so a crash anywhere
   * leaves either the old cursor with no rows of that chunk, or both.
   */
  private async replayFile(
    buffer: LocalEventBuffer,
    file: PendingFile,
    passDeadline: number,
  ): Promise<
    | {
        kind: "complete";
        events: number;
        metricSamples: number;
        chunks: number;
        deduplicated: number;
        collisions: number;
        enrollmentRejected: number;
      }
    | { kind: "deferred"; chunks: number }
    | { kind: "budget"; chunks: number }
    | { kind: "rejected" }
    | { kind: "vanished" }
    | { kind: "failed"; code: string; chunks: number }
  > {
    const target = path.join(this.directory, `${file.stem}.json`);
    try {
      fs.lstatSync(target);
    } catch (error) {
      if (errorCode(error) === "ENOENT") {
        // Removed outside the drain: nothing is left to replay.
        this.unindex(file.stem);
        this.warn({ warning: "otlp_spool_file_vanished" });
        return { kind: "vanished" };
      }
    }
    const read = await readEnvelope(target, file.stem);
    if (!read.ok) {
      if (read.reason === "spool_unreadable") return { kind: "deferred", chunks: 0 };
      await this.reject(file, read.reason);
      return { kind: "rejected" };
    }
    const { envelope } = read;
    file.events = envelope.batch.events.length;
    file.metricSamples = envelope.batch.metricSamples.length;
    const db = buffer.database;
    const key = `${CURSOR_KEY_PREFIX}${envelope.id}`;
    const chunks = otlpChunkCount(envelope.batch);
    let cursor: ReturnType<typeof readCursor>;
    try {
      cursor = readCursor(db, key);
    } catch (error) {
      if (isSqliteContentionError(error)) return { kind: "deferred", chunks: 0 };
      return { kind: "failed", code: errorCode(error), chunks: 0 };
    }
    // A cursor for different bytes under the same name cannot be ours.
    let next = cursor && cursor.digest === envelope.digest && cursor.chunks === chunks
      ? Math.min(cursor.nextChunk, chunks)
      : 0;
    let committed = 0;
    let deduplicated = 0;
    let collisions = 0;
    let enrollmentRejected = 0;
    // The live route's projection allowance for one request.
    const projectionDeadlineMs = performance.now() + 25;
    while (next < chunks) {
      if (this.stopped || performance.now() >= passDeadline) return { kind: "budget", chunks: committed };
      const part = otlpChunk(envelope.batch, next);
      const advanced = { nextChunk: next + 1, chunks, digest: envelope.digest };
      try {
        const appended = buffer.transactionWithRepoContextHandoffs(() => {
          const result = buffer.appendMany(part.events, part.metricSamples, part.admissionDrops, {
            projectionDeadlineMs,
          });
          writeCursor(db, key, advanced);
          return result;
        });
        deduplicated += appended.deduplicatedCount;
        collisions += appended.collisionQuarantinedCount;
        enrollmentRejected += appended.enrollmentRejectedEventCount + appended.enrollmentRejectedMetricCount;
      } catch (error) {
        if (isSqliteContentionError(error) || isTransientStorageError(errorCode(error))) {
          return { kind: "deferred", chunks: committed };
        }
        return { kind: "failed", code: errorCode(error), chunks: committed };
      }
      committed += 1;
      next += 1;
      this.hooks.afterChunkCommitted?.(envelope.id, next - 1, chunks);
      // Yield between writer turns, exactly as the live route does.
      if (next < chunks) await new Promise<void>((resolve) => setImmediate(resolve));
    }
    return {
      kind: "complete",
      events: envelope.batch.events.length,
      metricSamples: envelope.batch.metricSamples.length,
      chunks: committed,
      deduplicated,
      collisions,
      enrollmentRejected,
    };
  }

  /** Delete cursor rows whose file is gone; bounded, and skipped when busy. */
  private collectStaleCursors(buffer: LocalEventBuffer) {
    try {
      const db = buffer.database;
      const rows = db.prepare(
        `select key from maintenance_state where key > ? and key < ? order by key limit 128`,
      ).all(CURSOR_KEY_PREFIX, `${CURSOR_KEY_PREFIX}~`) as Array<{ key: string }>;
      const stale = rows
        .map((row) => row.key)
        .filter((key) => !this.pending.has(key.slice(CURSOR_KEY_PREFIX.length)));
      if (stale.length === 0) return;
      const remove = db.prepare(`delete from maintenance_state where key = ?`);
      db.transaction(() => {
        for (const key of stale) remove.run(key);
      }).immediate();
    } catch {
      /* a busy ledger keeps them until the next housekeeping tick */
    }
  }

  /**
   * One bounded drain pass: expire, then replay oldest first until the ledger
   * is busy, the pass has used its wall-clock allowance, or the file ceiling.
   * Completed files are removed only after one ledger flush covering them all.
   */
  async drain(buffer: LocalEventBuffer): Promise<OtlpSpoolDrainPass> {
    const result: OtlpSpoolDrainPass = {
      replayed: 0,
      replayedEvents: 0,
      replayedMetricSamples: 0,
      chunks: 0,
      rejected: 0,
      expired: 0,
      deferred: false,
      budgetExhausted: false,
      failed: 0,
    };
    if (!this.enabled || this.draining || this.stopped) return result;
    this.draining = true;
    try {
      this.ticks += 1;
      await this.expireOld(result);
      const passDeadline = performance.now() + this.limits.maxPassMs;
      const completed: PendingFile[] = [];
      let attempted = 0;
      for (const file of this.orderedPending()) {
        if (this.stopped) break;
        if (attempted >= this.limits.maxFilesPerPass || performance.now() >= passDeadline) {
          result.budgetExhausted = true;
          break;
        }
        attempted += 1;
        const outcome = await this.replayFile(buffer, file, passDeadline);
        if (outcome.kind === "vanished") continue;
        if (outcome.kind === "rejected") {
          result.rejected += 1;
          continue;
        }
        result.chunks += outcome.chunks;
        if (outcome.kind === "complete") {
          completed.push(file);
          result.replayedEvents += outcome.events;
          result.replayedMetricSamples += outcome.metricSamples;
          this.bump("replayDeduplicated", outcome.deduplicated);
          this.bump("replayCollisions", outcome.collisions);
          this.bump("replayEnrollmentRejected", outcome.enrollmentRejected);
          continue;
        }
        if (outcome.kind === "deferred") {
          result.deferred = true;
          this.bump("deferredPasses");
          break;
        }
        if (outcome.kind === "budget") {
          result.budgetExhausted = true;
          break;
        }
        // Not busy and not a verdict on the file yet: retry it next pass, and
        // quarantine it only when it keeps failing, so one poison file cannot
        // hold every later one behind it.
        file.failures += 1;
        result.failed += 1;
        this.warn({ warning: "otlp_spool_replay_failed", code: outcome.code, failures: file.failures });
        if (file.failures >= this.limits.replayFailureLimit) {
          await this.reject(file, "replay_failed");
          result.rejected += 1;
          continue;
        }
        break;
      }
      if (completed.length > 0) await this.remove(buffer, completed, result);
      if (this.ticks % this.limits.housekeepingEveryTicks === 0) {
        this.collectStaleCursors(buffer);
        this.pruneRejected();
      }
      this.logDrain(result);
      return result;
    } finally {
      this.persistCounters();
      this.draining = false;
    }
  }

  /**
   * A long drain runs a pass every few seconds, so its log is aggregated: the
   * first replay after a quiet spool is printed at once, later passes are
   * summed into one line per minute, and the line that empties the spool is
   * always printed. Rejections and expiries are losses and print every time.
   */
  private logDrain(result: OtlpSpoolDrainPass) {
    const window = this.drainLog;
    window.replayed += result.replayed;
    window.events += result.replayedEvents;
    window.metricSamples += result.replayedMetricSamples;
    window.rejected += result.rejected;
    window.expired += result.expired;
    if (window.replayed === 0 && window.rejected === 0 && window.expired === 0) return;
    const nowMs = this.nowMs();
    const emptied = result.replayed > 0 && this.pending.size === 0;
    const first = window.lastAtMs === null;
    if (!first && !emptied && result.rejected === 0 && result.expired === 0 &&
        nowMs - window.lastAtMs! < 60_000) return;
    console.log(JSON.stringify({
      status: "otlp_spool_drain",
      replayed: window.replayed,
      events: window.events,
      metricSamples: window.metricSamples,
      rejected: window.rejected,
      expired: window.expired,
      pendingFiles: this.pending.size,
      pendingBytes: this.pendingBytes,
    }));
    this.drainLog = {
      lastAtMs: emptied ? null : nowMs,
      replayed: 0,
      events: 0,
      metricSamples: 0,
      rejected: 0,
      expired: 0,
    };
  }

  private async remove(buffer: LocalEventBuffer, completed: PendingFile[], result: OtlpSpoolDrainPass) {
    try {
      await syncLedger(buffer.database);
    } catch (error) {
      // Kept on disk with a complete cursor: the next pass re-flushes and
      // removes them without committing anything again.
      this.warn({ warning: "otlp_spool_ledger_flush_failed", code: errorCode(error) });
      return;
    }
    this.hooks.beforeRemove?.(completed.map((file) => file.stem));
    const removed: PendingFile[] = [];
    for (const file of completed) {
      try {
        await fs.promises.unlink(path.join(this.directory, `${file.stem}.json`));
      } catch (error) {
        if (errorCode(error) !== "ENOENT") continue;
      }
      removed.push(file);
      this.unindex(file.stem);
    }
    await syncPath(this.directory).catch(() => undefined);
    try {
      const remove = buffer.database.prepare(`delete from maintenance_state where key = ?`);
      buffer.database.transaction(() => {
        for (const file of removed) remove.run(`${CURSOR_KEY_PREFIX}${file.stem}`);
      }).immediate();
    } catch {
      /* stale cursor rows are collected by housekeeping */
    }
    result.replayed += removed.length;
    this.bump("replayed", removed.length);
    this.bump("replayedEvents", removed.reduce((total, file) => total + (file.events ?? 0), 0));
    this.bump("replayedMetricSamples", removed.reduce((total, file) => total + (file.metricSamples ?? 0), 0));
    if (removed.length > 0) this.counters.lastReplayedAt = new Date(this.nowMs()).toISOString();
  }

  /** Arm the drain beside the daemon's other cadences. No timer when disabled. */
  startDrain(buffer: LocalEventBuffer, intervalMs = this.limits.drainIntervalMs) {
    if (!this.enabled || this.timer) return;
    this.stopped = false;
    this.timer = setInterval(() => {
      void this.drain(buffer).catch((error) => {
        this.warn({
          warning: "otlp_spool_drain_failed",
          message: error instanceof Error ? error.message : String(error),
        });
      });
    }, intervalMs);
    this.timer.unref();
  }

  /** Stop the timer; a pass in flight stops at its next chunk boundary. */
  stopDrain() {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    this.persistCounters();
  }
}
