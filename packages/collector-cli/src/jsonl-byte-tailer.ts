import crypto from "node:crypto";
import fs from "node:fs";

import type Database from "better-sqlite3";

const PROBE_BYTES = 512;
const DEFAULT_MAX_READ_BYTES = 1024 * 1024;
const DEFAULT_MAX_RECORDS = 1_000;
const MIN_MAX_READ_BYTES = PROBE_BYTES * 2 + 1;
const ABSOLUTE_MAX_READ_BYTES = 16 * 1024 * 1024;
const ABSOLUTE_MAX_RECORDS = 10_000;
const STATE_TABLE = "rollout_scan_state";
const SHA256_RE = /^[0-9a-f]{64}$/;

export type CheckpointStatus = "valid" | "legacy" | "invalid";
export type ParserStateValidator<T> = (value: unknown) => T | undefined;

export type JsonlScanCursor<T> = {
  observedSize: number;
  committedOffset: number | null;
  deferredBytes: number;
  fileIdentity: string | null;
  headHash: string | null;
  headBytes: number;
  continuityHash: string | null;
  continuityBytes: number;
  mtimeMs: number | null;
  ctimeMs: number | null;
  workRemaining: boolean;
  unresolvedRecord: JsonlUnresolvedRecord | null;
  parserState: T | undefined;
  checkpointStatus: CheckpointStatus;
};

export type JsonlUnresolvedRecord = {
  reason: "record_exceeds_byte_budget" | "generation_rewrite_ambiguous";
  offset: number;
  observedBytes: number;
  availableBytes: number;
  byteBudget: number;
};

export type JsonlTailReadLimits = {
  /** Total synchronous file-read budget, including bounded integrity probes. */
  maxBytes?: number;
  /** Maximum number of complete JSONL records returned in one slice. */
  maxRecords?: number;
};

export type JsonlTailRead = {
  lines: string[];
  observedSize: number;
  committedOffset: number;
  deferredBytes: number;
  fileIdentity: string;
  headHash: string | null;
  headBytes: number;
  continuityHash: string | null;
  continuityBytes: number;
  mtimeMs: number;
  ctimeMs: number;
  bytesRead: number;
  workRemaining: boolean;
  unresolvedRecord: JsonlUnresolvedRecord | null;
  reset: boolean;
  legacyRebuild: boolean;
  checkpointRebuild: boolean;
  /**
   * Revalidate the still-open generation immediately before the caller opens
   * its database write transaction. This performs user-path filesystem calls,
   * so it must never execute while SQLite holds the write lock.
   */
  assertStableForCommit(): void;
  /** Idempotent. Call from a finally block after commit or rollback. */
  close(): void;
};

/** Narrow filesystem seam used by tailers so failure paths can be proved
 * without replacing process-wide `fs` methods. The default delegates at call
 * time, preserving filesystem instrumentation used by the resource proofs.
 */
export type JsonlTailerIo = {
  readDirents(directory: string): fs.Dirent[];
  readNames(directory: string): string[];
  stat(file: string): fs.Stats;
  lstat(file: string): fs.Stats;
  readTail: typeof readJsonlTail;
};

export const DEFAULT_JSONL_TAILER_IO: JsonlTailerIo = {
  readDirents: (directory) => fs.readdirSync(directory, { withFileTypes: true }),
  readNames: (directory) => fs.readdirSync(directory),
  stat: (file) => fs.statSync(file),
  lstat: (file) => fs.lstatSync(file),
  readTail: readJsonlTail,
};

type RawCursorRow = {
  size: number;
  committedOffset: number | null;
  deferredBytes: number | null;
  fileIdentity: string | null;
  headHash: string | null;
  headBytes: number | null;
  continuityHash: string | null;
  continuityBytes: number | null;
  mtimeMs: number | null;
  ctimeMs: number | null;
  workRemaining: number | null;
  unresolvedKind: string | null;
  unresolvedOffset: number | null;
  unresolvedObservedBytes: number | null;
  unresolvedAvailableBytes: number | null;
  unresolvedByteBudget: number | null;
  parserKind: string | null;
  checkpointVersion: number | null;
  parserStateJson: string | null;
};

export class JsonlSnapshotChangedError extends Error {
  constructor() {
    super("JSONL file generation changed before cursor commit");
    this.name = "JsonlSnapshotChangedError";
  }
}

/** Opaque at-rest key. Source paths are never stored in rollout_scan_state. */
export function jsonlScanStateKey(file: string) {
  return crypto.createHash("sha256").update(file).digest("hex");
}

/**
 * Keep the historical table name so existing collector ledgers migrate in
 * place. Legacy rows contain only `size`; they stay cheap while unchanged and
 * rebuild once, deterministically, the next time their file grows.
 *
 * Parser changes use BOTH `parser_kind` and `checkpoint_version`. A state
 * shape change bumps the caller's version; incompatible checkpoints rebuild
 * from byte zero instead of being cast into a newer runtime type.
 */
export function ensureJsonlScanState(database: Database.Database) {
  database.exec(
    `create table if not exists ${STATE_TABLE} (
      file text primary key,
      size integer not null,
      scanned_at text not null
    )`,
  );
  const existing = new Set(
    (database.pragma(`table_info(${STATE_TABLE})`) as Array<{ name: string }>).map(
      (column) => column.name,
    ),
  );
  const additions = [
    "committed_offset integer",
    "deferred_bytes integer not null default 0",
    "file_identity text",
    "head_hash text",
    "head_bytes integer not null default 0",
    "continuity_hash text",
    "continuity_bytes integer not null default 0",
    "mtime_ms real",
    "ctime_ms real",
    "work_remaining integer not null default 0",
    "unresolved_kind text",
    "unresolved_offset integer",
    "unresolved_observed_bytes integer",
    "unresolved_available_bytes integer",
    "unresolved_byte_budget integer",
    "parser_kind text",
    "checkpoint_version integer",
    "parser_state_json text",
  ];
  for (const definition of additions) {
    const name = definition.split(" ")[0];
    if (!existing.has(name)) {
      try {
        database.exec(`alter table ${STATE_TABLE} add column ${definition}`);
      } catch (error) {
        // Two collector constructors (or a short-lived overlap during an
        // upgrade) can observe the same missing column. The second ALTER is
        // harmless; any other migration failure remains fatal.
        if (!(error instanceof Error) || !error.message.includes("duplicate column name")) {
          throw error;
        }
      }
    }
  }

  // Version-1 rows used the source pathname as their primary key. Complete
  // this one-time preflight before the listener is eligible to become active.
  // Each transaction materializes at most 257 keys, so migration memory and
  // lock spans stay bounded even though every finite legacy ledger is scrubbed
  // in one process lifetime (rather than inducing a KeepAlive restart loop).
  // Checkpointing before and after keeps plaintext out of both free cells and
  // the WAL without a full VACUUM/rewrite of a potentially multi-GB ledger.
  database.pragma("secure_delete = ON");
  database.pragma("wal_checkpoint(TRUNCATE)");
  const migrationLimit = 256;
  const migratePathKeys = database.transaction(() => {
    const rows = database
      .prepare(
        `select file, scanned_at as scannedAt from ${STATE_TABLE}
         where length(file) != 64 or file glob '*[^0-9a-f]*'
         order by file limit ?`,
      )
      .all(migrationLimit + 1) as Array<{ file: string; scannedAt: string }>;
    const findExisting = database.prepare(
      `select scanned_at as scannedAt from ${STATE_TABLE} where file = ?`,
    );
    const rename = database.prepare(`update ${STATE_TABLE} set file = ? where file = ?`);
    const remove = database.prepare(`delete from ${STATE_TABLE} where file = ?`);
    for (const row of rows.slice(0, migrationLimit)) {
      const key = jsonlScanStateKey(row.file);
      const hashed = findExisting.get(key) as { scannedAt: string } | undefined;
      if (!hashed) {
        rename.run(key, row.file);
        continue;
      }
      if (newerScan(row.scannedAt, hashed.scannedAt)) {
        remove.run(key);
        rename.run(key, row.file);
      } else {
        remove.run(row.file);
      }
    }
    return rows.length > migrationLimit;
  });
  while (migratePathKeys()) {
    // Continue with another bounded transaction. This is startup preflight,
    // never an automatic capture cadence or background polling loop.
  }
  database.pragma("wal_checkpoint(TRUNCATE)");
}

export function loadJsonlScanCursor<T>(
  database: Database.Database,
  file: string,
  parserKind: string,
  checkpointVersion: number,
  validateParserState: ParserStateValidator<T>,
): JsonlScanCursor<T> | undefined {
  const row = database
    .prepare(
      `select size,
         committed_offset as committedOffset,
         deferred_bytes as deferredBytes,
         file_identity as fileIdentity,
         head_hash as headHash,
         head_bytes as headBytes,
         continuity_hash as continuityHash,
         continuity_bytes as continuityBytes,
         mtime_ms as mtimeMs,
         ctime_ms as ctimeMs,
         work_remaining as workRemaining,
         unresolved_kind as unresolvedKind,
         unresolved_offset as unresolvedOffset,
         unresolved_observed_bytes as unresolvedObservedBytes,
         unresolved_available_bytes as unresolvedAvailableBytes,
         unresolved_byte_budget as unresolvedByteBudget,
         parser_kind as parserKind,
         checkpoint_version as checkpointVersion,
         parser_state_json as parserStateJson
       from ${STATE_TABLE} where file = ?`,
    )
    .get(jsonlScanStateKey(file)) as RawCursorRow | undefined;
  if (!row) return undefined;

  const legacy =
    row.committedOffset === null &&
    row.parserKind === null &&
    row.checkpointVersion === null &&
    row.parserStateJson === null;
  if (legacy) {
    return {
      observedSize: nonnegativeInteger(row.size) ? row.size : 0,
      committedOffset: null,
      deferredBytes: 0,
      fileIdentity: null,
      headHash: null,
      headBytes: 0,
      continuityHash: null,
      continuityBytes: 0,
      mtimeMs: null,
      ctimeMs: null,
      workRemaining: false,
      unresolvedRecord: null,
      parserState: undefined,
      checkpointStatus: "legacy",
    };
  }

  let decoded: unknown;
  try {
    decoded = row.parserStateJson === null ? undefined : JSON.parse(row.parserStateJson);
  } catch {
    decoded = undefined;
  }
  const parserState = validateParserState(decoded);
  const valid =
    row.parserKind === parserKind &&
    row.checkpointVersion === checkpointVersion &&
    parserState !== undefined &&
    validCursorEnvelope(row);

  if (!valid) {
    return {
      observedSize: nonnegativeInteger(row.size) ? row.size : 0,
      committedOffset: null,
      deferredBytes: 0,
      fileIdentity: row.fileIdentity,
      headHash: null,
      headBytes: 0,
      continuityHash: null,
      continuityBytes: 0,
      mtimeMs: row.mtimeMs,
      ctimeMs: row.ctimeMs,
      workRemaining: false,
      unresolvedRecord: null,
      parserState: undefined,
      checkpointStatus: "invalid",
    };
  }

  return {
    observedSize: row.size,
    committedOffset: row.committedOffset,
    deferredBytes: row.deferredBytes!,
    fileIdentity: row.fileIdentity,
    headHash: row.headHash,
    headBytes: row.headBytes!,
    continuityHash: row.continuityHash,
    continuityBytes: row.continuityBytes!,
    mtimeMs: row.mtimeMs,
    ctimeMs: row.ctimeMs,
    workRemaining: row.workRemaining === 1,
    unresolvedRecord: decodeUnresolvedRecord(row),
    parserState,
    checkpointStatus: "valid",
  };
}

export function rememberJsonlScanCursor<T>(
  database: Database.Database,
  file: string,
  parserKind: string,
  checkpointVersion: number,
  read: JsonlTailRead,
  parserState: T,
) {
  database
    .prepare(
      `insert into ${STATE_TABLE}
         (file, size, scanned_at, committed_offset, deferred_bytes, file_identity,
         head_hash, head_bytes, continuity_hash, continuity_bytes, mtime_ms,
          ctime_ms,
          work_remaining, unresolved_kind, unresolved_offset,
          unresolved_observed_bytes, unresolved_available_bytes,
          unresolved_byte_budget, parser_kind, checkpoint_version,
          parser_state_json)
       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       on conflict(file) do update set
         size = excluded.size,
         scanned_at = excluded.scanned_at,
         committed_offset = excluded.committed_offset,
         deferred_bytes = excluded.deferred_bytes,
         file_identity = excluded.file_identity,
         head_hash = excluded.head_hash,
         head_bytes = excluded.head_bytes,
         continuity_hash = excluded.continuity_hash,
         continuity_bytes = excluded.continuity_bytes,
         mtime_ms = excluded.mtime_ms,
         ctime_ms = excluded.ctime_ms,
         work_remaining = excluded.work_remaining,
         unresolved_kind = excluded.unresolved_kind,
         unresolved_offset = excluded.unresolved_offset,
         unresolved_observed_bytes = excluded.unresolved_observed_bytes,
         unresolved_available_bytes = excluded.unresolved_available_bytes,
         unresolved_byte_budget = excluded.unresolved_byte_budget,
         parser_kind = excluded.parser_kind,
         checkpoint_version = excluded.checkpoint_version,
         parser_state_json = excluded.parser_state_json`,
    )
    .run(
      jsonlScanStateKey(file),
      read.observedSize,
      new Date().toISOString(),
      read.committedOffset,
      read.deferredBytes,
      read.fileIdentity,
      read.headHash,
      read.headBytes,
      read.continuityHash,
      read.continuityBytes,
      read.mtimeMs,
      read.ctimeMs,
      read.workRemaining ? 1 : 0,
      read.unresolvedRecord?.reason ?? null,
      read.unresolvedRecord?.offset ?? null,
      read.unresolvedRecord?.observedBytes ?? null,
      read.unresolvedRecord?.availableBytes ?? null,
      read.unresolvedRecord?.byteBudget ?? null,
      parserKind,
      checkpointVersion,
      JSON.stringify(parserState),
    );
}

/**
 * Read complete JSONL records after the last committed newline. Partial bytes
 * remain only in the source file: no content carry is copied into SQLite.
 * After validating the open generation outside SQLite, the caller advances
 * parsed events and checkpoint state in one database transaction, so a crash
 * replays deterministic ids safely without holding a write lock over path IO.
 */
export function readJsonlTail(
  file: string,
  stat: fs.Stats,
  cursor: JsonlScanCursor<unknown> | undefined,
  limits: JsonlTailReadLimits = {},
): JsonlTailRead | undefined {
  const maxBytes = boundedLimit(
    limits.maxBytes,
    DEFAULT_MAX_READ_BYTES,
    MIN_MAX_READ_BYTES,
    ABSOLUTE_MAX_READ_BYTES,
    "maxBytes",
  );
  const maxRecords = boundedLimit(
    limits.maxRecords,
    DEFAULT_MAX_RECORDS,
    1,
    ABSOLUTE_MAX_RECORDS,
    "maxRecords",
  );
  let pathMetadata: fs.BigIntStats;
  try {
    pathMetadata = fs.lstatSync(file, { bigint: true });
  } catch {
    throw new JsonlSnapshotChangedError();
  }
  if (
    pathMetadata.isSymbolicLink() ||
    !pathMetadata.isFile() ||
    !normalStatMatchesPrecise(stat, pathMetadata)
  ) {
    throw new JsonlSnapshotChangedError();
  }
  const snapshot = generationSnapshot(pathMetadata);
  const observedSize = safeStatNumber(snapshot.size);
  const observedMtimeMs = nanosecondsToMilliseconds(snapshot.mtimeNs);
  const observedCtimeMs = nanosecondsToMilliseconds(snapshot.ctimeNs);
  const identity = fileIdentity(snapshot);
  const unchangedIdentity = !cursor?.fileIdentity || cursor.fileIdentity === identity;
  const largerBudgetCanRetryUnresolved =
    cursor?.unresolvedRecord?.reason === "record_exceeds_byte_budget" &&
    maxBytes > cursor.unresolvedRecord.byteBudget;
  if (
    cursor?.unresolvedRecord?.reason === "generation_rewrite_ambiguous" &&
    unchangedIdentity
  ) {
    return undefined;
  }
  if (
    cursor &&
    cursor.checkpointStatus !== "invalid" &&
    observedSize === cursor.observedSize &&
    unchangedIdentity &&
    (cursor.mtimeMs === null || cursor.mtimeMs === observedMtimeMs) &&
    (cursor.checkpointStatus === "legacy" ||
      (cursor.ctimeMs !== null && cursor.ctimeMs === observedCtimeMs)) &&
    !cursor.workRemaining &&
    !largerBudgetCanRetryUnresolved
  ) {
    return undefined;
  }

  let legacyRebuild = false;
  let checkpointRebuild = false;
  let reset = false;
  let rewriteAmbiguous = false;
  let start = cursor?.committedOffset ?? 0;
  if (cursor) {
    legacyRebuild = cursor.checkpointStatus === "legacy";
    checkpointRebuild = cursor.checkpointStatus === "invalid";
    rewriteAmbiguous =
      cursor.checkpointStatus === "valid" &&
      unchangedIdentity &&
      (observedSize < cursor.observedSize ||
        observedSize < start ||
        (observedSize === cursor.observedSize &&
          (cursor.mtimeMs !== observedMtimeMs || cursor.ctimeMs !== observedCtimeMs)));
    reset = legacyRebuild || checkpointRebuild || !unchangedIdentity;
    if (reset) start = 0;
  }

  const fd = fs.openSync(file, fs.constants.O_RDONLY);
  let closed = false;
  let bytesRead = 0;
  let priorContinuity = Buffer.alloc(0);
  let headProbe = Buffer.alloc(0);
  try {
    const opened = fs.fstatSync(fd);
    const openedPrecise = fs.fstatSync(fd, { bigint: true });
    if (!opened.isFile() || !sameGenerationSnapshot(openedPrecise, snapshot)) {
      throw new JsonlSnapshotChangedError();
    }
    const openedIdentity = fileIdentity(snapshot);
    const readBudgeted = (length: number, position: number) => {
      if (length <= 0) return Buffer.alloc(0);
      const remaining = maxBytes - bytesRead;
      if (remaining < length) {
        throw new RangeError("maxBytes is too small for required JSONL integrity probes");
      }
      const bytes = readAt(fd, length, position);
      bytesRead += bytes.length;
      return bytes;
    };

    // The head probe catches ordinary replacement. The independent continuity
    // probe immediately before committedOffset catches same-inode
    // truncate-and-regrow where the first bytes were deliberately preserved.
    if (!reset && !rewriteAmbiguous && cursor?.headHash && cursor.headBytes > 0) {
      headProbe = readBudgeted(cursor.headBytes, 0);
      if (
        headProbe.length !== cursor.headBytes ||
        hashBytes(headProbe) !== cursor.headHash
      ) {
        rewriteAmbiguous = true;
      }
    }
    if (!reset && !rewriteAmbiguous && cursor?.continuityHash && cursor.continuityBytes > 0) {
      const continuityPosition = Math.max(0, start - cursor.continuityBytes);
      priorContinuity =
        continuityPosition + cursor.continuityBytes <= headProbe.length
          ? headProbe.subarray(continuityPosition, continuityPosition + cursor.continuityBytes)
          : readBudgeted(cursor.continuityBytes, continuityPosition);
      if (
        priorContinuity.length !== cursor.continuityBytes ||
        hashBytes(priorContinuity) !== cursor.continuityHash
      ) {
        rewriteAmbiguous = true;
      }
    }

    if (rewriteAmbiguous) {
      const close = () => {
        if (closed) return;
        closed = true;
        fs.closeSync(fd);
      };
      const assertStableForCommit = () => {
        if (closed) throw new JsonlSnapshotChangedError();
        let currentFd: fs.BigIntStats;
        let currentPath: fs.BigIntStats;
        try {
          currentFd = fs.fstatSync(fd, { bigint: true });
          currentPath = fs.lstatSync(file, { bigint: true });
        } catch {
          throw new JsonlSnapshotChangedError();
        }
        if (
          currentPath.isSymbolicLink() ||
          !currentPath.isFile() ||
          !sameGenerationSnapshot(currentFd, snapshot) ||
          !sameGenerationSnapshot(currentPath, snapshot)
        ) {
          throw new JsonlSnapshotChangedError();
        }
      };
      return {
        lines: [],
        observedSize,
        committedOffset: 0,
        deferredBytes: observedSize,
        fileIdentity: openedIdentity,
        headHash: null,
        headBytes: 0,
        continuityHash: null,
        continuityBytes: 0,
        mtimeMs: observedMtimeMs,
        ctimeMs: observedCtimeMs,
        bytesRead,
        workRemaining: false,
        unresolvedRecord: {
          reason: "generation_rewrite_ambiguous",
          offset: 0,
          observedBytes: 0,
          availableBytes: observedSize,
          byteBudget: maxBytes,
        },
        reset: false,
        legacyRebuild,
        checkpointRebuild,
        assertStableForCommit,
        close,
      };
    }

    const available = Math.max(0, observedSize - start);
    const contentBudget = Math.max(0, maxBytes - bytesRead);
    const bytes = readAt(fd, Math.min(available, contentBudget), start);
    bytesRead += bytes.length;

    const slice = truncateJsonlReadToCompleteRecords(bytes, maxRecords);
    const committedOffset = start + slice.committedBytes;
    const completeBytes =
      slice.committedBytes > 0
        ? bytes.subarray(0, slice.committedBytes - 1)
        : Buffer.alloc(0);
    const lines =
      completeBytes.length === 0
        ? []
        : completeBytes
            .toString("utf8")
            .split("\n")
            .map((line) => (line.endsWith("\r") ? line.slice(0, -1) : line));

    const byteLimited = bytes.length < available;
    const unresolvedRecord =
      slice.committedBytes === 0 && byteLimited
        ? {
            reason: "record_exceeds_byte_budget" as const,
            offset: start,
            observedBytes: bytes.length,
            availableBytes: available,
            byteBudget: maxBytes,
          }
        : null;
    // Do not spin forever on an unchanged oversized/no-newline record. Its
    // metadata remains durable and a later append or a larger caller budget
    // can retry it. Ordinary byte/record slicing, however, must resume even
    // when the path stat is unchanged.
    const workRemaining =
      slice.moreCompleteRecords || (byteLimited && slice.committedBytes > 0);

    let headBytes = cursor?.headBytes ?? 0;
    let headHash = cursor?.headHash ?? null;
    if (reset || !cursor || !headHash) {
      headBytes = Math.min(PROBE_BYTES, observedSize, Math.max(bytes.length, headProbe.length));
      const head =
        headProbe.length >= headBytes
          ? headProbe.subarray(0, headBytes)
          : start === 0 && bytes.length >= headBytes
            ? bytes.subarray(0, headBytes)
            : readBudgeted(headBytes, 0);
      headHash = headBytes > 0 ? hashBytes(head) : null;
    }

    const continuityBytes = Math.min(PROBE_BYTES, committedOffset);
    let continuity = Buffer.alloc(0);
    if (continuityBytes > 0) {
      const relativeContinuityStart = committedOffset - continuityBytes - start;
      if (relativeContinuityStart >= 0 && bytes.length >= committedOffset - start) {
        continuity = bytes.subarray(
          relativeContinuityStart,
          relativeContinuityStart + continuityBytes,
        );
      } else if (!reset && priorContinuity.length > 0) {
        const newlyCommitted = bytes.subarray(0, Math.max(0, committedOffset - start));
        const framing = Buffer.concat([priorContinuity, newlyCommitted]);
        continuity = framing.subarray(Math.max(0, framing.length - continuityBytes));
      } else {
        continuity = readBudgeted(continuityBytes, committedOffset - continuityBytes);
      }
    }

    const close = () => {
      if (closed) return;
      closed = true;
      fs.closeSync(fd);
    };
    const assertStableForCommit = () => {
      if (closed) throw new JsonlSnapshotChangedError();
      let currentFd: fs.BigIntStats;
      let currentPath: fs.BigIntStats;
      try {
        currentFd = fs.fstatSync(fd, { bigint: true });
        currentPath = fs.lstatSync(file, { bigint: true });
      } catch {
        throw new JsonlSnapshotChangedError();
      }
      if (
        currentPath.isSymbolicLink() ||
        !currentPath.isFile() ||
        !sameGenerationSnapshot(currentFd, snapshot) ||
        !sameGenerationSnapshot(currentPath, snapshot)
      ) {
        throw new JsonlSnapshotChangedError();
      }
    };

    return {
      lines,
      observedSize,
      committedOffset,
      deferredBytes: Math.max(0, observedSize - committedOffset),
      fileIdentity: openedIdentity,
      headHash,
      headBytes,
      continuityHash: continuityBytes > 0 ? hashBytes(continuity) : null,
      continuityBytes,
      mtimeMs: observedMtimeMs,
      ctimeMs: observedCtimeMs,
      bytesRead,
      workRemaining,
      unresolvedRecord,
      reset,
      legacyRebuild,
      checkpointRebuild,
      assertStableForCommit,
      close,
    };
  } catch (error) {
    if (!closed) {
      closed = true;
      fs.closeSync(fd);
    }
    throw error;
  }
}

/**
 * Bound a byte slice to complete records without decoding its deferred tail.
 * `committedBytes` always ends immediately after a newline.
 */
export function truncateJsonlReadToCompleteRecords(bytes: Buffer, maxRecords: number) {
  if (!Number.isSafeInteger(maxRecords) || maxRecords < 1) {
    throw new RangeError("maxRecords must be a positive safe integer");
  }
  let committedBytes = 0;
  let records = 0;
  while (records < maxRecords) {
    const newline = bytes.indexOf(0x0a, committedBytes);
    if (newline < 0) break;
    committedBytes = newline + 1;
    records += 1;
  }
  const moreCompleteRecords =
    records === maxRecords && bytes.indexOf(0x0a, committedBytes) >= 0;
  return { committedBytes, records, moreCompleteRecords };
}

function validCursorEnvelope(row: RawCursorRow) {
  if (!nonnegativeInteger(row.size)) return false;
  if (!nonnegativeInteger(row.committedOffset) || row.committedOffset > row.size) return false;
  if (!nonnegativeInteger(row.deferredBytes)) return false;
  if (row.committedOffset + row.deferredBytes !== row.size) return false;
  if (typeof row.fileIdentity !== "string" || row.fileIdentity.length === 0) return false;
  if (typeof row.mtimeMs !== "number" || !Number.isFinite(row.mtimeMs)) return false;
  if (typeof row.ctimeMs !== "number" || !Number.isFinite(row.ctimeMs)) return false;
  if (row.workRemaining !== 0 && row.workRemaining !== 1) return false;
  const unresolved = decodeUnresolvedRecord(row);
  const hasAnyUnresolvedField =
    row.unresolvedKind !== null ||
    row.unresolvedOffset !== null ||
    row.unresolvedObservedBytes !== null ||
    row.unresolvedAvailableBytes !== null ||
    row.unresolvedByteBudget !== null;
  if (hasAnyUnresolvedField && !unresolved) return false;
  if (unresolved && row.workRemaining === 1) return false;
  if (unresolved && unresolved.offset !== row.committedOffset) return false;
  if (unresolved && unresolved.availableBytes !== row.size - row.committedOffset) return false;
  if (!boundedProbe(row.headBytes, row.headHash, row.size)) return false;
  if (
    row.size > 0 &&
    row.headBytes === 0 &&
    unresolved?.reason !== "generation_rewrite_ambiguous"
  ) return false;
  if (!boundedProbe(row.continuityBytes, row.continuityHash, row.committedOffset)) return false;
  return row.continuityBytes === Math.min(PROBE_BYTES, row.committedOffset);
}

function decodeUnresolvedRecord(row: RawCursorRow): JsonlUnresolvedRecord | null {
  if (row.unresolvedKind === null) return null;
  if (
    row.unresolvedKind !== "record_exceeds_byte_budget" &&
    row.unresolvedKind !== "generation_rewrite_ambiguous"
  ) return null;
  if (!nonnegativeInteger(row.unresolvedOffset)) return null;
  if (!nonnegativeInteger(row.unresolvedObservedBytes)) return null;
  if (!nonnegativeInteger(row.unresolvedAvailableBytes)) return null;
  if (!nonnegativeInteger(row.unresolvedByteBudget)) return null;
  if (row.unresolvedObservedBytes > row.unresolvedAvailableBytes) return null;
  if (row.unresolvedObservedBytes > row.unresolvedByteBudget) return null;
  return {
    reason: row.unresolvedKind,
    offset: row.unresolvedOffset,
    observedBytes: row.unresolvedObservedBytes,
    availableBytes: row.unresolvedAvailableBytes,
    byteBudget: row.unresolvedByteBudget,
  };
}

function boundedProbe(bytes: number | null, hash: string | null, upperBound: number) {
  if (!nonnegativeInteger(bytes) || bytes > PROBE_BYTES || bytes > upperBound) return false;
  if (bytes === 0) return hash === null;
  return typeof hash === "string" && SHA256_RE.test(hash);
}

function nonnegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function boundedLimit(
  value: number | undefined,
  fallback: number,
  minimum: number,
  maximum: number,
  name: string,
) {
  const resolved = value ?? fallback;
  if (!Number.isSafeInteger(resolved) || resolved < minimum || resolved > maximum) {
    throw new RangeError(`${name} must be a safe integer from ${minimum} through ${maximum}`);
  }
  return resolved;
}

type GenerationSnapshot = {
  dev: bigint;
  ino: bigint;
  size: bigint;
  mtimeNs: bigint;
  ctimeNs: bigint;
  birthtimeNs: bigint;
};

function generationSnapshot(stat: fs.BigIntStats): GenerationSnapshot {
  return {
    dev: stat.dev,
    ino: stat.ino,
    size: stat.size,
    mtimeNs: stat.mtimeNs,
    ctimeNs: stat.ctimeNs,
    birthtimeNs: stat.birthtimeNs,
  };
}

function sameGenerationSnapshot(stat: fs.BigIntStats, expected: GenerationSnapshot) {
  return (
    stat.dev === expected.dev &&
    stat.ino === expected.ino &&
    stat.size === expected.size &&
    stat.mtimeNs === expected.mtimeNs &&
    stat.ctimeNs === expected.ctimeNs &&
    stat.birthtimeNs === expected.birthtimeNs
  );
}

function fileIdentity(stat: GenerationSnapshot) {
  // Inodes may be reused after unlink/rotation. Birth time is persisted only
  // as part of this opaque metadata identity, so an inode-reuse replacement
  // cannot take the unchanged fast path even if size and mtime collide.
  return `${String(stat.dev)}:${String(stat.ino)}:${String(stat.birthtimeNs)}`;
}

function normalStatMatchesPrecise(stat: fs.Stats, precise: fs.BigIntStats) {
  return (
    BigInt(stat.dev) === precise.dev &&
    BigInt(stat.ino) === precise.ino &&
    BigInt(stat.size) === precise.size
  );
}

function safeStatNumber(value: bigint) {
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < 0) throw new JsonlSnapshotChangedError();
  return number;
}

function nanosecondsToMilliseconds(value: bigint) {
  return Number(value) / 1_000_000;
}

function newerScan(candidate: string, current: string) {
  const candidateTime = Date.parse(candidate);
  const currentTime = Date.parse(current);
  if (!Number.isFinite(candidateTime)) return false;
  if (!Number.isFinite(currentTime)) return true;
  return candidateTime > currentTime;
}

function readAt(fd: number, length: number, position: number) {
  if (length <= 0) return Buffer.alloc(0);
  const buffer = Buffer.allocUnsafe(length);
  let filled = 0;
  while (filled < length) {
    const read = fs.readSync(fd, buffer, filled, length - filled, position + filled);
    if (read === 0) break;
    filled += read;
  }
  return buffer.subarray(0, filled);
}

function hashBytes(bytes: Buffer) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}
