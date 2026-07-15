import crypto from "node:crypto";
import fs from "node:fs";

import type Database from "better-sqlite3";

const PROBE_BYTES = 512;
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
  parserState: T | undefined;
  checkpointStatus: CheckpointStatus;
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
  bytesRead: number;
  reset: boolean;
  legacyRebuild: boolean;
  checkpointRebuild: boolean;
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
  parserKind: string | null;
  checkpointVersion: number | null;
  parserStateJson: string | null;
};

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
         parser_kind as parserKind,
         checkpoint_version as checkpointVersion,
         parser_state_json as parserStateJson
       from ${STATE_TABLE} where file = ?`,
    )
    .get(file) as RawCursorRow | undefined;
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
          parser_kind, checkpoint_version, parser_state_json)
       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
         parser_kind = excluded.parser_kind,
         checkpoint_version = excluded.checkpoint_version,
         parser_state_json = excluded.parser_state_json`,
    )
    .run(
      file,
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
      parserKind,
      checkpointVersion,
      JSON.stringify(parserState),
    );
}

/**
 * Read complete JSONL records after the last committed newline. Partial bytes
 * remain only in the source file: no content carry is copied into SQLite.
 * The checkpoint advances after its caller commits parsed events and state in
 * one database transaction, so a crash replays deterministic ids safely.
 */
export function readJsonlTail(
  file: string,
  stat: fs.Stats,
  cursor: JsonlScanCursor<unknown> | undefined,
): JsonlTailRead | undefined {
  const identity = `${String(stat.dev)}:${String(stat.ino)}`;
  const unchangedIdentity = !cursor?.fileIdentity || cursor.fileIdentity === identity;
  if (
    cursor &&
    cursor.checkpointStatus !== "invalid" &&
    stat.size === cursor.observedSize &&
    unchangedIdentity &&
    (cursor.mtimeMs === null || cursor.mtimeMs === stat.mtimeMs)
  ) {
    return undefined;
  }

  let legacyRebuild = false;
  let checkpointRebuild = false;
  let reset = false;
  let start = cursor?.committedOffset ?? 0;
  if (cursor) {
    legacyRebuild = cursor.checkpointStatus === "legacy";
    checkpointRebuild = cursor.checkpointStatus === "invalid";
    reset =
      legacyRebuild ||
      checkpointRebuild ||
      !unchangedIdentity ||
      stat.size < cursor.observedSize ||
      stat.size < start ||
      (stat.size === cursor.observedSize && cursor.mtimeMs !== stat.mtimeMs);
    if (reset) start = 0;
  }

  const fd = fs.openSync(file, "r");
  let bytesRead = 0;
  let priorContinuity = Buffer.alloc(0);
  try {
    // The head probe catches ordinary replacement. The independent continuity
    // probe immediately before committedOffset catches same-inode
    // truncate-and-regrow where the first bytes were deliberately preserved.
    if (!reset && cursor?.headHash && cursor.headBytes > 0) {
      const head = readAt(fd, cursor.headBytes, 0);
      bytesRead += head.length;
      if (head.length !== cursor.headBytes || hashBytes(head) !== cursor.headHash) {
        reset = true;
        start = 0;
      }
    }
    if (!reset && cursor?.continuityHash && cursor.continuityBytes > 0) {
      priorContinuity = readAt(
        fd,
        cursor.continuityBytes,
        Math.max(0, start - cursor.continuityBytes),
      );
      bytesRead += priorContinuity.length;
      if (
        priorContinuity.length !== cursor.continuityBytes ||
        hashBytes(priorContinuity) !== cursor.continuityHash
      ) {
        reset = true;
        start = 0;
        priorContinuity = Buffer.alloc(0);
      }
    }

    const available = Math.max(0, stat.size - start);
    const bytes = readAt(fd, available, start);
    bytesRead += bytes.length;

    const lastNewline = bytes.lastIndexOf(0x0a);
    const completeBytes = lastNewline >= 0 ? bytes.subarray(0, lastNewline) : Buffer.alloc(0);
    const committedOffset = lastNewline >= 0 ? start + lastNewline + 1 : start;
    const lines =
      completeBytes.length === 0
        ? []
        : completeBytes
            .toString("utf8")
            .split("\n")
            .map((line) => (line.endsWith("\r") ? line.slice(0, -1) : line));

    let headBytes = cursor?.headBytes ?? 0;
    let headHash = cursor?.headHash ?? null;
    if (reset || !cursor || !headHash) {
      headBytes = Math.min(PROBE_BYTES, stat.size);
      const head = start === 0 && bytes.length >= headBytes
        ? bytes.subarray(0, headBytes)
        : readAt(fd, headBytes, 0);
      if (!(start === 0 && bytes.length >= headBytes)) bytesRead += head.length;
      headHash = headBytes > 0 ? hashBytes(head) : null;
    }

    const continuityBytes = Math.min(PROBE_BYTES, committedOffset);
    let continuity = Buffer.alloc(0);
    if (continuityBytes > 0) {
      if (start === 0 && bytes.length >= committedOffset) {
        continuity = bytes.subarray(committedOffset - continuityBytes, committedOffset);
      } else if (!reset && priorContinuity.length > 0) {
        const newlyCommitted = bytes.subarray(0, Math.max(0, committedOffset - start));
        const framing = Buffer.concat([priorContinuity, newlyCommitted]);
        continuity = framing.subarray(Math.max(0, framing.length - continuityBytes));
      } else {
        continuity = readAt(fd, continuityBytes, committedOffset - continuityBytes);
        bytesRead += continuity.length;
      }
    }

    return {
      lines,
      observedSize: stat.size,
      committedOffset,
      deferredBytes: Math.max(0, stat.size - committedOffset),
      fileIdentity: identity,
      headHash,
      headBytes,
      continuityHash: continuityBytes > 0 ? hashBytes(continuity) : null,
      continuityBytes,
      mtimeMs: stat.mtimeMs,
      bytesRead,
      reset,
      legacyRebuild,
      checkpointRebuild,
    };
  } finally {
    fs.closeSync(fd);
  }
}

function validCursorEnvelope(row: RawCursorRow) {
  if (!nonnegativeInteger(row.size)) return false;
  if (!nonnegativeInteger(row.committedOffset) || row.committedOffset > row.size) return false;
  if (!nonnegativeInteger(row.deferredBytes)) return false;
  if (row.committedOffset + row.deferredBytes !== row.size) return false;
  if (typeof row.fileIdentity !== "string" || row.fileIdentity.length === 0) return false;
  if (typeof row.mtimeMs !== "number" || !Number.isFinite(row.mtimeMs)) return false;
  if (!boundedProbe(row.headBytes, row.headHash, row.size)) return false;
  if (row.size > 0 && row.headBytes === 0) return false;
  if (!boundedProbe(row.continuityBytes, row.continuityHash, row.committedOffset)) return false;
  return row.continuityBytes === Math.min(PROBE_BYTES, row.committedOffset);
}

function boundedProbe(bytes: number | null, hash: string | null, upperBound: number) {
  if (!nonnegativeInteger(bytes) || bytes > PROBE_BYTES || bytes > upperBound) return false;
  if (bytes === 0) return hash === null;
  return typeof hash === "string" && SHA256_RE.test(hash);
}

function nonnegativeInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
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
