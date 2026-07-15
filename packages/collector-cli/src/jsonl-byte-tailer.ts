import crypto from "node:crypto";
import fs from "node:fs";

import type Database from "better-sqlite3";

const HEAD_PROBE_BYTES = 512;
const STATE_TABLE = "rollout_scan_state";

export type JsonlScanCursor<T> = {
  observedSize: number;
  committedOffset: number | null;
  deferredBytes: number;
  fileIdentity: string | null;
  headHash: string | null;
  headBytes: number;
  mtimeMs: number | null;
  parserState: T | undefined;
};

export type JsonlTailRead = {
  lines: string[];
  observedSize: number;
  committedOffset: number;
  deferredBytes: number;
  fileIdentity: string;
  headHash: string | null;
  headBytes: number;
  mtimeMs: number;
  bytesRead: number;
  reset: boolean;
  legacyRebuild: boolean;
};

type RawCursorRow = {
  size: number;
  committedOffset: number | null;
  deferredBytes: number | null;
  fileIdentity: string | null;
  headHash: string | null;
  headBytes: number | null;
  mtimeMs: number | null;
  parserKind: string | null;
  parserStateJson: string | null;
};

/**
 * Keep the historical table name so existing collector ledgers migrate in
 * place. Legacy rows contain only `size`; they stay cheap while unchanged and
 * rebuild once, deterministically, the next time their file grows.
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
    "mtime_ms real",
    "parser_kind text",
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
): JsonlScanCursor<T> | undefined {
  const row = database
    .prepare(
      `select size,
         committed_offset as committedOffset,
         deferred_bytes as deferredBytes,
         file_identity as fileIdentity,
         head_hash as headHash,
         head_bytes as headBytes,
         mtime_ms as mtimeMs,
         parser_kind as parserKind,
         parser_state_json as parserStateJson
       from ${STATE_TABLE} where file = ?`,
    )
    .get(file) as RawCursorRow | undefined;
  if (!row) return undefined;

  let parserState: T | undefined;
  if (row.parserKind === parserKind && row.parserStateJson) {
    try {
      parserState = JSON.parse(row.parserStateJson) as T;
    } catch {
      // A corrupt/incompatible parser checkpoint is treated like a legacy
      // cursor: the next growth gets one safe deterministic rebuild.
    }
  }
  return {
    observedSize: row.size,
    committedOffset: parserState === undefined ? null : row.committedOffset,
    deferredBytes: row.deferredBytes ?? 0,
    fileIdentity: row.fileIdentity,
    headHash: row.headHash,
    headBytes: row.headBytes ?? 0,
    mtimeMs: row.mtimeMs,
    parserState,
  };
}

export function rememberJsonlScanCursor<T>(
  database: Database.Database,
  file: string,
  parserKind: string,
  read: JsonlTailRead,
  parserState: T,
) {
  database
    .prepare(
      `insert into ${STATE_TABLE}
         (file, size, scanned_at, committed_offset, deferred_bytes, file_identity,
          head_hash, head_bytes, mtime_ms, parser_kind, parser_state_json)
       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       on conflict(file) do update set
         size = excluded.size,
         scanned_at = excluded.scanned_at,
         committed_offset = excluded.committed_offset,
         deferred_bytes = excluded.deferred_bytes,
         file_identity = excluded.file_identity,
         head_hash = excluded.head_hash,
         head_bytes = excluded.head_bytes,
         mtime_ms = excluded.mtime_ms,
         parser_kind = excluded.parser_kind,
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
      read.mtimeMs,
      parserKind,
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
    stat.size === cursor.observedSize &&
    unchangedIdentity &&
    (cursor.mtimeMs === null || cursor.mtimeMs === stat.mtimeMs)
  ) {
    return undefined;
  }

  let reset = false;
  let legacyRebuild = false;
  let start = cursor?.committedOffset ?? 0;
  if (cursor) {
    legacyRebuild = cursor.committedOffset === null || cursor.parserState === undefined;
    reset =
      legacyRebuild ||
      !unchangedIdentity ||
      stat.size < cursor.observedSize ||
      stat.size < start ||
      (stat.size === cursor.observedSize && cursor.mtimeMs !== stat.mtimeMs);
    if (reset) start = 0;
  }

  const fd = fs.openSync(file, "r");
  let bytesRead = 0;
  try {
    // A fixed, tiny head probe catches same-inode truncate-and-regrow between
    // polls. It is framing overhead only; historical content is never swept.
    if (!reset && cursor?.headHash && cursor.headBytes > 0) {
      const probe = Buffer.allocUnsafe(cursor.headBytes);
      const read = fs.readSync(fd, probe, 0, probe.length, 0);
      bytesRead += read;
      const currentHash = hashBytes(probe.subarray(0, read));
      if (read !== cursor.headBytes || currentHash !== cursor.headHash) {
        reset = true;
        start = 0;
      }
    }

    const available = Math.max(0, stat.size - start);
    const raw = Buffer.allocUnsafe(available);
    let filled = 0;
    while (filled < available) {
      const read = fs.readSync(fd, raw, filled, available - filled, start + filled);
      if (read === 0) break;
      filled += read;
    }
    const bytes = raw.subarray(0, filled);
    bytesRead += filled;

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
      headBytes = Math.min(HEAD_PROBE_BYTES, stat.size);
      const head = start === 0 && filled >= headBytes ? bytes.subarray(0, headBytes) : readAt(fd, headBytes, 0);
      if (!(start === 0 && filled >= headBytes)) bytesRead += head.length;
      headHash = headBytes > 0 ? hashBytes(head) : null;
    }

    return {
      lines,
      observedSize: stat.size,
      committedOffset,
      deferredBytes: Math.max(0, stat.size - committedOffset),
      fileIdentity: identity,
      headHash,
      headBytes,
      mtimeMs: stat.mtimeMs,
      bytesRead,
      reset,
      legacyRebuild,
    };
  } finally {
    fs.closeSync(fd);
  }
}

function readAt(fd: number, length: number, position: number) {
  if (length <= 0) return Buffer.alloc(0);
  const buffer = Buffer.allocUnsafe(length);
  const read = fs.readSync(fd, buffer, 0, length, position);
  return buffer.subarray(0, read);
}

function hashBytes(bytes: Buffer) {
  return crypto.createHash("sha256").update(bytes).digest("hex");
}
