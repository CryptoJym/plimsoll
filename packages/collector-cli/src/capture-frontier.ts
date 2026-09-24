import type fs from "node:fs";

import type Database from "better-sqlite3";

import { jsonlScanStateKey } from "./jsonl-byte-tailer";

/**
 * Capture watermark v1 (eco-6hoxj.163.18) — the capture half.
 *
 * The capture frontier is the time before which every usage event the tailed
 * sources (Codex rollouts, Claude transcripts) wrote is in the ledger, apart
 * from the gaps reported with it. It moves only from a coverage check of EVERY
 * rollout and transcript file under the capture roots, never from a capture
 * pass (review r2, B1): automatic passes read only today's and yesterday's
 * Codex day folders and files written in the last 48 hours, and never a file
 * excluded at enrollment, so a pass can finish clean while a long-running
 * session keeps writing a file it never opens.
 *
 * The check is stat-only. For each file it asks the tailer's own cursor
 * whether the file has been read to its current end.
 * - Read to its end, or untouched since the epoch began: covered.
 * - Otherwise the file is uncovered since the last check that saw it covered
 *   (or since it was created). For up to CAPTURE_HOLD_LIMIT_MS it holds the
 *   frontier there: a live file is normally read within a cadence or two.
 *   After that, its unread interval is reported as a gap — from that time,
 *   less the write lag, to its last write — and the frontier moves on. A gap
 *   lasts until the file is read to its end; a file deleted unread stays one.
 * The frontier is the earliest hold (or the check's start) less the write lag,
 * and it never moves backwards.
 *
 * Push-based sources (hooks, OTLP, live usage) append on receipt; what a spool
 * still holds, or lost, bounds the claim through `capture-spool-state.ts`.
 */
export const CAPTURE_FRONTIER_SOURCES = ["codex", "claude_code"] as const;
export type CaptureFrontierSource = (typeof CAPTURE_FRONTIER_SOURCES)[number];

/**
 * A tailed or spooled event is written within this long of its own
 * timestamp: a streamed assistant message is stamped when it starts and
 * written when it ends, an OTLP exporter batches and retries. The frontier is
 * stated in event time, so it trails file coverage by this much.
 */
export const CAPTURE_WRITE_LAG_MS = 60 * 60 * 1000;
/** How long a file with unread bytes may hold the frontier before it is reported as a gap. */
export const CAPTURE_HOLD_LIMIT_MS = 60 * 60 * 1000;
/** How often the maintenance worker runs the (stat-only) coverage check. */
export const CAPTURE_COVERAGE_INTERVAL_MS = 15 * 60 * 1000;
/** Directory entries one check may visit per source; beyond it the check is incomplete. */
export const CAPTURE_COVERAGE_MAX_ENTRIES = 200_000;
/** Gaps one claim carries. Closer gaps merge first, which only widens them. */
export const CAPTURE_CLAIM_MAX_GAPS = 8;

/** A closed interval, in event time, in which captured data may be missing. */
export type CaptureGap = { fromMs: number; toMs: number };

export type CaptureFrontier = {
  workspaceId: string;
  installationEpochId: string;
  epochStartedAt: string;
  /** Minimum over both sources; null until each has a frontier in this epoch. */
  capturedThrough: string | null;
  /** Unread intervals of tailed files that a source's frontier has moved past. */
  gaps: CaptureGap[];
};

/** One file as the coverage check sees it: stat plus the tailer's own cursor verdict. */
export type CaptureCoverageFile = {
  /** The tailer's opaque cursor key (a SHA-256), never the path. */
  key: string;
  size: number;
  mtimeMs: number;
  birthtimeMs: number;
  /** The tailer has a cursor for this file. */
  hasCursor: boolean;
  /** That cursor has committed every byte of the file's current size. */
  fullyRead: boolean;
};
/** `complete` is false when the walk hit its entry budget, a directory error or an unready root. */
export type CaptureCoverageSnapshot = { complete: boolean; files: CaptureCoverageFile[] };

const canonical = (value: unknown): value is string => {
  if (typeof value !== "string") return false;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString() === value;
};
const isoMs = (ms: number) => new Date(ms).toISOString();

export function ensureCaptureFrontierSchema(database: Database.Database): void {
  database.exec(`
    create table if not exists capture_coverage_state (
      workspace_id text not null,
      installation_epoch_id text not null,
      source text not null check (source in ('codex','claude_code')),
      checked_at text not null,
      complete_through text,
      updated_at text not null,
      primary key (workspace_id, installation_epoch_id, source)
    ) without rowid;
    create table if not exists capture_uncovered_files (
      workspace_id text not null,
      installation_epoch_id text not null,
      source text not null check (source in ('codex','claude_code')),
      file_key text not null,
      uncovered_since text not null,
      last_write_at text not null,
      primary key (workspace_id, installation_epoch_id, source, file_key)
    ) without rowid;
  `);
}

function tableExists(database: Database.Database, name: string) {
  return Boolean(database.prepare(`select 1 from sqlite_master where type='table' and name=?`).get(name));
}

function currentEpoch(database: Database.Database) {
  if (!tableExists(database, "collector_workspace_binding")) return null;
  const row = database
    .prepare(
      `select current_workspace_id as workspaceId,
         current_installation_epoch_id as installationEpochId,
         current_installation_epoch_started_at as epochStartedAt
       from collector_workspace_binding where singleton = 1`,
    )
    .get() as { workspaceId: string | null; installationEpochId: string | null; epochStartedAt: string | null } | undefined;
  if (!row?.workspaceId || !row.installationEpochId || !row.epochStartedAt) return null;
  const epochStartedMs = Date.parse(row.epochStartedAt);
  if (!Number.isFinite(epochStartedMs)) return null;
  return {
    workspaceId: row.workspaceId,
    installationEpochId: row.installationEpochId,
    epochStartedAt: isoMs(epochStartedMs),
  };
}

/**
 * Stat every listed file and read the tailer's own cursor row for it (the
 * tailers pass their `cursorKey`, so the key cannot drift from theirs). Null
 * when a listed file cannot be stat'ed for a reason other than having gone.
 */
export function captureCoverageFiles(
  database: Database.Database,
  files: readonly string[],
  cursorKey: (file: string) => string,
  lstat: (file: string) => fs.Stats,
): CaptureCoverageFile[] | null {
  const cursor = database.prepare(
    `select size, committed_offset as committedOffset, deferred_bytes as deferredBytes,
       work_remaining as workRemaining, unresolved_kind as unresolvedKind
     from rollout_scan_state where file = ?`,
  );
  const covered: CaptureCoverageFile[] = [];
  for (const file of files) {
    let stat: fs.Stats;
    try {
      stat = lstat(file);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      return null;
    }
    if (!stat.isFile()) continue;
    const key = jsonlScanStateKey(cursorKey(file));
    const row = cursor.get(key) as
      | { size: number; committedOffset: number | null; deferredBytes: number | null; workRemaining: number | null; unresolvedKind: string | null }
      | undefined;
    // A legacy row (no committed offset) recorded the size it had read.
    const committed = row ? row.committedOffset ?? row.size : 0;
    const fullyRead = row !== undefined && committed >= stat.size &&
      !row.workRemaining && !row.unresolvedKind && (row.deferredBytes ?? 0) === 0;
    covered.push({
      key,
      size: stat.size,
      mtimeMs: stat.mtimeMs,
      birthtimeMs: stat.birthtimeMs,
      hasCursor: row !== undefined,
      fullyRead,
    });
  }
  return covered;
}

/**
 * Apply one coverage check of one source that started at `startedAt`, before
 * its walk. Returns the source's frontier after the check (null while nothing
 * is attested). An incomplete walk, or one not newer than the last, changes
 * nothing.
 */
export function advanceCaptureFrontier(
  database: Database.Database,
  source: CaptureFrontierSource,
  snapshot: CaptureCoverageSnapshot,
  startedAt: string,
): string | null {
  const epoch = currentEpoch(database);
  if (!epoch || !canonical(startedAt)) return null;
  ensureCaptureFrontierSchema(database);
  const scope = [epoch.workspaceId, epoch.installationEpochId, source] as const;
  const run = database.transaction(() => {
    const state = database
      .prepare(
        `select checked_at as checkedAt, complete_through as completeThrough from capture_coverage_state
         where workspace_id = ? and installation_epoch_id = ? and source = ?`,
      )
      .get(...scope) as { checkedAt: string; completeThrough: string | null } | undefined;
    const previous = state?.completeThrough ?? null;
    const startedMs = Date.parse(startedAt);
    const lastCheckMs = state ? Date.parse(state.checkedAt) : null;
    if (!snapshot.complete || (lastCheckMs !== null && !(startedMs > lastCheckMs))) return previous;
    const epochStartMs = Date.parse(epoch.epochStartedAt);
    const uncovered = new Map(
      (database
        .prepare(
          `select file_key as key, uncovered_since as since, last_write_at as lastWriteAt
           from capture_uncovered_files
           where workspace_id = ? and installation_epoch_id = ? and source = ?`,
        )
        .all(...scope) as Array<{ key: string; since: string; lastWriteAt: string }>).map((row) => [row.key, row]),
    );
    const remove = database.prepare(
      `delete from capture_uncovered_files
       where workspace_id = ? and installation_epoch_id = ? and source = ? and file_key = ?`,
    );
    const upsert = database.prepare(
      `insert into capture_uncovered_files
         (workspace_id, installation_epoch_id, source, file_key, uncovered_since, last_write_at)
       values (?, ?, ?, ?, ?, ?)
       on conflict (workspace_id, installation_epoch_id, source, file_key) do update set
         last_write_at = excluded.last_write_at`,
    );
    let bound = startedMs;
    for (const file of snapshot.files) {
      const row = uncovered.get(file.key);
      if (file.fullyRead || file.mtimeMs < epochStartMs) {
        if (row) remove.run(...scope, file.key);
        continue;
      }
      // Since when may this file's unread bytes have been written? A file the
      // tailer has read before and the last check saw covered (no row): since
      // that check. A file never read, or born after that check: since birth.
      const born = Number.isFinite(file.birthtimeMs) && file.birthtimeMs > 0 ? file.birthtimeMs : null;
      const sinceMs = row
        ? Date.parse(row.since)
        : file.hasCursor && lastCheckMs !== null && (born === null || born <= lastCheckMs)
          ? lastCheckMs
          : Math.max(epochStartMs, born ?? epochStartMs);
      const lastWriteMs = Math.max(file.mtimeMs, row ? Date.parse(row.lastWriteAt) : 0);
      upsert.run(...scope, file.key, isoMs(sinceMs), isoMs(lastWriteMs));
      if (startedMs - sinceMs <= CAPTURE_HOLD_LIMIT_MS) bound = Math.min(bound, sinceMs);
    }
    // Rows of files no longer listed stay: their unread bytes are a gap now.
    const candidateMs = bound - CAPTURE_WRITE_LAG_MS;
    const next = candidateMs < epochStartMs || (previous !== null && candidateMs <= Date.parse(previous))
      ? previous
      : isoMs(candidateMs);
    database
      .prepare(
        `insert into capture_coverage_state
           (workspace_id, installation_epoch_id, source, checked_at, complete_through, updated_at)
         values (?, ?, ?, ?, ?, ?)
         on conflict (workspace_id, installation_epoch_id, source) do update set
           checked_at = excluded.checked_at, complete_through = excluded.complete_through,
           updated_at = excluded.updated_at`,
      )
      .run(...scope, startedAt, next, new Date().toISOString());
    return next;
  });
  return run.immediate();
}

/**
 * Sort, merge overlapping intervals, and merge the closest neighbours until at
 * most `max` remain. Merging only widens a gap, so the result stays honest.
 */
export function mergeCaptureGaps(gaps: readonly CaptureGap[], max = CAPTURE_CLAIM_MAX_GAPS): CaptureGap[] {
  const merged: CaptureGap[] = [];
  for (const gap of [...gaps].sort((a, b) => a.fromMs - b.fromMs || a.toMs - b.toMs)) {
    const last = merged.at(-1);
    if (last && gap.fromMs <= last.toMs) last.toMs = Math.max(last.toMs, gap.toMs);
    else merged.push({ fromMs: gap.fromMs, toMs: Math.max(gap.fromMs, gap.toMs) });
  }
  while (merged.length > Math.max(1, max)) {
    let closest = 0;
    for (let index = 1; index < merged.length - 1; index += 1) {
      if (merged[index + 1]!.fromMs - merged[index]!.toMs < merged[closest + 1]!.fromMs - merged[closest]!.toMs) {
        closest = index;
      }
    }
    merged[closest]!.toMs = Math.max(merged[closest]!.toMs, merged[closest + 1]!.toMs);
    merged.splice(closest + 1, 1);
  }
  return merged;
}

export function captureFrontier(database: Database.Database): CaptureFrontier | null {
  const epoch = currentEpoch(database);
  if (!epoch) return null;
  if (!tableExists(database, "capture_coverage_state")) return { ...epoch, capturedThrough: null, gaps: [] };
  const scope = [epoch.workspaceId, epoch.installationEpochId] as const;
  const rows = database
    .prepare(
      `select source, complete_through as completeThrough from capture_coverage_state
       where workspace_id = ? and installation_epoch_id = ?`,
    )
    .all(...scope) as Array<{ source: string; completeThrough: string | null }>;
  const bySource = new Map(rows.map((row) => [row.source, row.completeThrough]));
  const values = CAPTURE_FRONTIER_SOURCES.map((source) => bySource.get(source));
  const capturedThrough = values.every((value): value is string => canonical(value))
    ? values.reduce((a, b) => (Date.parse(b) < Date.parse(a) ? b : a))
    : null;
  // A file's unread interval is a gap once its source's frontier has moved
  // past its start; before that it only holds the frontier back. One row per
  // UTC day of the uncovered start keeps this read bounded on the upload path
  // (the stored times are canonical ISO strings, so text order is time order).
  const epochStartMs = Date.parse(epoch.epochStartedAt);
  const unread = database.prepare(
    `select min(uncovered_since) as since, max(last_write_at) as lastWriteAt from capture_uncovered_files
     where workspace_id = ? and installation_epoch_id = ? and source = ? and uncovered_since < ?
     group by substr(uncovered_since, 1, 10)`,
  );
  const gaps: CaptureGap[] = [];
  for (const source of CAPTURE_FRONTIER_SOURCES) {
    const through = bySource.get(source);
    if (!canonical(through)) continue;
    const throughMs = Date.parse(through);
    const rows = unread.all(...scope, source, isoMs(throughMs + CAPTURE_WRITE_LAG_MS)) as Array<{ since: string; lastWriteAt: string }>;
    for (const row of rows) {
      const fromMs = Math.max(epochStartMs, Date.parse(row.since) - CAPTURE_WRITE_LAG_MS);
      if (fromMs < throughMs) gaps.push({ fromMs, toMs: Math.max(fromMs, Date.parse(row.lastWriteAt)) });
    }
  }
  return { ...epoch, capturedThrough, gaps: mergeCaptureGaps(gaps) };
}
