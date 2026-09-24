import type fs from "node:fs";

import type Database from "better-sqlite3";

import { jsonlScanStateKey } from "./jsonl-byte-tailer";

/**
 * Capture watermark v1 (eco-6hoxj.163.18) — the capture half.
 *
 * The capture frontier is the time before which every usage event the tailed
 * sources (Codex rollouts, Claude transcripts, Grok usage files) wrote is in
 * the ledger, apart from the gaps reported with it. It moves only from a
 * coverage check of EVERY file those tailers read, never from a capture pass
 * (review r2, B1): automatic passes read only today's and yesterday's Codex
 * day folders and files written in the last 48 hours, never a file excluded at
 * enrollment, and Grok's sweep reaches a session only every few cadences, so a
 * pass can finish clean while a file it has not read keeps growing.
 *
 * The check is stat-only and walks each source in turns of at most
 * CAPTURE_COVERAGE_TURN_MS (review r3, N4), resuming where it stopped. For
 * each file it asks the tailer's own state whether the file has been read to
 * its current end.
 * - Read to its end, or untouched since the epoch began: covered.
 * - Otherwise the file is uncovered since the last check that saw it covered,
 *   or since it was created. For up to CAPTURE_HOLD_LIMIT_MS it holds the
 *   frontier there: a live file is normally read within a cadence or two.
 *   After that, its unread interval is reported as a gap — from that time,
 *   less the write lag, to its last write — and the frontier moves on. A gap
 *   lasts until the file is read to its end; a file deleted unread stays one.
 * - A file the tailer keeps up with is uncovered only since the walk before:
 *   when the tailer has read everything the file held at that walk, what is
 *   unread now came after it (review r3, S5).
 * - A symlink where a tailer would read a file or descend into a directory is
 *   never covered (review r4, S1). The tailers do not follow links, and
 *   neither does this check: nothing behind one is read, listed or stat'ed.
 *   Following links is a privacy and security decision the tailers have not
 *   taken. The link is uncovered from its creation (not before the epoch
 *   start, nor after the walk that first saw it) to the last walk that saw
 *   it, and stays a gap after it is removed.
 * The frontier is the earliest hold (or the check's start) less the write lag,
 * and it never moves backwards.
 *
 * Push-based sources (hooks, OTLP, live usage) append on receipt; what a spool
 * still holds, or lost, bounds the claim through `capture-spool-state.ts`.
 */
export const CAPTURE_FRONTIER_SOURCES = ["codex", "claude_code", "grok"] as const;
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
/** How often the maintenance worker starts a (stat-only) coverage check. */
export const CAPTURE_COVERAGE_INTERVAL_MS = 15 * 60 * 1000;
/** Wall time one maintenance cadence spends on a coverage check; the walk resumes on the next. */
export const CAPTURE_COVERAGE_TURN_MS = 250;
/** Directory entries one check may visit per source; beyond it the check is incomplete. */
export const CAPTURE_COVERAGE_MAX_ENTRIES = 200_000;
/** Gaps one claim carries. Closer gaps merge first, which only widens them. */
export const CAPTURE_CLAIM_MAX_GAPS = 8;
/** Files checked between two ledger writes of a walk: keeps every write short. */
const CAPTURE_COVERAGE_BATCH = 256;

/** A closed interval, in event time, in which captured data may be missing. */
export type CaptureGap = { fromMs: number; toMs: number };

export type CaptureFrontier = {
  workspaceId: string;
  installationEpochId: string;
  epochStartedAt: string;
  /** Minimum over every source; null until each has a frontier in this epoch. */
  capturedThrough: string | null;
  /** Unread intervals of tailed files that a source's frontier has moved past. */
  gaps: CaptureGap[];
};

/** One file as the coverage check sees it: its stat plus the tailer's own verdict. */
export type CaptureCoverageFile = {
  /** The tailer's opaque state key (a SHA-256), never the path. */
  key: string;
  mtimeMs: number;
  birthtimeMs: number;
  /**
   * How much the file holds, in its tailer's unit: bytes of a JSONL log, the
   * modification time of a whole-document usage file.
   */
  extent: number;
  /** How far the tailer has read, in the same unit; -1 when it never has. */
  progress: number;
  /** The tailer has read everything the file holds now. */
  fullyRead: boolean;
  /**
   * A symlink the tailer does not follow: never read, so never covered. Its
   * `key` is never a file's, and `mtimeMs` is the time the walk saw it (the
   * link's own times say nothing about what lies behind it).
   */
  link?: true;
};
/** `complete` is false when a walk hit its entry budget, a directory error or an unready root. */
export type CaptureCoverageSnapshot = { complete: boolean; files: CaptureCoverageFile[] };

/**
 * What one directory holds for a walk: directories to descend into, files to
 * check, and symlinks where the tailer would descend if they were directories.
 */
export type CaptureCoverageListing = { directories: string[]; files: string[]; links?: string[] };

export type CaptureCoverageWalkSpec = {
  roots: readonly string[];
  /** List one directory; depth 0 is a root. Throws when it cannot be listed. */
  list(directory: string, depth: number): CaptureCoverageListing;
  /**
   * Stat one file (lstat, never through a link) and read the tailer's state; a
   * symlinked file is a `link` verdict. Null when it is gone or not a regular
   * file or link. Throws otherwise.
   */
  check(file: string): CaptureCoverageFile | null;
  /** The `link` verdict for a listed symlink (lstat only); null once it is gone or no longer a link. */
  checkLink(link: string): CaptureCoverageFile | null;
  maxEntries?: number;
};

/** The verdict for a symlink the tailer does not follow, seen by a walk now. */
export function linkCoverageFile(key: string, birthtimeMs: number): CaptureCoverageFile {
  return { key, mtimeMs: Date.now(), birthtimeMs, extent: -1, progress: -1, fullyRead: false, link: true };
}

const errorCode = (error: unknown) => (error as NodeJS.ErrnoException | null)?.code;

/**
 * A resumable, stat-only walk of one source's files (review r3, N4). Each
 * `step` checks files until its deadline and hands them over in batches;
 * `done` once everything was visited, and `complete` unless an entry budget, a
 * listing error or a failed stat made the walk partial. It lives in memory: a
 * worker restart starts a new walk, and only a complete one moves the frontier.
 */
export class CaptureCoverageWalk {
  private readonly directories: Array<{ directory: string; depth: number }>;
  private readonly files: Array<{ path: string; link: boolean }> = [];
  private entries = 0;
  done = false;
  complete = true;

  constructor(private readonly spec: CaptureCoverageWalkSpec | null) {
    // A null spec is a walk that cannot be taken (for example an unready root).
    this.directories = spec ? spec.roots.map((directory) => ({ directory, depth: 0 })).reverse() : [];
    if (!spec) this.fail();
  }

  /** A complete walk of a source with nothing to read. */
  static empty() {
    return new CaptureCoverageWalk({
      roots: [],
      list: () => ({ directories: [], files: [] }),
      check: () => null,
      checkLink: () => null,
    });
  }

  step(deadline: number, onBatch: (files: CaptureCoverageFile[]) => void, now: () => number = () => performance.now()) {
    let batch: CaptureCoverageFile[] = [];
    const flush = () => {
      if (batch.length > 0) onBatch(batch);
      batch = [];
    };
    while (!this.done && now() < deadline) {
      const file = this.files.pop();
      if (file !== undefined) {
        try {
          const checked = file.link ? this.spec!.checkLink(file.path) : this.spec!.check(file.path);
          if (checked) batch.push(checked);
        } catch {
          this.fail();
        }
        if (batch.length >= CAPTURE_COVERAGE_BATCH) flush();
        continue;
      }
      const next = this.directories.pop();
      if (!next) {
        this.done = true;
        break;
      }
      let listing: CaptureCoverageListing;
      try {
        listing = this.spec!.list(next.directory, next.depth);
      } catch (error) {
        // A missing root holds no files, as it does for the tailers' scans.
        if (!(next.depth === 0 && errorCode(error) === "ENOENT")) this.fail();
        continue;
      }
      const links = listing.links ?? [];
      this.entries += listing.directories.length + listing.files.length + links.length;
      if (this.entries > (this.spec!.maxEntries ?? CAPTURE_COVERAGE_MAX_ENTRIES)) {
        this.fail();
        continue;
      }
      for (let index = listing.directories.length - 1; index >= 0; index -= 1) {
        this.directories.push({ directory: listing.directories[index]!, depth: next.depth + 1 });
      }
      for (let index = links.length - 1; index >= 0; index -= 1) this.files.push({ path: links[index]!, link: true });
      for (let index = listing.files.length - 1; index >= 0; index -= 1) {
        this.files.push({ path: listing.files[index]!, link: false });
      }
    }
    flush();
  }

  private fail() {
    this.complete = false;
    this.done = true;
    this.directories.length = 0;
    this.files.length = 0;
  }
}

/**
 * The verdict for one tailed JSONL file (rollout or transcript) from its
 * lstat and the tailer's own cursor row. The tailers pass their cursor key, so
 * the key cannot drift from theirs. A symlink is a `link` verdict under a key
 * of its own (a path holds no NUL, so none collides with a file's).
 */
export function jsonlCoverageCheck(database: Database.Database) {
  const cursor = database.prepare(
    `select size, committed_offset as committedOffset, deferred_bytes as deferredBytes,
       work_remaining as workRemaining, unresolved_kind as unresolvedKind
     from rollout_scan_state where file = ?`,
  );
  return (cursorKey: string, stat: fs.Stats): CaptureCoverageFile | null => {
    // The tailers never read a symlinked JSONL file or descend through a
    // symlinked directory (review r4, S1).
    if (stat.isSymbolicLink()) return linkCoverageFile(jsonlScanStateKey(`${cursorKey}\0symlink`), stat.birthtimeMs);
    if (!stat.isFile()) return null;
    const key = jsonlScanStateKey(cursorKey);
    const row = cursor.get(key) as
      | { size: number; committedOffset: number | null; deferredBytes: number | null; workRemaining: number | null; unresolvedKind: string | null }
      | undefined;
    // A legacy row (no committed offset) recorded the size it had read.
    const committed = row ? row.committedOffset ?? row.size : -1;
    return {
      key,
      mtimeMs: stat.mtimeMs,
      birthtimeMs: stat.birthtimeMs,
      extent: stat.size,
      progress: committed,
      fullyRead: row !== undefined && committed >= stat.size &&
        !row.workRemaining && !row.unresolvedKind && (row.deferredBytes ?? 0) === 0,
    };
  };
}

/** lstat that reports a vanished file as null, the way every walk treats one. */
export function lstatIfPresent(lstat: (file: string) => fs.Stats, file: string) {
  try {
    return lstat(file);
  } catch (error) {
    if (errorCode(error) === "ENOENT") return null;
    throw error;
  }
}

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
      source text not null,
      checked_at text not null,
      complete_through text,
      updated_at text not null,
      primary key (workspace_id, installation_epoch_id, source)
    ) without rowid;
    create table if not exists capture_uncovered_files (
      workspace_id text not null,
      installation_epoch_id text not null,
      source text not null,
      file_key text not null,
      uncovered_since text not null,
      last_write_at text not null,
      seen_extent real not null,
      seen_at text not null,
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

/** One source's coverage check in progress: the epoch it belongs to and the times it is judged by. */
export type CaptureCoverageCheck = {
  source: CaptureFrontierSource;
  workspaceId: string;
  installationEpochId: string;
  epochStartMs: number;
  /** Taken before the walk lists anything. */
  startedAt: string;
  startedMs: number;
  /** Start of the last complete check of this source, if any. */
  lastCheckMs: number | null;
};

function stillCurrent(database: Database.Database, check: CaptureCoverageCheck) {
  const epoch = currentEpoch(database);
  return epoch !== null && epoch.workspaceId === check.workspaceId &&
    epoch.installationEpochId === check.installationEpochId;
}

function frontierState(database: Database.Database, check: Pick<CaptureCoverageCheck, "workspaceId" | "installationEpochId" | "source">) {
  return database
    .prepare(
      `select checked_at as checkedAt, complete_through as completeThrough from capture_coverage_state
       where workspace_id = ? and installation_epoch_id = ? and source = ?`,
    )
    .get(check.workspaceId, check.installationEpochId, check.source) as
    | { checkedAt: string; completeThrough: string | null }
    | undefined;
}

/**
 * Start a coverage check of one source at `startedAt`, taken before its walk
 * lists anything. Null when there is no epoch, or the check is not newer than
 * the last complete one.
 */
export function beginCaptureCoverage(
  database: Database.Database,
  source: CaptureFrontierSource,
  startedAt: string,
): CaptureCoverageCheck | null {
  const epoch = currentEpoch(database);
  if (!epoch || !canonical(startedAt)) return null;
  ensureCaptureFrontierSchema(database);
  const startedMs = Date.parse(startedAt);
  const state = frontierState(database, { ...epoch, source });
  const lastCheckMs = state ? Date.parse(state.checkedAt) : null;
  if (lastCheckMs !== null && !(startedMs > lastCheckMs)) return null;
  return {
    source,
    workspaceId: epoch.workspaceId,
    installationEpochId: epoch.installationEpochId,
    epochStartMs: Date.parse(epoch.epochStartedAt),
    startedAt,
    startedMs,
    lastCheckMs,
  };
}

/** Record what a walk found for some of a source's files, in one short write. */
export function applyCaptureCoverage(
  database: Database.Database,
  check: CaptureCoverageCheck,
  files: readonly CaptureCoverageFile[],
) {
  if (files.length === 0) return;
  const scope = [check.workspaceId, check.installationEpochId, check.source] as const;
  const run = database.transaction(() => {
    if (!stillCurrent(database, check)) return;
    const read = database.prepare(
      `select uncovered_since as since, last_write_at as lastWriteAt, seen_extent as seenExtent, seen_at as seenAt
       from capture_uncovered_files
       where workspace_id = ? and installation_epoch_id = ? and source = ? and file_key = ?`,
    );
    const remove = database.prepare(
      `delete from capture_uncovered_files
       where workspace_id = ? and installation_epoch_id = ? and source = ? and file_key = ?`,
    );
    const upsert = database.prepare(
      `insert into capture_uncovered_files
         (workspace_id, installation_epoch_id, source, file_key, uncovered_since, last_write_at, seen_extent, seen_at)
       values (?, ?, ?, ?, ?, ?, ?, ?)
       on conflict (workspace_id, installation_epoch_id, source, file_key) do update set
         uncovered_since = excluded.uncovered_since, last_write_at = excluded.last_write_at,
         seen_extent = excluded.seen_extent, seen_at = excluded.seen_at`,
    );
    for (const file of files) {
      const row = read.get(...scope, file.key) as
        | { since: string; lastWriteAt: string; seenExtent: number; seenAt: string }
        | undefined;
      if (file.link) {
        // Nothing behind a link is read: uncovered from its creation, within
        // the epoch and no later than this first sight, to this walk. No
        // other rule applies, and none removes the row.
        const linkBorn = Number.isFinite(file.birthtimeMs) && file.birthtimeMs > 0 ? file.birthtimeMs : check.epochStartMs;
        const since = row ? row.since : isoMs(Math.min(check.startedMs, Math.max(check.epochStartMs, linkBorn)));
        const seenMs = Math.max(file.mtimeMs, row ? Date.parse(row.lastWriteAt) : 0);
        upsert.run(...scope, file.key, since, isoMs(seenMs), -1, check.startedAt);
        continue;
      }
      if (file.fullyRead || file.mtimeMs < check.epochStartMs) {
        if (row) remove.run(...scope, file.key);
        continue;
      }
      const born = Number.isFinite(file.birthtimeMs) && file.birthtimeMs > 0 ? file.birthtimeMs : null;
      let sinceMs: number;
      if (row) {
        sinceMs = Date.parse(row.since);
        // The tailer has since read everything the file held when a walk last
        // saw it, so what is unread now was written after that visit.
        if (file.progress >= row.seenExtent) sinceMs = Math.max(sinceMs, Date.parse(row.seenAt));
      } else if (check.lastCheckMs !== null && (born === null || born <= check.lastCheckMs) &&
        file.mtimeMs > check.lastCheckMs) {
        // Present at the last complete check without a row, the file was
        // covered then (read to its end, or untouched since the epoch began),
        // so everything unread now was written after it (review r3, S4).
        sinceMs = check.lastCheckMs;
      } else {
        // Not seen covered: unread since it was created, within the epoch.
        sinceMs = Math.max(check.epochStartMs, born ?? check.epochStartMs);
      }
      const lastWriteMs = Math.max(file.mtimeMs, row ? Date.parse(row.lastWriteAt) : 0);
      upsert.run(...scope, file.key, isoMs(sinceMs), isoMs(lastWriteMs), file.extent, check.startedAt);
    }
  });
  run.immediate();
}

/**
 * The walk finished: move the source's frontier to its earliest hold, or the
 * check's start, less the write lag. A file seen in this walk holds it while
 * it has been unread for at most the hold limit; any other uncovered file,
 * including one no longer listed, is a gap and holds nothing. Returns the
 * source's frontier (null while nothing is attested).
 */
export function finishCaptureCoverage(database: Database.Database, check: CaptureCoverageCheck): string | null {
  const run = database.transaction(() => {
    const state = frontierState(database, check);
    const previous = state?.completeThrough ?? null;
    if (!stillCurrent(database, check) || (state && !(check.startedMs > Date.parse(state.checkedAt)))) return previous;
    const hold = database
      .prepare(
        `select min(uncovered_since) as since from capture_uncovered_files
         where workspace_id = ? and installation_epoch_id = ? and source = ?
           and seen_at = ? and uncovered_since >= ?`,
      )
      .get(check.workspaceId, check.installationEpochId, check.source, check.startedAt,
        isoMs(check.startedMs - CAPTURE_HOLD_LIMIT_MS)) as { since: string | null };
    const bound = hold.since === null ? check.startedMs : Math.min(check.startedMs, Date.parse(hold.since));
    const candidateMs = bound - CAPTURE_WRITE_LAG_MS;
    const next = candidateMs < check.epochStartMs || (previous !== null && candidateMs <= Date.parse(previous))
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
      .run(check.workspaceId, check.installationEpochId, check.source, check.startedAt, next, new Date().toISOString());
    return next;
  });
  return run.immediate();
}

/**
 * One whole coverage check from a finished walk's files, for callers that
 * hold them all at once. An incomplete walk, or one not newer than the last,
 * changes nothing. Returns the source's frontier.
 */
export function advanceCaptureFrontier(
  database: Database.Database,
  source: CaptureFrontierSource,
  snapshot: CaptureCoverageSnapshot,
  startedAt: string,
): string | null {
  const epoch = currentEpoch(database);
  if (!epoch) return null;
  const check = snapshot.complete ? beginCaptureCoverage(database, source, startedAt) : null;
  if (!check) {
    return tableExists(database, "capture_coverage_state")
      ? frontierState(database, { ...epoch, source })?.completeThrough ?? null
      : null;
  }
  for (let index = 0; index < snapshot.files.length; index += CAPTURE_COVERAGE_BATCH) {
    applyCaptureCoverage(database, check, snapshot.files.slice(index, index + CAPTURE_COVERAGE_BATCH));
  }
  return finishCaptureCoverage(database, check);
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
