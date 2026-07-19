import type Database from "better-sqlite3";

import type { RolloutScanResult } from "./rollout-tailer";
import type { TranscriptScanResult } from "./transcript-tailer";

export const EXPLICIT_FULL_BACKFILL_NOT_COMPLETED =
  "explicit_full_backfill_not_completed" as const;
export const EXPLICIT_FULL_SCAN_NOT_EXHAUSTIVE =
  "explicit_full_scan_not_exhaustive" as const;

export type HistoryCoverageSource = "codex" | "claude_code";

type FullScanCounters = {
  filesSeen: number;
  filesRead: number;
  bytesRead: number;
  bytesDeferred: number;
  eventsAppended: number;
  parseErrors: number;
  discoveryErrors: number;
  statErrors: number;
  readErrors: number;
};

type CompletedFullScan = FullScanCounters & {
  completedAt: string;
};

export type LatestFullScanAttempt = FullScanCounters & {
  attemptedAt: string;
  status: "complete" | "incomplete";
  reason: typeof EXPLICIT_FULL_SCAN_NOT_EXHAUSTIVE | null;
  exhaustive: boolean;
  truncated: boolean;
};

type PersistedHistoryCoverage = {
  version: 2;
  source: HistoryCoverageSource;
  completion: CompletedFullScan | null;
  latestFullAttempt: LatestFullScanAttempt;
};

export type HistoryCoverageSourceStatus = {
  source: HistoryCoverageSource;
  status: "complete" | "incomplete";
  reason: typeof EXPLICIT_FULL_BACKFILL_NOT_COMPLETED | null;
  completedAt: string | null;
  lastFullScan: CompletedFullScan | null;
  latestFullAttempt: LatestFullScanAttempt | null;
};

export type HistoryCoverageStatus = {
  status: "complete" | "incomplete";
  reason: typeof EXPLICIT_FULL_BACKFILL_NOT_COMPLETED | null;
  sources: HistoryCoverageSourceStatus[];
};

type FullScanResult = RolloutScanResult | TranscriptScanResult;

function coverageKey(source: HistoryCoverageSource) {
  return `history_coverage_v2_${source}`;
}

function incomplete(source: HistoryCoverageSource): HistoryCoverageSourceStatus {
  return {
    source,
    status: "incomplete",
    reason: EXPLICIT_FULL_BACKFILL_NOT_COMPLETED,
    completedAt: null,
    lastFullScan: null,
    latestFullAttempt: null,
  };
}

function nonnegativeCounters(value: Partial<FullScanCounters> | null | undefined) {
  return Boolean(
    value &&
      [
        value.filesSeen,
        value.filesRead,
        value.bytesRead,
        value.bytesDeferred,
        value.eventsAppended,
        value.parseErrors,
        value.discoveryErrors,
        value.statErrors,
        value.readErrors,
      ].every((candidate) => Number.isSafeInteger(candidate) && candidate! >= 0),
  );
}

function parsePersistedCoverage(
  source: HistoryCoverageSource,
  value: string | undefined,
): HistoryCoverageSourceStatus {
  if (!value) return incomplete(source);
  try {
    const parsed = JSON.parse(value) as Partial<PersistedHistoryCoverage>;
    const completion = parsed.completion;
    const attempt = parsed.latestFullAttempt;
    const completionValid =
      completion === null ||
      (typeof completion?.completedAt === "string" && nonnegativeCounters(completion));
    const attemptValid =
      typeof attempt?.attemptedAt === "string" &&
      (attempt.status === "complete" || attempt.status === "incomplete") &&
      (attempt.reason === null || attempt.reason === EXPLICIT_FULL_SCAN_NOT_EXHAUSTIVE) &&
      typeof attempt.exhaustive === "boolean" &&
      typeof attempt.truncated === "boolean" &&
      nonnegativeCounters(attempt);
    if (
      parsed.version !== 2 ||
      parsed.source !== source ||
      !completionValid ||
      !attemptValid
    ) {
      return incomplete(source);
    }
    const completed = completion ?? null;
    return {
      source,
      status: completed ? "complete" : "incomplete",
      reason: completed ? null : EXPLICIT_FULL_BACKFILL_NOT_COMPLETED,
      completedAt: completed?.completedAt ?? null,
      lastFullScan: completed,
      latestFullAttempt: attempt!,
    };
  } catch {
    return incomplete(source);
  }
}

function readSourceCoverage(
  database: Database.Database,
  source: HistoryCoverageSource,
): HistoryCoverageSourceStatus {
  const row = database
    .prepare(`select value from maintenance_state where key = ?`)
    .get(coverageKey(source)) as { value: string } | undefined;
  return parsePersistedCoverage(source, row?.value);
}

/**
 * Historical coverage is deliberately independent from current capture
 * health. A missing or malformed marker is incomplete; recent tailing never
 * creates this marker.
 */
export function historyCoverageStatus(database: Database.Database): HistoryCoverageStatus {
  const sources: HistoryCoverageSourceStatus[] = [
    readSourceCoverage(database, "codex"),
    readSourceCoverage(database, "claude_code"),
  ];
  const complete = sources.every((source) => source.status === "complete");
  return {
    status: complete ? "complete" : "incomplete",
    reason: complete ? null : EXPLICIT_FULL_BACKFILL_NOT_COMPLETED,
    sources,
  };
}

function scanCounters(result: FullScanResult): FullScanCounters {
  return {
    filesSeen: result.filesSeen,
    filesRead: result.filesRead,
    bytesRead: result.bytesRead,
    bytesDeferred: result.bytesDeferred,
    eventsAppended: result.eventsAppended,
    parseErrors: result.parseErrors,
    discoveryErrors: result.discoveryErrors,
    statErrors: result.statErrors,
    readErrors: result.readErrors,
  };
}

/**
 * Persist every literal full attempt, but promote completion only for an
 * exhaustive, non-truncated, error-free receipt with no deferred JSONL bytes.
 * A later failed attempt remains visible without erasing an earlier completion.
 */
export function recordExplicitFullHistoryCoverage(
  database: Database.Database,
  source: HistoryCoverageSource,
  result: FullScanResult,
): { promoted: boolean; coverage: HistoryCoverageStatus } {
  if (result.scope !== "full") {
    throw new Error("history_coverage_requires_explicit_full_scan");
  }
  const attemptedAt = result.activity.lastScanAt;
  const successful =
    result.exhaustive &&
    !result.activity.truncated &&
    result.bytesDeferred === 0 &&
    result.parseErrors === 0 &&
    result.discoveryErrors === 0 &&
    result.statErrors === 0 &&
    result.readErrors === 0;
  const counters = scanCounters(result);
  const previous = readSourceCoverage(database, source);
  const completion: CompletedFullScan | null = successful
    ? { completedAt: attemptedAt, ...counters }
    : previous.lastFullScan;
  const latestFullAttempt: LatestFullScanAttempt = {
    attemptedAt,
    status: successful ? "complete" : "incomplete",
    reason: successful ? null : EXPLICIT_FULL_SCAN_NOT_EXHAUSTIVE,
    exhaustive: result.exhaustive,
    truncated: result.activity.truncated,
    ...counters,
  };
  const marker: PersistedHistoryCoverage = {
    version: 2,
    source,
    completion,
    latestFullAttempt,
  };
  database
    .prepare(
      `insert into maintenance_state (key, value, updated_at)
       values (?, ?, ?)
       on conflict(key) do update set
         value = excluded.value,
         updated_at = excluded.updated_at`,
    )
    .run(coverageKey(source), JSON.stringify(marker), attemptedAt);
  return { promoted: successful, coverage: historyCoverageStatus(database) };
}
