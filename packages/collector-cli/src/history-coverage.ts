import type Database from "better-sqlite3";

import type { RolloutScanResult } from "./rollout-tailer";
import type { TranscriptScanResult } from "./transcript-tailer";

export const EXPLICIT_FULL_BACKFILL_NOT_COMPLETED =
  "explicit_full_backfill_not_completed" as const;
export const EXPLICIT_FULL_SCAN_NOT_EXHAUSTIVE =
  "explicit_full_scan_not_exhaustive" as const;
export const EXCLUDED_GENERATION_GROWTH_INVALIDATED =
  "excluded_generation_grew_after_completion" as const;

export type HistoryCoverageIncompleteReason =
  | typeof EXPLICIT_FULL_BACKFILL_NOT_COMPLETED
  | typeof EXCLUDED_GENERATION_GROWTH_INVALIDATED;

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

export type HistoryCoverageInvalidation = {
  reason: typeof EXCLUDED_GENERATION_GROWTH_INVALIDATED;
  invalidatedAt: string;
};

export type LatestFullScanAttempt = FullScanCounters & {
  attemptedAt: string;
  status: "complete" | "incomplete";
  reason: typeof EXPLICIT_FULL_SCAN_NOT_EXHAUSTIVE | null;
  exhaustive: boolean;
  truncated: boolean;
};

type PersistedHistoryCoverage = {
  version: 2 | 3;
  source: HistoryCoverageSource;
  completion: CompletedFullScan | null;
  latestFullAttempt: LatestFullScanAttempt;
  invalidation?: HistoryCoverageInvalidation | null;
};

export type HistoryCoverageSourceStatus = {
  source: HistoryCoverageSource;
  status: "complete" | "incomplete";
  reason: HistoryCoverageIncompleteReason | null;
  completedAt: string | null;
  invalidatedAt: string | null;
  lastFullScan: CompletedFullScan | null;
  latestFullAttempt: LatestFullScanAttempt | null;
};

export type HistoryCoverageStatus = {
  status: "complete" | "incomplete";
  reason: HistoryCoverageIncompleteReason | null;
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
    invalidatedAt: null,
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
    const invalidation = parsed.version === 3 ? parsed.invalidation ?? null : null;
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
    const invalidationValid =
      invalidation === null ||
      (invalidation.reason === EXCLUDED_GENERATION_GROWTH_INVALIDATED &&
        typeof invalidation.invalidatedAt === "string");
    if (
      (parsed.version !== 2 && parsed.version !== 3) ||
      parsed.source !== source ||
      !completionValid ||
      !attemptValid ||
      !invalidationValid ||
      (invalidation !== null && completion === null)
    ) {
      return incomplete(source);
    }
    const completed = completion ?? null;
    const valid = completed !== null && invalidation === null;
    return {
      source,
      status: valid ? "complete" : "incomplete",
      reason: valid
        ? null
        : invalidation?.reason ?? EXPLICIT_FULL_BACKFILL_NOT_COMPLETED,
      completedAt: completed?.completedAt ?? null,
      invalidatedAt: invalidation?.invalidatedAt ?? null,
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
  const missingCompletion = sources.some((source) => source.lastFullScan === null);
  return {
    status: complete ? "complete" : "incomplete",
    reason: complete
      ? null
      : missingCompletion
        ? EXPLICIT_FULL_BACKFILL_NOT_COMPLETED
        : EXCLUDED_GENERATION_GROWTH_INVALIDATED,
    sources,
  };
}

/**
 * A same-generation append remains excluded from automatic capture, so it
 * invalidates any prior exhaustive-history claim. The completion receipt is
 * retained; status exposes the reason/time until another explicit full scan
 * succeeds.
 */
export function invalidateHistoryCoverageForExcludedGrowth(
  database: Database.Database,
  source: HistoryCoverageSource,
  invalidatedAt: string,
): boolean {
  const previous = readSourceCoverage(database, source);
  if (!previous.lastFullScan || previous.invalidatedAt) return false;
  if (!Number.isFinite(Date.parse(invalidatedAt))) {
    throw new Error("history_coverage_invalid_invalidation_time");
  }
  const marker: PersistedHistoryCoverage = {
    version: 3,
    source,
    completion: previous.lastFullScan,
    latestFullAttempt: previous.latestFullAttempt!,
    invalidation: {
      reason: EXCLUDED_GENERATION_GROWTH_INVALIDATED,
      invalidatedAt,
    },
  };
  database
    .prepare(
      `insert into maintenance_state (key, value, updated_at)
       values (?, ?, ?)
       on conflict(key) do update set
         value = excluded.value,
         updated_at = excluded.updated_at`,
    )
    .run(coverageKey(source), JSON.stringify(marker), invalidatedAt);
  return true;
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
    version: 3,
    source,
    completion,
    latestFullAttempt,
    // A failed repair preserves the literal invalidation. Only a successful,
    // exhaustive explicit scan may restore complete history truth.
    invalidation: successful
      ? null
      : previous.invalidatedAt
        ? {
            reason: EXCLUDED_GENERATION_GROWTH_INVALIDATED,
            invalidatedAt: previous.invalidatedAt,
          }
        : null,
  };
  const persist = database.transaction(() => {
    database
      .prepare(
        `insert into maintenance_state (key, value, updated_at)
         values (?, ?, ?)
         on conflict(key) do update set
           value = excluded.value,
           updated_at = excluded.updated_at`,
      )
      .run(coverageKey(source), JSON.stringify(marker), attemptedAt);
    if (successful) {
      // Keep every exclusion, but acknowledge all same-generation bytes the
      // successful full scan observed. The table may not exist on ledgers that
      // predate automatic baselining.
      const baselineTable = database
        .prepare(
          `select 1 from sqlite_master
           where type = 'table' and name = 'automatic_capture_baseline_generations'`,
        )
        .get();
      if (baselineTable) {
        database
          .prepare(
          `update automatic_capture_baseline_generations
             set history_covered_size = max(history_covered_size, last_observed_size),
               history_covered_at = ?
             where source = ? and run_id = (
               select run_id from automatic_capture_baseline_state
               where source = ? and status = 'complete'
             )`,
          )
          .run(attemptedAt, source, source);
      }
    }
  });
  persist.immediate();
  return { promoted: successful, coverage: historyCoverageStatus(database) };
}
