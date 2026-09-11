import type Database from "better-sqlite3";

import type { RolloutScanResult } from "./rollout-tailer";
import type { TranscriptScanResult } from "./transcript-tailer";

/** Native finance provenance is scoped to the current workspace epoch. */
export const FINANCE_COVERAGE_SOURCES = ["codex", "claude_code"] as const;
export type FinanceCoverageSource = (typeof FINANCE_COVERAGE_SOURCES)[number];

export type FinanceCoverageMutationRow = {
  source: FinanceCoverageSource;
  workspaceId: string | null;
  installationEpochId: string | null;
  observedAt: string;
};

export type FinanceSourceCoverageRow = {
  workspaceId: string;
  installationEpochId: string;
  source: FinanceCoverageSource;
  retainedFrom: string;
  coveredThrough: string | null;
  latestFullAttemptAt: string | null;
  latestFullComplete: number;
  invalidatedAt: string | null;
  lastScanAt: string | null;
  lastScanOk: number;
  lastScanTruncated: number;
  stateRevision: number;
  publishedRevision: number;
};

/**
 * Create the native finance provenance tables without relabeling legacy data.
 * The singleton control starts dirty so a new or upgraded ledger cannot be
 * read as a published finance snapshot until a native maintenance pass settles.
 */
export function ensureFinanceProvenanceSchema(database: Database.Database): void {
  database.exec(`
    create table if not exists finance_source_coverage (
      workspace_id text not null,
      installation_epoch_id text not null,
      source text not null check (source in ('codex','claude_code')),
      retained_from text not null,
      covered_through text,
      latest_full_attempt_at text,
      latest_full_complete integer not null default 0 check (latest_full_complete in (0,1)),
      invalidated_at text,
      last_scan_at text,
      last_scan_ok integer not null default 0 check (last_scan_ok in (0,1)),
      last_scan_truncated integer not null default 0 check (last_scan_truncated in (0,1)),
      state_revision integer not null default 0 check (state_revision >= 0),
      published_revision integer not null default 0 check (published_revision >= 0),
      primary key (workspace_id, installation_epoch_id, source)
    ) without rowid;
    create index if not exists idx_finance_source_coverage_scope
      on finance_source_coverage (workspace_id, installation_epoch_id, source);
    create table if not exists finance_publication_control (
      singleton integer primary key check (singleton = 1),
      dirty integer not null default 1 check (dirty in (0,1)),
      revision integer not null default 0 check (revision >= 0),
      workspace_id text,
      installation_epoch_id text,
      projection_generation integer,
      published_at text,
      updated_at text not null
    ) without rowid;
  `);
  database.prepare(
    `insert into finance_publication_control (singleton, dirty, revision, updated_at)
     values (1, 1, 0, ?) on conflict(singleton) do nothing`,
  ).run(new Date().toISOString());
}

function currentFinanceScope(database: Database.Database): {
  workspaceId: string;
  installationEpochId: string;
  epochStartedAt: string;
} | null {
  const row = database.prepare(
    `select current_workspace_id as workspaceId,
       current_installation_epoch_id as installationEpochId,
       current_installation_epoch_started_at as epochStartedAt
     from collector_workspace_binding where singleton = 1`,
  ).get() as {
    workspaceId: string;
    installationEpochId: string | null;
    epochStartedAt: string | null;
  } | undefined;
  if (!row || !row.workspaceId || !row.installationEpochId || !row.epochStartedAt) return null;
  return {
    workspaceId: row.workspaceId,
    installationEpochId: row.installationEpochId,
    epochStartedAt: row.epochStartedAt,
  };
}

/** A scan receipt must belong to the currently active epoch and not be future-dated. */
export function financeScanTimeIsCurrent(database: Database.Database, scanAt: string): boolean {
  const scanMs = Date.parse(scanAt);
  if (!Number.isFinite(scanMs) || new Date(scanMs).toISOString() !== scanAt || scanMs > Date.now()) return false;
  const scope = currentFinanceScope(database);
  if (!scope) return true;
  const epochStartedMs = Date.parse(scope.epochStartedAt);
  return Number.isFinite(epochStartedMs) && scanMs >= epochStartedMs;
}

/** Mark the one native finance publication as unavailable until republished. */
export function markFinancePublicationDirty(
  database: Database.Database,
  updatedAt = new Date().toISOString(),
): void {
  if (!database.prepare(
    `select 1 from sqlite_master where type='table' and name='finance_publication_control'`,
  ).get()) return;
  database.prepare(
    `update finance_publication_control set dirty=1, updated_at=? where singleton=1`,
  ).run(updatedAt);
}

/** Initialize both source rows for a newly created installation epoch. */
export function initializeFinanceSourceCoverage(
  database: Database.Database,
  workspaceId: string,
  installationEpochId: string,
  epochStartedAt: string,
): void {
  ensureFinanceProvenanceSchema(database);
  const insert = database.prepare(
    `insert into finance_source_coverage
       (workspace_id, installation_epoch_id, source, retained_from,
        covered_through, latest_full_attempt_at, latest_full_complete,
        invalidated_at, last_scan_at, last_scan_ok, last_scan_truncated,
        state_revision, published_revision)
     values (?, ?, ?, ?, null, null, 0, null, null, 0, 0, 0, 0)
     on conflict(workspace_id, installation_epoch_id, source) do nothing`,
  );
  let initialized = false;
  for (const source of FINANCE_COVERAGE_SOURCES) {
    initialized = insert.run(workspaceId, installationEpochId, source, epochStartedAt).changes > 0 || initialized;
  }
  if (initialized) markFinancePublicationDirty(database, epochStartedAt);
}

function withFinanceMutationTransaction<T>(database: Database.Database, run: () => T): T {
  if (database.inTransaction) return run();
  return database.transaction(run).immediate();
}

function financeCoverageRow(
  database: Database.Database,
  source: FinanceCoverageSource,
  scope = currentFinanceScope(database),
): FinanceSourceCoverageRow | null {
  if (!scope) return null;
  return (database.prepare(
    `select workspace_id as workspaceId, installation_epoch_id as installationEpochId,
       source, retained_from as retainedFrom, covered_through as coveredThrough,
       latest_full_attempt_at as latestFullAttemptAt,
       latest_full_complete as latestFullComplete, invalidated_at as invalidatedAt,
       last_scan_at as lastScanAt, last_scan_ok as lastScanOk,
       last_scan_truncated as lastScanTruncated, state_revision as stateRevision,
       published_revision as publishedRevision
     from finance_source_coverage
     where workspace_id=? and installation_epoch_id=? and source=?`,
  ).get(scope.workspaceId, scope.installationEpochId, source) as FinanceSourceCoverageRow | undefined) ?? null;
}

function nextCoverageRevision(database: Database.Database, row: FinanceSourceCoverageRow | null): number {
  return (row?.stateRevision ?? 0) + 1;
}

/** Record a full history attempt without erasing an earlier watermark on failure. */
export function recordFinanceFullHistoryAttempt(
  database: Database.Database,
  source: FinanceCoverageSource,
  attemptedAt: string,
  successful: boolean,
): void {
  withFinanceMutationTransaction(database, () => {
    const scope = currentFinanceScope(database);
    if (!scope) return;
    if (!financeScanTimeIsCurrent(database, attemptedAt)) {
      // A completion receipt that began before an epoch transition cannot
      // advance or clear the current epoch's native coverage.
      markFinancePublicationDirty(database, new Date().toISOString());
      return;
    }
    initializeFinanceSourceCoverage(database, scope.workspaceId, scope.installationEpochId, scope.epochStartedAt);
    const previous = financeCoverageRow(database, source, scope);
    const coveredThrough = successful
      ? [previous?.coveredThrough, attemptedAt].filter((value): value is string => Boolean(value))
        .sort((a, b) => Date.parse(a) - Date.parse(b)).at(-1) ?? attemptedAt
      : previous?.coveredThrough ?? null;
    database.prepare(
      `update finance_source_coverage set
         covered_through=?, latest_full_attempt_at=?, latest_full_complete=?,
         invalidated_at=case when ? then null else invalidated_at end,
         state_revision=?, last_scan_at=?, last_scan_ok=?, last_scan_truncated=0
       where workspace_id=? and installation_epoch_id=? and source=?`,
    ).run(
      coveredThrough,
      attemptedAt,
      successful ? 1 : 0,
      successful ? 1 : 0,
      nextCoverageRevision(database, previous),
      attemptedAt,
      successful ? 1 : 0,
      scope.workspaceId,
      scope.installationEpochId,
      source,
    );
    markFinancePublicationDirty(database, attemptedAt);
  });
}

/** Capture scans update health only; they never advance historical coverage. */
export function recordFinanceCaptureActivity(
  database: Database.Database,
  source: FinanceCoverageSource,
  scanAt: string,
  ok: boolean,
  truncated: boolean,
): boolean {
  if (!financeScanTimeIsCurrent(database, scanAt)) return false;
  withFinanceMutationTransaction(database, () => {
    const scope = currentFinanceScope(database);
    if (!scope) return;
    initializeFinanceSourceCoverage(database, scope.workspaceId, scope.installationEpochId, scope.epochStartedAt);
    const previous = financeCoverageRow(database, source, scope);
    database.prepare(
      `update finance_source_coverage set last_scan_at=?, last_scan_ok=?,
         last_scan_truncated=?, state_revision=?
       where workspace_id=? and installation_epoch_id=? and source=?`,
    ).run(
      scanAt,
      ok ? 1 : 0,
      truncated ? 1 : 0,
      nextCoverageRevision(database, previous),
      scope.workspaceId,
      scope.installationEpochId,
      source,
    );
    markFinancePublicationDirty(database, scanAt);
  });
  return true;
}

/** Preserve the old watermark while making exclusion growth a native hold. */
export function invalidateFinanceSourceCoverage(
  database: Database.Database,
  source: FinanceCoverageSource,
  invalidatedAt: string,
): void {
  withFinanceMutationTransaction(database, () => {
    const scope = currentFinanceScope(database);
    if (!scope) return;
    initializeFinanceSourceCoverage(database, scope.workspaceId, scope.installationEpochId, scope.epochStartedAt);
    const previous = financeCoverageRow(database, source, scope);
    database.prepare(
      `update finance_source_coverage set invalidated_at=?, state_revision=?
       where workspace_id=? and installation_epoch_id=? and source=?`,
    ).run(
      invalidatedAt,
      nextCoverageRevision(database, previous),
      scope.workspaceId,
      scope.installationEpochId,
      source,
    );
    markFinancePublicationDirty(database, invalidatedAt);
  });
}

/** Advance retention from deleted rows only; never infer it from survivors. */
export function advanceFinanceRetentionWatermarks(
  database: Database.Database,
  deletedRows: readonly FinanceCoverageMutationRow[],
  updatedAt = new Date().toISOString(),
): void {
  if (deletedRows.length === 0) return;
  withFinanceMutationTransaction(database, () => {
    const grouped = new Map<string, FinanceCoverageMutationRow>();
    for (const row of deletedRows) {
      if (!row.workspaceId || !row.installationEpochId || !FINANCE_COVERAGE_SOURCES.includes(row.source)) continue;
      const key = `${row.workspaceId}\u0000${row.installationEpochId}\u0000${row.source}`;
      const previous = grouped.get(key);
      if (!previous || Date.parse(row.observedAt) > Date.parse(previous.observedAt)) grouped.set(key, row);
    }
    for (const row of grouped.values()) {
      const observedMs = Date.parse(row.observedAt);
      if (!Number.isFinite(observedMs) || observedMs >= 8_640_000_000_000_000) continue;
      const nextRetained = new Date(observedMs + 1).toISOString();
      const current = database.prepare(
        `select retained_from as retainedFrom, state_revision as stateRevision
         from finance_source_coverage
         where workspace_id=? and installation_epoch_id=? and source=?`,
      ).get(row.workspaceId, row.installationEpochId, row.source) as {
        retainedFrom: string;
        stateRevision: number;
      } | undefined;
      if (!current || Date.parse(nextRetained) <= Date.parse(current.retainedFrom)) continue;
      database.prepare(
        `update finance_source_coverage set retained_from=?, state_revision=?
         where workspace_id=? and installation_epoch_id=? and source=?`,
      ).run(
        nextRetained,
        current.stateRevision + 1,
        row.workspaceId,
        row.installationEpochId,
        row.source,
      );
    }
    markFinancePublicationDirty(database, updatedAt);
  });
}

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
  if (!financeScanTimeIsCurrent(database, invalidatedAt)) return false;
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
  const persist = () => {
    database
      .prepare(
        `insert into maintenance_state (key, value, updated_at)
         values (?, ?, ?)
         on conflict(key) do update set
           value = excluded.value,
           updated_at = excluded.updated_at`,
      )
      .run(coverageKey(source), JSON.stringify(marker), invalidatedAt);
    invalidateFinanceSourceCoverage(database, source, invalidatedAt);
  };
  if (database.inTransaction) persist();
  else database.transaction(persist).immediate();
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
  if (!financeScanTimeIsCurrent(database, attemptedAt)) {
    return { promoted: false, coverage: historyCoverageStatus(database) };
  }
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
    recordFinanceFullHistoryAttempt(database, source, attemptedAt, successful);
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
