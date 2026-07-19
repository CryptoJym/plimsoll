import crypto from "node:crypto";
import path from "node:path";

import type Database from "better-sqlite3";

import {
  invalidateHistoryCoverageForExcludedGrowth,
  type HistoryCoverageSource,
} from "./history-coverage";

const STATE_TABLE = "automatic_capture_baseline_state";
const GENERATION_TABLE = "automatic_capture_baseline_generations";
const PENDING_GENERATION_TABLE = "automatic_capture_baseline_pending_generations";
const ERROR_TABLE = "automatic_capture_baseline_observation_errors";
const SCHEMA_VERSION = 2;
const initializedDatabases = new WeakSet<object>();

/** Automatic discovery holds at most one small metadata chunk per source. */
export const AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP = 64;
export const AUTOMATIC_DISCOVERY_ENTRY_CAP = 256;
export const AUTOMATIC_DISCOVERY_WALL_MS = 50;

export const CAPTURE_BASELINE_NOT_ESTABLISHED =
  "capture_baseline_not_established" as const;
export const CAPTURE_BASELINE_IN_PROGRESS = "capture_baseline_in_progress" as const;
export const CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS =
  "capture_baseline_discovery_ambiguous" as const;
export const CAPTURE_BASELINE_STAT_AMBIGUOUS =
  "capture_baseline_stat_ambiguous" as const;
export const CAPTURE_BASELINE_STATE_INVALID = "capture_baseline_state_invalid" as const;
export const CAPTURE_BASELINE_GENERATION_AMBIGUOUS =
  "capture_baseline_generation_ambiguous" as const;

export type CaptureBaselineFailureReason =
  | typeof CAPTURE_BASELINE_NOT_ESTABLISHED
  | typeof CAPTURE_BASELINE_IN_PROGRESS
  | typeof CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS
  | typeof CAPTURE_BASELINE_STAT_AMBIGUOUS
  | typeof CAPTURE_BASELINE_STATE_INVALID
  | typeof CAPTURE_BASELINE_GENERATION_AMBIGUOUS;

/**
 * A caller obtains these fields from one lstat/stat receipt. The registry
 * deliberately accepts metadata rather than a file handle or contents: first
 * install must never open a pre-existing transcript merely to exclude it.
 *
 * `birthtimeNs` is preferred when the caller used bigint stats. `birthtimeMs`
 * is the portable Node Stats fallback. Device + inode + birth time form a
 * stable generation identity across append growth while distinguishing a
 * replacement at the same path.
 */
export type CaptureBaselineFileObservation = {
  path: string;
  device: number | bigint;
  inode: number | bigint;
  size: number | bigint;
  birthtimeNs?: bigint;
  birthtimeMs?: number;
};

export type CaptureBaselineRunProgress = {
  runId: string;
  source: HistoryCoverageSource;
  status: "in_progress" | "complete" | "failed";
  reason: CaptureBaselineFailureReason | null;
  startedAt: string;
  updatedAt: string;
  completedAt: string | null;
  errorAt: string | null;
  filesDiscovered: number;
  filesValidated: number;
  filesBaselined: number;
  discoveryErrors: number;
  statErrors: number;
};

export type CaptureBaselineSourceStatus = {
  source: HistoryCoverageSource;
  status: "not_established" | "in_progress" | "complete" | "failed";
  reason: CaptureBaselineFailureReason | null;
  latestRun: CaptureBaselineRunProgress | null;
  excludedGenerations: number;
  excludedBaselineBytes: number;
  currentExcludedBytes: number;
  generationsWithObservedGrowth: number;
  lastGrowthObservedAt: string | null;
  unresolvedObservationErrors: number;
  lastObservationErrorAt: string | null;
};

export type CaptureBaselineStatus = {
  status: "complete" | "blocked";
  reason: CaptureBaselineFailureReason | null;
  progress: {
    state: "not_established" | "in_progress" | "complete" | "failed" | "ambiguous";
    sourcesComplete: number;
    sourcesInProgress: number;
    sourcesFailed: number;
    filesDiscovered: number;
    filesValidated: number;
    filesBaselined: number;
    pendingMetadata: number;
    pendingMetadataPerSourceCap: number;
    pendingMetadataAggregateCap: number;
    deferredSources: number;
  };
  sources: CaptureBaselineSourceStatus[];
};

export type BeginCaptureBaselineInput = {
  startedAt: string;
  filesDiscovered: number;
  discoveryErrors?: number;
  runId?: string;
};

export type CaptureBaselineProgressInput = {
  runId: string;
  updatedAt: string;
  filesDiscovered?: number;
  filesValidated: number;
  discoveryErrors?: number;
  statErrors?: number;
};

export type CompleteCaptureBaselineInput = {
  runId: string;
  completedAt: string;
  observations?: CaptureBaselineFileObservation[];
  discoveryErrors?: number;
  statErrors?: number;
};

export type StageCaptureBaselineObservationInput = {
  runId: string;
  observedAt: string;
  observation: CaptureBaselineFileObservation;
  filesDiscovered: number;
  filesValidated: number;
  resolvePending?: boolean;
};

export type StageCaptureBaselinePendingInput = {
  runId: string;
  observedAt: string;
  observations: CaptureBaselineFileObservation[];
};

export type CaptureBaselineDecision =
  | {
      decision: "capture";
      reason: "generation_not_baselined" | "explicit_full_scan";
      matchedExcludedGeneration: boolean;
      observedGrowth: boolean;
      historyInvalidated: boolean;
    }
  | {
      decision: "exclude";
      reason: "preexisting_generation";
      matchedExcludedGeneration: true;
      observedGrowth: boolean;
      historyInvalidated: boolean;
    }
  | {
      decision: "block";
      reason: CaptureBaselineFailureReason;
      matchedExcludedGeneration: boolean;
      observedGrowth: false;
      historyInvalidated: false;
    };

type StoredState = {
  version: number;
  runId: string;
  status: string;
  startedAt: string;
  updatedAt: string;
  completedAt: string | null;
  filesDiscovered: number;
  filesValidated: number;
  filesBaselined: number;
  discoveryErrors: number;
  statErrors: number;
  errorCode: string | null;
  errorAt: string | null;
};

type NormalizedObservation = {
  pathKey: string;
  generationKey: string;
  size: number;
};

function validCount(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0;
}

function validTimestamp(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    Number.isFinite(Date.parse(value))
  );
}

function validRunId(value: unknown): value is string {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 128 &&
    /^[A-Za-z0-9._:-]+$/.test(value)
  );
}

function canonicalInteger(value: number | bigint, allowZero: boolean): string | undefined {
  if (typeof value === "bigint") {
    if (value < (allowZero ? 0n : 1n)) return undefined;
    return value.toString(10);
  }
  if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1)) return undefined;
  return String(value);
}

function canonicalBirthTime(observation: CaptureBaselineFileObservation): string | undefined {
  if (typeof observation.birthtimeNs === "bigint" && observation.birthtimeNs > 0n) {
    return `ns:${observation.birthtimeNs.toString(10)}`;
  }
  if (
    typeof observation.birthtimeMs === "number" &&
    Number.isFinite(observation.birthtimeMs) &&
    observation.birthtimeMs > 0
  ) {
    // Stats timestamps can contain a fractional millisecond. A canonical
    // decimal string retains it without locale-dependent formatting.
    return `ms:${String(observation.birthtimeMs)}`;
  }
  return undefined;
}

function normalizeObservation(
  observation: CaptureBaselineFileObservation,
): NormalizedObservation | undefined {
  if (typeof observation.path !== "string" || !path.isAbsolute(observation.path)) {
    return undefined;
  }
  const device = canonicalInteger(observation.device, true);
  const inode = canonicalInteger(observation.inode, false);
  const sizeValue = canonicalInteger(observation.size, true);
  const birthTime = canonicalBirthTime(observation);
  if (!device || !inode || !sizeValue || !birthTime) return undefined;
  const size = Number(sizeValue);
  if (!Number.isSafeInteger(size)) return undefined;

  const canonicalPath = path.normalize(observation.path);
  return {
    pathKey: crypto.createHash("sha256").update(canonicalPath, "utf8").digest("hex"),
    generationKey: crypto
      .createHash("sha256")
      .update(`v1\0${device}\0${inode}\0${birthTime}`, "utf8")
      .digest("hex"),
    size,
  };
}

function stateRow(
  database: Database.Database,
  source: HistoryCoverageSource,
): StoredState | undefined {
  return database
    .prepare(
      `select schema_version as version,
         run_id as runId,
         status,
         started_at as startedAt,
         updated_at as updatedAt,
         completed_at as completedAt,
         files_discovered as filesDiscovered,
         files_validated as filesValidated,
         files_baselined as filesBaselined,
         discovery_errors as discoveryErrors,
         stat_errors as statErrors,
         error_code as errorCode,
         error_at as errorAt
       from ${STATE_TABLE}
       where source = ?`,
    )
    .get(source) as StoredState | undefined;
}

function rowReason(value: string | null): CaptureBaselineFailureReason | null {
  switch (value) {
    case CAPTURE_BASELINE_NOT_ESTABLISHED:
    case CAPTURE_BASELINE_IN_PROGRESS:
    case CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS:
    case CAPTURE_BASELINE_STAT_AMBIGUOUS:
    case CAPTURE_BASELINE_STATE_INVALID:
    case CAPTURE_BASELINE_GENERATION_AMBIGUOUS:
      return value;
    default:
      return null;
  }
}

function stateIsValid(row: StoredState): boolean {
  const reason = rowReason(row.errorCode);
  return (
    row.version === SCHEMA_VERSION &&
    validRunId(row.runId) &&
    (row.status === "in_progress" || row.status === "complete" || row.status === "failed") &&
    validTimestamp(row.startedAt) &&
    validTimestamp(row.updatedAt) &&
    (row.completedAt === null || validTimestamp(row.completedAt)) &&
    (row.errorAt === null || validTimestamp(row.errorAt)) &&
    validCount(row.filesDiscovered) &&
    validCount(row.filesValidated) &&
    validCount(row.filesBaselined) &&
    validCount(row.discoveryErrors) &&
    validCount(row.statErrors) &&
    row.filesValidated <= row.filesDiscovered &&
    row.filesBaselined <= row.filesValidated &&
    (row.status === "complete"
      ? row.completedAt !== null &&
        row.errorCode === null &&
        row.errorAt === null &&
        row.discoveryErrors === 0 &&
        row.statErrors === 0
      : row.completedAt === null) &&
    (row.status === "failed" ? reason !== null && row.errorAt !== null : true)
  );
}

function publicProgress(
  source: HistoryCoverageSource,
  row: StoredState,
): CaptureBaselineRunProgress {
  return {
    runId: row.runId,
    source,
    status: row.status as CaptureBaselineRunProgress["status"],
    reason:
      row.status === "in_progress"
        ? CAPTURE_BASELINE_IN_PROGRESS
        : rowReason(row.errorCode),
    startedAt: row.startedAt,
    updatedAt: row.updatedAt,
    completedAt: row.completedAt,
    errorAt: row.errorAt,
    filesDiscovered: row.filesDiscovered,
    filesValidated: row.filesValidated,
    filesBaselined: row.filesBaselined,
    discoveryErrors: row.discoveryErrors,
    statErrors: row.statErrors,
  };
}

export function ensureCaptureBaselineSchema(database: Database.Database): void {
  if (initializedDatabases.has(database)) return;
  database.exec(`
    create table if not exists ${STATE_TABLE} (
      source text primary key check (source in ('codex', 'claude_code')),
      schema_version integer not null check (schema_version = 2),
      run_id text not null,
      status text not null check (status in ('in_progress', 'complete', 'failed')),
      started_at text not null,
      updated_at text not null,
      completed_at text,
      files_discovered integer not null check (files_discovered >= 0),
      files_validated integer not null check (files_validated >= 0),
      files_baselined integer not null check (files_baselined >= 0),
      discovery_errors integer not null check (discovery_errors >= 0),
      stat_errors integer not null check (stat_errors >= 0),
      error_code text,
      error_at text
    );
    create table if not exists ${GENERATION_TABLE} (
      source text not null check (source in ('codex', 'claude_code')),
      run_id text not null,
      path_key text not null,
      generation_key text not null,
      baseline_size integer not null check (baseline_size >= 0),
      last_observed_size integer not null check (last_observed_size >= 0),
      history_covered_size integer not null check (history_covered_size >= 0),
      baselined_at text not null,
      last_observed_at text not null,
      growth_observed_at text,
      history_covered_at text,
      primary key (source, run_id, generation_key)
    );
    create index if not exists idx_capture_baseline_generation
      on ${GENERATION_TABLE} (source, run_id, generation_key);
    create table if not exists ${PENDING_GENERATION_TABLE} (
      source text not null check (source in ('codex', 'claude_code')),
      run_id text not null,
      path_key text not null,
      generation_key text not null,
      baseline_size integer not null check (baseline_size >= 0),
      discovered_at text not null,
      primary key (source, run_id, path_key, generation_key)
    );
    create index if not exists idx_capture_baseline_pending_run
      on ${PENDING_GENERATION_TABLE} (source, run_id);
    create table if not exists ${ERROR_TABLE} (
      source text not null check (source in ('codex', 'claude_code')),
      path_key text not null,
      error_code text not null,
      first_observed_at text not null,
      last_observed_at text not null,
      occurrence_count integer not null check (occurrence_count > 0),
      resolved_at text,
      primary key (source, path_key, error_code)
    );
    create index if not exists idx_capture_baseline_unresolved_errors
      on ${ERROR_TABLE} (source, resolved_at, last_observed_at);
  `);
  initializedDatabases.add(database);
}

function pendingGenerationCount(
  database: Database.Database,
  source: HistoryCoverageSource,
  runId: string,
): number {
  return (database.prepare(
    `select count(*) as count
     from ${PENDING_GENERATION_TABLE}
     where source = ? and run_id = ?`,
  ).get(source, runId) as { count: number }).count;
}

function sourceStatus(
  database: Database.Database,
  source: HistoryCoverageSource,
): CaptureBaselineSourceStatus {
  ensureCaptureBaselineSchema(database);
  const row = stateRow(database, source);
  const activeRunId = row?.status === "complete" ? row.runId : "__inactive__";
  const counts = database
    .prepare(
      `select count(*) as excludedGenerations,
         coalesce(sum(baseline_size), 0) as excludedBaselineBytes,
         coalesce(sum(last_observed_size), 0) as currentExcludedBytes,
         sum(case when growth_observed_at is not null then 1 else 0 end)
           as generationsWithObservedGrowth,
         max(growth_observed_at) as lastGrowthObservedAt
       from ${GENERATION_TABLE}
       where source = ? and run_id = ?`,
    )
    .get(source, activeRunId) as {
    excludedGenerations: number;
    excludedBaselineBytes: number;
    currentExcludedBytes: number;
    generationsWithObservedGrowth: number | null;
    lastGrowthObservedAt: string | null;
  };
  const errors = database
    .prepare(
      `select count(*) as unresolvedObservationErrors,
         max(last_observed_at) as lastObservationErrorAt
       from ${ERROR_TABLE}
       where source = ? and resolved_at is null`,
    )
    .get(source) as {
    unresolvedObservationErrors: number;
    lastObservationErrorAt: string | null;
  };
  const excludedGenerations = counts.excludedGenerations;
  const growth = counts.generationsWithObservedGrowth ?? 0;
  if (!row) {
    return {
      source,
      status: "not_established",
      reason: CAPTURE_BASELINE_NOT_ESTABLISHED,
      latestRun: null,
      excludedGenerations,
      excludedBaselineBytes: counts.excludedBaselineBytes,
      currentExcludedBytes: counts.currentExcludedBytes,
      generationsWithObservedGrowth: growth,
      lastGrowthObservedAt: counts.lastGrowthObservedAt,
      unresolvedObservationErrors: errors.unresolvedObservationErrors,
      lastObservationErrorAt: errors.lastObservationErrorAt,
    };
  }
  if (!stateIsValid(row) || (row.status === "complete" && row.filesBaselined !== excludedGenerations)) {
    return {
      source,
      status: "failed",
      reason: CAPTURE_BASELINE_STATE_INVALID,
      latestRun: null,
      excludedGenerations,
      excludedBaselineBytes: counts.excludedBaselineBytes,
      currentExcludedBytes: counts.currentExcludedBytes,
      generationsWithObservedGrowth: growth,
      lastGrowthObservedAt: counts.lastGrowthObservedAt,
      unresolvedObservationErrors: errors.unresolvedObservationErrors,
      lastObservationErrorAt: errors.lastObservationErrorAt,
    };
  }
  return {
    source,
    status:
      row.status === "complete"
        ? "complete"
        : row.status === "in_progress"
          ? "in_progress"
          : "failed",
    reason:
      row.status === "complete"
        ? null
        : row.status === "in_progress"
          ? CAPTURE_BASELINE_IN_PROGRESS
          : rowReason(row.errorCode) ?? CAPTURE_BASELINE_STATE_INVALID,
    latestRun: publicProgress(source, row),
    excludedGenerations,
    excludedBaselineBytes: counts.excludedBaselineBytes,
    currentExcludedBytes: counts.currentExcludedBytes,
    generationsWithObservedGrowth: growth,
    lastGrowthObservedAt: counts.lastGrowthObservedAt,
    unresolvedObservationErrors: errors.unresolvedObservationErrors,
    lastObservationErrorAt: errors.lastObservationErrorAt,
  };
}

/** The public status is aggregate-only: it never returns path or generation keys. */
export function captureBaselineStatus(database: Database.Database): CaptureBaselineStatus {
  const sources = [
    sourceStatus(database, "codex"),
    sourceStatus(database, "claude_code"),
  ];
  const ready = sources.every(
      (source) =>
        source.status === "complete" && source.unresolvedObservationErrors === 0,
    );
  const blocker = sources.find((source) => source.status !== "complete");
  const latestRuns = sources
    .map((source) => source.latestRun)
    .filter((run): run is CaptureBaselineRunProgress => Boolean(run));
  const sourcesComplete = sources.filter(
    (source) => source.status === "complete" && source.unresolvedObservationErrors === 0,
  ).length;
  const sourcesInProgress = sources.filter((source) => source.status === "in_progress").length;
  const sourcesFailed = sources.filter(
    (source) => source.status === "failed" || source.unresolvedObservationErrors > 0,
  ).length;
  const filesDiscovered = latestRuns.reduce((total, run) => total + run.filesDiscovered, 0);
  const filesValidated = latestRuns.reduce((total, run) => total + run.filesValidated, 0);
  const ambiguous = sources.some((source) =>
    source.reason === CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS ||
    source.reason === CAPTURE_BASELINE_STAT_AMBIGUOUS ||
    source.reason === CAPTURE_BASELINE_GENERATION_AMBIGUOUS ||
    source.unresolvedObservationErrors > 0,
  );
  return {
    status: ready ? "complete" : "blocked",
    reason: ready
      ? null
      : blocker?.reason ?? CAPTURE_BASELINE_GENERATION_AMBIGUOUS,
    progress: {
      state: ready
        ? "complete"
        : sourcesFailed > 0
          ? ambiguous ? "ambiguous" : "failed"
          : sourcesInProgress > 0
            ? "in_progress"
            : "not_established",
      sourcesComplete,
      sourcesInProgress,
      sourcesFailed,
      filesDiscovered,
      filesValidated,
      filesBaselined: latestRuns.reduce((total, run) => total + run.filesBaselined, 0),
      pendingMetadata: Math.max(0, filesDiscovered - filesValidated),
      pendingMetadataPerSourceCap: AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP,
      pendingMetadataAggregateCap:
        AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP * sources.length,
      deferredSources: sources.length - sourcesComplete,
    },
    sources,
  };
}

/**
 * Persist a fail-closed in-progress receipt before discovery results are
 * promoted. A crash between begin and complete therefore blocks automatic
 * capture instead of silently treating an incomplete snapshot as exhaustive.
 */
export function beginAutomaticCaptureBaseline(
  database: Database.Database,
  source: HistoryCoverageSource,
  input: BeginCaptureBaselineInput,
): CaptureBaselineSourceStatus {
  ensureCaptureBaselineSchema(database);
  if (!validTimestamp(input.startedAt) || !validCount(input.filesDiscovered)) {
    throw new Error("capture_baseline_invalid_begin_receipt");
  }
  const discoveryErrors = input.discoveryErrors ?? 0;
  if (!validCount(discoveryErrors)) throw new Error("capture_baseline_invalid_error_count");
  const prior = stateRow(database, source);
  const effectiveStartedAt = prior?.startedAt ?? input.startedAt;
  // Automatic discovery is resumable across process restarts. Reusing the
  // incomplete run avoids manufacturing an abandoned generation namespace on
  // every boot; a fresh exhaustive walk can safely add/dedupe observations in
  // the same inactive namespace before the single-row arm flip.
  const runId = input.runId ??
    (prior?.status === "in_progress" || prior?.status === "failed"
      ? prior.runId
      : crypto.randomUUID());
  if (!validRunId(runId)) throw new Error("capture_baseline_invalid_run_id");

  const existing = sourceStatus(database, source);
  if (existing.status === "complete") return existing;
  if (prior?.status === "in_progress" && prior.runId === runId) {
    // A crash loses the in-memory path queue but not its exact hashed
    // path+generation identities. Refuse count-only recovery from ledgers
    // created before that identity journal existed or from a torn write.
    const pending = pendingGenerationCount(database, source, runId);
    if (prior.filesDiscovered - prior.filesValidated !== pending) {
      database.prepare(
        `update ${STATE_TABLE}
         set status = 'failed', updated_at = ?, discovery_errors = discovery_errors + 1,
           error_code = ?, error_at = ?
         where source = ? and run_id = ? and status = 'in_progress'`,
      ).run(
        input.startedAt,
        CAPTURE_BASELINE_GENERATION_AMBIGUOUS,
        input.startedAt,
        source,
        runId,
      );
      return sourceStatus(database, source);
    }
    return existing;
  }
  if (prior?.status === "failed" && prior.runId === runId) {
    const pending = pendingGenerationCount(database, source, runId);
    if (prior.filesDiscovered - prior.filesValidated !== pending) return existing;
    database.prepare(
      `update ${STATE_TABLE}
       set status = 'in_progress', updated_at = ?, completed_at = null,
         discovery_errors = 0, stat_errors = 0, error_code = null, error_at = null
       where source = ? and run_id = ? and status = 'failed'`,
    ).run(input.startedAt, source, runId);
    return sourceStatus(database, source);
  }
  const failed = discoveryErrors > 0;
  database.transaction(() => {
    // An explicitly selected replacement run is rare (focused tests and
    // operator repair). Remove only that source's inactive namespace so failed
    // attempts cannot accumulate forever. Production restart recovery reuses
    // the prior run above and does no bulk cleanup on the hot path.
    if (prior && prior.runId !== runId && prior.status !== "complete") {
      database
        .prepare(`delete from ${GENERATION_TABLE} where source = ? and run_id = ?`)
        .run(source, prior.runId);
      database
        .prepare(`delete from ${PENDING_GENERATION_TABLE} where source = ? and run_id = ?`)
        .run(source, prior.runId);
    }
    database.prepare(
      `insert into ${STATE_TABLE} (
         source, schema_version, run_id, status, started_at, updated_at,
         completed_at, files_discovered, files_validated, files_baselined,
         discovery_errors, stat_errors, error_code, error_at
       ) values (?, ?, ?, ?, ?, ?, null, ?, 0, 0, ?, 0, ?, ?)
       on conflict(source) do update set
         schema_version = excluded.schema_version,
         run_id = excluded.run_id,
         status = excluded.status,
         started_at = excluded.started_at,
         updated_at = excluded.updated_at,
         completed_at = null,
         files_discovered = excluded.files_discovered,
         files_validated = 0,
         files_baselined = 0,
         discovery_errors = excluded.discovery_errors,
         stat_errors = 0,
         error_code = excluded.error_code,
         error_at = excluded.error_at`,
    ).run(
      source,
      SCHEMA_VERSION,
      runId,
      failed ? "failed" : "in_progress",
      effectiveStartedAt,
      effectiveStartedAt,
      input.filesDiscovered,
      discoveryErrors,
      failed ? CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS : null,
      failed ? effectiveStartedAt : null,
    );
  })();
  return sourceStatus(database, source);
}

export function recordAutomaticCaptureBaselineProgress(
  database: Database.Database,
  source: HistoryCoverageSource,
  input: CaptureBaselineProgressInput,
): CaptureBaselineSourceStatus {
  ensureCaptureBaselineSchema(database);
  const discoveryErrors = input.discoveryErrors ?? 0;
  const statErrors = input.statErrors ?? 0;
  const filesDiscovered = input.filesDiscovered;
  if (
    !validRunId(input.runId) ||
    !validTimestamp(input.updatedAt) ||
    (filesDiscovered !== undefined && !validCount(filesDiscovered)) ||
    !validCount(input.filesValidated) ||
    !validCount(discoveryErrors) ||
    !validCount(statErrors)
  ) {
    throw new Error("capture_baseline_invalid_progress_receipt");
  }
  const current = stateRow(database, source);
  if (!current || !stateIsValid(current) || current.runId !== input.runId) {
    throw new Error("capture_baseline_progress_run_mismatch");
  }
  if (current.status !== "in_progress") return sourceStatus(database, source);
  const nextFilesDiscovered = filesDiscovered ?? current.filesDiscovered;
  if (
    nextFilesDiscovered < current.filesDiscovered ||
    input.filesValidated < current.filesValidated ||
    input.filesValidated > nextFilesDiscovered
  ) {
    throw new Error("capture_baseline_nonmonotonic_progress");
  }
  const errorCode =
    discoveryErrors > 0
      ? CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS
      : statErrors > 0
        ? CAPTURE_BASELINE_STAT_AMBIGUOUS
        : null;
  database
    .prepare(
      `update ${STATE_TABLE}
       set status = ?, updated_at = ?, files_discovered = ?, files_validated = ?,
         discovery_errors = ?, stat_errors = ?, error_code = ?, error_at = ?
       where source = ? and run_id = ?`,
    )
    .run(
      errorCode ? "failed" : "in_progress",
      input.updatedAt,
      nextFilesDiscovered,
      input.filesValidated,
      discoveryErrors,
      statErrors,
      errorCode,
      errorCode ? input.updatedAt : null,
      source,
      input.runId,
    );
  return sourceStatus(database, source);
}

/**
 * Durably journal the exact stat-only identities held by the bounded in-memory
 * queue. A process restart may re-walk other files, but only the same hashed
 * path+generation can discharge this debt.
 */
export function stageAutomaticCaptureBaselinePending(
  database: Database.Database,
  source: HistoryCoverageSource,
  input: StageCaptureBaselinePendingInput,
): {
  filesDiscovered: number;
  pendingMetadata: number;
  newlyPending: number;
  accepted: boolean[];
  deferred: boolean[];
} {
  ensureCaptureBaselineSchema(database);
  if (
    !validRunId(input.runId) ||
    !validTimestamp(input.observedAt) ||
    input.observations.length > AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP
  ) throw new Error("capture_baseline_invalid_pending_receipt");
  const normalized = input.observations.map((observation) => normalizeObservation(observation));
  if (normalized.some((observation) => !observation)) {
    throw new Error("capture_baseline_invalid_pending_observation");
  }

  let filesDiscovered = 0;
  let pendingMetadata = 0;
  let newlyPending = 0;
  const accepted: boolean[] = [];
  const deferred: boolean[] = [];
  database.transaction(() => {
    const current = stateRow(database, source);
    if (!current || !stateIsValid(current) || current.runId !== input.runId) {
      throw new Error("capture_baseline_pending_run_mismatch");
    }
    if (current.status !== "in_progress") return;
    const exists = database.prepare(
      `select 1 from ${PENDING_GENERATION_TABLE}
       where source = ? and run_id = ? and path_key = ? and generation_key = ?`,
    );
    const insert = database.prepare(
      `insert into ${PENDING_GENERATION_TABLE} (
         source, run_id, path_key, generation_key, baseline_size, discovered_at
       ) values (?, ?, ?, ?, ?, ?)
       on conflict(source, run_id, path_key, generation_key) do nothing`,
    );
    const alreadyStaged = database.prepare(
      `select 1 from ${GENERATION_TABLE}
       where source = ? and run_id = ? and generation_key = ?`,
    );
    pendingMetadata = pendingGenerationCount(database, source, input.runId);
    for (const observation of normalized as NormalizedObservation[]) {
      if (exists.get(source, input.runId, observation.pathKey, observation.generationKey)) {
        accepted.push(true);
        deferred.push(false);
        continue;
      }
      // A stable-sweep observation that is already durably staged needs no
      // second queue row or counter increment. Discovery already obtained its
      // stat receipt; skipping it keeps cumulative progress identity-unique.
      if (alreadyStaged.get(source, input.runId, observation.generationKey)) {
        accepted.push(false);
        deferred.push(false);
        continue;
      }
      if (pendingMetadata >= AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP) {
        accepted.push(false);
        deferred.push(true);
        continue;
      }
      const inserted = insert.run(
        source,
        input.runId,
        observation.pathKey,
        observation.generationKey,
        observation.size,
        input.observedAt,
      ).changes;
      newlyPending += inserted;
      pendingMetadata += inserted;
      accepted.push(inserted === 1);
      deferred.push(inserted !== 1);
    }
    if (pendingMetadata > AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP) {
      throw new Error("automatic_baseline_pending_metadata_cap_exceeded");
    }
    filesDiscovered = current.filesValidated + pendingMetadata;
    if (filesDiscovered < current.filesDiscovered) {
      throw new Error("capture_baseline_pending_identity_mismatch");
    }
    database.prepare(
      `update ${STATE_TABLE}
       set updated_at = ?, files_discovered = ?
       where source = ? and run_id = ? and status = 'in_progress'`,
    ).run(input.observedAt, filesDiscovered, source, input.runId);
  }).immediate();
  return { filesDiscovered, pendingMetadata, newlyPending, accepted, deferred };
}

export function resolveAutomaticCaptureBaselinePending(
  database: Database.Database,
  source: HistoryCoverageSource,
  input: {
    runId: string;
    observedAt: string;
    observation: CaptureBaselineFileObservation;
    filesDiscovered: number;
    filesValidated: number;
  },
): boolean {
  ensureCaptureBaselineSchema(database);
  if (
    !validRunId(input.runId) ||
    !validTimestamp(input.observedAt) ||
    !validCount(input.filesDiscovered) ||
    !validCount(input.filesValidated) ||
    input.filesValidated > input.filesDiscovered
  ) throw new Error("capture_baseline_invalid_pending_run");
  const normalized = normalizeObservation(input.observation);
  if (!normalized) throw new Error("capture_baseline_invalid_pending_observation");
  let resolved = false;
  database.transaction(() => {
    const current = stateRow(database, source);
    if (!current || !stateIsValid(current) || current.runId !== input.runId) {
      throw new Error("capture_baseline_pending_run_mismatch");
    }
    if (current.status !== "in_progress") return;
    if (
      input.filesDiscovered < current.filesDiscovered ||
      input.filesValidated < current.filesValidated ||
      input.filesValidated > input.filesDiscovered
    ) throw new Error("capture_baseline_nonmonotonic_progress");
    const deleted = database.prepare(
      `delete from ${PENDING_GENERATION_TABLE}
       where source = ? and run_id = ? and path_key = ? and generation_key = ?`,
    ).run(source, input.runId, normalized.pathKey, normalized.generationKey);
    if (deleted.changes !== 1) return;
    const pendingMetadata = pendingGenerationCount(database, source, input.runId);
    if (input.filesDiscovered - input.filesValidated !== pendingMetadata) {
      throw new Error("capture_baseline_pending_identity_mismatch");
    }
    database.prepare(
      `update ${STATE_TABLE}
       set updated_at = ?, files_discovered = ?, files_validated = ?
       where source = ? and run_id = ? and status = 'in_progress'`,
    ).run(
      input.observedAt,
      input.filesDiscovered,
      input.filesValidated,
      source,
      input.runId,
    );
    resolved = true;
  }).immediate();
  return resolved;
}

/**
 * Stage one stat-only observation under an inactive run. Rows contain only
 * hashes and numeric metadata; the O(1) state-row arm flip later makes the
 * run visible to automatic classification.
 */
export function stageAutomaticCaptureBaselineObservation(
  database: Database.Database,
  source: HistoryCoverageSource,
  input: StageCaptureBaselineObservationInput,
): boolean {
  ensureCaptureBaselineSchema(database);
  if (
    !validRunId(input.runId) ||
    !validTimestamp(input.observedAt) ||
    !validCount(input.filesDiscovered) ||
    !validCount(input.filesValidated) ||
    input.filesValidated > input.filesDiscovered
  ) {
    throw new Error("capture_baseline_invalid_staged_observation");
  }
  const normalized = normalizeObservation(input.observation);
  if (!normalized) {
    recordAutomaticCaptureBaselineProgress(database, source, {
      runId: input.runId,
      updatedAt: input.observedAt,
      filesDiscovered: input.filesDiscovered,
      filesValidated: Math.max(0, input.filesValidated - 1),
      statErrors: 1,
    });
    return false;
  }
  let insertedGeneration = false;
  database.transaction(() => {
    const current = stateRow(database, source);
    if (!current || !stateIsValid(current) || current.runId !== input.runId) {
      throw new Error("capture_baseline_stage_run_mismatch");
    }
    if (current.status !== "in_progress") return;
    if (input.resolvePending) {
      const deleted = database.prepare(
        `delete from ${PENDING_GENERATION_TABLE}
         where source = ? and run_id = ? and path_key = ? and generation_key = ?`,
      ).run(
        source,
        input.runId,
        normalized.pathKey,
        normalized.generationKey,
      );
      if (deleted.changes !== 1) {
        throw new Error("capture_baseline_pending_identity_mismatch");
      }
      const pendingMetadata = pendingGenerationCount(database, source, input.runId);
      if (input.filesDiscovered - input.filesValidated !== pendingMetadata) {
        throw new Error("capture_baseline_pending_identity_mismatch");
      }
    }
    const inserted = database.prepare(
      `insert into ${GENERATION_TABLE} (
         source, run_id, path_key, generation_key, baseline_size,
         last_observed_size, history_covered_size, baselined_at,
         last_observed_at, growth_observed_at, history_covered_at
       ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, null, null)
       on conflict(source, run_id, generation_key) do nothing`,
    ).run(
      source,
      input.runId,
      normalized.pathKey,
      normalized.generationKey,
      normalized.size,
      normalized.size,
      normalized.size,
      input.observedAt,
      input.observedAt,
    );
    insertedGeneration = inserted.changes === 1;
    // A second pathname for the same physical generation (rename/hardlink)
    // updates aggregate metadata but does not increment the unique-generation
    // counter. Both statements are fixed-row work.
    if (inserted.changes === 0) {
      database.prepare(
        `update ${GENERATION_TABLE}
         set path_key = min(path_key, ?),
           baseline_size = max(baseline_size, ?),
           last_observed_size = max(last_observed_size, ?),
           history_covered_size = max(history_covered_size, ?),
           last_observed_at = ?
         where source = ? and run_id = ? and generation_key = ?`,
      ).run(
        normalized.pathKey,
        normalized.size,
        normalized.size,
        normalized.size,
        input.observedAt,
        source,
        input.runId,
        normalized.generationKey,
      );
    }
    database.prepare(
      `update ${STATE_TABLE}
       set updated_at = ?, files_discovered = ?, files_validated = ?,
         files_baselined = files_baselined + ?
       where source = ? and run_id = ? and status = 'in_progress'`,
    ).run(
      input.observedAt,
      input.filesDiscovered,
      input.filesValidated,
      inserted.changes,
      source,
      input.runId,
    );
  })();
  return insertedGeneration;
}

/**
 * Promote a fully validated discovery snapshot and all exclusions in one
 * SQLite transaction. Any discovery/stat ambiguity records a failed run and
 * inserts zero generations.
 */
export function completeAutomaticCaptureBaseline(
  database: Database.Database,
  source: HistoryCoverageSource,
  input: CompleteCaptureBaselineInput,
): CaptureBaselineSourceStatus {
  ensureCaptureBaselineSchema(database);
  const discoveryErrors = input.discoveryErrors ?? 0;
  const statErrors = input.statErrors ?? 0;
  if (
    !validRunId(input.runId) ||
    !validTimestamp(input.completedAt) ||
    !validCount(discoveryErrors) ||
    !validCount(statErrors)
  ) {
    throw new Error("capture_baseline_invalid_completion_receipt");
  }

  // Compatibility for focused callers that already hold a tiny, complete
  // metadata snapshot. Production tailers stage incrementally before this
  // call, so their arm transaction below is constant-row work.
  const compatibilityObservations = input.observations ?? [];
  for (let index = 0; index < compatibilityObservations.length; index += 1) {
    stageAutomaticCaptureBaselineObservation(database, source, {
      runId: input.runId,
      observedAt: input.completedAt,
      observation: compatibilityObservations[index]!,
      filesDiscovered: compatibilityObservations.length,
      filesValidated: index + 1,
    });
  }

  const promote = database.transaction(() => {
    const current = stateRow(database, source);
    if (!current || !stateIsValid(current) || current.runId !== input.runId) {
      throw new Error("capture_baseline_completion_run_mismatch");
    }
    if (current.status === "complete") return;
    if (current.status !== "in_progress") return;

    const pendingMetadata = pendingGenerationCount(database, source, input.runId);
    const discoveryMismatch =
      discoveryErrors > 0 ||
      current.filesValidated !== current.filesDiscovered ||
      pendingMetadata > 0;
    const failureReason = discoveryMismatch
      ? pendingMetadata > 0
        ? CAPTURE_BASELINE_GENERATION_AMBIGUOUS
        : CAPTURE_BASELINE_DISCOVERY_AMBIGUOUS
      : statErrors > 0
        ? CAPTURE_BASELINE_STAT_AMBIGUOUS
        : null;
    if (failureReason) {
      database
        .prepare(
          `update ${STATE_TABLE}
           set status = 'failed', updated_at = ?, files_validated = ?,
             discovery_errors = ?, stat_errors = ?, error_code = ?, error_at = ?
           where source = ? and run_id = ?`,
        )
        .run(
          input.completedAt,
          current.filesValidated,
          discoveryErrors + (current.filesValidated !== current.filesDiscovered ? 1 : 0),
          statErrors,
          failureReason,
          input.completedAt,
          source,
          input.runId,
        );
      return;
    }

    database
      .prepare(
        `update ${STATE_TABLE}
         set status = 'complete', updated_at = ?, completed_at = ?,
           files_validated = ?, files_baselined = ?, discovery_errors = 0,
           stat_errors = 0, error_code = null, error_at = null
         where source = ? and run_id = ?`,
      )
      .run(
        input.completedAt,
        input.completedAt,
        current.filesValidated,
        current.filesBaselined,
        source,
        input.runId,
      );
  });
  promote.immediate();
  return sourceStatus(database, source);
}

/**
 * Classify a stat-only observation. Automatic scans fail closed until the
 * source baseline is complete and exclude a matching generation regardless
 * of later append growth. Explicit full scans always capture and never delete
 * or bypass the durable exclusion record.
 */
export function classifyCaptureBaselineFile(
  database: Database.Database,
  source: HistoryCoverageSource,
  observation: CaptureBaselineFileObservation,
  options: { mode: "automatic" | "explicit_full"; observedAt: string },
): CaptureBaselineDecision {
  ensureCaptureBaselineSchema(database);
  if (!validTimestamp(options.observedAt)) {
    throw new Error("capture_baseline_invalid_observation_time");
  }
  const normalized = normalizeObservation(observation);
  if (!normalized) {
    return {
      decision: "block",
      reason: CAPTURE_BASELINE_STAT_AMBIGUOUS,
      matchedExcludedGeneration: false,
      observedGrowth: false,
      historyInvalidated: false,
    };
  }

  // Classification is the per-file hot path. Read the two keyed state rows
  // directly; aggregate COUNT/SUM status is reserved for public snapshots and
  // must never turn N discovered files into N full baseline scans.
  const sourceRows = (["codex", "claude_code"] as const).map((candidate) => ({
    source: candidate,
    row: stateRow(database, candidate),
  }));
  const sourceState = sourceRows.find((candidate) => candidate.source === source)!.row;
  const unresolvedError = database.prepare(
    `select 1 from ${ERROR_TABLE} where resolved_at is null limit 1`,
  ).get() as { 1: number } | undefined;
  const allSourcesEstablished = sourceRows.every(
    ({ row }) => row !== undefined && stateIsValid(row) && row.status === "complete",
  );
  // Explicit history repair stays available when the automatic baseline is
  // absent or failed. Automatic capture, however, is globally fail-closed:
  // neither source becomes live until both discovery snapshots are complete.
  if (options.mode === "automatic" && !allSourcesEstablished) {
    const blocker = sourceRows.find(({ row }) => !row || !stateIsValid(row) || row.status !== "complete")?.row;
    return {
      decision: "block",
      reason: blocker
        ? rowReason(blocker.errorCode) ??
          (blocker.status === "in_progress" ? CAPTURE_BASELINE_IN_PROGRESS : CAPTURE_BASELINE_STATE_INVALID)
        : unresolvedError
          ? CAPTURE_BASELINE_GENERATION_AMBIGUOUS
          : CAPTURE_BASELINE_NOT_ESTABLISHED,
      matchedExcludedGeneration: false,
      observedGrowth: false,
      historyInvalidated: false,
    };
  }

  const matching = database
    .prepare(
      `select baseline_size as baselineSize,
         last_observed_size as lastObservedSize,
         history_covered_size as historyCoveredSize
       from ${GENERATION_TABLE}
       where source = ? and run_id = ? and generation_key = ?`,
    )
    .get(source, sourceState?.status === "complete" ? sourceState.runId : "__inactive__", normalized.generationKey) as
      | { baselineSize: number; lastObservedSize: number; historyCoveredSize: number }
      | undefined;
  if (options.mode === "automatic" && unresolvedError && matching) {
    return {
      decision: "block",
      reason: CAPTURE_BASELINE_GENERATION_AMBIGUOUS,
      matchedExcludedGeneration: true,
      observedGrowth: false,
      historyInvalidated: false,
    };
  }
  if (!matching) {
    // A trustworthy replacement generation at the same hashed path resolves
    // an earlier same-generation truncation receipt. No path is persisted or
    // returned in plaintext.
    database
      .prepare(
        `update ${ERROR_TABLE}
         set resolved_at = ?
         where source = ? and path_key = ? and resolved_at is null`,
      )
      .run(options.observedAt, source, normalized.pathKey);
    const anotherUnresolvedError = Boolean(database.prepare(
      `select 1 from ${ERROR_TABLE} where resolved_at is null limit 1`,
    ).get());
    if (options.mode === "automatic" && anotherUnresolvedError) {
      return {
        decision: "block",
        reason: CAPTURE_BASELINE_GENERATION_AMBIGUOUS,
        matchedExcludedGeneration: false,
        observedGrowth: false,
        historyInvalidated: false,
      };
    }
    return {
      decision: "capture",
      reason: options.mode === "explicit_full" ? "explicit_full_scan" : "generation_not_baselined",
      matchedExcludedGeneration: false,
      observedGrowth: false,
      historyInvalidated: false,
    };
  }

  // Device/inode/birth time says this is the same generation. A smaller size
  // therefore means truncation or in-place rewrite, not a safe append. Keep
  // the body closed and persist only an aggregateable hashed-path receipt.
  if (
    normalized.size < matching.baselineSize ||
    normalized.size < matching.lastObservedSize
  ) {
    database
      .prepare(
        `insert into ${ERROR_TABLE} (
           source, path_key, error_code, first_observed_at, last_observed_at,
           occurrence_count, resolved_at
         ) values (?, ?, ?, ?, ?, 1, null)
         on conflict(source, path_key, error_code) do update set
           last_observed_at = excluded.last_observed_at,
           occurrence_count = ${ERROR_TABLE}.occurrence_count + 1,
           resolved_at = null`,
      )
      .run(
        source,
        normalized.pathKey,
        CAPTURE_BASELINE_GENERATION_AMBIGUOUS,
        options.observedAt,
        options.observedAt,
      );
    return {
      decision: "block",
      reason: CAPTURE_BASELINE_GENERATION_AMBIGUOUS,
      matchedExcludedGeneration: true,
      observedGrowth: false,
      historyInvalidated: false,
    };
  }

  const observedGrowth = normalized.size > matching.lastObservedSize;
  const uncoveredGrowth = normalized.size > matching.historyCoveredSize;
  database
    .prepare(
      `update ${GENERATION_TABLE}
       set last_observed_size = max(last_observed_size, ?),
         last_observed_at = ?,
         growth_observed_at = case
           when ? > baseline_size then coalesce(growth_observed_at, ?)
           else growth_observed_at
         end
       where source = ? and run_id = ? and generation_key = ?`,
    )
    .run(
      normalized.size,
      options.observedAt,
      normalized.size,
      options.observedAt,
      source,
      sourceState?.status === "complete" ? sourceState.runId : "__inactive__",
      normalized.generationKey,
    );

  // An explicit scan is in flight, not yet a successful completion. Preserve
  // an existing invalidation and let recordExplicitFullHistoryCoverage clear
  // it only after the scan proves exhaustive.
  const historyInvalidated = uncoveredGrowth
    ? invalidateHistoryCoverageForExcludedGrowth(database, source, options.observedAt)
    : false;
  if (options.mode === "explicit_full") {
    return {
      decision: "capture",
      reason: "explicit_full_scan",
      matchedExcludedGeneration: true,
      observedGrowth,
      historyInvalidated,
    };
  }
  return {
    decision: "exclude",
    reason: "preexisting_generation",
    matchedExcludedGeneration: true,
    observedGrowth,
    historyInvalidated,
  };
}

/**
 * Advance growth watermarks after an exhaustive explicit scan. The rows stay
 * present: explicit history repair acknowledges coverage but never clears an
 * automatic-capture exclusion.
 */
export function acknowledgeCaptureBaselineHistoryCoverage(
  database: Database.Database,
  source: HistoryCoverageSource,
  completedAt: string,
): void {
  ensureCaptureBaselineSchema(database);
  if (!validTimestamp(completedAt)) throw new Error("capture_baseline_invalid_coverage_time");
  database
    .prepare(
      `update ${GENERATION_TABLE}
       set history_covered_size = max(history_covered_size, last_observed_size),
         history_covered_at = ?
       where source = ? and run_id = (
         select run_id from ${STATE_TABLE}
         where source = ? and status = 'complete'
       )`,
    )
    .run(completedAt, source, source);
}

/** Internal guard used by history coverage without exposing stored keys. */
export function captureBaselineTablesExist(database: Database.Database): boolean {
  const rows = database
    .prepare(
      `select name from sqlite_master
       where type = 'table' and name in (?, ?)`,
    )
    .all(STATE_TABLE, GENERATION_TABLE) as Array<{ name: string }>;
  return rows.length === 2;
}
