import crypto from "node:crypto";

import type Database from "better-sqlite3";

import {
  buildProjectUsageExport,
  type ProjectUsageExport,
  type ProjectUsageExportInput,
  type ProjectUsageProjectMapping,
  type ProjectUsageSourceInput,
} from "../../shared/src/finance-project-usage-export";
import type { CostUsageRecord, ToolSource } from "../../shared/src/schemas";
import { historyCoverageStatus } from "./history-coverage";

/** The only source version implemented by this offline projection adapter. */
export const FINANCE_PROJECT_USAGE_SOURCE_VERSION = "finance_exact_period.v2" as const;

const DASHBOARD_SCHEMA_VERSION = 1;
const MAX_SOURCE_RECORDS = 10_000;
const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const NANOSECONDS_PER_MICRO = 1_000n;
const PROJECT_KEY_PATTERN = /^sha256:[a-f0-9]{64}$/;
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SOURCE_VALUES = ["codex", "claude_code"] as const;
const MAX_CAPTURE_AGE_MS = 86_400_000;
// The wire contract requires a UUID even when the native binding is absent.
// This value is used only for a finite, no-records hold; a healthy projection
// always uses the current native installation epoch instead.
const UNAVAILABLE_INSTALLATION_REF = "00000000-0000-4000-8000-000000000000";

export type FinanceProjectionSource = (typeof SOURCE_VALUES)[number];

/**
 * Finite, privacy-safe diagnostics.  These names deliberately contain no
 * database values, source keys, paths, model names, or error text.
 */
export type FinanceProjectionReason =
  | "WORKSPACE_BINDING_UNPROVEN"
  | "PROJECTION_SCHEMA_UNSUPPORTED"
  | "PROJECTION_SNAPSHOT_UNAVAILABLE"
  | "PROJECTION_SNAPSHOT_FUTURE"
  | "PROJECTION_NOT_READY"
  | "PROJECTION_PARITY_NOT_READY"
  | "PROJECTION_BACKFILL_INCOMPLETE"
  | "PROJECTION_DIRTY"
  | "PROJECTION_DEGRADED"
  | "PROJECTION_BACKLOG"
  | "HISTORY_COVERAGE_UNAVAILABLE"
  | "HISTORY_COVERAGE_INCOMPLETE"
  | "HISTORY_COVERAGE_INVALIDATED"
  | "HISTORY_FULL_SCAN_NOT_CURRENT"
  | "CAPTURE_ACTIVITY_UNAVAILABLE"
  | "CAPTURE_ACTIVITY_STALE"
  | "CAPTURE_ACTIVITY_ERROR"
  | "CAPTURE_ACTIVITY_TRUNCATED"
  | "OPEN_PERIOD"
  | "RETENTION_GAP"
  | "INSTALLATION_SCOPE_UNPROVEN"
  | "UNSUPPORTED_REQUIRED_SOURCE"
  | "SOURCE_REVISION_UNBOUND"
  | "OBSERVED_INTERVAL_UNQUALIFIED"
  | "CACHE_ONLY_USAGE_UNREPRESENTABLE"
  | "CACHE_USAGE_UNREPRESENTABLE"
  | "PROVENANCE_COVERAGE_UNAVAILABLE"
  | "PROVENANCE_SCHEMA_UNSUPPORTED"
  | "PROVENANCE_LINEAGE_UNAVAILABLE"
  | "FINANCE_PUBLICATION_SCHEMA_UNSUPPORTED"
  | "FINANCE_PUBLICATION_DIRTY"
  | "FINANCE_PUBLICATION_SCOPE_MISMATCH"
  | "FINANCE_PUBLICATION_REVISION_MISMATCH"
  | "FINANCE_COVERAGE_UNAVAILABLE"
  | "FINANCE_COVERAGE_INCOMPLETE"
  | "FINANCE_COVERAGE_INVALIDATED"
  | "UNKNOWN_COST_PROVENANCE"
  | "COST_PRECISION_UNSAFE"
  | "COST_VALUE_MISSING";

export type FinanceProjectionPeriod = {
  start: string;
  end: string;
};

/** Trusted caller input.  No secret, path, actor, session, or model values. */
export type FinanceProjectionRequest = {
  tenantRef: string;
  expectedWorkspaceId: string;
  period: FinanceProjectionPeriod;
  requiredSources: readonly FinanceProjectionSource[];
  registryVersion: string;
  sourceVersion: typeof FINANCE_PROJECT_USAGE_SOURCE_VERSION;
  billingPoolBySource: ProjectUsageSourceInput["billingPoolBySource"];
  projectMappings: readonly ProjectUsageProjectMapping[];
  /** Deprecated v1 caller fields are ignored by v2 and cannot prove coverage. */
  installationRef?: string;
  installationScopeAsserted?: boolean;
  now?: string;
  maxSourceAgeMs?: number;
  retentionCutoff?: string;
};

export type FinanceProjectUsageProjectionResult = {
  input: ProjectUsageExportInput;
  reasons: readonly FinanceProjectionReason[];
  sourceSnapshotAt: string;
};

export type FinanceProjectUsageExportResult = {
  exported: ProjectUsageExport;
  reasons: readonly FinanceProjectionReason[];
  sourceSnapshotAt: string;
};

type ParsedTime = { ms: number; iso: string };

type NormalizedRequest = Omit<
  FinanceProjectionRequest,
  "period" | "requiredSources" | "now" | "retentionCutoff" | "maxSourceAgeMs"
> & {
  period: { start: ParsedTime; end: ParsedTime };
  requiredSources: readonly FinanceProjectionSource[];
  nativeInstallationEpochId?: string;
};

type FinancePublicationRow = {
  dirty: number;
  revision: number;
  workspaceId: string | null;
  installationEpochId: string | null;
  projectionGeneration: number | null;
  publishedAt: string | null;
};

type FinanceCoverageRow = {
  workspaceId: string;
  installationEpochId: string;
  source: FinanceProjectionSource;
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

type ProjectionControlRow = {
  schemaVersion: number;
  ready: number;
  parityReady: number;
  generation: number;
  dirty: number;
  degradedReason: string | null;
  lastSuccessAt: string | null;
  backfillComplete: number;
  parityComplete: number;
  metricBackfillComplete: number;
  repairBacklog: number;
  dirtySessionBacklog: number;
  accountInvalidationBacklog: number;
  compactMutationBacklog: number;
  compactGcBacklog: number;
};

type ActivityRow = {
  lastActivityAt: string | null;
  lastScanAt: string | null;
  hasError: number;
  truncated: number;
};

type ProjectionFactRow = {
  eventType: string;
  projectionId: string;
  source: string;
  observedAtMs: unknown;
  inputTokens: unknown;
  outputTokens: unknown;
  cacheReadTokens: unknown;
  cacheCreationTokens: unknown;
  costNanos: unknown;
  repoHash: unknown;
  projectKey: unknown;
  costKind: unknown;
  rawGeneration: unknown;
  workspaceId: unknown;
  installationEpochId: unknown;
};

class FinanceProjectionRejected extends Error {
  constructor(code: string) {
    super(`finance_project_usage_projection_rejected:${code}`);
    this.name = "FinanceProjectionRejected";
  }
}

function reject(code: string): never {
  throw new FinanceProjectionRejected(code);
}

function addReason(reasons: Set<FinanceProjectionReason>, reason: FinanceProjectionReason): void {
  reasons.add(reason);
}

const COVERAGE_WATERMARK_BLOCKERS = new Set<FinanceProjectionReason>([
  "OBSERVED_INTERVAL_UNQUALIFIED",
  "WORKSPACE_BINDING_UNPROVEN",
  "PROJECTION_SCHEMA_UNSUPPORTED",
  "PROJECTION_SNAPSHOT_UNAVAILABLE",
  "PROJECTION_SNAPSHOT_FUTURE",
  "PROJECTION_NOT_READY",
  "PROJECTION_PARITY_NOT_READY",
  "PROJECTION_BACKFILL_INCOMPLETE",
  "PROJECTION_DIRTY",
  "PROJECTION_DEGRADED",
  "PROJECTION_BACKLOG",
  "HISTORY_COVERAGE_UNAVAILABLE",
  "HISTORY_COVERAGE_INCOMPLETE",
  "HISTORY_COVERAGE_INVALIDATED",
  "HISTORY_FULL_SCAN_NOT_CURRENT",
  "CAPTURE_ACTIVITY_UNAVAILABLE",
  "CAPTURE_ACTIVITY_STALE",
  "CAPTURE_ACTIVITY_ERROR",
  "CAPTURE_ACTIVITY_TRUNCATED",
  "OPEN_PERIOD",
  "RETENTION_GAP",
  "UNSUPPORTED_REQUIRED_SOURCE",
  "SOURCE_REVISION_UNBOUND",
  "CACHE_ONLY_USAGE_UNREPRESENTABLE",
  "CACHE_USAGE_UNREPRESENTABLE",
  "PROVENANCE_COVERAGE_UNAVAILABLE",
  "PROVENANCE_SCHEMA_UNSUPPORTED",
  "PROVENANCE_LINEAGE_UNAVAILABLE",
  "FINANCE_PUBLICATION_SCHEMA_UNSUPPORTED",
  "FINANCE_PUBLICATION_DIRTY",
  "FINANCE_PUBLICATION_SCOPE_MISMATCH",
  "FINANCE_PUBLICATION_REVISION_MISMATCH",
  "FINANCE_COVERAGE_UNAVAILABLE",
  "FINANCE_COVERAGE_INCOMPLETE",
  "FINANCE_COVERAGE_INVALIDATED",
]);

// A dirty, incomplete, or unlineaged native projection cannot safely provide
// partial finance records. Coverage-only holds (for example, the future
// finance publication and source-revision receipts) may still accompany
// lineaged facts, but these reasons suppress every fact in the result.
const NO_RECORD_REASONS = new Set<FinanceProjectionReason>([
  "WORKSPACE_BINDING_UNPROVEN",
  "PROJECTION_SCHEMA_UNSUPPORTED",
  "PROJECTION_SNAPSHOT_UNAVAILABLE",
  "PROJECTION_SNAPSHOT_FUTURE",
  "PROJECTION_NOT_READY",
  "PROJECTION_PARITY_NOT_READY",
  "PROJECTION_BACKFILL_INCOMPLETE",
  "PROJECTION_DIRTY",
  "PROJECTION_DEGRADED",
  "PROJECTION_BACKLOG",
  "PROVENANCE_SCHEMA_UNSUPPORTED",
  "PROVENANCE_LINEAGE_UNAVAILABLE",
  "FINANCE_PUBLICATION_SCHEMA_UNSUPPORTED",
  "FINANCE_PUBLICATION_DIRTY",
  "FINANCE_PUBLICATION_SCOPE_MISMATCH",
  "FINANCE_PUBLICATION_REVISION_MISMATCH",
  "FINANCE_COVERAGE_UNAVAILABLE",
  "FINANCE_COVERAGE_INCOMPLETE",
  "FINANCE_COVERAGE_INVALIDATED",
]);

function canonicalTime(value: unknown, code: string): ParsedTime {
  if (typeof value !== "string" || value.trim() !== value) reject(code);
  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) reject(code);
  const iso = new Date(ms).toISOString();
  if (value !== iso) reject(`${code}_noncanonical`);
  return { ms, iso };
}

function normalizeRequest(request: FinanceProjectionRequest): NormalizedRequest {
  if (request === null || typeof request !== "object" || Array.isArray(request)) {
    reject("invalid_request");
  }
  const input = request as Record<string, unknown>;
  const tenantRef = typeof input.tenantRef === "string" ? input.tenantRef : "";
  const expectedWorkspaceId = typeof input.expectedWorkspaceId === "string" ? input.expectedWorkspaceId.trim() : "";
  if (!tenantRef || !expectedWorkspaceId) reject("invalid_request");
  if (input.sourceVersion !== FINANCE_PROJECT_USAGE_SOURCE_VERSION) reject("unsupported_source_version");
  const periodInput = input.period;
  if (periodInput === null || typeof periodInput !== "object" || Array.isArray(periodInput)) reject("invalid_period");
  const periodRecord = periodInput as Record<string, unknown>;
  const periodStart = canonicalTime(periodRecord.start, "invalid_period_start");
  const periodEnd = canonicalTime(periodRecord.end, "invalid_period_end");
  if (periodStart.ms >= periodEnd.ms) reject("invalid_period");
  if (!Array.isArray(input.requiredSources) || input.requiredSources.length === 0) reject("invalid_required_sources");
  const requiredSources: FinanceProjectionSource[] = [];
  for (const source of input.requiredSources) {
    if (!SOURCE_VALUES.includes(source as FinanceProjectionSource)) {
      reject("unsupported_required_source");
    }
    if (requiredSources.includes(source as FinanceProjectionSource)) reject("duplicate_required_source");
    requiredSources.push(source as FinanceProjectionSource);
  }
  if (!input.billingPoolBySource || typeof input.billingPoolBySource !== "object" || Array.isArray(input.billingPoolBySource)) {
    reject("invalid_billing_pool_lookup");
  }
  if (!Array.isArray(input.projectMappings)) reject("invalid_project_mappings");

  return {
    tenantRef,
    expectedWorkspaceId,
    period: { start: periodStart, end: periodEnd },
    requiredSources,
    registryVersion: typeof input.registryVersion === "string" ? input.registryVersion : "",
    sourceVersion: FINANCE_PROJECT_USAGE_SOURCE_VERSION,
    billingPoolBySource: input.billingPoolBySource as ProjectUsageSourceInput["billingPoolBySource"],
    projectMappings: input.projectMappings as readonly ProjectUsageProjectMapping[],
  };
}

function tableExists(database: Database.Database, table: string): boolean {
  return Boolean(
    database.prepare(
      `select 1 as present from sqlite_master where type = 'table' and name = ?`,
    ).get(table),
  );
}

function tableColumns(database: Database.Database, table: string): Set<string> {
  return new Set(
    (database.pragma(`table_info(${table})`) as Array<{ name: string }>).map((column) => column.name),
  );
}

function hasColumns(database: Database.Database, table: string, columns: readonly string[]): boolean {
  if (!tableExists(database, table)) return false;
  const actual = tableColumns(database, table);
  return columns.every((column) => actual.has(column));
}

function readProjectionControl(database: Database.Database): ProjectionControlRow | undefined {
  if (!hasColumns(database, "dashboard_projection_control", [
    "schema_version", "ready", "parity_ready", "generation", "dirty", "degraded_reason",
    "last_success_at", "backfill_complete", "parity_complete", "metric_backfill_complete",
    "repair_backlog", "dirty_session_backlog", "account_invalidation_backlog",
    "compact_mutation_backlog", "compact_gc_backlog",
  ])) return undefined;
  return database.prepare(
    `select schema_version as schemaVersion, ready, parity_ready as parityReady,
       generation, dirty, degraded_reason as degradedReason, last_success_at as lastSuccessAt,
       backfill_complete as backfillComplete, parity_complete as parityComplete,
       metric_backfill_complete as metricBackfillComplete,
       repair_backlog as repairBacklog, dirty_session_backlog as dirtySessionBacklog,
       account_invalidation_backlog as accountInvalidationBacklog,
       compact_mutation_backlog as compactMutationBacklog, compact_gc_backlog as compactGcBacklog
     from dashboard_projection_control where singleton = 1`,
  ).get() as ProjectionControlRow | undefined;
}

function readActivity(database: Database.Database, source: FinanceProjectionSource): ActivityRow | undefined {
  if (!hasColumns(database, "capture_activity_state", [
    "source", "last_activity_at", "last_scan_at", "last_error_code", "truncated",
  ])) return undefined;
  return database.prepare(
    `select last_activity_at as lastActivityAt, last_scan_at as lastScanAt,
       case when last_error_code is null then 0 else 1 end as hasError, truncated
     from capture_activity_state where source = ?`,
  ).get(source) as ActivityRow | undefined;
}

function readFinancePublication(
  database: Database.Database,
  request: NormalizedRequest,
  reasons: Set<FinanceProjectionReason>,
): {
  publication: FinancePublicationRow | null;
  coverage: readonly FinanceCoverageRow[];
  publishedAtMs: number | null;
  ready: boolean;
} {
  const publicationColumns = [
    "singleton", "dirty", "revision", "workspace_id", "installation_epoch_id",
    "projection_generation", "published_at", "updated_at",
  ] as const;
  const coverageColumns = [
    "workspace_id", "installation_epoch_id", "source", "retained_from",
    "covered_through", "latest_full_attempt_at", "latest_full_complete",
    "invalidated_at", "last_scan_at", "last_scan_ok", "last_scan_truncated",
    "state_revision", "published_revision",
  ] as const;
  if (!hasColumns(database, "finance_publication_control", publicationColumns) ||
      !hasColumns(database, "finance_source_coverage", coverageColumns)) {
    addReason(reasons, "FINANCE_PUBLICATION_SCHEMA_UNSUPPORTED");
    addReason(reasons, "PROVENANCE_SCHEMA_UNSUPPORTED");
    addReason(reasons, "PROVENANCE_COVERAGE_UNAVAILABLE");
    addReason(reasons, "SOURCE_REVISION_UNBOUND");
    return { publication: null, coverage: [], publishedAtMs: null, ready: false };
  }
  const publication = database.prepare(
    `select dirty, revision, workspace_id as workspaceId,
       installation_epoch_id as installationEpochId,
       projection_generation as projectionGeneration, published_at as publishedAt
     from finance_publication_control where singleton=1`,
  ).get() as FinancePublicationRow | undefined;
  if (!publication) {
    addReason(reasons, "FINANCE_PUBLICATION_SCHEMA_UNSUPPORTED");
    addReason(reasons, "PROVENANCE_COVERAGE_UNAVAILABLE");
    addReason(reasons, "SOURCE_REVISION_UNBOUND");
    return { publication: null, coverage: [], publishedAtMs: null, ready: false };
  }
  const coverage = database.prepare(
    `select workspace_id as workspaceId, installation_epoch_id as installationEpochId,
       source, retained_from as retainedFrom, covered_through as coveredThrough,
       latest_full_attempt_at as latestFullAttemptAt,
       latest_full_complete as latestFullComplete, invalidated_at as invalidatedAt,
       last_scan_at as lastScanAt, last_scan_ok as lastScanOk,
       last_scan_truncated as lastScanTruncated, state_revision as stateRevision,
       published_revision as publishedRevision
     from finance_source_coverage where workspace_id=? and installation_epoch_id=?
       and source in (${request.requiredSources.map(() => "?").join(",")})
     order by source`,
  ).all(
    request.expectedWorkspaceId,
    request.nativeInstallationEpochId ?? "",
    ...request.requiredSources,
  ) as FinanceCoverageRow[];
  let publishedAtMs: number | null = null;
  if (publication.dirty !== 0) addReason(reasons, "FINANCE_PUBLICATION_DIRTY");
  if (publication.workspaceId !== request.expectedWorkspaceId ||
      publication.installationEpochId !== request.nativeInstallationEpochId) {
    addReason(reasons, "FINANCE_PUBLICATION_SCOPE_MISMATCH");
  }
  if (!Number.isSafeInteger(publication.revision) || publication.revision <= 0 ||
      publication.projectionGeneration === null ||
      !Number.isSafeInteger(publication.projectionGeneration) ||
      publication.projectionGeneration < 0 || publication.publishedAt === null) {
    addReason(reasons, "FINANCE_PUBLICATION_REVISION_MISMATCH");
  } else {
    const parsed = Date.parse(publication.publishedAt);
    if (!Number.isFinite(parsed) || new Date(parsed).toISOString() !== publication.publishedAt) {
      addReason(reasons, "FINANCE_PUBLICATION_REVISION_MISMATCH");
    } else {
      publishedAtMs = parsed;
    }
  }
  if (coverage.length !== request.requiredSources.length) {
    addReason(reasons, "FINANCE_COVERAGE_UNAVAILABLE");
  }
  for (const source of request.requiredSources) {
    const row = coverage.find((candidate) => candidate.source === source);
    if (!row) continue;
    const retainedMs = Date.parse(row.retainedFrom);
    const coveredMs = row.coveredThrough === null ? NaN : Date.parse(row.coveredThrough);
    const scanMs = row.lastScanAt === null ? NaN : Date.parse(row.lastScanAt);
    const validWatermark = Number.isFinite(retainedMs) && Number.isFinite(coveredMs) &&
      retainedMs <= request.period.start.ms && coveredMs >= request.period.end.ms &&
      Number.isFinite(scanMs) && scanMs > request.period.end.ms &&
      row.lastScanOk === 1 && row.lastScanTruncated === 0 &&
      publishedAtMs !== null && scanMs <= publishedAtMs &&
      publishedAtMs - scanMs <= MAX_CAPTURE_AGE_MS;
    if (row.workspaceId !== request.expectedWorkspaceId ||
        row.installationEpochId !== request.nativeInstallationEpochId ||
        row.latestFullComplete !== 1 || !validWatermark ||
        row.publishedRevision !== publication.revision) {
      addReason(reasons, "FINANCE_COVERAGE_INCOMPLETE");
      addReason(reasons, "SOURCE_REVISION_UNBOUND");
    }
    if (row.invalidatedAt !== null) {
      addReason(reasons, "FINANCE_COVERAGE_INVALIDATED");
      addReason(reasons, "SOURCE_REVISION_UNBOUND");
    }
    if (row.retainedFrom && retainedMs > request.period.start.ms) addReason(reasons, "RETENTION_GAP");
  }
  const ready = coverage.length === request.requiredSources.length &&
    ![...reasons].some((reason) => [
      "FINANCE_PUBLICATION_SCHEMA_UNSUPPORTED", "FINANCE_PUBLICATION_DIRTY",
      "FINANCE_PUBLICATION_SCOPE_MISMATCH", "FINANCE_PUBLICATION_REVISION_MISMATCH",
      "FINANCE_COVERAGE_UNAVAILABLE", "FINANCE_COVERAGE_INCOMPLETE",
      "FINANCE_COVERAGE_INVALIDATED", "SOURCE_REVISION_UNBOUND",
    ].includes(reason));
  if (!ready) {
    addReason(reasons, "PROVENANCE_COVERAGE_UNAVAILABLE");
    addReason(reasons, "SOURCE_REVISION_UNBOUND");
  }
  return { publication, coverage, publishedAtMs, ready };
}

function readFacts(
  database: Database.Database,
  request: NormalizedRequest,
): ProjectionFactRow[] {
  const placeholders = request.requiredSources.map(() => "?").join(",");
  const rows = database.prepare(
    `select projection_id as projectionId, source, event_type as eventType,
       observed_at_ms as observedAtMs, input_tokens as inputTokens,
       output_tokens as outputTokens, cache_read_tokens as cacheReadTokens,
       cache_creation_tokens as cacheCreationTokens, cost_nanos as costNanos,
       repo_hash as repoHash, project_key as projectKey, cost_kind as costKind,
       raw_generation as rawGeneration, workspace_id as workspaceId,
       installation_epoch_id as installationEpochId
     from dashboard_event_facts
     where workspace_id = ? and installation_epoch_id = ?
       and source in (${placeholders})
       and observed_at_ms >= ? and (observed_at_ms < ? or
         (event_type='usage_live' and case when json_valid(live_usage_json)
           then json_extract(live_usage_json,'$.intervalStart') < ? else 0 end))
       and (input_tokens is not null or output_tokens is not null
         or cache_read_tokens is not null or cache_creation_tokens is not null
         or cost_nanos is not null)
     order by observed_at_ms, projection_id limit ${MAX_SOURCE_RECORDS + 1}`,
  ).all(
    request.expectedWorkspaceId,
    request.nativeInstallationEpochId,
    ...request.requiredSources,
    request.period.start.ms,
    request.period.end.ms,
    request.period.end.iso,
  ) as ProjectionFactRow[];
  return rows;
}

function canonicalFactProjectKey(row: ProjectionFactRow): string | null {
  if (typeof row.projectKey === "string") {
    const explicit = row.projectKey.trim().toLowerCase();
    if (PROJECT_KEY_PATTERN.test(explicit)) return explicit;
  }
  if (typeof row.repoHash === "string") {
    const repoHash = row.repoHash.trim().toLowerCase();
    if (PROJECT_KEY_PATTERN.test(repoHash)) return repoHash;
  }
  return null;
}

function safeMetric(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) {
    reject("invalid_projection_metric");
  }
  return value;
}

function microsFromNumber(value: number): bigint | null {
  if (!Number.isFinite(value) || value < 0 || Math.abs(value) > Number.MAX_SAFE_INTEGER) return null;
  const text = Object.is(value, -0) ? "0" : value.toString();
  const parts = /^([0-9]+)(?:\.([0-9]*))?(?:e([+-]?[0-9]+))?$/i.exec(text);
  if (!parts) return null;
  const integerPart = parts[1]!;
  const fractionPart = parts[2] ?? "";
  const exponent = parts[3] === undefined ? 0 : Number(parts[3]);
  if (!Number.isSafeInteger(exponent)) return null;
  const digits = BigInt(`${integerPart}${fractionPart}` || "0");
  const decimalPlaces = fractionPart.length - exponent;
  if (decimalPlaces > 6) {
    const divisor = 10n ** BigInt(decimalPlaces - 6);
    if (digits % divisor !== 0n) return null;
    return digits / divisor;
  }
  if (decimalPlaces < 6) return digits * 10n ** BigInt(6 - decimalPlaces);
  return digits;
}

function costMicros(row: ProjectionFactRow, reasons: Set<FinanceProjectionReason>): { channel: "reported" | "estimated"; usd: number } | null {
  const costKind = row.costKind === "reported" || row.costKind === "estimated"
    ? row.costKind
    : null;
  if (row.costNanos !== null && row.costNanos !== undefined && costKind === null) {
    addReason(reasons, "UNKNOWN_COST_PROVENANCE");
    return null;
  }
  if (row.costNanos === null || row.costNanos === undefined) return null;
  if (typeof row.costNanos !== "number" || !Number.isSafeInteger(row.costNanos) || row.costNanos < 0) {
    addReason(reasons, "COST_PRECISION_UNSAFE");
    return null;
  }
  const nanos = BigInt(row.costNanos);
  if (nanos % NANOSECONDS_PER_MICRO !== 0n) {
    addReason(reasons, "COST_PRECISION_UNSAFE");
    return null;
  }
  const micros = nanos / NANOSECONDS_PER_MICRO;
  if (micros > MAX_SAFE_BIGINT) {
    addReason(reasons, "COST_PRECISION_UNSAFE");
    return null;
  }
  const usd = Number(micros) / 1_000_000;
  if (microsFromNumber(usd) !== micros) {
    addReason(reasons, "COST_PRECISION_UNSAFE");
    return null;
  }
  return { channel: costKind!, usd };
}

function uuidForProjection(projectionId: string): string {
  const digest = crypto.createHash("sha256").update(`finance-project-usage:${projectionId}`, "utf8").digest("hex");
  // A deterministic UUID-shaped id keeps CostUsageRecord compatible if the
  // source schema tightens its current non-empty-string id constraint.
  return `${digest.slice(0, 8)}-${digest.slice(8, 12)}-4${digest.slice(13, 16)}-8${digest.slice(17, 20)}-${digest.slice(20, 32)}`;
}

function recordFromFact(
  row: ProjectionFactRow,
  request: NormalizedRequest,
  reasons: Set<FinanceProjectionReason>,
): CostUsageRecord | null {
  // This exclusion survives raw retention and missing or damaged interval markers.
  // Observer deltas must never become synthetic one-millisecond Finance records.
  if (row.eventType === "usage_live") {
    addReason(reasons, "OBSERVED_INTERVAL_UNQUALIFIED");
    return null;
  }
  if (typeof row.projectionId !== "string" || !/^sha256:[a-f0-9]{64}$/.test(row.projectionId)) {
    reject("invalid_projection_identity");
  }
  if (!SOURCE_VALUES.includes(row.source as FinanceProjectionSource)) reject("invalid_projection_source");
  if (typeof row.rawGeneration !== "string" || row.rawGeneration.length === 0 ||
      row.workspaceId !== request.expectedWorkspaceId ||
      row.installationEpochId !== request.nativeInstallationEpochId) {
    addReason(reasons, "PROVENANCE_LINEAGE_UNAVAILABLE");
    return null;
  }
  if (typeof row.observedAtMs !== "number" || !Number.isSafeInteger(row.observedAtMs)) {
    addReason(reasons, "PROVENANCE_LINEAGE_UNAVAILABLE");
    return null;
  }
  const observedMs = row.observedAtMs;
  const periodEndMs = observedMs + 1;
  if (!Number.isSafeInteger(periodEndMs) || observedMs < request.period.start.ms || periodEndMs > request.period.end.ms) {
    reject("projection_fact_outside_period");
  }

  const inputTokens = safeMetric(row.inputTokens);
  const outputTokens = safeMetric(row.outputTokens);
  const cacheReadTokens = safeMetric(row.cacheReadTokens);
  const cacheCreationTokens = safeMetric(row.cacheCreationTokens);
  const hasCacheUsage = (cacheReadTokens ?? 0) > 0 || (cacheCreationTokens ?? 0) > 0;
  if (hasCacheUsage) {
    addReason(reasons, "CACHE_USAGE_UNREPRESENTABLE");
  }
  const cost = costMicros(row, reasons);
  if (inputTokens === null && outputTokens === null && cost === null) {
    if (hasCacheUsage) addReason(reasons, "CACHE_ONLY_USAGE_UNREPRESENTABLE");
    return null;
  }
  const observed = new Date(observedMs).toISOString();
  const projectKey = canonicalFactProjectKey(row);
  const record: CostUsageRecord = {
    id: uuidForProjection(row.projectionId),
    tenantId: request.tenantRef,
    source: row.source as ToolSource,
    periodStart: observed,
    periodEnd: new Date(periodEndMs).toISOString(),
    sourceRecordKey: row.projectionId,
    metadata: {},
  };
  if (inputTokens !== null) record.inputTokens = inputTokens;
  if (outputTokens !== null) record.outputTokens = outputTokens;
  if (projectKey !== null) record.projectKey = projectKey;
  if (cost?.channel === "reported") record.actualCostUsd = cost.usd;
  if (cost?.channel === "estimated") record.estimatedCostUsd = cost.usd;
  return record;
}

function applyCoverageReasons(
  reasons: Set<FinanceProjectionReason>,
  request: NormalizedRequest,
  control: ProjectionControlRow | undefined,
  sourceSnapshotMs: number | null,
  expiryWindows: number | null,
  financePublicationReady: boolean,
): void {
  if (!financePublicationReady) {
    addReason(reasons, "PROVENANCE_COVERAGE_UNAVAILABLE");
    addReason(reasons, "SOURCE_REVISION_UNBOUND");
  }
  if (sourceSnapshotMs !== null && request.period.end.ms > sourceSnapshotMs) addReason(reasons, "OPEN_PERIOD");
  if (control === undefined) {
    addReason(reasons, "PROJECTION_SCHEMA_UNSUPPORTED");
    return;
  }
  const controlCounts = [
    control.schemaVersion,
    control.generation,
    control.repairBacklog,
    control.dirtySessionBacklog,
    control.accountInvalidationBacklog,
    control.compactMutationBacklog,
    control.compactGcBacklog,
  ];
  const controlFlags = [
    control.ready,
    control.parityReady,
    control.backfillComplete,
    control.parityComplete,
    control.metricBackfillComplete,
    control.dirty,
  ];
  if (
    controlCounts.some((value) => !Number.isSafeInteger(value) || value < 0) ||
    controlFlags.some((value) => value !== 0 && value !== 1) ||
    expiryWindows !== null && (!Number.isSafeInteger(expiryWindows) || expiryWindows < 0)
  ) addReason(reasons, "PROJECTION_SCHEMA_UNSUPPORTED");
  if (control.schemaVersion !== DASHBOARD_SCHEMA_VERSION) addReason(reasons, "PROJECTION_SCHEMA_UNSUPPORTED");
  if (!control.ready) addReason(reasons, "PROJECTION_NOT_READY");
  if (!control.parityReady) addReason(reasons, "PROJECTION_PARITY_NOT_READY");
  if (!control.backfillComplete || !control.parityComplete || !control.metricBackfillComplete) {
    addReason(reasons, "PROJECTION_BACKFILL_INCOMPLETE");
  }
  if (control.dirty) addReason(reasons, "PROJECTION_DIRTY");
  if (control.degradedReason !== null) addReason(reasons, "PROJECTION_DEGRADED");
  if (
    control.repairBacklog > 0 ||
    control.dirtySessionBacklog > 0 ||
    control.accountInvalidationBacklog > 0 ||
    control.compactMutationBacklog > 0 ||
    control.compactGcBacklog > 0
  ) addReason(reasons, "PROJECTION_BACKLOG");
  if (expiryWindows === null) addReason(reasons, "PROJECTION_SCHEMA_UNSUPPORTED");
  else if (expiryWindows > 0) addReason(reasons, "PROJECTION_BACKLOG");
  if (sourceSnapshotMs === null) addReason(reasons, "PROJECTION_SNAPSHOT_UNAVAILABLE");
}

function readSourceHealth(
  database: Database.Database,
  request: NormalizedRequest,
  control: ProjectionControlRow | undefined,
  reasons: Set<FinanceProjectionReason>,
  sourceSnapshotMs: number | null,
): void {
  const historyAvailable = hasColumns(database, "maintenance_state", ["key", "value", "updated_at"]);
  const history = historyAvailable ? historyCoverageStatus(database) : null;
  for (const source of request.requiredSources) {
    if (history === null) {
      addReason(reasons, "HISTORY_COVERAGE_UNAVAILABLE");
    } else {
      const status = history.sources.find((candidate) => candidate.source === source);
      if (!status || status.status !== "complete") addReason(reasons, "HISTORY_COVERAGE_INCOMPLETE");
      if (status && status.invalidatedAt !== null) {
        addReason(reasons, "HISTORY_COVERAGE_INVALIDATED");
        addReason(reasons, "SOURCE_REVISION_UNBOUND");
      }
      const attempt = status?.latestFullAttempt;
      const completed = status?.lastFullScan;
      const completedMs = completed?.completedAt === null || completed?.completedAt === undefined
        ? null
        : Date.parse(completed.completedAt);
      if (
        !attempt ||
        !completed ||
        attempt.status !== "complete" ||
        !attempt.exhaustive ||
        attempt.truncated ||
        attempt.parseErrors !== 0 ||
        attempt.discoveryErrors !== 0 ||
        attempt.statErrors !== 0 ||
        attempt.readErrors !== 0 ||
        attempt.bytesDeferred !== 0 ||
        completed.completedAt === null ||
        completedMs === null ||
        !Number.isFinite(completedMs) ||
        completedMs <= request.period.end.ms
      ) addReason(reasons, "HISTORY_FULL_SCAN_NOT_CURRENT");
      if (sourceSnapshotMs !== null && completedMs !== null && Number.isFinite(completedMs)) {
        if (Number.isFinite(completedMs) && completedMs > sourceSnapshotMs) {
          addReason(reasons, "SOURCE_REVISION_UNBOUND");
        }
      }
    }

    const activity = readActivity(database, source);
    if (!activity || activity.lastScanAt === null) {
      addReason(reasons, "CAPTURE_ACTIVITY_UNAVAILABLE");
    } else {
      const scanMs = Date.parse(activity.lastScanAt);
      const staleByFixedWindow = sourceSnapshotMs !== null && sourceSnapshotMs - scanMs > MAX_CAPTURE_AGE_MS;
      const scanIsFuture = sourceSnapshotMs !== null && scanMs > sourceSnapshotMs;
      if (!Number.isFinite(scanMs) || scanMs <= request.period.end.ms || staleByFixedWindow || scanIsFuture) {
        addReason(reasons, "CAPTURE_ACTIVITY_STALE");
      }
      if (activity.hasError) {
        addReason(reasons, "CAPTURE_ACTIVITY_ERROR");
        addReason(reasons, "SOURCE_REVISION_UNBOUND");
      }
      if (activity.truncated) {
        addReason(reasons, "CAPTURE_ACTIVITY_TRUNCATED");
        addReason(reasons, "SOURCE_REVISION_UNBOUND");
      }
      if (sourceSnapshotMs !== null && Number.isFinite(scanMs) && scanMs > sourceSnapshotMs) {
        addReason(reasons, "SOURCE_REVISION_UNBOUND");
      }
      if (sourceSnapshotMs !== null && activity.lastActivityAt) {
        const activityMs = Date.parse(activity.lastActivityAt);
        if (Number.isFinite(activityMs) && activityMs > sourceSnapshotMs) {
          addReason(reasons, "SOURCE_REVISION_UNBOUND");
        }
      }
    }
  }
}

function readInTransaction(
  database: Database.Database,
  request: NormalizedRequest,
): FinanceProjectUsageProjectionResult {
  const reasons = new Set<FinanceProjectionReason>();

  // The finance read path intentionally touches only typed native tables. In
  // particular, it never joins or selects the durable raw event body.
  const bindingSchema = hasColumns(database, "collector_workspace_binding", [
    "singleton", "current_workspace_id", "current_installation_epoch_id",
  ]);
  const factSchema = hasColumns(database, "dashboard_event_facts", [
    "projection_id", "source", "event_type", "observed_at_ms", "input_tokens", "output_tokens",
    "cache_read_tokens", "cache_creation_tokens", "cost_nanos", "repo_hash", "project_key",
    "cost_kind", "raw_generation", "workspace_id", "installation_epoch_id", "live_usage_json",
  ]);
  if (!bindingSchema || !factSchema) {
    addReason(reasons, "PROVENANCE_SCHEMA_UNSUPPORTED");
    applyCoverageReasons(reasons, request, undefined, null, null, false);
    return makeProjectionResult(request, [], reasons, request.period.start.iso, 0);
  }

  const binding = database.prepare(
    `select current_workspace_id as currentWorkspaceId,
            current_installation_epoch_id as currentInstallationEpochId
     from collector_workspace_binding where singleton = 1`,
  ).get() as { currentWorkspaceId: string; currentInstallationEpochId: string | null } | undefined;
  if (binding && binding.currentWorkspaceId !== request.expectedWorkspaceId) {
    // This check intentionally happens before the fact query.
    reject("workspace_binding_mismatch");
  }
  if (!binding || !binding.currentInstallationEpochId || !UUID_PATTERN.test(binding.currentInstallationEpochId)) {
    addReason(reasons, "WORKSPACE_BINDING_UNPROVEN");
    addReason(reasons, "PROVENANCE_LINEAGE_UNAVAILABLE");
  } else {
    request.nativeInstallationEpochId = binding.currentInstallationEpochId;
  }

  const financePublication = readFinancePublication(database, request, reasons);
  const control = readProjectionControl(database);
  const expiryWindows = hasColumns(database, "dashboard_window_control", ["target_cutoff_at"])
    ? Number((database.prepare(
      `select count(*) as count from dashboard_window_control where target_cutoff_at is not null`,
    ).get() as { count: number }).count)
    : null;
  const sourceSnapshotParsed = financePublication.publishedAtMs;
  if (financePublication.publication && control &&
      financePublication.publication.projectionGeneration !== control.generation) {
    addReason(reasons, "FINANCE_PUBLICATION_REVISION_MISMATCH");
    addReason(reasons, "PROVENANCE_COVERAGE_UNAVAILABLE");
    addReason(reasons, "SOURCE_REVISION_UNBOUND");
  }
  applyCoverageReasons(
    reasons,
    request,
    control,
    sourceSnapshotParsed,
    expiryWindows,
    financePublication.ready,
  );
  if (!request.requiredSources.every((source) => SOURCE_VALUES.includes(source))) {
    addReason(reasons, "UNSUPPORTED_REQUIRED_SOURCE");
  }
  readSourceHealth(database, request, control, reasons, sourceSnapshotParsed);
  // Retained observations remain excluded even after their raw rows and ordinary
  // dashboard facts expire. The native workspace/epoch scope is still required.
  if (request.requiredSources.includes("codex") && request.nativeInstallationEpochId &&
      hasColumns(database,"dashboard_live_usage_retained",["workspace_id","installation_epoch_id","observed_at_ms","interval_start"])) {
    const retained=database.prepare(`select 1 from dashboard_live_usage_retained
      where workspace_id=? and installation_epoch_id=? and observed_at_ms>=?
        and (observed_at_ms<? or interval_start<?) limit 1`).get(
      request.expectedWorkspaceId,request.nativeInstallationEpochId,request.period.start.ms,
      request.period.end.ms,request.period.end.iso,
    );
    if(retained)addReason(reasons,"OBSERVED_INTERVAL_UNQUALIFIED");
  }

  // Any current-workspace fact without its immutable privacy generation,
  // epoch, or millisecond timestamp makes the whole native result unusable.
  // This also keeps legacy NULL-epoch facts unavailable after a transition.
  if (request.nativeInstallationEpochId) {
    const unlineaged = database.prepare(
      `select count(*) as count
       from dashboard_event_facts
       where installation_epoch_id = ?
         and (workspace_id is null or workspace_id <> ?)
         and (raw_generation is null or length(raw_generation) = 0
           or observed_at_ms is null)`,
    ).get(request.nativeInstallationEpochId, request.expectedWorkspaceId) as { count: number };
    if (unlineaged.count > 0) addReason(reasons, "PROVENANCE_LINEAGE_UNAVAILABLE");
  }

  const suppressBeforeFacts = [...reasons].some((reason) => NO_RECORD_REASONS.has(reason));
  const factRows = !suppressBeforeFacts && request.nativeInstallationEpochId
    ? readFacts(database, request)
    : [];
  if (factRows.length > MAX_SOURCE_RECORDS) reject("source_record_limit");
  const projectedRecords = suppressBeforeFacts
    ? []
    : factRows
      .map((row) => recordFromFact(row, request, reasons))
      .filter((record): record is CostUsageRecord => record !== null);

  let sourceSnapshotMs = sourceSnapshotParsed;
  if (sourceSnapshotMs === null) sourceSnapshotMs = request.period.start.ms;
  const sourceSnapshotAt = new Date(sourceSnapshotMs).toISOString();
  const canClaimThroughEnd =
    sourceSnapshotMs >= request.period.end.ms &&
    ![...reasons].some((reason) => COVERAGE_WATERMARK_BLOCKERS.has(reason));
  const coveredThrough = canClaimThroughEnd
    ? request.period.end.iso
    : request.period.start.iso;
  const records = [...reasons].some((reason) => NO_RECORD_REASONS.has(reason))
    ? []
    : projectedRecords;
  const generation = financePublication.publication &&
      Number.isSafeInteger(financePublication.publication.revision) &&
      financePublication.publication.revision >= 0
    ? financePublication.publication.revision
    : 0;
  return makeProjectionResult(request, records, reasons, sourceSnapshotAt, generation, coveredThrough);
}

function makeProjectionResult(
  request: NormalizedRequest,
  records: readonly CostUsageRecord[],
  reasons: Set<FinanceProjectionReason>,
  sourceSnapshotAt: string,
  generation: number,
  coveredThrough = request.period.start.iso,
): FinanceProjectUsageProjectionResult {
  const input: ProjectUsageExportInput = {
    envelope: {
      tenantRef: request.tenantRef,
      installationRef: request.nativeInstallationEpochId ?? UNAVAILABLE_INSTALLATION_REF,
      registryVersion: request.registryVersion,
      sourceVersion: request.sourceVersion,
      generation,
      generatedAt: sourceSnapshotAt,
      sourceUpdatedAt: sourceSnapshotAt,
      period: { start: request.period.start.iso, end: request.period.end.iso },
      coverage: {
        complete: reasons.size === 0,
        coveredThrough,
        expectedRecordCount: records.length,
      },
    },
    source: {
      records,
      billingPoolBySource: request.billingPoolBySource,
      projectMappings: request.projectMappings,
    },
  };
  return {
    input,
    reasons: Object.freeze([...reasons]),
    sourceSnapshotAt,
  };
}

/** Read one immutable projection snapshot in one SQLite read transaction. */
export function readFinanceProjectUsageProjection(
  database: Database.Database,
  request: FinanceProjectionRequest,
): FinanceProjectUsageProjectionResult {
  const normalized = normalizeRequest(request);
  if (!database || typeof database.transaction !== "function") reject("invalid_database");
  try {
    const read = database.transaction(() => readInTransaction(database, normalized));
    return read.deferred();
  } catch (error) {
    if (error instanceof FinanceProjectionRejected) throw error;
    reject("read_failed");
  }
}

/** Build the existing pure exporter only after the read transaction closes. */
export function buildFinanceProjectUsageExportFromProjection(
  database: Database.Database,
  request: FinanceProjectionRequest,
): FinanceProjectUsageExportResult {
  const projection = readFinanceProjectUsageProjection(database, request);
  const exported = buildProjectUsageExport(projection.input);
  return {
    exported,
    reasons: projection.reasons,
    sourceSnapshotAt: projection.sourceSnapshotAt,
  };
}
