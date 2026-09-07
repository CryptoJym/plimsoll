import { createHash } from "node:crypto";

import { z } from "zod";
import type { CostUsageRecord, ToolSource } from "./schemas";

/**
 * The producer side of the finance/Plimsoll bridge is deliberately small and
 * offline.  It accepts an already trusted projection and registry snapshot;
 * it never reads a ledger, starts a service, or discovers source data.
 */

export const PROJECT_USAGE_EXPORT_SCHEMA_VERSION = "plimsoll-project-usage.v1" as const;

export const PROJECT_USAGE_EXPORT_LIMITS = {
  maxSourceRecords: 10_000,
  maxAggregateRows: 2_000,
  maxCanonicalBytes: 2 * 1024 * 1024,
  maxCostDigits: 24,
} as const;

const MAX_SAFE_BIGINT = BigInt(Number.MAX_SAFE_INTEGER);
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HASH_PATTERN = /^[a-f0-9]{64}$/;
const VERSION_PATTERN = /^[A-Za-z0-9._-]{1,64}$/;
const MICROS_PATTERN = /^(0|[1-9][0-9]*)$/;
const TIMESTAMP_PATTERN =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/;

const TOOL_SOURCES = [
  "anthropic_admin",
  "anthropic_usage",
  "claude_code",
  "codex",
  "gemini_cli",
  "github",
  "openai_usage",
  "manual",
  "unknown",
] as const satisfies readonly ToolSource[];
const toolSourceSchema = z.enum(TOOL_SOURCES);
const uuidSchema = z.string().regex(UUID_PATTERN);
const versionSchema = z.string().regex(VERSION_PATTERN);
const safeNonnegativeIntegerSchema = z.number().finite().refine(Number.isSafeInteger).nonnegative();
const microsSchema = z.string().regex(MICROS_PATTERN);

export type ProjectUsageUuid = string;
export type ProjectUsageTimestamp = string;
export type ProjectUsageVersion = string;

export type ProjectUsageAttribution =
  | "APPROVED_MAPPING"
  | "UNALLOCATED"
  | "EXCLUDED";

export interface ProjectUsagePeriod {
  start: ProjectUsageTimestamp;
  end: ProjectUsageTimestamp;
}

export interface ProjectUsageCoverage {
  complete: boolean;
  coveredThrough: ProjectUsageTimestamp;
  expectedRecordCount: number;
}

export interface ProjectUsageExportEnvelope {
  tenantRef: ProjectUsageUuid;
  installationRef: ProjectUsageUuid;
  registryVersion: ProjectUsageVersion;
  sourceVersion: ProjectUsageVersion;
  generation: number;
  generatedAt: ProjectUsageTimestamp;
  sourceUpdatedAt: ProjectUsageTimestamp;
  period: ProjectUsagePeriod;
  coverage: ProjectUsageCoverage;
}

export interface ProjectUsageProjectMapping {
  projectKey: string;
  companyRef: ProjectUsageUuid | null;
  projectRef: ProjectUsageUuid | null;
  effectiveFrom: ProjectUsageTimestamp;
  effectiveTo: ProjectUsageTimestamp | null;
  attribution: Exclude<ProjectUsageAttribution, "UNALLOCATED">;
}

export interface ProjectUsageSourceInput {
  records: readonly CostUsageRecord[];
  billingPoolBySource: Readonly<Partial<Record<ToolSource, ProjectUsageUuid>>>;
  projectMappings: readonly ProjectUsageProjectMapping[];
}

export interface ProjectUsageExportInput {
  envelope: ProjectUsageExportEnvelope;
  source: ProjectUsageSourceInput;
}

export interface ProjectUsageMetrics {
  recordCount: number;
  inputTokens: number | null;
  outputTokens: number | null;
  reportedCostMicros: string | null;
  estimatedCostMicros: string | null;
  unpricedRecordCount: number;
}

export interface ProjectUsageRow {
  recordId: string;
  companyRef: ProjectUsageUuid | null;
  projectRef: ProjectUsageUuid | null;
  billingPoolRef: ProjectUsageUuid;
  attribution: ProjectUsageAttribution;
  metrics: ProjectUsageMetrics;
}

export interface ProjectUsageExport {
  schemaVersion: typeof PROJECT_USAGE_EXPORT_SCHEMA_VERSION;
  exportId: string;
  tenantRef: ProjectUsageUuid;
  installationRef: ProjectUsageUuid;
  registryVersion: ProjectUsageVersion;
  sourceVersion: ProjectUsageVersion;
  generation: number;
  generatedAt: ProjectUsageTimestamp;
  sourceUpdatedAt: ProjectUsageTimestamp;
  period: ProjectUsagePeriod;
  coverage: ProjectUsageCoverage;
  rows: readonly ProjectUsageRow[];
  totals: ProjectUsageMetrics;
}

interface ParsedTimestamp {
  ms: number;
  canonical: ProjectUsageTimestamp;
}

interface NormalizedEnvelope {
  tenantRef: string;
  installationRef: string;
  registryVersion: string;
  sourceVersion: string;
  generation: number;
  generatedAt: ParsedTimestamp;
  sourceUpdatedAt: ParsedTimestamp;
  period: { start: ParsedTimestamp; end: ParsedTimestamp };
  coverage: {
    complete: boolean;
    coveredThrough: ParsedTimestamp;
    expectedRecordCount: number;
  };
}

interface NormalizedMapping {
  projectKey: string;
  companyRef: string | null;
  projectRef: string | null;
  effectiveFrom: ParsedTimestamp;
  effectiveTo: ParsedTimestamp | null;
  attribution: Exclude<ProjectUsageAttribution, "UNALLOCATED">;
}

interface NormalizedRecord {
  sourceRecordKey: string;
  tenantId: string;
  source: ToolSource;
  periodStart: ParsedTimestamp;
  periodEnd: ParsedTimestamp;
  projectKey: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  reportedCostMicros: bigint | null;
  estimatedCostMicros: bigint | null;
}

interface Aggregate {
  recordCount: number;
  inputTokens: bigint;
  inputTokensKnown: boolean;
  outputTokens: bigint;
  outputTokensKnown: boolean;
  reportedCostMicros: bigint;
  reportedCostKnown: boolean;
  estimatedCostMicros: bigint;
  estimatedCostKnown: boolean;
  unpricedRecordCount: number;
}

function reject(code: string): never {
  // Keep errors useful to a caller without echoing a rejected identifier,
  // metadata value, source key, or other input content.
  throw new Error(`finance_project_usage_export_rejected:${code}`);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return false;
  }
  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertPlainObject(value: unknown, code = "object_required"): asserts value is Record<string, unknown> {
  if (!isPlainObject(value)) reject(code);
}

function assertExactKeys(value: Record<string, unknown>, expected: readonly string[]): void {
  const actual = Object.keys(value).sort();
  const wanted = [...expected].sort();
  if (actual.length !== wanted.length || actual.some((key, index) => key !== wanted[index])) {
    reject("unknown_or_missing_property");
  }
}

function assertUuid(value: unknown): string {
  const parsed = uuidSchema.safeParse(value);
  if (!parsed.success) reject("invalid_uuid");
  return parsed.data;
}

function assertVersion(value: unknown): string {
  const parsed = versionSchema.safeParse(value);
  if (!parsed.success) reject("invalid_version");
  return parsed.data;
}

function assertSafeNonnegativeInteger(value: unknown): number {
  const parsed = safeNonnegativeIntegerSchema.safeParse(value);
  if (!parsed.success) reject("invalid_safe_integer");
  return parsed.data;
}

function assertTimestamp(
  value: unknown,
  options: { canonicalRequired: boolean },
): ParsedTimestamp {
  if (typeof value !== "string" || value !== value.trim()) reject("invalid_timestamp");
  const match = TIMESTAMP_PATTERN.exec(value);
  if (!match) reject("invalid_timestamp");

  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const fraction = match[7] ?? "";
  const zone = match[8]!;

  if (fraction.length > 3 && /[^0]/.test(fraction.slice(3))) {
    reject("timestamp_precision");
  }
  if (zone !== "Z") {
    const offsetHour = Number(zone.slice(1, 3));
    const offsetMinute = Number(zone.slice(4, 6));
    if (offsetHour > 23 || offsetMinute > 59) reject("invalid_timestamp");
  }

  const millisecond = Number(fraction.padEnd(3, "0"));
  // Date.parse normalizes some invalid calendar dates (for example February
  // 30). Check the wall-clock components before accepting it.
  const wallClock = new Date(0);
  wallClock.setUTCFullYear(year, month - 1, day);
  wallClock.setUTCHours(hour, minute, second, millisecond);
  if (
    wallClock.getUTCFullYear() !== year ||
    wallClock.getUTCMonth() !== month - 1 ||
    wallClock.getUTCDate() !== day ||
    wallClock.getUTCHours() !== hour ||
    wallClock.getUTCMinutes() !== minute ||
    wallClock.getUTCSeconds() !== second ||
    wallClock.getUTCMilliseconds() !== millisecond
  ) {
    reject("invalid_timestamp");
  }

  const ms = Date.parse(value);
  if (!Number.isFinite(ms)) reject("invalid_timestamp");
  const canonical = new Date(ms).toISOString();
  if (options.canonicalRequired && value !== canonical) reject("noncanonical_timestamp");
  return { ms, canonical };
}

function assertOptionalString(value: unknown, allowNull = true): string | null {
  if (value === undefined || (allowNull && value === null)) return null;
  if (typeof value !== "string") reject("invalid_string");
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function assertRecordIdentity(value: unknown): string {
  const identity = assertOptionalString(value, false);
  if (identity === null) reject("invalid_record_identity");
  return identity;
}

function assertOptionalTokenCount(value: unknown): number | null {
  if (value === undefined || value === null) return null;
  return assertSafeNonnegativeInteger(value);
}

function parseDecimalMicros(value: unknown): bigint | null {
  if (value === undefined || value === null) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    reject("invalid_cost");
  }
  if (Math.abs(value) > Number.MAX_SAFE_INTEGER) reject("unsafe_cost");

  const text = Object.is(value, -0) ? "0" : value.toString();
  const parts = /^([0-9]+)(?:\.([0-9]*))?(?:e([+-]?[0-9]+))?$/i.exec(text);
  if (!parts) reject("invalid_cost");

  const integerPart = parts[1]!;
  const fractionPart = parts[2] ?? "";
  const exponent = parts[3] === undefined ? 0 : Number(parts[3]);
  if (!Number.isSafeInteger(exponent)) reject("unsafe_cost");

  const digits = BigInt(`${integerPart}${fractionPart}` || "0");
  const decimalPlaces = fractionPart.length - exponent;
  let micros: bigint;
  if (decimalPlaces > 6) {
    const divisor = 10n ** BigInt(decimalPlaces - 6);
    if (digits % divisor !== 0n) reject("cost_precision");
    micros = digits / divisor;
  } else if (decimalPlaces < 6) {
    micros = digits * 10n ** BigInt(6 - decimalPlaces);
  } else {
    micros = digits;
  }

  if (micros.toString().length > PROJECT_USAGE_EXPORT_LIMITS.maxCostDigits) {
    reject("cost_out_of_range");
  }
  return micros;
}

function assertMicrosString(value: unknown): string | null {
  if (value === null) return null;
  const parsed = microsSchema.safeParse(value);
  if (!parsed.success) reject("invalid_cost_micros");
  const micros = parsed.data;
  if (micros.length > PROJECT_USAGE_EXPORT_LIMITS.maxCostDigits) reject("cost_out_of_range");
  return micros;
}

/** Stable JSON used for export identity and byte-size checks. */
export function canonicalProjectUsageJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) reject("nonfinite_json_number");
    return JSON.stringify(Object.is(value, -0) ? 0 : value);
  }
  if (Array.isArray(value)) {
    const items: string[] = [];
    for (let index = 0; index < value.length; index += 1) {
      if (!Object.prototype.hasOwnProperty.call(value, index)) reject("sparse_json_array");
      items.push(canonicalProjectUsageJson(value[index]));
    }
    return `[${items.join(",")}]`;
  }
  if (typeof value === "object") {
    if (!isPlainObject(value)) reject("nonplain_json_object");
    const entries = Object.keys(value)
      .sort()
      .map((key) => {
        const nested = value[key];
        if (nested === undefined) reject("undefined_json_value");
        return `${JSON.stringify(key)}:${canonicalProjectUsageJson(nested)}`;
      });
    return `{${entries.join(",")}}`;
  }
  reject("unsupported_json_value");
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function normalizeEnvelope(value: unknown): NormalizedEnvelope {
  assertPlainObject(value);
  assertExactKeys(value, [
    "tenantRef",
    "installationRef",
    "registryVersion",
    "sourceVersion",
    "generation",
    "generatedAt",
    "sourceUpdatedAt",
    "period",
    "coverage",
  ]);

  const tenantRef = assertUuid(value.tenantRef);
  const installationRef = assertUuid(value.installationRef);
  const registryVersion = assertVersion(value.registryVersion);
  const sourceVersion = assertVersion(value.sourceVersion);
  const generation = assertSafeNonnegativeInteger(value.generation);
  const generatedAt = assertTimestamp(value.generatedAt, { canonicalRequired: true });
  const sourceUpdatedAt = assertTimestamp(value.sourceUpdatedAt, { canonicalRequired: true });

  assertPlainObject(value.period);
  assertExactKeys(value.period, ["start", "end"]);
  const periodStart = assertTimestamp(value.period.start, { canonicalRequired: true });
  const periodEnd = assertTimestamp(value.period.end, { canonicalRequired: true });
  if (periodStart.ms >= periodEnd.ms) reject("invalid_period");

  assertPlainObject(value.coverage);
  assertExactKeys(value.coverage, ["complete", "coveredThrough", "expectedRecordCount"]);
  if (typeof value.coverage.complete !== "boolean") reject("invalid_coverage");
  const coveredThrough = assertTimestamp(value.coverage.coveredThrough, { canonicalRequired: true });
  const expectedRecordCount = assertSafeNonnegativeInteger(value.coverage.expectedRecordCount);
  if (expectedRecordCount > PROJECT_USAGE_EXPORT_LIMITS.maxSourceRecords) {
    reject("source_record_limit");
  }
  if (coveredThrough.ms < periodStart.ms || coveredThrough.ms > periodEnd.ms) {
    reject("coverage_out_of_period");
  }
  if (sourceUpdatedAt.ms > generatedAt.ms || sourceUpdatedAt.ms < coveredThrough.ms) {
    reject("invalid_time_relationship");
  }

  return {
    tenantRef,
    installationRef,
    registryVersion,
    sourceVersion,
    generation,
    generatedAt,
    sourceUpdatedAt,
    period: { start: periodStart, end: periodEnd },
    coverage: { complete: value.coverage.complete, coveredThrough, expectedRecordCount },
  };
}

function normalizeMappings(value: unknown): NormalizedMapping[] {
  if (!Array.isArray(value)) reject("invalid_project_mappings");
  const mappings: NormalizedMapping[] = [];
  for (const mapping of value) {
    assertPlainObject(mapping);
    assertExactKeys(mapping, [
      "projectKey",
      "companyRef",
      "projectRef",
      "effectiveFrom",
      "effectiveTo",
      "attribution",
    ]);

    const projectKey = assertOptionalString(mapping.projectKey, false);
    if (projectKey === null) reject("invalid_project_key");
    const companyRef = mapping.companyRef === null ? null : assertUuid(mapping.companyRef);
    const projectRef = mapping.projectRef === null ? null : assertUuid(mapping.projectRef);
    if ((companyRef === null) !== (projectRef === null)) reject("invalid_mapping_pair");

    if (mapping.attribution !== "APPROVED_MAPPING" && mapping.attribution !== "EXCLUDED") {
      reject("invalid_mapping_attribution");
    }
    if (mapping.attribution === "APPROVED_MAPPING" && (companyRef === null || projectRef === null)) {
      reject("invalid_mapping_pair");
    }

    const effectiveFrom = assertTimestamp(mapping.effectiveFrom, { canonicalRequired: false });
    const effectiveTo =
      mapping.effectiveTo === null
        ? null
        : assertTimestamp(mapping.effectiveTo, { canonicalRequired: false });
    if (effectiveTo !== null && effectiveFrom.ms >= effectiveTo.ms) reject("invalid_mapping_window");
    mappings.push({ projectKey, companyRef, projectRef, effectiveFrom, effectiveTo, attribution: mapping.attribution });
  }

  // Adjacent effective windows are valid. Any actual overlap is ambiguous,
  // even if a particular record would not happen to use the overlap. Sort
  // copies per key so a large registry stays bounded instead of requiring an
  // O(n^2) pair scan.
  const mappingsByKey = new Map<string, NormalizedMapping[]>();
  for (const mapping of mappings) {
    const byKey = mappingsByKey.get(mapping.projectKey);
    if (byKey === undefined) mappingsByKey.set(mapping.projectKey, [mapping]);
    else byKey.push(mapping);
  }
  for (const byKey of mappingsByKey.values()) {
    const ordered = [...byKey].sort((left, right) => left.effectiveFrom.ms - right.effectiveFrom.ms);
    let previousEnd = Number.NEGATIVE_INFINITY;
    for (const mapping of ordered) {
      if (mapping.effectiveFrom.ms < previousEnd) reject("overlapping_project_mappings");
      previousEnd = mapping.effectiveTo?.ms ?? Number.POSITIVE_INFINITY;
    }
  }
  return mappings;
}

function normalizeRecord(value: unknown, envelope: NormalizedEnvelope): NormalizedRecord {
  assertPlainObject(value);
  const id = assertRecordIdentity(value.id);
  const tenantId = assertRecordIdentity(value.tenantId);
  if (tenantId !== envelope.tenantRef) reject("tenant_mismatch");

  const sourceResult = toolSourceSchema.safeParse(value.source);
  if (!sourceResult.success) reject("invalid_source");
  const source = sourceResult.data;
  const sourceRecordKey = assertRecordIdentity(value.sourceRecordKey);
  const periodStart = assertTimestamp(value.periodStart, { canonicalRequired: false });
  const periodEnd = assertTimestamp(value.periodEnd, { canonicalRequired: false });
  if (periodStart.ms > periodEnd.ms) reject("invalid_record_window");
  if (periodStart.ms < envelope.period.start.ms || periodEnd.ms > envelope.period.end.ms) {
    reject("record_outside_period");
  }

  // These fields are intentionally read only for their approved vocabulary;
  // all raw IDs, models, hashes, metadata, and keys are dropped below.
  const projectKey = assertOptionalString(value.projectKey);
  const inputTokens = assertOptionalTokenCount(value.inputTokens);
  const outputTokens = assertOptionalTokenCount(value.outputTokens);
  const reportedCostMicros = parseDecimalMicros(value.actualCostUsd);
  const estimatedCostMicros = parseDecimalMicros(value.estimatedCostUsd);

  // Keep the required source identity check explicit so a future compiler
  // relaxation cannot make an omitted CostUsageRecord id look valid.
  void id;
  return {
    sourceRecordKey,
    tenantId,
    source,
    periodStart,
    periodEnd,
    projectKey,
    inputTokens,
    outputTokens,
    reportedCostMicros,
    estimatedCostMicros,
  };
}

function sameRelevantRecord(left: NormalizedRecord, right: NormalizedRecord): boolean {
  return (
    left.tenantId === right.tenantId &&
    left.source === right.source &&
    left.periodStart.ms === right.periodStart.ms &&
    left.periodEnd.ms === right.periodEnd.ms &&
    left.projectKey === right.projectKey &&
    left.inputTokens === right.inputTokens &&
    left.outputTokens === right.outputTokens &&
    left.reportedCostMicros === right.reportedCostMicros &&
    left.estimatedCostMicros === right.estimatedCostMicros
  );
}

function createAggregate(): Aggregate {
  return {
    recordCount: 0,
    inputTokens: 0n,
    inputTokensKnown: true,
    outputTokens: 0n,
    outputTokensKnown: true,
    reportedCostMicros: 0n,
    reportedCostKnown: true,
    estimatedCostMicros: 0n,
    estimatedCostKnown: true,
    unpricedRecordCount: 0,
  };
}

function addRecord(aggregate: Aggregate, record: NormalizedRecord): void {
  aggregate.recordCount += 1;
  if (!Number.isSafeInteger(aggregate.recordCount)) reject("aggregate_out_of_range");

  if (record.inputTokens === null) aggregate.inputTokensKnown = false;
  else aggregate.inputTokens += BigInt(record.inputTokens);
  if (record.outputTokens === null) aggregate.outputTokensKnown = false;
  else aggregate.outputTokens += BigInt(record.outputTokens);
  if (record.reportedCostMicros === null) aggregate.reportedCostKnown = false;
  else aggregate.reportedCostMicros += record.reportedCostMicros;
  if (record.estimatedCostMicros === null) aggregate.estimatedCostKnown = false;
  else aggregate.estimatedCostMicros += record.estimatedCostMicros;
  if (record.reportedCostMicros === null && record.estimatedCostMicros === null) {
    aggregate.unpricedRecordCount += 1;
  }
}

function safeNumber(value: bigint): number {
  if (value < 0n || value > MAX_SAFE_BIGINT) reject("aggregate_out_of_range");
  return Number(value);
}

function aggregateToMetrics(aggregate: Aggregate): ProjectUsageMetrics {
  if (aggregate.recordCount === 0) {
    return {
      recordCount: 0,
      inputTokens: 0,
      outputTokens: 0,
      reportedCostMicros: "0",
      estimatedCostMicros: "0",
      unpricedRecordCount: 0,
    };
  }
  const reportedCostMicros = aggregate.reportedCostKnown
    ? aggregate.reportedCostMicros.toString()
    : null;
  const estimatedCostMicros = aggregate.estimatedCostKnown
    ? aggregate.estimatedCostMicros.toString()
    : null;
  if (
    reportedCostMicros !== null &&
    reportedCostMicros.length > PROJECT_USAGE_EXPORT_LIMITS.maxCostDigits
  ) {
    reject("cost_out_of_range");
  }
  if (
    estimatedCostMicros !== null &&
    estimatedCostMicros.length > PROJECT_USAGE_EXPORT_LIMITS.maxCostDigits
  ) {
    reject("cost_out_of_range");
  }
  return {
    recordCount: aggregate.recordCount,
    inputTokens: aggregate.inputTokensKnown ? safeNumber(aggregate.inputTokens) : null,
    outputTokens: aggregate.outputTokensKnown ? safeNumber(aggregate.outputTokens) : null,
    reportedCostMicros,
    estimatedCostMicros,
    unpricedRecordCount: aggregate.unpricedRecordCount,
  };
}

function mappingForRecord(
  record: NormalizedRecord,
  mappings: readonly NormalizedMapping[],
): { companyRef: string | null; projectRef: string | null; attribution: ProjectUsageAttribution } {
  if (record.projectKey === null) {
    return { companyRef: null, projectRef: null, attribution: "UNALLOCATED" };
  }
  const matches = mappings.filter(
    (mapping) =>
      mapping.projectKey === record.projectKey &&
      mapping.effectiveFrom.ms <= record.periodStart.ms &&
      (mapping.effectiveTo === null || record.periodEnd.ms <= mapping.effectiveTo.ms),
  );
  if (matches.length === 0) {
    return { companyRef: null, projectRef: null, attribution: "UNALLOCATED" };
  }
  if (matches.length !== 1) reject("ambiguous_project_mapping");
  const mapping = matches[0]!;
  if (mapping.attribution === "EXCLUDED") {
    return { companyRef: null, projectRef: null, attribution: "EXCLUDED" };
  }
  if (mapping.companyRef === null || mapping.projectRef === null) reject("invalid_mapping_pair");
  return {
    companyRef: mapping.companyRef,
    projectRef: mapping.projectRef,
    attribution: "APPROVED_MAPPING",
  };
}

function rowWithoutRecordId(row: Omit<ProjectUsageRow, "recordId">): Omit<ProjectUsageRow, "recordId"> {
  return {
    companyRef: row.companyRef,
    projectRef: row.projectRef,
    billingPoolRef: row.billingPoolRef,
    attribution: row.attribution,
    metrics: {
      recordCount: row.metrics.recordCount,
      inputTokens: row.metrics.inputTokens,
      outputTokens: row.metrics.outputTokens,
      reportedCostMicros: row.metrics.reportedCostMicros,
      estimatedCostMicros: row.metrics.estimatedCostMicros,
      unpricedRecordCount: row.metrics.unpricedRecordCount,
    },
  };
}

function rowIdentity(row: Omit<ProjectUsageRow, "recordId">): string {
  return sha256(canonicalProjectUsageJson(rowWithoutRecordId(row)));
}

function exportCore(value: Omit<ProjectUsageExport, "exportId">): Omit<ProjectUsageExport, "exportId"> {
  return {
    schemaVersion: value.schemaVersion,
    tenantRef: value.tenantRef,
    installationRef: value.installationRef,
    registryVersion: value.registryVersion,
    sourceVersion: value.sourceVersion,
    generation: value.generation,
    generatedAt: value.generatedAt,
    sourceUpdatedAt: value.sourceUpdatedAt,
    period: { start: value.period.start, end: value.period.end },
    coverage: {
      complete: value.coverage.complete,
      coveredThrough: value.coverage.coveredThrough,
      expectedRecordCount: value.coverage.expectedRecordCount,
    },
    rows: value.rows.map((row) => ({
      recordId: row.recordId,
      companyRef: row.companyRef,
      projectRef: row.projectRef,
      billingPoolRef: row.billingPoolRef,
      attribution: row.attribution,
      metrics: {
        recordCount: row.metrics.recordCount,
        inputTokens: row.metrics.inputTokens,
        outputTokens: row.metrics.outputTokens,
        reportedCostMicros: row.metrics.reportedCostMicros,
        estimatedCostMicros: row.metrics.estimatedCostMicros,
        unpricedRecordCount: row.metrics.unpricedRecordCount,
      },
    })),
    totals: {
      recordCount: value.totals.recordCount,
      inputTokens: value.totals.inputTokens,
      outputTokens: value.totals.outputTokens,
      reportedCostMicros: value.totals.reportedCostMicros,
      estimatedCostMicros: value.totals.estimatedCostMicros,
      unpricedRecordCount: value.totals.unpricedRecordCount,
    },
  };
}

function groupKey(
  mapping: { companyRef: string | null; projectRef: string | null; attribution: ProjectUsageAttribution },
  billingPoolRef: string,
): string {
  return `${mapping.companyRef ?? "null"}/${mapping.projectRef ?? "null"}/${billingPoolRef}/${mapping.attribution}`;
}

/**
 * Build a complete, privacy-safe aggregate from caller-supplied records.
 * Every source record is either represented in one aggregate row or causes a
 * rejection; no tail/rolling-window truncation is performed.
 */
export function buildProjectUsageExport(input: ProjectUsageExportInput): ProjectUsageExport {
  assertPlainObject(input);
  assertExactKeys(input, ["envelope", "source"]);
  const envelope = normalizeEnvelope(input.envelope);

  assertPlainObject(input.source);
  assertExactKeys(input.source, ["records", "billingPoolBySource", "projectMappings"]);
  if (!Array.isArray(input.source.records)) reject("invalid_source_records");
  if (input.source.records.length > PROJECT_USAGE_EXPORT_LIMITS.maxSourceRecords) {
    reject("source_record_limit");
  }
  assertPlainObject(input.source.billingPoolBySource);
  for (const [source, billingPoolRef] of Object.entries(input.source.billingPoolBySource)) {
    if (!TOOL_SOURCES.includes(source as ToolSource)) reject("invalid_billing_pool_lookup");
    assertUuid(billingPoolRef);
  }
  const mappings = normalizeMappings(input.source.projectMappings);

  const deduplicated = new Map<string, NormalizedRecord>();
  for (const rawRecord of input.source.records) {
    const record = normalizeRecord(rawRecord, envelope);
    const previous = deduplicated.get(record.sourceRecordKey);
    if (previous !== undefined) {
      if (!sameRelevantRecord(previous, record)) reject("conflicting_duplicate");
      continue;
    }
    deduplicated.set(record.sourceRecordKey, record);
  }

  const uniqueRecordCount = deduplicated.size;
  if (uniqueRecordCount > envelope.coverage.expectedRecordCount) reject("coverage_count_exceeds_expected");
  if (
    envelope.coverage.complete &&
    (uniqueRecordCount !== envelope.coverage.expectedRecordCount ||
      envelope.coverage.coveredThrough.ms !== envelope.period.end.ms)
  ) {
    reject("complete_coverage_mismatch");
  }

  const aggregates = new Map<string, { mapping: ReturnType<typeof mappingForRecord>; billingPoolRef: string; aggregate: Aggregate }>();
  const totalsAggregate = createAggregate();
  for (const record of deduplicated.values()) {
    const billingPoolRef = input.source.billingPoolBySource[record.source];
    if (typeof billingPoolRef !== "string") reject("missing_billing_pool");
    const mapping = mappingForRecord(record, mappings);
    const key = groupKey(mapping, billingPoolRef);
    let grouped = aggregates.get(key);
    if (grouped === undefined) {
      if (aggregates.size >= PROJECT_USAGE_EXPORT_LIMITS.maxAggregateRows) {
        reject("aggregate_row_limit");
      }
      grouped = { mapping, billingPoolRef, aggregate: createAggregate() };
      aggregates.set(key, grouped);
    }
    addRecord(grouped.aggregate, record);
    addRecord(totalsAggregate, record);
  }

  const rowsWithIds: ProjectUsageRow[] = [];
  const rowIds = new Set<string>();
  for (const grouped of aggregates.values()) {
    const rowBody: Omit<ProjectUsageRow, "recordId"> = {
      companyRef: grouped.mapping.companyRef,
      projectRef: grouped.mapping.projectRef,
      billingPoolRef: grouped.billingPoolRef,
      attribution: grouped.mapping.attribution,
      metrics: aggregateToMetrics(grouped.aggregate),
    };
    const recordId = rowIdentity(rowBody);
    if (rowIds.has(recordId)) reject("duplicate_row_identity");
    rowIds.add(recordId);
    rowsWithIds.push({ recordId, ...rowBody });
  }
  rowsWithIds.sort((left, right) => (left.recordId < right.recordId ? -1 : left.recordId > right.recordId ? 1 : 0));

  const core: Omit<ProjectUsageExport, "exportId"> = {
    schemaVersion: PROJECT_USAGE_EXPORT_SCHEMA_VERSION,
    tenantRef: envelope.tenantRef,
    installationRef: envelope.installationRef,
    registryVersion: envelope.registryVersion,
    sourceVersion: envelope.sourceVersion,
    generation: envelope.generation,
    generatedAt: envelope.generatedAt.canonical,
    sourceUpdatedAt: envelope.sourceUpdatedAt.canonical,
    period: { start: envelope.period.start.canonical, end: envelope.period.end.canonical },
    coverage: {
      complete: envelope.coverage.complete,
      coveredThrough: envelope.coverage.coveredThrough.canonical,
      expectedRecordCount: envelope.coverage.expectedRecordCount,
    },
    rows: rowsWithIds,
    totals: aggregateToMetrics(totalsAggregate),
  };
  const exportValue: ProjectUsageExport = {
    ...core,
    exportId: sha256(canonicalProjectUsageJson(exportCore(core))),
  };
  // Full shape/integrity validation also proves the producer did not leave a
  // duplicate group, unsorted row, or inconsistent total behind.
  serializeProjectUsageExport(exportValue);
  return exportValue;
}

function assertMetrics(value: unknown, allowZero = false): asserts value is ProjectUsageMetrics {
  assertPlainObject(value);
  assertExactKeys(value, [
    "recordCount",
    "inputTokens",
    "outputTokens",
    "reportedCostMicros",
    "estimatedCostMicros",
    "unpricedRecordCount",
  ]);
  const recordCount = assertSafeNonnegativeInteger(value.recordCount);
  const inputTokens = value.inputTokens === null ? null : assertSafeNonnegativeInteger(value.inputTokens);
  const outputTokens = value.outputTokens === null ? null : assertSafeNonnegativeInteger(value.outputTokens);
  const reportedCostMicros = assertMicrosString(value.reportedCostMicros);
  const estimatedCostMicros = assertMicrosString(value.estimatedCostMicros);
  const unpricedRecordCount = assertSafeNonnegativeInteger(value.unpricedRecordCount);
  if (unpricedRecordCount > recordCount) reject("invalid_metric_counts");
  if (!allowZero && recordCount === 0) reject("empty_aggregate_row");
  if (recordCount === 0) {
    if (
      inputTokens !== 0 ||
      outputTokens !== 0 ||
      reportedCostMicros !== "0" ||
      estimatedCostMicros !== "0" ||
      unpricedRecordCount !== 0
    ) {
      reject("invalid_zero_metrics");
    }
  }
}

function assertMetricMatchesRows(
  rows: readonly ProjectUsageRow[],
  totals: ProjectUsageMetrics,
): void {
  const rowRecordCount = rows.reduce((sum, row) => sum + row.metrics.recordCount, 0);
  const rowUnpricedCount = rows.reduce((sum, row) => sum + row.metrics.unpricedRecordCount, 0);
  if (!Number.isSafeInteger(rowRecordCount) || !Number.isSafeInteger(rowUnpricedCount)) {
    reject("aggregate_out_of_range");
  }
  if (totals.recordCount !== rowRecordCount || totals.unpricedRecordCount !== rowUnpricedCount) {
    reject("totals_mismatch");
  }

  const metricFields = [
    "inputTokens",
    "outputTokens",
    "reportedCostMicros",
    "estimatedCostMicros",
  ] as const;
  for (const field of metricFields) {
    const missing = rows.some((row) => row.metrics[field] === null);
    if (missing) {
      if (totals[field] !== null) reject("totals_mismatch");
      continue;
    }
    if (field === "inputTokens" || field === "outputTokens") {
      const sum = rows.reduce((total, row) => total + BigInt(row.metrics[field] as number), 0n);
      if (sum > MAX_SAFE_BIGINT || totals[field] !== Number(sum)) reject("totals_mismatch");
    } else {
      const sum = rows.reduce((total, row) => total + BigInt(row.metrics[field] as string), 0n);
      if (sum.toString().length > PROJECT_USAGE_EXPORT_LIMITS.maxCostDigits || totals[field] !== sum.toString()) {
        reject("totals_mismatch");
      }
    }
  }
}

function validateProjectUsageExport(value: unknown): asserts value is ProjectUsageExport {
  assertPlainObject(value);
  assertExactKeys(value, [
    "schemaVersion",
    "exportId",
    "tenantRef",
    "installationRef",
    "registryVersion",
    "sourceVersion",
    "generation",
    "generatedAt",
    "sourceUpdatedAt",
    "period",
    "coverage",
    "rows",
    "totals",
  ]);
  if (value.schemaVersion !== PROJECT_USAGE_EXPORT_SCHEMA_VERSION) reject("invalid_schema_version");
  if (typeof value.exportId !== "string" || !HASH_PATTERN.test(value.exportId)) reject("invalid_export_id");
  assertUuid(value.tenantRef);
  assertUuid(value.installationRef);
  assertVersion(value.registryVersion);
  assertVersion(value.sourceVersion);
  assertSafeNonnegativeInteger(value.generation);
  const generatedAt = assertTimestamp(value.generatedAt, { canonicalRequired: true });
  const sourceUpdatedAt = assertTimestamp(value.sourceUpdatedAt, { canonicalRequired: true });

  assertPlainObject(value.period);
  assertExactKeys(value.period, ["start", "end"]);
  const periodStart = assertTimestamp(value.period.start, { canonicalRequired: true });
  const periodEnd = assertTimestamp(value.period.end, { canonicalRequired: true });
  if (periodStart.ms >= periodEnd.ms) reject("invalid_period");

  assertPlainObject(value.coverage);
  assertExactKeys(value.coverage, ["complete", "coveredThrough", "expectedRecordCount"]);
  if (typeof value.coverage.complete !== "boolean") reject("invalid_coverage");
  const coveredThrough = assertTimestamp(value.coverage.coveredThrough, { canonicalRequired: true });
  const expectedRecordCount = assertSafeNonnegativeInteger(value.coverage.expectedRecordCount);
  if (coveredThrough.ms < periodStart.ms || coveredThrough.ms > periodEnd.ms) reject("coverage_out_of_period");
  if (sourceUpdatedAt.ms > generatedAt.ms || sourceUpdatedAt.ms < coveredThrough.ms) {
    reject("invalid_time_relationship");
  }

  if (!Array.isArray(value.rows) || value.rows.length > PROJECT_USAGE_EXPORT_LIMITS.maxAggregateRows) {
    reject("invalid_rows");
  }
  const rows: ProjectUsageRow[] = [];
  const rowIds = new Set<string>();
  const groupKeys = new Set<string>();
  for (let index = 0; index < value.rows.length; index += 1) {
    const row = value.rows[index];
    assertPlainObject(row);
    assertExactKeys(row, ["recordId", "companyRef", "projectRef", "billingPoolRef", "attribution", "metrics"]);
    if (typeof row.recordId !== "string" || !HASH_PATTERN.test(row.recordId)) reject("invalid_record_id");
    if (row.companyRef !== null) assertUuid(row.companyRef);
    if (row.projectRef !== null) assertUuid(row.projectRef);
    if ((row.companyRef === null) !== (row.projectRef === null)) reject("invalid_mapping_pair");
    assertUuid(row.billingPoolRef);
    if (
      row.attribution !== "APPROVED_MAPPING" &&
      row.attribution !== "UNALLOCATED" &&
      row.attribution !== "EXCLUDED"
    ) {
      reject("invalid_attribution");
    }
    if (row.attribution === "APPROVED_MAPPING" && (row.companyRef === null || row.projectRef === null)) {
      reject("invalid_mapping_pair");
    }
    if (row.attribution !== "APPROVED_MAPPING" && (row.companyRef !== null || row.projectRef !== null)) {
      reject("invalid_mapping_pair");
    }
    assertMetrics(row.metrics);
    const normalizedRow = row as unknown as ProjectUsageRow;
    const body = rowWithoutRecordId(normalizedRow);
    if (sha256(canonicalProjectUsageJson(body)) !== row.recordId) reject("record_id_mismatch");
    if (rowIds.has(row.recordId)) reject("duplicate_row_identity");
    rowIds.add(row.recordId);
    const key = groupKey(
      row as unknown as Pick<ProjectUsageRow, "companyRef" | "projectRef" | "attribution">,
      row.billingPoolRef as string,
    );
    if (groupKeys.has(key)) reject("duplicate_group_key");
    groupKeys.add(key);
    if (index > 0 && rows[index - 1]!.recordId >= row.recordId) reject("rows_not_sorted");
    rows.push(normalizedRow);
  }

  assertPlainObject(value.totals);
  assertMetrics(value.totals, true);
  const totals = value.totals as ProjectUsageMetrics;
  assertMetricMatchesRows(rows, totals);
  if (value.coverage.complete && (totals.recordCount !== expectedRecordCount || coveredThrough.ms !== periodEnd.ms)) {
    reject("complete_coverage_mismatch");
  }
  if (totals.recordCount > expectedRecordCount) reject("coverage_count_exceeds_expected");

  const core = exportCore(value as unknown as Omit<ProjectUsageExport, "exportId">);
  if (sha256(canonicalProjectUsageJson(core)) !== value.exportId) reject("export_id_mismatch");
}

/** Serialize an export as canonical, recursive-key-sorted, compact JSON. */
export function serializeProjectUsageExport(value: ProjectUsageExport): string {
  validateProjectUsageExport(value);
  const full = canonicalProjectUsageJson({ ...exportCore(value), exportId: value.exportId });
  if (Buffer.byteLength(full, "utf8") > PROJECT_USAGE_EXPORT_LIMITS.maxCanonicalBytes) {
    reject("canonical_size_limit");
  }
  return full;
}
