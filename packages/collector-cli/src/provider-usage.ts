import crypto from "node:crypto";

import type Database from "better-sqlite3";

/**
 * Provider usage is a reconciliation source, not a replacement for the local
 * event ledger.  Provider reports are usually bucketed by day, workspace,
 * project, key, or user and therefore cannot prove a thread.  Keeping them in
 * their own table lets finance compare them with local thread events without
 * silently charging an aggregate to a guessed project.
 */
export const PROVIDER_USAGE_SCHEMA = "plimsoll.provider-usage.v1" as const;

export type ProviderName = "openai" | "anthropic" | "xai" | "cursor";
export type ProviderUsageSource =
  | "openai_usage"
  | "anthropic_usage"
  | "grok"
  | "manual";
export type ProviderUsageAuthority =
  | "provider_reported"
  | "local_observed"
  | "estimated"
  | "unknown";
export type ProviderUsageGranularity = "event" | "bucket" | "daily";
export type ProviderUsageStatus =
  | "matched"
  | "local_under"
  | "local_over"
  | "partial"
  | "unknown_scope"
  | "unknown_dimension"
  | "provider_only"
  | "local_only";

export type ProviderUsageRecord = {
  provider: ProviderName;
  source: ProviderUsageSource;
  accountRef: string | null;
  observedAt: string;
  periodStart: string;
  periodEnd: string;
  granularity: ProviderUsageGranularity;
  model: string | null;
  projectRef: string | null;
  workspaceRef: string | null;
  apiKeyRef: string | null;
  userRef: string | null;
  requestRef: string | null;
  threadId: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheWriteTokens: number | null;
  requestCount: number | null;
  costUsd: number | null;
  authority: ProviderUsageAuthority;
  costKind: "reported" | "estimated" | "unknown";
};

export type ProviderUsageImportOptions = {
  observedAt?: string;
  accountRef?: string | null;
  source?: ProviderUsageSource;
};

export type ProviderUsageImportReceipt = {
  schema: typeof PROVIDER_USAGE_SCHEMA;
  records: number;
  inserted: number;
  replayed: number;
  revised: number;
  snapshotKeys: string[];
};

export type ProviderUsageAdjudication = {
  snapshotKey: string;
  provider: ProviderName;
  periodStart: string;
  periodEnd: string;
  status: ProviderUsageStatus;
  reason: string;
  localSource: string | null;
  providerInputTokens: number;
  providerOutputTokens: number;
  providerCacheReadTokens: number;
  providerCacheWriteTokens: number;
  providerRequestCount: number;
  providerCostUsd: number | null;
  localInputTokens: number;
  localOutputTokens: number;
  localCacheReadTokens: number;
  localCacheWriteTokens: number;
  localRequestCount: number;
  localCostUsd: number | null;
  projectKey: string | null;
  projectState: "exact" | "unknown";
  threadId: string | null;
  threadState: "exact" | "candidate" | "unknown";
  candidateThreadIds: string[];
  calculatedAt: string;
};

type ProviderUsageRow = ProviderUsageRecord & {
  snapshotKey: string;
  sourceDigest: string;
  revision: number;
};

type JsonRecord = Record<string, unknown>;

const PROVIDERS = new Set<ProviderName>(["openai", "anthropic", "xai", "cursor"]);
const SOURCES = new Set<ProviderUsageSource>(["openai_usage", "anthropic_usage", "grok", "manual"]);
const AUTHORITIES = new Set<ProviderUsageAuthority>([
  "provider_reported",
  "local_observed",
  "estimated",
  "unknown",
]);
const GRANULARITIES = new Set<ProviderUsageGranularity>(["event", "bucket", "daily"]);
const COST_KINDS = new Set(["reported", "estimated", "unknown"] as const);
const ISO_MILLIS = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
const MAX_RECORDS = 100_000;
const MAX_STRING = 256;

function record(value: unknown): JsonRecord {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as JsonRecord
    : {};
}

function stringValue(value: unknown, max = MAX_STRING): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed && trimmed.length <= max ? trimmed : null;
}

function numberValue(value: unknown, minimum = 0, integer = false): number | null {
  if (typeof value === "number" && Number.isFinite(value) && value >= minimum && (!integer || Number.isSafeInteger(value))) return value;
  if (typeof value === "string" && value.trim() && /^\d+(?:\.\d+)?$/.test(value.trim())) {
    const parsed = Number(value);
    return Number.isFinite(parsed) && parsed >= minimum && (!integer || Number.isSafeInteger(parsed)) ? parsed : null;
  }
  return null;
}

function integerValue(value: unknown, minimum = 0): number | null {
  const parsed = numberValue(value, minimum, true);
  return parsed !== null && Number.isSafeInteger(parsed) ? parsed : null;
}

function canonicalTimestamp(value: unknown, fallback?: string): string {
  const candidate = typeof value === "number" && Number.isFinite(value)
    ? new Date(value < 10_000_000_000 ? value * 1_000 : value).toISOString()
    : typeof value === "string" && Number.isFinite(Date.parse(value))
      ? new Date(Date.parse(value)).toISOString()
      : fallback;
  if (!candidate || !ISO_MILLIS.test(candidate) || Number.isNaN(Date.parse(candidate))) {
    throw new Error("provider_usage_timestamp_invalid");
  }
  return candidate;
}

function boundedMoney(value: unknown): number | null {
  const parsed = numberValue(value, 0);
  return parsed !== null && Number.isFinite(parsed) && parsed <= 1_000_000_000_000
    ? Number(parsed.toFixed(8))
    : null;
}

function canonicalJson(value: unknown): string {
  if (value === null || typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("provider_usage_non_finite");
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object).sort().map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`).join(",")}}`;
  }
  throw new Error("provider_usage_non_json");
}

function digest(value: unknown): string {
  return `sha256:${crypto.createHash("sha256").update(canonicalJson(value)).digest("hex")}`;
}

function arrayValue(value: unknown): unknown[] {
  return Array.isArray(value) ? value : [];
}

function firstNumber(object: JsonRecord, keys: readonly string[], integer = true): number | null {
  for (const key of keys) {
    const value = integer ? integerValue(object[key]) : numberValue(object[key]);
    if (value !== null) return value;
  }
  return null;
}

function firstString(object: JsonRecord, keys: readonly string[]): string | null {
  for (const key of keys) {
    const value = stringValue(object[key]);
    if (value !== null) return value;
  }
  return null;
}

function periodFrom(object: JsonRecord, fallbackStart: string, fallbackEnd: string): { start: string; end: string } {
  const start = canonicalTimestamp(
    object.start_time ?? object.startTime ?? object.starting_at ?? object.startingAt ??
      object.periodStart ?? object.period_start ?? object.date ?? object.day,
    fallbackStart,
  );
  const endCandidate = object.end_time ?? object.endTime ?? object.ending_at ?? object.endingAt ??
    object.periodEnd ?? object.period_end;
  const end = endCandidate === undefined
    ? fallbackEnd
    : canonicalTimestamp(endCandidate, fallbackEnd);
  if (Date.parse(end) <= Date.parse(start)) throw new Error("provider_usage_period_invalid");
  return { start, end };
}

function sourceFor(provider: ProviderName, requested?: ProviderUsageSource): ProviderUsageSource {
  if (requested) return requested;
  if (provider === "openai") return "openai_usage";
  if (provider === "anthropic") return "anthropic_usage";
  if (provider === "xai") return "grok";
  return "manual";
}

function baseRecord(provider: ProviderName, object: JsonRecord, options: {
  observedAt: string;
  periodStart: string;
  periodEnd: string;
  source: ProviderUsageSource;
  accountRef: string | null;
  granularity: ProviderUsageGranularity;
  model?: string | null;
}): ProviderUsageRecord {
  const period = periodFrom(object, options.periodStart, options.periodEnd);
  const tokenUsage = { ...object, ...record(object.tokenUsage ?? object.token_usage ?? object.usage) };
  const ticks = firstNumber(tokenUsage, ["cost_in_usd_ticks", "costUsdTicks", "cost_usd_ticks"], false);
  const costFromTicks = ticks === null ? null : Number((ticks / 10_000_000_000).toFixed(8));
  const cents = firstNumber({ ...object, ...tokenUsage }, ["chargedCents", "charged_cents", "cost_cents"], false);
  const amount = record(object.amount);
  const cost = boundedMoney(object.costUsd ?? object.cost_usd ?? object.cost ?? amount.value ??
    object.amount ?? (cents === null ? null : cents / 100) ?? costFromTicks);
  const inputTokens = firstNumber(tokenUsage, ["inputTokens", "input_tokens", "prompt_tokens"]);
  const outputTokens = firstNumber(tokenUsage, ["outputTokens", "output_tokens", "completion_tokens"]);
  const cacheReadTokens = firstNumber(tokenUsage, ["cacheReadTokens", "cache_read_input_tokens", "input_cached_tokens", "cached_tokens"]);
  const cacheWriteTokens = firstNumber(tokenUsage, ["cacheWriteTokens", "cache_creation_input_tokens", "cache_write_tokens"]);
  const requestCount = firstNumber(object, ["num_model_requests", "num_requests", "requestCount", "requests"], true);
  const providerReported = options.source !== "manual";
  return {
    provider,
    source: options.source,
    accountRef: options.accountRef,
    observedAt: options.observedAt,
    periodStart: period.start,
    periodEnd: period.end,
    granularity: options.granularity,
    model: options.model ?? firstString(object, ["model", "model_name"]),
    projectRef: firstString(object, ["project_id", "projectId", "project"]),
    workspaceRef: firstString(object, ["workspace_id", "workspaceId", "workspace"]),
    apiKeyRef: firstString(object, ["api_key_id", "apiKeyId", "api_key"]),
    userRef: firstString(object, ["user_id", "userId", "email"]),
    requestRef: firstString(object, ["request_id", "requestId", "id"]),
    threadId: firstString(object, ["thread_id", "threadId", "conversation_id", "conversationId"]),
    inputTokens,
    outputTokens,
    cacheReadTokens,
    cacheWriteTokens,
    requestCount,
    costUsd: cost ?? costFromTicks,
    authority: providerReported ? "provider_reported" : "unknown",
    costKind: (cost ?? costFromTicks) === null ? "unknown" : providerReported ? "reported" : "unknown",
  };
}

function normalizeOpenAi(payload: JsonRecord, options: Required<ProviderUsageImportOptions> & { periodStart: string; periodEnd: string }): ProviderUsageRecord[] {
  const rows: ProviderUsageRecord[] = [];
  for (const bucketValue of arrayValue(payload.data)) {
    const bucket = record(bucketValue);
    const results = arrayValue(bucket.results).length ? arrayValue(bucket.results) : [bucket];
    for (const resultValue of results) {
      const result = record(resultValue);
      const merged = { ...bucket, ...result };
      const hasCost = record(result.amount).value !== undefined || result.amount !== undefined || result.line_item !== undefined;
      const row = baseRecord("openai", merged, {
        ...options,
        granularity: "bucket",
        model: firstString(result, ["model"]),
      });
      if (!hasCost || row.inputTokens !== null || row.outputTokens !== null || row.requestCount !== null) rows.push(row);
      if (hasCost) {
        rows.push({ ...row, inputTokens: null, outputTokens: null, cacheReadTokens: null, cacheWriteTokens: null,
          requestCount: null, costUsd: boundedMoney(record(result.amount).value ?? result.amount), costKind: "reported" });
      }
    }
  }
  return rows;
}

function normalizeAnthropic(payload: JsonRecord, options: Required<ProviderUsageImportOptions> & { periodStart: string; periodEnd: string }): ProviderUsageRecord[] {
  const rows: ProviderUsageRecord[] = [];
  for (const bucketValue of arrayValue(payload.data)) {
    const bucket = record(bucketValue);
    const results = arrayValue(bucket.results).length ? arrayValue(bucket.results) : [bucket];
    for (const resultValue of results) {
      const result = record(resultValue);
      const merged = { ...bucket, ...result };
      rows.push(baseRecord("anthropic", merged, {
        ...options,
        granularity: "bucket",
        model: firstString(result, ["model"]),
      }));
    }
  }
  return rows;
}

function normalizeCursor(payload: JsonRecord, options: Required<ProviderUsageImportOptions> & { periodStart: string; periodEnd: string }): ProviderUsageRecord[] {
  const rows: ProviderUsageRecord[] = [];
  for (const value of arrayValue(payload.data)) {
    const object = record(value);
    const usage = record(object.tokenUsage ?? object.token_usage);
    const row = baseRecord("cursor", { ...object, usage }, {
      ...options,
      granularity: usage.inputTokens !== undefined || usage.input_tokens !== undefined ? "event" : "daily",
      model: firstString(object, ["model", "mostUsedModel"]),
    });
    if (row.periodEnd === row.periodStart) {
      const end = new Date(Date.parse(row.periodStart) + 86_400_000).toISOString();
      rows.push({ ...row, periodEnd: end });
    } else rows.push(row);
  }
  return rows;
}

function normalizeXai(payload: JsonRecord, options: Required<ProviderUsageImportOptions> & { periodStart: string; periodEnd: string }): ProviderUsageRecord[] {
  const items = arrayValue(payload.data).length ? arrayValue(payload.data) : [payload];
  return items.map((value) => {
    const object = record(value);
    return baseRecord("xai", object, { ...options, granularity: "event", model: firstString(object, ["model"]) });
  });
}

function normalizeCanonical(payload: JsonRecord, provider: ProviderName, options: Required<ProviderUsageImportOptions> & { periodStart: string; periodEnd: string }): ProviderUsageRecord[] {
  return arrayValue(payload.records).map((value) => {
    const object = record(value);
    const row = baseRecord(provider, object, {
      ...options,
      granularity: (stringValue(object.granularity) as ProviderUsageGranularity | null) ?? "bucket",
      model: firstString(object, ["model"]),
    });
    return {
      ...row,
      source: (stringValue(object.source) as ProviderUsageSource | null) ?? options.source,
      authority: (stringValue(object.authority) as ProviderUsageAuthority | null) ?? row.authority,
      costKind: (stringValue(object.costKind) as ProviderUsageRecord["costKind"] | null) ?? row.costKind,
    };
  });
}

/**
 * Convert a dashboard/compliance CSV into the canonical `records` envelope.
 * The parser is deliberately small and value-preserving: unknown columns are
 * dropped, token fields are never inferred from a total, and a row with no
 * usable counter is rejected by the normalizer rather than becoming zero.
 */
export function parseProviderUsageCsv(csv: string): { records: JsonRecord[] } {
  if (typeof csv !== "string" || csv.length > 16 * 1024 * 1024) throw new Error("provider_usage_csv_invalid");
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let quoted = false;
  for (let index = 0; index < csv.length; index += 1) {
    const character = csv[index];
    if (quoted) {
      if (character === '"' && csv[index + 1] === '"') {
        cell += '"';
        index += 1;
      } else if (character === '"') {
        quoted = false;
      } else {
        cell += character;
      }
    } else if (character === '"') {
      quoted = true;
    } else if (character === ",") {
      row.push(cell);
      cell = "";
    } else if (character === "\n" || character === "\r") {
      if (character === "\r" && csv[index + 1] === "\n") index += 1;
      row.push(cell);
      if (row.some((value) => value.trim())) rows.push(row);
      row = [];
      cell = "";
    } else {
      cell += character;
    }
  }
  if (quoted) throw new Error("provider_usage_csv_unclosed_quote");
  if (cell || row.length) {
    row.push(cell);
    if (row.some((value) => value.trim())) rows.push(row);
  }
  if (rows.length < 2) throw new Error("provider_usage_csv_header_missing");
  const header = rows[0].map((value) => value.trim().toLowerCase().replace(/[^a-z0-9]+/g, ""));
  const aliases: Record<string, string> = {
    date: "periodStart", day: "periodStart", startdate: "periodStart", starttime: "periodStart", periodstart: "periodStart",
    enddate: "periodEnd", endtime: "periodEnd", periodend: "periodEnd",
    inputtokens: "inputTokens", prompttokens: "inputTokens", inputtokencount: "inputTokens",
    outputtokens: "outputTokens", completiontokens: "outputTokens", outputtokencount: "outputTokens",
    cachereadtokens: "cacheReadTokens", cachecreationinputtokens: "cacheWriteTokens", cachewritetokens: "cacheWriteTokens",
    requests: "requestCount", requestcount: "requestCount", count: "requestCount",
    cost: "costUsd", costusd: "costUsd", dollars: "costUsd",
    model: "model", threadid: "threadId", conversationid: "threadId", project: "projectRef", projectref: "projectRef",
    workspace: "workspaceRef", workspaceref: "workspaceRef", user: "userRef", userref: "userRef",
    requestid: "requestRef", requestref: "requestRef", account: "accountRef", accountref: "accountRef",
    granularity: "granularity", authority: "authority", costkind: "costKind", source: "source",
  };
  const records: JsonRecord[] = [];
  for (const values of rows.slice(1)) {
    const record: JsonRecord = {};
    header.forEach((name, index) => {
      const key = aliases[name];
      const value = values[index]?.trim();
      if (key && value) record[key] = value;
    });
    if (Object.keys(record).length) records.push(record);
  }
  if (!records.length) throw new Error("provider_usage_csv_empty");
  return { records };
}

function validateRecord(value: ProviderUsageRecord): ProviderUsageRecord {
  if (!PROVIDERS.has(value.provider) || !SOURCES.has(value.source) || !AUTHORITIES.has(value.authority) ||
      !GRANULARITIES.has(value.granularity) || !COST_KINDS.has(value.costKind)) {
    throw new Error("provider_usage_record_enum_invalid");
  }
  for (const key of ["observedAt", "periodStart", "periodEnd"] as const) canonicalTimestamp(value[key]);
  if (Date.parse(value.periodEnd) <= Date.parse(value.periodStart)) throw new Error("provider_usage_period_invalid");
  for (const key of ["accountRef", "model", "projectRef", "workspaceRef", "apiKeyRef", "userRef", "requestRef", "threadId"] as const) {
    if (value[key] !== null && stringValue(value[key]) === null) throw new Error("provider_usage_string_invalid");
  }
  for (const key of ["inputTokens", "outputTokens", "cacheReadTokens", "cacheWriteTokens", "requestCount"] as const) {
    if (value[key] !== null && integerValue(value[key]) === null) throw new Error("provider_usage_counter_invalid");
  }
  if (value.costUsd !== null && boundedMoney(value.costUsd) === null) throw new Error("provider_usage_cost_invalid");
  if ([value.inputTokens, value.outputTokens, value.cacheReadTokens, value.cacheWriteTokens, value.requestCount, value.costUsd].every((item) => item === null)) {
    throw new Error("provider_usage_empty_record");
  }
  return value;
}

export function normalizeProviderPayload(
  provider: ProviderName,
  payload: unknown,
  options: ProviderUsageImportOptions & { periodStart?: string; periodEnd?: string } = {},
): ProviderUsageRecord[] {
  if (!PROVIDERS.has(provider) || payload === null || typeof payload !== "object") throw new Error("provider_usage_payload_invalid");
  const observedAt = canonicalTimestamp(options.observedAt ?? new Date().toISOString());
  const periodStart = canonicalTimestamp(options.periodStart ?? new Date(Date.parse(observedAt) - 86_400_000).toISOString());
  const periodEnd = canonicalTimestamp(options.periodEnd ?? observedAt);
  if (Date.parse(periodEnd) <= Date.parse(periodStart)) throw new Error("provider_usage_period_invalid");
  const importOptions = {
    observedAt,
    accountRef: options.accountRef ?? null,
    source: sourceFor(provider, options.source),
    periodStart,
    periodEnd,
  };
  const object = record(payload);
  const rows = "records" in object
    ? normalizeCanonical(object, provider, importOptions)
    : provider === "openai"
      ? normalizeOpenAi(object, importOptions)
      : provider === "anthropic"
        ? normalizeAnthropic(object, importOptions)
        : provider === "cursor"
          ? normalizeCursor(object, importOptions)
          : normalizeXai(object, importOptions);
  if (rows.length > MAX_RECORDS) throw new Error("provider_usage_record_limit");
  return rows.map(validateRecord);
}

function dimensionJson(row: ProviderUsageRecord): string {
  return canonicalJson({
    accountRef: row.accountRef,
    model: row.model,
    projectRef: row.projectRef,
    workspaceRef: row.workspaceRef,
    apiKeyRef: row.apiKeyRef,
    userRef: row.userRef,
    requestRef: row.requestRef,
    threadId: row.threadId,
    granularity: row.granularity,
  });
}

function snapshotKey(row: ProviderUsageRecord): string {
  return digest(["provider_usage_snapshot", row.provider, row.source, row.accountRef, row.periodStart,
    row.periodEnd, dimensionJson(row)]);
}

function rowFromDb(row: JsonRecord): ProviderUsageRow {
  return {
    snapshotKey: String(row.snapshotKey),
    sourceDigest: String(row.sourceDigest),
    revision: Number(row.revision),
    provider: row.provider as ProviderName,
    source: row.source as ProviderUsageSource,
    accountRef: (row.accountRef as string | null) ?? null,
    observedAt: String(row.observedAt),
    periodStart: String(row.periodStart),
    periodEnd: String(row.periodEnd),
    granularity: row.granularity as ProviderUsageGranularity,
    model: (row.model as string | null) ?? null,
    projectRef: (row.projectRef as string | null) ?? null,
    workspaceRef: (row.workspaceRef as string | null) ?? null,
    apiKeyRef: (row.apiKeyRef as string | null) ?? null,
    userRef: (row.userRef as string | null) ?? null,
    requestRef: (row.requestRef as string | null) ?? null,
    threadId: (row.threadId as string | null) ?? null,
    inputTokens: row.inputTokens === null ? null : Number(row.inputTokens),
    outputTokens: row.outputTokens === null ? null : Number(row.outputTokens),
    cacheReadTokens: row.cacheReadTokens === null ? null : Number(row.cacheReadTokens),
    cacheWriteTokens: row.cacheWriteTokens === null ? null : Number(row.cacheWriteTokens),
    requestCount: row.requestCount === null ? null : Number(row.requestCount),
    costUsd: row.costUsd === null ? null : Number(row.costUsd),
    authority: row.authority as ProviderUsageAuthority,
    costKind: row.costKind as ProviderUsageRecord["costKind"],
  };
}

export function ensureProviderUsageSchema(database: Database.Database): void {
  database.exec(`
    create table if not exists provider_usage_snapshots (
      snapshot_key text primary key,
      source_digest text not null,
      revision integer not null default 1 check (revision >= 1),
      provider text not null check (provider in ('openai','anthropic','xai','cursor')),
      source text not null check (source in ('openai_usage','anthropic_usage','grok','manual')),
      account_ref text,
      observed_at text not null,
      period_start text not null,
      period_end text not null,
      granularity text not null check (granularity in ('event','bucket','daily')),
      model text,
      project_ref text,
      workspace_ref text,
      api_key_ref text,
      user_ref text,
      request_ref text,
      thread_id text,
      input_tokens integer,
      output_tokens integer,
      cache_read_tokens integer,
      cache_write_tokens integer,
      request_count integer,
      cost_usd real,
      authority text not null check (authority in ('provider_reported','local_observed','estimated','unknown')),
      cost_kind text not null check (cost_kind in ('reported','estimated','unknown')),
      dimensions_json text not null,
      updated_at text not null
    );
    create index if not exists idx_provider_usage_period
      on provider_usage_snapshots (provider, period_start, period_end);
    create table if not exists provider_usage_adjudications (
      snapshot_key text primary key,
      calculated_at text not null,
      status text not null check (status in ('matched','local_under','local_over','partial','unknown_scope','unknown_dimension','provider_only','local_only')),
      reason text not null,
      local_source text,
      provider_input_tokens integer not null,
      provider_output_tokens integer not null,
      provider_cache_read_tokens integer not null,
      provider_cache_write_tokens integer not null,
      provider_request_count integer not null,
      provider_cost_usd real,
      local_input_tokens integer not null,
      local_output_tokens integer not null,
      local_cache_read_tokens integer not null,
      local_cache_write_tokens integer not null,
      local_request_count integer not null,
      local_cost_usd real,
      project_key text,
      project_state text not null check (project_state in ('exact','unknown')),
      thread_id text,
      thread_state text not null check (thread_state in ('exact','candidate','unknown')),
      candidate_thread_ids_json text not null,
      updated_at text not null
    );
  `);
}

export function importProviderUsage(
  database: Database.Database,
  records: readonly ProviderUsageRecord[],
  now = new Date().toISOString(),
): ProviderUsageImportReceipt {
  ensureProviderUsageSchema(database);
  const insert = database.prepare(`
    insert into provider_usage_snapshots (
      snapshot_key, source_digest, revision, provider, source, account_ref,
      observed_at, period_start, period_end, granularity, model, project_ref,
      workspace_ref, api_key_ref, user_ref, request_ref, thread_id,
      input_tokens, output_tokens, cache_read_tokens, cache_write_tokens,
      request_count, cost_usd, authority, cost_kind, dimensions_json, updated_at
    ) values (
      @snapshotKey, @sourceDigest, 1, @provider, @source, @accountRef,
      @observedAt, @periodStart, @periodEnd, @granularity, @model, @projectRef,
      @workspaceRef, @apiKeyRef, @userRef, @requestRef, @threadId,
      @inputTokens, @outputTokens, @cacheReadTokens, @cacheWriteTokens,
      @requestCount, @costUsd, @authority, @costKind, @dimensionsJson, @updatedAt
    ) on conflict(snapshot_key) do update set
      revision = provider_usage_snapshots.revision +
        case when provider_usage_snapshots.source_digest = excluded.source_digest then 0 else 1 end,
      source_digest=excluded.source_digest, observed_at=excluded.observed_at,
      period_start=excluded.period_start, period_end=excluded.period_end,
      granularity=excluded.granularity, model=excluded.model,
      project_ref=excluded.project_ref, workspace_ref=excluded.workspace_ref,
      api_key_ref=excluded.api_key_ref, user_ref=excluded.user_ref,
      request_ref=excluded.request_ref, thread_id=excluded.thread_id,
      input_tokens=excluded.input_tokens, output_tokens=excluded.output_tokens,
      cache_read_tokens=excluded.cache_read_tokens, cache_write_tokens=excluded.cache_write_tokens,
      request_count=excluded.request_count, cost_usd=excluded.cost_usd,
      authority=excluded.authority, cost_kind=excluded.cost_kind,
      dimensions_json=excluded.dimensions_json, updated_at=excluded.updated_at
  `);
  let inserted = 0;
  let replayed = 0;
  let revised = 0;
  const keys: string[] = [];
  const run = database.transaction(() => {
    for (const untrusted of records) {
      const row = validateRecord(untrusted);
      const snapshot = snapshotKey(row);
      const sourceDigest = digest(row);
      const existing = database.prepare(`select source_digest as sourceDigest from provider_usage_snapshots where snapshot_key=?`).get(snapshot) as { sourceDigest: string } | undefined;
      insert.run({
        snapshotKey: snapshot,
        sourceDigest,
        provider: row.provider,
        source: row.source,
        accountRef: row.accountRef,
        observedAt: row.observedAt,
        periodStart: row.periodStart,
        periodEnd: row.periodEnd,
        granularity: row.granularity,
        model: row.model,
        projectRef: row.projectRef,
        workspaceRef: row.workspaceRef,
        apiKeyRef: row.apiKeyRef,
        userRef: row.userRef,
        requestRef: row.requestRef,
        threadId: row.threadId,
        inputTokens: row.inputTokens,
        outputTokens: row.outputTokens,
        cacheReadTokens: row.cacheReadTokens,
        cacheWriteTokens: row.cacheWriteTokens,
        requestCount: row.requestCount,
        costUsd: row.costUsd,
        authority: row.authority,
        costKind: row.costKind,
        dimensionsJson: dimensionJson(row),
        updatedAt: now,
      });
      keys.push(snapshot);
      if (!existing) inserted += 1;
      else if (existing.sourceDigest === sourceDigest) replayed += 1;
      else revised += 1;
    }
  })();
  return { schema: PROVIDER_USAGE_SCHEMA, records: records.length, inserted, replayed, revised, snapshotKeys: keys };
}

function hasTable(database: Database.Database, name: string): boolean {
  return Boolean(database.prepare(`select 1 from sqlite_master where type='table' and name=?`).get(name));
}

function localAggregate(database: Database.Database, source: string, start: string, end: string, model: string | null) {
  if (!hasTable(database, "buffered_events")) return { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, requests: 0, cost: null as number | null };
  const params: unknown[] = [source, start, end];
  const modelPredicate = model ? " and model = ?" : "";
  if (model) params.push(model);
  const rows = database.prepare(`
    select
      coalesce(sum(input_tokens),0) as input,
      coalesce(sum(output_tokens),0) as output,
      coalesce(sum(cache_read_tokens),0) as cacheRead,
      coalesce(sum(cache_creation_tokens),0) as cacheWrite,
      sum(case when input_tokens is not null or output_tokens is not null
        or cache_read_tokens is not null or cache_creation_tokens is not null
        or cost_usd is not null then 1 else 0 end) as requests,
      sum(cost_usd) as cost
    from buffered_events
    where source=? and observed_at >= ? and observed_at < ?${modelPredicate}
      and not (event_type in ('usage_rollout','usage_transcript') and session_id is not null and exists (
        select 1 from buffered_events live
        where live.source=buffered_events.source and live.session_id=buffered_events.session_id
          and live.event_type not in ('usage_rollout','usage_transcript')
          and (live.input_tokens is not null or live.output_tokens is not null
            or live.cache_read_tokens is not null or live.cache_creation_tokens is not null
            or live.cost_usd is not null)
      ))
  `).get(...params) as JsonRecord;
  return {
    input: Number(rows.input ?? 0),
    output: Number(rows.output ?? 0),
    cacheRead: Number(rows.cacheRead ?? 0),
    cacheWrite: Number(rows.cacheWrite ?? 0),
    requests: Number(rows.requests ?? 0),
    cost: rows.cost === null || rows.cost === undefined ? null : Number(rows.cost),
  };
}

function localCandidates(database: Database.Database, source: string, start: string, end: string, model: string | null) {
  if (!hasTable(database, "buffered_events")) return { sessions: [] as string[], projects: [] as string[] };
  const params: unknown[] = [source, start, end];
  const modelPredicate = model ? " and model = ?" : "";
  if (model) params.push(model);
  const rows = database.prepare(`
    select distinct session_id as sessionId, project_key as projectKey
    from buffered_events
    where source=? and observed_at >= ? and observed_at < ?${modelPredicate}
      and (input_tokens is not null or output_tokens is not null
        or cache_read_tokens is not null or cache_creation_tokens is not null or cost_usd is not null)
    limit 65
  `).all(...params) as JsonRecord[];
  return {
    sessions: [...new Set(rows.map((row) => stringValue(row.sessionId)).filter((value): value is string => value !== null))].slice(0, 64),
    projects: [...new Set(rows.map((row) => stringValue(row.projectKey)).filter((value): value is string => value !== null))].slice(0, 64),
  };
}

function equalCost(a: number | null, b: number | null): boolean {
  return a === null || b === null || Math.abs(a - b) <= 0.01;
}

function adjudicateRow(database: Database.Database, row: ProviderUsageRow, localSource: string | null, calculatedAt: string): ProviderUsageAdjudication {
  const providerInput = row.inputTokens ?? 0;
  const providerOutput = row.outputTokens ?? 0;
  const providerCacheRead = row.cacheReadTokens ?? 0;
  const providerCacheWrite = row.cacheWriteTokens ?? 0;
  const providerRequests = row.requestCount ?? 0;
  const providerCost = row.costUsd;
  const candidates = localSource ? localCandidates(database, localSource, row.periodStart, row.periodEnd, row.model) : { sessions: [], projects: [] };
  const local = localSource ? localAggregate(database, localSource, row.periodStart, row.periodEnd, row.model) : { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, requests: 0, cost: null };
  let status: ProviderUsageStatus;
  let reason: string;
  if (!localSource) {
    status = "unknown_scope";
    reason = "provider_snapshot_has_no_declared_local_product_scope";
  } else if (row.projectRef || row.workspaceRef || row.apiKeyRef || row.userRef) {
    status = "unknown_dimension";
    reason = "provider_dimension_requires_an_explicit_local_join";
  } else if (providerInput === 0 && providerOutput === 0 && providerCost === null && providerRequests === 0) {
    status = "partial";
    reason = "provider_record_has_no_billable_measure";
  } else {
    const tokenEqual = providerInput === local.input && providerOutput === local.output &&
      providerCacheRead === local.cacheRead && providerCacheWrite === local.cacheWrite;
    const costEqual = equalCost(providerCost, local.cost);
    if (tokenEqual && costEqual) { status = "matched"; reason = "provider_and_local_totals_match"; }
    else if (local.input + local.output + local.cacheRead + local.cacheWrite < providerInput + providerOutput + providerCacheRead + providerCacheWrite) {
      status = "local_under"; reason = "local_capture_is_below_provider_total";
    } else if (local.input + local.output + local.cacheRead + local.cacheWrite > providerInput + providerOutput + providerCacheRead + providerCacheWrite) {
      status = "local_over"; reason = "local_capture_is_above_provider_total";
    } else { status = "partial"; reason = "provider_and_local_dimensions_are_not_fully_comparable"; }
  }
  let projectKey: string | null = null;
  let projectState: "exact" | "unknown" = "unknown";
  if (row.projectRef && candidates.projects.includes(row.projectRef)) {
    projectKey = row.projectRef;
    projectState = "exact";
  }
  let threadId: string | null = null;
  let threadState: "exact" | "candidate" | "unknown" = "unknown";
  if (row.threadId && candidates.sessions.includes(row.threadId)) {
    threadId = row.threadId;
    threadState = "exact";
  } else if (row.granularity === "event" && candidates.sessions.length === 1) {
    threadState = "candidate";
  }
  return {
    snapshotKey: row.snapshotKey,
    provider: row.provider,
    periodStart: row.periodStart,
    periodEnd: row.periodEnd,
    status,
    reason,
    localSource,
    providerInputTokens: providerInput,
    providerOutputTokens: providerOutput,
    providerCacheReadTokens: providerCacheRead,
    providerCacheWriteTokens: providerCacheWrite,
    providerRequestCount: providerRequests,
    providerCostUsd: providerCost,
    localInputTokens: local.input,
    localOutputTokens: local.output,
    localCacheReadTokens: local.cacheRead,
    localCacheWriteTokens: local.cacheWrite,
    localRequestCount: local.requests,
    localCostUsd: local.cost,
    projectKey,
    projectState,
    threadId,
    threadState,
    candidateThreadIds: candidates.sessions,
    calculatedAt,
  };
}

export function adjudicateProviderUsage(
  database: Database.Database,
  options: { localSource?: string | null; now?: string } = {},
): ProviderUsageAdjudication[] {
  ensureProviderUsageSchema(database);
  const calculatedAt = canonicalTimestamp(options.now ?? new Date().toISOString());
  const rows = database.prepare(`select
    snapshot_key as snapshotKey, source_digest as sourceDigest, revision,
    provider, source, account_ref as accountRef, observed_at as observedAt,
    period_start as periodStart, period_end as periodEnd, granularity, model,
    project_ref as projectRef, workspace_ref as workspaceRef, api_key_ref as apiKeyRef,
    user_ref as userRef, request_ref as requestRef, thread_id as threadId,
    input_tokens as inputTokens, output_tokens as outputTokens,
    cache_read_tokens as cacheReadTokens, cache_write_tokens as cacheWriteTokens,
    request_count as requestCount, cost_usd as costUsd, authority, cost_kind as costKind
    from provider_usage_snapshots order by period_start, snapshot_key`).all() as JsonRecord[];
  const adjudications = rows.map((value) => adjudicateRow(database, rowFromDb(value), options.localSource ?? null, calculatedAt));
  const save = database.prepare(`insert into provider_usage_adjudications (
    snapshot_key, calculated_at, status, reason, local_source,
    provider_input_tokens, provider_output_tokens, provider_cache_read_tokens, provider_cache_write_tokens,
    provider_request_count, provider_cost_usd, local_input_tokens, local_output_tokens,
    local_cache_read_tokens, local_cache_write_tokens, local_request_count, local_cost_usd,
    project_key, project_state, thread_id, thread_state, candidate_thread_ids_json, updated_at
  ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on conflict(snapshot_key) do update set
    calculated_at=excluded.calculated_at, status=excluded.status, reason=excluded.reason,
    local_source=excluded.local_source, provider_input_tokens=excluded.provider_input_tokens,
    provider_output_tokens=excluded.provider_output_tokens, provider_cache_read_tokens=excluded.provider_cache_read_tokens,
    provider_cache_write_tokens=excluded.provider_cache_write_tokens, provider_request_count=excluded.provider_request_count,
    provider_cost_usd=excluded.provider_cost_usd, local_input_tokens=excluded.local_input_tokens,
    local_output_tokens=excluded.local_output_tokens, local_cache_read_tokens=excluded.local_cache_read_tokens,
    local_cache_write_tokens=excluded.local_cache_write_tokens, local_request_count=excluded.local_request_count,
    local_cost_usd=excluded.local_cost_usd, project_key=excluded.project_key, project_state=excluded.project_state,
    thread_id=excluded.thread_id, thread_state=excluded.thread_state,
    candidate_thread_ids_json=excluded.candidate_thread_ids_json, updated_at=excluded.updated_at`);
  const run = database.transaction(() => {
    for (const item of adjudications) save.run(
      item.snapshotKey, item.calculatedAt, item.status, item.reason, item.localSource,
      item.providerInputTokens, item.providerOutputTokens, item.providerCacheReadTokens, item.providerCacheWriteTokens,
      item.providerRequestCount, item.providerCostUsd, item.localInputTokens, item.localOutputTokens,
      item.localCacheReadTokens, item.localCacheWriteTokens, item.localRequestCount, item.localCostUsd,
      item.projectKey, item.projectState, item.threadId, item.threadState, JSON.stringify(item.candidateThreadIds), item.calculatedAt,
    );
  });
  run();
  return adjudications;
}

export function readProviderUsageRows(database: Database.Database): ProviderUsageRow[] {
  ensureProviderUsageSchema(database);
  return (database.prepare(`select
    snapshot_key as snapshotKey, source_digest as sourceDigest, revision,
    provider, source, account_ref as accountRef, observed_at as observedAt,
    period_start as periodStart, period_end as periodEnd, granularity, model,
    project_ref as projectRef, workspace_ref as workspaceRef, api_key_ref as apiKeyRef,
    user_ref as userRef, request_ref as requestRef, thread_id as threadId,
    input_tokens as inputTokens, output_tokens as outputTokens,
    cache_read_tokens as cacheReadTokens, cache_write_tokens as cacheWriteTokens,
    request_count as requestCount, cost_usd as costUsd, authority, cost_kind as costKind
    from provider_usage_snapshots order by period_start, snapshot_key`).all() as JsonRecord[]).map(rowFromDb);
}

export type ProviderCapability = {
  provider: ProviderName;
  endpoint: string;
  credentialEnv: string;
  requiredScope: string;
  configured: boolean;
  supported: boolean;
  note: string;
};

/**
 * Capability is deliberately separate from authentication.  A normal model
 * key must not be reported as a billing/admin key merely because it exists.
 */
export function providerUsageCapabilities(env: NodeJS.ProcessEnv = process.env): ProviderCapability[] {
  return [
    { provider: "openai", endpoint: "https://api.openai.com/v1/organization/usage/completions", credentialEnv: "OPENAI_ADMIN_KEY",
      requiredScope: "organization usage + costs", configured: Boolean(env.OPENAI_ADMIN_KEY), supported: true,
      note: "API organization usage only; ChatGPT/Codex subscription usage is a separate product." },
    { provider: "anthropic", endpoint: "https://api.anthropic.com/v1/organizations/usage_report/messages", credentialEnv: "ANTHROPIC_ADMIN_KEY",
      requiredScope: "organization Admin API", configured: Boolean(env.ANTHROPIC_ADMIN_KEY), supported: true,
      note: "API/Enterprise organization reports; consumer Claude plan usage is not represented." },
    { provider: "cursor", endpoint: "https://api.cursor.com/teams/filtered-usage-events", credentialEnv: "CURSOR_TEAM_ADMIN_KEY",
      requiredScope: "team admin API key", configured: Boolean(env.CURSOR_TEAM_ADMIN_KEY), supported: true,
      note: "Team admin scope is required; a user-scoped /v1/me key is not sufficient." },
    { provider: "xai", endpoint: "https://management-api.x.ai", credentialEnv: "XAI_MANAGEMENT_API_KEY",
      requiredScope: "management API", configured: Boolean(env.XAI_MANAGEMENT_API_KEY), supported: false,
      note: "Exact per-request cost is available from inference responses; aggregate billing access needs a management scope." },
  ];
}
