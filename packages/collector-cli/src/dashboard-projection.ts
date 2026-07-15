import { createHash } from "node:crypto";
import { gzipSync, gunzipSync } from "node:zlib";

import type Database from "better-sqlite3";

import type { SubscriptionConfig } from "./dashboard-api";

export const DASHBOARD_SCHEMA_VERSION = 1;
export const DASHBOARD_WINDOWS = [30, 90, 182, 365, 1825] as const;
const INTERNAL_WINDOWS = [7, ...DASHBOARD_WINDOWS] as const;
const DAY_MS = 24 * 60 * 60 * 1000;
const BACKFILL_ROWS = 1_000;
const REPAIR_ROWS = 250;
const COMPACT_GC_ITEMS = 1_000;
const SESSION_REPAIR_ROWS = 1_000;
const SESSION_REPAIR_BUDGET_MS = 50;
const CANONICAL_SHA256 = /^sha256:[0-9a-f]{64}$/;
const UNLINKED_REPO = "__unlinked__";
const UNLINKED_ACCOUNT = "__unlinked_account__";
const SAFE_SOURCES=new Set(["anthropic_admin","anthropic_usage","claude_code","codex","github","openai_usage","manual","unknown"]);
const SAFE_EVENT_TYPES=new Set(["session_start","session_stop","user_prompt_submit","assistant_response","tool_use","tool_result","otel_span","usage_rollout","usage_transcript","unknown"]);
const SAFE_ACTIONS=new Set(["continue","validate","test","edit","read","write","shell","mcp","browser","review","other"]);

type RawProjectionRow = {
  rawRowid: number;
  id: string;
  source: string;
  eventType: string;
  observedAt: string;
  sessionId: string | null;
  actionClass: string | null;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  costUsd: number | null;
  repoHash: string | null;
  branchHash: string | null;
  headSha: string | null;
  machine: string | null;
  accountHash: string | null;
  suppressedFieldsJson: string;
};

type CompactProjectionItem = {
  rawRowid: number;
  observedAt: string;
  source: string;
  eventType: string;
  actionClass: string | null;
  windowMask: number;
};

type ProjectionFact = {
  projectionId: string;
  rawRowid: number;
  source: string;
  eventType: string;
  observedAt: string;
  sessionHash: string | null;
  actionClass: string | null;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  costNanos: number | null;
  repoHash: string | null;
  branchHash: string | null;
  headHash: string | null;
  machineHash: string | null;
  accountHash: string | null;
  suppressed: number;
};

type ProjectionControl = {
  backfillHighWater: number | null;
  backfillCursor: number;
  backfillComplete: number;
  parityCursor: number;
  parityComplete: number;
  ready: number;
  parityReady: number;
  generation: number;
  dirty: number;
  degradedReason: string | null;
  lastSuccessAt: string | null;
  lastErrorAt: string | null;
  projectionRowsVisited: number;
  projectionRowsWritten: number;
  snapshotRowsVisited: number;
  snapshotBuilds: number;
  snapshotCacheHits: number;
  repairFacts: number;
  backfillFacts: number;
  expiryFacts: number;
  rawRowsScannedByDashboard: number;
  filesystemEntriesScannedByDashboard: number;
  metricBackfillHighWater: number | null;
  metricBackfillCursor: number;
  metricBackfillComplete: number;
  metricSampleCount: number | null;
  repairBacklog: number;
  dirtySessionBacklog: number;
  accountInvalidationBacklog: number;
  compactMutationBacklog: number;
  compactGcBacklog: number;
  compactGcSchedule: number;
  compactSegmentsWritten: number;
  compactGcItemsVisited: number;
  compactGcItemsRemoved: number;
  compactGcSegmentsRewritten: number;
  compactGcSegmentsDeleted: number;
  compactGcDaysCompleted: number;
  compactGcRestarts: number;
};

export type ProjectionMaintenanceReceipt = {
  backfillRowsVisited: number;
  parityRowsVisited: number;
  repairRowsVisited: number;
  dirtySessionsVisited: number;
  sessionRepairRowsVisited: number;
  metricRowsVisited: number;
  expiryFacts: number;
  compactSegmentsWritten: number;
  compactGcItemsVisited: number;
  compactGcItemsRemoved: number;
  compactGcSegmentsRewritten: number;
  compactGcSegmentsDeleted: number;
  compactGcDaysCompleted: number;
  compactGcRestarts: number;
  compactGcDurationMs: number;
  snapshotBuilds: number;
  ready: boolean;
  degraded: boolean;
  backlog: { repairs: number; compactMutations: number; compactGcDays: number; dirtySessions: number; accountInvalidations: number; expiryWindows: number };
};

export type CaptureActivityReceipt = {
  source: "claude_code" | "codex";
  lastActivityAt: string | null;
  filesToday: number;
  discoveryEntries: number;
  lastScanAt: string;
  error?: string | null;
  truncated?: boolean;
};

type SnapshotCore = {
  schemaVersion: number;
  generation: number;
  window: { days: number; since: string };
  projection: {
    status: "ready" | "stale";
    freshnessAt: string;
    degraded: boolean;
    degradedReason: string | null;
    parityReady: boolean;
    counters: Record<string, number>;
  };
  summary: Record<string, unknown>;
  sessions: Array<Record<string, unknown>>;
  repos: Array<Record<string, unknown>>;
  accounts: { days: number; buckets: Record<string, number>; accounts: Array<Record<string, unknown>>; priorityRepoCount: number };
  status: Record<string, unknown>;
};

export type SnapshotRead =
  | { kind: "unsupported"; supportedDays: readonly number[] }
  | { kind: "backfilling"; status: ReturnType<DashboardProjectionStore["status"]> }
  | { kind: "ready"; snapshot: SnapshotCore; etagSeed: string };

function sha256(value: string) {
  return `sha256:${createHash("sha256").update(value).digest("hex")}`;
}

/** Projection identities are never assumed safe merely because a producer supplied them. */
function safeHash(value: string | null | undefined) {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  return CANONICAL_SHA256.test(normalized) ? normalized : sha256(value);
}

/** Repo/branch linkage is accepted only in the canonical sanitizer form. */
function canonicalLinkage(value: string | null | undefined) {
  if (!value) return null;
  const normalized = value.trim().toLowerCase();
  return CANONICAL_SHA256.test(normalized) ? normalized : null;
}

function costNanos(value: number | null | undefined) {
  if (value === null || value === undefined || !Number.isFinite(value)) return null;
  return Math.round(value * 1_000_000_000);
}

function safeClassification(value:string|null|undefined,allowed:Set<string>,fallback:string){
  return value&&allowed.has(value)?value:fallback;
}

function safeModel(value:string|null|undefined){
  if(!value)return null;
  return /^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$/.test(value)?value:sha256(`model:${value}`);
}

function usd(value: unknown) {
  return Number(value ?? 0) / 1_000_000_000;
}

function sinceIso(days: number, now: Date) {
  return new Date(now.getTime() - days * DAY_MS).toISOString();
}

function day(value: string) {
  return value.slice(0, 10);
}

function factFromRaw(row: RawProjectionRow): ProjectionFact {
  return {
    projectionId: sha256(`event:${row.id}`),
    rawRowid: row.rawRowid,
    source: safeClassification(row.source,SAFE_SOURCES,"unknown"),
    eventType: safeClassification(row.eventType,SAFE_EVENT_TYPES,"unknown"),
    observedAt: row.observedAt,
    sessionHash: safeHash(row.sessionId),
    actionClass: safeClassification(row.actionClass,SAFE_ACTIONS,"other"),
    model: safeModel(row.model),
    inputTokens: row.inputTokens,
    outputTokens: row.outputTokens,
    cacheReadTokens: row.cacheReadTokens,
    cacheCreationTokens: row.cacheCreationTokens,
    costNanos: costNanos(row.costUsd),
    repoHash: canonicalLinkage(row.repoHash),
    branchHash: canonicalLinkage(row.branchHash),
    headHash: safeHash(row.headSha),
    machineHash: safeHash(row.machine),
    accountHash: safeHash(row.accountHash),
    suppressed: row.suppressedFieldsJson !== "[]" ? 1 : 0,
  };
}

function compactable(row: RawProjectionRow) {
  return SAFE_SOURCES.has(row.source)&&SAFE_EVENT_TYPES.has(row.eventType)&&
    (row.actionClass===null||SAFE_ACTIONS.has(row.actionClass))&&
    row.sessionId === null && row.model === null && row.inputTokens === null &&
    row.outputTokens === null && row.cacheReadTokens === null &&
    row.cacheCreationTokens === null && row.costUsd === null && row.repoHash === null &&
    row.branchHash === null && row.headSha === null && row.accountHash === null;
}

function encodeCompact(items: CompactProjectionItem[]) {
  return gzipSync(JSON.stringify(items), { level: 6 });
}

function decodeCompact(payload: Buffer) {
  return json<CompactProjectionItem[]>(gunzipSync(payload).toString("utf8"));
}

function factFromDb(row: Record<string, unknown>): ProjectionFact {
  return {
    projectionId: String(row.projectionId),
    rawRowid: Number(row.rawRowid),
    source: String(row.source),
    eventType: String(row.eventType),
    observedAt: String(row.observedAt),
    sessionHash: row.sessionHash === null ? null : String(row.sessionHash),
    actionClass: row.actionClass === null ? null : String(row.actionClass),
    model: row.model === null ? null : String(row.model),
    inputTokens: row.inputTokens === null ? null : Number(row.inputTokens),
    outputTokens: row.outputTokens === null ? null : Number(row.outputTokens),
    cacheReadTokens: row.cacheReadTokens === null ? null : Number(row.cacheReadTokens),
    cacheCreationTokens: row.cacheCreationTokens === null ? null : Number(row.cacheCreationTokens),
    costNanos: row.costNanos === null ? null : Number(row.costNanos),
    repoHash: row.repoHash === null ? null : String(row.repoHash),
    branchHash: row.branchHash === null ? null : String(row.branchHash),
    headHash: row.headHash === null ? null : String(row.headHash),
    machineHash: row.machineHash === null ? null : String(row.machineHash),
    accountHash: row.accountHash === null ? null : String(row.accountHash),
    suppressed: Number(row.suppressed),
  };
}

function add<K>(map: Map<K, number>, key: K, value: number) {
  map.set(key, (map.get(key) ?? 0) + value);
}

function json<T>(value: string): T {
  return JSON.parse(value) as T;
}

export class DashboardProjectionStore {
  private failNextApply = false;
  private failNextCompactGcAfterRewrite = false;

  constructor(
    private readonly db: Database.Database,
    options: { newLedger?: boolean; now?: Date } = {},
  ) {
    const now = options.now ?? new Date(Date.now());
    this.createSchema(now, Boolean(options.newLedger));
    if (options.newLedger) this.publishSnapshots(now);
  }

  private createSchema(now: Date, newLedger: boolean) {
    this.db.exec(`
      create table if not exists dashboard_projection_control (
        singleton integer primary key check (singleton = 1),
        schema_version integer not null,
        backfill_high_water integer,
        backfill_cursor integer not null default 0,
        backfill_complete integer not null default 0,
        parity_cursor integer not null default 0,
        parity_complete integer not null default 0,
        ready integer not null default 0,
        parity_ready integer not null default 0,
        generation integer not null default 0,
        dirty integer not null default 1,
        degraded_reason text,
        last_success_at text,
        last_error_at text,
        projection_rows_visited integer not null default 0,
        projection_rows_written integer not null default 0,
        snapshot_rows_visited integer not null default 0,
        snapshot_builds integer not null default 0,
        snapshot_cache_hits integer not null default 0,
        repair_facts integer not null default 0,
        backfill_facts integer not null default 0,
        expiry_facts integer not null default 0,
        raw_rows_scanned_by_dashboard integer not null default 0,
        filesystem_entries_scanned_by_dashboard integer not null default 0,
        metric_backfill_high_water integer,
        metric_backfill_cursor integer not null default 0,
        metric_backfill_complete integer not null default 0,
        metric_sample_count integer,
        repair_backlog integer not null default 0,
        dirty_session_backlog integer not null default 0,
        account_invalidation_backlog integer not null default 0,
        compact_mutation_backlog integer not null default 0,
        compact_gc_backlog integer not null default 0,
        compact_gc_schedule integer not null default 0,
        compact_segments_written integer not null default 0,
        compact_gc_items_visited integer not null default 0,
        compact_gc_items_removed integer not null default 0,
        compact_gc_segments_rewritten integer not null default 0,
        compact_gc_segments_deleted integer not null default 0,
        compact_gc_days_completed integer not null default 0,
        compact_gc_restarts integer not null default 0,
        settings_version integer not null default 0
      );
      create table if not exists dashboard_window_control (
        days integer primary key,
        cutoff_at text not null,
        target_cutoff_at text,
        expiry_cursor_at text,
        expiry_cursor_id text,
        compact_expiry_high_water integer,
        compact_expiry_cursor_segment integer,
        compact_expiry_cursor_offset integer,
        last_success_at text
      );
      create table if not exists dashboard_event_facts (
        projection_id text primary key,
        raw_rowid integer not null unique,
        source text not null,
        event_type text not null,
        observed_at text not null,
        session_hash text,
        action_class text,
        model text,
        input_tokens integer,
        output_tokens integer,
        cache_read_tokens integer,
        cache_creation_tokens integer,
        cost_nanos integer,
        repo_hash text,
        branch_hash text,
        head_hash text,
        machine_hash text,
        account_hash text,
        suppressed integer not null default 0
      );
      create index if not exists idx_dashboard_facts_observed
        on dashboard_event_facts (observed_at, projection_id);
      create index if not exists idx_dashboard_facts_session
        on dashboard_event_facts (session_hash, observed_at, projection_id);
      create index if not exists idx_dashboard_facts_repo
        on dashboard_event_facts (repo_hash, observed_at, session_hash);
      create index if not exists idx_dashboard_facts_account
        on dashboard_event_facts (account_hash, session_hash, observed_at);
      create index if not exists idx_dashboard_facts_source
        on dashboard_event_facts (source, observed_at, session_hash);
      create index if not exists idx_dashboard_facts_source_token
        on dashboard_event_facts (source, observed_at)
        where input_tokens is not null;
      create table if not exists dashboard_projection_repairs (
        raw_rowid integer primary key,
        reason text not null,
        queued_at text not null
      );
      create table if not exists dashboard_compact_segments (
        segment_id integer primary key autoincrement,
        bucket_day text not null,
        min_observed_at text not null,
        max_observed_at text not null,
        event_count integer not null,
        payload_gzip blob not null
      );
      create index if not exists idx_dashboard_compact_expiry
        on dashboard_compact_segments (min_observed_at,max_observed_at,segment_id);
      create index if not exists idx_dashboard_compact_day_segment
        on dashboard_compact_segments (bucket_day,segment_id);
      create table if not exists dashboard_compact_mutations (
        raw_rowid integer primary key,
        observed_at text not null,
        source text not null,
        event_type text not null,
        action_class text,
        queued_at text not null
      );
      create table if not exists dashboard_compact_cancellations (
        raw_rowid integer not null,
        bucket_day text not null,
        observed_at text not null,
        source text not null,
        event_type text not null,
        action_key text not null,
        primary key (raw_rowid,observed_at,source,event_type,action_key)
      );
      create table if not exists dashboard_compact_day_source (
        bucket_day text not null,
        source text not null,
        event_count integer not null,
        min_observed_at text not null,
        max_observed_at text not null,
        primary key (bucket_day,source)
      );
      create index if not exists idx_dashboard_compact_day_source_min
        on dashboard_compact_day_source (min_observed_at,bucket_day,source);
      create index if not exists idx_dashboard_compact_day_source_max
        on dashboard_compact_day_source (max_observed_at desc,bucket_day,source);
      create index if not exists idx_dashboard_compact_source_latest
        on dashboard_compact_day_source (source,max_observed_at desc,bucket_day);
      create table if not exists dashboard_compact_gc_days (
        bucket_day text primary key,
        revision integer not null default 1,
        processing_revision integer not null default 0,
        high_water_segment integer,
        cursor_segment integer not null default 0,
        last_schedule integer not null default 0,
        queued_at text not null,
        updated_at text not null
      );
      create index if not exists idx_dashboard_compact_gc_fair
        on dashboard_compact_gc_days (last_schedule,queued_at,bucket_day);
      create table if not exists dashboard_compact_gc_source_scratch (
        bucket_day text not null,
        processing_revision integer not null,
        source text not null,
        event_count integer not null,
        min_observed_at text not null,
        max_observed_at text not null,
        primary key (bucket_day,processing_revision,source)
      );
      create table if not exists dashboard_dirty_sessions (
        days integer not null,
        session_hash text not null,
        reason text not null,
        queued_at text not null,
        revision integer not null default 1,
        restart_revision integer not null default 1,
        primary key (days, session_hash)
      );
      create table if not exists dashboard_account_invalidations (
        account_hash text primary key,
        cursor_session_hash text,
        queued_at text not null
      );
      create table if not exists dashboard_session_repair_jobs (
        days integer not null,
        session_hash text not null,
        cutoff_at text not null,
        high_water integer not null,
        cursor_raw_rowid integer not null default 0,
        restart_revision integer not null,
        started_at text,
        ended_at text,
        events integer not null default 0,
        token_events integer not null default 0,
        input_tokens integer not null default 0,
        output_tokens integer not null default 0,
        cache_read_tokens integer not null default 0,
        cache_creation_tokens integer not null default 0,
        cost_nanos integer not null default 0,
        unlinked_input_tokens integer not null default 0,
        unlinked_output_tokens integer not null default 0,
        unlinked_cost_nanos integer not null default 0,
        branch_hash text,
        source text,
        started_repair_at text not null,
        updated_at text not null,
        primary key (days, session_hash)
      );
      create table if not exists dashboard_session_repair_source (
        days integer not null, session_hash text not null, source text not null,
        started_at text not null, ended_at text not null, events integer not null,
        token_events integer not null, input_tokens integer not null,
        output_tokens integer not null, cache_read_tokens integer not null,
        cache_creation_tokens integer not null, cost_nanos integer not null,
        primary key (days, session_hash, source)
      );
      create table if not exists dashboard_session_repair_repo (
        days integer not null, session_hash text not null, repo_hash text not null,
        events integer not null, input_tokens integer not null,
        output_tokens integer not null, cost_nanos integer not null,
        primary key (days, session_hash, repo_hash)
      );
      create table if not exists dashboard_session_repair_branch (
        days integer not null, session_hash text not null, repo_key text not null,
        repo_hash text, branch_hash text not null, events integer not null,
        primary key (days, session_hash, repo_key, branch_hash)
      );
      create table if not exists dashboard_session_repair_account (
        days integer not null, session_hash text not null, account_hash text not null,
        cost_nanos integer not null, events integer not null,
        primary key (days, session_hash, account_hash)
      );
      create table if not exists dashboard_session_repair_machine (
        days integer not null, session_hash text not null, machine_hash text not null,
        primary key (days, session_hash, machine_hash)
      );
      create table if not exists dashboard_window_totals (
        days integer primary key,
        events integer not null default 0,
        token_events integer not null default 0,
        input_tokens integer not null default 0,
        output_tokens integer not null default 0,
        cache_read_tokens integer not null default 0,
        cache_creation_tokens integer not null default 0,
        cost_nanos integer not null default 0
      );
      create table if not exists dashboard_lifetime_totals (
        singleton integer primary key check (singleton = 1),
        events integer not null default 0,
        token_events integer not null default 0,
        input_tokens integer not null default 0,
        output_tokens integer not null default 0,
        cost_nanos integer not null default 0,
        oldest_observed_at text,
        newest_observed_at text
      );
      create table if not exists dashboard_parity_window (
        days integer primary key,
        events integer not null default 0,
        token_events integer not null default 0,
        input_tokens integer not null default 0,
        output_tokens integer not null default 0,
        cache_read_tokens integer not null default 0,
        cache_creation_tokens integer not null default 0,
        cost_nanos integer not null default 0
      );
      create table if not exists dashboard_post_highwater_window (
        days integer primary key,
        events integer not null default 0,
        token_events integer not null default 0,
        input_tokens integer not null default 0,
        output_tokens integer not null default 0,
        cache_read_tokens integer not null default 0,
        cache_creation_tokens integer not null default 0,
        cost_nanos integer not null default 0
      );
      create table if not exists dashboard_source_window (
        days integer not null, source text not null,
        events integer not null default 0, token_events integer not null default 0,
        input_tokens integer not null default 0, output_tokens integer not null default 0,
        cost_nanos integer not null default 0,
        primary key (days, source)
      );
      create table if not exists dashboard_source_lifetime (
        source text primary key,
        last_event_at text,
        last_token_event_at text
      );
      create table if not exists dashboard_daily_window (
        days integer not null, day text not null,
        events integer not null default 0,
        cost_nanos integer not null default 0, tokens integer not null default 0,
        primary key (days, day)
      );
      create table if not exists dashboard_model_window (
        days integer not null, model text not null,
        calls integer not null default 0, unpriced_calls integer not null default 0,
        input_tokens integer not null default 0, output_tokens integer not null default 0,
        cache_read_tokens integer not null default 0, cache_creation_tokens integer not null default 0,
        cost_nanos integer not null default 0,
        primary key (days, model)
      );
      create table if not exists dashboard_action_window (
        days integer not null, action_class text not null,
        events integer not null default 0,
        primary key (days, action_class)
      );
      create table if not exists dashboard_session_source_window (
        days integer not null, session_hash text not null, source text not null,
        started_at text not null, ended_at text not null, events integer not null,
        token_events integer not null, input_tokens integer not null, output_tokens integer not null,
        cache_read_tokens integer not null, cache_creation_tokens integer not null, cost_nanos integer not null,
        primary key (days, session_hash, source)
      );
      create table if not exists dashboard_session_root_window (
        days integer not null, session_hash text not null,
        started_at text not null, ended_at text not null, events integer not null,
        token_events integer not null, dominant_repo_hash text, repo_count integer not null,
        branch_hash text, dominant_account_hash text, source text not null,
        machine_hashes_json text not null default '[]',
        input_tokens integer not null, output_tokens integer not null,
        cache_read_tokens integer not null, cache_creation_tokens integer not null, cost_nanos integer not null,
        primary key (days, session_hash)
      );
      create table if not exists dashboard_repo_session_window (
        days integer not null, repo_key text not null, repo_hash text,
        session_hash text not null, input_tokens integer not null,
        output_tokens integer not null, cost_nanos integer not null,
        primary key (days, repo_key, session_hash)
      );
      create table if not exists dashboard_repo_branch_window (
        days integer not null, repo_key text not null, repo_hash text,
        branch_hash text not null, session_hash text not null, events integer not null,
        primary key (days, repo_key, branch_hash, session_hash)
      );
      create table if not exists dashboard_account_session_window (
        days integer not null, account_key text not null, account_hash text,
        session_hash text not null, dominant_repo_hash text, source text not null,
        machine_hashes_json text not null default '[]', cost_nanos integer not null,
        input_tokens integer not null, output_tokens integer not null,
        primary key (days, session_hash)
      );
      create index if not exists idx_dashboard_repo_sessions
        on dashboard_repo_session_window (days, repo_key);
      create index if not exists idx_dashboard_repo_branches
        on dashboard_repo_branch_window (days, repo_key, branch_hash);
      create index if not exists idx_dashboard_account_sessions
        on dashboard_account_session_window (days, account_key);
      create index if not exists idx_dashboard_facts_session_rowid
        on dashboard_event_facts (session_hash, raw_rowid);
      create table if not exists dashboard_snapshots (
        days integer primary key,
        schema_version integer not null,
        generation integer not null,
        since_at text not null,
        payload_json text not null,
        created_at text not null
      );
      create table if not exists capture_activity_state (
        source text primary key,
        last_activity_at text,
        files_today integer not null default 0,
        discovery_entries integer not null default 0,
        last_scan_at text not null,
        last_error_code text,
        truncated integer not null default 0
      );
    `);
    this.ensureProjectionSchemaColumns();
    this.db.prepare(`insert or ignore into dashboard_lifetime_totals (singleton) values (1)`).run();

    this.db.prepare(
      `insert or ignore into dashboard_projection_control
       (singleton, schema_version, backfill_high_water, backfill_cursor,
        backfill_complete, parity_cursor, parity_complete, ready, parity_ready, dirty,
        metric_backfill_high_water,metric_backfill_cursor,metric_backfill_complete,
        metric_sample_count)
       values (1, ?, ?, 0, ?, 0, ?, ?, ?, 1, ?, 0, ?, ?)`,
    ).run(
      DASHBOARD_SCHEMA_VERSION,
      newLedger ? 0 : null,
      newLedger ? 1 : 0,
      newLedger ? 1 : 0,
      newLedger ? 1 : 0,
      newLedger ? 1 : 0,
      newLedger ? 0 : null,
      newLedger ? 1 : 0,
      newLedger ? 0 : null,
    );
    for (const days of INTERNAL_WINDOWS) {
      this.db.prepare(
        `insert or ignore into dashboard_window_control (days, cutoff_at)
         values (?, ?)`,
      ).run(days, sinceIso(days, now));
      this.db.prepare(
        `insert or ignore into dashboard_window_totals (days) values (?)`,
      ).run(days);
      if (DASHBOARD_WINDOWS.includes(days as typeof DASHBOARD_WINDOWS[number])) {
        this.db.prepare(`insert or ignore into dashboard_parity_window (days) values (?)`).run(days);
        this.db.prepare(`insert or ignore into dashboard_post_highwater_window (days) values (?)`).run(days);
      }
    }

    this.db.exec(`
      create trigger if not exists trg_dashboard_raw_insert
      after insert on buffered_events
      begin
        insert into dashboard_projection_repairs (raw_rowid, reason, queued_at)
        values (new.rowid, 'raw_insert', strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(raw_rowid) do update set reason = excluded.reason, queued_at = excluded.queued_at;
        update dashboard_projection_control set dirty=1,
          degraded_reason=case when generation>0 then 'projection_repair_backlog' else degraded_reason end
        where singleton=1;
      end;
      create trigger if not exists trg_dashboard_raw_update
      after update of source, event_type, observed_at, session_id, action_class, model,
        input_tokens, output_tokens, cache_read_tokens, cache_creation_tokens, cost_usd,
        repo_hash, branch_hash, head_sha, machine, account_hash, suppressed_fields_json
      on buffered_events
      begin
        insert into dashboard_compact_mutations
          (raw_rowid,observed_at,source,event_type,action_class,queued_at)
        select old.rowid,old.observed_at,old.source,old.event_type,old.action_class,
          strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where old.session_id is null and old.model is null
          and old.source in ('anthropic_admin','anthropic_usage','claude_code','codex','github','openai_usage','manual','unknown')
          and old.event_type in ('session_start','session_stop','user_prompt_submit','assistant_response','tool_use','tool_result','otel_span','usage_rollout','usage_transcript','unknown')
          and (old.action_class is null or old.action_class in ('continue','validate','test','edit','read','write','shell','mcp','browser','review','other'))
          and old.input_tokens is null and old.output_tokens is null
          and old.cache_read_tokens is null and old.cache_creation_tokens is null
          and old.cost_usd is null and old.repo_hash is null and old.branch_hash is null
          and old.head_sha is null and old.account_hash is null
          and not exists (select 1 from dashboard_projection_repairs where raw_rowid=old.rowid)
          and exists (select 1 from dashboard_projection_control
            where singleton=1 and backfill_high_water is not null
              and (old.rowid>backfill_high_water or old.rowid<=backfill_cursor))
          and (old.observed_at is not new.observed_at or old.source is not new.source
            or old.event_type is not new.event_type or old.action_class is not new.action_class
            or new.session_id is not null or new.model is not null
            or new.input_tokens is not null or new.output_tokens is not null
            or new.cache_read_tokens is not null or new.cache_creation_tokens is not null
            or new.cost_usd is not null or new.repo_hash is not null or new.branch_hash is not null
            or new.head_sha is not null or new.account_hash is not null)
        on conflict(raw_rowid) do nothing;
        insert into dashboard_projection_repairs (raw_rowid, reason, queued_at)
        values (new.rowid, 'raw_update', strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(raw_rowid) do update set reason = excluded.reason, queued_at = excluded.queued_at;
        update dashboard_projection_control set dirty=1,
          degraded_reason=case when generation>0 then 'projection_repair_backlog' else degraded_reason end
        where singleton=1;
      end;
      create trigger if not exists trg_dashboard_raw_delete
      after delete on buffered_events
      begin
        insert into dashboard_compact_mutations
          (raw_rowid,observed_at,source,event_type,action_class,queued_at)
        select old.rowid,old.observed_at,old.source,old.event_type,old.action_class,
          strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where old.session_id is null and old.model is null
          and old.source in ('anthropic_admin','anthropic_usage','claude_code','codex','github','openai_usage','manual','unknown')
          and old.event_type in ('session_start','session_stop','user_prompt_submit','assistant_response','tool_use','tool_result','otel_span','usage_rollout','usage_transcript','unknown')
          and (old.action_class is null or old.action_class in ('continue','validate','test','edit','read','write','shell','mcp','browser','review','other'))
          and old.input_tokens is null and old.output_tokens is null
          and old.cache_read_tokens is null and old.cache_creation_tokens is null
          and old.cost_usd is null and old.repo_hash is null and old.branch_hash is null
          and old.head_sha is null and old.account_hash is null
          and not exists (select 1 from dashboard_projection_repairs where raw_rowid=old.rowid)
          and exists (select 1 from dashboard_projection_control
            where singleton=1 and backfill_high_water is not null
              and (old.rowid>backfill_high_water or old.rowid<=backfill_cursor))
        on conflict(raw_rowid) do nothing;
        insert into dashboard_projection_repairs (raw_rowid, reason, queued_at)
        values (old.rowid, 'raw_delete', strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(raw_rowid) do update set reason=excluded.reason,queued_at=excluded.queued_at;
        update dashboard_projection_control set dirty=1,
          degraded_reason=case when generation>0 then 'projection_repair_backlog' else degraded_reason end
        where singleton=1;
      end;
      create trigger if not exists trg_dashboard_metric_insert
      after insert on metric_samples
      begin
        update dashboard_projection_control
        set metric_sample_count = coalesce(metric_sample_count,0) + 1, dirty=1
        where singleton = 1 and metric_backfill_high_water is not null
          and new.rowid > metric_backfill_high_water;
      end;
      create trigger if not exists trg_dashboard_metric_delete
      after delete on metric_samples
      begin
        update dashboard_projection_control
        set metric_sample_count = max(0,coalesce(metric_sample_count,0) - 1), dirty=1
        where singleton = 1 and metric_backfill_high_water is not null and (
          old.rowid > metric_backfill_high_water or old.rowid <= metric_backfill_cursor
        );
      end;
      create trigger if not exists trg_dashboard_repair_backlog_insert
      after insert on dashboard_projection_repairs begin
        update dashboard_projection_control set repair_backlog=repair_backlog+1 where singleton=1;
      end;
      create trigger if not exists trg_dashboard_repair_backlog_delete
      after delete on dashboard_projection_repairs begin
        update dashboard_projection_control set repair_backlog=max(0,repair_backlog-1) where singleton=1;
      end;
      create trigger if not exists trg_dashboard_dirty_backlog_insert
      after insert on dashboard_dirty_sessions begin
        update dashboard_projection_control set dirty_session_backlog=dirty_session_backlog+1 where singleton=1;
      end;
      create trigger if not exists trg_dashboard_dirty_backlog_delete
      after delete on dashboard_dirty_sessions begin
        update dashboard_projection_control set dirty_session_backlog=max(0,dirty_session_backlog-1) where singleton=1;
      end;
      create trigger if not exists trg_dashboard_account_backlog_insert
      after insert on dashboard_account_invalidations begin
        update dashboard_projection_control set account_invalidation_backlog=account_invalidation_backlog+1 where singleton=1;
      end;
      create trigger if not exists trg_dashboard_account_backlog_delete
      after delete on dashboard_account_invalidations begin
        update dashboard_projection_control set account_invalidation_backlog=max(0,account_invalidation_backlog-1) where singleton=1;
      end;
      create trigger if not exists trg_dashboard_compact_mutation_backlog_insert
      after insert on dashboard_compact_mutations begin
        update dashboard_projection_control set compact_mutation_backlog=compact_mutation_backlog+1 where singleton=1;
      end;
      create trigger if not exists trg_dashboard_compact_mutation_backlog_delete
      after delete on dashboard_compact_mutations begin
        update dashboard_projection_control set compact_mutation_backlog=max(0,compact_mutation_backlog-1) where singleton=1;
      end;
      create trigger if not exists trg_dashboard_compact_cancellation_gc_day
      after insert on dashboard_compact_cancellations begin
        insert into dashboard_compact_gc_days
          (bucket_day,revision,processing_revision,high_water_segment,cursor_segment,
           last_schedule,queued_at,updated_at)
        values (new.bucket_day,1,0,null,0,0,strftime('%Y-%m-%dT%H:%M:%fZ','now'),
          strftime('%Y-%m-%dT%H:%M:%fZ','now'))
        on conflict(bucket_day) do update set
          revision=revision+1,updated_at=excluded.updated_at;
      end;
      create trigger if not exists trg_dashboard_compact_segment_during_gc
      after insert on dashboard_compact_segments
      when exists (select 1 from dashboard_compact_gc_days where bucket_day=new.bucket_day)
      begin
        update dashboard_compact_gc_days set revision=revision+1,
          updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') where bucket_day=new.bucket_day;
      end;
      create trigger if not exists trg_dashboard_compact_gc_backlog_insert
      after insert on dashboard_compact_gc_days begin
        update dashboard_projection_control set compact_gc_backlog=compact_gc_backlog+1
          where singleton=1;
      end;
      create trigger if not exists trg_dashboard_compact_gc_backlog_delete
      after delete on dashboard_compact_gc_days begin
        update dashboard_projection_control set compact_gc_backlog=max(0,compact_gc_backlog-1)
          where singleton=1;
      end;
    `);
    this.db.prepare(
      `update dashboard_projection_control set compact_gc_backlog=(select count(*) from dashboard_compact_gc_days)
       where singleton=1`,
    ).run();
  }

  private ensureProjectionSchemaColumns() {
    const controlColumns=new Set((this.db.pragma("table_info(dashboard_projection_control)") as Array<{name:string}>).map((row)=>row.name));
    for(const definition of [
      "compact_gc_backlog integer not null default 0",
      "compact_gc_schedule integer not null default 0",
      "compact_segments_written integer not null default 0",
      "compact_gc_items_visited integer not null default 0",
      "compact_gc_items_removed integer not null default 0",
      "compact_gc_segments_rewritten integer not null default 0",
      "compact_gc_segments_deleted integer not null default 0",
      "compact_gc_days_completed integer not null default 0",
      "compact_gc_restarts integer not null default 0",
    ]){
      const name=definition.split(" ")[0]!;
      if(!controlColumns.has(name))this.db.exec(`alter table dashboard_projection_control add column ${definition}`);
    }
    const cancellationColumns=new Set((this.db.pragma("table_info(dashboard_compact_cancellations)") as Array<{name:string}>).map((row)=>row.name));
    if(!cancellationColumns.has("bucket_day")){
      this.db.exec(`alter table dashboard_compact_cancellations add column bucket_day text`);
      this.db.exec(`update dashboard_compact_cancellations set bucket_day=substr(observed_at,1,10) where bucket_day is null`);
    }
    this.db.exec(`create index if not exists idx_dashboard_compact_cancellation_day
      on dashboard_compact_cancellations (bucket_day,raw_rowid)`);
  }

  private control(): ProjectionControl {
    return this.db.prepare(
      `select backfill_high_water as backfillHighWater,
        backfill_cursor as backfillCursor, backfill_complete as backfillComplete,
        parity_cursor as parityCursor,parity_complete as parityComplete,
        ready, parity_ready as parityReady, generation, dirty,
        degraded_reason as degradedReason, last_success_at as lastSuccessAt,
        last_error_at as lastErrorAt,
        projection_rows_visited as projectionRowsVisited,
        projection_rows_written as projectionRowsWritten,
        snapshot_rows_visited as snapshotRowsVisited,
        snapshot_builds as snapshotBuilds, snapshot_cache_hits as snapshotCacheHits,
        repair_facts as repairFacts, backfill_facts as backfillFacts,
        expiry_facts as expiryFacts,
        raw_rows_scanned_by_dashboard as rawRowsScannedByDashboard,
        filesystem_entries_scanned_by_dashboard as filesystemEntriesScannedByDashboard,
        metric_backfill_high_water as metricBackfillHighWater,
        metric_backfill_cursor as metricBackfillCursor,
        metric_backfill_complete as metricBackfillComplete,
        metric_sample_count as metricSampleCount,
        repair_backlog as repairBacklog,
        dirty_session_backlog as dirtySessionBacklog,
        account_invalidation_backlog as accountInvalidationBacklog,
        compact_mutation_backlog as compactMutationBacklog,
        compact_gc_backlog as compactGcBacklog,
        compact_gc_schedule as compactGcSchedule,
        compact_segments_written as compactSegmentsWritten,
        compact_gc_items_visited as compactGcItemsVisited,
        compact_gc_items_removed as compactGcItemsRemoved,
        compact_gc_segments_rewritten as compactGcSegmentsRewritten,
        compact_gc_segments_deleted as compactGcSegmentsDeleted,
        compact_gc_days_completed as compactGcDaysCompleted,
        compact_gc_restarts as compactGcRestarts
       from dashboard_projection_control where singleton = 1`,
    ).get() as ProjectionControl;
  }

  private rawRow(rawRowid: number) {
    return this.db.prepare(
      `select rowid as rawRowid, id, source, event_type as eventType,
        observed_at as observedAt, session_id as sessionId, action_class as actionClass,
        model, input_tokens as inputTokens, output_tokens as outputTokens,
        cache_read_tokens as cacheReadTokens, cache_creation_tokens as cacheCreationTokens,
        cost_usd as costUsd, repo_hash as repoHash, branch_hash as branchHash,
        head_sha as headSha, machine, account_hash as accountHash,
        suppressed_fields_json as suppressedFieldsJson
       from buffered_events where rowid = ?`,
    ).get(rawRowid) as RawProjectionRow | undefined;
  }

  private storedFact(projectionId: string) {
    const row = this.db.prepare(
      `select projection_id as projectionId, raw_rowid as rawRowid, source,
        event_type as eventType, observed_at as observedAt, session_hash as sessionHash,
        action_class as actionClass, model, input_tokens as inputTokens,
        output_tokens as outputTokens, cache_read_tokens as cacheReadTokens,
        cache_creation_tokens as cacheCreationTokens, cost_nanos as costNanos,
        repo_hash as repoHash, branch_hash as branchHash, head_hash as headHash,
        machine_hash as machineHash, account_hash as accountHash, suppressed
       from dashboard_event_facts where projection_id = ?`,
    ).get(projectionId) as Record<string, unknown> | undefined;
    return row ? factFromDb(row) : undefined;
  }

  private storedFactByRawRowid(rawRowid: number) {
    const row = this.db.prepare(
      `select projection_id as projectionId, raw_rowid as rawRowid, source,
        event_type as eventType, observed_at as observedAt, session_hash as sessionHash,
        action_class as actionClass, model, input_tokens as inputTokens,
        output_tokens as outputTokens, cache_read_tokens as cacheReadTokens,
        cache_creation_tokens as cacheCreationTokens, cost_nanos as costNanos,
        repo_hash as repoHash, branch_hash as branchHash, head_hash as headHash,
        machine_hash as machineHash, account_hash as accountHash, suppressed
       from dashboard_event_facts where raw_rowid = ?`,
    ).get(rawRowid) as Record<string, unknown> | undefined;
    return row ? factFromDb(row) : undefined;
  }

  /** Capture calls this inside its raw-event transaction. Failure is isolated and repair is durable. */
  tryApplyRawRow(rawRowid: number, now = new Date(Date.now())) {
    try {
      this.db.transaction(() => {
        if (this.failNextApply) {
          this.failNextApply = false;
          throw new Error("injected_projection_failure");
        }
        const row = this.rawRow(rawRowid);
        if (!row) return;
        // Generic active rows keep the trigger-authored durable repair receipt.
        // Maintenance drains them as one day-grouped batch; capture never pays
        // one gzip segment per event and the published generation stays stale.
        if (compactable(row)) return;
        this.applyProjectionRows([row], now);
        this.db.prepare(`delete from dashboard_projection_repairs where raw_rowid = ?`).run(rawRowid);
      })();
      return true;
    } catch (error) {
      const at = now.toISOString();
      this.db.prepare(
        `insert into dashboard_projection_repairs (raw_rowid, reason, queued_at)
         values (?, 'projection_apply_failed', ?)
         on conflict(raw_rowid) do update set reason = excluded.reason, queued_at = excluded.queued_at`,
      ).run(rawRowid, at);
      this.db.prepare(
        `update dashboard_projection_control set dirty = 1,
          degraded_reason = 'projection_repair_backlog', last_error_at = ? where singleton = 1`,
      ).run(at);
      return false;
    }
  }

  /** Test seam for the transaction/repair boundary; never enabled by production flow. */
  failNextApplyForProof() {
    this.failNextApply = true;
  }

  /** Test seam: the enclosing SQLite transaction must roll back both payload and receipts. */
  failNextCompactGcAfterRewriteForProof() {
    this.failNextCompactGcAfterRewrite = true;
  }

  private applyProjectionRows(rows: RawProjectionRow[], now: Date) {
    const compactRows: RawProjectionRow[] = [];
    for (const row of rows) {
      if (compactable(row)) {
        const previous = this.storedFact(sha256(`event:${row.id}`));
        if (previous) this.removeStoredFact(previous, now);
        compactRows.push(row);
      } else {
        this.applyFact(factFromRaw(row), now);
      }
    }
    if (compactRows.length) this.addCompactRows(compactRows);
  }

  private compactWindowCutoffs() {
    return this.db.prepare(
      `select days,coalesce(target_cutoff_at,cutoff_at) as cutoffAt
       from dashboard_window_control`,
    ).all() as Array<{days:number;cutoffAt:string}>;
  }

  private compactMask(observedAt: string,windows=this.compactWindowCutoffs()) {
    let mask = 0;
    for (const window of windows) {
      const bit = INTERNAL_WINDOWS.indexOf(window.days as typeof INTERNAL_WINDOWS[number]);
      if (bit >= 0 && observedAt >= window.cutoffAt) mask |= 1 << bit;
    }
    return mask;
  }

  private addCompactRows(rows: RawProjectionRow[]) {
    const windows=this.compactWindowCutoffs();
    const items = rows.map((row):CompactProjectionItem=>({rawRowid:row.rawRowid,
      observedAt:row.observedAt,source:safeClassification(row.source,SAFE_SOURCES,"unknown"),
      eventType:safeClassification(row.eventType,SAFE_EVENT_TYPES,"unknown"),
      actionClass:safeClassification(row.actionClass,SAFE_ACTIONS,"other"),windowMask:this.compactMask(row.observedAt,windows)}));
    this.applyCompactAggregates(items,1);
    const control=this.control();
    for(const item of items)this.applyCompactReference(item,1,control);
    const sourceLatest=new Map<string,string>();
    for(const item of items)if(!sourceLatest.has(item.source)||item.observedAt>sourceLatest.get(item.source)!)sourceLatest.set(item.source,item.observedAt);
    for(const [source,observedAt] of sourceLatest)this.touchSourceLatest(source,observedAt,false);
    this.addCompactDaySummaries(items);
    const buckets = new Map<string,CompactProjectionItem[]>();
    for (const item of items){const bucket=day(item.observedAt),values=buckets.get(bucket)??[];values.push(item);buckets.set(bucket,values);}
    let segments=0;
    for (const [bucket, bucketItems] of buckets) {
      let offset=0;
      const gcActive=this.db.prepare(`select 1 from dashboard_compact_gc_days where bucket_day=?`).get(bucket);
      const tail=!gcActive?this.db.prepare(
        `select segment_id as segmentId,event_count as eventCount,payload_gzip as payload
         from dashboard_compact_segments where bucket_day=? order by segment_id desc limit 1`,
      ).get(bucket) as {segmentId:number;eventCount:number;payload:Buffer}|undefined:undefined;
      if(tail&&tail.eventCount<BACKFILL_ROWS){
        const added=bucketItems.slice(0,BACKFILL_ROWS-tail.eventCount);
        if(added.length){
          const combined=[...decodeCompact(tail.payload),...added];
          const observed=combined.map((item)=>item.observedAt).sort();
          this.db.prepare(
            `update dashboard_compact_segments set min_observed_at=?,max_observed_at=?,
              event_count=?,payload_gzip=? where segment_id=?`,
          ).run(observed[0],observed.at(-1),combined.length,encodeCompact(combined),tail.segmentId);
          offset=added.length;
          segments++;
        }
      }
      for (;offset<bucketItems.length;offset+=BACKFILL_ROWS) {
        const chunk=bucketItems.slice(offset,offset+BACKFILL_ROWS);
        const observed=chunk.map((item)=>item.observedAt).sort();
        this.db.prepare(
          `insert into dashboard_compact_segments
           (bucket_day,min_observed_at,max_observed_at,event_count,payload_gzip)
           values (?,?,?,?,?)`,
        ).run(bucket,observed[0],observed.at(-1),chunk.length,encodeCompact(chunk));
        segments++;
      }
    }
    this.db.prepare(
      `update dashboard_projection_control set dirty=1,
        projection_rows_visited=projection_rows_visited+?,
        projection_rows_written=projection_rows_written+?,
        compact_segments_written=compact_segments_written+? where singleton=1`,
    ).run(items.length,segments,segments);
  }

  private addCompactDaySummaries(items:CompactProjectionItem[]) {
    const groups=new Map<string,{bucketDay:string;source:string;count:number;min:string;max:string}>();
    for(const item of items){
      const bucketDay=day(item.observedAt),key=`${bucketDay}\u0000${item.source}`;
      const current=groups.get(key);
      if(current){
        current.count++;
        if(item.observedAt<current.min)current.min=item.observedAt;
        if(item.observedAt>current.max)current.max=item.observedAt;
      }else groups.set(key,{bucketDay,source:item.source,count:1,min:item.observedAt,max:item.observedAt});
    }
    const upsert=this.db.prepare(
      `insert into dashboard_compact_day_source
       (bucket_day,source,event_count,min_observed_at,max_observed_at)
       values (@bucketDay,@source,@count,@min,@max)
       on conflict(bucket_day,source) do update set
        event_count=event_count+excluded.event_count,
        min_observed_at=min(min_observed_at,excluded.min_observed_at),
        max_observed_at=max(max_observed_at,excluded.max_observed_at)`,
    );
    for(const group of groups.values())upsert.run(group);
  }

  private applyCompactReference(item:CompactProjectionItem,sign:1|-1,
    control=this.control()){
    if(control.backfillHighWater===null)return;
    const table=item.rawRowid>control.backfillHighWater?"dashboard_post_highwater_window":
      control.parityCursor>=item.rawRowid?"dashboard_parity_window":null;
    if(!table)return;
    const fact:ProjectionFact={projectionId:`compact:${item.rawRowid}`,rawRowid:item.rawRowid,
      source:item.source,eventType:item.eventType,observedAt:item.observedAt,sessionHash:null,
      actionClass:item.actionClass,model:null,inputTokens:null,outputTokens:null,cacheReadTokens:null,
      cacheCreationTokens:null,costNanos:null,repoHash:null,branchHash:null,headHash:null,
      machineHash:null,accountHash:null,suppressed:0};
    for(let bit=0;bit<INTERNAL_WINDOWS.length;bit++)if((item.windowMask&(1<<bit))&&
      DASHBOARD_WINDOWS.includes(INTERNAL_WINDOWS[bit] as typeof DASHBOARD_WINDOWS[number])){
      this.applyReferenceDelta(table,INTERNAL_WINDOWS[bit]!,fact,sign);
    }
  }

  private applyCompactAggregates(items:CompactProjectionItem[],sign:1|-1,lifetime=true) {
    if(!items.length)return;
    const sourceCounts=new Map<string,number>(),dailyCounts=new Map<string,number>(),actionCounts=new Map<string,number>();
    const windowCounts=new Map<number,number>();
    for(const item of items){
      for(let bit=0;bit<INTERNAL_WINDOWS.length;bit++){
        if(!(item.windowMask&(1<<bit)))continue;
        const days=INTERNAL_WINDOWS[bit]!;
        add(windowCounts,days,1);
        add(sourceCounts,`${days}\u0000${item.source}`,1);
        add(dailyCounts,`${days}\u0000${day(item.observedAt)}`,1);
        if(item.eventType==="tool_use"||item.eventType==="tool_result")add(actionCounts,`${days}\u0000${item.actionClass??"other"}`,1);
      }
    }
    for(const [days,count] of windowCounts)this.db.prepare(
      `update dashboard_window_totals set events=events+? where days=?`,
    ).run(sign*count,days);
    for(const [key,count] of sourceCounts){const [days,source]=key.split("\u0000");this.db.prepare(
      `insert into dashboard_source_window (days,source,events)
       values (?,?,?) on conflict(days,source) do update set events=events+excluded.events`,
    ).run(Number(days),source,sign*count);}
    for(const [key,count] of dailyCounts){const [days,bucket]=key.split("\u0000");this.db.prepare(
      `insert into dashboard_daily_window (days,day,events)
       values (?,?,?) on conflict(days,day) do update set events=events+excluded.events`,
    ).run(Number(days),bucket,sign*count);}
    for(const [key,count] of actionCounts){const [days,action]=key.split("\u0000");this.db.prepare(
      `insert into dashboard_action_window (days,action_class,events)
       values (?,?,?) on conflict(days,action_class) do update set events=events+excluded.events`,
    ).run(Number(days),action,sign*count);}
    if(lifetime){
      const observed=items.map((item)=>item.observedAt).sort();
      this.db.prepare(
        `update dashboard_lifetime_totals set events=events+?,
          oldest_observed_at=case when ?>0 and (oldest_observed_at is null or ?<oldest_observed_at) then ? else oldest_observed_at end,
          newest_observed_at=case when ?>0 and (newest_observed_at is null or ?>newest_observed_at) then ? else newest_observed_at end
         where singleton=1`,
      ).run(sign*items.length,sign,observed[0],observed[0],sign,observed.at(-1),observed.at(-1));
    }
    if(sign<0){
      this.db.exec(`
        delete from dashboard_source_window where events=0;
        delete from dashboard_daily_window where events=0;
        delete from dashboard_action_window where events=0;
      `);
    }
  }

  private removeStoredFact(previous:ProjectionFact,now:Date){
    const bounds=this.db.prepare(
      `select oldest_observed_at as oldest,newest_observed_at as newest from dashboard_lifetime_totals where singleton=1`,
    ).get() as {oldest:string|null;newest:string|null};
    const sourceLatest=this.db.prepare(
      `select last_event_at as lastEventAt,last_token_event_at as lastTokenAt
       from dashboard_source_lifetime where source=?`,
    ).get(previous.source) as {lastEventAt:string|null;lastTokenAt:string|null}|undefined;
    const windows=this.db.prepare(
      `select days,cutoff_at as cutoffAt,target_cutoff_at as targetCutoffAt,
        expiry_cursor_at as expiryCursorAt,expiry_cursor_id as expiryCursorId from dashboard_window_control`,
    ).all() as Array<{days:number;cutoffAt:string;targetCutoffAt:string|null;expiryCursorAt:string|null;expiryCursorId:string|null}>;
    for(const window of windows)if(this.factIncludedInWindow(previous,window)){
      this.applyFlatDelta(window.days,previous,-1);
      if(previous.sessionHash)this.markSessionDirty(window.days,previous.sessionHash,"fact_delete",now,true);
    }
    const reference=this.control();
    const table=reference.backfillHighWater!==null?(previous.rawRowid>reference.backfillHighWater
      ?"dashboard_post_highwater_window":reference.parityCursor>=previous.rawRowid?"dashboard_parity_window":null):null;
    if(table)for(const window of windows)if(DASHBOARD_WINDOWS.includes(window.days as typeof DASHBOARD_WINDOWS[number])&&previous.observedAt>=(window.targetCutoffAt??window.cutoffAt))this.applyReferenceDelta(table,window.days,previous,-1);
    this.applyLifetimeDelta(previous,-1);
    this.db.prepare(`delete from dashboard_event_facts where projection_id=?`).run(previous.projectionId);
    if(bounds.oldest===previous.observedAt||bounds.newest===previous.observedAt)this.refreshLifetimeBounds();
    if(sourceLatest&&(sourceLatest.lastEventAt===previous.observedAt||
      (previous.inputTokens!==null&&sourceLatest.lastTokenAt===previous.observedAt)))this.refreshSourceLatest(previous.source);
    this.db.prepare(`update dashboard_projection_control set dirty=1,
      projection_rows_visited=projection_rows_visited+1,projection_rows_written=projection_rows_written+1 where singleton=1`).run();
  }

  private applyFact(next: ProjectionFact, now: Date) {
    const previous = this.storedFact(next.projectionId);
    if (previous && JSON.stringify(previous) === JSON.stringify(next)) return false;
    const windows = this.db.prepare(
      `select days, cutoff_at as cutoffAt,target_cutoff_at as targetCutoffAt,
        expiry_cursor_at as expiryCursorAt,expiry_cursor_id as expiryCursorId
       from dashboard_window_control`,
    ).all() as Array<{ days: number; cutoffAt: string; targetCutoffAt:string|null;expiryCursorAt:string|null;expiryCursorId:string|null }>;
    for (const window of windows) {
      const previousIncluded = previous && this.factIncludedInWindow(previous, window);
      if (previousIncluded && previous) {
        this.applyFlatDelta(window.days, previous, -1);
        if (previous.sessionHash) this.markSessionDirty(window.days, previous.sessionHash, "fact_update", now);
      }
      const admissionCutoff = window.targetCutoffAt ?? window.cutoffAt;
      if (next.observedAt >= admissionCutoff) {
        this.applyFlatDelta(window.days, next, 1);
        if (next.sessionHash) this.markSessionDirty(window.days, next.sessionHash, "fact_update", now,Boolean(previous));
      }
    }
    const reference = this.control();
    if (reference.backfillHighWater !== null) {
      const referenceTable = next.rawRowid > reference.backfillHighWater
        ? "dashboard_post_highwater_window"
        : reference.parityCursor >= next.rawRowid
          ? "dashboard_parity_window"
          : null;
      if (referenceTable) {
        for (const window of windows) {
          if (!DASHBOARD_WINDOWS.includes(window.days as typeof DASHBOARD_WINDOWS[number])) continue;
          const cutoff = window.targetCutoffAt ?? window.cutoffAt;
          if (previous && previous.observedAt >= cutoff) {
            this.applyReferenceDelta(referenceTable, window.days, previous, -1);
          }
          if (next.observedAt >= cutoff) {
            this.applyReferenceDelta(referenceTable, window.days, next, 1);
          }
        }
      }
    }
    if (previous) this.applyLifetimeDelta(previous, -1);
    this.applyLifetimeDelta(next, 1);
    const previousSourceLatest=previous?this.db.prepare(
      `select last_event_at as lastEventAt,last_token_event_at as lastTokenAt
       from dashboard_source_lifetime where source=?`,
    ).get(previous.source) as {lastEventAt:string|null;lastTokenAt:string|null}|undefined:undefined;
    this.db.prepare(
      `insert into dashboard_event_facts
       (projection_id, raw_rowid, source, event_type, observed_at, session_hash,
        action_class, model, input_tokens, output_tokens, cache_read_tokens,
        cache_creation_tokens, cost_nanos, repo_hash, branch_hash, head_hash,
        machine_hash, account_hash, suppressed)
       values (@projectionId, @rawRowid, @source, @eventType, @observedAt, @sessionHash,
        @actionClass, @model, @inputTokens, @outputTokens, @cacheReadTokens,
        @cacheCreationTokens, @costNanos, @repoHash, @branchHash, @headHash,
        @machineHash, @accountHash, @suppressed)
       on conflict(projection_id) do update set
        raw_rowid=excluded.raw_rowid, source=excluded.source, event_type=excluded.event_type,
        observed_at=excluded.observed_at, session_hash=excluded.session_hash,
        action_class=excluded.action_class, model=excluded.model,
        input_tokens=excluded.input_tokens, output_tokens=excluded.output_tokens,
        cache_read_tokens=excluded.cache_read_tokens,
        cache_creation_tokens=excluded.cache_creation_tokens,
        cost_nanos=excluded.cost_nanos, repo_hash=excluded.repo_hash,
        branch_hash=excluded.branch_hash, head_hash=excluded.head_hash,
        machine_hash=excluded.machine_hash, account_hash=excluded.account_hash,
        suppressed=excluded.suppressed`,
    ).run(next);
    if(previous&&previousSourceLatest&&(previousSourceLatest.lastEventAt===previous.observedAt||
      (previous.inputTokens!==null&&previousSourceLatest.lastTokenAt===previous.observedAt))){
      this.refreshSourceLatest(previous.source);
    }
    this.touchSourceLatest(next.source,next.observedAt,next.inputTokens!==null);
    if (previous && previous.observedAt !== next.observedAt) {
      const bounds = this.db.prepare(
        `select oldest_observed_at as oldest,newest_observed_at as newest
         from dashboard_lifetime_totals where singleton=1`,
      ).get() as { oldest: string | null; newest: string | null };
      if (bounds.oldest === previous.observedAt || bounds.newest === previous.observedAt) {
        this.refreshLifetimeBounds();
      }
    }
    this.db.prepare(
      `update dashboard_projection_control set dirty = 1,
        projection_rows_visited = projection_rows_visited + ?,
        projection_rows_written = projection_rows_written + ? where singleton = 1`,
    ).run(previous ? 2 : 1, windows.length * (previous ? 2 : 1) + 1);
    return true;
  }

  private factIncludedInWindow(
    fact: ProjectionFact,
    window: { cutoffAt:string;targetCutoffAt:string|null;expiryCursorAt:string|null;expiryCursorId:string|null },
  ) {
    if (!window.targetCutoffAt) return fact.observedAt >= window.cutoffAt;
    if (fact.observedAt >= window.targetCutoffAt) return true;
    if (fact.observedAt < window.cutoffAt) return false;
    const cursorAt = window.expiryCursorAt ?? window.cutoffAt;
    const cursorId = window.expiryCursorId ?? "";
    return fact.observedAt > cursorAt ||
      (fact.observedAt === cursorAt && fact.projectionId > cursorId);
  }

  private applyLifetimeDelta(fact: ProjectionFact, sign: 1 | -1) {
    const tokenEvent = fact.inputTokens !== null || fact.outputTokens !== null ? 1 : 0;
    this.db.prepare(
      `update dashboard_lifetime_totals set
        events=events+@events, token_events=token_events+@tokenEvents,
        input_tokens=input_tokens+@inputTokens, output_tokens=output_tokens+@outputTokens,
        cost_nanos=cost_nanos+@costNanos,
        oldest_observed_at=case
          when @events>0 and (oldest_observed_at is null or @observedAt<oldest_observed_at) then @observedAt
          else oldest_observed_at end,
        newest_observed_at=case
          when @events>0 and (newest_observed_at is null or @observedAt>newest_observed_at) then @observedAt
          else newest_observed_at end
       where singleton=1`,
    ).run({ events: sign, tokenEvents: sign * tokenEvent,
      inputTokens: sign * (fact.inputTokens ?? 0), outputTokens: sign * (fact.outputTokens ?? 0),
      costNanos: sign * (fact.costNanos ?? 0), observedAt: fact.observedAt });
  }

  private applyFlatDelta(days: number, fact: ProjectionFact, sign: 1 | -1) {
    const tokenEvent = fact.inputTokens !== null || fact.outputTokens !== null ? 1 : 0;
    const values = {
      days,
      events: sign,
      tokenEvents: sign * tokenEvent,
      inputTokens: sign * (fact.inputTokens ?? 0),
      outputTokens: sign * (fact.outputTokens ?? 0),
      cacheReadTokens: sign * (fact.cacheReadTokens ?? 0),
      cacheCreationTokens: sign * (fact.cacheCreationTokens ?? 0),
      costNanos: sign * (fact.costNanos ?? 0),
    };
    this.db.prepare(
      `insert into dashboard_window_totals
       (days, events, token_events, input_tokens, output_tokens,
        cache_read_tokens, cache_creation_tokens, cost_nanos)
       values (@days,@events,@tokenEvents,@inputTokens,@outputTokens,
        @cacheReadTokens,@cacheCreationTokens,@costNanos)
       on conflict(days) do update set
        events=events+excluded.events, token_events=token_events+excluded.token_events,
        input_tokens=input_tokens+excluded.input_tokens,
        output_tokens=output_tokens+excluded.output_tokens,
        cache_read_tokens=cache_read_tokens+excluded.cache_read_tokens,
        cache_creation_tokens=cache_creation_tokens+excluded.cache_creation_tokens,
        cost_nanos=cost_nanos+excluded.cost_nanos`,
    ).run(values);
    this.db.prepare(
      `insert into dashboard_source_window
       (days,source,events,token_events,input_tokens,output_tokens,cost_nanos)
       values (@days,@source,@events,@tokenEvents,@inputTokens,@outputTokens,@costNanos)
       on conflict(days,source) do update set events=events+excluded.events,
        token_events=token_events+excluded.token_events,
        input_tokens=input_tokens+excluded.input_tokens,
        output_tokens=output_tokens+excluded.output_tokens,
        cost_nanos=cost_nanos+excluded.cost_nanos`,
    ).run({ ...values, source: fact.source });
    this.db.prepare(
      `insert into dashboard_daily_window (days,day,events,cost_nanos,tokens)
       values (@days,@day,@events,@costNanos,@tokens)
       on conflict(days,day) do update set cost_nanos=cost_nanos+excluded.cost_nanos,
        tokens=tokens+excluded.tokens,events=events+excluded.events`,
    ).run({ days, day: day(fact.observedAt), events:sign, costNanos: values.costNanos, tokens: values.inputTokens + values.outputTokens });
    if (
      fact.model &&
      (fact.inputTokens !== null || fact.outputTokens !== null || fact.costNanos !== null)
    ) {
      this.db.prepare(
        `insert into dashboard_model_window
         (days,model,calls,unpriced_calls,input_tokens,output_tokens,
          cache_read_tokens,cache_creation_tokens,cost_nanos)
         values (@days,@model,@calls,@unpricedCalls,@inputTokens,@outputTokens,
          @cacheReadTokens,@cacheCreationTokens,@costNanos)
         on conflict(days,model) do update set calls=calls+excluded.calls,
          unpriced_calls=unpriced_calls+excluded.unpriced_calls,
          input_tokens=input_tokens+excluded.input_tokens,
          output_tokens=output_tokens+excluded.output_tokens,
          cache_read_tokens=cache_read_tokens+excluded.cache_read_tokens,
          cache_creation_tokens=cache_creation_tokens+excluded.cache_creation_tokens,
          cost_nanos=cost_nanos+excluded.cost_nanos`,
      ).run({ ...values, model: fact.model, calls: sign, unpricedCalls: fact.costNanos === null ? sign : 0 });
    }
    if (fact.eventType === "tool_use" || fact.eventType === "tool_result") {
      this.db.prepare(
        `insert into dashboard_action_window (days,action_class,events)
         values (?,?,?) on conflict(days,action_class) do update set events=events+excluded.events`,
      ).run(days, fact.actionClass ?? "other", sign);
    }
    if (sign < 0) {
      this.db.prepare(`delete from dashboard_source_window where days=? and source=? and events=0`).run(days,fact.source);
      this.db.prepare(`delete from dashboard_daily_window where days=? and day=? and events=0`).run(days,day(fact.observedAt));
      if (fact.model) this.db.prepare(`delete from dashboard_model_window where days=? and model=? and calls=0`).run(days,fact.model);
      if (fact.eventType === "tool_use" || fact.eventType === "tool_result") {
        this.db.prepare(`delete from dashboard_action_window where days=? and action_class=? and events=0`).run(days,fact.actionClass??"other");
      }
    }
  }

  private applyReferenceDelta(
    table: "dashboard_parity_window" | "dashboard_post_highwater_window",
    days: number,
    fact: ProjectionFact,
    sign: 1 | -1,
  ) {
    const tokenEvent = fact.inputTokens !== null || fact.outputTokens !== null ? 1 : 0;
    this.db.prepare(
      `insert into ${table}
       (days,events,token_events,input_tokens,output_tokens,cache_read_tokens,
        cache_creation_tokens,cost_nanos)
       values (@days,@events,@tokenEvents,@inputTokens,@outputTokens,@cacheReadTokens,
        @cacheCreationTokens,@costNanos)
       on conflict(days) do update set events=events+excluded.events,
        token_events=token_events+excluded.token_events,
        input_tokens=input_tokens+excluded.input_tokens,
        output_tokens=output_tokens+excluded.output_tokens,
        cache_read_tokens=cache_read_tokens+excluded.cache_read_tokens,
        cache_creation_tokens=cache_creation_tokens+excluded.cache_creation_tokens,
        cost_nanos=cost_nanos+excluded.cost_nanos`,
    ).run({ days, events: sign, tokenEvents: sign * tokenEvent,
      inputTokens: sign * (fact.inputTokens ?? 0), outputTokens: sign * (fact.outputTokens ?? 0),
      cacheReadTokens: sign * (fact.cacheReadTokens ?? 0),
      cacheCreationTokens: sign * (fact.cacheCreationTokens ?? 0),
      costNanos: sign * (fact.costNanos ?? 0) });
  }

  private applyReferenceBatch(
    table:"dashboard_parity_window"|"dashboard_post_highwater_window",
    windows:Array<{days:number;cutoffAt:string}>,facts:ProjectionFact[],
  ){
    for(const window of windows){
      let events=0,tokenEvents=0,inputTokens=0,outputTokens=0,cacheReadTokens=0,
        cacheCreationTokens=0,totalCostNanos=0;
      for(const fact of facts)if(fact.observedAt>=window.cutoffAt){
        events++;if(fact.inputTokens!==null||fact.outputTokens!==null)tokenEvents++;
        inputTokens+=fact.inputTokens??0;outputTokens+=fact.outputTokens??0;
        cacheReadTokens+=fact.cacheReadTokens??0;cacheCreationTokens+=fact.cacheCreationTokens??0;
        totalCostNanos+=fact.costNanos??0;
      }
      if(!events)continue;
      this.db.prepare(
        `update ${table} set events=events+?,token_events=token_events+?,
          input_tokens=input_tokens+?,output_tokens=output_tokens+?,
          cache_read_tokens=cache_read_tokens+?,cache_creation_tokens=cache_creation_tokens+?,
          cost_nanos=cost_nanos+? where days=?`,
      ).run(events,tokenEvents,inputTokens,outputTokens,cacheReadTokens,cacheCreationTokens,totalCostNanos,window.days);
    }
  }

  private markSessionDirty(days: number, sessionHash: string, reason: string, now: Date, restart=true) {
    this.db.prepare(
      `insert into dashboard_dirty_sessions
       (days,session_hash,reason,queued_at,revision,restart_revision)
       values (?,?,?,?,1,?) on conflict(days,session_hash) do update set
        reason=excluded.reason,queued_at=excluded.queued_at,
        revision=dashboard_dirty_sessions.revision+1,
        restart_revision=dashboard_dirty_sessions.restart_revision+excluded.restart_revision`,
    ).run(days, sessionHash, reason, now.toISOString(),restart?1:0);
  }

  private aliases() {
    const rows = this.db.prepare(
      `select alias_hash as aliasHash, canonical_hash as canonicalHash from account_aliases`,
    ).all() as Array<{ aliasHash: string; canonicalHash: string }>;
    const aliases = new Map<string, string>();
    for (const row of rows) aliases.set(safeHash(row.aliasHash)!, safeHash(row.canonicalHash)!);
    return aliases;
  }

  private clearSessionRepair(days:number,sessionHash:string){
    for(const table of ["dashboard_session_repair_source","dashboard_session_repair_repo",
      "dashboard_session_repair_branch","dashboard_session_repair_account","dashboard_session_repair_machine",
      "dashboard_session_repair_jobs"]){
      this.db.prepare(`delete from ${table} where days=? and session_hash=?`).run(days,sessionHash);
    }
  }

  private repairSessionChunk(days:number,sessionHash:string,limit:number,now:Date){
    const dirty=this.db.prepare(
      `select restart_revision as restartRevision from dashboard_dirty_sessions where days=? and session_hash=?`,
    ).get(days,sessionHash) as {restartRevision:number}|undefined;
    if(!dirty)return {rowsVisited:0,finalized:false};
    let job=this.db.prepare(
      `select cutoff_at as cutoffAt,high_water as highWater,cursor_raw_rowid as cursorRawRowid,
        restart_revision as restartRevision from dashboard_session_repair_jobs
       where days=? and session_hash=?`,
    ).get(days,sessionHash) as {cutoffAt:string;highWater:number;cursorRawRowid:number;restartRevision:number}|undefined;
    const highWater=(this.db.prepare(
      `select coalesce(max(raw_rowid),0) as n from dashboard_event_facts where session_hash=?`,
    ).get(sessionHash) as {n:number}).n;
    if(!job||job.restartRevision!==dirty.restartRevision){
      this.clearSessionRepair(days,sessionHash);
      const cutoff=(this.db.prepare(
        `select coalesce(target_cutoff_at,cutoff_at) as cutoffAt from dashboard_window_control where days=?`,
      ).get(days) as {cutoffAt:string}).cutoffAt;
      this.db.prepare(
        `insert into dashboard_session_repair_jobs
         (days,session_hash,cutoff_at,high_water,restart_revision,started_repair_at,updated_at)
         values (?,?,?,?,?,?,?)`,
      ).run(days,sessionHash,cutoff,highWater,dirty.restartRevision,now.toISOString(),now.toISOString());
      job={cutoffAt:cutoff,highWater,cursorRawRowid:0,restartRevision:dirty.restartRevision};
    }else if(highWater>job.highWater){
      this.db.prepare(`update dashboard_session_repair_jobs set high_water=?,updated_at=? where days=? and session_hash=?`).run(highWater,now.toISOString(),days,sessionHash);
      job.highWater=highWater;
    }
    const facts=(this.db.prepare(
      `select projection_id as projectionId,raw_rowid as rawRowid,source,event_type as eventType,
        observed_at as observedAt,session_hash as sessionHash,action_class as actionClass,model,
        input_tokens as inputTokens,output_tokens as outputTokens,cache_read_tokens as cacheReadTokens,
        cache_creation_tokens as cacheCreationTokens,cost_nanos as costNanos,repo_hash as repoHash,
        branch_hash as branchHash,head_hash as headHash,machine_hash as machineHash,
        account_hash as accountHash,suppressed from dashboard_event_facts
       where session_hash=? and raw_rowid>? and raw_rowid<=? and observed_at>=?
       order by raw_rowid limit ?`,
    ).all(sessionHash,job.cursorRawRowid,job.highWater,job.cutoffAt,limit) as Array<Record<string,unknown>>).map(factFromDb);
    const aliases=this.aliases();
    const updateJob=this.db.prepare(
      `update dashboard_session_repair_jobs set cursor_raw_rowid=?,updated_at=?,
        started_at=case when started_at is null or ?<started_at then ? else started_at end,
        ended_at=case when ended_at is null or ?>ended_at then ? else ended_at end,
        events=events+1,token_events=token_events+?,input_tokens=input_tokens+?,
        output_tokens=output_tokens+?,cache_read_tokens=cache_read_tokens+?,
        cache_creation_tokens=cache_creation_tokens+?,cost_nanos=cost_nanos+?,
        unlinked_input_tokens=unlinked_input_tokens+?,unlinked_output_tokens=unlinked_output_tokens+?,
        unlinked_cost_nanos=unlinked_cost_nanos+?,
        branch_hash=case when ? is not null and (branch_hash is null or ?>branch_hash) then ? else branch_hash end,
        source=case when source is null or ?>source then ? else source end
       where days=? and session_hash=?`,
    );
    const upsertSource=this.db.prepare(
      `insert into dashboard_session_repair_source
       (days,session_hash,source,started_at,ended_at,events,token_events,input_tokens,output_tokens,
        cache_read_tokens,cache_creation_tokens,cost_nanos) values (?,?,?,?,?,1,?,?,?,?,?,?)
       on conflict(days,session_hash,source) do update set
        started_at=min(started_at,excluded.started_at),ended_at=max(ended_at,excluded.ended_at),
        events=events+1,token_events=token_events+excluded.token_events,
        input_tokens=input_tokens+excluded.input_tokens,output_tokens=output_tokens+excluded.output_tokens,
        cache_read_tokens=cache_read_tokens+excluded.cache_read_tokens,
        cache_creation_tokens=cache_creation_tokens+excluded.cache_creation_tokens,
        cost_nanos=cost_nanos+excluded.cost_nanos`,
    );
    const upsertRepo=this.db.prepare(
      `insert into dashboard_session_repair_repo
       (days,session_hash,repo_hash,events,input_tokens,output_tokens,cost_nanos) values (?,?,?,1,?,?,?)
       on conflict(days,session_hash,repo_hash) do update set events=events+1,
        input_tokens=input_tokens+excluded.input_tokens,output_tokens=output_tokens+excluded.output_tokens,
        cost_nanos=cost_nanos+excluded.cost_nanos`,
    );
    const upsertBranch=this.db.prepare(
      `insert into dashboard_session_repair_branch
       (days,session_hash,repo_key,repo_hash,branch_hash,events) values (?,?,?,?,?,1)
       on conflict(days,session_hash,repo_key,branch_hash) do update set events=events+1`,
    );
    const upsertAccount=this.db.prepare(
      `insert into dashboard_session_repair_account
       (days,session_hash,account_hash,cost_nanos,events) values (?,?,?,?,1)
       on conflict(days,session_hash,account_hash) do update set cost_nanos=cost_nanos+excluded.cost_nanos,events=events+1`,
    );
    const upsertMachine=this.db.prepare(
      `insert or ignore into dashboard_session_repair_machine (days,session_hash,machine_hash) values (?,?,?)`,
    );
    for(const fact of facts){
      const tokenEvent=fact.inputTokens!==null?1:0;
      updateJob.run(fact.rawRowid,now.toISOString(),fact.observedAt,fact.observedAt,fact.observedAt,fact.observedAt,
        tokenEvent,fact.inputTokens??0,fact.outputTokens??0,fact.cacheReadTokens??0,fact.cacheCreationTokens??0,
        fact.costNanos??0,fact.repoHash?0:fact.inputTokens??0,fact.repoHash?0:fact.outputTokens??0,
        fact.repoHash?0:fact.costNanos??0,fact.branchHash,fact.branchHash,fact.branchHash,
        fact.source,fact.source,days,sessionHash);
      upsertSource.run(days,sessionHash,fact.source,fact.observedAt,fact.observedAt,tokenEvent,
        fact.inputTokens??0,fact.outputTokens??0,fact.cacheReadTokens??0,fact.cacheCreationTokens??0,fact.costNanos??0);
      if(fact.repoHash)upsertRepo.run(days,sessionHash,fact.repoHash,fact.inputTokens??0,fact.outputTokens??0,fact.costNanos??0);
      if(fact.branchHash)upsertBranch.run(days,sessionHash,fact.repoHash??UNLINKED_REPO,fact.repoHash,fact.branchHash);
      if(fact.accountHash){const account=aliases.get(fact.accountHash)??fact.accountHash;upsertAccount.run(days,sessionHash,account,fact.costNanos??0);}
      if(fact.machineHash)upsertMachine.run(days,sessionHash,fact.machineHash);
    }
    const exhausted=facts.length<limit;
    if(exhausted)this.finalizeSessionRepair(days,sessionHash);
    return {rowsVisited:facts.length,finalized:exhausted};
  }

  private finalizeSessionRepair(days:number,sessionHash:string){
    const job=this.db.prepare(`select * from dashboard_session_repair_jobs where days=? and session_hash=?`).get(days,sessionHash) as Record<string,unknown>;
    for(const table of ["dashboard_session_source_window","dashboard_session_root_window",
      "dashboard_repo_session_window","dashboard_repo_branch_window","dashboard_account_session_window"]){
      this.db.prepare(`delete from ${table} where days=? and session_hash=?`).run(days,sessionHash);
    }
    if(Number(job.events)>0){
      const dominantRepo=(this.db.prepare(
        `select r.repo_hash as repoHash from dashboard_session_repair_repo r
         left join (select repo_hash,count(*) as branches from dashboard_session_repair_branch
          where days=? and session_hash=? and repo_hash is not null group by repo_hash) b
          on b.repo_hash=r.repo_hash
         where r.days=? and r.session_hash=?
         order by r.events desc,coalesce(b.branches,0) desc,r.repo_hash limit 1`,
      ).get(days,sessionHash,days,sessionHash) as {repoHash:string}|undefined)?.repoHash??null;
      const dominantAccount=(this.db.prepare(
        `select account_hash as accountHash from dashboard_session_repair_account
         where days=? and session_hash=? order by cost_nanos desc,events desc,account_hash limit 1`,
      ).get(days,sessionHash) as {accountHash:string}|undefined)?.accountHash??null;
      const machines=(this.db.prepare(
        `select machine_hash as machineHash from dashboard_session_repair_machine
         where days=? and session_hash=? order by machine_hash`,
      ).all(days,sessionHash) as Array<{machineHash:string}>).map((row)=>row.machineHash);
      const repoCount=(this.db.prepare(
        `select count(*) as n from dashboard_session_repair_repo where days=? and session_hash=?`,
      ).get(days,sessionHash) as {n:number}).n;
      this.db.prepare(
        `insert into dashboard_session_root_window
         (days,session_hash,started_at,ended_at,events,token_events,dominant_repo_hash,repo_count,
          branch_hash,dominant_account_hash,source,machine_hashes_json,input_tokens,output_tokens,
          cache_read_tokens,cache_creation_tokens,cost_nanos)
         values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
      ).run(days,sessionHash,job.started_at,job.ended_at,job.events,job.token_events,dominantRepo,repoCount,
        job.branch_hash,dominantAccount,job.source,JSON.stringify(machines),job.input_tokens,job.output_tokens,
        job.cache_read_tokens,job.cache_creation_tokens,job.cost_nanos);
      this.db.prepare(
        `insert into dashboard_session_source_window
         select days,session_hash,source,started_at,ended_at,events,token_events,input_tokens,
          output_tokens,cache_read_tokens,cache_creation_tokens,cost_nanos
         from dashboard_session_repair_source where days=? and session_hash=?`,
      ).run(days,sessionHash);
      this.db.prepare(
        `insert into dashboard_repo_session_window
         select days,repo_hash,repo_hash,session_hash,input_tokens,output_tokens,cost_nanos
         from dashboard_session_repair_repo where days=? and session_hash=?`,
      ).run(days,sessionHash);
      const fallback=dominantRepo??UNLINKED_REPO;
      this.db.prepare(
        `insert into dashboard_repo_session_window
         (days,repo_key,repo_hash,session_hash,input_tokens,output_tokens,cost_nanos)
         values (?,?,?,?,?,?,?) on conflict(days,repo_key,session_hash) do update set
          input_tokens=input_tokens+excluded.input_tokens,output_tokens=output_tokens+excluded.output_tokens,
          cost_nanos=cost_nanos+excluded.cost_nanos`,
      ).run(days,fallback,dominantRepo,sessionHash,job.unlinked_input_tokens,job.unlinked_output_tokens,job.unlinked_cost_nanos);
      this.db.prepare(
        `insert into dashboard_repo_branch_window
         select days,repo_key,repo_hash,branch_hash,session_hash,events
         from dashboard_session_repair_branch where days=? and session_hash=? and repo_hash is not null`,
      ).run(days,sessionHash);
      const unlinkedBranches=this.db.prepare(
        `select branch_hash as branchHash,events from dashboard_session_repair_branch
         where days=? and session_hash=? and repo_hash is null`,
      ).all(days,sessionHash) as Array<{branchHash:string;events:number}>;
      for(const branch of unlinkedBranches)this.db.prepare(
        `insert into dashboard_repo_branch_window
         (days,repo_key,repo_hash,branch_hash,session_hash,events) values (?,?,?,?,?,?)
         on conflict(days,repo_key,branch_hash,session_hash) do update set events=events+excluded.events`,
      ).run(days,fallback,dominantRepo,branch.branchHash,sessionHash,branch.events);
      this.db.prepare(
        `insert into dashboard_account_session_window
         (days,account_key,account_hash,session_hash,dominant_repo_hash,source,machine_hashes_json,
          cost_nanos,input_tokens,output_tokens) values (?,?,?,?,?,?,?,?,?,?)`,
      ).run(days,dominantAccount??UNLINKED_ACCOUNT,dominantAccount,sessionHash,dominantRepo,job.source,
        JSON.stringify(machines),job.cost_nanos,job.input_tokens,job.output_tokens);
    }
    this.clearSessionRepair(days,sessionHash);
    this.db.prepare(`delete from dashboard_dirty_sessions where days=? and session_hash=?`).run(days,sessionHash);
  }

  private drainAccountInvalidations(now: Date) {
    const invalidation = this.db.prepare(
      `select account_hash as accountHash, cursor_session_hash as cursorSessionHash
       from dashboard_account_invalidations order by queued_at limit 1`,
    ).get() as { accountHash: string; cursorSessionHash: string | null } | undefined;
    if (!invalidation) return 0;
    const sessions = this.db.prepare(
      `select distinct session_hash as sessionHash from dashboard_event_facts
       where account_hash = ? and session_hash is not null
         and (? is null or session_hash > ?)
       order by session_hash limit ?`,
    ).all(invalidation.accountHash, invalidation.cursorSessionHash, invalidation.cursorSessionHash, REPAIR_ROWS) as Array<{sessionHash:string}>;
    for (const row of sessions) {
      for (const days of INTERNAL_WINDOWS) this.markSessionDirty(days, row.sessionHash, "account_alias", now);
    }
    if (sessions.length < REPAIR_ROWS) {
      this.db.prepare(`delete from dashboard_account_invalidations where account_hash = ?`).run(invalidation.accountHash);
    } else {
      this.db.prepare(
        `update dashboard_account_invalidations set cursor_session_hash = ? where account_hash = ?`,
      ).run(sessions.at(-1)!.sessionHash, invalidation.accountHash);
    }
    return sessions.length;
  }

  queueAccountInvalidation(accountHash: string, now = new Date(Date.now())) {
    const hash = safeHash(accountHash);
    if (!hash) return;
    this.db.prepare(
      `insert into dashboard_account_invalidations (account_hash,cursor_session_hash,queued_at)
       values (?,null,?) on conflict(account_hash) do update set
        cursor_session_hash=null, queued_at=excluded.queued_at`,
    ).run(hash, now.toISOString());
    this.db.prepare(
      `update dashboard_projection_control set dirty=1 where singleton=1`,
    ).run();
  }

  invalidatePresentation() {
    this.db.prepare(
      `update dashboard_projection_control set settings_version=settings_version+1 where singleton=1`,
    ).run();
  }

  markSnapshotDirty() {
    this.db.prepare(
      `update dashboard_projection_control set dirty=1 where singleton=1`,
    ).run();
  }

  recordCaptureActivity(receipt: CaptureActivityReceipt) {
    const previous = this.db.prepare(
      `select last_activity_at as lastActivityAt,files_today as filesToday,
        discovery_entries as discoveryEntries,last_error_code as lastErrorCode,truncated
       from capture_activity_state where source=?`,
    ).get(receipt.source) as Record<string, unknown> | undefined;
    const errorCode = receipt.error ? sha256(`activity-error:${receipt.error}`).slice(0, 24) : null;
    this.db.prepare(
      `insert into capture_activity_state
       (source,last_activity_at,files_today,discovery_entries,last_scan_at,last_error_code,truncated)
       values (@source,@lastActivityAt,@filesToday,@discoveryEntries,@lastScanAt,@errorCode,@truncated)
       on conflict(source) do update set last_activity_at=excluded.last_activity_at,
        files_today=excluded.files_today, discovery_entries=excluded.discovery_entries,
        last_scan_at=excluded.last_scan_at, last_error_code=excluded.last_error_code,
        truncated=excluded.truncated`,
    ).run({ ...receipt, errorCode, truncated: receipt.truncated ? 1 : 0 });
    this.invalidatePresentation();
    const meaningfulChange = !previous ||
      previous.lastActivityAt !== receipt.lastActivityAt ||
      Number(previous.filesToday) !== receipt.filesToday ||
      Number(previous.discoveryEntries) !== receipt.discoveryEntries ||
      previous.lastErrorCode !== errorCode ||
      Boolean(previous.truncated) !== Boolean(receipt.truncated);
    if (meaningfulChange) this.markSnapshotDirty();
  }

  private drainCompactMutations(limit=REPAIR_ROWS){
    const rows=this.db.prepare(
      `select raw_rowid as rawRowid,observed_at as observedAt,source,
        event_type as eventType,action_class as actionClass
       from dashboard_compact_mutations order by queued_at,raw_rowid limit ?`,
    ).all(limit) as Array<Omit<CompactProjectionItem,"windowMask">>;
    if(!rows.length)return 0;
    const windows=this.compactWindowCutoffs();
    const items=rows.map((row):CompactProjectionItem=>({rawRowid:row.rawRowid,
      observedAt:row.observedAt,
      source:safeClassification(row.source,SAFE_SOURCES,"unknown"),
      eventType:safeClassification(row.eventType,SAFE_EVENT_TYPES,"unknown"),
      actionClass:safeClassification(row.actionClass,SAFE_ACTIONS,"other"),
      windowMask:this.compactMask(row.observedAt,windows)}));
    this.applyCompactAggregates(items,-1);
    const reference=this.control();
    for(const item of items)this.applyCompactReference(item,-1,reference);
    const cancel=this.db.prepare(
      `insert or ignore into dashboard_compact_cancellations
       (raw_rowid,bucket_day,observed_at,source,event_type,action_key) values (?,?,?,?,?,?)`,
    );
    const remove=this.db.prepare(`delete from dashboard_compact_mutations where raw_rowid=?`);
    for(const item of items){
      cancel.run(item.rawRowid,day(item.observedAt),item.observedAt,item.source,item.eventType,item.actionClass??"");
      remove.run(item.rawRowid);
    }
    this.db.prepare(`update dashboard_projection_control set dirty=1,
      projection_rows_visited=projection_rows_visited+?,projection_rows_written=projection_rows_written+?
      where singleton=1`).run(items.length,items.length);
    return items.length;
  }

  private runCompactGcSlice(now:Date){
    const started=performance.now();
    const empty={itemsVisited:0,itemsRemoved:0,segmentsRewritten:0,segmentsDeleted:0,
      daysCompleted:0,restarts:0,durationMs:0};
    const control=this.control();
    let job=this.db.prepare(
      `select bucket_day as bucketDay,revision,processing_revision as processingRevision,
        high_water_segment as highWater,cursor_segment as cursorSegment,last_schedule as lastSchedule
       from dashboard_compact_gc_days
       order by last_schedule,queued_at,bucket_day limit 1`,
    ).get() as {bucketDay:string;revision:number;processingRevision:number;
      highWater:number|null;cursorSegment:number;lastSchedule:number}|undefined;
    if(!job)return empty;
    const schedule=control.compactGcSchedule+1;
    this.db.prepare(`update dashboard_projection_control set compact_gc_schedule=? where singleton=1`).run(schedule);
    this.db.prepare(
      `update dashboard_compact_gc_days set last_schedule=?,updated_at=? where bucket_day=?`,
    ).run(schedule,now.toISOString(),job.bucketDay);
    if(job.processingRevision===0||job.highWater===null){
      const high=(this.db.prepare(
        `select coalesce(max(segment_id),0) as n from dashboard_compact_segments where bucket_day=?`,
      ).get(job.bucketDay) as {n:number}).n;
      this.db.prepare(`delete from dashboard_compact_gc_source_scratch where bucket_day=?`).run(job.bucketDay);
      this.db.prepare(
        `update dashboard_compact_gc_days set processing_revision=revision,
          high_water_segment=?,cursor_segment=0,updated_at=? where bucket_day=?`,
      ).run(high,now.toISOString(),job.bucketDay);
      job=this.db.prepare(
        `select bucket_day as bucketDay,revision,processing_revision as processingRevision,
          high_water_segment as highWater,cursor_segment as cursorSegment,last_schedule as lastSchedule
         from dashboard_compact_gc_days where bucket_day=?`,
      ).get(job.bucketDay) as typeof job;
    }
    if(!job)return empty;
    let itemsVisited=0,itemsRemoved=0,segmentsRewritten=0,segmentsDeleted=0;
    const segment=this.db.prepare(
      `select segment_id as segmentId,payload_gzip as payload
       from dashboard_compact_segments where bucket_day=? and segment_id>? and segment_id<=?
       order by segment_id limit 1`,
    ).get(job.bucketDay,job.cursorSegment,job.highWater??0) as {segmentId:number;payload:Buffer}|undefined;
    let cursor=job.cursorSegment;
    if(segment){
      const items=decodeCompact(segment.payload);
      if(items.length>COMPACT_GC_ITEMS)throw new Error("compact_gc_segment_exceeds_item_bound");
      const cancelled=this.db.prepare(
        `select 1 from dashboard_compact_cancellations where raw_rowid=? and observed_at=?
          and source=? and event_type=? and action_key=?`,
      );
      const kept:CompactProjectionItem[]=[];
      const removed:CompactProjectionItem[]=[];
      for(const item of items)(cancelled.get(item.rawRowid,item.observedAt,item.source,
        item.eventType,item.actionClass??"")?removed:kept).push(item);
      if(removed.length&&kept.length){
        const observed=kept.map((item)=>item.observedAt).sort();
        this.db.prepare(
          `update dashboard_compact_segments set min_observed_at=?,max_observed_at=?,
            event_count=?,payload_gzip=? where segment_id=?`,
        ).run(observed[0],observed.at(-1),kept.length,encodeCompact(kept),segment.segmentId);
        segmentsRewritten=1;
      }else if(removed.length){
        this.db.prepare(`delete from dashboard_compact_segments where segment_id=?`).run(segment.segmentId);
        segmentsDeleted=1;
      }
      if(this.failNextCompactGcAfterRewrite&&removed.length){
        this.failNextCompactGcAfterRewrite=false;
        throw new Error("injected_compact_gc_failure");
      }
      const scratch=new Map<string,{source:string;count:number;min:string;max:string}>();
      for(const item of kept){
        const value=scratch.get(item.source);
        if(value){
          value.count++;
          if(item.observedAt<value.min)value.min=item.observedAt;
          if(item.observedAt>value.max)value.max=item.observedAt;
        }else scratch.set(item.source,{source:item.source,count:1,min:item.observedAt,max:item.observedAt});
      }
      const upsertScratch=this.db.prepare(
        `insert into dashboard_compact_gc_source_scratch
         (bucket_day,processing_revision,source,event_count,min_observed_at,max_observed_at)
         values (@bucketDay,@processingRevision,@source,@count,@min,@max)
         on conflict(bucket_day,processing_revision,source) do update set
          event_count=event_count+excluded.event_count,
          min_observed_at=min(min_observed_at,excluded.min_observed_at),
          max_observed_at=max(max_observed_at,excluded.max_observed_at)`,
      );
      for(const value of scratch.values())upsertScratch.run({bucketDay:job.bucketDay,
        processingRevision:job.processingRevision,...value});
      const settleCancellation=this.db.prepare(
        `delete from dashboard_compact_cancellations where raw_rowid=? and observed_at=?
          and source=? and event_type=? and action_key=?`,
      );
      for(const item of removed)settleCancellation.run(item.rawRowid,item.observedAt,item.source,
        item.eventType,item.actionClass??"");
      cursor=segment.segmentId;
      this.db.prepare(
        `update dashboard_compact_gc_days set cursor_segment=?,updated_at=? where bucket_day=?`,
      ).run(cursor,now.toISOString(),job.bucketDay);
      itemsVisited=items.length;
      itemsRemoved=removed.length;
    }
    const next=this.db.prepare(
      `select 1 from dashboard_compact_segments where bucket_day=? and segment_id>? and segment_id<=? limit 1`,
    ).get(job.bucketDay,cursor,job.highWater??0);
    let daysCompleted=0,restarts=0;
    if(!next){
      const latest=this.db.prepare(
        `select revision,processing_revision as processingRevision from dashboard_compact_gc_days
         where bucket_day=?`,
      ).get(job.bucketDay) as {revision:number;processingRevision:number};
      if(latest.revision!==latest.processingRevision){
        this.db.prepare(
          `update dashboard_compact_gc_days set processing_revision=0,high_water_segment=null,
            cursor_segment=0,updated_at=? where bucket_day=?`,
        ).run(now.toISOString(),job.bucketDay);
        restarts=1;
      }else{
        const affectedSources=new Set<string>();
        for(const row of this.db.prepare(
          `select source from dashboard_compact_day_source where bucket_day=?
           union select source from dashboard_compact_gc_source_scratch
            where bucket_day=? and processing_revision=?`,
        ).all(job.bucketDay,job.bucketDay,job.processingRevision) as Array<{source:string}>)affectedSources.add(row.source);
        this.db.prepare(`delete from dashboard_compact_day_source where bucket_day=?`).run(job.bucketDay);
        this.db.prepare(
          `insert into dashboard_compact_day_source
           (bucket_day,source,event_count,min_observed_at,max_observed_at)
           select bucket_day,source,event_count,min_observed_at,max_observed_at
           from dashboard_compact_gc_source_scratch
           where bucket_day=? and processing_revision=? and event_count>0`,
        ).run(job.bucketDay,job.processingRevision);
        // A complete fixed-high-water scan proves unmatched receipts absent.
        this.db.prepare(`delete from dashboard_compact_cancellations where bucket_day=?`).run(job.bucketDay);
        this.db.prepare(`delete from dashboard_compact_gc_source_scratch where bucket_day=?`).run(job.bucketDay);
        this.db.prepare(`delete from dashboard_compact_gc_days where bucket_day=?`).run(job.bucketDay);
        this.refreshLifetimeBounds();
        for(const source of affectedSources)this.refreshSourceLatest(source);
        daysCompleted=1;
      }
    }
    const durationMs=performance.now()-started;
    this.db.prepare(
      `update dashboard_projection_control set
        compact_gc_items_visited=compact_gc_items_visited+?,
        compact_gc_items_removed=compact_gc_items_removed+?,
        compact_gc_segments_rewritten=compact_gc_segments_rewritten+?,
        compact_gc_segments_deleted=compact_gc_segments_deleted+?,
        compact_gc_days_completed=compact_gc_days_completed+?,
        compact_gc_restarts=compact_gc_restarts+? where singleton=1`,
    ).run(itemsVisited,itemsRemoved,segmentsRewritten,segmentsDeleted,daysCompleted,restarts);
    return {itemsVisited,itemsRemoved,segmentsRewritten,segmentsDeleted,daysCompleted,restarts,
      durationMs:Number(durationMs.toFixed(3))};
  }

  private compactBoundary(direction:"oldest"|"newest"){
    const column=direction==="oldest"?"min_observed_at":"max_observed_at";
    const order=direction==="oldest"?"asc":"desc";
    const row=this.db.prepare(
      `select ${column} as boundary from dashboard_compact_day_source
       where event_count>0 order by ${column} ${order},bucket_day ${order},source ${order} limit 1`,
    ).get() as {boundary:string}|undefined;
    return row?.boundary??null;
  }

  private compactWindowBoundary(days:number,cutoff:string,direction:"oldest"|"newest"){
    const bit=INTERNAL_WINDOWS.indexOf(days as typeof INTERNAL_WINDOWS[number]);
    if(bit<0)return null;
    if(direction==="newest"){
      const row=this.db.prepare(
        `select max_observed_at as boundary from dashboard_compact_day_source
         where event_count>0 and max_observed_at>=?
         order by max_observed_at desc,bucket_day desc,source limit 1`,
      ).get(cutoff) as {boundary:string}|undefined;
      return row?.boundary??null;
    }
    const candidate=this.db.prepare(
      `select bucket_day as bucketDay,min(min_observed_at) as boundary
       from dashboard_compact_day_source where event_count>0 and max_observed_at>=?
       group by bucket_day order by bucket_day limit 1`,
    ).get(cutoff) as {bucketDay:string;boundary:string}|undefined;
    if(!candidate)return null;
    if(candidate.bucketDay>day(cutoff))return candidate.boundary;
    // Only the straddling cutoff day needs item-level precision. All later
    // days are answered by the compact day/source summary indexes above.
    const cancelled=this.db.prepare(
      `select 1 from dashboard_compact_cancellations where raw_rowid=? and observed_at=?
        and source=? and event_type=? and action_key=?`,
    );
    let best:string|null=null;
    const segments=this.db.prepare(
      `select min_observed_at as boundary,payload_gzip as payload from dashboard_compact_segments
       where bucket_day=? and max_observed_at>=? order by min_observed_at,segment_id`,
    ).iterate(candidate.bucketDay,cutoff) as Iterable<{boundary:string;payload:Buffer}>;
    for(const segment of segments){
      if(best&&segment.boundary>=best)break;
      for(const item of decodeCompact(segment.payload)){
        if(!(item.windowMask&(1<<bit))||item.observedAt<cutoff||cancelled.get(item.rawRowid,item.observedAt,item.source,item.eventType,item.actionClass??""))continue;
        if(best===null||item.observedAt<best)best=item.observedAt;
      }
    }
    return best;
  }

  private touchSourceLatest(source:string,observedAt:string,token:boolean){
    this.db.prepare(
      `insert into dashboard_source_lifetime (source,last_event_at,last_token_event_at)
       values (?,?,?) on conflict(source) do update set
        last_event_at=case when last_event_at is null or excluded.last_event_at>last_event_at then excluded.last_event_at else last_event_at end,
        last_token_event_at=case when excluded.last_token_event_at is not null and
          (last_token_event_at is null or excluded.last_token_event_at>last_token_event_at)
          then excluded.last_token_event_at else last_token_event_at end`,
    ).run(source,observedAt,token?observedAt:null);
  }

  private compactSourceLatest(source:string){
    const row=this.db.prepare(
      `select max_observed_at as boundary from dashboard_compact_day_source
       where source=? and event_count>0 order by max_observed_at desc,bucket_day desc limit 1`,
    ).get(source) as {boundary:string}|undefined;
    return row?.boundary??null;
  }

  private refreshSourceLatest(source:string){
    const fact=this.db.prepare(
      `select
        (select observed_at from dashboard_event_facts where source=? order by observed_at desc,projection_id desc limit 1) as lastEventAt,
        (select observed_at from dashboard_event_facts where source=? and input_tokens is not null
         order by observed_at desc,projection_id desc limit 1) as lastTokenAt`,
    ).get(source,source) as {lastEventAt:string|null;lastTokenAt:string|null};
    const latest=[fact.lastEventAt,this.compactSourceLatest(source)].filter((v):v is string=>Boolean(v)).sort().at(-1)??null;
    if(latest||fact.lastTokenAt)this.db.prepare(
      `insert into dashboard_source_lifetime (source,last_event_at,last_token_event_at) values (?,?,?)
       on conflict(source) do update set last_event_at=excluded.last_event_at,
        last_token_event_at=excluded.last_token_event_at`,
    ).run(source,latest,fact.lastTokenAt);
    else this.db.prepare(`delete from dashboard_source_lifetime where source=?`).run(source);
  }

  private refreshLifetimeBounds(){
    const facts=this.db.prepare(
      `select
        (select observed_at from dashboard_event_facts order by observed_at,projection_id limit 1) as oldest,
        (select observed_at from dashboard_event_facts order by observed_at desc,projection_id desc limit 1) as newest`,
    ).get() as {oldest:string|null;newest:string|null};
    const compactOldest=this.compactBoundary("oldest"),compactNewest=this.compactBoundary("newest");
    const oldest=[facts.oldest,compactOldest].filter((v):v is string=>Boolean(v)).sort()[0]??null;
    const newest=[facts.newest,compactNewest].filter((v):v is string=>Boolean(v)).sort().at(-1)??null;
    this.db.prepare(`update dashboard_lifetime_totals set oldest_observed_at=?,newest_observed_at=? where singleton=1`).run(oldest,newest);
  }

  runMaintenance(now = new Date(Date.now())): ProjectionMaintenanceReceipt {
    let backfillRowsVisited = 0;
    let parityRowsVisited = 0;
    let repairRowsVisited = 0;
    let dirtySessionsVisited = 0;
    let sessionRepairRowsVisited = 0;
    let metricRowsVisited = 0;
    let expiryFacts = 0;
    let compactGc={itemsVisited:0,itemsRemoved:0,segmentsRewritten:0,segmentsDeleted:0,
      daysCompleted:0,restarts:0,durationMs:0};
    const countersBefore = this.control();
    const buildsBefore = countersBefore.snapshotBuilds;
    this.db.transaction(() => {
      const current = this.control();
      if (current.backfillHighWater === null) {
        const high = this.db.prepare(
          `select coalesce(max(rowid),0) as highWater from buffered_events`,
        ).get() as { highWater: number };
        this.db.prepare(
          `update dashboard_projection_control set backfill_high_water = ?,
            backfill_complete = case when ? = 0 then 1 else 0 end where singleton = 1`,
        ).run(high.highWater, high.highWater);
      }
      if (current.metricBackfillHighWater === null) {
        const high=this.db.prepare(`select coalesce(max(rowid),0) as highWater from metric_samples`).get() as {highWater:number};
        this.db.prepare(
          `update dashboard_projection_control set metric_backfill_high_water=?,
            metric_backfill_complete=case when ?=0 then 1 else 0 end,
            metric_sample_count=case when ?=0 then 0 else metric_sample_count end where singleton=1`,
        ).run(high.highWater,high.highWater,high.highWater);
      }
      const control = this.control();
      if (!control.backfillComplete) {
        const rows = this.db.prepare(
          `select rowid as rawRowid, id, source, event_type as eventType,
            observed_at as observedAt, session_id as sessionId, action_class as actionClass,
            model, input_tokens as inputTokens, output_tokens as outputTokens,
            cache_read_tokens as cacheReadTokens, cache_creation_tokens as cacheCreationTokens,
            cost_usd as costUsd, repo_hash as repoHash, branch_hash as branchHash,
            head_sha as headSha, machine, account_hash as accountHash,
            suppressed_fields_json as suppressedFieldsJson
           from buffered_events where rowid > ? and rowid <= ? order by rowid limit ?`,
        ).all(control.backfillCursor, control.backfillHighWater ?? 0, BACKFILL_ROWS) as RawProjectionRow[];
        this.applyProjectionRows(rows,now);
        for (const row of rows) {
          this.db.prepare(`delete from dashboard_projection_repairs where raw_rowid = ?`).run(row.rawRowid);
        }
        backfillRowsVisited = rows.length;
        const exhausted=rows.length<BACKFILL_ROWS;
        const cursor = exhausted?(control.backfillHighWater??0):(rows.at(-1)?.rawRowid ?? control.backfillCursor);
        this.db.prepare(
          `update dashboard_projection_control set backfill_cursor=?,
            backfill_complete=case when ? >= coalesce(backfill_high_water,0) then 1 else 0 end,
            backfill_facts=backfill_facts+? where singleton=1`,
        ).run(cursor, cursor, rows.length);
      }

      const metricControl=this.control();
      if(!metricControl.metricBackfillComplete){
        const rows=this.db.prepare(
          `select rowid as rawRowid from metric_samples where rowid>? and rowid<=? order by rowid limit ?`,
        ).all(metricControl.metricBackfillCursor,metricControl.metricBackfillHighWater??0,BACKFILL_ROWS) as Array<{rawRowid:number}>;
        metricRowsVisited=rows.length;
        const exhausted=rows.length<BACKFILL_ROWS;
        const cursor=exhausted?(metricControl.metricBackfillHighWater??0):(rows.at(-1)?.rawRowid??metricControl.metricBackfillCursor);
        this.db.prepare(
          `update dashboard_projection_control set metric_backfill_cursor=?,
            metric_backfill_complete=?,metric_sample_count=coalesce(metric_sample_count,0)+?,dirty=1 where singleton=1`,
        ).run(cursor,exhausted?1:0,rows.length);
      }

      this.drainCompactMutations();
      const repairs = this.db.prepare(
        `select r.raw_rowid as repairRawRowid,r.reason,
          b.rowid as rawRowid,b.id,b.source,b.event_type as eventType,b.observed_at as observedAt,
          b.session_id as sessionId,b.action_class as actionClass,b.model,
          b.input_tokens as inputTokens,b.output_tokens as outputTokens,
          b.cache_read_tokens as cacheReadTokens,b.cache_creation_tokens as cacheCreationTokens,
          b.cost_usd as costUsd,b.repo_hash as repoHash,b.branch_hash as branchHash,
          b.head_sha as headSha,b.machine,b.account_hash as accountHash,
          b.suppressed_fields_json as suppressedFieldsJson
         from dashboard_projection_repairs r left join buffered_events b on b.rowid=r.raw_rowid
         order by r.raw_rowid limit ?`,
      ).all(REPAIR_ROWS) as Array<RawProjectionRow&{repairRawRowid:number;reason:string;id:string|null}>;
      const rowsToApply:RawProjectionRow[]=[];
      for (const repair of repairs) {
        if(repair.id!==null){
          const row=repair as RawProjectionRow;
          const compactNoOpUpdate=compactable(row)&&repair.reason==="raw_update"&&
            !this.storedFact(sha256(`event:${row.id}`))&&
            !this.db.prepare(`select 1 from dashboard_compact_cancellations where raw_rowid=?`).get(repair.repairRawRowid);
          if(!compactNoOpUpdate)rowsToApply.push(row);
        }else{
          const stored=this.storedFactByRawRowid(repair.repairRawRowid);
          if(stored)this.removeStoredFact(stored,now);
        }
      }
      this.applyProjectionRows(rowsToApply,now);
      const removeRepair=this.db.prepare(`delete from dashboard_projection_repairs where raw_rowid = ?`);
      for(const repair of repairs)removeRepair.run(repair.repairRawRowid);
      repairRowsVisited = repairs.length;
      this.db.prepare(
        `update dashboard_projection_control set repair_facts=repair_facts+? where singleton=1`,
      ).run(repairs.length);
      const preGc=this.control();
      // Finish mutation/repair admission first. This freezes the useful GC
      // revision once per burst instead of repeatedly rescanning a hot day
      // while thousands of cancellation receipts are still arriving.
      if(preGc.compactMutationBacklog===0&&preGc.repairBacklog===0){
        compactGc=this.runCompactGcSlice(now);
      }
      parityRowsVisited = backfillRowsVisited === 0 ? this.runParitySlice() : 0;
      this.drainAccountInvalidations(now);
      expiryFacts = this.advanceExpiry(now);

      const repairStarted=performance.now();
      while(sessionRepairRowsVisited<SESSION_REPAIR_ROWS&&
        (performance.now()-repairStarted<SESSION_REPAIR_BUDGET_MS||sessionRepairRowsVisited===0)){
        const row=this.db.prepare(
          `select days,session_hash as sessionHash from dashboard_dirty_sessions
           order by queued_at,days,session_hash limit 1`,
        ).get() as {days:number;sessionHash:string}|undefined;
        if(!row)break;
        const result=this.repairSessionChunk(row.days,row.sessionHash,
          SESSION_REPAIR_ROWS-sessionRepairRowsVisited,now);
        sessionRepairRowsVisited+=result.rowsVisited;
        if(result.finalized)dirtySessionsVisited++;
        if(!result.finalized||result.rowsVisited===0)break;
      }

      const backlog = this.backlog();
      const settled = this.control();
      const complete = Boolean(settled.backfillComplete && settled.parityComplete&&settled.metricBackfillComplete);
      if (complete && backlog.repairs === 0 && backlog.compactMutations===0 && backlog.compactGcDays===0 && backlog.dirtySessions === 0 &&
        backlog.accountInvalidations === 0 && backlog.expiryWindows === 0 &&
        settled.degradedReason !== "projection_clock_rollback") {
        if (settled.dirty || !settled.ready) this.publishSnapshots(now);
      } else {
        this.db.prepare(
          `update dashboard_projection_control set ready=case when generation>0 then 1 else 0 end,
            parity_ready=0,
            degraded_reason=case
              when degraded_reason='projection_clock_rollback' then degraded_reason
              when backfill_complete=0 or metric_backfill_complete=0 then 'projection_backfilling'
              else 'projection_repair_backlog' end where singleton=1`,
        ).run();
      }
    })();
    const control = this.control();
    return {
      backfillRowsVisited,
      parityRowsVisited,
      repairRowsVisited,
      dirtySessionsVisited,
      sessionRepairRowsVisited,
      metricRowsVisited,
      expiryFacts,
      compactSegmentsWritten:control.compactSegmentsWritten-countersBefore.compactSegmentsWritten,
      compactGcItemsVisited:compactGc.itemsVisited,
      compactGcItemsRemoved:compactGc.itemsRemoved,
      compactGcSegmentsRewritten:compactGc.segmentsRewritten,
      compactGcSegmentsDeleted:compactGc.segmentsDeleted,
      compactGcDaysCompleted:compactGc.daysCompleted,
      compactGcRestarts:compactGc.restarts,
      compactGcDurationMs:compactGc.durationMs,
      snapshotBuilds: control.snapshotBuilds - buildsBefore,
      ready: Boolean(control.ready),
      degraded: Boolean(control.degradedReason),
      backlog: this.backlog(),
    };
  }

  private runParitySlice() {
    const control = this.control();
    if (!control.backfillComplete || control.parityComplete) return 0;
    const rows = this.db.prepare(
      `select rowid as rawRowid,id,source,event_type as eventType,observed_at as observedAt,
        session_id as sessionId,action_class as actionClass,model,input_tokens as inputTokens,
        output_tokens as outputTokens,cache_read_tokens as cacheReadTokens,
        cache_creation_tokens as cacheCreationTokens,cost_usd as costUsd,
        repo_hash as repoHash,branch_hash as branchHash,head_sha as headSha,machine,
        account_hash as accountHash,suppressed_fields_json as suppressedFieldsJson
       from buffered_events where rowid>? and rowid<=? order by rowid limit ?`,
    ).all(control.parityCursor, control.backfillHighWater ?? 0, BACKFILL_ROWS) as RawProjectionRow[];
    const windows = this.db.prepare(
      `select days,cutoff_at as cutoffAt from dashboard_window_control
       where days in (30,90,182,365,1825)`,
    ).all() as Array<{days:number;cutoffAt:string}>;
    this.applyReferenceBatch("dashboard_parity_window",windows,rows.map(factFromRaw));
    const exhausted=rows.length<BACKFILL_ROWS;
    const cursor = exhausted?(control.backfillHighWater??0):(rows.at(-1)?.rawRowid ?? control.parityCursor);
    let complete = cursor >= (control.backfillHighWater ?? 0);
    if (complete) {
      const mismatches = this.db.prepare(
        `select t.days from dashboard_window_totals t
         join dashboard_parity_window p on p.days=t.days
         join dashboard_post_highwater_window n on n.days=t.days
         where t.days in (30,90,182,365,1825) and (
           t.events != p.events+n.events or
           t.token_events != p.token_events+n.token_events or
           t.input_tokens != p.input_tokens+n.input_tokens or
           t.output_tokens != p.output_tokens+n.output_tokens or
           t.cache_read_tokens != p.cache_read_tokens+n.cache_read_tokens or
           t.cache_creation_tokens != p.cache_creation_tokens+n.cache_creation_tokens or
           t.cost_nanos != p.cost_nanos+n.cost_nanos
         )`,
      ).all() as Array<{days:number}>;
      if (mismatches.length) {
        complete = false;
        this.db.prepare(
          `update dashboard_projection_control set degraded_reason='projection_parity_mismatch',
            last_error_at=strftime('%Y-%m-%dT%H:%M:%fZ','now') where singleton=1`,
        ).run();
      }
    }
    this.db.prepare(
      `update dashboard_projection_control set parity_cursor=?,parity_complete=? where singleton=1`,
    ).run(cursor, complete ? 1 : 0);
    return rows.length;
  }

  private advanceExpiry(now: Date) {
    const parity = this.control();
    if (parity.backfillComplete && !parity.parityComplete && parity.parityCursor > 0) return 0;
    let visited = 0;
    let rollback = false;
    for (const days of INTERNAL_WINDOWS) {
      if (visited >= BACKFILL_ROWS) break;
      const row = this.db.prepare(
        `select cutoff_at as cutoffAt,target_cutoff_at as targetCutoffAt,
          expiry_cursor_at as cursorAt,expiry_cursor_id as cursorId,
          compact_expiry_high_water as compactHighWater,
          compact_expiry_cursor_segment as compactCursorSegment,
          compact_expiry_cursor_offset as compactCursorOffset
         from dashboard_window_control where days=?`,
      ).get(days) as { cutoffAt: string;targetCutoffAt:string|null;cursorAt:string|null;cursorId:string|null;
        compactHighWater:number|null;compactCursorSegment:number|null;compactCursorOffset:number|null };
      const target = sinceIso(days, now);
      const delta = Date.parse(target) - Date.parse(row.cutoffAt);
      if (delta < 0) {
        rollback = true;
        this.db.prepare(
          `update dashboard_projection_control set parity_ready=0, dirty=1,
            degraded_reason='projection_clock_rollback', last_error_at=? where singleton=1`,
        ).run(now.toISOString());
        continue;
      }
      if (delta === 0) continue;
      const activeTarget = row.targetCutoffAt ?? target;
      const cursorAt = row.cursorAt ?? row.cutoffAt;
      const cursorId = row.cursorId ?? "";
      let compactHighWater=row.compactHighWater;
      if (!row.targetCutoffAt) {
        compactHighWater=(this.db.prepare(`select coalesce(max(segment_id),0) as n from dashboard_compact_segments`).get() as {n:number}).n;
        this.db.prepare(
          `update dashboard_window_control set target_cutoff_at=?,expiry_cursor_at=?,expiry_cursor_id='',
            compact_expiry_high_water=?,compact_expiry_cursor_segment=0,compact_expiry_cursor_offset=0
           where days=?`,
        ).run(target, row.cutoffAt, compactHighWater,days);
      }
      const factLimit=Math.min(REPAIR_ROWS,BACKFILL_ROWS-visited);
      const expired = this.db.prepare(
        `select projection_id as projectionId, raw_rowid as rawRowid, source,
          event_type as eventType, observed_at as observedAt, session_hash as sessionHash,
          action_class as actionClass, model, input_tokens as inputTokens,
          output_tokens as outputTokens, cache_read_tokens as cacheReadTokens,
          cache_creation_tokens as cacheCreationTokens, cost_nanos as costNanos,
          repo_hash as repoHash, branch_hash as branchHash, head_hash as headHash,
          machine_hash as machineHash, account_hash as accountHash, suppressed
         from dashboard_event_facts where observed_at >= ? and observed_at < ?
          and (observed_at > ? or (observed_at = ? and projection_id > ?))
         order by observed_at,projection_id limit ?`,
      ).all(row.cutoffAt, activeTarget, cursorAt, cursorAt, cursorId,
        factLimit) as Array<Record<string, unknown>>;
      for (const raw of expired) {
        const fact = factFromDb(raw);
        this.applyFlatDelta(days, fact, -1);
        const reference=this.control();
        const table=reference.backfillHighWater!==null?(fact.rawRowid>reference.backfillHighWater
          ?"dashboard_post_highwater_window":reference.parityCursor>=fact.rawRowid?"dashboard_parity_window":null):null;
        if(table&&DASHBOARD_WINDOWS.includes(days as typeof DASHBOARD_WINDOWS[number]))this.applyReferenceDelta(table,days,fact,-1);
        if (fact.sessionHash) this.markSessionDirty(days, fact.sessionHash, "expiry", now);
      }
      visited += expired.length;
      const factsDone=expired.length<factLimit;
      if(!factsDone){
        const last = factFromDb(expired.at(-1)!);
        this.db.prepare(
          `update dashboard_window_control set expiry_cursor_at=?,expiry_cursor_id=? where days=?`,
        ).run(last.observedAt, last.projectionId, days);
        continue;
      }
      const compact=this.expireCompactSlice(days,row.cutoffAt,activeTarget,compactHighWater??0,
        row.targetCutoffAt?(row.compactCursorSegment??0):0,
        row.targetCutoffAt?(row.compactCursorOffset??0):0,BACKFILL_ROWS-visited);
      visited+=compact.rowsVisited;
      if(compact.done){
        this.db.prepare(
          `update dashboard_window_control set cutoff_at=?,target_cutoff_at=null,
            expiry_cursor_at=null,expiry_cursor_id=null,compact_expiry_high_water=null,
            compact_expiry_cursor_segment=null,compact_expiry_cursor_offset=null where days=?`,
        ).run(activeTarget,days);
      }else{
        this.db.prepare(
          `update dashboard_window_control set compact_expiry_cursor_segment=?,
            compact_expiry_cursor_offset=? where days=?`,
        ).run(compact.cursorSegment,compact.cursorOffset,days);
      }
    }
    this.db.prepare(
      `update dashboard_projection_control set expiry_facts=expiry_facts+?,
        dirty=case when ? then 1 else dirty end where singleton=1`,
    ).run(visited, visited>0 ? 1 : 0);
    if (!rollback) {
      this.db.prepare(
        `update dashboard_projection_control set degraded_reason=null
         where singleton=1 and degraded_reason='projection_clock_rollback'`,
      ).run();
    }
    return visited;
  }

  private expireCompactSlice(days:number,cutoff:string,target:string,highWater:number,
    cursorSegment:number,cursorOffset:number,limit:number){
    if(limit<=0)return {rowsVisited:0,done:false,cursorSegment,cursorOffset};
    const segment=cursorOffset>0?this.db.prepare(
      `select segment_id as segmentId,payload_gzip as payload from dashboard_compact_segments
       where segment_id=? and segment_id<=?`,
    ).get(cursorSegment,highWater) as {segmentId:number;payload:Buffer}|undefined:this.db.prepare(
      `select segment_id as segmentId,payload_gzip as payload from dashboard_compact_segments
       where segment_id>? and segment_id<=? and min_observed_at<? and max_observed_at>=?
       order by segment_id limit 1`,
    ).get(cursorSegment,highWater,target,cutoff) as {segmentId:number;payload:Buffer}|undefined;
    if(!segment)return {rowsVisited:0,done:true,cursorSegment,cursorOffset:0};
    const all=decodeCompact(segment.payload),start=cursorOffset>0?cursorOffset:0;
    const slice=all.slice(start,start+limit);
    const bit=INTERNAL_WINDOWS.indexOf(days as typeof INTERNAL_WINDOWS[number]);
    const cancelled=this.db.prepare(
      `select 1 from dashboard_compact_cancellations where raw_rowid=? and observed_at=?
        and source=? and event_type=? and action_key=?`,
    );
    const eligible=slice.filter((item)=>bit>=0&&(item.windowMask&(1<<bit))&&
      item.observedAt>=cutoff&&item.observedAt<target&&!cancelled.get(item.rawRowid,item.observedAt,
        item.source,item.eventType,item.actionClass??"")).map((item)=>({...item,windowMask:1<<bit}));
    if(eligible.length){this.applyCompactAggregates(eligible,-1,false);const reference=this.control();for(const item of eligible)this.applyCompactReference(item,-1,reference);}
    const nextOffset=start+slice.length;
    if(nextOffset<all.length)return {rowsVisited:slice.length,done:false,
      cursorSegment:segment.segmentId,cursorOffset:nextOffset};
    const next=this.db.prepare(
      `select 1 from dashboard_compact_segments where segment_id>? and segment_id<=?
        and min_observed_at<? and max_observed_at>=? limit 1`,
    ).get(segment.segmentId,highWater,target,cutoff);
    return {rowsVisited:slice.length,done:!next,cursorSegment:segment.segmentId,cursorOffset:0};
  }

  private backlog() {
    const control=this.control();
    return {
      repairs: control.repairBacklog,
      compactMutations:control.compactMutationBacklog,
      compactGcDays:control.compactGcBacklog,
      dirtySessions:control.dirtySessionBacklog,
      accountInvalidations:control.accountInvalidationBacklog,
      expiryWindows: (this.db.prepare(`select count(*) as n from dashboard_window_control where target_cutoff_at is not null`).get() as {n:number}).n,
    };
  }

  private publishSnapshots(now: Date) {
    const control = this.control();
    if (!control.backfillComplete||!control.metricBackfillComplete) return false;
    const backlog = this.backlog();
    if (backlog.repairs || backlog.compactMutations||backlog.compactGcDays||backlog.dirtySessions || backlog.accountInvalidations) return false;
    const generation = control.generation + 1;
    let rowsVisited = 0;
    for (const days of DASHBOARD_WINDOWS) {
      const built = this.buildSnapshot(days, generation, now);
      rowsVisited += built.rowsVisited;
      this.db.prepare(
        `insert into dashboard_snapshots
         (days,schema_version,generation,since_at,payload_json,created_at)
         values (?,?,?,?,?,?) on conflict(days) do update set
          schema_version=excluded.schema_version,generation=excluded.generation,
          since_at=excluded.since_at,payload_json=excluded.payload_json,created_at=excluded.created_at`,
      ).run(days, DASHBOARD_SCHEMA_VERSION, generation, built.snapshot.window.since,
        JSON.stringify(built.snapshot), now.toISOString());
    }
    this.db.prepare(
      `update dashboard_projection_control set generation=?,dirty=0,ready=1,
        parity_ready=1,degraded_reason=null,last_success_at=?,
        snapshot_rows_visited=snapshot_rows_visited+?,snapshot_builds=snapshot_builds+1
       where singleton=1`,
    ).run(generation, now.toISOString(), rowsVisited);
    return true;
  }

  private buildSnapshot(days: number, generation: number, now: Date) {
    let rowsVisited = 0;
    const cutoff = (this.db.prepare(
      `select cutoff_at as cutoffAt from dashboard_window_control where days=?`,
    ).get(days) as { cutoffAt: string }).cutoffAt;
    const total = this.db.prepare(
      `select events,token_events as tokenEvents,input_tokens as inputTokens,
        output_tokens as outputTokens,cache_read_tokens as cacheReadTokens,
        cache_creation_tokens as cacheCreationTokens,cost_nanos as costNanos
       from dashboard_window_totals where days=?`,
    ).get(days) as Record<string, number>;
    const factSpan = this.db.prepare(
      `select
        (select observed_at from dashboard_event_facts where observed_at>=? order by observed_at,projection_id limit 1) as oldest,
        (select observed_at from dashboard_event_facts where observed_at>=? order by observed_at desc,projection_id desc limit 1) as newest`,
    ).get(cutoff, cutoff) as { oldest: string | null; newest: string | null };
    const compactOldest=this.compactWindowBoundary(days,cutoff,"oldest");
    const compactNewest=this.compactWindowBoundary(days,cutoff,"newest");
    const span={
      oldest:[factSpan.oldest,compactOldest].filter((v):v is string=>Boolean(v)).sort()[0]??null,
      newest:[factSpan.newest,compactNewest].filter((v):v is string=>Boolean(v)).sort().at(-1)??null,
    };
    const sessionCounts = this.db.prepare(
      `select count(*) as sessions,
        sum(case when token_events>0 then 1 else 0 end) as sessionsWithTokens
       from dashboard_session_root_window where days=?`,
    ).get(days) as { sessions: number; sessionsWithTokens: number | null };
    const sourceSessionCounts = new Map((this.db.prepare(
      `select source,count(*) as sessions,
        sum(case when token_events>0 then 1 else 0 end) as sessionsWithTokens
       from dashboard_session_source_window where days=? group by source`,
    ).all(days) as Array<{source:string;sessions:number;sessionsWithTokens:number}>).map((row)=>[row.source,row]));
    const bySourceRows = this.db.prepare(
      `select source,events,input_tokens as inputTokens,output_tokens as outputTokens,
        cost_nanos as costNanos from dashboard_source_window
       where days=? and events>0 order by cost_nanos desc`,
    ).all(days) as Array<Record<string, unknown>>;
    const bySource = bySourceRows.map((row) => ({
      source: row.source,
      events: Number(row.events),
      sessions: sourceSessionCounts.get(String(row.source))?.sessions ?? 0,
      sessionsWithTokens: sourceSessionCounts.get(String(row.source))?.sessionsWithTokens ?? 0,
      inputTokens: Number(row.inputTokens), outputTokens: Number(row.outputTokens), costUsd: usd(row.costNanos),
    }));
    const dailyRows = this.db.prepare(
      `select day,cost_nanos as costNanos,tokens from dashboard_daily_window
       where days=? and (cost_nanos!=0 or tokens!=0) order by day`,
    ).all(days) as Array<Record<string, unknown>>;
    const daily = dailyRows.map((row)=>({day:row.day,costUsd:usd(row.costNanos),tokens:Number(row.tokens)}));
    const modelRows = this.db.prepare(
      `select model,calls,unpriced_calls as unpricedCalls,input_tokens as inputTokens,
        output_tokens as outputTokens,cache_read_tokens as cacheReadTokens,
        cache_creation_tokens as cacheCreationTokens,cost_nanos as costNanos
       from dashboard_model_window where days=? and calls>0
       order by cost_nanos desc,input_tokens desc limit 12`,
    ).all(days) as Array<Record<string, unknown>>;
    const byModel = modelRows.map((row)=>({...row,costUsd:usd(row.costNanos),costNanos:undefined}));
    const actionMix = this.db.prepare(
      `select action_class as actionClass,events as n from dashboard_action_window
       where days=? and events>0 order by events desc`,
    ).all(days);
    const sessionRows = this.db.prepare(
      `select s.session_hash as sessionId,s.source,s.started_at as startedAt,s.ended_at as endedAt,
        s.events,s.input_tokens as inputTokens,s.output_tokens as outputTokens,
        s.cache_read_tokens as cacheReadTokens,s.cost_nanos as costNanos,
        r.branch_hash as branchHash,r.repo_count as repoCount,r.dominant_repo_hash as repoHash
       from dashboard_session_source_window s join dashboard_session_root_window r
        on r.days=s.days and r.session_hash=s.session_hash
       where s.days=? order by s.cost_nanos desc,s.events desc limit 60`,
    ).all(days) as Array<Record<string, unknown>>;
    const sessions = sessionRows.map((row)=>({...row,costUsd:usd(row.costNanos),costNanos:undefined,repoLabel:null}));

    const repoSessions = this.db.prepare(
      `select repo_key as repoKey,repo_hash as repoHash,count(*) as sessions,
        sum(input_tokens) as inputTokens,sum(output_tokens) as outputTokens,
        sum(cost_nanos) as costNanos
       from dashboard_repo_session_window where days=? group by repo_key,repo_hash
       order by costNanos desc`,
    ).all(days) as Array<Record<string, unknown>>;
    const branchRows = this.db.prepare(
      `select repo_key as repoKey,branch_hash as branchHash
       from dashboard_repo_branch_window where days=? group by repo_key,branch_hash`,
    ).all(days) as Array<{repoKey:string;branchHash:string}>;
    const branchCounts = new Map<string,number>();
    for (const row of branchRows) add(branchCounts,row.repoKey,1);
    let repos: Array<{repoHash:unknown;label:string|null;sessions:number;branchRefs:number;inputTokens:number;outputTokens:number;costUsd:number}> = repoSessions.map((row)=>({repoHash:row.repoHash,label:null,sessions:Number(row.sessions),
      branchRefs:branchCounts.get(String(row.repoKey))??0,inputTokens:Number(row.inputTokens),
      outputTokens:Number(row.outputTokens),costUsd:usd(row.costNanos)}));
    if (repos.length > 12) {
      const head = repos.slice(0,11);
      const tail = repos.slice(11);
      head.push({repoHash:"__tail__",label:`(${tail.length} more repositories)`,
        sessions:tail.reduce((sum,row)=>sum+row.sessions,0),branchRefs:0,
        inputTokens:tail.reduce((sum,row)=>sum+row.inputTokens,0),
        outputTokens:tail.reduce((sum,row)=>sum+row.outputTokens,0),
        costUsd:Number(tail.reduce((sum,row)=>sum+row.costUsd,0).toFixed(4))});
      repos=head;
    }

    const priority = new Set((this.db.prepare(`select repo_hash as repoHash from priority_repos`).all() as Array<{repoHash:string}>).map((row)=>canonicalLinkage(row.repoHash)).filter((v):v is string=>Boolean(v)));
    const accountSessions = this.db.prepare(
      `select account_key as accountKey,account_hash as accountHash,session_hash as sessionHash,
        dominant_repo_hash as repoHash,source,machine_hashes_json as machinesJson,
        cost_nanos as costNanos,input_tokens as inputTokens,output_tokens as outputTokens
       from dashboard_account_session_window where days=?`,
    ).all(days) as Array<Record<string, unknown>>;
    const accountMap = new Map<string,Record<string,unknown>>();
    for (const row of accountSessions) {
      const key=String(row.accountKey);
      const value=accountMap.get(key)??{accountHash:row.accountHash,machines:new Set<string>(),sessions:0,
        priorityUsd:0,otherUsd:0,unlinkedUsd:0,totalUsd:0,claudeUsd:0,codexUsd:0,inputTokens:0,outputTokens:0};
      const cost=usd(row.costNanos); (value.machines as Set<string>).forEach(()=>undefined);
      for(const machine of json<string[]>(String(row.machinesJson))) (value.machines as Set<string>).add(machine);
      value.sessions=Number(value.sessions)+1; value.totalUsd=Number(value.totalUsd)+cost;
      if(!row.repoHash)value.unlinkedUsd=Number(value.unlinkedUsd)+cost;
      else if(priority.has(String(row.repoHash)))value.priorityUsd=Number(value.priorityUsd)+cost;
      else value.otherUsd=Number(value.otherUsd)+cost;
      if(row.source==="claude_code")value.claudeUsd=Number(value.claudeUsd)+cost;
      if(row.source==="codex")value.codexUsd=Number(value.codexUsd)+cost;
      value.inputTokens=Number(value.inputTokens)+Number(row.inputTokens);
      value.outputTokens=Number(value.outputTokens)+Number(row.outputTokens);
      accountMap.set(key,value);
    }
    const accountRows: Array<Record<string,unknown>>=[...accountMap.values()].sort((a,b)=>Number(b.totalUsd)-Number(a.totalUsd)).map((row)=>({...row,machines:[...(row.machines as Set<string>)],label:null,email:null,subscription:null}));
    const buckets={priorityUsd:Number(accountRows.reduce((s,r)=>s+Number(r.priorityUsd),0).toFixed(4)),
      otherUsd:Number(accountRows.reduce((s,r)=>s+Number(r.otherUsd),0).toFixed(4)),
      unlinkedUsd:Number(accountRows.reduce((s,r)=>s+Number(r.unlinkedUsd),0).toFixed(4))};
    const health=this.buildHealth(days,now);
    const stats=this.lifetimeStats();
    const counters=this.workCounters();
    rowsVisited += 3 + bySourceRows.length + dailyRows.length + modelRows.length + actionMix.length +
      sessionRows.length + repoSessions.length + branchRows.length + accountSessions.length;
    const snapshot: SnapshotCore={schemaVersion:DASHBOARD_SCHEMA_VERSION,generation,
      window:{days,since:cutoff},projection:{status:"ready",freshnessAt:now.toISOString(),degraded:false,
        degradedReason:null,parityReady:true,counters},
      summary:{days,since:cutoff,totals:{events:total.events,tokenEvents:total.tokenEvents,
        inputTokens:total.inputTokens,outputTokens:total.outputTokens,cacheReadTokens:total.cacheReadTokens,
        cacheCreationTokens:total.cacheCreationTokens,costUsd:usd(total.costNanos),sessions:sessionCounts.sessions,
        sessionsWithTokens:sessionCounts.sessionsWithTokens??0,oldest:span.oldest,newest:span.newest},
        bySource,daily,byModel,actionMix},sessions,repos,
      accounts:{days,buckets,accounts:accountRows,priorityRepoCount:priority.size},
      status:{stats,health,projection:{ready:true,parityReady:true,generation,counters}}};
    return {snapshot,rowsVisited};
  }

  private lifetimeStats() {
    const row=this.db.prepare(
      `select events as count,oldest_observed_at as oldestCreatedAt,
        newest_observed_at as newestCreatedAt,token_events as tokenAttributedEvents,
        input_tokens as totalInputTokens,output_tokens as totalOutputTokens,
        cost_nanos as totalCostNanos from dashboard_lifetime_totals where singleton=1`,
    ).get() as Record<string,unknown>;
    const control=this.control();
    return {...row,unuploadedCount:null,
      metricSampleCount:control.metricBackfillComplete?control.metricSampleCount:null,
      totalCostUsd:usd(row.totalCostNanos),totalCostNanos:undefined};
  }

  private buildHealth(_days:number,now:Date) {
    const today=now.toISOString().slice(0,10);
    const activityRows=this.db.prepare(
      `select source,last_activity_at as lastActivityAt,files_today as filesToday,
        discovery_entries as discoveryEntries,last_scan_at as lastScanAt,
        last_error_code as lastErrorCode,truncated from capture_activity_state`,
    ).all() as Array<Record<string,unknown>>;
    const activity=new Map(activityRows.map((row)=>[String(row.source),row]));
    const sources=["claude_code","codex"].map((source)=>{
      const local=activity.get(source);
      const latest=(this.db.prepare(
        `select last_event_at as lastEventAt,last_token_event_at as tokenAt
         from dashboard_source_lifetime where source=?`,
      ).get(source) as {lastEventAt:string|null;tokenAt:string|null}|undefined)??{lastEventAt:null,tokenAt:null};
      const sessions=this.db.prepare(
        `select count(*) as ledgerSessionsToday,
          sum(case when token_events>0 then 1 else 0 end) as tokenSessionsToday
         from dashboard_session_source_window where days=7 and source=? and ended_at>=?`,
      ).get(source,`${today}T00:00:00.000Z`) as {ledgerSessionsToday:number;tokenSessionsToday:number|null};
      let status:"green"|"amber"|"red"="green"; let reason="capture current";
      if(!local){status="amber";reason="local activity state unavailable — awaiting a tailer scan";}
      else if(Number(local.truncated)||local.lastErrorCode){status="amber";reason="local activity scan incomplete — capture state is directional";}
      else if(now.getTime()-Date.parse(String(local.lastScanAt))>3*60_000){status="amber";reason="local activity state is stale — quiet cannot be confirmed";}
      else if(local.lastActivityAt){
        const activityAge=now.getTime()-Date.parse(String(local.lastActivityAt));
        const lag=latest.lastEventAt?Date.parse(String(local.lastActivityAt))-Date.parse(latest.lastEventAt):Infinity;
        if(activityAge<=60*60_000&&lag>10*60_000){status="red";reason="recent local activity is not reaching the projected ledger";}
        else if(Number(local.filesToday)>0&&(sessions.tokenSessionsToday??0)===0){status="red";reason=`${local.filesToday} local session file(s) today, 0 sessions captured with tokens`;}
        else if(source==="codex"&&Number(local.filesToday)>0&&(sessions.tokenSessionsToday??0)*2<Number(local.filesToday)){status="amber";reason="rollout activity exceeds token-attributed capture";}
        else reason=`capture current — ${sessions.tokenSessionsToday??0} session(s) with tokens today`;
      } else reason="no local activity observed by the latest tailer scan";
      return {source,lastEventAt:latest.lastEventAt,lastTokenEventAt:latest.tokenAt,
        localLastActivityAt:local?.lastActivityAt??null,localSessionsToday:Number(local?.filesToday??0),
        ledgerSessionsToday:sessions.ledgerSessionsToday,tokenSessionsToday:sessions.tokenSessionsToday??0,status,reason,
        activityState:{lastScanAt:local?.lastScanAt??null,discoveryEntries:Number(local?.discoveryEntries??0),truncated:Boolean(local?.truncated)}};
    });
    const rank={green:0,amber:1,red:2};
    const overall=sources.reduce<"green"|"amber"|"red">((worst,row)=>rank[row.status]>rank[worst]?row.status:worst,"green");
    return {generatedAt:now.toISOString(),overall,sources};
  }

  workCounters() {
    const c=this.control();
    return {projectionRowsVisited:c.projectionRowsVisited,projectionRowsWritten:c.projectionRowsWritten,
      snapshotRowsVisited:c.snapshotRowsVisited,snapshotBuilds:c.snapshotBuilds,
      snapshotCacheHits:c.snapshotCacheHits,repairFacts:c.repairFacts,backfillFacts:c.backfillFacts,
      expiryFacts:c.expiryFacts,rawRowsScannedByDashboard:c.rawRowsScannedByDashboard,
      filesystemEntriesScannedByDashboard:c.filesystemEntriesScannedByDashboard,
      compactSegmentsWritten:c.compactSegmentsWritten,
      compactGcItemsVisited:c.compactGcItemsVisited,
      compactGcItemsRemoved:c.compactGcItemsRemoved,
      compactGcSegmentsRewritten:c.compactGcSegmentsRewritten,
      compactGcSegmentsDeleted:c.compactGcSegmentsDeleted,
      compactGcDaysCompleted:c.compactGcDaysCompleted,
      compactGcRestarts:c.compactGcRestarts};
  }

  status() {
    const c=this.control(); const backlog=this.backlog();
    return {schemaVersion:DASHBOARD_SCHEMA_VERSION,generation:c.generation,
      ready:Boolean(c.ready),parityReady:Boolean(c.parityReady),dirty:Boolean(c.dirty),
      degraded:Boolean(c.degradedReason),degradedReason:c.degradedReason,
      lastSuccessAt:c.lastSuccessAt,lastErrorAt:c.lastErrorAt,
      backfill:{highWater:c.backfillHighWater,cursor:c.backfillCursor,complete:Boolean(c.backfillComplete),
        parityCursor:c.parityCursor,parityComplete:Boolean(c.parityComplete),
        metricHighWater:c.metricBackfillHighWater,metricCursor:c.metricBackfillCursor,
        metricComplete:Boolean(c.metricBackfillComplete),metricSampleCount:c.metricBackfillComplete?c.metricSampleCount:null,
        progressMode:"bounded_rowid_watermark_no_exact_remaining",sliceRows:BACKFILL_ROWS},
      backlog,counters:this.workCounters(),retention:{rawTtlActivation:"explicit_proof_gated",
        projectionParityReady:Boolean(c.parityReady)}};
  }

  readSnapshot(days:number,subscriptions:SubscriptionConfig[]=[]):SnapshotRead {
    if(!DASHBOARD_WINDOWS.includes(days as typeof DASHBOARD_WINDOWS[number])) return {kind:"unsupported",supportedDays:DASHBOARD_WINDOWS};
    const row=this.db.prepare(
      `select payload_json as payloadJson,generation from dashboard_snapshots where days=?`,
    ).get(days) as {payloadJson:string;generation:number}|undefined;
    const control=this.control();
    if(!row||!control.ready)return {kind:"backfilling",status:this.status()};
    const snapshot=json<SnapshotCore>(row.payloadJson);
    const cutoff=(this.db.prepare(`select cutoff_at as cutoffAt from dashboard_window_control where days=?`).get(days) as {cutoffAt:string}).cutoffAt;
    snapshot.window.since=cutoff;
    snapshot.summary.since=cutoff;
    if(control.dirty||control.degradedReason){snapshot.projection.status="stale";snapshot.projection.degraded=true;
      snapshot.projection.degradedReason=control.degradedReason??"projection_pending";}
    this.decoratePresentation(snapshot,subscriptions);
    snapshot.status.health = this.buildHealth(days, new Date(Date.now()));
    this.db.prepare(`update dashboard_projection_control set snapshot_cache_hits=snapshot_cache_hits+1 where singleton=1`).run();
    const settings=(this.db.prepare(`select settings_version as version from dashboard_projection_control where singleton=1`).get() as {version:number}).version;
    return {kind:"ready",snapshot,etagSeed:`${row.generation}-${settings}-${cutoff}`};
  }

  private decoratePresentation(snapshot:SnapshotCore,subscriptions:SubscriptionConfig[]) {
    const repoLabels=new Map((this.db.prepare(`select repo_hash as repoHash,label from repo_labels`).all() as Array<{repoHash:string;label:string}>).map((r)=>[canonicalLinkage(r.repoHash),r.label]));
    for(const row of snapshot.repos){const hash=row.repoHash; if(typeof hash==="string")row.label=repoLabels.get(hash)??row.label;}
    for(const row of snapshot.sessions){const hash=row.repoHash; if(typeof hash==="string")row.repoLabel=repoLabels.get(hash)??null;}
    const accountLabels=new Map((this.db.prepare(`select account_hash as accountHash,label,email from account_labels`).all() as Array<{accountHash:string;label:string;email:string|null}>).map((r)=>[safeHash(r.accountHash),r]));
    const windowMonths=snapshot.window.days/30.44;
    for(const row of snapshot.accounts.accounts){
      const label=typeof row.accountHash==="string"?accountLabels.get(row.accountHash):undefined;
      row.label=label?.label??null; row.email=label?.email??null;
      const matching=subscriptions.filter((sub)=>safeHash(sub.account)===row.accountHash||sub.account===label?.label||sub.account===label?.email);
      const monthly=matching.reduce((sum,sub)=>sum+sub.usdPerMonth,0);
      const planCost=matching.length?Number((monthly*windowMonths).toFixed(2)):null;
      const vendorSpend={anthropic:Number(row.claudeUsd??0),openai:Number(row.codexUsd??0)};
      const byVendor=(["anthropic","openai"] as const).flatMap((vendor)=>{const plans=matching.filter((sub)=>sub.vendor===vendor);if(!plans.length)return[];const cost=Number((plans.reduce((s,p)=>s+p.usdPerMonth,0)*windowMonths).toFixed(2));return[{vendor,plans:plans.map((p)=>p.plan).join(" + "),planCostWindow:cost,spendUsd:Number(vendorSpend[vendor].toFixed(2)),leverage:cost>0?Number((vendorSpend[vendor]/cost).toFixed(2)):null}];});
      row.subscription=matching.length?{plan:matching.map((s)=>s.plan).join(" + "),usdPerMonth:monthly,
        planCostWindow:planCost,leverage:planCost&&planCost>0?Number((Number(row.totalUsd)/planCost).toFixed(4)):null,byVendor}:null;
    }
  }

  sessionDetail(sessionHash:string) {
    if(!CANONICAL_SHA256.test(sessionHash))return null;
    const rows=(this.db.prepare(
      `select source,event_type as eventType,observed_at as observedAt,action_class as actionClass,
        model,input_tokens as inputTokens,output_tokens as outputTokens,
        cache_read_tokens as cacheReadTokens,cache_creation_tokens as cacheCreationTokens,
        cost_nanos as costNanos,repo_hash as repoHash,branch_hash as branchHash,
        head_hash as headHash,suppressed from dashboard_event_facts
       where session_hash=? order by observed_at`,
    ).all(sessionHash) as Array<Record<string,unknown>>);
    if(!rows.length)return null;
    const eventTypes=new Map<string,number>(),actions=new Map<string,number>(),models=new Map<string,Record<string,number>>(),links=new Map<string,Record<string,unknown>>();
    for(const row of rows){add(eventTypes,String(row.eventType),1);if(row.eventType==="tool_use"||row.eventType==="tool_result")add(actions,String(row.actionClass??"other"),1);
      if(row.model){const key=String(row.model),m=models.get(key)??{calls:0,inputTokens:0,outputTokens:0,cacheReadTokens:0,cacheCreationTokens:0,costNanos:0};m.calls++;m.inputTokens+=Number(row.inputTokens??0);m.outputTokens+=Number(row.outputTokens??0);m.cacheReadTokens+=Number(row.cacheReadTokens??0);m.cacheCreationTokens+=Number(row.cacheCreationTokens??0);m.costNanos+=Number(row.costNanos??0);models.set(key,m);}
      if(row.repoHash||row.branchHash||row.headHash){const key=`${row.repoHash??""}|${row.branchHash??""}|${row.headHash??""}`,l=links.get(key)??{repoHash:row.repoHash,branchHash:row.branchHash,headSha:row.headHash,events:0};l.events=Number(l.events)+1;links.set(key,l);}}
    const rollup={sessionId:sessionHash,source:String(rows.map((r)=>r.source).sort().at(-1)),startedAt:rows[0]!.observedAt,endedAt:rows.at(-1)!.observedAt,events:rows.length,
      inputTokens:rows.reduce((s,r)=>s+Number(r.inputTokens??0),0),outputTokens:rows.reduce((s,r)=>s+Number(r.outputTokens??0),0),cacheReadTokens:rows.reduce((s,r)=>s+Number(r.cacheReadTokens??0),0),cacheCreationTokens:rows.reduce((s,r)=>s+Number(r.cacheCreationTokens??0),0),costUsd:usd(rows.reduce((s,r)=>s+Number(r.costNanos??0),0)),tokenEvents:rows.filter((r)=>r.inputTokens!==null).length};
    return {rollup,receipts:{linkage:[...links.values()].sort((a,b)=>Number(b.events)-Number(a.events)).slice(0,10),eventTypes:[...eventTypes].map(([eventType,n])=>({eventType,n})).sort((a,b)=>b.n-a.n),actionMix:[...actions].map(([actionClass,n])=>({actionClass,n})).sort((a,b)=>b.n-a.n),models:[...models].map(([model,m])=>({model,...m,costUsd:usd(m.costNanos),costNanos:undefined})).sort((a,b)=>b.costUsd-a.costUsd),suppression:{suppressedEvents:rows.filter((r)=>Number(r.suppressed)).length}}};
  }

  repoDetail(repoHash:string,days:number) {
    if(!CANONICAL_SHA256.test(repoHash)||!DASHBOARD_WINDOWS.includes(days as typeof DASHBOARD_WINDOWS[number]))return null;
    const cutoff=(this.db.prepare(`select cutoff_at as cutoffAt from dashboard_window_control where days=?`).get(days) as {cutoffAt:string}).cutoffAt;
    const rows=this.db.prepare(
      `select session_hash as sessionHash,observed_at as observedAt,branch_hash as branchHash,
        action_class as actionClass,event_type as eventType,model,input_tokens as inputTokens,
        output_tokens as outputTokens,cost_nanos as costNanos from dashboard_event_facts
       where repo_hash=? and observed_at>=?`,
    ).all(repoHash,cutoff) as Array<Record<string,unknown>>;
    const label=(this.db.prepare(`select label from repo_labels where repo_hash=?`).get(repoHash) as {label:string}|undefined)?.label;
    if(!rows.length&&!label)return null;
    const sessions=new Set(rows.map((r)=>r.sessionHash).filter(Boolean)),daily=new Map<string,number>(),branches=new Map<string,{events:number;sessions:Set<unknown>}>(),actions=new Map<string,number>(),models=new Map<string,{input:number;output:number;cost:number}>();
    for(const row of rows){add(daily,day(String(row.observedAt)),Number(row.costNanos??0));if(row.branchHash){const key=String(row.branchHash),b=branches.get(key)??{events:0,sessions:new Set()};b.events++;if(row.sessionHash)b.sessions.add(row.sessionHash);branches.set(key,b);}if(row.eventType==="tool_use"||row.eventType==="tool_result")add(actions,String(row.actionClass??"other"),1);if(row.model){const key=String(row.model),m=models.get(key)??{input:0,output:0,cost:0};m.input+=Number(row.inputTokens??0);m.output+=Number(row.outputTokens??0);m.cost+=Number(row.costNanos??0);models.set(key,m);}}
    return {repoHash,label:label??null,days,totals:{sessions:sessions.size,events:rows.length,inputTokens:rows.reduce((s,r)=>s+Number(r.inputTokens??0),0),outputTokens:rows.reduce((s,r)=>s+Number(r.outputTokens??0),0),costUsd:usd(rows.reduce((s,r)=>s+Number(r.costNanos??0),0))},daily:[...daily].sort().map(([day,cost])=>({day,costUsd:usd(cost)})),branches:[...branches].map(([branchHash,b])=>({branchHash,events:b.events,sessions:b.sessions.size})).sort((a,b)=>b.events-a.events).slice(0,15),actionMix:[...actions].map(([actionClass,n])=>({actionClass,n})).sort((a,b)=>b.n-a.n),models:[...models].map(([model,m])=>({model,inputTokens:m.input,outputTokens:m.output,costUsd:usd(m.cost)})).sort((a,b)=>b.costUsd-a.costUsd).slice(0,8)};
  }
}
