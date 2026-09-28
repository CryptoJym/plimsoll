import Database from "better-sqlite3";

import { BOUNDED_SQL_READ_PREDICATE, boundedSqlRows } from "./bounded-sql-read";
import { terminalPrivacyEligibilitySql } from "./privacy-disposition";
import { SyncStorageRetryController } from "./sqlite-contention";

/** A read query that can be executed by the session-summary read worker. */
export type SessionReadQuery = {
  sql: string;
  params: Record<string, unknown>;
  /** Optional SQLite worker interrupt deadline for bounded maintenance reads. */
  maxMs?: number;
};

/** The stable, privacy-filtered value sent by session sync. */
export type SessionSnapshot = {
  sessionId: string;
  source: string;
  startedAt: string;
  endedAt: string;
  events: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  pricedEvents: number;
  costUsd: number;
  repoHash: string | null;
  branchHash: string | null;
  accountHash: string | null;
};

export const SESSION_SUMMARY_SCHEMA_VERSION = 4 as const;
export const SESSION_SUMMARY_DEFAULT_MAX_ROWS = 5_000;
export const SESSION_SUMMARY_DEFAULT_MAX_MS = 250;
export const SESSION_SUMMARY_ZERO_PROGRESS_STUCK_AFTER = 3;
const ZERO_PROGRESS_RETRY_BASE_MS = 100;
const ZERO_PROGRESS_RETRY_MAX_MS = 5_000;
const SESSION_SUMMARY_SEGMENT_ROWS = 4_096;

type SummaryAggregate = {
  /** The next future row in this bounded rowid segment. */
  futureRows: boolean;
  futureCreatedAt: string | null;
  sourceMax: string | null;
  startedAt: string | null;
  endedAt: string | null;
  events: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  pricedEvents: number;
  costUsd: number;
  costCompensation: number;
  repoNonNull: number;
  repoValue: string | null;
  repoMixed: boolean;
  branchNonNull: number;
  branchValue: string | null;
  branchMixed: boolean;
  accountNonNull: number;
  accountValue: string | null;
  accountMixed: boolean;
};

type SummaryRepair = {
  segment: number;
  revision: number;
  until: string;
  cursorRowid: number;
  aggregate: SummaryAggregate;
};

type SummaryAccumulator = SummaryAggregate & {
  sessionId: string;
  /** Rows through this rowid belong to the frozen historical scan. */
  scanBoundary: number;
  /** Historical scan cursor follows idx_events_session (session, observation, rowid). */
  cursorObservedAt: string | null;
  cursorRowid: number;
  cursorId: string | null;
  /** Next upper rowid for a bounded predecessor search after checkpoint loss. */
  checkpointSearchRowid: number | null;
  /** A bounded rowid segment is replaced after a scanned edit or maturity. */
  segments: Record<string, SummaryAggregate>;
  scanComplete: boolean;
  activeRepair: SummaryRepair | null;
};

type SummaryState = {
  sessionId: string;
  /** Incremented with every durable replacement, independent of ledger revision. */
  stateGeneration: number;
  schemaVersion: number;
  highWater: number;
  checkpointId: string | null;
  coveredUntil: string;
  complete: boolean;
  mutationRevision: number;
  mode: "initial" | "incremental" | "fallback";
  accumulator: SummaryAccumulator;
};

type StoredSummaryState = Omit<SummaryState, "accumulator"> & {
  accumulatorJson: string;
};

type RawSummaryRow = {
  rowid: number;
  id: string;
  sessionId: string | null;
  source: string;
  observedAt: string;
  createdAt: string;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  costUsd: number | null;
  repoHash: string | null;
  branchHash: string | null;
  accountHash: string | null;
  eligible: number;
};

type ControlRow = {
  mutationRevision: number;
  fallbackRecomputes: number;
};

export type SessionSummaryRead = <T>(queries: SessionReadQuery[]) => Promise<T[]>;

export type SessionSummaryUpdateOptions = {
  maxRows?: number;
  maxMs?: number;
  read: SessionSummaryRead;
  /** Share one bounded wait budget across schema setup and summary writes in a daemon pass. */
  writeRetry?: SyncStorageRetryController;
  /** Proof-injectable clock for the durable zero-progress retry deadline. */
  now?: () => Date;
};

export type SessionSummaryUpdateResult = {
  snapshot: SessionSnapshot | null;
  complete: boolean;
  rowsRead: number;
  rowsApplied: number;
  durationMs: number;
  highWater: number;
  mode: "initial" | "incremental" | "cached" | "fallback";
  fullRecompute: boolean;
  fallbackReason: string | null;
  /** Revision committed with this snapshot, for the final upload fence. */
  mutationRevision: number;
};

export type SessionSummaryCounters = {
  mutationRevision: number;
  fallbackRecomputes: number;
};

function tableExists(db: Database.Database, name: string): boolean {
  return Boolean(
    db.prepare("select 1 from sqlite_master where type='table' and name=? limit 1").get(name),
  );
}

function columnNames(db: Database.Database, table: string): Set<string> {
  if (!tableExists(db, table)) return new Set();
  return new Set(
    (db.pragma(`table_info(${table})`) as Array<{ name: string }>).map((row) => row.name),
  );
}

/** True only when the durable accumulator has not read this version of a row.
 * The historical cursor is in (observed_at, rowid) order, while appended rows
 * are read from the rowid queue. A missing or malformed state fails closed.
 */
function unscannedRowSql(sessionId: string, rowid: string, observedAt: string): string {
  return `exists (select 1 from session_sync_summary_state s
    where s.session_id = ${sessionId} and json_valid(s.accumulator_json)
      and (
        ((s.complete = 1 or s.mode = 'incremental') and ${rowid} > s.high_water)
        or (s.complete = 0 and s.mode != 'incremental' and
          ((${rowid} > json_extract(s.accumulator_json, '$.scanBoundary') and ${rowid} > s.high_water)
           or (${rowid} <= json_extract(s.accumulator_json, '$.scanBoundary') and
             (json_extract(s.accumulator_json, '$.cursorObservedAt') is null
              or ${observedAt} > json_extract(s.accumulator_json, '$.cursorObservedAt')
              or (${observedAt} = json_extract(s.accumulator_json, '$.cursorObservedAt') and
                  ${rowid} > json_extract(s.accumulator_json, '$.cursorRowid')))))
      )))`;
}

function outboxLineageMismatchSql(outbox: string, event: string): string {
  return `(${outbox}.raw_id is null or ${outbox}.raw_created_at is null or
    ${outbox}.raw_generation is null or ${outbox}.raw_id is not ${event}.id or
    ${outbox}.raw_created_at is not ${event}.created_at or
    ${outbox}.raw_generation is not ${event}.privacy_generation)`;
}

/**
 * Install the small durable summary index. This is additive and deliberately
 * does not backfill or scan buffered_events. The raw mutation triggers make a
 * lower-than-HWM edit observable. A separate per-session revision protects
 * against a damaged/deleted dirty marker without restarting other sessions.
 */
export function ensureSessionSummarySchema(db: Database.Database): void {
  const revisionTableMissing = !tableExists(db, "session_sync_summary_revision");
  const rawInsertTrigger = db.prepare(
    "select sql from sqlite_master where type='trigger' and name='trg_session_summary_raw_insert'",
  ).get() as { sql: string } | undefined;
  const leaseTriggers = db.prepare(`select name, sql from sqlite_master where type='trigger'
    and name in ('trg_session_sync_upload_lease_insert',
      'trg_session_sync_upload_lease_dirty_insert',
      'trg_session_sync_upload_lease_dirty_update')`).all() as Array<{ name: string; sql: string }>;
  const oldUnscanned = unscannedRowSql("old.session_id", "old.rowid", "old.observed_at");
  const newUnscanned = unscannedRowSql("new.session_id", "new.rowid", "new.observed_at");
  const rawSummaryChanged = [
    "id", "source", "data_mode", "observed_at", "created_at", "session_id",
    "input_tokens", "output_tokens", "cache_read_tokens", "cache_creation_tokens",
    "cost_usd", "repo_hash", "branch_hash", "account_hash",
    "privacy_generation", "privacy_disposition",
  ].map((column) => `old.${column} is not new.${column}`).join(" or ");
  const oldOutboxMismatch = outboxLineageMismatchSql("old", "e");
  const newOutboxMismatch = outboxLineageMismatchSql("new", "e");
  const oldOutboxChange = `${oldOutboxMismatch} and
    (new.raw_rowid is not old.raw_rowid or not ${newOutboxMismatch})`;
  const newOutboxChange = `${newOutboxMismatch} and
    (new.raw_rowid is not old.raw_rowid or not ${oldOutboxMismatch})`;
  const linkedUnscanned = unscannedRowSql("e.session_id", "e.rowid", "e.observed_at");
  const queueLinkedRepair = (filter: string) => `insert into session_sync_summary_repairs
    (session_id, segment, revision)
    select e.session_id, cast((e.rowid - 1) / ${SESSION_SUMMARY_SEGMENT_ROWS} as integer), 1
    from buffered_events e where ${filter} and e.session_id is not null and not ${linkedUnscanned}
    on conflict(session_id, segment) do update set revision = revision + 1;`;
  const oldOutboxAffects = `exists (select 1 from buffered_events e
    join session_sync_summary_state s on s.session_id = e.session_id
    where e.rowid = old.raw_rowid and ${oldOutboxChange})`;
  const newOutboxAffects = `exists (select 1 from buffered_events e
    join session_sync_summary_state s on s.session_id = e.session_id
    where e.rowid = new.raw_rowid and ${newOutboxChange})`;
  const terminalOld = "old.reason in ('local_evidence_quarantined','local_privacy_violation')";
  const terminalNew = "new.reason in ('local_evidence_quarantined','local_privacy_violation')";
  const oldReceiptChange = `${terminalOld} and (new.delivery_id is not old.delivery_id or not ${terminalNew})`;
  const newReceiptChange = `${terminalNew} and (new.delivery_id is not old.delivery_id or not ${terminalOld})`;
  const oldReceiptAffects = `exists (select 1 from buffered_events e
    join session_sync_summary_state s on s.session_id = e.session_id
    where e.id = old.delivery_id and ${oldReceiptChange})`;
  const newReceiptAffects = `exists (select 1 from buffered_events e
    join session_sync_summary_state s on s.session_id = e.session_id
    where e.id = new.delivery_id and ${newReceiptChange})`;
  // Keep the 0.7.41 trigger names free. Its installer uses IF NOT EXISTS and
  // must restore its own revision marks on downgrade. Remove those old-name
  // triggers on re-upgrade; the scanned-aware triggers have distinct names.
  db.transaction(() => {
    // Existing 0.7.40 ledgers have this trigger. An append is outside the
    // leased snapshot; only edits and erasures of existing rows need a fence.
    for (const trigger of leaseTriggers) {
      if (trigger.name === "trg_session_sync_upload_lease_insert" ||
          !trigger.sql.includes("raw_insert_before_high_water")) {
        db.exec(`drop trigger ${trigger.name}`);
      }
    }
    if (rawInsertTrigger && !rawInsertTrigger.sql.includes("scanBoundary")) {
      db.exec("drop trigger trg_session_summary_raw_insert");
    }
    const legacySummaryTriggers = db.prepare(`select name from sqlite_master where type='trigger'
      and name in ('trg_session_summary_raw_update', 'trg_session_summary_raw_delete',
        'trg_session_summary_outbox_insert', 'trg_session_summary_outbox_update',
        'trg_session_summary_outbox_delete', 'trg_session_summary_receipt_insert',
        'trg_session_summary_receipt_update', 'trg_session_summary_receipt_delete')`)
      .all() as Array<{ name: string }>;
    for (const trigger of legacySummaryTriggers) db.exec(`drop trigger ${trigger.name}`);
    db.exec(`
    create table if not exists session_sync_summary_control (
      singleton integer primary key check (singleton = 1),
      mutation_revision integer not null default 0,
      fallback_recomputes integer not null default 0,
      updated_at text not null
    );
    insert or ignore into session_sync_summary_control
      (singleton, updated_at) values (1, strftime('%Y-%m-%dT%H:%M:%fZ','now'));
    create table if not exists session_sync_summary_state (
      session_id text primary key,
      state_generation integer not null default 0 check (state_generation >= 0),
      schema_version integer not null,
      high_water integer not null check (high_water >= 0),
      checkpoint_id text,
      covered_until text not null,
      complete integer not null check (complete in (0, 1)),
      mutation_revision integer not null check (mutation_revision >= 0),
      mode text not null check (mode in ('initial', 'incremental', 'fallback')),
      accumulator_json text not null,
      updated_at text not null
    );
    create table if not exists session_sync_summary_pending (
      session_id text primary key,
      reason text not null,
      mutation_revision integer not null,
      queued_high_water integer not null,
      consecutive_zero_progress integer not null check (consecutive_zero_progress >= 0),
      next_retry_at text,
      updated_at text not null
    );
    create table if not exists session_sync_summary_dirty (
      session_id text primary key,
      reason text not null,
      updated_at text not null
    );
    create table if not exists session_sync_summary_revision (
      session_id text primary key,
      mutation_revision integer not null check (mutation_revision >= 0)
    );
    -- This counter fences a worker read without invalidating an unscanned
    -- edit. If it moves during the read, the slice is retried from its last
    -- committed cursor rather than committing a stale worker snapshot.
    create table if not exists session_sync_summary_activity (
      session_id text primary key,
      activity_revision integer not null check (activity_revision >= 0)
    );
    -- A session-sync upload owns a short, per-session lease rather than the
    -- database-wide write reservation. Appends remain in the summary queue;
    -- edits and erasures defer so an in-flight body cannot be overtaken by
    -- an erasure. The transport clears these rows after the response (or
    -- they become eligible for reuse after their bounded expiry).
    create table if not exists session_sync_upload_leases (
      session_id text primary key,
      lease_token text not null,
      lease_expires_at text not null,
      mutation_revision integer not null,
      high_water integer not null
    );
    create index if not exists idx_session_sync_upload_leases_expiry
      on session_sync_upload_leases (lease_expires_at);
    create trigger if not exists trg_session_sync_upload_lease_update
    before update of id, source, event_type, data_mode, observed_at, created_at,
      session_id, input_tokens, output_tokens, cache_read_tokens,
      cache_creation_tokens, cost_usd, repo_hash, branch_hash, account_hash,
      privacy_generation, privacy_disposition on buffered_events
    when exists (
      select 1 from session_sync_upload_leases
       where lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now')
         and (session_id = old.session_id or session_id = new.session_id)
    )
    begin
      select raise(abort, 'session_sync_upload_lease');
    end;
    create trigger if not exists trg_session_sync_upload_lease_delete
    before delete on buffered_events
    when old.session_id is not null and exists (
      select 1 from session_sync_upload_leases
       where session_id = old.session_id
         and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now')
    )
    begin
      select raise(abort, 'session_sync_upload_lease');
    end;
    -- Privacy receipt/outbox changes also dirty the summary. Abort their
    -- original statement so callers retain the work for retry. A backdated
    -- append may dirty a frozen scan; its marker is allowed through so the
    -- post-send freshness check resends a complete snapshot on the next pass.
    create trigger if not exists trg_session_sync_upload_lease_dirty_insert
    before insert on session_sync_summary_dirty
    when new.reason != 'raw_insert_before_high_water' and exists (select 1 from session_sync_upload_leases
      where session_id = new.session_id
        and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    begin
      select raise(abort, 'session_sync_upload_lease');
    end;
    create trigger if not exists trg_session_sync_upload_lease_dirty_update
    before update on session_sync_summary_dirty
    when new.reason != 'raw_insert_before_high_water' and exists (select 1 from session_sync_upload_leases
      where session_id = new.session_id
        and lease_expires_at > strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    begin
      select raise(abort, 'session_sync_upload_lease');
    end;
    create trigger if not exists trg_session_summary_dirty_insert_revision
    after insert on session_sync_summary_dirty
    begin
      insert into session_sync_summary_revision (session_id, mutation_revision)
        values (new.session_id, 1)
        on conflict(session_id) do update set mutation_revision = mutation_revision + 1;
    end;
    create trigger if not exists trg_session_summary_dirty_update_revision
    after update on session_sync_summary_dirty
    begin
      insert into session_sync_summary_revision (session_id, mutation_revision)
        values (new.session_id, 1)
        on conflict(session_id) do update set mutation_revision = mutation_revision + 1;
    end;
    -- New raw rowids are cheap to retain until the corresponding summary
    -- slice commits. This avoids seeking the historical session index on the
    -- steady-state path, without building a 69 GB index during upgrade.
    create table if not exists session_sync_summary_rows (
      raw_rowid integer primary key,
      session_id text not null,
      created_at text not null
    );
    create table if not exists session_sync_summary_repairs (
      session_id text not null,
      segment integer not null,
      revision integer not null,
      primary key (session_id, segment)
    );
    create table if not exists session_sync_summary_due (
      session_id text primary key,
      next_due_at text not null
    );
    create index if not exists idx_session_summary_state_incomplete
      on session_sync_summary_state (complete, updated_at);
    create index if not exists idx_session_summary_dirty_updated
      on session_sync_summary_dirty (updated_at);
    create index if not exists idx_session_summary_rows_session
      on session_sync_summary_rows (session_id, raw_rowid);
    create index if not exists idx_session_summary_due_at
      on session_sync_summary_due (next_due_at, session_id);

    create trigger if not exists trg_session_summary_raw_insert
    after insert on buffered_events
    when new.session_id is not null and exists (
      select 1 from session_sync_summary_state where session_id = new.session_id
    )
    begin
      insert or ignore into session_sync_summary_rows (raw_rowid, session_id, created_at)
        values (new.rowid, new.session_id, new.created_at);
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select new.session_id, 'raw_insert_before_high_water', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where exists (select 1 from session_sync_summary_state
          where session_id = new.session_id and
            (high_water >= new.rowid or
             (complete = 0 and mode != 'incremental' and
              (case when json_valid(accumulator_json)
                then json_extract(accumulator_json, '$.scanBoundary') end) >= new.rowid)))
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
    end;

    create trigger if not exists trg_session_summary_raw_update_v42
    after update of id, source, data_mode, observed_at, created_at,
      session_id, input_tokens, output_tokens, cache_read_tokens,
      cache_creation_tokens, cost_usd, repo_hash, branch_hash, account_hash,
      privacy_generation, privacy_disposition on buffered_events
    when ${rawSummaryChanged}
    begin
      -- summary_scanned_aware_v1: unscanned changes retry an in-flight read.
      update session_sync_summary_control
        set mutation_revision = mutation_revision + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where singleton = 1;
      insert into session_sync_summary_activity (session_id, activity_revision)
        select old.session_id, 1 where old.session_id is not null
        on conflict(session_id) do update set activity_revision = activity_revision + 1;
      insert into session_sync_summary_activity (session_id, activity_revision)
        select new.session_id, 1 where new.session_id is not null and new.session_id is not old.session_id
        on conflict(session_id) do update set activity_revision = activity_revision + 1;
      delete from session_sync_summary_rows
        where raw_rowid = old.rowid and session_id is not new.session_id;
      insert into session_sync_summary_rows (raw_rowid, session_id, created_at)
        select new.rowid, new.session_id, new.created_at
        where new.session_id is not null and exists (
          select 1 from session_sync_summary_state s where s.session_id = new.session_id
            and new.rowid > s.high_water
            and (s.complete = 1 or s.mode = 'incremental' or
                 (json_valid(s.accumulator_json) and
                  new.rowid > json_extract(s.accumulator_json, '$.scanBoundary')))
        )
        on conflict(raw_rowid) do update set
          session_id = excluded.session_id, created_at = excluded.created_at;
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select old.session_id, 'raw_update', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where old.session_id is not null and not ${oldUnscanned}
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select new.session_id, 'raw_update', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where new.session_id is not null and not ${newUnscanned}
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
    end;

    create trigger if not exists trg_session_summary_raw_delete_v42
    after delete on buffered_events
    begin
      update session_sync_summary_control
        set mutation_revision = mutation_revision + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where singleton = 1;
      insert into session_sync_summary_activity (session_id, activity_revision)
        select old.session_id, 1 where old.session_id is not null
        on conflict(session_id) do update set activity_revision = activity_revision + 1;
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select old.session_id, 'raw_delete', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where old.session_id is not null and not ${oldUnscanned}
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
      delete from session_sync_summary_rows where raw_rowid = old.rowid;
    end;
    -- These additive triggers survive upgrades from the v42 trigger names.
    -- A scanned mutation records only its bounded rowid segment; an unscanned
    -- mutation is still retried by the existing activity fence.
    create trigger if not exists trg_session_summary_repair_insert_v1
    after insert on buffered_events
    when new.session_id is not null and exists (
      select 1 from session_sync_summary_state where session_id = new.session_id
    ) and not ${newUnscanned}
    begin
      insert into session_sync_summary_repairs (session_id, segment, revision)
        values (new.session_id, cast((new.rowid - 1) / ${SESSION_SUMMARY_SEGMENT_ROWS} as integer), 1)
        on conflict(session_id, segment) do update set revision = revision + 1;
    end;
    create trigger if not exists trg_session_summary_repair_update_old_v1
    after update of id, source, data_mode, observed_at, created_at,
      session_id, input_tokens, output_tokens, cache_read_tokens,
      cache_creation_tokens, cost_usd, repo_hash, branch_hash, account_hash,
      privacy_generation, privacy_disposition on buffered_events
    when ${rawSummaryChanged} and old.session_id is not null and exists (
      select 1 from session_sync_summary_state where session_id = old.session_id
    ) and not ${oldUnscanned}
    begin
      insert into session_sync_summary_repairs (session_id, segment, revision)
        values (old.session_id, cast((old.rowid - 1) / ${SESSION_SUMMARY_SEGMENT_ROWS} as integer), 1)
        on conflict(session_id, segment) do update set revision = revision + 1;
    end;
    create trigger if not exists trg_session_summary_repair_update_new_v1
    after update of id, source, data_mode, observed_at, created_at,
      session_id, input_tokens, output_tokens, cache_read_tokens,
      cache_creation_tokens, cost_usd, repo_hash, branch_hash, account_hash,
      privacy_generation, privacy_disposition on buffered_events
    when ${rawSummaryChanged} and new.session_id is not null and exists (
      select 1 from session_sync_summary_state where session_id = new.session_id
    ) and not ${newUnscanned}
    begin
      insert into session_sync_summary_repairs (session_id, segment, revision)
        values (new.session_id, cast((new.rowid - 1) / ${SESSION_SUMMARY_SEGMENT_ROWS} as integer), 1)
        on conflict(session_id, segment) do update set revision = revision + 1;
    end;
    create trigger if not exists trg_session_summary_repair_delete_v1
    after delete on buffered_events
    when old.session_id is not null and exists (
      select 1 from session_sync_summary_state where session_id = old.session_id
    ) and not ${oldUnscanned}
    begin
      insert into session_sync_summary_repairs (session_id, segment, revision)
        values (old.session_id, cast((old.rowid - 1) / ${SESSION_SUMMARY_SEGMENT_ROWS} as integer), 1)
        on conflict(session_id, segment) do update set revision = revision + 1;
    end;
    `);
  }).immediate();

  // Existing v4 states gain a constant-size conflict fence without reading
  // buffered_events or rebuilding their summary.
  if (!columnNames(db, "session_sync_summary_state").has("state_generation")) {
    db.exec(`alter table session_sync_summary_state
      add column state_generation integer not null default 0 check (state_generation >= 0)`);
  }

  // The previous schema stored the global revision in each state. Preserve
  // that baseline once, then let dirty-marker triggers advance only the
  // affected session. No historical event table is scanned during upgrade.
  if (revisionTableMissing) {
    db.exec(`insert or ignore into session_sync_summary_revision (session_id, mutation_revision)
      select session_id, mutation_revision from session_sync_summary_state`);
  }

  // Codex pairing can change only the duplicate eligibility column. Its
  // response-span rewrite does not touch any of the v42 raw-update columns.
  if (columnNames(db, "buffered_events").has("usage_duplicate_reason")) {
    db.exec(`create trigger if not exists trg_session_summary_repair_duplicate_v1
      after update of usage_duplicate_reason on buffered_events
      when old.usage_duplicate_reason is not new.usage_duplicate_reason
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_activity (session_id, activity_revision)
          select old.session_id, 1 where old.session_id is not null
          on conflict(session_id) do update set activity_revision = activity_revision + 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select old.session_id, 'usage_duplicate', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where old.session_id is not null and exists (
            select 1 from session_sync_summary_state where session_id = old.session_id
          ) and not ${oldUnscanned}
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
        insert into session_sync_summary_repairs (session_id, segment, revision)
          select old.session_id, cast((old.rowid - 1) / ${SESSION_SUMMARY_SEGMENT_ROWS} as integer), 1
          where old.session_id is not null and exists (
            select 1 from session_sync_summary_state where session_id = old.session_id
          ) and not ${oldUnscanned}
          on conflict(session_id, segment) do update set revision = revision + 1;
      end;`);
  }

  // These tables are created by DeliveryOutbox, but a small proof ledger or a
  // pre-delivery install may not have them. Raw edits/deletes remain covered.
  if (tableExists(db, "upload_outbox")) {
    db.exec(`
      create trigger if not exists trg_session_summary_outbox_insert_v42
      after insert on upload_outbox
      when new.raw_rowid is not null and exists (
        select 1 from buffered_events e
        join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = new.raw_rowid and ${newOutboxMismatch}
      )
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_activity (session_id, activity_revision)
          select e.session_id, 1 from buffered_events e where e.rowid = new.raw_rowid
          on conflict(session_id) do update set activity_revision = activity_revision + 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = new.raw_rowid and e.session_id is not null
            and not ${linkedUnscanned}
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
      end;
      create trigger if not exists trg_session_summary_outbox_update_v42
      after update of raw_rowid, raw_id, raw_created_at, raw_generation on upload_outbox
      when ${oldOutboxAffects} or ${newOutboxAffects}
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_activity (session_id, activity_revision)
          select e.session_id, 1 from buffered_events e
          where e.rowid = old.raw_rowid and ${oldOutboxChange}
          on conflict(session_id) do update set activity_revision = activity_revision + 1;
        insert into session_sync_summary_activity (session_id, activity_revision)
          select e.session_id, 1 from buffered_events e
          where e.rowid = new.raw_rowid and ${newOutboxChange}
          on conflict(session_id) do update set activity_revision = activity_revision + 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = old.raw_rowid and e.session_id is not null
            and ${oldOutboxChange} and not ${linkedUnscanned}
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = new.raw_rowid and e.session_id is not null
            and ${newOutboxChange} and not ${linkedUnscanned}
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
      end;
      create trigger if not exists trg_session_summary_outbox_delete_v42
      after delete on upload_outbox
      when old.raw_rowid is not null and exists (
        select 1 from buffered_events e
        join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = old.raw_rowid and ${oldOutboxMismatch}
      )
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_activity (session_id, activity_revision)
          select e.session_id, 1 from buffered_events e where e.rowid = old.raw_rowid
          on conflict(session_id) do update set activity_revision = activity_revision + 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = old.raw_rowid and e.session_id is not null
            and not ${linkedUnscanned}
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
      end;
    `);
    db.exec(`
      create trigger if not exists trg_session_summary_repair_outbox_insert_v1
      after insert on upload_outbox
      when new.raw_rowid is not null and exists (
        select 1 from buffered_events e join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = new.raw_rowid and ${newOutboxMismatch})
      begin ${queueLinkedRepair(`e.rowid = new.raw_rowid and ${newOutboxMismatch}`)} end;
      create trigger if not exists trg_session_summary_repair_outbox_update_v1
      after update of raw_rowid, raw_id, raw_created_at, raw_generation on upload_outbox
      when ${oldOutboxAffects} or ${newOutboxAffects}
      begin
        ${queueLinkedRepair(`e.rowid = old.raw_rowid and ${oldOutboxChange}`)}
        ${queueLinkedRepair(`e.rowid = new.raw_rowid and ${newOutboxChange}`)}
      end;
      create trigger if not exists trg_session_summary_repair_outbox_delete_v1
      after delete on upload_outbox
      when old.raw_rowid is not null and exists (
        select 1 from buffered_events e join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = old.raw_rowid and ${oldOutboxMismatch})
      begin ${queueLinkedRepair(`e.rowid = old.raw_rowid and ${oldOutboxMismatch}`)} end;
    `);
  }

  if (tableExists(db, "upload_receipts")) {
    const receiptColumns = columnNames(db, "upload_receipts");
    if (receiptColumns.has("reason") && receiptColumns.has("delivery_id")) {
      db.exec(`
        create trigger if not exists trg_session_summary_receipt_insert_v42
        after insert on upload_receipts
        when new.reason in ('local_evidence_quarantined','local_privacy_violation')
          and exists (
            select 1 from buffered_events e
            join session_sync_summary_state s on s.session_id = e.session_id
            where e.id = new.delivery_id
          )
        begin
          update session_sync_summary_control
            set mutation_revision = mutation_revision + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            where singleton = 1;
          insert into session_sync_summary_activity (session_id, activity_revision)
            select e.session_id, 1 from buffered_events e where e.id = new.delivery_id
            on conflict(session_id) do update set activity_revision = activity_revision + 1;
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e where e.id = new.delivery_id and e.session_id is not null
              and not ${linkedUnscanned}
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
        end;
        create trigger if not exists trg_session_summary_receipt_update_v42
        after update of delivery_id, reason on upload_receipts
        when ${oldReceiptAffects} or ${newReceiptAffects}
        begin
          update session_sync_summary_control
            set mutation_revision = mutation_revision + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            where singleton = 1;
          insert into session_sync_summary_activity (session_id, activity_revision)
            select e.session_id, 1 from buffered_events e
            where e.id = old.delivery_id and ${oldReceiptChange}
            on conflict(session_id) do update set activity_revision = activity_revision + 1;
          insert into session_sync_summary_activity (session_id, activity_revision)
            select e.session_id, 1 from buffered_events e
            where e.id = new.delivery_id and ${newReceiptChange}
            on conflict(session_id) do update set activity_revision = activity_revision + 1;
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e where e.id = old.delivery_id and e.session_id is not null
              and ${oldReceiptChange} and not ${linkedUnscanned}
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e where e.id = new.delivery_id and e.session_id is not null
              and ${newReceiptChange} and not ${linkedUnscanned}
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
        end;
        create trigger if not exists trg_session_summary_receipt_delete_v42
        after delete on upload_receipts
        when old.reason in ('local_evidence_quarantined','local_privacy_violation')
          and exists (
            select 1 from buffered_events e
            join session_sync_summary_state s on s.session_id = e.session_id
            where e.id = old.delivery_id
          )
        begin
          update session_sync_summary_control
            set mutation_revision = mutation_revision + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            where singleton = 1;
          insert into session_sync_summary_activity (session_id, activity_revision)
            select e.session_id, 1 from buffered_events e where e.id = old.delivery_id
            on conflict(session_id) do update set activity_revision = activity_revision + 1;
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e where e.id = old.delivery_id and e.session_id is not null
              and not ${linkedUnscanned}
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
        end;
        -- The 0.7.41 update trigger marks only the new delivery_id. Keep
        -- its revision fence for the old side when a terminal id is retargeted.
        create trigger if not exists trg_session_summary_receipt_retarget_revision_v42
        after update of delivery_id, reason on upload_receipts
        when ${oldReceiptAffects}
        begin
          insert into session_sync_summary_revision (session_id, mutation_revision)
            select e.session_id, 1 from buffered_events e
            join session_sync_summary_state s on s.session_id = e.session_id
            where e.id = old.delivery_id and ${oldReceiptChange}
            on conflict(session_id) do update set mutation_revision = mutation_revision + 1;
        end;
      `);
      db.exec(`
        create trigger if not exists trg_session_summary_repair_receipt_insert_v1
        after insert on upload_receipts
        when new.reason in ('local_evidence_quarantined','local_privacy_violation')
          and exists (select 1 from buffered_events e
            join session_sync_summary_state s on s.session_id = e.session_id
            where e.id = new.delivery_id)
        begin ${queueLinkedRepair("e.id = new.delivery_id")} end;
        create trigger if not exists trg_session_summary_repair_receipt_update_v1
        after update of delivery_id, reason on upload_receipts
        when ${oldReceiptAffects} or ${newReceiptAffects}
        begin
          ${queueLinkedRepair(`e.id = old.delivery_id and ${oldReceiptChange}`)}
          ${queueLinkedRepair(`e.id = new.delivery_id and ${newReceiptChange}`)}
        end;
        create trigger if not exists trg_session_summary_repair_receipt_delete_v1
        after delete on upload_receipts
        when old.reason in ('local_evidence_quarantined','local_privacy_violation')
          and exists (select 1 from buffered_events e
            join session_sync_summary_state s on s.session_id = e.session_id
            where e.id = old.delivery_id)
        begin ${queueLinkedRepair("e.id = old.delivery_id")} end;
      `);
    }
  }
}

function control(db: Database.Database): ControlRow {
  const row = db.prepare(
    `select mutation_revision as mutationRevision, fallback_recomputes as fallbackRecomputes
     from session_sync_summary_control where singleton = 1`,
  ).get() as ControlRow | undefined;
  if (!row) throw new Error("session_summary_control_missing");
  return row;
}

function sessionRevision(db: Database.Database, sessionId: string): number {
  const row = db.prepare(
    `select mutation_revision as mutationRevision
     from session_sync_summary_revision where session_id = ?`,
  ).get(sessionId) as { mutationRevision: number } | undefined;
  return row?.mutationRevision ?? 0;
}

function sessionActivityRevision(db: Database.Database, sessionId: string): number {
  const row = db.prepare(
    `select activity_revision as activityRevision
     from session_sync_summary_activity where session_id = ?`,
  ).get(sessionId) as { activityRevision: number } | undefined;
  return row?.activityRevision ?? 0;
}

export function sessionSummaryCounters(db: Database.Database): SessionSummaryCounters {
  ensureSessionSummarySchema(db);
  return control(db);
}

function emptyAggregate(): SummaryAggregate {
  return {
    futureRows: false,
    futureCreatedAt: null,
    sourceMax: null,
    startedAt: null,
    endedAt: null,
    events: 0,
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    cacheCreationTokens: 0,
    pricedEvents: 0,
    costUsd: 0,
    costCompensation: 0,
    repoNonNull: 0,
    repoValue: null,
    repoMixed: false,
    branchNonNull: 0,
    branchValue: null,
    branchMixed: false,
    accountNonNull: 0,
    accountValue: null,
    accountMixed: false,
  };
}

function emptyAccumulator(sessionId: string): SummaryAccumulator {
  return {
    ...emptyAggregate(), sessionId, scanBoundary: 0,
    cursorObservedAt: null, cursorRowid: 0, cursorId: null,
    checkpointSearchRowid: null,
    segments: {}, scanComplete: false, activeRepair: null,
  };
}

function segmentOf(rowid: number): number {
  return Math.floor((rowid - 1) / SESSION_SUMMARY_SEGMENT_ROWS);
}

function trackFuture(aggregate: SummaryAggregate, createdAt: string): void {
  aggregate.futureRows = true;
  if (aggregate.futureCreatedAt === null || createdAt < aggregate.futureCreatedAt) {
    aggregate.futureCreatedAt = createdAt;
  }
}

function foldIdentity(
  count: number,
  value: string | null,
  mixed: boolean,
  next: string | null,
): { count: number; value: string | null; mixed: boolean } {
  if (next === null) return { count, value, mixed };
  if (count === 0) return { count: 1, value: next, mixed: false };
  return { count: count + 1, value, mixed: mixed || value !== next };
}

function fold(accumulator: SummaryAggregate, row: RawSummaryRow): void {
  accumulator.events += 1;
  accumulator.inputTokens += row.inputTokens ?? 0;
  accumulator.outputTokens += row.outputTokens ?? 0;
  accumulator.cacheReadTokens += row.cacheReadTokens ?? 0;
  accumulator.cacheCreationTokens += row.cacheCreationTokens ?? 0;
  if (row.costUsd !== null) {
    accumulator.pricedEvents += 1;
    // SQLite's built-in sum() uses a compensated floating-point accumulator.
    // Keep the same Neumaier correction across persisted slices so a restart
    // or a late row serializes the same number as the full recompute.
    const next = accumulator.costUsd + row.costUsd;
    accumulator.costCompensation += Math.abs(accumulator.costUsd) >= Math.abs(row.costUsd)
      ? (accumulator.costUsd - next) + row.costUsd
      : (row.costUsd - next) + accumulator.costUsd;
    accumulator.costUsd = next;
  }
  accumulator.sourceMax = accumulator.sourceMax === null
    ? row.source
    : accumulator.sourceMax > row.source ? accumulator.sourceMax : row.source;
  accumulator.startedAt = accumulator.startedAt === null || row.observedAt < accumulator.startedAt
    ? row.observedAt : accumulator.startedAt;
  accumulator.endedAt = accumulator.endedAt === null || row.observedAt > accumulator.endedAt
    ? row.observedAt : accumulator.endedAt;

  const repo = foldIdentity(accumulator.repoNonNull, accumulator.repoValue, accumulator.repoMixed, row.repoHash);
  accumulator.repoNonNull = repo.count;
  accumulator.repoValue = repo.value;
  accumulator.repoMixed = repo.mixed;
  const branch = foldIdentity(accumulator.branchNonNull, accumulator.branchValue, accumulator.branchMixed, row.branchHash);
  accumulator.branchNonNull = branch.count;
  accumulator.branchValue = branch.value;
  accumulator.branchMixed = branch.mixed;
  const account = foldIdentity(accumulator.accountNonNull, accumulator.accountValue, accumulator.accountMixed, row.accountHash);
  accumulator.accountNonNull = account.count;
  accumulator.accountValue = account.value;
  accumulator.accountMixed = account.mixed;
}

function addCost(accumulator: SummaryAggregate, cost: number): void {
  const next = accumulator.costUsd + cost;
  accumulator.costCompensation += Math.abs(accumulator.costUsd) >= Math.abs(cost)
    ? (accumulator.costUsd - next) + cost
    : (cost - next) + accumulator.costUsd;
  accumulator.costUsd = next;
}

function mergeAggregate(target: SummaryAggregate, source: SummaryAggregate): void {
  target.events += source.events;
  target.inputTokens += source.inputTokens;
  target.outputTokens += source.outputTokens;
  target.cacheReadTokens += source.cacheReadTokens;
  target.cacheCreationTokens += source.cacheCreationTokens;
  target.pricedEvents += source.pricedEvents;
  addCost(target, source.costUsd);
  addCost(target, source.costCompensation);
  if (source.sourceMax !== null && (target.sourceMax === null || source.sourceMax > target.sourceMax)) {
    target.sourceMax = source.sourceMax;
  }
  if (source.startedAt !== null && (target.startedAt === null || source.startedAt < target.startedAt)) {
    target.startedAt = source.startedAt;
  }
  if (source.endedAt !== null && (target.endedAt === null || source.endedAt > target.endedAt)) {
    target.endedAt = source.endedAt;
  }
  for (const prefix of ["repo", "branch", "account"] as const) {
    const countKey = `${prefix}NonNull` as const;
    const valueKey = `${prefix}Value` as const;
    const mixedKey = `${prefix}Mixed` as const;
    const currentCount = target[countKey];
    target[mixedKey] = target[mixedKey] || source[mixedKey] ||
      (currentCount > 0 && source[countKey] > 0 && target[valueKey] !== source[valueKey]);
    if (currentCount === 0) target[valueKey] = source[valueKey];
    target[countKey] += source[countKey];
  }
  if (source.futureCreatedAt !== null) trackFuture(target, source.futureCreatedAt);
  else target.futureRows ||= source.futureRows;
}

function combineSegments(segments: Record<string, SummaryAggregate>): SummaryAggregate {
  const combined = emptyAggregate();
  for (const key of Object.keys(segments).sort((a, b) => Number(a) - Number(b))) {
    mergeAggregate(combined, segments[key]!);
  }
  return combined;
}

function refreshAggregate(accumulator: SummaryAccumulator): void {
  Object.assign(accumulator, combineSegments(accumulator.segments));
}

function snapshot(accumulator: SummaryAccumulator): SessionSnapshot | null {
  if (accumulator.events === 0 || accumulator.sourceMax === null || accumulator.startedAt === null || accumulator.endedAt === null) {
    return null;
  }
  const repoAll = accumulator.repoNonNull === accumulator.events && !accumulator.repoMixed;
  const branchAll = repoAll && accumulator.branchNonNull === accumulator.events && !accumulator.branchMixed;
  return {
    sessionId: accumulator.sessionId,
    source: accumulator.sourceMax,
    startedAt: accumulator.startedAt,
    endedAt: accumulator.endedAt,
    events: accumulator.events,
    inputTokens: accumulator.inputTokens,
    outputTokens: accumulator.outputTokens,
    cacheReadTokens: accumulator.cacheReadTokens,
    cacheCreationTokens: accumulator.cacheCreationTokens,
    pricedEvents: accumulator.pricedEvents,
    costUsd: accumulator.costUsd + accumulator.costCompensation,
    repoHash: repoAll ? accumulator.repoValue : null,
    branchHash: branchAll ? accumulator.branchValue : null,
    accountHash: accumulator.accountNonNull === accumulator.events && !accumulator.accountMixed
      ? accumulator.accountValue : null,
  };
}

function validAggregate(candidate: SummaryAggregate): boolean {
  if (typeof candidate.futureRows !== "boolean") return false;
  if (typeof candidate.futureCreatedAt !== "string" && candidate.futureCreatedAt !== null) return false;
  if (!Number.isSafeInteger(candidate.events) || candidate.events < 0) return false;
  for (const key of [
    "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens",
    "pricedEvents", "repoNonNull", "branchNonNull", "accountNonNull",
  ] as const) {
    if (!Number.isSafeInteger(candidate[key]) || candidate[key] < 0) return false;
  }
  if (!Number.isFinite(candidate.costUsd) || !Number.isFinite(candidate.costCompensation)) return false;
  for (const key of ["sourceMax", "startedAt", "endedAt", "repoValue", "branchValue", "accountValue"] as const) {
    if (typeof candidate[key] !== "string" && candidate[key] !== null) return false;
  }
  for (const key of ["repoMixed", "branchMixed", "accountMixed"] as const) {
    if (typeof candidate[key] !== "boolean") return false;
  }
  return true;
}

function parseAccumulator(sessionId: string, value: string): SummaryAccumulator | null {
  try {
    const parsed = JSON.parse(value) as Partial<SummaryAccumulator>;
    const candidate = { ...emptyAccumulator(sessionId), ...parsed } as SummaryAccumulator;
    if (candidate.sessionId !== sessionId) return null;
    if (typeof parsed.scanBoundary !== "number" ||
        !Number.isSafeInteger(parsed.scanBoundary) || parsed.scanBoundary < 0) return null;
    if (typeof candidate.cursorObservedAt !== "string" && candidate.cursorObservedAt !== null) return null;
    if (!Number.isSafeInteger(candidate.cursorRowid) || candidate.cursorRowid < 0) return null;
    if (typeof candidate.cursorId !== "string" && candidate.cursorId !== null) return null;
    if (candidate.checkpointSearchRowid !== null &&
        (!Number.isSafeInteger(candidate.checkpointSearchRowid) || candidate.checkpointSearchRowid < 0)) return null;
    if (!validAggregate(candidate)) return null;
    if (typeof candidate.scanComplete !== "boolean") return null;
    if (!candidate.segments || typeof candidate.segments !== "object" || Array.isArray(candidate.segments)) return null;
    for (const [key, aggregate] of Object.entries(candidate.segments)) {
      const segment = Number(key);
      if (!Number.isSafeInteger(segment) || segment < 0 || !aggregate || !validAggregate(aggregate)) return null;
    }
    if (candidate.activeRepair !== null) {
      const repair = candidate.activeRepair;
      if (!repair || !Number.isSafeInteger(repair.segment) || repair.segment < 0 ||
          !Number.isSafeInteger(repair.revision) || repair.revision < 0 ||
          typeof repair.until !== "string" || Number.isNaN(Date.parse(repair.until)) ||
          !Number.isSafeInteger(repair.cursorRowid) || repair.cursorRowid < 0 ||
          !repair.aggregate || !validAggregate(repair.aggregate)) return null;
    }
    return candidate;
  } catch {
    return null;
  }
}

function storedState(db: Database.Database, sessionId: string): StoredSummaryState | null {
  const row = db.prepare(
    `select session_id as sessionId, state_generation as stateGeneration,
       schema_version as schemaVersion, high_water as highWater,
       checkpoint_id as checkpointId, covered_until as coveredUntil, complete,
       mutation_revision as mutationRevision, mode, accumulator_json as accumulatorJson
     from session_sync_summary_state where session_id = ?`,
  ).get(sessionId) as (Omit<StoredSummaryState, "complete"> & { complete: number }) | undefined;
  if (!row) return null;
  return { ...row, complete: Boolean(row.complete), mode: row.mode } as StoredSummaryState;
}

function currentStateGeneration(db: Database.Database, sessionId: string): number | null {
  const row = db.prepare(`select state_generation as stateGeneration
    from session_sync_summary_state
    where session_id = ?`)
    .get(sessionId) as { stateGeneration: number } | undefined;
  return row?.stateGeneration ?? null;
}

type PendingSummaryRow = {
  reason: string;
  mutationRevision: number;
  queuedHighWater: number;
  consecutiveZeroProgress: number;
  nextRetryAt: string | null;
};

function pendingSummaryRow(db: Database.Database, sessionId: string): PendingSummaryRow | null {
  return db.prepare(`select reason, mutation_revision as mutationRevision,
      queued_high_water as queuedHighWater, consecutive_zero_progress as consecutiveZeroProgress,
      next_retry_at as nextRetryAt from session_sync_summary_pending where session_id = ?`)
    .get(sessionId) as PendingSummaryRow | undefined ?? null;
}

function queuedSummaryHighWater(db: Database.Database, sessionId: string): number {
  const row = db.prepare(`select max(raw_rowid) as highWater
    from session_sync_summary_rows where session_id = ?`).get(sessionId) as { highWater: number | null };
  return row.highWater ?? 0;
}

function validStoredState(state: StoredSummaryState, sessionId: string, until: string): boolean {
  return state.sessionId === sessionId &&
    Number.isSafeInteger(state.stateGeneration) && state.stateGeneration >= 0 &&
    state.schemaVersion === SESSION_SUMMARY_SCHEMA_VERSION &&
    Number.isSafeInteger(state.highWater) && state.highWater >= 0 &&
    (typeof state.checkpointId === "string" || state.checkpointId === null) &&
    typeof state.coveredUntil === "string" &&
    !Number.isNaN(Date.parse(state.coveredUntil)) &&
    !Number.isNaN(Date.parse(until)) &&
    typeof state.complete === "boolean" &&
    Number.isSafeInteger(state.mutationRevision) && state.mutationRevision >= 0 &&
    (state.mode === "initial" || state.mode === "incremental" || state.mode === "fallback");
}

function rowCheckpointQuery(state: StoredSummaryState, maxMs?: number): SessionReadQuery[] {
  return [{
    sql: `select id, session_id as sessionId, observed_at as observedAt
      from buffered_events where rowid = @rowid`,
    params: { rowid: state.highWater },
    ...(maxMs === undefined ? {} : { maxMs }),
  }];
}

async function checkpointValid(
  state: StoredSummaryState,
  accumulator: SummaryAccumulator,
  sessionId: string,
  read: SessionSummaryRead,
  maxMs?: number,
  cursorSegmentQueued = false,
  highWaterSegmentQueued = false,
): Promise<"valid" | "invalid" | "search_pending"> {
  if (state.highWater === 0) return state.checkpointId === null &&
    accumulator.cursorRowid === 0 && accumulator.cursorObservedAt === null && accumulator.cursorId === null
    ? "valid" : "invalid";
  if (state.checkpointId === null) return "invalid";
  let rows = await read<{ id: string; sessionId: string | null; observedAt: string }>(rowCheckpointQuery(state, maxMs));
  if ((rows.length === 0 || rows[0]?.id !== state.checkpointId ||
       rows[0]?.sessionId !== sessionId) && highWaterSegmentQueued && accumulator.scanComplete) {
    // Erasure, rowid reuse, and reattribution all remove this checkpoint from
    // the old session. Search one bounded rowid window per pass. The cursor is
    // persisted before the next cycle, so a distant predecessor never forces
    // one unbounded sort or repeats the same window after a read deadline.
    const erasedRowid = state.highWater;
    const upperRowid = accumulator.checkpointSearchRowid ?? erasedRowid - 1;
    const lowerRowid = Math.max(1, upperRowid - SESSION_SUMMARY_SEGMENT_ROWS + 1);
    const prior = upperRowid > 0
      ? await read<{ rowid: number; id: string; sessionId: string; observedAt: string }>([{
        sql: `select rowid, id, session_id as sessionId, observed_at as observedAt
          from buffered_events not indexed
          where rowid >= @lowerRowid and rowid <= @upperRowid and session_id = @sessionId
          order by rowid desc limit 1`,
        params: { lowerRowid, upperRowid, sessionId },
        ...(maxMs === undefined ? {} : { maxMs }),
      }]) : [];
    if (prior.length === 0 && lowerRowid > 1) {
      accumulator.checkpointSearchRowid = lowerRowid - 1;
      return "search_pending";
    }
    accumulator.checkpointSearchRowid = null;
    state.highWater = prior[0]?.rowid ?? 0;
    state.checkpointId = prior[0]?.id ?? null;
    if (accumulator.cursorRowid === erasedRowid || state.highWater === 0) {
      accumulator.cursorRowid = 0;
      accumulator.cursorId = null;
      accumulator.cursorObservedAt = null;
    }
    if (state.highWater === 0) return "valid";
    rows = prior;
  }
  if (rows.length !== 1 || rows[0]?.id !== state.checkpointId || rows[0]?.sessionId !== sessionId) return "invalid";
  accumulator.checkpointSearchRowid = null;
  if (accumulator.cursorRowid === 0) return accumulator.cursorObservedAt === null && accumulator.cursorId === null
    ? "valid" : "invalid";
  if (accumulator.cursorObservedAt === null || accumulator.cursorId === null) return "invalid";
  if (accumulator.cursorRowid === state.highWater) {
    if (rows[0]?.id !== accumulator.cursorId) return "invalid";
    if (rows[0]?.observedAt === accumulator.cursorObservedAt) return "valid";
    if (!cursorSegmentQueued) return "invalid";
    // A completed historical scan no longer seeks by this timestamp. The
    // trigger queued its segment, so repair that row and persist the new
    // checkpoint timestamp instead of discarding the whole prefix.
    accumulator.cursorObservedAt = rows[0].observedAt;
    return "valid";
  }
  const cursorRows = await read<{ id: string; sessionId: string | null; observedAt: string }>([{
    sql: `select id, session_id as sessionId, observed_at as observedAt
      from buffered_events where rowid = @rowid`,
    params: { rowid: accumulator.cursorRowid },
    ...(maxMs === undefined ? {} : { maxMs }),
  }]);
  if (cursorRows.length === 0 && cursorSegmentQueued && accumulator.scanComplete) {
    // The completed historical scan no longer seeks from this cursor. Its
    // segment repair accounts for the erased row; discard the dead cursor.
    accumulator.cursorRowid = 0;
    accumulator.cursorId = null;
    accumulator.cursorObservedAt = null;
    return "valid";
  }
  if (cursorRows.length !== 1 || cursorRows[0]?.id !== accumulator.cursorId ||
      cursorRows[0]?.sessionId !== sessionId) return "invalid";
  if (cursorRows[0]?.observedAt === accumulator.cursorObservedAt) return "valid";
  if (!cursorSegmentQueued) return "invalid";
  accumulator.cursorObservedAt = cursorRows[0].observedAt;
  return "valid";
}

function summaryRowsQuery(
  db: Database.Database,
  sessionId: string,
  until: string,
  highWater: number,
  cursorObservedAt: string | null,
  cursorRowid: number,
  scanBoundary: number,
  limit: number,
  maxMs?: number,
  appendRows = false,
): SessionReadQuery {
  const eligible = terminalPrivacyEligibilitySql(db, "e");
  // SQLite cannot seek by rowid within a timestamp group for the tuple
  // predicate (observed_at, rowid) > (?, ?). Split the equal-timestamp and
  // later-timestamp ranges so a long session with many equal observations
  // still resumes in index order within the read deadline.
  const historicalRows = `(select e0.rowid as raw_rowid, e0.observed_at as sort_observed_at
      from buffered_events e0 indexed by idx_events_session
      where e0.session_id = @sessionId and e0.observed_at = @cursorObservedAt
        and e0.rowid > @cursorRowid and e0.rowid <= @scanBoundary
      union all
      select e0.rowid as raw_rowid, e0.observed_at as sort_observed_at
      from buffered_events e0 indexed by idx_events_session
      where e0.session_id = @sessionId and e0.observed_at > @cursorObservedAt
        and e0.rowid <= @scanBoundary
      order by sort_observed_at, raw_rowid limit @limit) scan
      join buffered_events e on e.rowid = scan.raw_rowid`;
  return {
    sql: `select e.rowid as rowid, e.id, e.session_id as sessionId, e.source,
       e.observed_at as observedAt, e.created_at as createdAt,
       e.input_tokens as inputTokens,
       e.output_tokens as outputTokens, e.cache_read_tokens as cacheReadTokens,
       e.cache_creation_tokens as cacheCreationTokens, e.cost_usd as costUsd,
       e.repo_hash as repoHash, e.branch_hash as branchHash,
       e.account_hash as accountHash,
       case when ${eligible} then 1 else 0 end as eligible
     from ${appendRows
       ? "session_sync_summary_rows r join buffered_events e on e.rowid = r.raw_rowid"
       : historicalRows}
     ${appendRows ? "where r.session_id = @sessionId and r.raw_rowid > @highWater" : ""}
     order by ${appendRows ? "e.rowid" : "scan.sort_observed_at, scan.raw_rowid"} asc limit @limit`,
    params: { sessionId, highWater, cursorObservedAt: cursorObservedAt ?? "", cursorRowid, scanBoundary, limit },
    ...(maxMs === undefined ? {} : { maxMs }),
  };
}

function newerSummaryRowQuery(
  sessionId: string,
  until: string,
  highWater: number,
  maxMs?: number,
  appendRows = false,
): SessionReadQuery {
  const cursor = appendRows
    ? "r.session_id = @sessionId and r.raw_rowid > @highWater"
    : "e.session_id = @sessionId and e.rowid > @highWater";
  return {
    sql: `select 1 as present from ${appendRows
      ? "session_sync_summary_rows r join buffered_events e on e.rowid = r.raw_rowid"
      : "buffered_events e"}
      where ${cursor} and e.created_at <= @until
      limit 1`,
    params: { sessionId, until, highWater },
    ...(maxMs === undefined ? {} : { maxMs }),
  };
}

function repairQueueRow(db: Database.Database, sessionId: string, segment?: number):
  { segment: number; revision: number } | null {
  const row = segment === undefined
    ? db.prepare(`select segment, revision from session_sync_summary_repairs
       where session_id = ? order by segment limit 1`).get(sessionId)
    : db.prepare(`select segment, revision from session_sync_summary_repairs
       where session_id = ? and segment = ?`).get(sessionId, segment);
  return (row as { segment: number; revision: number } | undefined) ?? null;
}

function dueSegment(accumulator: SummaryAccumulator, until: string): number | null {
  for (const key of Object.keys(accumulator.segments).sort((a, b) => Number(a) - Number(b))) {
    const date = accumulator.segments[key]?.futureCreatedAt;
    if (date !== null && date !== undefined && date <= until) return Number(key);
  }
  return null;
}

function repairRowsQuery(
  db: Database.Database, sessionId: string, repair: SummaryRepair, highWater: number,
  limit: number, maxMs: number,
): SessionReadQuery {
  const eligible = terminalPrivacyEligibilitySql(db, "e");
  return {
    sql: `select e.rowid as rowid, e.id, e.session_id as sessionId, e.source,
       e.observed_at as observedAt, e.created_at as createdAt,
       e.input_tokens as inputTokens, e.output_tokens as outputTokens,
       e.cache_read_tokens as cacheReadTokens,
       e.cache_creation_tokens as cacheCreationTokens, e.cost_usd as costUsd,
       e.repo_hash as repoHash, e.branch_hash as branchHash,
       e.account_hash as accountHash,
       case when ${eligible} then 1 else 0 end as eligible
     from buffered_events e
     where e.rowid > @cursorRowid and e.rowid <= @upperRowid and e.session_id = @sessionId
     order by e.rowid limit @limit`,
    params: { sessionId, cursorRowid: repair.cursorRowid,
      upperRowid: Math.min(highWater, (repair.segment + 1) * SESSION_SUMMARY_SEGMENT_ROWS), limit },
    maxMs,
  };
}

function writeState(db: Database.Database, state: SummaryState): void {
  const nextGeneration = state.stateGeneration + 1;
  db.prepare(
    `insert into session_sync_summary_state
       (session_id, state_generation, schema_version, high_water, checkpoint_id, covered_until, complete,
        mutation_revision, mode, accumulator_json, updated_at)
     values (@sessionId, @stateGeneration, @schemaVersion, @highWater, @checkpointId, @coveredUntil, @complete,
       @mutationRevision, @mode, @accumulatorJson, @updatedAt)
     on conflict(session_id) do update set
       state_generation=excluded.state_generation,
       schema_version=excluded.schema_version, high_water=excluded.high_water,
       checkpoint_id=excluded.checkpoint_id, covered_until=excluded.covered_until,
       complete=excluded.complete, mutation_revision=excluded.mutation_revision,
       mode=excluded.mode, accumulator_json=excluded.accumulator_json,
       updated_at=excluded.updated_at`,
  ).run({
    sessionId: state.sessionId,
    stateGeneration: nextGeneration,
    schemaVersion: state.schemaVersion,
    highWater: state.highWater,
    checkpointId: state.checkpointId,
    coveredUntil: state.coveredUntil,
    complete: state.complete ? 1 : 0,
    mutationRevision: state.mutationRevision,
    mode: state.mode,
    accumulatorJson: JSON.stringify(state.accumulator),
    updatedAt: new Date().toISOString(),
  });
  state.stateGeneration = nextGeneration;
  if (state.accumulator.futureCreatedAt === null) {
    db.prepare("delete from session_sync_summary_due where session_id = ?").run(state.sessionId);
  } else {
    db.prepare(`insert into session_sync_summary_due (session_id, next_due_at)
      values (?, ?) on conflict(session_id) do update set next_due_at = excluded.next_due_at`)
      .run(state.sessionId, state.accumulator.futureCreatedAt);
  }
}

function stateFromStored(stored: StoredSummaryState, accumulator: SummaryAccumulator): SummaryState {
  return {
    sessionId: stored.sessionId,
    stateGeneration: stored.stateGeneration,
    schemaVersion: stored.schemaVersion,
    highWater: stored.highWater,
    checkpointId: stored.checkpointId,
    coveredUntil: stored.coveredUntil,
    complete: stored.complete,
    mutationRevision: stored.mutationRevision,
    mode: stored.mode,
    accumulator,
  };
}

function fallbackReason(
  stored: StoredSummaryState | null,
  parsed: SummaryAccumulator | null,
  currentRevision: number,
  until: string,
  checkpointOk: boolean,
  dirtyReason: string | null,
  repairAvailable: boolean,
): string | null {
  if (!stored) return null;
  if (!parsed) return "accumulator_corrupt";
  if (stored.schemaVersion !== SESSION_SUMMARY_SCHEMA_VERSION) return "schema_version";
  if (!Number.isSafeInteger(stored.highWater) || stored.highWater < 0) return "high_water_invalid";
  if (Number.isNaN(Date.parse(stored.coveredUntil))) return "covered_until_invalid";
  if (Date.parse(stored.coveredUntil) > Date.parse(until)) return "until_rollback";
  // A skipped future row only invalidates the scanned prefix when it actually
  // enters the new horizon. Old 0.7.40 states lack the date and rebuild once.
  if (stored.schemaVersion < 4 && parsed.futureRows && Date.parse(stored.coveredUntil) < Date.parse(until) &&
      (parsed.futureCreatedAt === null || parsed.futureCreatedAt <= until)) return "future_horizon";
  if (currentRevision !== stored.mutationRevision && !repairAvailable) return "ledger_mutation";
  if (!checkpointOk) return "checkpoint_mismatch";
  if (dirtyReason !== null && !repairAvailable) return "dirty_marker";
  return null;
}

/** Session ids whose durable summaries need another bounded pass. */
export function listSessionSummaryPendingIds(db: Database.Database, until: string, maxIds = 8_000): string[] {
  if (!tableExists(db, "session_sync_summary_state") || !tableExists(db, "session_sync_summary_dirty")) return [];
  const limit = Math.max(1, Math.min(Math.trunc(maxIds), 8_000));
  // A ledger upgraded from before the pending table existed keeps its lazy
  // summary migrations; read the pending reasons only once the table is there.
  const pendingUnion = tableExists(db, "session_sync_summary_pending")
    ? `select session_id as sessionId from session_sync_summary_pending
       where ${BOUNDED_SQL_READ_PREDICATE}
     union
     ` : "";
  const repairs = tableExists(db, "session_sync_summary_repairs")
    ? `union select session_id as sessionId from session_sync_summary_repairs
       where ${BOUNDED_SQL_READ_PREDICATE}` : "";
  const due = tableExists(db, "session_sync_summary_due")
    ? `union select session_id as sessionId from session_sync_summary_due
       where next_due_at <= @until and ${BOUNDED_SQL_READ_PREDICATE}` : "";
  const rows = boundedSqlRows<{ sessionId: string }>(db,
    `select session_id as sessionId from session_sync_summary_dirty
       where ${BOUNDED_SQL_READ_PREDICATE}
     union
     ${pendingUnion}     select session_id as sessionId from session_sync_summary_state
       where complete = 0 and ${BOUNDED_SQL_READ_PREDICATE}
     union
     select r.session_id as sessionId from session_sync_summary_revision r
       left join session_sync_summary_state s on s.session_id = r.session_id
       where (s.session_id is null or r.mutation_revision != s.mutation_revision)
         and ${BOUNDED_SQL_READ_PREDICATE}
     union
     select r.session_id as sessionId from session_sync_summary_rows r
       join buffered_events e on e.rowid = r.raw_rowid
       where e.created_at <= @until and ${BOUNDED_SQL_READ_PREDICATE}
     ${repairs}
     ${due}
     limit @limit`,
    { until, limit: limit + 1 }, limit);
  return rows.map((row) => row.sessionId);
}

function queuedRowsAfter(db: Database.Database, sessionId: string, highWater: number, until: string): boolean {
  return Boolean(db.prepare(`select 1 from session_sync_summary_rows r
    join buffered_events e on e.rowid = r.raw_rowid
    where r.session_id = ? and r.raw_rowid > ? and e.created_at <= ? limit 1`)
    .get(sessionId, highWater, until));
}

/** Synchronous final fence used immediately before every network attempt. */
export function sessionSummaryCurrent(
  db: Database.Database,
  sessionId: string,
  until: string,
  mutationRevision: number,
  highWater: number,
): boolean {
  const state = storedState(db, sessionId);
  return state !== null && state.complete && state.highWater === highWater &&
    state.mutationRevision === mutationRevision && state.coveredUntil === until &&
    sessionRevision(db, sessionId) === mutationRevision &&
    !db.prepare("select 1 from session_sync_summary_dirty where session_id = ?").get(sessionId) &&
    !db.prepare("select 1 from session_sync_summary_repairs where session_id = ? limit 1").get(sessionId) &&
    !queuedRowsAfter(db, sessionId, highWater, until);
}

/**
 * Advance one session by a bounded number of raw rows. A state row is written
 * after every slice, so a process death resumes from the last committed HWM.
 * A summary is returned only after the complete prefix through `until` is
 * known; partial accumulators never reach the network.
 */
export async function updateSessionSummary(
  db: Database.Database,
  sessionId: string,
  until: string,
  options: SessionSummaryUpdateOptions,
): Promise<SessionSummaryUpdateResult> {
  return updateSessionSummaryAttempt(db, sessionId, until, options, 0);
}

async function updateSessionSummaryAttempt(
  db: Database.Database,
  sessionId: string,
  until: string,
  options: SessionSummaryUpdateOptions,
  generationRetries: number,
): Promise<SessionSummaryUpdateResult> {
  const started = performance.now();
  const writeRetry = options.writeRetry ?? new SyncStorageRetryController({ budgetMs: 1_000 });
  const requestedRows = options.maxRows ?? SESSION_SUMMARY_DEFAULT_MAX_ROWS;
  const requestedMs = options.maxMs ?? SESSION_SUMMARY_DEFAULT_MAX_MS;
  const maxRows = Math.max(1, Math.min(
    Math.trunc(Number.isFinite(requestedRows) ? requestedRows : SESSION_SUMMARY_DEFAULT_MAX_ROWS),
    SESSION_SUMMARY_DEFAULT_MAX_ROWS,
  ));
  const maxMs = Math.max(1, Math.min(
    Number.isFinite(requestedMs) ? requestedMs : SESSION_SUMMARY_DEFAULT_MAX_MS,
    SESSION_SUMMARY_DEFAULT_MAX_MS,
  ));
  const activityAtStart = sessionActivityRevision(db, sessionId);
  const currentRevision = sessionRevision(db, sessionId);
  const stored = storedState(db, sessionId);
  const stateGenerationAtStart = stored?.stateGeneration ?? null;
  const pending = pendingSummaryRow(db, sessionId);
  const queuedHighWaterAtStart = queuedSummaryHighWater(db, sessionId);
  const now = options.now ?? (() => new Date());
  const samePendingInput = () => pending?.mutationRevision === currentRevision &&
    pending.queuedHighWater === queuedHighWaterAtStart;
  const finish = async (result: SessionSummaryUpdateResult, zeroProgressRead = false) => {
    if (result.complete) {
      await writeRetry.run(() => db.prepare(
        "delete from session_sync_summary_pending where session_id = ?",
      ).run(sessionId));
      return result;
    }
    const consecutive = zeroProgressRead && samePendingInput()
      ? pending!.consecutiveZeroProgress : 0;
    const nextConsecutive = zeroProgressRead ? consecutive + 1 : 0;
    const reason = zeroProgressRead && nextConsecutive >= SESSION_SUMMARY_ZERO_PROGRESS_STUCK_AFTER
      ? "rows_read_stuck" : result.fallbackReason ?? `${result.mode}_in_progress`;
    const delayMs = zeroProgressRead
      ? Math.min(ZERO_PROGRESS_RETRY_MAX_MS, ZERO_PROGRESS_RETRY_BASE_MS * 2 ** Math.min(nextConsecutive - 1, 10))
      : null;
    const observedAt = now();
    await writeRetry.run(() => db.prepare(`insert into session_sync_summary_pending
        (session_id, reason, mutation_revision, queued_high_water,
          consecutive_zero_progress, next_retry_at, updated_at)
        values (?, ?, ?, ?, ?, ?, ?)
        on conflict(session_id) do update set reason = excluded.reason,
          mutation_revision = excluded.mutation_revision,
          queued_high_water = excluded.queued_high_water,
          consecutive_zero_progress = excluded.consecutive_zero_progress,
          next_retry_at = excluded.next_retry_at, updated_at = excluded.updated_at`)
      .run(sessionId, reason, currentRevision, queuedHighWaterAtStart, nextConsecutive,
        delayMs === null ? null : new Date(observedAt.getTime() + delayMs).toISOString(),
        observedAt.toISOString()));
    return { ...result, fallbackReason: reason };
  };
  const retryFromLatest = async (discardedRows: number): Promise<SessionSummaryUpdateResult> => {
    const elapsedMs = performance.now() - started;
    if (generationRetries === 0 && discardedRows < maxRows && elapsedMs < maxMs) {
      const retry = await updateSessionSummaryAttempt(db, sessionId, until, {
        ...options, maxRows: maxRows - discardedRows, maxMs: maxMs - elapsedMs,
      }, 1);
      return { ...retry, rowsRead: discardedRows + retry.rowsRead,
        durationMs: Math.round(performance.now() - started) };
    }
    return finish({
      snapshot: null, complete: false, rowsRead: discardedRows, rowsApplied: 0,
      durationMs: Math.round(elapsedMs), highWater: stored?.highWater ?? 0,
      mode: stored?.mode ?? "initial", fullRecompute: false,
      fallbackReason: "state_generation_changed", mutationRevision: currentRevision,
    });
  };
  if (pending?.nextRetryAt && samePendingInput() &&
      Date.parse(pending.nextRetryAt) > now().getTime()) {
    return {
      snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
      durationMs: Math.round(performance.now() - started), highWater: stored?.highWater ?? 0,
      mode: stored?.mode ?? "initial", fullRecompute: false, fallbackReason: pending.reason,
      mutationRevision: stored?.mutationRevision ?? currentRevision,
    };
  }
  const parsed = stored ? parseAccumulator(sessionId, stored.accumulatorJson) : null;
  const dirtyReason = (db.prepare(
    `select reason from session_sync_summary_dirty where session_id = ? limit 1`,
  ).get(sessionId) as { reason: string } | undefined)?.reason ?? null;
  const pendingRepair = repairQueueRow(db, sessionId);
  const cursorSegmentQueued = Boolean(parsed?.scanComplete && parsed.cursorRowid > 0 &&
    repairQueueRow(db, sessionId, segmentOf(parsed.cursorRowid)) !== null);
  const highWaterSegmentQueued = Boolean(parsed?.scanComplete && stored && stored.highWater > 0 &&
    repairQueueRow(db, sessionId, segmentOf(stored.highWater)) !== null);
  // A completed queued repair may leave the dirty marker in place while a
  // future row in its segment matures. Its next due repair is still bounded
  // work, provided the durable state already records this ledger revision.
  const repairAvailable = pendingRepair !== null || Boolean(stored && parsed &&
    stored.mutationRevision === currentRevision &&
    (parsed.activeRepair !== null || dueSegment(parsed, until) !== null));
  let checkpointStatus: "valid" | "invalid" | "search_pending" = "invalid";
  if (stored && parsed && validStoredState(stored, sessionId, until)) {
    try {
      checkpointStatus = await checkpointValid(stored, parsed, sessionId, options.read, maxMs,
        cursorSegmentQueued, highWaterSegmentQueued);
    } catch (error) {
      if (!(error instanceof Error && error.message.includes("session_summary_read_interrupted"))) throw error;
      // A worker deadline says nothing about checkpoint integrity. Preserve
      // the stored cursor and retry the bounded read on the next slice.
      return finish({
        snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
        durationMs: Math.round(performance.now() - started), highWater: stored.highWater,
        mode: stored.mode, fullRecompute: false, fallbackReason: "checkpoint_timeout",
        mutationRevision: stored.mutationRevision,
      });
    }
  }
  if (checkpointStatus === "search_pending" && stored && parsed) {
    const saved = await writeRetry.run(() => db.transaction(() => {
      if (currentStateGeneration(db, sessionId) !== stateGenerationAtStart ||
          sessionActivityRevision(db, sessionId) !== activityAtStart ||
          sessionRevision(db, sessionId) !== currentRevision) return false;
      const searching = stateFromStored(stored, parsed);
      searching.complete = false;
      writeState(db, searching);
      return true;
    }).immediate());
    if (!saved) return retryFromLatest(0);
    return finish({
      snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
      durationMs: Math.round(performance.now() - started), highWater: stored.highWater,
      mode: stored.mode, fullRecompute: false,
      fallbackReason: "checkpoint_search_in_progress", mutationRevision: stored.mutationRevision,
    });
  }
  const checkpointOk = checkpointStatus === "valid";
  const reason = fallbackReason(stored, parsed, currentRevision, until, checkpointOk,
    dirtyReason, repairAvailable);
  const resumableFallback = stored?.mode === "fallback" &&
    !stored.complete && (stored.mutationRevision === currentRevision || pendingRepair !== null) &&
    parsed !== null && checkpointOk && (reason === null || reason === "dirty_marker") &&
    Date.parse(stored.coveredUntil) <= Date.parse(until);
  const needsFallback = reason !== null && !resumableFallback;
  const fullRecompute = needsFallback;
  let mode: SessionSummaryUpdateResult["mode"] = needsFallback
    ? "fallback"
    : stored?.complete && pendingRepair === null && parsed?.activeRepair === null &&
        dueSegment(parsed, until) === null && Date.parse(stored.coveredUntil) === Date.parse(until)
      ? "cached"
      : stored ? "incremental" : "initial";

  let state: SummaryState;
  if (needsFallback || !stored) {
    // Freeze the old rowid range while installing the trigger-visible state.
    // Rows inserted afterward enter the append queue, even if their observed
    // time sorts behind the historical cursor.
    const installed = await writeRetry.run(() => db.transaction(() => {
      // A second connection may have replaced the state while checkpoint
      // validation ran. Never clear its repair queue or reset its prefix.
      if (currentStateGeneration(db, sessionId) !== stateGenerationAtStart) return null;
      if (needsFallback) {
        db.prepare(
          `update session_sync_summary_control
           set fallback_recomputes = fallback_recomputes + 1,
               updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
           where singleton = 1`,
        ).run();
      }
      const boundary = db.prepare(
        "select rowid from buffered_events order by rowid desc limit 1",
      ).get() as { rowid: number } | undefined;
      const accumulator = emptyAccumulator(sessionId);
      accumulator.scanBoundary = boundary?.rowid ?? 0;
      db.prepare("delete from session_sync_summary_repairs where session_id = ?").run(sessionId);
      const fresh: SummaryState = {
        sessionId,
        stateGeneration: stored?.stateGeneration ?? 0,
        schemaVersion: SESSION_SUMMARY_SCHEMA_VERSION,
        highWater: 0,
        checkpointId: null,
        coveredUntil: until,
        complete: false,
        mutationRevision: sessionRevision(db, sessionId),
        mode: needsFallback ? "fallback" : "initial",
        accumulator,
      };
      writeState(db, fresh);
      return fresh;
    }).immediate());
    if (installed === null) return retryFromLatest(0);
    state = installed;
  } else if (parsed) {
    state = stateFromStored(stored, parsed);
    // The repair queue is durable. A changed segment must be read before this
    // revision can be published, but it need not invalidate the old prefix.
    if (pendingRepair !== null) state.mutationRevision = currentRevision;
  } else {
    throw new Error("session_summary_accumulator_missing");
  }

  if (mode === "cached" && !needsFallback) {
    let newer: Array<{ present: number }> = [];
    let newerReadOk = true;
    try {
      newer = await options.read<{ present: number }>([
        newerSummaryRowQuery(
          sessionId,
          until,
          state.highWater,
          maxMs,
          state.mode === "incremental",
        ),
      ]);
    } catch (error) {
      if (!(error instanceof Error && error.message.includes("session_summary_read_interrupted"))) throw error;
      newerReadOk = false;
    }
    if (!newerReadOk) {
      return finish({
        snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
        durationMs: Math.round(performance.now() - started), highWater: state.highWater,
        mode: "cached", fullRecompute: false, fallbackReason: "newer_read_timeout",
        mutationRevision: state.mutationRevision,
      });
    }
    if (newer.length > 0 || sessionActivityRevision(db, sessionId) !== activityAtStart ||
        !sessionSummaryCurrent(db, sessionId, until, state.mutationRevision, state.highWater)) {
      mode = "incremental";
      state.complete = false;
    } else {
    return finish({
      snapshot: snapshot(state.accumulator),
      complete: true,
      rowsRead: 0,
      rowsApplied: 0,
      durationMs: Math.round(performance.now() - started),
      highWater: state.highWater,
      mode,
      fullRecompute: false,
      fallbackReason: null,
      mutationRevision: state.mutationRevision,
    });
    }
  }

  // A stored state from an older run may have been written for a newer
  // horizon. The fallback above handles it; this guard keeps malformed dates
  // from ever becoming a silently advancing cursor.
  if (Date.parse(state.coveredUntil) > Date.parse(until) || Number.isNaN(Date.parse(until))) {
    throw new Error("session_summary_until_invalid");
  }
  state.coveredUntil = until;
  const queryLimit = Math.min(maxRows, 5_000);
  let rowsRead = 0;
  let rowsApplied = 0;
  let complete = state.accumulator.scanComplete && state.mode !== "incremental";
  let readInterrupted = false;
  while (!complete && rowsRead < maxRows && performance.now() - started < maxMs) {
    const limit = Math.min(queryLimit, maxRows - rowsRead);
    let rows: RawSummaryRow[];
    try {
      rows = await options.read<RawSummaryRow>([
        summaryRowsQuery(
          db,
          sessionId,
          until,
          state.highWater,
          state.accumulator.cursorObservedAt,
          state.accumulator.cursorRowid,
          state.accumulator.scanBoundary,
          limit,
          Math.max(1, maxMs - (performance.now() - started)),
          state.mode === "incremental",
        ),
      ]);
    } catch (error) {
      if (error instanceof Error && error.message.includes("session_summary_read_interrupted")) {
        readInterrupted = true;
        break;
      }
      throw error;
    }
    if (rows.length === 0) {
      complete = true;
      break;
    }
    let processedAllChunk = true;
    for (const row of rows) {
      rowsRead += 1;
      if (state.mode !== "incremental") {
        state.accumulator.cursorObservedAt = row.observedAt;
        state.accumulator.cursorRowid = row.rowid;
        state.accumulator.cursorId = row.id;
      }
      if (row.rowid > state.highWater) {
        state.highWater = row.rowid;
        state.checkpointId = row.id;
      }
      const segment = String(segmentOf(row.rowid));
      const aggregate = state.accumulator.segments[segment] ?? emptyAggregate();
      state.accumulator.segments[segment] = aggregate;
      if (row.createdAt > until) {
        trackFuture(aggregate, row.createdAt);
      } else if (row.eligible) {
        fold(aggregate, row);
        rowsApplied += 1;
      }
      if (rowsRead >= maxRows || performance.now() - started >= maxMs) {
        processedAllChunk = rowsRead >= maxRows ? true : false;
        break;
      }
    }
    if (rows.length < limit && processedAllChunk) {
      complete = true;
      break;
    }
  }
  if (complete) state.accumulator.scanComplete = true;

  // Historical reads may visit segments in observation order. Scanned edits
  // are queued until that walk ends; replacing a segment earlier would let a
  // later historical slice count its rows twice. A repair itself is bounded
  // by both the row and time budgets and can resume from a durable cursor.
  let completedRepair: { segment: number; revision: number } | null = null;
  if (complete && !readInterrupted && rowsRead < maxRows && performance.now() - started < maxMs) {
    const queued = repairQueueRow(db, sessionId);
    const nextSegment = state.accumulator.activeRepair?.segment ??
      queued?.segment ?? dueSegment(state.accumulator, until);
    if (nextSegment !== null) {
      const queueRevision = repairQueueRow(db, sessionId, nextSegment)?.revision ?? 0;
      let repair = state.accumulator.activeRepair;
      if (repair === null || repair.segment !== nextSegment || repair.revision !== queueRevision) {
        repair = {
          segment: nextSegment, revision: queueRevision, until,
          cursorRowid: nextSegment * SESSION_SUMMARY_SEGMENT_ROWS,
          aggregate: emptyAggregate(),
        };
      }
      const limit = maxRows - rowsRead;
      try {
        const repairRows = await options.read<RawSummaryRow>([
          repairRowsQuery(db, sessionId, repair, state.highWater, limit,
            Math.max(1, maxMs - (performance.now() - started))),
        ]);
        let processedRepairRows = 0;
        for (const row of repairRows) {
          rowsRead += 1;
          processedRepairRows += 1;
          repair.cursorRowid = row.rowid;
          if (row.createdAt > repair.until) trackFuture(repair.aggregate, row.createdAt);
          else if (row.eligible) {
            fold(repair.aggregate, row);
            rowsApplied += 1;
          }
          if (rowsRead >= maxRows || performance.now() - started >= maxMs) break;
        }
        if ((processedRepairRows === repairRows.length && repairRows.length < limit) ||
            repair.cursorRowid === (nextSegment + 1) * SESSION_SUMMARY_SEGMENT_ROWS) {
          state.accumulator.segments[String(nextSegment)] = repair.aggregate;
          state.accumulator.activeRepair = null;
          completedRepair = { segment: nextSegment, revision: queueRevision };
        } else {
          state.accumulator.activeRepair = repair;
        }
      } catch (error) {
        if (error instanceof Error && error.message.includes("session_summary_read_interrupted")) {
          readInterrupted = true;
        } else throw error;
      }
    }
  }
  refreshAggregate(state.accumulator);

  if (readInterrupted && rowsRead === 0) {
    return finish({
      snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
      durationMs: Math.round(performance.now() - started), highWater: state.highWater,
      mode, fullRecompute, fallbackReason: "rows_read_timeout",
      mutationRevision: state.mutationRevision,
    }, true);
  }

  // Revision, queued-row check, and state write share one write transaction.
  // A concurrent append can land before it (and is observed) or afterward
  // (and remains in the queue for the upload fence).
  const stability = await writeRetry.run(() => db.transaction(() => {
    // The ledger revision does not change when another connection completes a
    // repair. Fence the exact state version before clearing a queue or writing.
    if (currentStateGeneration(db, sessionId) !== state.stateGeneration) return {
      stateStable: false, revisionStable: false, activityStable: false,
      noQueuedRows: false, finalComplete: false, repairsRemaining: true,
    };
    const revisionStable = sessionRevision(db, sessionId) === state.mutationRevision;
    const activityStable = sessionActivityRevision(db, sessionId) === activityAtStart;
    const noQueuedRows = !queuedRowsAfter(db, sessionId, state.highWater, until);
    // The read worker may have seen an older snapshot of a row that was still
    // beyond the durable cursor. Retry that slice; the prior cursor remains
    // valid and the next worker read sees the edit.
    if (!activityStable) return {
      stateStable: true,
      revisionStable, activityStable, noQueuedRows,
      finalComplete: false, repairsRemaining: true,
    };
    if (completedRepair !== null) {
      db.prepare(`delete from session_sync_summary_repairs
        where session_id = ? and segment = ? and revision = ?`)
        .run(sessionId, completedRepair.segment, completedRepair.revision);
    }
    const repairsRemaining = repairQueueRow(db, sessionId) !== null ||
      state.accumulator.activeRepair !== null || dueSegment(state.accumulator, until) !== null;
    const finalComplete = complete && revisionStable && noQueuedRows && !repairsRemaining;
    state.complete = finalComplete;
    state.mode = complete ? "incremental" : needsFallback ? "fallback" : state.mode;
    writeState(db, state);
    // A repaired edit is discharged once its segment and revision are stable.
    // A concurrent append can still block this pass; it remains in the queue
    // and must not force a full recompute on the next pass.
    if (complete && revisionStable && activityStable && !repairsRemaining) {
      db.prepare(`delete from session_sync_summary_dirty where session_id = ?`).run(sessionId);
    }
    if (finalComplete) {
      db.prepare(`delete from session_sync_summary_rows where session_id = ? and raw_rowid <= ?`)
        .run(sessionId, state.highWater);
    }
    return { stateStable: true, revisionStable, activityStable, noQueuedRows,
      finalComplete, repairsRemaining };
  }).immediate());
  if (!stability.stateStable) return retryFromLatest(rowsRead);
  const stable = stability.revisionStable && stability.activityStable && stability.noQueuedRows;

  const finalMode: SessionSummaryUpdateResult["mode"] = fullRecompute
    ? "fallback"
    : mode === "initial" ? "initial" : "incremental";
  return finish({
    snapshot: stability.finalComplete && stable ? snapshot(state.accumulator) : null,
    complete: stability.finalComplete && stable,
    rowsRead,
    rowsApplied,
    durationMs: Math.round(performance.now() - started),
    highWater: state.highWater,
    mode: finalMode,
    fullRecompute,
    fallbackReason: fullRecompute ? reason
      : !stability.revisionStable ? "ledger_mutation_during_slice"
      : !stability.activityStable ? "ledger_edit_during_slice"
      : stability.repairsRemaining ? "segment_repair_in_progress"
      : !complete && state.mode === "fallback" ? "fallback_in_progress"
      : !stability.noQueuedRows ? "append_queue" : null,
    mutationRevision: state.mutationRevision,
  });
}
