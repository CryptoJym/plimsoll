import Database from "better-sqlite3";

import { terminalPrivacyEligibilitySql } from "./privacy-disposition";

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

export const SESSION_SUMMARY_SCHEMA_VERSION = 3 as const;
export const SESSION_SUMMARY_DEFAULT_MAX_ROWS = 5_000;
export const SESSION_SUMMARY_DEFAULT_MAX_MS = 250;

type SummaryAccumulator = {
  sessionId: string;
  /** Rows through this rowid belong to the frozen historical scan. */
  scanBoundary: number;
  /** Historical scan cursor follows idx_events_session (session, observation, rowid). */
  cursorObservedAt: string | null;
  cursorRowid: number;
  cursorId: string | null;
  /** A preexisting future-created row requires one fallback when the horizon advances. */
  futureRows: boolean;
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

type SummaryState = {
  sessionId: string;
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
  // SQLite's CREATE TRIGGER IF NOT EXISTS keeps the old trigger body. Replace
  // it atomically when upgrading a v1/v2 ledger to the frozen scan boundary.
  db.transaction(() => {
    if (rawInsertTrigger && !rawInsertTrigger.sql.includes("scanBoundary")) {
      db.exec("drop trigger trg_session_summary_raw_insert");
    }
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
    create table if not exists session_sync_summary_dirty (
      session_id text primary key,
      reason text not null,
      updated_at text not null
    );
    create table if not exists session_sync_summary_revision (
      session_id text primary key,
      mutation_revision integer not null check (mutation_revision >= 0)
    );
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
    create index if not exists idx_session_summary_state_incomplete
      on session_sync_summary_state (complete, updated_at);
    create index if not exists idx_session_summary_dirty_updated
      on session_sync_summary_dirty (updated_at);
    create index if not exists idx_session_summary_rows_session
      on session_sync_summary_rows (session_id, raw_rowid);

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

    create trigger if not exists trg_session_summary_raw_update
    after update of id, source, event_type, data_mode, observed_at, created_at,
      session_id, input_tokens, output_tokens, cache_read_tokens,
      cache_creation_tokens, cost_usd, repo_hash, branch_hash, account_hash,
      privacy_generation, privacy_disposition on buffered_events
    begin
      update session_sync_summary_control
        set mutation_revision = mutation_revision + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where singleton = 1;
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select old.session_id, 'raw_update', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where old.session_id is not null
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select new.session_id, 'raw_update', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where new.session_id is not null
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
    end;

    create trigger if not exists trg_session_summary_raw_delete
    after delete on buffered_events
    begin
      update session_sync_summary_control
        set mutation_revision = mutation_revision + 1,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where singleton = 1;
      insert into session_sync_summary_dirty (session_id, reason, updated_at)
        select old.session_id, 'raw_delete', strftime('%Y-%m-%dT%H:%M:%fZ','now')
        where old.session_id is not null
        on conflict(session_id) do update set
          reason = excluded.reason, updated_at = excluded.updated_at;
      delete from session_sync_summary_rows where raw_rowid = old.rowid;
    end;
    `);
  }).immediate();

  // The previous schema stored the global revision in each state. Preserve
  // that baseline once, then let dirty-marker triggers advance only the
  // affected session. No historical event table is scanned during upgrade.
  if (revisionTableMissing) {
    db.exec(`insert or ignore into session_sync_summary_revision (session_id, mutation_revision)
      select session_id, mutation_revision from session_sync_summary_state`);
    // Round 1 only invalidated privacy changes behind a committed HWM. A
    // privacy change during the first read must invalidate that read too.
    db.exec(`
      drop trigger if exists trg_session_summary_outbox_insert;
      drop trigger if exists trg_session_summary_outbox_update;
      drop trigger if exists trg_session_summary_outbox_delete;
      drop trigger if exists trg_session_summary_receipt_insert;
      drop trigger if exists trg_session_summary_receipt_update;
      drop trigger if exists trg_session_summary_receipt_delete;
    `);
  }

  // These tables are created by DeliveryOutbox, but a small proof ledger or a
  // pre-delivery install may not have them. Raw edits/deletes remain covered.
  if (tableExists(db, "upload_outbox")) {
    db.exec(`
      create trigger if not exists trg_session_summary_outbox_insert
      after insert on upload_outbox
      when new.raw_rowid is not null and exists (
        select 1 from buffered_events e
        join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = new.raw_rowid
          and (new.raw_id is null or new.raw_created_at is null or new.raw_generation is null
            or new.raw_id is not e.id or new.raw_created_at is not e.created_at
            or new.raw_generation is not e.privacy_generation)
      )
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = new.raw_rowid and e.session_id is not null
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
      end;
      create trigger if not exists trg_session_summary_outbox_update
      after update of raw_rowid, raw_id, raw_created_at, raw_generation on upload_outbox
      when new.raw_rowid is not null and exists (
        select 1 from buffered_events e
        join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = new.raw_rowid
          and (
            old.raw_id is null or old.raw_created_at is null or old.raw_generation is null
            or old.raw_id is not e.id or old.raw_created_at is not e.created_at
            or old.raw_generation is not e.privacy_generation
            or new.raw_id is null or new.raw_created_at is null or new.raw_generation is null
            or new.raw_id is not e.id or new.raw_created_at is not e.created_at
            or new.raw_generation is not e.privacy_generation
          )
      )
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = new.raw_rowid and e.session_id is not null
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
      end;
      create trigger if not exists trg_session_summary_outbox_delete
      after delete on upload_outbox
      when old.raw_rowid is not null and exists (
        select 1 from buffered_events e
        join session_sync_summary_state s on s.session_id = e.session_id
        where e.rowid = old.raw_rowid
      )
      begin
        update session_sync_summary_control
          set mutation_revision = mutation_revision + 1,
              updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
          where singleton = 1;
        insert into session_sync_summary_dirty (session_id, reason, updated_at)
          select e.session_id, 'privacy_outbox', strftime('%Y-%m-%dT%H:%M:%fZ','now')
          from buffered_events e where e.rowid = old.raw_rowid and e.session_id is not null
          on conflict(session_id) do update set
            reason = excluded.reason, updated_at = excluded.updated_at;
      end;
    `);
  }

  if (tableExists(db, "upload_receipts")) {
    const receiptColumns = columnNames(db, "upload_receipts");
    if (receiptColumns.has("reason") && receiptColumns.has("delivery_id")) {
      db.exec(`
        create trigger if not exists trg_session_summary_receipt_insert
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
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e where e.id = new.delivery_id and e.session_id is not null
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
        end;
        create trigger if not exists trg_session_summary_receipt_update
        after update of delivery_id, reason on upload_receipts
        when (
          old.reason in ('local_evidence_quarantined','local_privacy_violation') or
          new.reason in ('local_evidence_quarantined','local_privacy_violation')
        )
        begin
          update session_sync_summary_control
            set mutation_revision = mutation_revision + 1,
                updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
            where singleton = 1;
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e
            join session_sync_summary_state s on s.session_id = e.session_id
            where e.id = new.delivery_id and e.session_id is not null
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
        end;
        create trigger if not exists trg_session_summary_receipt_delete
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
          insert into session_sync_summary_dirty (session_id, reason, updated_at)
            select e.session_id, 'privacy_receipt', strftime('%Y-%m-%dT%H:%M:%fZ','now')
            from buffered_events e where e.id = old.delivery_id and e.session_id is not null
            on conflict(session_id) do update set
              reason = excluded.reason, updated_at = excluded.updated_at;
        end;
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

export function sessionSummaryCounters(db: Database.Database): SessionSummaryCounters {
  ensureSessionSummarySchema(db);
  return control(db);
}

function emptyAccumulator(sessionId: string): SummaryAccumulator {
  return {
    sessionId,
    scanBoundary: 0,
    cursorObservedAt: null,
    cursorRowid: 0,
    cursorId: null,
    futureRows: false,
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

function fold(accumulator: SummaryAccumulator, row: RawSummaryRow): void {
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
    if (typeof candidate.futureRows !== "boolean") return null;
    if (!Number.isSafeInteger(candidate.events) || candidate.events < 0) return null;
    for (const key of [
      "inputTokens", "outputTokens", "cacheReadTokens", "cacheCreationTokens",
      "pricedEvents", "repoNonNull", "branchNonNull", "accountNonNull",
    ] as const) {
      if (!Number.isSafeInteger(candidate[key]) || candidate[key] < 0) return null;
    }
    if (!Number.isFinite(candidate.costUsd) || !Number.isFinite(candidate.costCompensation)) return null;
    if (typeof candidate.sourceMax !== "string" && candidate.sourceMax !== null) return null;
    if (typeof candidate.startedAt !== "string" && candidate.startedAt !== null) return null;
    if (typeof candidate.endedAt !== "string" && candidate.endedAt !== null) return null;
    for (const key of ["repoValue", "branchValue", "accountValue"] as const) {
      if (typeof candidate[key] !== "string" && candidate[key] !== null) return null;
    }
    for (const key of ["repoMixed", "branchMixed", "accountMixed"] as const) {
      if (typeof candidate[key] !== "boolean") return null;
    }
    return candidate;
  } catch {
    return null;
  }
}

function storedState(db: Database.Database, sessionId: string): StoredSummaryState | null {
  const row = db.prepare(
    `select session_id as sessionId, schema_version as schemaVersion, high_water as highWater,
       checkpoint_id as checkpointId, covered_until as coveredUntil, complete,
       mutation_revision as mutationRevision, mode, accumulator_json as accumulatorJson
     from session_sync_summary_state where session_id = ?`,
  ).get(sessionId) as (Omit<StoredSummaryState, "complete"> & { complete: number }) | undefined;
  if (!row) return null;
  return { ...row, complete: Boolean(row.complete), mode: row.mode } as StoredSummaryState;
}

function validStoredState(state: StoredSummaryState, sessionId: string, until: string): boolean {
  return state.sessionId === sessionId &&
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
): Promise<boolean> {
  if (state.highWater === 0) return state.checkpointId === null &&
    accumulator.cursorRowid === 0 && accumulator.cursorObservedAt === null && accumulator.cursorId === null;
  if (state.checkpointId === null) return false;
  const rows = await read<{ id: string; sessionId: string | null; observedAt: string }>(rowCheckpointQuery(state, maxMs));
  if (rows.length !== 1 || rows[0]?.id !== state.checkpointId || rows[0]?.sessionId !== sessionId) return false;
  if (accumulator.cursorRowid === 0) return accumulator.cursorObservedAt === null && accumulator.cursorId === null;
  if (accumulator.cursorObservedAt === null || accumulator.cursorId === null) return false;
  if (accumulator.cursorRowid === state.highWater) {
    return rows[0]?.id === accumulator.cursorId && rows[0]?.observedAt === accumulator.cursorObservedAt;
  }
  const cursorRows = await read<{ id: string; sessionId: string | null; observedAt: string }>([{
    sql: `select id, session_id as sessionId, observed_at as observedAt
      from buffered_events where rowid = @rowid`,
    params: { rowid: accumulator.cursorRowid },
    ...(maxMs === undefined ? {} : { maxMs }),
  }]);
  return cursorRows.length === 1 && cursorRows[0]?.id === accumulator.cursorId &&
    cursorRows[0]?.sessionId === sessionId && cursorRows[0]?.observedAt === accumulator.cursorObservedAt;
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
  const cursor = appendRows
    ? "r.session_id = @sessionId and r.raw_rowid > @highWater"
    : "e.session_id = @sessionId and e.rowid <= @scanBoundary and (e.observed_at, e.rowid) > (@cursorObservedAt, @cursorRowid)";
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
       : "buffered_events e indexed by idx_events_session"}
     where ${cursor}
     order by ${appendRows ? "e.rowid" : "e.observed_at, e.rowid"} asc limit @limit`,
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

function writeState(db: Database.Database, state: SummaryState): void {
  db.prepare(
    `insert into session_sync_summary_state
       (session_id, schema_version, high_water, checkpoint_id, covered_until, complete,
        mutation_revision, mode, accumulator_json, updated_at)
     values (@sessionId, @schemaVersion, @highWater, @checkpointId, @coveredUntil, @complete,
       @mutationRevision, @mode, @accumulatorJson, @updatedAt)
     on conflict(session_id) do update set
       schema_version=excluded.schema_version, high_water=excluded.high_water,
       checkpoint_id=excluded.checkpoint_id, covered_until=excluded.covered_until,
       complete=excluded.complete, mutation_revision=excluded.mutation_revision,
       mode=excluded.mode, accumulator_json=excluded.accumulator_json,
       updated_at=excluded.updated_at`,
  ).run({
    sessionId: state.sessionId,
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
}

function stateFromStored(stored: StoredSummaryState, accumulator: SummaryAccumulator): SummaryState {
  return {
    sessionId: stored.sessionId,
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
  dirty: boolean,
): string | null {
  if (!stored) return null;
  if (!parsed) return "accumulator_corrupt";
  if (stored.schemaVersion !== SESSION_SUMMARY_SCHEMA_VERSION) return "schema_version";
  if (!Number.isSafeInteger(stored.highWater) || stored.highWater < 0) return "high_water_invalid";
  if (Number.isNaN(Date.parse(stored.coveredUntil))) return "covered_until_invalid";
  if (Date.parse(stored.coveredUntil) > Date.parse(until)) return "until_rollback";
  if (parsed.futureRows && Date.parse(stored.coveredUntil) < Date.parse(until)) return "future_horizon";
  if (currentRevision !== stored.mutationRevision) return "ledger_mutation";
  if (!checkpointOk) return "checkpoint_mismatch";
  if (dirty) return "dirty_marker";
  return null;
}

/** Session ids whose durable summaries need another bounded pass. */
export function listSessionSummaryPendingIds(db: Database.Database): string[] {
  if (!tableExists(db, "session_sync_summary_state") || !tableExists(db, "session_sync_summary_dirty")) return [];
  const rows = db.prepare(
    `select session_id as sessionId from session_sync_summary_dirty
     union
     select session_id as sessionId from session_sync_summary_state where complete = 0
     union
     select r.session_id as sessionId from session_sync_summary_revision r
       left join session_sync_summary_state s on s.session_id = r.session_id
       where s.session_id is null or r.mutation_revision != s.mutation_revision`,
  ).all() as Array<{ sessionId: string }>;
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
  const started = performance.now();
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
  const currentRevision = sessionRevision(db, sessionId);
  const stored = storedState(db, sessionId);
  const parsed = stored ? parseAccumulator(sessionId, stored.accumulatorJson) : null;
  const dirty = Boolean(db.prepare(
    `select 1 from session_sync_summary_dirty where session_id = ? limit 1`,
  ).get(sessionId));
  let checkpointOk = false;
  if (stored && parsed && validStoredState(stored, sessionId, until)) {
    try {
      checkpointOk = await checkpointValid(stored, parsed, sessionId, options.read, maxMs);
    } catch (error) {
      if (!(error instanceof Error && error.message.includes("session_summary_read_interrupted"))) throw error;
      // A worker deadline says nothing about checkpoint integrity. Preserve
      // the stored cursor and retry the bounded read on the next slice.
      return {
        snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
        durationMs: Math.round(performance.now() - started), highWater: stored.highWater,
        mode: stored.mode, fullRecompute: false, fallbackReason: "checkpoint_timeout",
        mutationRevision: stored.mutationRevision,
      };
    }
  }
  const reason = fallbackReason(stored, parsed, currentRevision, until, checkpointOk, dirty);
  const resumableFallback = stored?.mode === "fallback" &&
    !stored.complete && stored.mutationRevision === currentRevision &&
    parsed !== null && checkpointOk && (reason === null || reason === "dirty_marker") &&
    Date.parse(stored.coveredUntil) === Date.parse(until);
  const needsFallback = reason !== null && !resumableFallback;
  const fullRecompute = needsFallback;
  let mode: SessionSummaryUpdateResult["mode"] = needsFallback
    ? "fallback"
    : stored?.complete && Date.parse(stored.coveredUntil) === Date.parse(until)
      ? "cached"
      : stored ? "incremental" : "initial";

  let state: SummaryState;
  if (needsFallback || !stored) {
    // Freeze the old rowid range while installing the trigger-visible state.
    // Rows inserted afterward enter the append queue, even if their observed
    // time sorts behind the historical cursor.
    state = db.transaction(() => {
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
      const fresh: SummaryState = {
        sessionId,
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
    }).immediate();
  } else if (parsed) {
    state = stateFromStored(stored, parsed);
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
      return {
        snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
        durationMs: Math.round(performance.now() - started), highWater: state.highWater,
        mode: "cached", fullRecompute: false, fallbackReason: "newer_read_timeout",
        mutationRevision: state.mutationRevision,
      };
    }
    if (newer.length > 0 ||
        !sessionSummaryCurrent(db, sessionId, until, state.mutationRevision, state.highWater)) {
      mode = "incremental";
      state.complete = false;
    } else {
    return {
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
    };
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
  let complete = false;
  let readInterrupted = false;
  while (rowsRead < maxRows && performance.now() - started < maxMs) {
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
      if (row.createdAt > until) state.accumulator.futureRows = true;
      else if (row.eligible) {
        fold(state.accumulator, row);
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

  if (readInterrupted && rowsRead === 0) {
    return {
      snapshot: null, complete: false, rowsRead: 0, rowsApplied: 0,
      durationMs: Math.round(performance.now() - started), highWater: state.highWater,
      mode, fullRecompute, fallbackReason: "rows_read_timeout",
      mutationRevision: state.mutationRevision,
    };
  }

  // Revision, queued-row check, and state write share one write transaction.
  // A concurrent append can land before it (and is observed) or afterward
  // (and remains in the queue for the upload fence).
  const stable = db.transaction(() => {
    const revisionStable = sessionRevision(db, sessionId) === state.mutationRevision;
    const noQueuedRows = !queuedRowsAfter(db, sessionId, state.highWater, until);
    const finalComplete = complete && revisionStable && noQueuedRows;
    state.complete = finalComplete;
    state.mode = complete ? "incremental" : needsFallback ? "fallback" : state.mode;
    writeState(db, state);
    if (finalComplete) {
      db.prepare(`delete from session_sync_summary_dirty where session_id = ?`).run(sessionId);
      db.prepare(`delete from session_sync_summary_rows where session_id = ? and raw_rowid <= ?`)
        .run(sessionId, state.highWater);
    }
    return revisionStable && noQueuedRows;
  }).immediate();

  const finalMode: SessionSummaryUpdateResult["mode"] = fullRecompute
    ? "fallback"
    : mode === "initial" ? "initial" : "incremental";
  return {
    snapshot: complete && stable ? snapshot(state.accumulator) : null,
    complete: complete && stable,
    rowsRead,
    rowsApplied,
    durationMs: Math.round(performance.now() - started),
    highWater: state.highWater,
    mode: finalMode,
    fullRecompute,
    fallbackReason: fullRecompute ? reason : null,
    mutationRevision: state.mutationRevision,
  };
}
