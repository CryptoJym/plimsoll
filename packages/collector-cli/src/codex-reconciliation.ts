import type Database from "better-sqlite3";

import { estimateCostUsd } from "../../shared/src/index";

const WINDOW_SECONDS = 10 * 60;
const CONTROL_ROW = 1;

type LegacyRow = {
  rowid: number;
  id: string;
  source: string;
  eventType: string;
  observedAt: string;
  sessionId: string | null;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  costUsd: number | null;
};

type CandidateRow = LegacyRow & {
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  payloadJson: string;
};

type ContextRow = {
  eventId: string;
  observedAt: string;
  value: string;
};

type LegacySeedStatements = {
  upsertContext: Database.Statement;
  upsertPending: Database.Statement;
  enqueueCandidate: Database.Statement;
  enqueueWindow: Database.Statement;
};

type NearestStatements = {
  before: Database.Statement;
  after: Database.Statement;
};

export type CodexReconciliationResult = {
  backfillComplete: boolean;
  legacyRowsVisited: number;
  contextRowsVisited: number;
  candidateRowsVisited: number;
  rowsVisited: number;
  rowsChanged: number;
  stitched: number;
  priced: number;
  timeBudgetExhausted: boolean;
};

export type CodexReconciliationStatus = {
  candidateBacklog: number;
  contextWindowBacklog: number;
  legacyCursorRowid: number;
  legacyTargetRowid: number;
  legacyComplete: boolean;
  rowsVisited: number;
  rowsChanged: number;
  lastRowsVisited: number;
  lastRowsChanged: number;
  lastSuccessAt: string | null;
  degradedReason: "maintenance_failed" | null;
};

function isCandidate(row: {
  source: string;
  eventType: string;
  sessionId: string | null;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  costUsd: number | null;
}) {
  return (
    row.source === "codex" &&
    row.eventType === "assistant_response" &&
    (row.inputTokens !== null || row.outputTokens !== null) &&
    (row.sessionId === null || row.model === null || row.costUsd === null)
  );
}

function validObservedAt(value: string) {
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function windowBucket(observedAt: string) {
  const parsed = validObservedAt(observedAt);
  return parsed === null ? null : Math.floor(parsed / 1000 / WINDOW_SECONDS) * WINDOW_SECONDS;
}

function isoAt(seconds: number) {
  return new Date(seconds * 1000).toISOString();
}

function enqueueWindow(
  statement: Database.Statement,
  observedAt: string,
  now: string,
) {
  const bucket = windowBucket(observedAt);
  if (bucket === null) return;
  statement.run(bucket, now, now);
}

function seedLegacyRow(statements: LegacySeedStatements, row: LegacyRow, now: string) {
  if (row.source === "codex" && (row.sessionId !== null || row.model !== null)) {
    statements.upsertContext.run(row.id, row.observedAt, row.sessionId, row.model);
    enqueueWindow(statements.enqueueWindow, row.observedAt, now);
  }
  if (!isCandidate(row)) return;
  statements.upsertPending.run(row.id, row.observedAt);
  statements.enqueueCandidate.run(row.id, now);
}

function nearestValue(
  row: CandidateRow,
  statements: NearestStatements,
) {
  const observedMs = validObservedAt(row.observedAt);
  if (observedMs === null) return null;
  const base = {
    eventId: row.id,
    observedAt: row.observedAt,
    start: new Date(observedMs - WINDOW_SECONDS * 1000).toISOString(),
    end: new Date(observedMs + WINDOW_SECONDS * 1000).toISOString(),
  };
  const before = statements.before.get(base) as ContextRow | undefined;
  const after = statements.after.get(base) as ContextRow | undefined;
  const nearest = [before, after]
    .filter((candidate): candidate is ContextRow => Boolean(candidate))
    .sort((left, right) => {
      const distance =
        Math.abs(Date.parse(left.observedAt) - observedMs) -
        Math.abs(Date.parse(right.observedAt) - observedMs);
      return distance || left.eventId.localeCompare(right.eventId);
    })[0];
  return nearest?.value ?? null;
}

function nearestStatements(
  database: Database.Database,
  column: "session_id" | "model",
): NearestStatements {
  const predicate = column === "session_id" ? "session_id is not null" : "model is not null";
  const index =
    column === "session_id"
      ? "idx_codex_reconciliation_context_session"
      : "idx_codex_reconciliation_context_model";
  return {
    before: database.prepare(
      `select event_id as eventId, observed_at as observedAt, ${column} as value
       from codex_reconciliation_context indexed by ${index}
       where ${predicate} and event_id != @eventId
         and observed_at >= @start and observed_at <= @observedAt
       order by observed_at desc, event_id desc
       limit 1`,
    ),
    after: database.prepare(
      `select event_id as eventId, observed_at as observedAt, ${column} as value
       from codex_reconciliation_context indexed by ${index}
       where ${predicate} and event_id != @eventId
         and observed_at >= @observedAt and observed_at <= @end
       order by observed_at asc, event_id asc
       limit 1`,
    ),
  };
}

function reconciledPayload(
  payloadJson: string,
  changes: {
    sessionId: string | null;
    model: string | null;
    costUsd: number | null;
    sessionChanged: boolean;
    modelChanged: boolean;
    costChanged: boolean;
  },
) {
  try {
    const parsed = JSON.parse(payloadJson) as Record<string, unknown>;
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return payloadJson;
    if (changes.sessionChanged) parsed.sessionId = changes.sessionId;
    if (changes.modelChanged) parsed.model = changes.model;
    if (changes.costChanged) parsed.costUsd = changes.costUsd;
    if (changes.sessionChanged || changes.costChanged) {
      const existingMetadata = parsed.metadata;
      const metadata =
        existingMetadata && typeof existingMetadata === "object" && !Array.isArray(existingMetadata)
          ? (existingMetadata as Record<string, unknown>)
          : {};
      if (changes.sessionChanged) metadata.stitched = "time_window";
      if (changes.costChanged) metadata.costEstimated = true;
      parsed.metadata = metadata;
    }
    return JSON.stringify(parsed);
  } catch {
    // A malformed legacy payload must not poison the bounded promoted-column
    // repair. Outbound privacy/schema validation remains the authority for it.
    return payloadJson;
  }
}

/**
 * Additive, local-only schema. The raw ledger is never indexed or walked here:
 * legacy discovery advances by rowid in maintenance, while new writes maintain
 * compact candidate/context indexes transactionally through triggers.
 */
export function ensureCodexReconciliationSchema(database: Database.Database) {
  database.exec(`
    create table if not exists codex_reconciliation_control (
      singleton integer primary key check (singleton = 1),
      candidate_backlog integer not null default 0,
      context_window_backlog integer not null default 0,
      legacy_cursor_rowid integer not null default 0,
      legacy_target_rowid integer not null default 0,
      legacy_complete integer not null default 0,
      rows_visited integer not null default 0,
      rows_changed integer not null default 0,
      last_rows_visited integer not null default 0,
      last_rows_changed integer not null default 0,
      last_success_at text,
      degraded_reason text,
      updated_at text not null
    );
    insert into codex_reconciliation_control (singleton, updated_at)
    values (1, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    on conflict(singleton) do nothing;

    create table if not exists codex_reconciliation_pending (
      event_id text primary key,
      observed_at text not null
    );
    create index if not exists idx_codex_reconciliation_pending_observed
      on codex_reconciliation_pending (observed_at, event_id);

    create table if not exists codex_reconciliation_candidates (
      event_id text primary key,
      queued_at text not null
    );
    create index if not exists idx_codex_reconciliation_candidates_queue
      on codex_reconciliation_candidates (queued_at, event_id);

    create table if not exists codex_reconciliation_context (
      event_id text primary key,
      observed_at text not null,
      session_id text,
      model text
    );
    create index if not exists idx_codex_reconciliation_context_session
      on codex_reconciliation_context (observed_at, event_id)
      where session_id is not null;
    create index if not exists idx_codex_reconciliation_context_model
      on codex_reconciliation_context (observed_at, event_id)
      where model is not null;

    create table if not exists codex_reconciliation_windows (
      window_start_seconds integer primary key,
      revision integer not null default 1,
      processing_revision integer not null default 0,
      cursor_observed_at text not null default '',
      cursor_event_id text not null default '',
      target_observed_at text not null default '',
      target_event_id text not null default '',
      queued_at text not null,
      updated_at text not null
    );
    create index if not exists idx_codex_reconciliation_windows_queue
      on codex_reconciliation_windows (queued_at, window_start_seconds);

    create trigger if not exists trg_codex_candidate_backlog_insert
    after insert on codex_reconciliation_candidates
    begin
      update codex_reconciliation_control
      set candidate_backlog = candidate_backlog + 1,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where singleton = 1;
    end;
    create trigger if not exists trg_codex_candidate_backlog_delete
    after delete on codex_reconciliation_candidates
    begin
      update codex_reconciliation_control
      set candidate_backlog = max(0, candidate_backlog - 1),
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where singleton = 1;
    end;
    create trigger if not exists trg_codex_window_backlog_insert
    after insert on codex_reconciliation_windows
    begin
      update codex_reconciliation_control
      set context_window_backlog = context_window_backlog + 1,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where singleton = 1;
    end;
    create trigger if not exists trg_codex_window_backlog_delete
    after delete on codex_reconciliation_windows
    begin
      update codex_reconciliation_control
      set context_window_backlog = max(0, context_window_backlog - 1),
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where singleton = 1;
    end;

    create trigger if not exists trg_codex_reconciliation_insert
    after insert on buffered_events
    begin
      insert into codex_reconciliation_context (event_id, observed_at, session_id, model)
      select new.id, new.observed_at, new.session_id, new.model
      where new.source = 'codex' and (new.session_id is not null or new.model is not null)
      on conflict(event_id) do update set
        observed_at = excluded.observed_at,
        session_id = excluded.session_id,
        model = excluded.model;

      insert into codex_reconciliation_windows
        (window_start_seconds, revision, processing_revision,
         cursor_observed_at, cursor_event_id, queued_at, updated_at)
      select
        (cast(strftime('%s', new.observed_at) as integer) / 600) * 600,
        1, 0, '', '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where new.source = 'codex'
        and (new.session_id is not null or new.model is not null)
        and strftime('%s', new.observed_at) is not null
      on conflict(window_start_seconds) do update set
        revision = revision + 1, updated_at = excluded.updated_at;

      insert into codex_reconciliation_pending (event_id, observed_at)
      select new.id, new.observed_at
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null)
      on conflict(event_id) do update set observed_at = excluded.observed_at;

      insert into codex_reconciliation_candidates (event_id, queued_at)
      select new.id, strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null)
      on conflict(event_id) do nothing;
    end;

    create trigger if not exists trg_codex_reconciliation_update
    after update of source, event_type, observed_at, session_id, model,
      input_tokens, output_tokens, cost_usd on buffered_events
    begin
      delete from codex_reconciliation_context where event_id = new.id;
      insert into codex_reconciliation_context (event_id, observed_at, session_id, model)
      select new.id, new.observed_at, new.session_id, new.model
      where new.source = 'codex' and (new.session_id is not null or new.model is not null);

      insert into codex_reconciliation_windows
        (window_start_seconds, revision, processing_revision,
         cursor_observed_at, cursor_event_id, queued_at, updated_at)
      select
        (cast(strftime('%s', new.observed_at) as integer) / 600) * 600,
        1, 0, '', '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where new.source = 'codex'
        and (new.session_id is not null or new.model is not null)
        and strftime('%s', new.observed_at) is not null
        and (old.source is not new.source or old.observed_at is not new.observed_at
          or old.session_id is not new.session_id or old.model is not new.model)
      on conflict(window_start_seconds) do update set
        revision = revision + 1, updated_at = excluded.updated_at;

      delete from codex_reconciliation_pending where event_id = new.id;
      insert into codex_reconciliation_pending (event_id, observed_at)
      select new.id, new.observed_at
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null);

      insert into codex_reconciliation_candidates (event_id, queued_at)
      select new.id, strftime('%Y-%m-%dT%H:%M:%fZ','now')
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null)
      on conflict(event_id) do nothing;
    end;

    create trigger if not exists trg_codex_reconciliation_delete
    after delete on buffered_events
    begin
      delete from codex_reconciliation_pending where event_id = old.id;
      delete from codex_reconciliation_candidates where event_id = old.id;
      delete from codex_reconciliation_context where event_id = old.id;
    end;
  `);
}

export function codexReconciliationStatus(
  database: Database.Database,
): CodexReconciliationStatus {
  const row = database
    .prepare(
      `select candidate_backlog as candidateBacklog,
         context_window_backlog as contextWindowBacklog,
         legacy_cursor_rowid as legacyCursorRowid,
         legacy_target_rowid as legacyTargetRowid,
         legacy_complete as legacyComplete,
         rows_visited as rowsVisited,
         rows_changed as rowsChanged,
         last_rows_visited as lastRowsVisited,
         last_rows_changed as lastRowsChanged,
         last_success_at as lastSuccessAt,
         degraded_reason as degradedReason
       from codex_reconciliation_control where singleton = ?`,
    )
    .get(CONTROL_ROW) as Omit<CodexReconciliationStatus, "legacyComplete"> & {
    legacyComplete: number;
  };
  return { ...row, legacyComplete: row.legacyComplete === 1 };
}

/**
 * One fixed reconciliation slice. All promoted-column and payload changes use
 * one UPDATE per event, so transaction-local outbox/projection triggers observe
 * one old/new pair. Queue work is replay-safe and commits with that mutation.
 */
export function runCodexReconciliationMaintenance(
  database: Database.Database,
  options: {
    legacyRowLimit?: number;
    legacyChunkLimit?: number;
    contextWindowLimit?: number;
    contextRowLimit?: number;
    candidateLimit?: number;
    timeLimitMs?: number;
  } = {},
): CodexReconciliationResult {
  const legacyRowLimit = Math.max(
    1,
    Math.min(options.legacyRowLimit ?? 100_000, 250_000),
  );
  const legacyChunkLimit = Math.max(
    1,
    Math.min(options.legacyChunkLimit ?? 2_000, 10_000),
  );
  const contextWindowLimit = Math.max(
    1,
    Math.min(options.contextWindowLimit ?? 8, 64),
  );
  const contextRowLimit = Math.max(1, Math.min(options.contextRowLimit ?? 1_000, 10_000));
  const candidateLimit = Math.max(1, Math.min(options.candidateLimit ?? 500, 5_000));
  const timeLimitMs = Math.max(1, Math.min(options.timeLimitMs ?? 50, 1_000));
  const deadline = performance.now() + timeLimitMs;

  try {
    return database.transaction(() => {
      const now = new Date().toISOString();
      let control = database
        .prepare(
          `select legacy_cursor_rowid as legacyCursorRowid,
             legacy_target_rowid as legacyTargetRowid,
             legacy_complete as legacyComplete
           from codex_reconciliation_control where singleton = ?`,
        )
        .get(CONTROL_ROW) as {
        legacyCursorRowid: number;
        legacyTargetRowid: number;
        legacyComplete: number;
      };

      if (control.legacyComplete !== 1 && control.legacyTargetRowid === 0) {
        const highWater = database
          .prepare(`select coalesce(max(rowid), 0) as rowid from buffered_events`)
          .get() as { rowid: number };
        database
          .prepare(
            `update codex_reconciliation_control
             set legacy_target_rowid = ?, legacy_complete = ?, updated_at = ?
             where singleton = ?`,
          )
          .run(highWater.rowid, highWater.rowid === 0 ? 1 : 0, now, CONTROL_ROW);
        control = {
          ...control,
          legacyTargetRowid: highWater.rowid,
          legacyComplete: highWater.rowid === 0 ? 1 : 0,
        };
      }

      let legacyRowsVisited = 0;
      if (control.legacyComplete !== 1 && performance.now() < deadline) {
        const legacySeedStatements: LegacySeedStatements = {
          upsertContext: database.prepare(
            `insert into codex_reconciliation_context
               (event_id, observed_at, session_id, model)
             values (?, ?, ?, ?)
             on conflict(event_id) do update set
               observed_at = excluded.observed_at,
               session_id = excluded.session_id,
               model = excluded.model`,
          ),
          upsertPending: database.prepare(
            `insert into codex_reconciliation_pending (event_id, observed_at)
             values (?, ?)
             on conflict(event_id) do update set observed_at = excluded.observed_at`,
          ),
          enqueueCandidate: database.prepare(
            `insert into codex_reconciliation_candidates (event_id, queued_at)
             values (?, ?)
             on conflict(event_id) do nothing`,
          ),
          enqueueWindow: database.prepare(
            `insert into codex_reconciliation_windows
               (window_start_seconds, revision, processing_revision,
                cursor_observed_at, cursor_event_id, queued_at, updated_at)
             values (?, 1, 0, '', '', ?, ?)
             on conflict(window_start_seconds) do update set
               revision = revision + 1, updated_at = excluded.updated_at`,
          ),
        };
        const selectLegacy = database.prepare(
          `select rowid, id, source, event_type as eventType, observed_at as observedAt,
             session_id as sessionId, model, input_tokens as inputTokens,
             output_tokens as outputTokens, cost_usd as costUsd
           from buffered_events
           where rowid > @cursor and rowid <= @target
           order by rowid
           limit @limit`,
        );
        let cursor = control.legacyCursorRowid;
        let complete = false;
        while (legacyRowsVisited < legacyRowLimit && performance.now() < deadline) {
          const limit = Math.min(legacyChunkLimit, legacyRowLimit - legacyRowsVisited);
          const rows = selectLegacy.all({
            cursor,
            target: control.legacyTargetRowid,
            limit,
          }) as LegacyRow[];
          legacyRowsVisited += rows.length;
          for (const row of rows) seedLegacyRow(legacySeedStatements, row, now);
          cursor = rows.at(-1)?.rowid ?? cursor;
          complete = rows.length < limit || cursor >= control.legacyTargetRowid;
          if (complete || rows.length === 0) break;
        }
        database
          .prepare(
            `update codex_reconciliation_control
             set legacy_cursor_rowid = ?, legacy_complete = ?, updated_at = ?
             where singleton = ?`,
          )
          .run(cursor, complete ? 1 : 0, now, CONTROL_ROW);
        control = { ...control, legacyCursorRowid: cursor, legacyComplete: complete ? 1 : 0 };
      }

      let contextRowsVisited = 0;
      const windows = (performance.now() < deadline
        ? database
            .prepare(
              `select window_start_seconds as windowStartSeconds,
                 revision, processing_revision as processingRevision,
                 cursor_observed_at as cursorObservedAt,
                 cursor_event_id as cursorEventId,
                 target_observed_at as targetObservedAt,
                 target_event_id as targetEventId
               from codex_reconciliation_windows
               order by queued_at, window_start_seconds
               limit ?`,
            )
            .all(contextWindowLimit)
        : []) as Array<{
        windowStartSeconds: number;
        revision: number;
        processingRevision: number;
        cursorObservedAt: string;
        cursorEventId: string;
        targetObservedAt: string;
        targetEventId: string;
      }>;
      const enqueueCandidate = database.prepare(
        `insert into codex_reconciliation_candidates (event_id, queued_at)
         values (?, ?)
         on conflict(event_id) do nothing`,
      );
      const deleteWindow = database.prepare(
        `delete from codex_reconciliation_windows where window_start_seconds = ?`,
      );
      const advanceWindow = database.prepare(
        `update codex_reconciliation_windows
         set cursor_observed_at = ?, cursor_event_id = ?, updated_at = ?
         where window_start_seconds = ?`,
      );
      const beginWindowPass = database.prepare(
        `update codex_reconciliation_windows set
           processing_revision = ?, cursor_observed_at = '', cursor_event_id = '',
           target_observed_at = ?, target_event_id = ?, updated_at = ?
         where window_start_seconds = ?`,
      );
      for (const window of windows) {
        if (performance.now() >= deadline || contextRowsVisited >= contextRowLimit) break;
        const remaining = contextRowLimit - contextRowsVisited;
        const start = isoAt(window.windowStartSeconds - WINDOW_SECONDS);
        const end = isoAt(window.windowStartSeconds + WINDOW_SECONDS * 2);
        let processingRevision = window.processingRevision;
        let cursorObservedAt = window.cursorObservedAt || start;
        let cursorEventId = window.cursorEventId;
        let targetObservedAt = window.targetObservedAt;
        let targetEventId = window.targetEventId;
        if (processingRevision === 0) {
          const target = database
            .prepare(
              `select event_id as eventId, observed_at as observedAt
               from codex_reconciliation_pending indexed by idx_codex_reconciliation_pending_observed
               where observed_at >= ? and observed_at < ?
               order by observed_at desc, event_id desc limit 1`,
            )
            .get(start, end) as { eventId: string; observedAt: string } | undefined;
          if (!target) {
            deleteWindow.run(window.windowStartSeconds);
            continue;
          }
          processingRevision = window.revision;
          targetObservedAt = target.observedAt;
          targetEventId = target.eventId;
          cursorObservedAt = start;
          cursorEventId = "";
          beginWindowPass.run(
            processingRevision,
            targetObservedAt,
            targetEventId,
            now,
            window.windowStartSeconds,
          );
        }
        const rows = database
          .prepare(
            `select event_id as eventId, observed_at as observedAt
             from codex_reconciliation_pending indexed by idx_codex_reconciliation_pending_observed
             where observed_at >= @start and observed_at < @end
               and (observed_at > @cursorObservedAt
                 or (observed_at = @cursorObservedAt and event_id > @cursorEventId))
               and (observed_at < @targetObservedAt
                 or (observed_at = @targetObservedAt and event_id <= @targetEventId))
             order by observed_at, event_id
             limit @limit`,
          )
          .all({
            start,
            end,
            cursorObservedAt,
            cursorEventId,
            targetObservedAt,
            targetEventId,
            limit: remaining,
          }) as Array<{ eventId: string; observedAt: string }>;
        contextRowsVisited += rows.length;
        for (const row of rows) enqueueCandidate.run(row.eventId, now);
        const last = rows.at(-1);
        const passComplete =
          rows.length < remaining ||
          (last?.observedAt === targetObservedAt && last.eventId === targetEventId);
        if (passComplete) {
          if (window.revision > processingRevision) {
            // An invalidation arrived during a prior slice. Finish that
            // revision's high-water first, then restart once; never reset a
            // live cursor in-place and starve its tail.
            const target = database
              .prepare(
                `select event_id as eventId, observed_at as observedAt
                 from codex_reconciliation_pending indexed by idx_codex_reconciliation_pending_observed
                 where observed_at >= ? and observed_at < ?
                 order by observed_at desc, event_id desc limit 1`,
              )
              .get(start, end) as { eventId: string; observedAt: string } | undefined;
            if (target) {
              beginWindowPass.run(
                window.revision,
                target.observedAt,
                target.eventId,
                now,
                window.windowStartSeconds,
              );
            } else {
              deleteWindow.run(window.windowStartSeconds);
            }
          } else {
            deleteWindow.run(window.windowStartSeconds);
          }
        } else {
          const cursor = rows[rows.length - 1]!;
          advanceWindow.run(
            cursor.observedAt,
            cursor.eventId,
            now,
            window.windowStartSeconds,
          );
        }
      }

      const candidateRows = (performance.now() < deadline
        ? database
            .prepare(
              `select e.rowid, e.id, e.source, e.event_type as eventType,
                 e.observed_at as observedAt, e.session_id as sessionId, e.model,
                 e.input_tokens as inputTokens, e.output_tokens as outputTokens,
                 e.cache_read_tokens as cacheReadTokens,
                 e.cache_creation_tokens as cacheCreationTokens,
                 e.cost_usd as costUsd, e.payload_json as payloadJson
               from codex_reconciliation_candidates q
               join buffered_events e on e.id = q.event_id
               order by q.queued_at, q.event_id
               limit ?`,
            )
            .all(candidateLimit)
        : []) as CandidateRow[];
      const removeCandidate = database.prepare(
        `delete from codex_reconciliation_candidates where event_id = ?`,
      );
      const apply = database.prepare(
        `update buffered_events set
           session_id = @sessionId,
           model = @model,
           cost_usd = @costUsd,
           payload_json = @payloadJson
         where id = @id`,
      );
      const sessionContext = nearestStatements(database, "session_id");
      const modelContext = nearestStatements(database, "model");
      let candidateRowsVisited = 0;
      let rowsChanged = 0;
      let stitched = 0;
      let priced = 0;
      for (const row of candidateRows) {
        if (performance.now() >= deadline) break;
        candidateRowsVisited += 1;
        if (!row.id || !isCandidate(row)) {
          removeCandidate.run(row.id);
          continue;
        }
        const sessionId = row.sessionId ?? nearestValue(row, sessionContext);
        const model = row.model ?? nearestValue(row, modelContext);
        let costUsd = row.costUsd;
        if (costUsd === null && model) {
          costUsd =
            estimateCostUsd({
              model,
              inputTokens: row.inputTokens ?? 0,
              outputTokens: row.outputTokens ?? 0,
              cacheReadTokens: row.cacheReadTokens ?? 0,
              cacheCreationTokens: row.cacheCreationTokens ?? 0,
            })?.costUsd ?? null;
        }
        const sessionChanged = sessionId !== row.sessionId;
        const modelChanged = model !== row.model;
        const costChanged = costUsd !== row.costUsd;
        if (sessionChanged || modelChanged || costChanged) {
          const payloadJson = reconciledPayload(row.payloadJson, {
            sessionId,
            model,
            costUsd,
            sessionChanged,
            modelChanged,
            costChanged,
          });
          rowsChanged += apply.run({ id: row.id, sessionId, model, costUsd, payloadJson }).changes;
          if (sessionChanged) stitched += 1;
          if (costChanged) priced += 1;
        }
        // Unresolved rows leave the active queue instead of spinning. They
        // remain in `pending`; a later context-window invalidation requeues.
        removeCandidate.run(row.id);
      }

      const rowsVisited = legacyRowsVisited + contextRowsVisited + candidateRowsVisited;
      database
        .prepare(
          `update codex_reconciliation_control set
             rows_visited = rows_visited + @rowsVisited,
             rows_changed = rows_changed + @rowsChanged,
             last_rows_visited = @rowsVisited,
             last_rows_changed = @rowsChanged,
             last_success_at = @now,
             degraded_reason = null,
             updated_at = @now
           where singleton = 1`,
        )
        .run({ rowsVisited, rowsChanged, now });
      return {
        backfillComplete: control.legacyComplete === 1,
        legacyRowsVisited,
        contextRowsVisited,
        candidateRowsVisited,
        rowsVisited,
        rowsChanged,
        stitched,
        priced,
        timeBudgetExhausted: performance.now() >= deadline,
      };
    })();
  } catch (error) {
    database
      .prepare(
        `update codex_reconciliation_control
         set degraded_reason = 'maintenance_failed', updated_at = ?
         where singleton = ?`,
      )
      .run(new Date().toISOString(), CONTROL_ROW);
    throw error;
  }
}
