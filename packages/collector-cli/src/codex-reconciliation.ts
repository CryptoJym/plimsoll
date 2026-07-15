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

type NearestContextRow = {
  eventId: string;
  observedAt: string;
  value: string;
};

type LegacySeedStatements = {
  upsertPending: Database.Statement;
  enqueueCandidate: Database.Statement;
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
  sliceDurationMs: number;
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
  lastSliceDurationMs: number;
  maxSliceDurationMs: number;
  lastTimeBudgetExhausted: boolean;
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

function isoAt(seconds: number) {
  return new Date(seconds * 1000).toISOString();
}

function seedLegacyRow(statements: LegacySeedStatements, row: LegacyRow, now: string) {
  if (!isCandidate(row)) return;
  statements.upsertPending.run(row.id, row.observedAt);
  statements.enqueueCandidate.run(row.id, now, 1);
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
  const before = statements.before.get(base) as NearestContextRow | undefined;
  const after = statements.after.get(base) as NearestContextRow | undefined;
  const nearest = [before, after]
    .filter((candidate): candidate is NearestContextRow => Boolean(candidate))
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
  return {
    before: database.prepare(
      `select id as eventId, observed_at as observedAt, ${column} as value
       from buffered_events indexed by idx_events_observed
       where source = 'codex' and ${predicate} and id != @eventId
         and observed_at >= @start and observed_at <= @observedAt
       order by observed_at desc, id desc
       limit 1`,
    ),
    after: database.prepare(
      `select id as eventId, observed_at as observedAt, ${column} as value
       from buffered_events indexed by idx_events_observed
       where source = 'codex' and ${predicate} and id != @eventId
         and observed_at >= @observedAt and observed_at <= @end
       order by observed_at asc, id asc
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
 * compact candidate/window indexes transactionally through triggers.
 */
export function ensureCodexReconciliationSchema(database: Database.Database) {
  // The first draft mirrored every context row. Migrate that draft state away
  // before rebuilding the raw-event triggers: pending usage is the only
  // historical side state that reconciliation needs.
  database.exec(`
    drop trigger if exists trg_codex_reconciliation_insert;
    drop trigger if exists trg_codex_reconciliation_update;
    drop trigger if exists trg_codex_reconciliation_delete;
    drop index if exists idx_codex_reconciliation_candidates_queue;
    drop index if exists idx_codex_reconciliation_windows_queue;
    drop table if exists codex_reconciliation_context;

    create table if not exists codex_reconciliation_candidates (
      event_id text primary key,
      queued_at text not null,
      priority integer not null default 0
    );

    create table if not exists codex_reconciliation_windows (
      window_start_seconds integer primary key,
      revision integer not null default 1,
      processing_revision integer not null default 0,
      cursor_observed_at text not null default '',
      cursor_event_id text not null default '',
      target_observed_at text not null default '',
      target_event_id text not null default '',
      queued_at text not null,
      updated_at text not null,
      priority integer not null default 1
    );
  `);
  const candidateColumns = new Set(
    (
      database.pragma("table_info(codex_reconciliation_candidates)") as Array<{
        name: string;
      }>
    ).map((column) => column.name),
  );
  if (!candidateColumns.has("priority")) {
    database.transaction(() => {
      database.exec(
        `alter table codex_reconciliation_candidates
         add column priority integer not null default 0;
         update codex_reconciliation_candidates set priority = 1`,
      );
    })();
  }
  const windowColumns = new Set(
    (
      database.pragma("table_info(codex_reconciliation_windows)") as Array<{
        name: string;
      }>
    ).map((column) => column.name),
  );
  if (!windowColumns.has("priority")) {
    database.exec(
      `alter table codex_reconciliation_windows
       add column priority integer not null default 1`,
    );
  }

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
      last_slice_duration_ms real not null default 0,
      max_slice_duration_ms real not null default 0,
      last_time_budget_exhausted integer not null default 0,
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
      queued_at text not null,
      priority integer not null default 0
    );
    create index if not exists idx_codex_reconciliation_candidates_queue
      on codex_reconciliation_candidates (priority, queued_at, event_id);

    create table if not exists codex_reconciliation_windows (
      window_start_seconds integer primary key,
      revision integer not null default 1,
      processing_revision integer not null default 0,
      cursor_observed_at text not null default '',
      cursor_event_id text not null default '',
      target_observed_at text not null default '',
      target_event_id text not null default '',
      queued_at text not null,
      updated_at text not null,
      priority integer not null default 1
    );
    create index if not exists idx_codex_reconciliation_windows_queue
      on codex_reconciliation_windows (priority, queued_at, window_start_seconds);

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
      insert into codex_reconciliation_windows
        (window_start_seconds, revision, processing_revision,
         cursor_observed_at, cursor_event_id, queued_at, updated_at, priority)
      select
        (cast(strftime('%s', new.observed_at) as integer) / 600) * 600,
        1, 0, '', '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'),
        case when new.rowid <= (
          select legacy_target_rowid from codex_reconciliation_control where singleton = 1
        ) and (
          select legacy_complete from codex_reconciliation_control where singleton = 1
        ) = 0 then 1 else 0 end
      where new.source = 'codex'
        and (new.session_id is not null or new.model is not null)
        and strftime('%s', new.observed_at) is not null
      on conflict(window_start_seconds) do update set
        revision = revision + 1,
        priority = min(codex_reconciliation_windows.priority, excluded.priority),
        updated_at = excluded.updated_at;

      insert into codex_reconciliation_pending (event_id, observed_at)
      select new.id, new.observed_at
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null)
      on conflict(event_id) do update set observed_at = excluded.observed_at;

      insert into codex_reconciliation_candidates (event_id, queued_at, priority)
      select new.id, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 0
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null)
      on conflict(event_id) do update set
        priority = min(codex_reconciliation_candidates.priority, excluded.priority);
    end;

    create trigger if not exists trg_codex_reconciliation_update
    after update of source, event_type, observed_at, session_id, model,
      input_tokens, output_tokens, cost_usd on buffered_events
    begin
      insert into codex_reconciliation_windows
        (window_start_seconds, revision, processing_revision,
         cursor_observed_at, cursor_event_id, queued_at, updated_at, priority)
      select
        (cast(strftime('%s', new.observed_at) as integer) / 600) * 600,
        1, 0, '', '', strftime('%Y-%m-%dT%H:%M:%fZ','now'), strftime('%Y-%m-%dT%H:%M:%fZ','now'),
        case when new.rowid <= (
          select legacy_target_rowid from codex_reconciliation_control where singleton = 1
        ) and (
          select legacy_complete from codex_reconciliation_control where singleton = 1
        ) = 0 then 1 else 0 end
      where new.source = 'codex'
        and (new.session_id is not null or new.model is not null)
        and strftime('%s', new.observed_at) is not null
        and (old.source is not new.source or old.observed_at is not new.observed_at
          or old.session_id is not new.session_id or old.model is not new.model)
      on conflict(window_start_seconds) do update set
        revision = revision + 1,
        priority = min(codex_reconciliation_windows.priority, excluded.priority),
        updated_at = excluded.updated_at;

      delete from codex_reconciliation_pending where event_id = new.id;
      insert into codex_reconciliation_pending (event_id, observed_at)
      select new.id, new.observed_at
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null);

      insert into codex_reconciliation_candidates (event_id, queued_at, priority)
      select new.id, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 0
      where new.source = 'codex' and new.event_type = 'assistant_response'
        and (new.input_tokens is not null or new.output_tokens is not null)
        and (new.session_id is null or new.model is null or new.cost_usd is null)
      on conflict(event_id) do update set
        priority = min(codex_reconciliation_candidates.priority, excluded.priority);
    end;

    create trigger if not exists trg_codex_reconciliation_delete
    after delete on buffered_events
    begin
      delete from codex_reconciliation_pending where event_id = old.id;
      delete from codex_reconciliation_candidates where event_id = old.id;
    end;

    -- Snapshot the pre-existing ledger at constructor time. Rows appended
    -- after this point are already trigger-maintained and must not be seeded a
    -- second time by the legacy walker.
    update codex_reconciliation_control set
      legacy_target_rowid = (select coalesce(max(rowid), 0) from buffered_events),
      legacy_complete = case
        when (select coalesce(max(rowid), 0) from buffered_events) = 0 then 1
        else 0
      end,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    where singleton = 1
      and legacy_cursor_rowid = 0
      and legacy_target_rowid = 0
      and legacy_complete = 0;
  `);

  const controlColumns = new Set(
    (
      database.pragma("table_info(codex_reconciliation_control)") as Array<{
        name: string;
      }>
    ).map((column) => column.name),
  );
  const controlMigrations = [
    "last_slice_duration_ms real not null default 0",
    "max_slice_duration_ms real not null default 0",
    "last_time_budget_exhausted integer not null default 0",
  ];
  for (const definition of controlMigrations) {
    const name = definition.split(" ")[0]!;
    if (!controlColumns.has(name)) {
      database.exec(`alter table codex_reconciliation_control add column ${definition}`);
    }
  }
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
         last_slice_duration_ms as lastSliceDurationMs,
         max_slice_duration_ms as maxSliceDurationMs,
         last_time_budget_exhausted as lastTimeBudgetExhausted,
         last_success_at as lastSuccessAt,
         degraded_reason as degradedReason
       from codex_reconciliation_control where singleton = ?`,
    )
    .get(CONTROL_ROW) as Omit<
    CodexReconciliationStatus,
    "legacyComplete" | "lastTimeBudgetExhausted"
  > & {
    legacyComplete: number;
    lastTimeBudgetExhausted: number;
  };
  return {
    ...row,
    legacyComplete: row.legacyComplete === 1,
    lastTimeBudgetExhausted: row.lastTimeBudgetExhausted === 1,
  };
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
    freshCandidateLimit?: number;
    timeLimitMs?: number;
  } = {},
): CodexReconciliationResult {
  const legacyRowLimit = Math.max(
    1,
    Math.min(options.legacyRowLimit ?? 100_000, 250_000),
  );
  const legacyChunkLimit = Math.max(
    1,
    Math.min(options.legacyChunkLimit ?? 500, 10_000),
  );
  const contextWindowLimit = Math.max(
    1,
    Math.min(options.contextWindowLimit ?? 8, 64),
  );
  const contextRowLimit = Math.max(1, Math.min(options.contextRowLimit ?? 1_000, 10_000));
  const candidateLimit = Math.max(1, Math.min(options.candidateLimit ?? 500, 5_000));
  const freshCandidateLimit = Math.max(
    1,
    Math.min(options.freshCandidateLimit ?? 64, candidateLimit),
  );
  const timeLimitMs = Math.max(1, Math.min(options.timeLimitMs ?? 50, 1_000));
  const sliceStarted = performance.now();
  const deadline = sliceStarted + timeLimitMs;

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
      const selectFreshCandidates = database.prepare(
        `select e.rowid, e.id, e.source, e.event_type as eventType,
           e.observed_at as observedAt, e.session_id as sessionId, e.model,
           e.input_tokens as inputTokens, e.output_tokens as outputTokens,
           e.cache_read_tokens as cacheReadTokens,
           e.cache_creation_tokens as cacheCreationTokens,
           e.cost_usd as costUsd, e.payload_json as payloadJson
         from codex_reconciliation_candidates q
         join buffered_events e on e.id = q.event_id
         where q.priority = 0
         order by q.queued_at, q.event_id
         limit ?`,
      );
      const selectCandidates = database.prepare(
        `select e.rowid, e.id, e.source, e.event_type as eventType,
           e.observed_at as observedAt, e.session_id as sessionId, e.model,
           e.input_tokens as inputTokens, e.output_tokens as outputTokens,
           e.cache_read_tokens as cacheReadTokens,
           e.cache_creation_tokens as cacheCreationTokens,
           e.cost_usd as costUsd, e.payload_json as payloadJson
         from codex_reconciliation_candidates q
         join buffered_events e on e.id = q.event_id
         order by q.priority, q.queued_at, q.event_id
         limit ?`,
      );
      let candidateRowsVisited = 0;
      let rowsChanged = 0;
      let stitched = 0;
      let priced = 0;
      const processCandidates = (freshOnly: boolean, limit: number) => {
        if (limit <= 0 || performance.now() >= deadline) return;
        const rows = (freshOnly ? selectFreshCandidates : selectCandidates).all(
          limit,
        ) as CandidateRow[];
        for (const row of rows) {
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
            rowsChanged += apply.run({ id: row.id, sessionId, model, costUsd, payloadJson })
              .changes;
            if (sessionChanged) stitched += 1;
            if (costChanged) priced += 1;
          }
          // Unresolved rows leave the active queue instead of spinning. They
          // remain in `pending`; a later context-window invalidation requeues.
          removeCandidate.run(row.id);
        }
      };

      // New writes are priority zero and always receive service before any
      // legacy scan. This reservation keeps current work useful even while a
      // dense historical high-water is still advancing.
      processCandidates(true, freshCandidateLimit);

      let contextRowsVisited = 0;
      const windows = (performance.now() < deadline
        ? database
            .prepare(
              `select window_start_seconds as windowStartSeconds,
                 revision, processing_revision as processingRevision,
                 cursor_observed_at as cursorObservedAt,
                 cursor_event_id as cursorEventId,
                 target_observed_at as targetObservedAt,
                 target_event_id as targetEventId, priority
               from codex_reconciliation_windows
               order by priority, queued_at, window_start_seconds
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
        priority: number;
      }>;
      const enqueueCandidate = database.prepare(
        `insert into codex_reconciliation_candidates (event_id, queued_at, priority)
         values (?, ?, ?)
         on conflict(event_id) do update set
           priority = min(codex_reconciliation_candidates.priority, excluded.priority)`,
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
        for (const row of rows) enqueueCandidate.run(row.eventId, now, window.priority);
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

      // Window invalidation may have promoted old pending work to priority
      // zero. Drain it before giving the remainder of the slice to history.
      processCandidates(false, candidateLimit - candidateRowsVisited);

      let legacyRowsVisited = 0;
      if (control.legacyComplete !== 1 && performance.now() < deadline) {
        const legacySeedStatements: LegacySeedStatements = {
          upsertPending: database.prepare(
            `insert into codex_reconciliation_pending (event_id, observed_at)
             values (?, ?)
             on conflict(event_id) do update set observed_at = excluded.observed_at`,
          ),
          enqueueCandidate: database.prepare(
            `insert into codex_reconciliation_candidates (event_id, queued_at, priority)
             values (?, ?, ?)
             on conflict(event_id) do nothing`,
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
          // A query/chunk is the hard unit. Complete the at-most-500-row
          // chunk before advancing its cursor, even if the soft clock expires.
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

      const rowsVisited = legacyRowsVisited + contextRowsVisited + candidateRowsVisited;
      const sliceDurationMs = performance.now() - sliceStarted;
      const timeBudgetExhausted = performance.now() >= deadline;
      database
        .prepare(
          `update codex_reconciliation_control set
             rows_visited = rows_visited + @rowsVisited,
             rows_changed = rows_changed + @rowsChanged,
             last_rows_visited = @rowsVisited,
             last_rows_changed = @rowsChanged,
             last_slice_duration_ms = @sliceDurationMs,
             max_slice_duration_ms = max(max_slice_duration_ms, @sliceDurationMs),
             last_time_budget_exhausted = @timeBudgetExhausted,
             last_success_at = @now,
             degraded_reason = null,
             updated_at = @now
           where singleton = 1`,
        )
        .run({
          rowsVisited,
          rowsChanged,
          sliceDurationMs,
          timeBudgetExhausted: timeBudgetExhausted ? 1 : 0,
          now,
        });
      return {
        backfillComplete: control.legacyComplete === 1,
        legacyRowsVisited,
        contextRowsVisited,
        candidateRowsVisited,
        rowsVisited,
        rowsChanged,
        stitched,
        priced,
        sliceDurationMs,
        timeBudgetExhausted,
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
