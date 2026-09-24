import type Database from "better-sqlite3";

/**
 * Capture-time project evidence per session (eco-6hoxj.163.21).
 *
 * Session inheritance may only use ledger rows that carry a session and a
 * repo_hash and are neither local evidence nor privacy-disposed. Those rows
 * are a small fraction of the ledger, so `session_repo_contexts` holds just
 * them, keyed for the inheritance window query. Triggers write it in the same
 * transaction as every insert, update and delete of the columns that decide
 * membership, whatever the code path (capture, repo-context fill and cleanup,
 * privacy disposition, quarantine, retention). A lookup then costs the context
 * rows in its window, not every event the session produced.
 *
 * A ledger that predates the index is backfilled by bounded maintenance
 * batches that walk only repo-bearing rows, in `idx_events_repo` order. Until
 * that walk completes, `sessionContextIndexComplete` is false and inheritance
 * keeps the bounded 0.7.36 session scan for every window.
 */

const INDEX_TABLE = "session_repo_contexts";
const CONTROL_TABLE = "session_repo_context_control";
const EVENT_TRIGGERS = [
  "trg_events_session_context_insert",
  "trg_events_session_context_update",
  "trg_events_session_context_delete",
] as const;
const COUNT_TRIGGERS = [
  "trg_session_repo_contexts_count_insert",
  "trg_session_repo_contexts_count_delete",
] as const;
const SCHEMA_OBJECTS = [INDEX_TABLE, CONTROL_TABLE, ...EVENT_TRIGGERS, ...COUNT_TRIGGERS];

/** Rows one backfill transaction may visit. */
export const SESSION_CONTEXT_BACKFILL_MAX_BATCH_ROWS = 5_000;

/** The 0.7.36 context predicate, verbatim, over one buffered_events row. */
function contextRow(alias: string) {
  return `${alias}.session_id is not null and ${alias}.repo_hash is not null
    and ${alias}.data_mode <> 'evidence' and ${alias}.privacy_disposition is null`;
}

const UPSERT_CONTEXT = `on conflict (session_id, observed_at, source_rowid)
  do update set repo_hash = excluded.repo_hash
  where session_repo_contexts.repo_hash is not excluded.repo_hash`;

const SCHEMA = `
  create table if not exists session_repo_contexts (
    session_id text not null,
    observed_at text not null,
    source_rowid integer not null,
    repo_hash text not null,
    primary key (session_id, observed_at, source_rowid)
  ) without rowid;
  create table if not exists session_repo_context_control (
    singleton integer primary key check (singleton = 1),
    installed_at text not null,
    backfill_complete integer not null check (backfill_complete in (0, 1)),
    backfill_cursor_repo_hash text,
    backfill_cursor_branch_hash text,
    backfill_cursor_rowid integer,
    backfill_rows_visited integer not null default 0 check (backfill_rows_visited >= 0),
    backfill_rows_indexed integer not null default 0 check (backfill_rows_indexed >= 0),
    backfill_batches integer not null default 0 check (backfill_batches >= 0),
    backfill_last_batch_at text,
    backfill_completed_at text,
    indexed_rows integer not null default 0
  );

  create trigger if not exists trg_events_session_context_insert
  after insert on buffered_events
  when ${contextRow("new")}
  begin
    insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
    values (new.session_id, new.observed_at, new.rowid, new.repo_hash)
    ${UPSERT_CONTEXT};
  end;

  create trigger if not exists trg_events_session_context_update
  after update of session_id, observed_at, repo_hash, data_mode, privacy_disposition
  on buffered_events
  when ((old.session_id is not null and old.repo_hash is not null) or
      (new.session_id is not null and new.repo_hash is not null))
    and (old.session_id is not new.session_id or old.observed_at is not new.observed_at or
      old.repo_hash is not new.repo_hash or old.data_mode is not new.data_mode or
      old.privacy_disposition is not new.privacy_disposition)
  begin
    delete from session_repo_contexts
    where session_id = old.session_id and observed_at = old.observed_at
      and source_rowid = old.rowid;
    insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
    select new.session_id, new.observed_at, new.rowid, new.repo_hash
    where ${contextRow("new")}
    ${UPSERT_CONTEXT};
  end;

  create trigger if not exists trg_events_session_context_delete
  after delete on buffered_events
  when old.session_id is not null and old.repo_hash is not null
  begin
    delete from session_repo_contexts
    where session_id = old.session_id and observed_at = old.observed_at
      and source_rowid = old.rowid;
  end;

  create trigger if not exists trg_session_repo_contexts_count_insert
  after insert on session_repo_contexts
  begin
    update session_repo_context_control set indexed_rows = indexed_rows + 1 where singleton = 1;
  end;

  create trigger if not exists trg_session_repo_contexts_count_delete
  after delete on session_repo_contexts
  begin
    update session_repo_context_control set indexed_rows = indexed_rows - 1 where singleton = 1;
  end;
`;

type ControlRow = {
  installedAt: string;
  complete: number;
  cursorRepoHash: unknown;
  cursorBranchHash: unknown;
  cursorRowid: number | null;
  rowsVisited: number;
  rowsIndexed: number;
  batches: number;
  lastBatchAt: string | null;
  completedAt: string | null;
  indexedRows: number;
};

function readControl(db: Database.Database): ControlRow | null {
  try {
    return (db.prepare(
      `select installed_at as installedAt, backfill_complete as complete,
         backfill_cursor_repo_hash as cursorRepoHash,
         backfill_cursor_branch_hash as cursorBranchHash,
         backfill_cursor_rowid as cursorRowid,
         backfill_rows_visited as rowsVisited, backfill_rows_indexed as rowsIndexed,
         backfill_batches as batches, backfill_last_batch_at as lastBatchAt,
         backfill_completed_at as completedAt, indexed_rows as indexedRows
       from session_repo_context_control where singleton = 1`,
    ).get() as ControlRow | undefined) ?? null;
  } catch (error) {
    // A ledger this build has not opened read-write yet (e.g. a read-only
    // history upload of an older ledger) has no index: use the 0.7.36 path.
    if (error instanceof Error && /no such table/.test(error.message)) return null;
    throw error;
  }
}

function installed(db: Database.Database) {
  const present = db.prepare(
    `select count(*) as n from sqlite_master
     where name in (${SCHEMA_OBJECTS.map(() => "?").join(", ")})`,
  ).get(...SCHEMA_OBJECTS) as { n: number };
  return present.n === SCHEMA_OBJECTS.length && readControl(db) !== null;
}

/**
 * Called on every ledger open. An installed index costs one schema read.
 * First install is constant-time: it creates an empty table and its triggers
 * and records whether any repo-bearing row exists (one indexed probe); the
 * backfill itself only ever runs in maintenance. Creating the triggers and
 * recording the coverage marker share one write transaction, so no row can
 * be written between them unseen by both the triggers and the backfill. If
 * any piece is missing (writes may have bypassed the index), the index is
 * rebuilt from empty and backfilled again.
 */
export function ensureSessionContextIndexSchema(
  db: Database.Database,
  now: () => Date = () => new Date(),
) {
  if (installed(db)) return;
  db.transaction(() => {
    if (installed(db)) return;
    db.exec(`drop table if exists ${INDEX_TABLE};
      ${EVENT_TRIGGERS.map((name) => `drop trigger if exists ${name};`).join("\n")}`);
    db.exec(SCHEMA);
    const backfillNeeded = Boolean(db.prepare(
      `select 1 from buffered_events indexed by idx_events_repo
       where repo_hash is not null limit 1`,
    ).get());
    const at = now().toISOString();
    db.prepare(
      `insert into session_repo_context_control
         (singleton, installed_at, backfill_complete, backfill_completed_at, indexed_rows)
       values (1, @at, @complete, @completedAt, 0)
       on conflict (singleton) do update set
         installed_at = excluded.installed_at,
         backfill_complete = excluded.backfill_complete,
         backfill_cursor_repo_hash = null,
         backfill_cursor_branch_hash = null,
         backfill_cursor_rowid = null,
         backfill_rows_visited = 0,
         backfill_rows_indexed = 0,
         backfill_batches = 0,
         backfill_last_batch_at = null,
         backfill_completed_at = excluded.backfill_completed_at,
         indexed_rows = 0`,
    ).run({
      at,
      complete: backfillNeeded ? 0 : 1,
      completedAt: backfillNeeded ? null : at,
    });
  }).immediate();
}

export type SessionContextIndexState = "absent" | "backfilling" | "complete";

function stateOf(control: ControlRow | null): SessionContextIndexState {
  return control === null ? "absent" : control.complete === 1 ? "complete" : "backfilling";
}

/** One control-row read. */
export function sessionContextIndexState(db: Database.Database) {
  return stateOf(readControl(db));
}

/**
 * True only when the index is installed and its backfill has covered every
 * row that predates it. Read inside the caller's snapshot.
 */
export function sessionContextIndexComplete(db: Database.Database) {
  return sessionContextIndexState(db) === "complete";
}

type RepoKey = { rowid: number; repoHash: unknown; branchHash: unknown };

const REPO_WALK = `select rowid, repo_hash as repoHash, branch_hash as branchHash
  from buffered_events indexed by idx_events_repo where`;
const REPO_WALK_ORDER = `order by repo_hash, branch_hash, rowid limit ?`;

/**
 * The next `limit` repo-bearing rows after `cursor` in idx_events_repo order
 * (repo_hash, branch_hash with NULL first, rowid). Every step is a covering
 * index range; NULL branch hashes get their own steps because a NULL never
 * compares greater than a cursor value.
 */
function nextRepoRows(db: Database.Database, cursor: RepoKey | null, limit: number) {
  const steps: Array<[string, unknown[]]> = cursor === null
    ? [["repo_hash is not null", []]]
    : [
        cursor.branchHash === null
          ? ["repo_hash = ? and branch_hash is null and rowid > ?", [cursor.repoHash, cursor.rowid]]
          : ["repo_hash = ? and branch_hash = ? and rowid > ?", [cursor.repoHash, cursor.branchHash, cursor.rowid]],
        cursor.branchHash === null
          ? ["repo_hash = ? and branch_hash is not null", [cursor.repoHash]]
          : ["repo_hash = ? and branch_hash > ?", [cursor.repoHash, cursor.branchHash]],
        ["repo_hash > ?", [cursor.repoHash]],
      ];
  const rows: RepoKey[] = [];
  for (const [where, params] of steps) {
    if (rows.length >= limit) break;
    rows.push(...db.prepare(`${REPO_WALK} ${where} ${REPO_WALK_ORDER}`)
      .all(...params, limit - rows.length) as RepoKey[]);
  }
  return rows;
}

export type SessionContextBackfillBatch = {
  state: SessionContextIndexState;
  visited: number;
  indexed: number;
};

/**
 * One backfill transaction: visit at most `maxRows` repo-bearing rows after
 * the stored cursor, index the ones that are session context, and advance the
 * cursor. Rows written after install are already indexed by the triggers; a
 * repeated visit is an idempotent upsert. The walk completes when a batch
 * comes back short. A complete or absent index takes no write lock.
 */
export function backfillSessionContextIndex(
  db: Database.Database,
  maxRows: number,
  now: () => Date = () => new Date(),
): SessionContextBackfillBatch {
  const before = stateOf(readControl(db));
  if (before !== "backfilling") return { state: before, visited: 0, indexed: 0 };
  const limit = Math.max(1, Math.min(Math.trunc(maxRows) || 1, SESSION_CONTEXT_BACKFILL_MAX_BATCH_ROWS));
  return db.transaction((): SessionContextBackfillBatch => {
    const control = readControl(db);
    if (stateOf(control) !== "backfilling") return { state: stateOf(control), visited: 0, indexed: 0 };
    const cursor = control!.cursorRowid === null ? null : {
      rowid: control!.cursorRowid,
      repoHash: control!.cursorRepoHash,
      branchHash: control!.cursorBranchHash,
    };
    const rows = nextRepoRows(db, cursor, limit);
    const index = db.prepare(
      `insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
       select e.session_id, e.observed_at, e.rowid, e.repo_hash
       from buffered_events e where e.rowid = ? and ${contextRow("e")}
       ${UPSERT_CONTEXT}`,
    );
    let indexed = 0;
    for (const row of rows) indexed += index.run(row.rowid).changes;
    const complete = rows.length < limit;
    const last = complete ? null : rows[rows.length - 1]!;
    const at = now().toISOString();
    db.prepare(
      `update session_repo_context_control set
         backfill_cursor_repo_hash = @repoHash,
         backfill_cursor_branch_hash = @branchHash,
         backfill_cursor_rowid = @rowid,
         backfill_rows_visited = backfill_rows_visited + @visited,
         backfill_rows_indexed = backfill_rows_indexed + @indexed,
         backfill_batches = backfill_batches + 1,
         backfill_last_batch_at = @at,
         backfill_complete = @complete,
         backfill_completed_at = case when @complete = 1 then @at else null end
       where singleton = 1`,
    ).run({
      repoHash: last?.repoHash ?? null,
      branchHash: last?.branchHash ?? null,
      rowid: last?.rowid ?? null,
      visited: rows.length,
      indexed,
      at,
      complete: complete ? 1 : 0,
    });
    return { state: complete ? "complete" : "backfilling", visited: rows.length, indexed };
  }).immediate();
}

export type SessionContextIndexStatus = {
  state: SessionContextIndexState;
  /** Where new upload batches read session context. */
  inheritanceSource: "context_index" | "session_scan";
  /** Rows in session_repo_contexts (a trigger-maintained count). */
  indexedRows: number | null;
  backfill: {
    complete: boolean;
    rowsVisited: number;
    rowsIndexed: number;
    batches: number;
    installedAt: string | null;
    lastBatchAt: string | null;
    completedAt: string | null;
  };
};

/** One control-row read; safe on the status refresh path. */
export function sessionContextIndexStatus(db: Database.Database): SessionContextIndexStatus {
  const control = readControl(db);
  const state = stateOf(control);
  return {
    state,
    inheritanceSource: state === "complete" ? "context_index" : "session_scan",
    indexedRows: control?.indexedRows ?? null,
    backfill: {
      complete: state === "complete",
      rowsVisited: control?.rowsVisited ?? 0,
      rowsIndexed: control?.rowsIndexed ?? 0,
      batches: control?.batches ?? 0,
      installedAt: control?.installedAt ?? null,
      lastBatchAt: control?.lastBatchAt ?? null,
      completedAt: control?.completedAt ?? null,
    },
  };
}
