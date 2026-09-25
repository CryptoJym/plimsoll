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
const INTEGRITY_TRIGGERS = [
  "trg_session_repo_contexts_integrity_update",
] as const;
const ALL_TRIGGERS = [...EVENT_TRIGGERS, ...COUNT_TRIGGERS, ...INTEGRITY_TRIGGERS] as const;
const SCHEMA_OBJECTS = [INDEX_TABLE, CONTROL_TABLE, ...ALL_TRIGGERS];

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

/** An order-independent checksum of the primary-key material. */
function keyChecksum(alias: string) {
  return `(coalesce(${alias}.source_rowid, 0) + coalesce(unixepoch(${alias}.observed_at), 0))`;
}

/** The same checksum for a buffered_events source row. */
function eventKeyChecksum(alias: string) {
  return `(coalesce(${alias}.rowid, 0) + coalesce(unixepoch(${alias}.observed_at), 0))`;
}

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
    indexed_rows integer not null default 0 check (indexed_rows >= 0),
    ledger_context_rows integer not null default 0 check (ledger_context_rows >= 0),
    indexed_key_checksum integer not null default 0,
    ledger_key_checksum integer not null default 0,
    integrity_state text not null default 'valid'
      check (integrity_state in ('valid', 'invalid')),
    integrity_failures integer not null default 0 check (integrity_failures >= 0),
    integrity_last_failure_at text,
    integrity_last_failure_reason text
  );

  create trigger if not exists trg_events_session_context_insert
  after insert on buffered_events
  when ${contextRow("new")}
  begin
    insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
    values (new.session_id, new.observed_at, new.rowid, new.repo_hash)
    ${UPSERT_CONTEXT};
    update session_repo_context_control
    set ledger_context_rows = ledger_context_rows + 1,
        ledger_key_checksum = ledger_key_checksum + ${eventKeyChecksum("new")}
    where singleton = 1;
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
    update session_repo_context_control set
      ledger_context_rows = max(0, ledger_context_rows - case when exists (
        select 1 from session_repo_contexts
        where session_id = old.session_id and observed_at = old.observed_at
          and source_rowid = old.rowid
      ) then 1 else 0 end),
      ledger_key_checksum = ledger_key_checksum - case when exists (
        select 1 from session_repo_contexts
        where session_id = old.session_id and observed_at = old.observed_at
          and source_rowid = old.rowid
      ) then ${eventKeyChecksum("old")} else 0 end
    where singleton = 1;
    delete from session_repo_contexts
    where session_id = old.session_id and observed_at = old.observed_at
      and source_rowid = old.rowid;
    insert into session_repo_contexts (session_id, observed_at, source_rowid, repo_hash)
    select new.session_id, new.observed_at, new.rowid, new.repo_hash
    where ${contextRow("new")}
    ${UPSERT_CONTEXT};
    update session_repo_context_control set
      ledger_context_rows = ledger_context_rows + case when ${contextRow("new")} then 1 else 0 end,
      ledger_key_checksum = ledger_key_checksum + case when ${contextRow("new")}
        then ${eventKeyChecksum("new")} else 0 end
    where singleton = 1;
  end;

  create trigger if not exists trg_events_session_context_delete
  after delete on buffered_events
  when old.session_id is not null and old.repo_hash is not null
  begin
    update session_repo_context_control set
      ledger_context_rows = max(0, ledger_context_rows - case when exists (
        select 1 from session_repo_contexts
        where session_id = old.session_id and observed_at = old.observed_at
          and source_rowid = old.rowid
      ) then 1 else 0 end),
      ledger_key_checksum = ledger_key_checksum - case when exists (
        select 1 from session_repo_contexts
        where session_id = old.session_id and observed_at = old.observed_at
          and source_rowid = old.rowid
      ) then ${eventKeyChecksum("old")} else 0 end
    where singleton = 1;
    delete from session_repo_contexts
    where session_id = old.session_id and observed_at = old.observed_at
      and source_rowid = old.rowid;
  end;

  create trigger if not exists trg_session_repo_contexts_count_insert
  after insert on session_repo_contexts
  begin
    update session_repo_context_control set
      indexed_rows = indexed_rows + 1,
      indexed_key_checksum = indexed_key_checksum + ${keyChecksum("new")}
    where singleton = 1;
  end;

  create trigger if not exists trg_session_repo_contexts_count_delete
  after delete on session_repo_contexts
  begin
    update session_repo_context_control set
      indexed_rows = max(0, indexed_rows - 1),
      indexed_key_checksum = indexed_key_checksum - ${keyChecksum("old")}
    where singleton = 1;
  end;

  create trigger if not exists trg_session_repo_contexts_integrity_update
  after update on session_repo_contexts
  begin
    update session_repo_context_control set
      integrity_state = 'invalid',
      backfill_complete = 0,
      backfill_completed_at = null,
      integrity_failures = integrity_failures + case when integrity_state = 'valid' then 1 else 0 end,
      integrity_last_failure_at = case when integrity_state = 'valid'
        then strftime('%Y-%m-%dT%H:%M:%fZ', 'now') else integrity_last_failure_at end,
      integrity_last_failure_reason = case when integrity_state = 'valid'
        then 'session_repo_contexts_updated' else integrity_last_failure_reason end
    where singleton = 1;
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
  ledgerContextRows: number;
  checksumsEqual: number;
  integrityState: "valid" | "invalid";
  integrityFailures: number;
  integrityLastFailureAt: string | null;
  integrityLastFailureReason: string | null;
};

function auxiliaryIndexError(error: unknown) {
  return error instanceof Error &&
    /(?:no such (?:table|column)|malformed database schema)/i.test(error.message);
}

function readControl(db: Database.Database): ControlRow | null {
  try {
    return (db.prepare(
      `select installed_at as installedAt, backfill_complete as complete,
         backfill_cursor_repo_hash as cursorRepoHash,
         backfill_cursor_branch_hash as cursorBranchHash,
         backfill_cursor_rowid as cursorRowid,
         backfill_rows_visited as rowsVisited, backfill_rows_indexed as rowsIndexed,
         backfill_batches as batches, backfill_last_batch_at as lastBatchAt,
         backfill_completed_at as completedAt, indexed_rows as indexedRows,
         ledger_context_rows as ledgerContextRows,
         indexed_key_checksum = ledger_key_checksum as checksumsEqual,
         integrity_state as integrityState,
         integrity_failures as integrityFailures,
         integrity_last_failure_at as integrityLastFailureAt,
         integrity_last_failure_reason as integrityLastFailureReason
       from session_repo_context_control where singleton = 1`,
    ).get() as ControlRow | undefined) ?? null;
  } catch (error) {
    // A ledger this build has not opened read-write yet (e.g. a read-only
    // history upload of an older ledger) has no index: use the 0.7.36 path.
    if (auxiliaryIndexError(error)) return null;
    throw error;
  }
}

function indexTableUsable(db: Database.Database) {
  try {
    // LIMIT 0 validates the auxiliary object's shape without reading data.
    db.prepare(
      `select session_id, observed_at, source_rowid, repo_hash
       from session_repo_contexts limit 0`,
    ).all();
    const columns = db.prepare(`pragma table_info(session_repo_contexts)`).all() as Array<{
      name: string; type: string; notnull: number; pk: number;
    }>;
    const expected = [
      { name: "session_id", type: "TEXT", notnull: 1, pk: 1 },
      { name: "observed_at", type: "TEXT", notnull: 1, pk: 2 },
      { name: "source_rowid", type: "INTEGER", notnull: 1, pk: 3 },
      { name: "repo_hash", type: "TEXT", notnull: 1, pk: 0 },
    ];
    return columns.length === expected.length && columns.every((column, index) => {
      const required = expected[index]!;
      return column.name === required.name && column.type.toUpperCase() === required.type &&
        column.notnull === required.notnull && column.pk === required.pk;
    });
  } catch (error) {
    if (auxiliaryIndexError(error)) return false;
    throw error;
  }
}

function countsAgree(control: ControlRow) {
  return control.indexedRows === control.ledgerContextRows;
}

function checksumsAgree(control: ControlRow) {
  return control.checksumsEqual === 1;
}

function controlTableUsable(db: Database.Database) {
  try {
    const columns = db.prepare(`pragma table_info(session_repo_context_control)`).all() as Array<{ name: string }>;
    const names = new Set(columns.map((column) => column.name));
    return [
      "singleton", "installed_at", "backfill_complete", "backfill_cursor_repo_hash",
      "backfill_cursor_branch_hash", "backfill_cursor_rowid", "backfill_rows_visited",
      "backfill_rows_indexed", "backfill_batches", "backfill_last_batch_at",
      "backfill_completed_at", "indexed_rows", "ledger_context_rows", "indexed_key_checksum",
      "ledger_key_checksum", "integrity_state", "integrity_failures", "integrity_last_failure_at",
      "integrity_last_failure_reason",
    ].every((name) => names.has(name));
  } catch (error) {
    if (auxiliaryIndexError(error)) return false;
    throw error;
  }
}

function recomputedIndexChecksumAgrees(db: Database.Database) {
  // SQLite compares INTEGER values exactly. Reading either checksum into a
  // JavaScript number would round away one-unit drift above 2^53.
  const row = db.prepare(
    `select coalesce(sum(${keyChecksum("session_repo_contexts")}), 0) =
         (select indexed_key_checksum from session_repo_context_control where singleton = 1) as agrees
       from session_repo_contexts`,
  ).get() as { agrees: number };
  return row.agrees === 1;
}

// A stale row can be discovered from a read-only history connection. Keep an
// in-process marker for that connection, and persist the same state whenever
// the database is writable. The warning is deliberately once per connection.
const invalidDatabases = new WeakSet<Database.Database>();
// The aggregate checksum is an open/reopen validation, not a per-lookup scan.
// After that proof, trigger-maintained counts and checksums make each lookup
// O(1); a new connection validates the aggregate again.
const validatedIndexes = new WeakSet<Database.Database>();

export function markSessionContextIndexInvalid(db: Database.Database, reason: string) {
  const first = !invalidDatabases.has(db);
  invalidDatabases.add(db);
  validatedIndexes.delete(db);
  try {
    db.prepare(
      `update session_repo_context_control set
         integrity_state = 'invalid',
         backfill_complete = 0,
         backfill_completed_at = null,
         integrity_failures = integrity_failures + case when integrity_state = 'valid' then 1 else 0 end,
         integrity_last_failure_at = case when integrity_state = 'valid' then @at else integrity_last_failure_at end,
         integrity_last_failure_reason = case when integrity_state = 'valid' then @reason else integrity_last_failure_reason end
       where singleton = 1`,
    ).run({ at: new Date().toISOString(), reason });
  } catch {
    // Read-only history and a missing/partially-corrupt auxiliary table must
    // fail closed, never make the attribution read throw.
  }
  if (first) console.warn(JSON.stringify({ status: "session_context_index_invalid", reason }));
}

function invariantAgrees(db: Database.Database, control: ControlRow) {
  if (!countsAgree(control) || !checksumsAgree(control)) return false;
  // A backfilling index is never used for attribution, so its maintained
  // checksum is enough. Complete indexes get an aggregate point validation;
  // this catches an out-of-band UPDATE that did not fire a checksum trigger.
  if (control.complete !== 1) return true;
  if (validatedIndexes.has(db)) return true;
  if (!recomputedIndexChecksumAgrees(db)) return false;
  validatedIndexes.add(db);
  return true;
}

const TRIGGER_FRAGMENTS: Record<
  typeof EVENT_TRIGGERS[number] | typeof COUNT_TRIGGERS[number] | typeof INTEGRITY_TRIGGERS[number], readonly string[]
> = {
  trg_events_session_context_insert: ["after insert on buffered_events", "insert into session_repo_contexts", "ledger_context_rows = ledger_context_rows + 1"],
  trg_events_session_context_update: ["after update of session_id", "delete from session_repo_contexts", "ledger_context_rows"],
  trg_events_session_context_delete: ["after delete on buffered_events", "delete from session_repo_contexts", "ledger_context_rows"],
  trg_session_repo_contexts_count_insert: ["after insert on session_repo_contexts", "indexed_rows = indexed_rows + 1", "indexed_key_checksum"],
  trg_session_repo_contexts_count_delete: ["after delete on session_repo_contexts", "indexed_rows = max(0, indexed_rows - 1)", "indexed_key_checksum"],
  trg_session_repo_contexts_integrity_update: ["after update on session_repo_contexts", "integrity_state = 'invalid'", "backfill_completed_at = null"],
};

function auxiliaryObjectsUsable(db: Database.Database) {
  try {
    const present = db.prepare(
      `select name, type, sql from sqlite_master
       where name in (${SCHEMA_OBJECTS.map(() => "?").join(", ")})`,
    ).all(...SCHEMA_OBJECTS) as Array<{ name: string; type: string; sql: string | null }>;
    const types = new Map(present.map((row) => [row.name, row.type]));
    const objectsPresent = types.get(INDEX_TABLE) === "table" && types.get(CONTROL_TABLE) === "table" &&
      ALL_TRIGGERS.every((name) => types.get(name) === "trigger");
    if (!objectsPresent || !indexTableUsable(db) || !controlTableUsable(db)) return false;
    const definitions = new Map(present.map((row) => [row.name, row.sql?.toLowerCase() ?? ""]));
    return (Object.entries(TRIGGER_FRAGMENTS) as Array<[
      typeof EVENT_TRIGGERS[number] | typeof COUNT_TRIGGERS[number] | typeof INTEGRITY_TRIGGERS[number], readonly string[]
    ]>).every(
      ([name, fragments]) => fragments.every((fragment) => definitions.get(name)?.includes(fragment)),
    );
  } catch (error) {
    if (auxiliaryIndexError(error)) return false;
    throw error;
  }
}

function installed(db: Database.Database) {
  if (invalidDatabases.has(db)) return false;
  if (!auxiliaryObjectsUsable(db)) return false;
  const control = readControl(db);
  return control !== null && control.integrityState === "valid" && countsAgree(control) && checksumsAgree(control);
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
      ${ALL_TRIGGERS.map((name) => `drop trigger if exists ${name};`).join("\n")}`);
    db.exec(`drop table if exists ${CONTROL_TABLE};`);
    db.exec(SCHEMA);
    const backfillNeeded = Boolean(db.prepare(
      `select 1 from buffered_events indexed by idx_events_repo
       where repo_hash is not null limit 1`,
    ).get());
    const at = now().toISOString();
    db.prepare(
      `insert into session_repo_context_control
         (singleton, installed_at, backfill_complete, backfill_completed_at,
          indexed_rows, ledger_context_rows, indexed_key_checksum, ledger_key_checksum,
          integrity_state, integrity_failures, integrity_last_failure_at, integrity_last_failure_reason)
       values (1, @at, @complete, @completedAt, 0, 0, 0, 0, 'valid', 0, null, null)
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
         indexed_rows = 0,
         ledger_context_rows = 0,
         indexed_key_checksum = 0,
         ledger_key_checksum = 0,
         integrity_state = 'valid',
         integrity_failures = 0,
         integrity_last_failure_at = null,
         integrity_last_failure_reason = null`,
    ).run({
      at,
      complete: backfillNeeded ? 0 : 1,
      completedAt: backfillNeeded ? null : at,
    });
  }).immediate();
  // A writable reopen may repair an index that a read-only connection marked
  // invalid in-process. The new schema has a fresh aggregate validation.
  invalidDatabases.delete(db);
  validatedIndexes.delete(db);
}

export type SessionContextIndexState = "absent" | "backfilling" | "complete" | "invalid";

function stateOf(control: ControlRow | null): SessionContextIndexState {
  return control === null ? "absent" : control.integrityState === "invalid"
    ? "invalid" : control.complete === 1 ? "complete" : "backfilling";
}

function verifiedState(db: Database.Database, control: ControlRow | null): SessionContextIndexState {
  if (control === null || !auxiliaryObjectsUsable(db)) return "absent";
  if (invalidDatabases.has(db) || control.integrityState === "invalid") {
    if (!invalidDatabases.has(db)) markSessionContextIndexInvalid(
      db, control.integrityLastFailureReason ?? "index_marked_invalid",
    );
    return "invalid";
  }
  if (!invariantAgrees(db, control)) {
    markSessionContextIndexInvalid(db, "index_invariant_mismatch");
    return "invalid";
  }
  return stateOf(control);
}

/** One control-row read. */
export function sessionContextIndexState(db: Database.Database) {
  return verifiedState(db, readControl(db));
}

/**
 * True only when the index is installed and its backfill has covered every
 * row that predates it. Read inside the caller's snapshot.
 */
export function sessionContextIndexComplete(db: Database.Database) {
  return verifiedState(db, readControl(db)) === "complete";
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

export type SessionContextBackfillOptions = {
  now?: () => Date;
  /**
   * Called before each row. Returning false leaves the unvisited rows for the
   * next maintenance slice; rows already visited remain in this transaction.
   */
  shouldContinue?: () => boolean;
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
  options: SessionContextBackfillOptions | (() => Date) = {},
): SessionContextBackfillBatch {
  // Keep the original third-argument clock callback source-compatible for
  // maintenance callers outside this package; the stage uses the richer
  // object so it can also stop inside the row loop.
  const backfillOptions: SessionContextBackfillOptions = typeof options === "function"
    ? { now: options }
    : options;
  const before = verifiedState(db, readControl(db));
  if (before !== "backfilling") return { state: before, visited: 0, indexed: 0 };
  const limit = Math.max(1, Math.min(Math.trunc(maxRows) || 1, SESSION_CONTEXT_BACKFILL_MAX_BATCH_ROWS));
  return db.transaction((): SessionContextBackfillBatch => {
    const control = readControl(db);
    const current = verifiedState(db, control);
    if (current !== "backfilling") return { state: current, visited: 0, indexed: 0 };
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
       on conflict (session_id, observed_at, source_rowid) do nothing`,
    );
    const sourceChecksum = db.prepare(
      `select coalesce(rowid, 0) + coalesce(unixepoch(observed_at), 0) as checksum
         from buffered_events where rowid = ?`,
    ).safeIntegers();
    let indexed = 0;
    // SQLite's checksum is an INTEGER. Keep the delta as a bigint all the
    // way through the write so better-sqlite3 binds an INTEGER instead of a
    // lossy REAL once the ledger crosses JavaScript's 2^53 safe-integer limit.
    let indexedChecksum = 0n;
    let visited = 0;
    for (const row of rows) {
      if (backfillOptions.shouldContinue && !backfillOptions.shouldContinue()) break;
      const changes = index.run(row.rowid).changes;
      indexed += changes;
      if (changes > 0) {
        indexedChecksum += (sourceChecksum.get(row.rowid) as { checksum: bigint } | undefined)?.checksum ?? 0n;
      }
      visited += 1;
    }
    // A clock can expire before the first row. Do not advance the cursor or
    // manufacture a maintenance batch in that case. The surrounding stage
    // will stop immediately and retry the same rows next time.
    if (visited === 0 && rows.length > 0) {
      return { state: current, visited: 0, indexed: 0 };
    }
    if (indexed > 0) {
      db.prepare(
        `update session_repo_context_control
         set ledger_context_rows = ledger_context_rows + ?,
             ledger_key_checksum = ledger_key_checksum + ?
         where singleton = 1`,
      ).run(indexed, indexedChecksum);
    }
    const complete = visited === rows.length && rows.length < limit;
    const last = complete ? null : visited > 0 ? rows[visited - 1]! : cursor;
    const at = (backfillOptions.now ?? (() => new Date()))().toISOString();
    db.prepare(
      `update session_repo_context_control set
         backfill_cursor_repo_hash = @repoHash,
         backfill_cursor_branch_hash = @branchHash,
         backfill_cursor_rowid = @rowid,
         backfill_rows_visited = backfill_rows_visited + @visited,
         backfill_rows_indexed = backfill_rows_indexed + @indexed,
         backfill_batches = backfill_batches + 1,
         backfill_last_batch_at = @at,
         backfill_complete = case
           when @complete = 1 and indexed_rows = ledger_context_rows
             and indexed_key_checksum = ledger_key_checksum then 1 else 0 end,
         backfill_completed_at = case
           when @complete = 1 and indexed_rows = ledger_context_rows
             and indexed_key_checksum = ledger_key_checksum then @at else null end
       where singleton = 1`,
    ).run({
      repoHash: last?.repoHash ?? null,
      branchHash: last?.branchHash ?? null,
      rowid: last?.rowid ?? null,
      visited,
      indexed,
      at,
      complete: complete ? 1 : 0,
    });
    return {
      state: verifiedState(db, readControl(db)),
      visited,
      indexed,
    };
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
  const state = verifiedState(db, control);
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
      // A stale completion marker must never accompany an invalid/absent
      // state. Writable reopen will rebuild; read-only callers stay honest.
      completedAt: state === "complete" ? control?.completedAt ?? null : null,
    },
  };
}
