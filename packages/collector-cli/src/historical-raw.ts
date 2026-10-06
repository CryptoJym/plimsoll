import type Database from "better-sqlite3";
import type { AiInteractionEvent } from "../../shared/src/index";

const boundaries = new WeakMap<Database.Database, number>();

/** Capture under the first opener's writer lock, before migration or repair.
 * This is an immutable ledger boundary, not the moving upload watermark. */
export function initializeHistoricalRawBoundary(db: Database.Database, rawExists: boolean) {
  db.exec(`create table if not exists collector_historical_raw_boundary (
    singleton integer primary key check(singleton=1),
    historical_high_water_rowid integer not null check(historical_high_water_rowid>=0),
    first_opened_at text not null
  );
  create trigger if not exists trg_historical_raw_boundary_immutable_update
    before update on collector_historical_raw_boundary
    begin select raise(abort,'historical_raw_boundary_is_immutable'); end;
  create trigger if not exists trg_historical_raw_boundary_immutable_delete
    before delete on collector_historical_raw_boundary
    begin select raise(abort,'historical_raw_boundary_is_immutable'); end;
  create table if not exists collector_historical_raw_projection (
    raw_rowid integer primary key,raw_id text not null,raw_created text not null,
    raw_generation text,event_json text not null,duplicate_reason text,paired_id text
  );
  create index if not exists idx_historical_raw_projection_id on collector_historical_raw_projection(raw_id);`);
  // INSERT's conflict arm does not recapture a boundary on a later connection.
  db.prepare(`insert or ignore into collector_historical_raw_boundary
    values (1,${rawExists ? "(select coalesce(max(rowid),0) from buffered_events)" : "0"},?)`)
    .run(new Date().toISOString());
  const highWater = historicalRawHighWater(db);
  if (rawExists && highWater > 0) {
    // Connection-local enforcement also fences less frequent maintenance
    // callers. Released rollback readers do not acquire this TEMP trigger.
    // Protected updates report zero changes instead of rewriting observations.
    db.exec(`create temp trigger if not exists trg_preserve_pre_upgrade_raw
      before update on main.buffered_events when old.rowid<=${highWater}
      begin select raise(ignore); end;`);
  }
  return highWater;
}

export function historicalRawHighWater(db: Database.Database) {
  const cached = boundaries.get(db);
  if (cached !== undefined) return cached;
  const exists = db.prepare(`select 1 from sqlite_master where type='table'
    and name='collector_historical_raw_boundary'`).get();
  if (!exists) return 0;
  const row = db.prepare(`select historical_high_water_rowid as n
    from collector_historical_raw_boundary where singleton=1`).get() as { n: number } | undefined;
  if (!row || !Number.isSafeInteger(row.n) || row.n < 0) throw new Error("historical_raw_boundary_invalid");
  boundaries.set(db, row.n);
  return row.n;
}

export function isHistoricalRaw(db: Database.Database, rowid: number | null | undefined) {
  return rowid !== null && rowid !== undefined && rowid > 0 && rowid <= historicalRawHighWater(db);
}

/** Standalone lifecycle/pairing callers use the same first-open boundary. */
export function ensureHistoricalMutationBoundary(db: Database.Database) {
  if (boundaries.has(db)) return;
  const ensure = () => initializeHistoricalRawBoundary(db,
    Boolean(db.prepare(`select 1 from sqlite_master where type='table' and name='buffered_events'`).get()));
  if (db.inTransaction) ensure(); else db.transaction(ensure).immediate();
}

/** The override is derived state bound to an exact incarnation; native
 * evidence and archived columns still come from the original raw row. */
export function historicalRawProjection(db: Database.Database, rawId: string) {
  if (historicalRawHighWater(db) === 0) return undefined;
  const row = db.prepare(`select p.event_json as payload,p.duplicate_reason as duplicate,p.paired_id as paired
    from collector_historical_raw_projection p join buffered_events e
      on e.rowid=p.raw_rowid and e.id=p.raw_id and e.created_at=p.raw_created
        and e.privacy_generation is p.raw_generation where p.raw_id=?`).get(rawId) as
    { payload: string; duplicate: string | null; paired: string | null } | undefined;
  return row ? { event: JSON.parse(row.payload) as AiInteractionEvent, duplicate: row.duplicate, paired: row.paired } : undefined;
}

export function rememberHistoricalRawProjection(db: Database.Database, rawId: string,
  event: AiInteractionEvent, duplicate: string | null = null, paired?: string) {
  const row = db.prepare(`select rowid as n,created_at as created,privacy_generation as generation
    from buffered_events where id=?`).get(rawId) as { n: number; created: string; generation: string | null } | undefined;
  if (!row || !isHistoricalRaw(db, row.n)) return false;
  db.prepare(`insert into collector_historical_raw_projection values (?,?,?,?,?,?,?)
    on conflict(raw_rowid) do update set raw_id=excluded.raw_id,raw_created=excluded.raw_created,
      raw_generation=excluded.raw_generation,event_json=excluded.event_json,
      duplicate_reason=excluded.duplicate_reason,paired_id=coalesce(excluded.paired_id,paired_id)`)
    .run(row.n,rawId,row.created,row.generation,JSON.stringify(event),duplicate,paired ?? null);
  return true;
}

export type HistoricalRepairHold = "historical_migration_requires_opt_in" |
  "historical_receipt_recovery_requires_opt_in";

/** Indexed preflight, evaluated before any constructor recovery can move its
 * cursor. No automatic path supplies an opt-in or consumes historical work. */
export function historicalRepairHold(db: Database.Database): HistoricalRepairHold | null {
  const highWater = historicalRawHighWater(db);
  if (highWater === 0) return null;
  if (db.prepare(`select 1 from upload_receipts indexed by idx_upload_receipts_raw_lineage
    where terminal_state='dead' and raw_rowid is null and raw_id is null
      and raw_created_at is null and raw_generation is null and rowid>
        (select cursor_rowid from upload_receipt_lineage_backfill where singleton=1) limit 1`).get())
    return "historical_receipt_recovery_requires_opt_in";
  const control = db.prepare(`select migration_cursor_rowid as cursor from upload_control where singleton=1`)
    .get() as { cursor: number };
  return control.cursor < highWater ? "historical_migration_requires_opt_in" : null;
}
