import type Database from "better-sqlite3";

export type RuntimeFactDropReason =
  | "missing_session"
  | "episode_seed_failed"
  | "invalid_signal"
  | "identity_conflict"
  | "unpaired_result"
  | "retry_target_missing"
  | "capacity_exceeded"
  | "stale_reference";

type DropRow = { reason: RuntimeFactDropReason; droppedCount: number; lastDroppedAt: string };

export function ensureRuntimeFactDropSchema(db: Database.Database) {
  db.exec(`
    create table if not exists runtime_fact_drops (
      reason text primary key,
      dropped_count integer not null check(dropped_count >= 0),
      last_dropped_at text not null
    );
  `);
}

function dropTimestamp() {
  return new Date().toISOString();
}

/** One bounded dirty-state row per drop reason; never throws into capture. */
export function recordRuntimeFactDrop(db: Database.Database, reason: RuntimeFactDropReason) {
  try {
    ensureRuntimeFactDropSchema(db);
    db.prepare(
      `insert into runtime_fact_drops (reason, dropped_count, last_dropped_at)
       values (?, 1, ?)
       on conflict(reason) do update set
         dropped_count = dropped_count + 1,
         last_dropped_at = excluded.last_dropped_at`,
    ).run(reason, dropTimestamp());
  } catch {
    // Drop accounting must never break capture.
  }
}

export function runtimeFactDropCounters(db: Database.Database): DropRow[] {
  try {
    return db.prepare(
      `select reason, dropped_count as droppedCount, last_dropped_at as lastDroppedAt
       from runtime_fact_drops order by reason`,
    ).all() as DropRow[];
  } catch {
    ensureRuntimeFactDropSchema(db);
    return [];
  }
}
