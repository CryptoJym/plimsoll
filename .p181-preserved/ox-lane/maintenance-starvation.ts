import type Database from "better-sqlite3";

/**
 * Durable starvation ledger (issue #181).
 *
 * The maintenance child used to be deadline-killed all-or-nothing: whatever
 * partial batch progress existed at kill time vanished without a trace, the
 * quarantine blamed whichever candidate happened to be on stage, and nothing
 * surfaced the resulting enrichment backlog. This module owns the receipts
 * that make starvation observable and kills accountable:
 *
 *   - deadline-kill counter + timestamp (kill rate),
 *   - the last deadline-blame checkpoint (what the killed run had reached),
 *   - pending-enrichment backlog counts read straight from the queue tables.
 */

export const MAINTENANCE_DEADLINE_KILLS_KEY = "maintenance_deadline_kills";
export const MAINTENANCE_LAST_DEADLINE_KILL_AT_KEY = "maintenance_last_deadline_kill_at";
export const MAINTENANCE_PROGRESS_CHECKPOINT_KEY = "maintenance_progress_checkpoint";

export type MaintenanceDeadlineBlameCheckpoint = {
  at: string;
  source: string | null;
  stage: string | null;
  heldMs: number | null;
  attribution: "proven" | "unknown";
};

export type MaintenanceStarvationReceipt = {
  deadlineKills: number;
  lastDeadlineKillAt: string | null;
  lastCheckpoint: MaintenanceDeadlineBlameCheckpoint | null;
  backlog: {
    fillPendingEventLinks: number;
    dirtyEnrichmentSessions: number;
  };
  /** Kills have occurred while enrichment work is still visibly stuck. */
  starving: boolean;
};

function ensureMaintenanceStateTable(database: Database.Database) {
  database.exec(`
    create table if not exists maintenance_state (
      key text primary key,
      value text not null,
      updated_at text not null
    )
  `);
}

function stateValue(database: Database.Database, key: string): string | null {
  const row = database
    .prepare(`select value from maintenance_state where key = ?`)
    .get(key) as { value: string } | undefined;
  return row?.value ?? null;
}

function setStateValue(database: Database.Database, key: string, value: string) {
  ensureMaintenanceStateTable(database);
  database
    .prepare(
      `insert into maintenance_state (key, value, updated_at)
       values (?, ?, ?)
       on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
    )
    .run(key, value, new Date().toISOString());
}

/** Count one parent-side deadline kill of the maintenance child. */
export function recordMaintenanceDeadlineKill(
  database: Database.Database,
  at = new Date(),
): number {
  const previous = Number(stateValue(database, MAINTENANCE_DEADLINE_KILLS_KEY) ?? "0");
  const kills = Number.isSafeInteger(previous) && previous >= 0 ? previous + 1 : 1;
  setStateValue(database, MAINTENANCE_DEADLINE_KILLS_KEY, String(kills));
  setStateValue(database, MAINTENANCE_LAST_DEADLINE_KILL_AT_KEY, at.toISOString());
  return kills;
}

/** Durably record what the killed child was last seen doing. */
export function recordMaintenanceDeadlineBlame(
  database: Database.Database,
  checkpoint: MaintenanceDeadlineBlameCheckpoint,
): void {
  setStateValue(database, MAINTENANCE_PROGRESS_CHECKPOINT_KEY, JSON.stringify(checkpoint));
}

/** Backlog census over the enrichment queues; missing tables count as zero. */
export function maintenanceBacklogSnapshot(database: Database.Database): {
  fillPendingEventLinks: number;
  dirtyEnrichmentSessions: number;
} {
  const count = (sql: string): number => {
    try {
      const row = database.prepare(sql).get() as { n: unknown };
      const n = Number(row.n);
      return Number.isSafeInteger(n) && n >= 0 ? n : 0;
    } catch {
      return 0;
    }
  };
  return {
    fillPendingEventLinks: count(
      `select count(*) as n from repo_context_event_links where fill_pending = 1`,
    ),
    dirtyEnrichmentSessions: count(`select count(*) as n from repo_enrichment_dirty`),
  };
}

export function maintenanceStarvationReceipt(
  database: Database.Database,
): MaintenanceStarvationReceipt {
  const killsRaw = Number(stateValue(database, MAINTENANCE_DEADLINE_KILLS_KEY) ?? "0");
  const deadlineKills = Number.isSafeInteger(killsRaw) && killsRaw >= 0 ? killsRaw : 0;
  const lastDeadlineKillAt = stateValue(database, MAINTENANCE_LAST_DEADLINE_KILL_AT_KEY);
  const checkpointRaw = stateValue(database, MAINTENANCE_PROGRESS_CHECKPOINT_KEY);
  let lastCheckpoint: MaintenanceDeadlineBlameCheckpoint | null = null;
  if (checkpointRaw) {
    try {
      const parsed = JSON.parse(checkpointRaw) as MaintenanceDeadlineBlameCheckpoint;
      if (
        parsed &&
        typeof parsed === "object" &&
        typeof parsed.at === "string" &&
        (parsed.attribution === "proven" || parsed.attribution === "unknown")
      ) {
        lastCheckpoint = parsed;
      }
    } catch {
      lastCheckpoint = null;
    }
  }
  const backlog = maintenanceBacklogSnapshot(database);
  return {
    deadlineKills,
    lastDeadlineKillAt,
    lastCheckpoint,
    backlog,
    starving: deadlineKills > 0 && (backlog.fillPendingEventLinks > 0 || backlog.dirtyEnrichmentSessions > 0),
  };
}
