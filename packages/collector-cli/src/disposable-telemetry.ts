import { performance } from "node:perf_hooks";
import type Database from "better-sqlite3";
import { isSqliteContentionError } from "./sqlite-contention";

/** Existing footprint-measurement contract; never apply these limits to usage,
 * metric_samples, delivery, lineage, privacy, audit or account evidence. */
export const BUDGET_RING_LIMIT = 1_440;
export const BUDGET_SAMPLE_MAX_AGE_MS = 86_400_000;
export const BUDGET_DAILY_PRIOR_DAYS = 7;
export const TELEMETRY_SWEEP_INTERVAL_MS = 6 * 60 * 60_000;
const STATE_KEY = "disposable_telemetry_retention_v1";

export type DisposableTelemetryReceipt = {
  samplesExpired: number; daysExpired: number; rowsVisited: number;
  hasMore: boolean; madeProgress: boolean; busy: boolean; elapsedMs: number;
  sampleCutoffMs: number; dayCutoff: string;
};

/** Explicit two-table allowlist, indexed candidate pages and one aggregate
 * receipt. Runs even when sampling has stopped; it never scans raw history.
 * The time bound is cooperative: finish the current SQLite statement/row. */
export function pruneDisposableTelemetry(db: Database.Database, options: {
  maxRows: number; maxWriterMs: number; busyTimeoutMs?: number; now?: Date;
}): DisposableTelemetryReceipt {
  const started = performance.now();
  const maxRows = Math.max(1, Math.min(4_096, Math.trunc(options.maxRows) || 1));
  const maxWriterMs = Math.max(1, options.maxWriterMs);
  const now = options.now ?? new Date();
  const sampleCutoffMs = now.getTime() - BUDGET_SAMPLE_MAX_AGE_MS;
  const dayCutoff = new Date(now.getTime() - BUDGET_DAILY_PRIOR_DAYS * 86_400_000).toISOString().slice(0, 10);
  const receipt: DisposableTelemetryReceipt = { samplesExpired: 0, daysExpired: 0,
    rowsVisited: 0, hasMore: false, madeProgress: false, busy: false, elapsedMs: 0, sampleCutoffMs, dayCutoff };
  const previousTimeout = Number(db.pragma("busy_timeout", { simple: true }));
  db.pragma(`busy_timeout = ${Math.max(0, Math.min(25, Math.trunc(options.busyTimeoutMs ?? 25)))}`);
  try {
    return db.transaction(() => {
    // Read aggregate progress under the writer reservation too, so two
    // callers cannot overwrite each other's committed expiry totals.
    const stored = db.prepare("select value from maintenance_state where key=?").get(STATE_KEY) as { value: string } | undefined;
    const state = stored ? JSON.parse(stored.value) as {
      sweepCompletedAt?: string; samplesExpired?: number; daysExpired?: number;
    } : {};
    if (state.sweepCompletedAt && now.getTime() - Date.parse(state.sweepCompletedAt) < TELEMETRY_SWEEP_INTERVAL_MS) return receipt;
      const exists = db.prepare("select 1 from sqlite_master where type='table' and name=?");
      const samples = Boolean(exists.get("budget_samples"));
      const days = Boolean(exists.get("budget_daily"));
      // Find the overflow boundary by walking at most 1,441 primary keys.
      const overflow = samples ? db.prepare("select id from budget_samples order by id desc limit 1 offset ?")
        .get(BUDGET_RING_LIMIT) as { id: number } | undefined : undefined;
      const withinBudget = () => receipt.rowsVisited < maxRows &&
        (receipt.rowsVisited === 0 || performance.now() - started < maxWriterMs);
      if (samples) {
        const remove = db.prepare("delete from budget_samples where id=?");
        const expire = (rows: Array<{ id: number }>) => {
          for (const row of rows) {
            if (!withinBudget()) break;
            receipt.rowsVisited += 1;
            receipt.samplesExpired += remove.run(row.id).changes;
          }
        };
        if (overflow) expire(db.prepare("select id from budget_samples where id<=? order by id limit ?")
          .all(overflow.id, maxRows) as Array<{ id: number }>);
        if (withinBudget()) expire(db.prepare(`select id from budget_samples indexed by idx_budget_samples_at_ms
          where at_ms<? order by at_ms,id limit ?`).all(sampleCutoffMs, maxRows - receipt.rowsVisited) as Array<{ id: number }>);
      }
      if (days && withinBudget()) {
        const page = db.prepare("select day from budget_daily where day<? order by day limit ?")
          .all(dayCutoff, maxRows - receipt.rowsVisited) as Array<{ day: string }>;
        const remove = db.prepare("delete from budget_daily where day=?");
        for (const row of page) {
          if (!withinBudget()) break;
          receipt.rowsVisited += 1;
          receipt.daysExpired += remove.run(row.day).changes;
        }
      }
      receipt.hasMore = Boolean(samples && (db.prepare("select 1 from budget_samples order by id desc limit 1 offset ?")
        .get(BUDGET_RING_LIMIT) || db.prepare("select 1 from budget_samples indexed by idx_budget_samples_at_ms where at_ms<? limit 1")
        .get(sampleCutoffMs))) || Boolean(days && db.prepare("select 1 from budget_daily where day<? limit 1").get(dayCutoff));
      receipt.madeProgress = receipt.rowsVisited > 0;
      db.prepare(`insert into maintenance_state(key,value,updated_at) values(?,?,?)
        on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`).run(STATE_KEY, JSON.stringify({
          samplesExpired: (state.samplesExpired ?? 0) + receipt.samplesExpired,
          daysExpired: (state.daysExpired ?? 0) + receipt.daysExpired,
          sweepCompletedAt: receipt.hasMore ? null : now.toISOString(),
          lastRunAt: now.toISOString(), lastHasMore: receipt.hasMore,
        }), now.toISOString());
      return receipt;
    }).immediate();
  } catch (error) {
    if (!isSqliteContentionError(error)) throw error;
    // The transaction rolled back, including aggregate progress.
    return { ...receipt, samplesExpired: 0, daysExpired: 0, rowsVisited: 0,
      hasMore: true, madeProgress: false, busy: true, elapsedMs: performance.now() - started };
  } finally {
    receipt.elapsedMs = performance.now() - started;
    db.pragma(`busy_timeout = ${previousTimeout}`);
  }
}
