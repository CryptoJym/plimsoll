import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";

import type Database from "better-sqlite3";
import { POSTGRES_UUID_RE, RETENTION_DELIVERY_ID_WORKER_SOURCE } from "./delivery-id";
import { ledgerConnectionWorkerSource } from "./ledger-connection";

/**
 * The exact hold count reads in pages. A read snapshot pins every WAL frame
 * written after it, so one `count(*)` over a large backlog stopped the
 * checkpoint worker from copying anything back (incident of 2026-11-01: 33
 * million rows past the cutoff; the WAL grew from 1.4 GB to 12.6 GB in
 * eighteen minutes while the count ran). Each page is its own statement
 * through `idx_events_retention (created_at, id)`, so the snapshot is released
 * between pages, and a count that has not finished by `maxMs` stops with
 * `retention_hold_count_interrupted`; the caller keeps its bounded sample and
 * waits `RETENTION_HOLD_COUNT_BACKOFF_MS` before asking again.
 */
export const RETENTION_HOLD_COUNT_PAGE_ROWS = 5_000;
export const RETENTION_HOLD_COUNT_MAX_MS = 30_000;
export const RETENTION_HOLD_COUNT_BACKOFF_MS = 10 * 60_000;
export const RETENTION_HOLD_COUNT_INTERRUPTED = "retention_hold_count_interrupted";

export type RetentionHoldCountProgress = { rows: number; pages: number; elapsedMs: number; partialCount?: number };

// The ledger reader keeps this exact count off the collector's event loop.
// The UUID test and worker registration are shared with session sync.
const workerSource = `
  const { parentPort, workerData } = require('node:worker_threads');
  const Database = require(workerData.sqliteModule);
  ${ledgerConnectionWorkerSource}
  let db;
  try {
    db = openLedgerDatabase(workerData.ledgerPath, { readonly: true, fileMustExist: true });
    ${RETENTION_DELIVERY_ID_WORKER_SOURCE}
    const pageRows = workerData.pageRows;
    const page = db.prepare('select e.created_at as createdAt, e.id as id, '
      + 'case when ' + workerData.holdSql + ' then 1 else 0 end as held '
      + 'from buffered_events e indexed by idx_events_retention '
      + 'where (e.created_at, e.id) > (?, ?) and e.created_at < ? '
      + 'order by e.created_at, e.id limit ' + pageRows);
    const started = Date.now();
    let after = ['', ''];
    let count = 0, rows = 0, pages = 0;
    for (;;) {
      const batch = page.all(after[0], after[1], workerData.cutoffAt);
      pages += 1;
      rows += batch.length;
      for (const row of batch) count += row.held;
      if (batch.length < pageRows) {
        parentPort.postMessage({ count, rows, pages, elapsedMs: Date.now() - started });
        break;
      }
      const last = batch[batch.length - 1];
      after = [last.createdAt, last.id];
      if (Date.now() - started >= workerData.maxMs) {
        parentPort.postMessage({ error: ${JSON.stringify(RETENTION_HOLD_COUNT_INTERRUPTED)},
          rows, pages, partialCount: count, elapsedMs: Date.now() - started });
        break;
      }
    }
  } catch (error) {
    parentPort.postMessage({ error: String(error && error.message || error) });
  } finally {
    if (db) db.close();
    parentPort.close();
  }
`;

export function countRetentionHoldsOffThread(
  ledger: Database.Database,
  cutoffAt: string,
  holdSql: string,
  options: { maxMs?: number; pageRows?: number } = {},
): { worker: Worker; result: Promise<number>; exited: Promise<void>; progress: () => RetentionHoldCountProgress | null } {
  const maxMs = Math.max(1, Math.trunc(options.maxMs ?? RETENTION_HOLD_COUNT_MAX_MS));
  const pageRows = Math.max(1, Math.min(50_000, Math.trunc(options.pageRows ?? RETENTION_HOLD_COUNT_PAGE_ROWS)));
  const worker = new Worker(workerSource, {
    eval: true,
    execArgv: [],
    workerData: {
      ledgerPath: ledger.name,
      sqliteModule: createRequire(import.meta.url).resolve("better-sqlite3"),
      cutoffAt,
      holdSql,
      maxMs,
      pageRows,
      postgresUuid: POSTGRES_UUID_RE,
    },
  });
  worker.unref();
  let progress: RetentionHoldCountProgress | null = null;
  const exited = new Promise<void>((resolve) => worker.once("exit", () => resolve()));
  const result = new Promise<number>((resolve, reject) => {
    let settled = false;
    worker.once("message", (reply: { count?: number; error?: string; rows?: number; pages?: number;
      elapsedMs?: number; partialCount?: number }) => {
      settled = true;
      if (typeof reply.rows === "number") {
        progress = { rows: reply.rows, pages: reply.pages ?? 0, elapsedMs: reply.elapsedMs ?? 0,
          ...(typeof reply.partialCount === "number" ? { partialCount: reply.partialCount } : {}) };
      }
      if (reply.error) reject(new Error(reply.error));
      else if (Number.isSafeInteger(reply.count) && Number(reply.count) >= 0) resolve(Number(reply.count));
      else reject(new Error("retention_hold_count_invalid"));
    });
    worker.once("error", (error) => {
      if (!settled) { settled = true; reject(error); }
    });
    worker.once("exit", (code) => {
      if (!settled) { settled = true; reject(new Error(`retention_hold_count_exit_${code}`)); }
    });
  });
  return { worker, result, exited, progress: () => progress };
}
