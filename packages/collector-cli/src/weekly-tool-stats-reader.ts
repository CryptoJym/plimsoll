import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";

import type Database from "better-sqlite3";
import { POSTGRES_UUID_RE, RETENTION_DELIVERY_ID_WORKER_SOURCE } from "./delivery-id";
import { ledgerConnectionWorkerSource } from "./ledger-connection";
import { assembleToolStatsWeek, planToolStatsWeekRead, readToolStatsWeekRows, TOOL_STATS_READ_DEADLINE_ERROR,
  type ToolStatsWeekInput, type ToolStatsWeekRows, type WeeklyToolStats } from "./weekly-tool-stats";

/** The week's read must finish inside this on the reader thread. */
export const WEEKLY_TOOL_STATS_READ_MS = 30 * 60_000;

// The week's rows are read off the collector's event loop (2026-11-01: the
// same read, planned without statistics, held the main thread for 3.5 hours).
// The plan's SQL is computed on the main thread, where the schema inspection
// behind it lives; the reader only runs it, chunk by chunk, with a deadline.
const workerSource = `
  const { parentPort, workerData } = require('node:worker_threads');
  const Database = require(workerData.sqliteModule);
  ${ledgerConnectionWorkerSource}
  const readToolStatsWeekRows = (${readToolStatsWeekRows.toString()});
  let db;
  try {
    db = openLedgerDatabase(workerData.ledgerPath, { readonly: true, fileMustExist: true });
    ${RETENTION_DELIVERY_ID_WORKER_SOURCE}
    const rows = readToolStatsWeekRows(db, workerData.plan, { deadlineAt: workerData.deadlineAt });
    parentPort.postMessage({ rows });
  } catch (error) {
    parentPort.postMessage({ error: String(error && error.message || error) });
  } finally {
    if (db) db.close();
    parentPort.close();
  }
`;

/**
 * One week's statistics, read on a reader thread. Resolves to the same value
 * as `aggregateToolStatsWeek` for the same ledger. The read honours `maxMs`
 * between its chunks; a reader stuck inside one chunk for a minute past that
 * is ended. The ledger must be a file: the reader opens it by path.
 */
export function aggregateToolStatsWeekOffThread(
  ledger: Database.Database,
  input: ToolStatsWeekInput,
  options: { maxMs?: number; now?: () => number } = {},
): Promise<WeeklyToolStats> {
  const plan = planToolStatsWeekRead(ledger, input);
  const maxMs = Math.max(1, Math.trunc(options.maxMs ?? WEEKLY_TOOL_STATS_READ_MS));
  const deadlineAt = (options.now ?? Date.now)() + maxMs;
  const worker = new Worker(workerSource, {
    eval: true,
    execArgv: [],
    workerData: {
      ledgerPath: ledger.name,
      sqliteModule: createRequire(import.meta.url).resolve("better-sqlite3"),
      plan,
      deadlineAt,
      postgresUuid: POSTGRES_UUID_RE,
    },
  });
  worker.unref();
  return new Promise<WeeklyToolStats>((resolve, reject) => {
    let settled = false;
    const settle = (outcome: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(watchdog);
      outcome();
    };
    const watchdog = setTimeout(() => settle(() => {
      reject(new Error(TOOL_STATS_READ_DEADLINE_ERROR));
      void worker.terminate();
    }), maxMs + 60_000);
    watchdog.unref();
    worker.once("message", (reply: { rows?: ToolStatsWeekRows; error?: string }) => settle(() => {
      if (reply.error || !reply.rows) { reject(new Error(reply.error || "weekly_tool_stats_read_invalid")); return; }
      try { resolve(assembleToolStatsWeek(input, reply.rows)); } catch (error) { reject(error); }
    }));
    worker.once("error", (error) => settle(() => reject(error)));
    worker.once("exit", (code) => settle(() => reject(new Error(`weekly_tool_stats_reader_exit_${code}`))));
  });
}
