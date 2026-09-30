import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";

import type Database from "better-sqlite3";
import { POSTGRES_UUID_RE, RETENTION_DELIVERY_ID_WORKER_SOURCE } from "./delivery-id";
import { ledgerConnectionWorkerSource } from "./ledger-connection";

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
    const row = db.prepare('select count(*) as n from buffered_events e indexed by idx_events_retention '
      + 'where e.created_at < ? and ' + workerData.holdSql).get(workerData.cutoffAt);
    parentPort.postMessage({ count: row.n });
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
): { worker: Worker; result: Promise<number>; exited: Promise<void> } {
  const worker = new Worker(workerSource, {
    eval: true,
    execArgv: [],
    workerData: {
      ledgerPath: ledger.name,
      sqliteModule: createRequire(import.meta.url).resolve("better-sqlite3"),
      cutoffAt,
      holdSql,
      postgresUuid: POSTGRES_UUID_RE,
    },
  });
  worker.unref();
  const exited = new Promise<void>((resolve) => worker.once("exit", () => resolve()));
  const result = new Promise<number>((resolve, reject) => {
    let settled = false;
    worker.once("message", (reply: { count?: number; error?: string }) => {
      settled = true;
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
  return { worker, result, exited };
}
