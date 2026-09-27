import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";

import type Database from "better-sqlite3";
import { POSTGRES_UUID_RE } from "./upload-history";

// The ledger reader keeps this exact count off the collector's event loop.
// The UUID test is shared with ensureUuidEventId in upload-history.ts.
const workerSource = `
  const crypto = require('node:crypto');
  const { parentPort, workerData } = require('node:worker_threads');
  const Database = require(workerData.sqliteModule);
  const db = new Database(workerData.ledgerPath, { readonly: true, fileMustExist: true });
  const postgresUuid = workerData.postgresUuid;
  db.function('retention_delivery_id', { deterministic: true }, (rawId) => {
    if (postgresUuid.test(rawId)) return rawId;
    const digest = crypto.createHash('sha256').update('workspace-backfill|' + rawId).digest('hex');
    return [digest.slice(0, 8), digest.slice(8, 12),
      '5' + digest.slice(13, 16), '9' + digest.slice(17, 20), digest.slice(20, 32)].join('-');
  });
  try {
    const row = db.prepare('select count(*) as n from buffered_events e indexed by idx_events_retention '
      + 'where e.created_at < ? and ' + workerData.holdSql).get(workerData.cutoffAt);
    parentPort.postMessage({ count: row.n });
  } catch (error) {
    parentPort.postMessage({ error: String(error && error.message || error) });
  } finally {
    db.close();
    parentPort.close();
  }
`;

export function countRetentionHoldsOffThread(
  ledger: Database.Database,
  cutoffAt: string,
  holdSql: string,
): { worker: Worker; result: Promise<number> } {
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
  return { worker, result };
}
