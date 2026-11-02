import { createRequire } from "node:module";
import { Worker, type WorkerOptions } from "node:worker_threads";

import { ledgerConnectionWorkerSource } from "./ledger-connection";
import type { LifecycleCloneFallback } from "./lifecycle";
import type { LifecycleFenceGuard } from "./lifecycle-filesystem";
import type { OpenHandleCheck } from "./ledger-open-handles";

type QuiescedLedger = { close(): Promise<void> } | { fallback: LifecycleCloneFallback };
export type QuiesceWorkerFactory = (source: string, options: WorkerOptions) => Worker;

const workerSource = `
const { parentPort, workerData } = require("node:worker_threads");
const fs = require("node:fs");
const Database = require(workerData.entry);
${ledgerConnectionWorkerSource}
let connection;
function close() { try { connection?.close(); } finally { parentPort.close(); } }
function fail(error) {
  parentPort.postMessage({ fallback: /^SQLITE_(BUSY|LOCKED)/.test(error?.code ?? "")
    ? "ledger_in_use" : "quiescence_unproven" });
  close();
}
parentPort.on("message", (message) => {
  if (message === "close") { close(); return; }
  if (message !== "checkpoint") return;
  try {
    if (connection.pragma("journal_mode", { simple: true }) !== "wal") {
      parentPort.postMessage({ fallback: "ledger_not_wal" }); close(); return;
    }
    connection.pragma("checkpoint_fullfsync = ON");
    const [checkpoint] = connection.pragma("wal_checkpoint(TRUNCATE)");
    const wal = workerData.file + "-wal";
    if (!checkpoint || checkpoint.busy !== 0 || checkpoint.log !== 0 ||
        (fs.existsSync(wal) && fs.statSync(wal).size !== 0)) {
      parentPort.postMessage({ fallback: "wal_not_empty" }); close(); return;
    }
    parentPort.postMessage({ ready: true });
  } catch (error) { fail(error); }
});
try {
  connection = openLedgerDatabase(workerData.file, { fileMustExist: true, timeout: 0 }, true);
  connection.pragma("locking_mode = EXCLUSIVE");
  connection.exec("BEGIN EXCLUSIVE; COMMIT");
  parentPort.postMessage({ locked: true });
} catch (error) { fail(error); }
`;

/** The worker holds the exclusive lock until the caller finishes cloning. */
export async function quiesceLedgerOffThread(file: string, openHandles: OpenHandleCheck,
  guard?: LifecycleFenceGuard, startWorker: QuiesceWorkerFactory = (source, options) => new Worker(source, options),
): Promise<QuiescedLedger> {
  guard?.keepAlive();
  const worker = startWorker(workerSource, { eval: true, workerData: {
    entry: createRequire(import.meta.url).resolve("better-sqlite3"), file,
  } });
  let exited = false;
  const exit = new Promise<void>((resolve) => worker.once("exit", () => { exited = true; resolve(); }));
  const close = async () => {
    if (!exited) worker.postMessage("close");
    await exit;
  };
  let renewal: NodeJS.Timeout | undefined;
  try {
    const result = await new Promise<QuiescedLedger>((resolve, reject) => {
      let settled = false;
      const finish = (value: QuiescedLedger) => { settled = true; resolve(value); };
      const fail = (error: unknown) => { if (!settled) { settled = true; reject(error); } };
      worker.once("error", fail);
      worker.once("exit", () => fail(new Error("lifecycle checkpoint worker exited before quiescence was proven")));
      worker.on("message", (message: { locked?: boolean; ready?: boolean; fallback?: LifecycleCloneFallback }) => {
        if (settled) return;
        try {
          if (message.locked) {
            const others = openHandles([file, `${file}-wal`, `${file}-shm`]);
            if (others === null || others.length > 0) {
              finish({ fallback: others === null ? "quiescence_unproven" : "ledger_in_use" });
            } else worker.postMessage("checkpoint");
          } else if (message.ready) {
            guard?.assertCurrent();
            finish({ close });
          } else if (message.fallback) finish({ fallback: message.fallback });
        } catch (error) { fail(error); }
      });
      if (guard) renewal = setInterval(() => {
        try { guard.keepAlive(); } catch (error) { fail(error); }
      }, guard.keepAliveIntervalMs);
    });
    if ("fallback" in result) await close();
    return result;
  } catch (error) {
    // Fence loss cannot wait for a synchronous SQLite checkpoint to finish.
    // Termination releases the worker's connection before any caller can resume.
    if (!exited) await worker.terminate();
    await exit;
    throw error;
  } finally {
    if (renewal) clearInterval(renewal);
  }
}
