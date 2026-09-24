import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";

import type Database from "better-sqlite3";

import type { WalCheckpointCounts } from "./startup-wal-self-heal";

/**
 * Checkpoint the ledger WAL off the daemon's event loop (eco-6hoxj.163.24).
 *
 * SQLite's automatic checkpoint runs inside whichever connection commits once
 * the WAL holds 1,000 frames. In the daemon that is nearly always the intake
 * connection, so the event loop copied frames back into the database and
 * waited for its fsync: on the Studio0 ledger ~7 s of blocking fsync in 400 s,
 * single stalls of ~2.5 s that a CPU profile cannot see. The daemon turns the
 * automatic checkpoint off on its own connection and runs a PASSIVE checkpoint
 * here on a worker thread's connection instead. PASSIVE never waits for a
 * reader or writer, so intake keeps committing while the fsync runs. Any
 * worker failure restores the automatic checkpoint on the daemon connection.
 */
export const WAL_CHECKPOINT_INTERVAL_MS = 5_000;
/** SQLite's own default for `wal_autocheckpoint`. */
const SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES = 1_000;

const workerSource = `
  const { parentPort, workerData } = require('node:worker_threads');
  const Database = require(workerData.sqliteModule);
  const db = new Database(workerData.ledgerPath, { fileMustExist: true, timeout: 0 });
  parentPort.on('message', () => {
    let counts = null;
    try { counts = db.pragma('wal_checkpoint(PASSIVE)')[0] ?? null; } catch { /* retried next tick */ }
    parentPort.postMessage(counts);
  });
`;

export class WalCheckpointWorker {
  private worker: Worker | null = null;
  private timer: NodeJS.Timeout | undefined;
  private inFlight = false;
  private fallenBack = false;
  readonly counters = { runs: 0, busy: 0, framesCheckpointed: 0, failures: 0 };

  constructor(
    private readonly database: Database.Database,
    private readonly intervalMs = WAL_CHECKPOINT_INTERVAL_MS,
  ) {}

  /** Returns false (and changes nothing) for a ledger a worker cannot reopen. */
  start() {
    if (this.timer || this.fallenBack || this.database.memory) return false;
    this.database.pragma("wal_autocheckpoint = 0");
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    this.timer.unref();
    return true;
  }

  private tick() {
    if (this.inFlight) return;
    try {
      this.worker ??= this.spawn();
      this.inFlight = true;
      this.worker.postMessage(null);
    } catch {
      this.fallBack();
    }
  }

  private spawn() {
    const worker = new Worker(workerSource, {
      eval: true,
      execArgv: [],
      workerData: {
        ledgerPath: this.database.name,
        sqliteModule: createRequire(import.meta.url).resolve("better-sqlite3"),
      },
    });
    worker.unref();
    worker.on("message", (counts: WalCheckpointCounts | null) => {
      this.inFlight = false;
      this.counters.runs += 1;
      if (!counts || counts.busy !== 0) this.counters.busy += 1;
      else this.counters.framesCheckpointed += Math.max(0, counts.checkpointed);
    });
    worker.once("error", () => this.fallBack());
    worker.once("exit", () => {
      if (this.worker === worker && this.timer) this.fallBack();
    });
    return worker;
  }

  private fallBack() {
    this.counters.failures += 1;
    this.fallenBack = true;
    this.inFlight = false;
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    const worker = this.worker;
    this.worker = null;
    void worker?.terminate().catch(() => undefined);
    try {
      if (this.database.open) {
        this.database.pragma(`wal_autocheckpoint = ${SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES}`);
      }
    } catch {
      /* the connection is closing; nothing is left to checkpoint for */
    }
    console.warn(JSON.stringify({ warning: "wal_checkpoint_worker_unavailable", fallback: "sqlite_autocheckpoint" }));
  }

  /** Stop before the daemon closes its connection; a checkpoint mid-flight is crash-safe. */
  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    const worker = this.worker;
    this.worker = null;
    return worker ? worker.terminate().then(() => undefined, () => undefined) : Promise.resolve();
  }
}
