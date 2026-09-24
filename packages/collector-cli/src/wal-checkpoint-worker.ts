import { createRequire } from "node:module";
import { Worker } from "node:worker_threads";

import type Database from "better-sqlite3";

import type { WalCheckpointCounts } from "./startup-wal-self-heal";

/**
 * Checkpoint the ledger WAL off the daemon's event loop (eco-6hoxj.163.24).
 *
 * SQLite's automatic checkpoint runs inside whichever connection commits once
 * the WAL holds `wal_autocheckpoint` frames. In the daemon that is nearly
 * always the intake connection, so at SQLite's default of 1,000 frames the
 * event loop copied frames back into the database and waited for its fsync
 * every few seconds: on the Studio0 ledger ~7 s of blocking fsync in 400 s,
 * single stalls of ~2.5 s that a CPU profile cannot see. A worker thread's
 * connection now runs a PASSIVE checkpoint every few seconds instead; PASSIVE
 * never waits for a reader or writer, so intake keeps committing meanwhile.
 *
 * The worker also bounds the WAL. SQLite rewinds the WAL only when a write
 * begins after every frame has been copied back, and commits that land during
 * a PASSIVE pass prevent exactly that: with nothing else checkpointing, the
 * WAL grew without limit whenever the maintenance child's checkpoint was not
 * running (11 -> 647 MB in 300 s at ~19 commits/s). So once the WAL holds
 * WAL_TARGET_FRAMES frames, the worker follows its PASSIVE pass with a FULL
 * one. FULL holds the writer lock only while it copies the frames committed
 * during the pass, and the next write rewinds the WAL. Daemon writes that meet
 * the lock are retried or spooled, as when the maintenance child writes; the
 * event loop never waits for it.
 *
 * The daemon connection keeps its own automatic checkpoint at
 * WAL_AUTOCHECKPOINT_VALVE_FRAMES as the hard bound, a backstop that copies
 * back on the event loop only when the worker falls that far behind. Its
 * fsync waits behind every other write to the disk: as the only bound, it
 * cost the event loop 5.0 s of fsync in a 7-minute replay of the Studio0
 * snapshot, as much as SQLite's default did. Losing the worker restores
 * SQLite's default.
 *
 * No checkpoint can copy past a reader holding an older snapshot (the
 * session-sync catch-up reads for minutes), so the WAL grows meanwhile under
 * any bound. When the reader lets go, the first checkpoint copies the whole
 * backlog, and the backstop would do that inside the next commit (4.0 s for
 * ~810 MB in the replay). So while a reader holds more than WAL_HELD_FRAMES
 * frames the backstop is raised above the WAL, and the worker, passing every
 * second, copies the backlog once the reader lets go.
 */
export const WAL_CHECKPOINT_INTERVAL_MS = 5_000;
/** ~41 MB of 4 KB pages: past this the worker finishes its pass with FULL so the WAL rewinds. */
export const WAL_TARGET_FRAMES = 10_000;
/** ~82 MB: the daemon connection's own automatic checkpoint, the WAL's hard bound. */
export const WAL_AUTOCHECKPOINT_VALVE_FRAMES = 20_000;
/** SQLite's own default for `wal_autocheckpoint`, restored when the worker is lost. */
export const SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES = 1_000;
/** Frames a reader may keep a pass from copying before the WAL counts as held. */
export const WAL_HELD_FRAMES = 5_000;
/** How soon the worker passes again while a reader holds the WAL, a FULL is due, or the WAL fills fast. */
const PROMPT_INTERVAL_MS = 1_000;
/** How long FULL waits for the writer lock and for readers before it settles for PASSIVE. */
const FULL_BUSY_TIMEOUT_MS = 50;
/** A pass unanswered this long is stuck; its worker is replaced. */
export const WAL_CHECKPOINT_WATCHDOG_MS = 120_000;
/** Workers replaced after a stall, a crash or an exit before the daemon falls back. */
const MAX_WORKER_RESTARTS = 3;
/** Consecutive failed passes that are reported once. */
const ERROR_STREAK_WARNING = 3;

const workerSource = `
  const fs = require('node:fs');
  const { parentPort, workerData } = require('node:worker_threads');
  const Database = require(workerData.sqliteModule);
  const db = new Database(workerData.ledgerPath, { fileMustExist: true, timeout: 0 });
  const walBytes = () => { try { return fs.statSync(workerData.ledgerPath + '-wal').size; } catch { return 0; } };
  const checkpoint = (mode) => db.pragma('wal_checkpoint(' + mode + ')')[0] ?? null;
  parentPort.on('message', () => {
    const started = performance.now();
    try {
      const counts = checkpoint('PASSIVE');
      let full = null;
      if (counts && counts.busy === 0 && counts.log >= workerData.targetFrames &&
        counts.log - counts.checkpointed <= workerData.heldFrames) {
        db.pragma('busy_timeout = ' + workerData.fullBusyTimeoutMs);
        try { full = checkpoint('FULL'); } finally { db.pragma('busy_timeout = 0'); }
      }
      parentPort.postMessage({ ok: true, counts, full, ms: performance.now() - started, walBytes: walBytes() });
    } catch (error) {
      parentPort.postMessage({ ok: false, code: String(error?.code ?? error?.name ?? 'unknown'),
        ms: performance.now() - started, walBytes: walBytes() });
    }
  });
`;

type PassReply =
  | { ok: true; counts: WalCheckpointCounts | null; full: WalCheckpointCounts | null; ms: number; walBytes: number }
  | { ok: false; code: string; ms: number; walBytes: number };

export type WalCheckpointStatus = {
  mode: "not_started" | "worker" | "sqlite_autocheckpoint" | "stopped";
  /** `wal_autocheckpoint` this daemon set on its own connection: the backstop. */
  autocheckpointFrames: number | null;
  /** The last pass could not copy more than WAL_HELD_FRAMES frames past a reader. */
  heldByReader: boolean;
  intervalMs: number;
  runs: number;
  busy: number;
  errors: number;
  consecutiveErrors: number;
  restarts: number;
  /** FULL passes run once the WAL reached WAL_TARGET_FRAMES, and those that copied every frame. */
  fullRuns: number;
  fullCompleted: number;
  lastSuccessAt: string | null;
  lastErrorAt: string | null;
  lastErrorCode: string | null;
  /**
   * The last pass: frames in the WAL, frames copied back, whether it ended with
   * a completed FULL (the next write rewinds the WAL), and how long it took.
   */
  lastPass: {
    at: string;
    walFrames: number | null;
    checkpointedFrames: number | null;
    busy: boolean;
    completedFull: boolean;
    ms: number;
  } | null;
  /** WAL file size the worker saw after its last pass (it is never read on the request path). */
  walBytes: number | null;
  inFlightSince: string | null;
  fallbackReason: string | null;
};

export class WalCheckpointWorker {
  private worker: Worker | null = null;
  private timer: NodeJS.Timeout | undefined;
  private followUp: NodeJS.Timeout | undefined;
  private inFlightSinceMs: number | null = null;
  /** WAL frames at the last pass (0 once a completed FULL lets the next write rewind it). */
  private lastWalFrames = 0;
  private state: WalCheckpointStatus;

  constructor(
    private readonly database: Database.Database,
    private readonly intervalMs = WAL_CHECKPOINT_INTERVAL_MS,
    private readonly watchdogMs = WAL_CHECKPOINT_WATCHDOG_MS,
  ) {
    this.state = {
      mode: "not_started", autocheckpointFrames: null, heldByReader: false, intervalMs, runs: 0, busy: 0, errors: 0,
      consecutiveErrors: 0, restarts: 0, fullRuns: 0, fullCompleted: 0, lastSuccessAt: null, lastErrorAt: null,
      lastErrorCode: null, lastPass: null, walBytes: null, inFlightSince: null, fallbackReason: null,
    };
  }

  /** Returns false (and changes nothing) for a ledger a worker cannot reopen. */
  start() {
    if (this.timer || this.state.mode === "sqlite_autocheckpoint" || this.database.memory) return false;
    this.setAutocheckpoint(WAL_AUTOCHECKPOINT_VALVE_FRAMES);
    this.state.mode = "worker";
    this.timer = setInterval(() => this.tick(), this.intervalMs);
    this.timer.unref();
    return true;
  }

  /** In-memory snapshot for /status; no SQLite or filesystem work. */
  status(): WalCheckpointStatus {
    return {
      ...this.state,
      inFlightSince: this.inFlightSinceMs === null ? null : new Date(this.inFlightSinceMs).toISOString(),
      lastPass: this.state.lastPass ? { ...this.state.lastPass } : null,
    };
  }

  private setAutocheckpoint(frames: number) {
    try {
      if (this.database.open) {
        this.database.pragma(`wal_autocheckpoint = ${frames}`);
        this.state.autocheckpointFrames = frames;
      }
    } catch {
      /* the connection is closing; nothing is left to checkpoint for */
    }
  }

  private tick() {
    if (this.inFlightSinceMs !== null) {
      if (Date.now() - this.inFlightSinceMs >= this.watchdogMs) this.replace("stalled");
      return;
    }
    try {
      this.worker ??= this.spawn();
      this.inFlightSinceMs = Date.now();
      this.worker.postMessage(null);
    } catch {
      this.replace("spawn_failed");
    }
  }

  private spawn() {
    const worker = new Worker(workerSource, {
      eval: true,
      execArgv: [],
      workerData: {
        ledgerPath: this.database.name,
        sqliteModule: createRequire(import.meta.url).resolve("better-sqlite3"),
        targetFrames: WAL_TARGET_FRAMES,
        heldFrames: WAL_HELD_FRAMES,
        fullBusyTimeoutMs: FULL_BUSY_TIMEOUT_MS,
      },
    });
    worker.unref();
    worker.on("message", (reply: PassReply) => this.record(reply));
    worker.once("error", () => {
      if (this.worker === worker) this.replace("error");
    });
    worker.once("exit", () => {
      if (this.worker === worker && this.timer) this.replace("exit");
    });
    return worker;
  }

  private record(reply: PassReply) {
    this.inFlightSinceMs = null;
    const at = new Date().toISOString();
    this.state.runs += 1;
    this.state.walBytes = reply.walBytes;
    if (!reply.ok) {
      this.state.errors += 1;
      this.state.consecutiveErrors += 1;
      this.state.lastErrorAt = at;
      this.state.lastErrorCode = reply.code;
      this.state.lastPass = { at, walFrames: null, checkpointedFrames: null, busy: false, completedFull: false,
        ms: Math.round(reply.ms) };
      if (this.state.consecutiveErrors === ERROR_STREAK_WARNING) {
        console.warn(JSON.stringify({ warning: "wal_checkpoint_worker_failing", code: reply.code,
          consecutiveErrors: this.state.consecutiveErrors }));
      }
      return;
    }
    this.state.consecutiveErrors = 0;
    const counts = reply.counts;
    const busy = !counts || counts.busy !== 0;
    const full = reply.full;
    const completedFull = Boolean(full && full.busy === 0 && full.checkpointed === full.log);
    if (full) this.state.fullRuns += 1;
    if (completedFull) this.state.fullCompleted += 1;
    const last = full ?? counts;
    this.state.lastPass = { at, walFrames: last?.log ?? null, checkpointedFrames: last?.checkpointed ?? null,
      busy, completedFull, ms: Math.round(reply.ms) };
    if (busy) {
      this.state.busy += 1;
      return;
    }
    this.state.lastSuccessAt = at;
    this.pace(counts, completedFull);
  }

  /**
   * Raise the backstop above a WAL a reader holds, and restore it once the
   * worker has copied the backlog. Pass again within a second while a reader
   * holds the WAL, while a FULL is due, or when the WAL grew by more than half
   * the gap between the target and the backstop since the last pass, so the
   * worker's FULL, not the backstop, rewinds it.
   */
  private pace(counts: WalCheckpointCounts, completedFull: boolean) {
    if (!this.timer) return;
    const grew = counts.log >= this.lastWalFrames ? counts.log - this.lastWalFrames : counts.log;
    this.lastWalFrames = completedFull ? 0 : counts.log;
    this.state.heldByReader = counts.log - counts.checkpointed > WAL_HELD_FRAMES;
    if (this.state.heldByReader) {
      this.setAutocheckpoint(counts.log + WAL_AUTOCHECKPOINT_VALVE_FRAMES);
    } else if (this.state.autocheckpointFrames !== WAL_AUTOCHECKPOINT_VALVE_FRAMES) {
      this.setAutocheckpoint(WAL_AUTOCHECKPOINT_VALVE_FRAMES);
    }
    const due = counts.log >= WAL_TARGET_FRAMES && !completedFull;
    const fast = grew > (WAL_AUTOCHECKPOINT_VALVE_FRAMES - WAL_TARGET_FRAMES) / 2;
    if (!this.state.heldByReader && !due && !fast) return;
    this.followUp ??= setTimeout(() => {
      this.followUp = undefined;
      this.tick();
    }, Math.min(PROMPT_INTERVAL_MS, this.intervalMs));
    this.followUp.unref();
  }

  /** Replace a stalled, crashed or exited worker; after a few, fall back for good. */
  private replace(reason: string) {
    this.inFlightSinceMs = null;
    const worker = this.worker;
    this.worker = null;
    void worker?.terminate().catch(() => undefined);
    if (!this.timer) return;
    if (this.state.restarts >= MAX_WORKER_RESTARTS) {
      this.fallBack(reason);
      return;
    }
    this.state.restarts += 1;
    console.warn(JSON.stringify({ warning: "wal_checkpoint_worker_restarted", reason, restarts: this.state.restarts }));
  }

  private fallBack(reason: string) {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    clearTimeout(this.followUp);
    this.followUp = undefined;
    this.state.heldByReader = false;
    this.state.mode = "sqlite_autocheckpoint";
    this.state.fallbackReason = reason;
    this.setAutocheckpoint(SQLITE_DEFAULT_AUTOCHECKPOINT_FRAMES);
    console.warn(JSON.stringify({ warning: "wal_checkpoint_worker_unavailable", reason, fallback: "sqlite_autocheckpoint" }));
  }

  /** Stop before the daemon closes its connection; a checkpoint mid-flight is crash-safe. */
  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = undefined;
    clearTimeout(this.followUp);
    this.followUp = undefined;
    if (this.state.mode === "worker") this.state.mode = "stopped";
    this.inFlightSinceMs = null;
    const worker = this.worker;
    this.worker = null;
    return worker ? worker.terminate().then(() => undefined, () => undefined) : Promise.resolve();
  }
}
