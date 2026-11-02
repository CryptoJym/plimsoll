import type { LocalEventBuffer } from "./buffer";
import type { AutomaticMaintenanceCadenceTimer } from "./maintenance";

type Receipt = ReturnType<LocalEventBuffer["prune"]> & {
  /** The writer was held by another connection; nothing was read or written. */
  busy?: boolean;
  elapsedMs?: number;
  secondary?: unknown;
};

/** What one pass may do. The prune bounds itself by rows and by writer time. */
export type RetentionPassRequest = { maxRows: number; maxWriterMs: number };

export const RETENTION_PASS_BASE_ROWS = 128;
export const RETENTION_PASS_MAX_ROWS = 4_096;
export const RETENTION_PASS_WRITER_MS = 250;
/** The wait before the next pass while no large backlog waits, after a held
 * writer without one, and after an error. */
export const RETENTION_FOLLOWUP_MS = 5_000;
/** The least wait before the next pass while a large backlog clears. The
 * wait is the last pass's own duration, never less than this and never more
 * than the follow-up, so the prune takes at most half of its thread and
 * intake queued behind a pass is served. */
export const RETENTION_FAST_FOLLOWUP_MS = 200;
/** Raw rows older than the cutoff that make a backlog "large": passes grow
 * and the follow-up shortens only while at least this many wait. The probe
 * that answers it stops counting at this number. */
export const RETENTION_LARGE_BACKLOG_ROWS = 50_000;

/**
 * One bounded pass per turn; schedule the next only after the pass settles.
 *
 * The pass yields to the ledger, never to another scheduler's flag: a prune
 * that finds the writer held returns `busy` and is retried later. While a
 * backlog exists (`hasMore` with progress, and `largeBacklog` when supplied)
 * and passes finish inside half their writer budget, the pass size doubles up
 * to `max`; a pass that overran its budget halves it; a pass that found no
 * more work resets it to `base`.
 *
 * While a large backlog clears, the next pass follows after a wait equal to
 * the last pass's own duration, at least `fastFollowupMs` and at most
 * `followupMs`, whether or not that pass overran: the prune takes at most
 * half of its thread, a quick pass is followed quickly, and a pass stalled
 * by cold reads on a contended disk is followed by an equal pause for the
 * intake queued behind it. (Before this rule, a 14-day retention on a 171 GB
 * ledger drained at under a hundred rows a second: most passes were cut
 * short by cold reads, and each cut was followed by a 5 s wait.) A held
 * writer under a large backlog is retried after `fastFollowupMs`, doubling
 * on each consecutive hold up to `followupMs`, so a writer held for long is
 * not hammered. Without a large backlog the follow-up is `followupMs`; an
 * empty backlog rests `intervalMs`.
 */
export class AutomaticRetentionCadence {
  private accepting = true;
  private timer: unknown | null = null;
  private inFlight = false;
  private nextRetryAt: string | null = null;
  private lastPass: Receipt | null = null;
  private rows: number;
  private busyStreak = 0;
  private counters = { passes: 0, deferred: 0, busyDeferrals: 0, failures: 0,
    eventRowsVisited: 0, metricRowsVisited: 0, eventsExpired: 0, metricsExpired: 0,
    migrationProtectedRows: 0, grownPasses: 0, shrunkPasses: 0 };

  constructor(private readonly prune: (request: RetentionPassRequest) => Receipt, private readonly options: {
    canRun?: () => boolean;
    onPass?: (receipt: Receipt) => void;
    onError?: (error: unknown) => void;
    followupMs?: number;
    fastFollowupMs?: number;
    intervalMs?: number;
    timer?: AutomaticMaintenanceCadenceTimer;
    passRows?: { base?: number; max?: number; maxWriterMs?: number };
    /** When supplied, passes grow and follow fast only while it returns true. */
    largeBacklog?: () => boolean;
  } = {}) {
    this.rows = this.baseRows();
  }

  start() { if (this.accepting && !this.inFlight && this.timer === null) this.schedule(0); }
  stop() {
    this.accepting = false;
    if (this.timer !== null) this.timerApi().clearTimeout(this.timer);
    this.timer = null; this.nextRetryAt = null;
  }
  status() {
    return { accepting: this.accepting, inFlight: this.inFlight, nextRetryAt: this.nextRetryAt,
      lastPass: this.lastPass, passRows: this.rows, busyStreak: this.busyStreak,
      passBounds: { base: this.baseRows(), max: this.maxRows(), maxWriterMs: this.maxWriterMs() },
      followup: { ms: this.followupMs(), fastMs: this.fastFollowupMs() },
      counters: { ...this.counters } };
  }
  private baseRows() { return Math.max(1, Math.trunc(this.options.passRows?.base ?? RETENTION_PASS_BASE_ROWS)); }
  private maxRows() { return Math.max(this.baseRows(), Math.trunc(this.options.passRows?.max ?? RETENTION_PASS_MAX_ROWS)); }
  private maxWriterMs() { return Math.max(1, this.options.passRows?.maxWriterMs ?? RETENTION_PASS_WRITER_MS); }
  private followupMs() { return Math.max(1, this.options.followupMs ?? RETENTION_FOLLOWUP_MS); }
  private fastFollowupMs() {
    return Math.max(1, Math.min(this.followupMs(), this.options.fastFollowupMs ?? RETENTION_FAST_FOLLOWUP_MS));
  }
  private timerApi(): AutomaticMaintenanceCadenceTimer {
    return this.options.timer ?? { now: () => Date.now(),
      setTimeout: (callback, delay) => setTimeout(callback, delay),
      clearTimeout: handle => clearTimeout(handle as ReturnType<typeof setTimeout>) };
  }
  private schedule(delay: number) {
    if (!this.accepting) return;
    const timer = this.timerApi();
    this.nextRetryAt = new Date(timer.now() + delay).toISOString();
    this.timer = timer.setTimeout(() => this.run(), delay);
    (this.timer as { unref?: () => void } | null)?.unref?.();
  }
  private run() {
    this.timer = null; this.nextRetryAt = null;
    if (!this.accepting || this.inFlight) return;
    const followup = this.followupMs();
    const fastFollowup = this.fastFollowupMs();
    let delay = Math.max(followup, this.options.intervalMs ?? 6 * 60 * 60_000);
    this.inFlight = true;
    try {
      if (this.options.canRun?.() === false) {
        this.counters.deferred += 1; delay = followup; return;
      }
      const maxWriterMs = this.maxWriterMs();
      const receipt = this.prune({ maxRows: this.rows, maxWriterMs });
      this.lastPass = receipt;
      if (receipt.busy) {
        // Another connection held the writer. Keep the pass size. Under a
        // known large backlog retry soon, doubling on each consecutive hold.
        this.counters.busyDeferrals += 1;
        this.busyStreak += 1;
        delay = this.options.largeBacklog?.() === true
          ? Math.min(followup, fastFollowup * 2 ** Math.min(this.busyStreak - 1, 30))
          : followup;
        return;
      }
      this.busyStreak = 0;
      this.counters.passes += 1;
      this.counters.eventRowsVisited += receipt.eventRowsVisited;
      this.counters.metricRowsVisited += receipt.metricRowsVisited;
      this.counters.eventsExpired += receipt.events;
      this.counters.metricsExpired += receipt.metricSamples;
      this.counters.migrationProtectedRows += receipt.migrationProtectedRows;
      const continuing = receipt.hasMore && receipt.madeProgress;
      if (continuing) delay = followup;
      const elapsed = receipt.elapsedMs ?? 0;
      const backlog = continuing && (this.options.largeBacklog?.() ?? true);
      if (elapsed > maxWriterMs && this.rows > this.baseRows()) {
        this.rows = Math.max(this.baseRows(), Math.floor(this.rows / 2));
        this.counters.shrunkPasses += 1;
      } else if (backlog && elapsed * 2 <= maxWriterMs && this.rows < this.maxRows()) {
        this.rows = Math.min(this.maxRows(), this.rows * 2);
        this.counters.grownPasses += 1;
      }
      // Under a large backlog the next pass follows after the last pass's
      // own duration, between the fast follow-up and the follow-up.
      if (backlog) delay = Math.min(followup, Math.max(fastFollowup, elapsed));
      if (!receipt.hasMore) this.rows = this.baseRows();
      this.options.onPass?.(receipt);
    } catch (error) {
      this.counters.failures += 1;
      delay = followup;
      this.options.onError?.(error);
    } finally {
      this.inFlight = false;
      this.schedule(delay);
    }
  }
}

/** Retention passes defer while a reader pins a WAL at least this large: a
 * read snapshot pins every frame written after it, so the checkpoint worker
 * copies nothing back and the file grows with every pass (incident of
 * 2026-11-01: 1.4 GB to 12.6 GB in eighteen minutes at 871 rows a second). */
export const RETENTION_WAL_PRESSURE_BYTES = 1024 * 1024 * 1024;

/** True while the checkpoint worker reports a reader holding the WAL and the
 * file is at or past `limitBytes`. */
export function walPressureDefersRetention(
  status: { heldByReader: boolean; walBytes: number | null } | null | undefined,
  limitBytes = RETENTION_WAL_PRESSURE_BYTES,
): boolean {
  return Boolean(status && status.heldByReader && (status.walBytes ?? 0) >= limitBytes);
}
