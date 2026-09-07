import type { LocalEventBuffer } from "./buffer";
import type { AutomaticMaintenanceCadenceTimer } from "./maintenance";

type Receipt = ReturnType<LocalEventBuffer["prune"]>;

/** One bounded pass per turn; schedule the next only after the pass settles. */
export class AutomaticRetentionCadence {
  private accepting = true;
  private timer: unknown | null = null;
  private inFlight = false;
  private nextRetryAt: string | null = null;
  private lastPass: Receipt | null = null;
  private counters = { passes: 0, deferred: 0, failures: 0,
    eventRowsVisited: 0, metricRowsVisited: 0, eventsExpired: 0, metricsExpired: 0,
    migrationProtectedRows: 0 };

  constructor(private readonly prune: () => Receipt, private readonly options: {
    canRun?: () => boolean;
    onPass?: (receipt: Receipt) => void;
    onError?: (error: unknown) => void;
    followupMs?: number;
    intervalMs?: number;
    timer?: AutomaticMaintenanceCadenceTimer;
  } = {}) {}

  start() { if (this.accepting && !this.inFlight && this.timer === null) this.schedule(0); }
  stop() {
    this.accepting = false;
    if (this.timer !== null) this.timerApi().clearTimeout(this.timer);
    this.timer = null; this.nextRetryAt = null;
  }
  status() {
    return { accepting: this.accepting, inFlight: this.inFlight, nextRetryAt: this.nextRetryAt,
      lastPass: this.lastPass, counters: { ...this.counters } };
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
    const followup = Math.max(1, this.options.followupMs ?? 5_000);
    let delay = Math.max(followup, this.options.intervalMs ?? 6 * 60 * 60_000);
    this.inFlight = true;
    try {
      if (this.options.canRun?.() === false) {
        this.counters.deferred += 1; delay = followup; return;
      }
      const receipt = this.prune();
      this.lastPass = receipt;
      this.counters.passes += 1;
      this.counters.eventRowsVisited += receipt.eventRowsVisited;
      this.counters.metricRowsVisited += receipt.metricRowsVisited;
      this.counters.eventsExpired += receipt.events;
      this.counters.metricsExpired += receipt.metricSamples;
      this.counters.migrationProtectedRows += receipt.migrationProtectedRows;
      if (receipt.hasMore) delay = followup;
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
