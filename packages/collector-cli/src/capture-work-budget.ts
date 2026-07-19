export const AUTOMATIC_CAPTURE_LIMITS = Object.freeze({
  maxBytes: 512 * 1024,
  maxRecords: 512,
  maxEvents: 512,
  maxWallMs: 200,
  sliceBytes: 64 * 1024,
  sliceRecords: 64,
});

export type CaptureBudgetLimits = typeof AUTOMATIC_CAPTURE_LIMITS;

export type CaptureBudgetStatus = {
  maxBytes: number;
  maxRecords: number;
  maxEvents: number;
  maxWallMs: number;
  bytesRead: number;
  recordsParsed: number;
  eventsAppended: number;
  slices: number;
  yields: number;
  elapsedWallMs: number;
  exhausted: boolean;
  exhaustedBy: "bytes" | "records" | "events" | "wall" | null;
};

/**
 * One shared automatic-maintenance budget. The wall clock starts before
 * discovery, so enumeration, stat calls, exclusion bookkeeping, parsing and
 * SQLite commits all consume the same cadence allowance. A single synchronous
 * filesystem/SQLite call can still overrun the deadline; every call site must
 * check before starting the next bounded unit.
 */
export class CaptureWorkBudget {
  private readonly startedAt = performance.now();
  private bytesRead = 0;
  private recordsParsed = 0;
  private eventsAppended = 0;
  private slices = 0;
  private yields = 0;

  constructor(
    private readonly limits: CaptureBudgetLimits = AUTOMATIC_CAPTURE_LIMITS,
  ) {}

  remainingSlice() {
    if (!this.canContinue()) return null;
    const remainingBytes = this.limits.maxBytes - this.bytesRead;
    if (remainingBytes < 2_048) return null;
    return {
      maxBytes: Math.max(
        2_048,
        Math.min(this.limits.sliceBytes, remainingBytes),
      ),
      maxRecords: Math.max(
        1,
        Math.min(
          this.limits.sliceRecords,
          this.limits.maxRecords - this.recordsParsed,
          this.limits.maxEvents - this.eventsAppended,
        ),
      ),
    };
  }

  canContinue() {
    return this.exhaustedBy() === null;
  }

  recordSlice(input: { bytesRead: number; recordsParsed: number; eventsAppended: number }) {
    this.bytesRead += Math.max(0, input.bytesRead);
    this.recordsParsed += Math.max(0, input.recordsParsed);
    this.eventsAppended += Math.max(0, input.eventsAppended);
    this.slices += 1;
  }

  recordYield() {
    this.yields += 1;
  }

  elapsedWallMs() {
    return Math.max(0, performance.now() - this.startedAt);
  }

  remainingWallMs() {
    return Math.max(0, this.limits.maxWallMs - this.elapsedWallMs());
  }

  canStart(minimumWallMs = 1) {
    return this.canContinue() && this.remainingWallMs() >= Math.max(0, minimumWallMs);
  }

  remainingEventSlots() {
    return Math.max(0, this.limits.maxEvents - this.eventsAppended);
  }

  status(): CaptureBudgetStatus {
    const exhaustedBy = this.exhaustedBy();
    return {
      maxBytes: this.limits.maxBytes,
      maxRecords: this.limits.maxRecords,
      maxEvents: this.limits.maxEvents,
      maxWallMs: this.limits.maxWallMs,
      bytesRead: this.bytesRead,
      recordsParsed: this.recordsParsed,
      eventsAppended: this.eventsAppended,
      slices: this.slices,
      yields: this.yields,
      elapsedWallMs: Number(this.elapsedWallMs().toFixed(3)),
      exhausted: exhaustedBy !== null,
      exhaustedBy,
    };
  }

  private exhaustedBy(): CaptureBudgetStatus["exhaustedBy"] {
    if (this.limits.maxBytes - this.bytesRead < 2_048) return "bytes";
    if (this.recordsParsed >= this.limits.maxRecords) return "records";
    if (this.eventsAppended >= this.limits.maxEvents) return "events";
    if (this.elapsedWallMs() >= this.limits.maxWallMs) return "wall";
    return null;
  }
}
