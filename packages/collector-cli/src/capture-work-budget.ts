export const AUTOMATIC_CAPTURE_LIMITS = Object.freeze({
  maxBytes: 512 * 1024,
  maxRecords: 512,
  maxEvents: 512,
  maxWallMs: 200,
  sliceBytes: 64 * 1024,
  sliceRecords: 64,
});

export type CaptureBudgetLimits = {
  [Key in keyof typeof AUTOMATIC_CAPTURE_LIMITS]: number;
};

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

type BudgetExhaustion = CaptureBudgetStatus["exhaustedBy"];

type BudgetScopeOptions = Partial<CaptureBudgetLimits>;
type BudgetScopePolicy = {
  /** Permit a reserved source slice after an earlier synchronous wall overrun. */
  allowWallOverrun?: boolean;
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
    private readonly parent: CaptureWorkBudget | null = null,
    private readonly policy: BudgetScopePolicy = {},
  ) {}

  /**
   * Create a source-local view of this budget.
   *
   * Automatic maintenance has several independent producers sharing one
   * cadence. A producer which reaches the global wall/byte/event ceiling first
   * must not consume the allowance reserved for the producers that follow it.
   * A scoped budget keeps the parent ceilings as a hard upper bound while
   * enforcing the supplied per-source caps. Accounting is charged to both
   * views, so the existing aggregate receipt remains unchanged.
   */
  scoped(overrides: BudgetScopeOptions, policy: BudgetScopePolicy = {}): CaptureWorkBudget {
    const maxBytes = Math.max(2_048, Math.min(
      Math.trunc(overrides.maxBytes ?? this.remainingByteBudget()),
      Math.max(2_048, this.remainingByteBudget()),
    ));
    const maxRecords = Math.max(1, Math.min(
      Math.trunc(overrides.maxRecords ?? this.remainingRecordSlots()),
      Math.max(1, this.remainingRecordSlots()),
    ));
    const maxEvents = Math.max(1, Math.min(
      Math.trunc(overrides.maxEvents ?? this.remainingEventSlots()),
      Math.max(1, this.remainingEventSlots()),
    ));
    const parentWallRemaining = policy.allowWallOverrun ? Number.MAX_SAFE_INTEGER : this.remainingWallMs();
    const maxWallMs = Math.max(1, Math.min(
      Math.trunc(overrides.maxWallMs ?? (policy.allowWallOverrun
        ? this.limits.maxWallMs
        : this.remainingWallMs())),
      Math.max(1, parentWallRemaining),
    ));
    const sliceBytes = Math.max(2_048, Math.min(
      Math.trunc(overrides.sliceBytes ?? this.limits.sliceBytes), maxBytes,
    ));
    const sliceRecords = Math.max(1, Math.min(
      Math.trunc(overrides.sliceRecords ?? this.limits.sliceRecords), maxRecords, maxEvents,
    ));
    return new CaptureWorkBudget({
      maxBytes,
      maxRecords,
      maxEvents,
      maxWallMs,
      sliceBytes,
      sliceRecords,
    }, this, policy);
  }

  remainingSlice(retryOversizedRecord = false) {
    if (!this.canContinue()) return null;
    const remainingBytes = this.remainingByteBudget();
    if (remainingBytes < 2_048) return null;
    return {
      maxBytes: Math.max(
        2_048,
        // An unresolved record needs a larger read to make progress. Permit
        // its retry to use the remaining cadence allowance without raising
        // the shared byte, record, event, or wall-clock ceilings.
        retryOversizedRecord ? remainingBytes : Math.min(this.limits.sliceBytes, remainingBytes),
      ),
      maxRecords: Math.max(
        1,
        Math.min(
          this.limits.sliceRecords,
          this.remainingRecordSlots(),
          this.remainingEventSlots(),
        ),
      ),
    };
  }

  canContinue(): boolean {
    const parentCanContinue = this.parent
      ? (this.policy.allowWallOverrun
        ? this.parent.countersCanContinue()
        : this.parent.canContinue())
      : true;
    return parentCanContinue && this.localExhaustedBy() === null;
  }

  recordSlice(input: { bytesRead: number; recordsParsed: number; eventsAppended: number }) {
    const bytesRead = Math.max(0, input.bytesRead);
    const recordsParsed = Math.max(0, input.recordsParsed);
    const eventsAppended = Math.max(0, input.eventsAppended);
    this.bytesRead += bytesRead;
    this.recordsParsed += recordsParsed;
    this.eventsAppended += eventsAppended;
    this.slices += 1;
    this.parent?.recordSlice({ bytesRead, recordsParsed, eventsAppended });
  }

  recordYield() {
    this.yields += 1;
    this.parent?.recordYield();
  }

  elapsedWallMs(): number {
    return this.parent?.elapsedWallMs() ?? Math.max(0, performance.now() - this.startedAt);
  }

  remainingWallMs(): number {
    const local = Math.max(0, this.limits.maxWallMs - (performance.now() - this.startedAt));
    return this.policy.allowWallOverrun
      ? local
      : Math.min(local, this.parent?.remainingWallMs() ?? local);
  }

  canStart(minimumWallMs = 1) {
    return this.canContinue() && this.remainingWallMs() >= Math.max(0, minimumWallMs);
  }

  remainingEventSlots(): number {
    return Math.min(
      Math.max(0, this.limits.maxEvents - this.eventsAppended),
      this.parent?.remainingEventSlots() ?? Number.MAX_SAFE_INTEGER,
    );
  }

  remainingRecordSlots(): number {
    return Math.min(
      Math.max(0, this.limits.maxRecords - this.recordsParsed),
      this.parent?.remainingRecordSlots() ?? Number.MAX_SAFE_INTEGER,
    );
  }

  remainingByteBudget(): number {
    return Math.min(
      Math.max(0, this.limits.maxBytes - this.bytesRead),
      this.parent?.remainingByteBudget() ?? Number.MAX_SAFE_INTEGER,
    );
  }

  status(): CaptureBudgetStatus {
    const aggregate = this.parent?.status() ?? this.rootStatus();
    const exhaustedBy = aggregate.exhaustedBy ?? this.localExhaustedBy();
    return {
      // Source receipts retain the aggregate ceilings/counters. This keeps
      // the existing dashboard and worker contract stable while canContinue()
      // still observes the narrower source-local share.
      maxBytes: aggregate.maxBytes,
      maxRecords: aggregate.maxRecords,
      maxEvents: aggregate.maxEvents,
      maxWallMs: aggregate.maxWallMs,
      bytesRead: aggregate.bytesRead,
      recordsParsed: aggregate.recordsParsed,
      eventsAppended: aggregate.eventsAppended,
      slices: aggregate.slices,
      yields: aggregate.yields,
      elapsedWallMs: aggregate.elapsedWallMs,
      exhausted: exhaustedBy !== null,
      exhaustedBy,
    };
  }

  private rootStatus(): CaptureBudgetStatus {
    const exhaustedBy = this.localExhaustedBy();
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

  private localExhaustedBy(): BudgetExhaustion {
    if (this.limits.maxBytes - this.bytesRead < 2_048) return "bytes";
    if (this.recordsParsed >= this.limits.maxRecords) return "records";
    if (this.eventsAppended >= this.limits.maxEvents) return "events";
    if (performance.now() - this.startedAt >= this.limits.maxWallMs) return "wall";
    return null;
  }

  private countersCanContinue(): boolean {
    return this.remainingByteBudget() >= 2_048 &&
      this.remainingRecordSlots() > 0 && this.remainingEventSlots() > 0;
  }
}
