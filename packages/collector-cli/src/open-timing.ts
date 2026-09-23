export type LedgerOpenTimingStep = {
  step: string;
  durationMs: number;
  elapsedMs: number;
};

export type LedgerOpenTimingSink = (step: LedgerOpenTimingStep) => void;
