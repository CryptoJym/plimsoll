/** Two ordinary capture cadences. Availability never implies current evidence. */
export const STATUS_MAX_AGE_MS = 120_000;

export function evidenceAge(at: string | null | undefined, nowMs = Date.now()) {
  const age = at ? nowMs - Date.parse(at) : NaN;
  return Number.isFinite(age) && age >= 0 ? age : null;
}

export function projectionValidity(input: {
  ready: boolean;
  parityReady: boolean;
  dirty: boolean;
  degradedReason: string | null;
  lastSuccessAt: string | null;
}, nowMs = Date.now(), invalidReason: string | null = null) {
  const ageMs = evidenceAge(input.lastSuccessAt, nowMs);
  const reason = invalidReason ?? input.degradedReason ??
    (input.dirty ? "projection_pending" : !input.ready || !input.parityReady ? "projection_not_reconciled" :
      ageMs === null ? "projection_time_unknown" : ageMs > STATUS_MAX_AGE_MS ? "projection_expired" : null);
  return {
    status: reason ? "stale" as const : "ready" as const,
    ready: reason === null,
    parityReady: reason === null,
    dirty: input.dirty,
    degraded: reason !== null,
    degradedReason: reason,
    lastGoodAt: input.lastSuccessAt,
    lastSuccessAt: input.lastSuccessAt,
    ageMs,
    maxAgeMs: STATUS_MAX_AGE_MS,
  };
}
