import type Database from "better-sqlite3";

import { budgetHostClass } from "./budget-sampler";

/** The schema default for `retentionDays`; the value a config file carries
 * when nobody chose one, because every config write materializes defaults. */
export const DEFAULT_RETENTION_DAYS = 90;
/** The budget rule never derives fewer days than this. */
export const BUDGET_RETENTION_FLOOR_DAYS = 7;
/**
 * Bytes one raw row (a `buffered_events` or `metric_samples` row with its
 * indexes) occupies on disk. Measured on 2026-11-01 on a `studio0_scale` host:
 * about 103 GB of raw rows and indexes over 43 million rows. It is an
 * estimate and status names it as one; `dbstat` is skipped on ledgers of that
 * size, so the collector cannot measure it live.
 */
export const RAW_BYTES_PER_ROW_ESTIMATE = 2_400;

export type RetentionPolicySource = "config" | "budget" | "default";

export type RetentionPolicyBasis = {
  hostClass: string | null;
  attemptedRowsPerDay: number | null;
  observedDays: number;
  provisional: boolean;
  ledgerBytesTarget: number | null;
  rawBytesPerRowEstimate: number;
  /** Whole days the target holds at the measured rate, before the floor and cap. */
  projectedDays: number | null;
};

export type RetentionPolicy = {
  /** The raw retention the collector enforces. */
  days: number;
  source: RetentionPolicySource;
  /** One sentence an operator can read in `/status`. */
  reason: string;
  configuredDays: number;
  pinned: boolean;
  basis: RetentionPolicyBasis;
  resolvedAt: string;
};

export type RetentionPolicyInput = {
  configuredDays: number;
  /** `retentionDaysPinned` in the config: keep the configured value even when it equals the default. */
  pinned?: boolean;
  hostClass: string | null;
  attemptedRowsPerDay: number | null;
  observedDays?: number;
  provisional?: boolean;
  ledgerBytesTarget: number | null;
  rawBytesPerRowEstimate?: number;
  now?: Date;
};

/**
 * Pure derivation. A configured value other than the default, or a pinned
 * value, always wins. On a `studio0_scale` host with a measured admission rate
 * and a ledger target, the default becomes the largest whole number of days
 * whose projected raw bytes fit the target, floored at 7 and never above the
 * default. Every other host keeps the default.
 */
export function deriveRetentionPolicy(input: RetentionPolicyInput): RetentionPolicy {
  const configuredDays = Math.max(1, Math.trunc(input.configuredDays));
  const pinned = Boolean(input.pinned);
  const bytesPerRow = input.rawBytesPerRowEstimate ?? RAW_BYTES_PER_ROW_ESTIMATE;
  const rate = input.attemptedRowsPerDay !== null && Number.isFinite(input.attemptedRowsPerDay) &&
    input.attemptedRowsPerDay > 0 ? input.attemptedRowsPerDay : null;
  const target = input.ledgerBytesTarget !== null && Number.isFinite(input.ledgerBytesTarget) &&
    input.ledgerBytesTarget > 0 ? input.ledgerBytesTarget : null;
  const projectedDays = rate !== null && target !== null ? Math.floor(target / (rate * bytesPerRow)) : null;
  const basis: RetentionPolicyBasis = {
    hostClass: input.hostClass,
    attemptedRowsPerDay: rate,
    observedDays: input.observedDays ?? 0,
    provisional: input.provisional ?? true,
    ledgerBytesTarget: target,
    rawBytesPerRowEstimate: bytesPerRow,
    projectedDays,
  };
  const resolvedAt = (input.now ?? new Date()).toISOString();
  if (pinned || configuredDays !== DEFAULT_RETENTION_DAYS) {
    return { days: configuredDays, source: "config", configuredDays, pinned, basis, resolvedAt,
      reason: pinned
        ? `retentionDays ${configuredDays} is pinned in the config`
        : `retentionDays ${configuredDays} was set in the config` };
  }
  if (input.hostClass === "studio0_scale" && projectedDays !== null && !(input.provisional ?? true)) {
    const days = Math.max(BUDGET_RETENTION_FLOOR_DAYS, Math.min(DEFAULT_RETENTION_DAYS, projectedDays));
    return { days, source: "budget", configuredDays, pinned, basis, resolvedAt,
      reason: `studio0_scale host admitting about ${Math.round(rate!).toLocaleString("en-US")} rows a day: ` +
        `${target!.toLocaleString("en-US")} ledger bytes hold ${projectedDays} days at an estimated ` +
        `${bytesPerRow} bytes a row; floored at ${BUDGET_RETENTION_FLOOR_DAYS}, capped at ${DEFAULT_RETENTION_DAYS}` };
  }
  return { days: configuredDays, source: "default", configuredDays, pinned, basis, resolvedAt,
    reason: input.hostClass === "studio0_scale"
      ? "studio0_scale host, but the admission rate is still provisional; the default holds until seven days are measured"
      : `host class ${input.hostClass ?? "unmeasured"} keeps the default ${DEFAULT_RETENTION_DAYS} days` };
}

/** Resolve the policy against a ledger's budget tables. Reads two small tables. */
export function resolveRetentionPolicy(
  config: { retentionDays: number; retentionDaysPinned?: boolean },
  db: Database.Database,
  now = new Date(),
): RetentionPolicy {
  let host: ReturnType<typeof budgetHostClass>;
  try { host = budgetHostClass(db, now.getTime()); }
  catch {
    host = { hostClass: null, attemptedRowsPerDay: null, observedDays: 0, provisional: true,
      rateBasis: "budget_read_failed", targets: null };
  }
  return deriveRetentionPolicy({
    configuredDays: config.retentionDays,
    pinned: config.retentionDaysPinned,
    hostClass: host.hostClass,
    attemptedRowsPerDay: host.attemptedRowsPerDay,
    observedDays: host.observedDays,
    provisional: host.provisional,
    ledgerBytesTarget: host.targets?.ledgerBytes ?? null,
    now,
  });
}
