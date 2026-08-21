/**
 * Capacity planning intelligence (issue #173).
 *
 * This module is deliberately pure and descriptive. It turns owner-supplied
 * provider facts (billing renewal dates, quota reset schedules, earned-reset
 * expiries, plan limits) and local token telemetry into planning views:
 * a reset calendar, stale-signal alerts, per-provider constraint summaries,
 * low-headroom notifications, and gated linear pace estimates.
 *
 * Doctrine enforced here and by scripts/capacity-proof.ts:
 * - Every derived number is labeled `derived` and `uncertain` and carries its
 *   observation window and freshness.
 * - Missing or stale evidence stays literal UNKNOWN and never becomes zero.
 * - Billing renewal, quota reset, earned-reset expiry, local token telemetry,
 *   subscription cost, and API-equivalent value remain separate facts; none
 *   is collapsed into another.
 * - Multiple provider profiles remain distinct unless an explicit merge is
 *   approved in the input.
 * - Nothing here routes tasks, coaches, ranks, compensates, disciplines,
 *   intervenes, or issues performance verdicts. Capacity never feeds those
 *   surfaces; the static dependency proof blocks that wiring at the source
 *   level.
 */

export const CAPACITY_SCHEMA_VERSION = 1 as const;
export const CAPACITY_PLAN_SCHEMA = "plimsoll.capacity-plan.v1" as const;

export const CAPACITY_IDENTITY_MAX_LENGTH = 128 as const;
const CAPACITY_IDENTITY_REGEX =
  /^[A-Za-z0-9](?:[A-Za-z0-9._:@/-]{0,126}[A-Za-z0-9])?$/;

/** Reset-calendar event kinds stay separate facts, never summed or merged. */
export const CAPACITY_RESET_KINDS = [
  "billing_renewal",
  "quota_reset",
  "earned_reset_expiry",
] as const;
export type CapacityResetKind = (typeof CAPACITY_RESET_KINDS)[number];

/** Units are distinct dimensions; unlike units are never added or compared. */
export const CAPACITY_UNITS = ["tokens", "requests", "usd", "percent"] as const;
export type CapacityUnit = (typeof CAPACITY_UNITS)[number];

export type CapacitySignalSource = "local_telemetry" | "provider_report";

export type CapacityProfile = {
  profileId: string;
  /** Provider family, e.g. "anthropic" or "openai". Profiles of the same
   * provider still stay distinct until an explicit merge is approved. */
  provider: string;
};

export type CapacityResetEvent = {
  eventId: string;
  profileId: string;
  kind: CapacityResetKind;
  /** ISO timestamp of the scheduled reset/renewal/expiry. */
  at: string;
};

export type CapacityUsageSignal = {
  signalId: string;
  profileId: string;
  /** Bounded constraint dimension label, e.g. "five_hour_window". */
  dimension: string;
  unit: CapacityUnit;
  /** Known plan limit; null means the limit is UNKNOWN, not unlimited/zero. */
  limit: number | null;
  /** Observed consumption; null means usage is UNKNOWN, not zero. */
  used: number | null;
  source: CapacitySignalSource;
  /** Freshness anchor for this observation; null means never observed. */
  observedAt: string | null;
};

export type CapacityCostFacts = {
  profileId: string;
  /** Subscription price; kept separate from telemetry and API-equivalent value. */
  subscriptionUsdPerMonth: number | null;
  /** API-equivalent value of consumed tokens; never blended into the above. */
  apiEquivalentUsd: number | null;
  observedAt: string;
};

/** A single local-telemetry observation feeding gated pace estimates. */
export type CapacityPaceObservation = {
  at: string;
  cumulativeTokens: number;
};

export type CapacityFreshnessStatus = "fresh" | "STALE" | "UNKNOWN";

export type CapacityStaleAlert = {
  profileId: string;
  dimension: string;
  severity: "STALE_SIGNAL" | "MISSING_SIGNAL";
  message: string;
  lastObservedAt: string | null;
  ageMs: number | null;
};

export type CapacityHeadroomNotification = {
  profileId: string;
  dimension: string;
  remainingFraction: number;
  used: number;
  limit: number;
  unit: CapacityUnit;
  observedAt: string;
};

export type CapacityConstraintRow = {
  profileId: string;
  dimension: string;
  unit: CapacityUnit;
  source: CapacitySignalSource;
  freshness: CapacityFreshnessStatus;
  ageMs: number | null;
  lastObservedAt: string | null;
  limit: number | null;
  used: number | null;
  /** Fresh-evidence headroom only; stale or missing evidence stays null. */
  remaining: number | null;
  /** Fresh-evidence fraction only; stale or missing evidence stays null. */
  remainingFraction: number | null;
};

export type CapacityProfileSummary = {
  profileId: string;
  provider: string;
  constraints: CapacityConstraintRow[];
  lowHeadroomNotifications: CapacityHeadroomNotification[];
};

export type CapacityCalendarEntry = {
  eventId: string;
  profileId: string;
  kind: CapacityResetKind;
  at: string;
  msUntil: number;
  daysUntil: number;
};

/** Pace estimates carry their gates, window, and freshness on every result. */
export type CapacityPaceEstimate = {
  state: "ESTIMATED" | "UNKNOWN";
  reason: string | null;
  claimClass: "observed" | "derived";
  certainty: "uncertain" | null;
  method: string | null;
  tokensPerDay: number | null;
  observationWindow: { since: string; until: string } | null;
  elapsedMs: number | null;
  observationCount: number;
  freshness: { asOf: string; lastObservedAt: string; ageMs: number } | null;
  limitations: string[];
  /** Projected exhaustion is derived from a fresh limit plus the pace only. */
  projectedExhaustion: {
    state: "PROJECTED" | "UNKNOWN";
    reason: string | null;
    projectedAt: string | null;
    limitTokens: number | null;
    basis: string;
  };
};

export type CapacityPlan = {
  schema: typeof CAPACITY_PLAN_SCHEMA;
  schemaVersion: typeof CAPACITY_SCHEMA_VERSION;
  asOf: string;
  profilesDistinct: true;
  mergeApproval: "none" | "approved";
  calendar: CapacityCalendarEntry[];
  summaries: CapacityProfileSummary[];
  staleAlerts: CapacityStaleAlert[];
  paceEstimates: Array<{ profileId: string; estimate: CapacityPaceEstimate }>;
  unknownLegend: string;
};

export type CapacityMergeApproval = {
  approved: boolean;
  approvedBy: string;
  approvedAt: string;
};

function requireIdentity(field: string, value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > CAPACITY_IDENTITY_MAX_LENGTH ||
    !CAPACITY_IDENTITY_REGEX.test(value)
  ) {
    throw new Error(`invalid capacity ${field}`);
  }
  return value;
}

function requireIsoTimestamp(field: string, value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) {
    throw new Error(`invalid capacity ${field}`);
  }
  return value;
}

function requireNonNegativeNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) {
    throw new Error("invalid capacity non-negative number");
  }
  return value;
}

/**
 * The reset calendar lists upcoming billing renewals, quota resets, and
 * earned-reset expiries as separate entries inside the horizon. Kinds are
 * never collapsed; each entry names its profile.
 */
export function buildCapacityResetCalendar(input: {
  events: CapacityResetEvent[];
  now: string;
  horizonDays?: number;
}): CapacityCalendarEntry[] {
  const nowMs = Date.parse(requireIsoTimestamp("now", input.now));
  const horizonDays = input.horizonDays ?? 30;
  if (!Number.isInteger(horizonDays) || horizonDays < 1) {
    throw new Error("capacity calendar horizon must be a positive integer");
  }
  const horizonEndMs = nowMs + horizonDays * 24 * 60 * 60 * 1000;
  const seen = new Set<string>();
  const entries: CapacityCalendarEntry[] = [];
  for (const event of input.events) {
    requireIdentity("eventId", event.eventId);
    requireIdentity("profileId", event.profileId);
    if (!CAPACITY_RESET_KINDS.includes(event.kind)) {
      throw new Error(`invalid capacity reset kind ${String(event.kind)}`);
    }
    const atMs = Date.parse(requireIsoTimestamp("event.at", event.at));
    if (seen.has(event.eventId)) {
      throw new Error(`duplicate capacity eventId ${event.eventId}`);
    }
    seen.add(event.eventId);
    if (atMs < nowMs || atMs > horizonEndMs) continue;
    entries.push({
      eventId: event.eventId,
      profileId: event.profileId,
      kind: event.kind,
      at: event.at,
      msUntil: atMs - nowMs,
      daysUntil: Math.round(((atMs - nowMs) / (24 * 60 * 60 * 1000)) * 10) / 10,
    });
  }
  return entries.sort(
    (left, right) => left.at.localeCompare(right.at) || left.eventId.localeCompare(right.eventId),
  );
}

/**
 * Freshness classification for one signal. Missing evidence is UNKNOWN —
 * never zero, never fresh-by-default. Older than maxAgeMs is STALE — it stays
 * visible as stale rather than being silently trusted or zeroed.
 */
export function classifyCapacitySignalFreshness(input: {
  observedAt: string | null;
  now: string;
  maxAgeMs: number;
}): { status: CapacityFreshnessStatus; ageMs: number | null } {
  const nowMs = Date.parse(requireIsoTimestamp("now", input.now));
  if (!Number.isFinite(input.maxAgeMs) || input.maxAgeMs <= 0) {
    throw new Error("capacity maxAgeMs must be positive");
  }
  if (input.observedAt === null) return { status: "UNKNOWN", ageMs: null };
  const observedMs = Date.parse(requireIsoTimestamp("observedAt", input.observedAt));
  const ageMs = Math.max(0, nowMs - observedMs);
  return { status: ageMs <= input.maxAgeMs ? "fresh" : "STALE", ageMs };
}

function summarizeConstraintsForProfile(
  signals: CapacityUsageSignal[],
  options: { now: string; maxAgeMs: number; lowHeadroomFraction: number },
): { constraints: CapacityConstraintRow[]; notifications: CapacityHeadroomNotification[] } {
  const constraints: CapacityConstraintRow[] = [];
  const notifications: CapacityHeadroomNotification[] = [];
  const seen = new Set<string>();
  for (const signal of signals) {
    requireIdentity("signalId", signal.signalId);
    requireIdentity("profileId", signal.profileId);
    requireIdentity("dimension", signal.dimension);
    if (!CAPACITY_UNITS.includes(signal.unit)) {
      throw new Error(`invalid capacity unit ${String(signal.unit)}`);
    }
    if (!["local_telemetry", "provider_report"].includes(signal.source)) {
      throw new Error(`invalid capacity signal source ${String(signal.source)}`);
    }
    const key = `${signal.profileId}\u0000${signal.dimension}`;
    if (seen.has(key)) {
      throw new Error(`duplicate capacity dimension ${signal.profileId}/${signal.dimension}`);
    }
    seen.add(key);
    const freshness = classifyCapacitySignalFreshness({
      observedAt: signal.observedAt,
      now: options.now,
      maxAgeMs: options.maxAgeMs,
    });
    const limit = requireNonNegativeNumber(signal.limit);
    const used = requireNonNegativeNumber(signal.used);
    // Headroom exists only when BOTH sides are known AND the observation is
    // fresh. Stale or missing evidence keeps remaining/fraction null (UNKNOWN)
    // — it never degrades to zero and never raises an alarm on absent data.
    const usable =
      freshness.status === "fresh" && limit !== null && used !== null && limit >= 0 && used <= Number.MAX_SAFE_INTEGER;
    const remaining = usable ? Math.max(0, limit! - used!) : null;
    const remainingFraction = usable && limit! > 0 ? Math.max(0, Math.min(1, remaining! / limit!)) : null;
    constraints.push({
      profileId: signal.profileId,
      dimension: signal.dimension,
      unit: signal.unit,
      source: signal.source,
      freshness: freshness.status,
      ageMs: freshness.ageMs,
      lastObservedAt: signal.observedAt ?? null,
      limit,
      used,
      remaining,
      remainingFraction,
    });
    if (remainingFraction !== null && remainingFraction <= options.lowHeadroomFraction) {
      notifications.push({
        profileId: signal.profileId,
        dimension: signal.dimension,
        remainingFraction,
        used: used!,
        limit: limit!,
        unit: signal.unit,
        observedAt: signal.observedAt as string,
      });
    }
  }
  constraints.sort((left, right) =>
    left.dimension.localeCompare(right.dimension),
  );
  notifications.sort((left, right) =>
    left.remainingFraction - right.remainingFraction ||
    left.dimension.localeCompare(right.dimension),
  );
  return { constraints, notifications };
}

function staleAlertsForProfile(
  signals: CapacityUsageSignal[],
  options: { now: string; maxAgeMs: number },
): CapacityStaleAlert[] {
  const alerts: CapacityStaleAlert[] = [];
  for (const signal of signals) {
    const freshness = classifyCapacitySignalFreshness({
      observedAt: signal.observedAt,
      now: options.now,
      maxAgeMs: options.maxAgeMs,
    });
    if (freshness.status === "STALE") {
      alerts.push({
        profileId: signal.profileId,
        dimension: signal.dimension,
        severity: "STALE_SIGNAL",
        message:
          `signal ${signal.signalId} for ${signal.profileId}/${signal.dimension} is stale; ` +
          "its capacity stays UNKNOWN, not zero",
        lastObservedAt: signal.observedAt,
        ageMs: freshness.ageMs,
      });
    } else if (freshness.status === "UNKNOWN") {
      alerts.push({
        profileId: signal.profileId,
        dimension: signal.dimension,
        severity: "MISSING_SIGNAL",
        message:
          `signal ${signal.signalId} for ${signal.profileId}/${signal.dimension} has no ` +
          "observation timestamp; its capacity stays UNKNOWN, not zero",
        lastObservedAt: null,
        ageMs: null,
      });
    }
  }
  return alerts.sort((left, right) =>
    left.severity.localeCompare(right.severity) ||
    left.profileId.localeCompare(right.profileId) ||
    left.dimension.localeCompare(right.dimension),
  );
}

/**
 * Gated linear pace estimate over local token telemetry. Two hard gates must
 * pass before any estimate is emitted: minimum elapsed time between the first
 * and last observation AND a minimum observation count. Until then the result
 * is UNKNOWN with the failing gate named — never a zero pace.
 *
 * Every ESTIMATED result is labeled claimClass "derived" and certainty
 * "uncertain" and carries its exact observation window and freshness.
 */
export function estimateCapacityLinearPace(input: {
  observations: CapacityPaceObservation[];
  now: string;
  minElapsedMs?: number;
  minObservations?: number;
  knownLimitTokens?: number | null;
  limitObservedAt?: string | null;
  maxAgeMs?: number;
}): CapacityPaceObservationsResult {
  const minElapsedMs = input.minElapsedMs ?? 24 * 60 * 60 * 1000;
  const minObservations = input.minObservations ?? 3;
  const nowMs = Date.parse(requireIsoTimestamp("now", input.now));
  if (!Number.isFinite(minElapsedMs) || minElapsedMs <= 0) {
    throw new Error("capacity minElapsedMs must be positive");
  }
  if (!Number.isInteger(minObservations) || minObservations < 2) {
    throw new Error("capacity minObservations must be an integer >= 2");
  }
  const observations = [...input.observations].map((observation) => ({
    at: requireIsoTimestamp("observation.at", observation.at),
    cumulativeTokens: requireNonNegativeNumber(observation.cumulativeTokens)!,
  }));
  observations.sort((left, right) => left.at.localeCompare(right.at));

  const limitations = [
    "Linear interpolation over local telemetry; assumes constant consumption rate.",
    "Derived and uncertain: real demand is bursty, so projections drift.",
    "Descriptive planning aid only — never a routing, coaching, ranking, or verdict input.",
  ];

  const base: CapacityPaceEstimate = {
    state: "UNKNOWN",
    reason: null,
    claimClass: "observed",
    certainty: null,
    method: null,
    tokensPerDay: null,
    observationWindow: null,
    elapsedMs: null,
    observationCount: observations.length,
    freshness: null,
    limitations,
    projectedExhaustion: { state: "UNKNOWN", reason: "pace_unknown", projectedAt: null, limitTokens: null, basis: "" },
  };

  if (observations.length < minObservations) {
    return {
      ...base,
      reason: `observation_count_gate_not_met: ${observations.length}/${minObservations}`,
      projectedExhaustion: {
        ...base.projectedExhaustion,
        reason: `observation_count_gate_not_met: ${observations.length}/${minObservations}`,
      },
    };
  }
  const first = observations[0]!;
  const last = observations[observations.length - 1]!;
  const elapsedMs = Date.parse(last.at) - Date.parse(first.at);
  if (elapsedMs < minElapsedMs) {
    return {
      ...base,
      reason: `elapsed_time_gate_not_met: ${elapsedMs}ms/${minElapsedMs}ms`,
      projectedExhaustion: {
        ...base.projectedExhaustion,
        reason: `elapsed_time_gate_not_met: ${elapsedMs}ms/${minElapsedMs}ms`,
      },
    };
  }

  const deltaTokens = last.cumulativeTokens - first.cumulativeTokens;
  const tokensPerDay = Math.round(((deltaTokens / elapsedMs) * 86_400_000) * 100) / 100;
  const lastObservedMs = Date.parse(last.at);
  const freshness = {
    asOf: input.now,
    lastObservedAt: last.at,
    ageMs: Math.max(0, nowMs - lastObservedMs),
  };

  let projectedExhaustion: CapacityPaceEstimate["projectedExhaustion"] = {
    state: "UNKNOWN",
    reason: "no_known_limit",
    projectedAt: null,
    limitTokens: null,
    basis: "",
  };
  const limitKnown =
    input.knownLimitTokens != null &&
    input.knownLimitTokens > 0 &&
    input.limitObservedAt != null &&
    classifyCapacitySignalFreshness({
      observedAt: input.limitObservedAt,
      now: input.now,
      maxAgeMs: input.maxAgeMs ?? 7 * 24 * 60 * 60 * 1000,
    }).status === "fresh";
  if (limitKnown && tokensPerDay > 0) {
    const remainingTokens = input.knownLimitTokens! - last.cumulativeTokens;
    if (remainingTokens <= 0) {
      projectedExhaustion = {
        state: "PROJECTED",
        reason: "limit_already_consumed_by_last_observation",
        projectedAt: last.at,
        limitTokens: input.knownLimitTokens!,
        basis: "fresh known limit minus last observed cumulative tokens <= 0",
      };
    } else {
      const msToExhaustion = (remainingTokens / (tokensPerDay / 86_400_000));
      projectedExhaustion = {
        state: "PROJECTED",
        reason: null,
        projectedAt: new Date(lastObservedMs + msToExhaustion).toISOString(),
        limitTokens: input.knownLimitTokens!,
        basis: "linear pace applied to fresh known limit minus last observed cumulative tokens",
      };
    }
  }

  return {
    state: "ESTIMATED",
    reason: null,
    claimClass: "derived",
    certainty: "uncertain",
    method: "linear_interpolation_between_first_and_last_observation",
    tokensPerDay,
    observationWindow: { since: first.at, until: last.at },
    elapsedMs,
    observationCount: observations.length,
    freshness,
    limitations,
    projectedExhaustion,
  };
}

type CapacityPaceObservationsResult = CapacityPaceEstimate;

/**
 * Explicit-merge guard. Without an explicit approval object the caller gets
 * per-profile results only; there is no silent cross-profile aggregation.
 */
export function assertCapacityMergeApproval(approval: CapacityMergeApproval | undefined): void {
  if (!approval || approval.approved !== true) {
    throw new Error(
      "cross-profile merge requires explicit approval { approved: true, approvedBy, approvedAt }; profiles stay distinct otherwise",
    );
  }
  requireIdentity("approval.approvedBy", approval.approvedBy);
  requireIsoTimestamp("approval.approvedAt", approval.approvedAt);
}

/**
 * Compose the full owner-facing capacity plan. All functions stay strictly
 * per-profile; the plan itself records that profiles remained distinct.
 */
export function buildCapacityPlan(input: {
  profiles: CapacityProfile[];
  resets: CapacityResetEvent[];
  signalsByProfile: Record<string, CapacityUsageSignal[]>;
  costsByProfile?: Record<string, CapacityCostFacts>;
  paceObservationsByProfile?: Record<string, CapacityPaceObservation[]>;
  paceKnownLimitsByProfile?: Record<string, { limitTokens: number | null; observedAt: string | null }>;
  now: string;
  horizonDays?: number;
  maxAgeMs?: number;
  lowHeadroomFraction?: number;
  paceMinElapsedMs?: number;
  paceMinObservations?: number;
}): CapacityPlan {
  const maxAgeMs = input.maxAgeMs ?? 6 * 60 * 60 * 1000;
  const lowHeadroomFraction = input.lowHeadroomFraction ?? 0.2;
  if (!Number.isFinite(lowHeadroomFraction) || lowHeadroomFraction <= 0 || lowHeadroomFraction >= 1) {
    throw new Error("capacity lowHeadroomFraction must be between 0 and 1 exclusive");
  }
  const profileIds = new Set<string>();
  const providerById = new Map<string, string>();
  for (const profile of input.profiles) {
    requireIdentity("profileId", profile.profileId);
    requireIdentity("provider", profile.provider);
    if (profileIds.has(profile.profileId)) {
      throw new Error(`duplicate capacity profile ${profile.profileId}`);
    }
    profileIds.add(profile.profileId);
    providerById.set(profile.profileId, profile.provider);
  }
  const options = { now: input.now, maxAgeMs };
  const calendar = buildCapacityResetCalendar({
    events: input.resets.filter((event) => profileIds.has(event.profileId)),
    now: input.now,
    horizonDays: input.horizonDays,
  });

  const summaries: CapacityProfileSummary[] = [];
  const staleAlerts: CapacityStaleAlert[] = [];
  for (const profileId of profileIds) {
    const signals = input.signalsByProfile[profileId] ?? [];
    const { constraints, notifications } = summarizeConstraintsForProfile(signals, {
      ...options,
      lowHeadroomFraction,
    });
    summaries.push({
      profileId,
      provider: providerById.get(profileId)!,
      constraints,
      lowHeadroomNotifications: notifications,
    });
    staleAlerts.push(...staleAlertsForProfile(signals, options));
  }
  summaries.sort((left, right) => left.profileId.localeCompare(right.profileId));
  staleAlerts.sort((left, right) =>
    left.profileId.localeCompare(right.profileId) || left.dimension.localeCompare(right.dimension),
  );

  const paceEstimates: CapacityPlan["paceEstimates"] = [];
  for (const profileId of profileIds) {
    const observations = input.paceObservationsByProfile?.[profileId];
    if (!observations) continue;
    const known = input.paceKnownLimitsByProfile?.[profileId] ?? {
      limitTokens: null,
      observedAt: null,
    };
    paceEstimates.push({
      profileId,
      estimate: estimateCapacityLinearPace({
        observations,
        now: input.now,
        minElapsedMs: input.paceMinElapsedMs,
        minObservations: input.paceMinObservations,
        knownLimitTokens: known.limitTokens,
        limitObservedAt: known.observedAt,
        maxAgeMs,
      }),
    });
  }
  paceEstimates.sort((left, right) => left.profileId.localeCompare(right.profileId));

  // Cost facts are validated but deliberately NOT folded into constraints,
  // notifications, or pace: subscription dollars and API-equivalent value
  // remain separate lenses alongside local token telemetry.
  if (input.costsByProfile) {
    for (const [profileId, costs] of Object.entries(input.costsByProfile)) {
      if (!profileIds.has(profileId)) {
        throw new Error(`cost facts reference unknown profile ${profileId}`);
      }
      requireNonNegativeNumber(costs.subscriptionUsdPerMonth);
      requireNonNegativeNumber(costs.apiEquivalentUsd);
      requireIsoTimestamp("costs.observedAt", costs.observedAt);
    }
  }

  return {
    schema: CAPACITY_PLAN_SCHEMA,
    schemaVersion: CAPACITY_SCHEMA_VERSION,
    asOf: input.now,
    profilesDistinct: true,
    mergeApproval: "none",
    calendar,
    summaries,
    staleAlerts,
    paceEstimates,
    unknownLegend:
      "UNKNOWN means evidence is missing or stale; it is never a zero. Headroom, pace, and exhaustion projections exist only where fresh evidence supports them.",
  };
}
