import {
  derivePullOutcomeTimeline,
  type OutcomeTimelineCoverage,
  type PullTimelineFact,
  type RequiredCheckPolicy,
} from "../../shared/src/index";

/**
 * A compact deterministic read model over immutable outcome facts. Absence is
 * never converted into a negative result: consumers receive literal UNKNOWN.
 */
export const OUTCOME_PERFORMANCE_SCHEMA_VERSION = 1 as const;

export type OutcomePerformanceRecord = {
  schemaVersion: typeof OUTCOME_PERFORMANCE_SCHEMA_VERSION;
  repositoryExternalId: string;
  pullExternalId: string;
  pullNumber: number;
  /** A fact timestamp, preferring observed merge over pull creation. */
  occurredAt: string | null;
  createdAt: string | null;
  mergedAt: string | null;
  mergeOutcome: "MERGED" | "UNKNOWN";
  checkOutcome: "FIRST_PASS" | "FIRST_PASS_FAILED" | "UNKNOWN";
  reworkOutcome: "REWORK_OBSERVED" | "UNKNOWN";
  coverage: "COMPLETE" | "UNKNOWN";
  revisionCount: number;
  timeToGreenMs: number | null;
  retryEpisodes: number | "UNKNOWN";
  correctionLoops: number | "UNKNOWN";
  reviewCorrections: number | "UNKNOWN";
};

export type OutcomePerformanceSummary = {
  schema: "plimsoll.outcome-performance.v1";
  generation: number;
  days: number;
  since: string;
  asOf: string;
  totals: {
    pulls: number;
    undatedPulls: number;
    merged: number;
    mergeUnknown: number;
    firstPass: number;
    firstPassFailed: number;
    checksUnknown: number;
    reworkObserved: number;
    reworkUnknown: number;
    timeToGreenKnown: number;
    averageTimeToGreenMs: number | null;
    correctionLoops: number | "UNKNOWN";
    retryEpisodes: number | "UNKNOWN";
  };
  daily: Array<{
    day: string;
    pulls: number;
    merged: number;
    firstPass: number;
    firstPassFailed: number;
    checksUnknown: number;
    reworkObserved: number;
  }>;
};

function completeCoverage(
  coverage: OutcomeTimelineCoverage[],
  repositoryExternalId: string,
  pullExternalId: string,
  dimension: OutcomeTimelineCoverage["dimension"],
) {
  const rows = coverage.filter(
    (row) => row.repositoryExternalId === repositoryExternalId &&
      row.pullExternalId === pullExternalId && row.dimension === dimension,
  );
  return rows.length > 0 && rows.every((row) => row.status === "complete");
}

export function deriveOutcomePerformanceRecords(input: {
  facts: PullTimelineFact[];
  coverage?: OutcomeTimelineCoverage[];
  requiredChecks?: RequiredCheckPolicy;
  reworkWindowDays?: number;
}): OutcomePerformanceRecord[] {
  const coverage = input.coverage ?? [];
  const derived = derivePullOutcomeTimeline({
    facts: input.facts,
    coverage,
    requiredChecks: input.requiredChecks,
    reworkWindowDays: input.reworkWindowDays ?? 14,
  });
  return derived.map((row) => {
    const facts = input.facts.filter((fact) => fact.pullExternalId === row.pullExternalId);
    const repositoryExternalId = facts[0]!.repositoryExternalId;
    const pull = facts.find(
      (fact): fact is Extract<PullTimelineFact, { kind: "pull" }> => fact.kind === "pull",
    );
    const merge = facts
      .filter((fact): fact is Extract<PullTimelineFact, { kind: "merge" }> => fact.kind === "merge")
      .sort((left, right) => left.mergedAt.localeCompare(right.mergedAt))[0];
    const reviewsComplete = completeCoverage(coverage, repositoryExternalId, row.pullExternalId, "reviews");
    // `derivePullOutcomeTimeline` is also used with an in-memory complete
    // fixture, where omitted coverage is intentionally allowed. A persisted
    // performance row is stricter: historical check outcomes require both a
    // named policy and explicit complete provider coverage.
    const checksComplete = Boolean(input.requiredChecks?.names.length) &&
      ["pull", "revisions", "checks", "required_checks"].every((dimension) =>
        completeCoverage(
          coverage,
          repositoryExternalId,
          row.pullExternalId,
          dimension as OutcomeTimelineCoverage["dimension"],
        ),
      );
    return {
      schemaVersion: OUTCOME_PERFORMANCE_SCHEMA_VERSION,
      repositoryExternalId,
      pullExternalId: row.pullExternalId,
      pullNumber: row.pullNumber,
      occurredAt: merge?.mergedAt ?? pull?.createdAt ?? null,
      createdAt: pull?.createdAt ?? null,
      mergedAt: merge?.mergedAt ?? null,
      mergeOutcome: merge ? "MERGED" : "UNKNOWN",
      checkOutcome: checksComplete && row.firstPassSuccess === true
        ? "FIRST_PASS"
        : checksComplete && row.firstPassSuccess === false ? "FIRST_PASS_FAILED" : "UNKNOWN",
      reworkOutcome: row.rework.some((signal) => signal.inWindow) ? "REWORK_OBSERVED" : "UNKNOWN",
      coverage: checksComplete && row.coverage === "complete" ? "COMPLETE" : "UNKNOWN",
      revisionCount: row.revisionCount,
      timeToGreenMs: checksComplete ? row.timeToGreenMs : null,
      retryEpisodes: checksComplete && row.retryEpisodes !== null ? row.retryEpisodes.length : "UNKNOWN",
      correctionLoops: checksComplete && row.correctionLoops !== null ? row.correctionLoops.length : "UNKNOWN",
      reviewCorrections: reviewsComplete ? row.reviewCorrections.length : "UNKNOWN",
    };
  });
}

export function parseOutcomePerformanceRecord(value: unknown): OutcomePerformanceRecord {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid outcome performance record");
  const row = value as Record<string, unknown>;
  const string = (field: "repositoryExternalId" | "pullExternalId") => {
    if (typeof row[field] !== "string" || row[field].trim() === "") throw new Error(`invalid outcome performance ${field}`);
    return row[field];
  };
  const date = (field: "occurredAt" | "createdAt" | "mergedAt") => {
    const value = row[field];
    if (value !== null && (typeof value !== "string" || !Number.isFinite(Date.parse(value)))) throw new Error(`invalid outcome performance ${field}`);
    return value as string | null;
  };
  const choice = <T extends string>(field: string, values: readonly T[]) => {
    if (typeof row[field] !== "string" || !values.includes(row[field] as T)) throw new Error(`invalid outcome performance ${field}`);
    return row[field] as T;
  };
  const count = (field: string) => {
    if (row[field] === "UNKNOWN") return "UNKNOWN" as const;
    if (!Number.isInteger(row[field]) || Number(row[field]) < 0) throw new Error(`invalid outcome performance ${field}`);
    return Number(row[field]);
  };
  if (row.schemaVersion !== OUTCOME_PERFORMANCE_SCHEMA_VERSION || !Number.isInteger(row.pullNumber) || Number(row.pullNumber) <= 0 || !Number.isInteger(row.revisionCount) || Number(row.revisionCount) < 0) throw new Error("invalid outcome performance identity");
  if (row.timeToGreenMs !== null && (!Number.isFinite(row.timeToGreenMs) || Number(row.timeToGreenMs) < 0)) throw new Error("invalid outcome performance timeToGreenMs");
  return {
    schemaVersion: OUTCOME_PERFORMANCE_SCHEMA_VERSION,
    repositoryExternalId: string("repositoryExternalId"),
    pullExternalId: string("pullExternalId"),
    pullNumber: Number(row.pullNumber),
    occurredAt: date("occurredAt"),
    createdAt: date("createdAt"),
    mergedAt: date("mergedAt"),
    mergeOutcome: choice("mergeOutcome", ["MERGED", "UNKNOWN"] as const),
    checkOutcome: choice("checkOutcome", ["FIRST_PASS", "FIRST_PASS_FAILED", "UNKNOWN"] as const),
    reworkOutcome: choice("reworkOutcome", ["REWORK_OBSERVED", "UNKNOWN"] as const),
    coverage: choice("coverage", ["COMPLETE", "UNKNOWN"] as const),
    revisionCount: Number(row.revisionCount),
    timeToGreenMs: row.timeToGreenMs === null ? null : Number(row.timeToGreenMs),
    retryEpisodes: count("retryEpisodes"),
    correctionLoops: count("correctionLoops"),
    reviewCorrections: count("reviewCorrections"),
  };
}

function aggregateKnown(records: OutcomePerformanceRecord[], field: "correctionLoops" | "retryEpisodes") {
  if (records.some((record) => record[field] === "UNKNOWN")) return "UNKNOWN" as const;
  return records.reduce((total, record) => total + Number(record[field]), 0);
}

export function summarizeOutcomePerformance(input: {
  records: OutcomePerformanceRecord[];
  days: number;
  generation: number;
  asOf?: string;
}): OutcomePerformanceSummary {
  const asOf = input.asOf ?? new Date().toISOString();
  const asOfMs = Date.parse(asOf);
  if (!Number.isInteger(input.days) || input.days < 1 || !Number.isFinite(asOfMs)) throw new Error("outcome performance summary requires positive days and ISO asOf");
  const since = new Date(asOfMs - input.days * 24 * 60 * 60 * 1000).toISOString();
  const included = input.records.filter((record) => record.occurredAt !== null && record.occurredAt >= since && record.occurredAt <= asOf);
  const timeValues = included.map((record) => record.timeToGreenMs).filter((value): value is number => value !== null);
  const daily = new Map<string, OutcomePerformanceSummary["daily"][number]>();
  for (const record of included) {
    const day = record.occurredAt!.slice(0, 10);
    const bucket = daily.get(day) ?? { day, pulls: 0, merged: 0, firstPass: 0, firstPassFailed: 0, checksUnknown: 0, reworkObserved: 0 };
    bucket.pulls += 1;
    if (record.mergeOutcome === "MERGED") bucket.merged += 1;
    if (record.checkOutcome === "FIRST_PASS") bucket.firstPass += 1;
    if (record.checkOutcome === "FIRST_PASS_FAILED") bucket.firstPassFailed += 1;
    if (record.checkOutcome === "UNKNOWN") bucket.checksUnknown += 1;
    if (record.reworkOutcome === "REWORK_OBSERVED") bucket.reworkObserved += 1;
    daily.set(day, bucket);
  }
  return {
    schema: "plimsoll.outcome-performance.v1", generation: input.generation, days: input.days, since, asOf,
    totals: {
      pulls: included.length,
      undatedPulls: input.records.filter((record) => record.occurredAt === null).length,
      merged: included.filter((record) => record.mergeOutcome === "MERGED").length,
      mergeUnknown: included.filter((record) => record.mergeOutcome === "UNKNOWN").length,
      firstPass: included.filter((record) => record.checkOutcome === "FIRST_PASS").length,
      firstPassFailed: included.filter((record) => record.checkOutcome === "FIRST_PASS_FAILED").length,
      checksUnknown: included.filter((record) => record.checkOutcome === "UNKNOWN").length,
      reworkObserved: included.filter((record) => record.reworkOutcome === "REWORK_OBSERVED").length,
      reworkUnknown: included.filter((record) => record.reworkOutcome === "UNKNOWN").length,
      timeToGreenKnown: timeValues.length,
      averageTimeToGreenMs: timeValues.length ? Math.round(timeValues.reduce((total, value) => total + value, 0) / timeValues.length) : null,
      correctionLoops: aggregateKnown(included, "correctionLoops"),
      retryEpisodes: aggregateKnown(included, "retryEpisodes"),
    },
    daily: [...daily.values()].sort((left, right) => left.day.localeCompare(right.day)),
  };
}

function duration(value: number | null) {
  if (value === null) return "UNKNOWN";
  if (value < 60_000) return `${Math.round(value / 1_000)}s`;
  if (value < 3_600_000) return `${Math.round(value / 60_000)}m`;
  return `${Math.round(value / 3_600_000)}h`;
}

/** A small, local-only markdown companion to the JSON weekly rollup. */
export function formatWeeklyPerformanceMarkdown(summary: OutcomePerformanceSummary) {
  const totals = summary.totals;
  const lines = [
    "# Plimsoll weekly performance",
    "",
    `Window: ${summary.since} to ${summary.asOf}`,
    `Outcome materialization generation: ${summary.generation}`,
    "",
    "| Metric | Value |",
    "| --- | ---: |",
    `| Timeline pulls | ${totals.pulls} |`,
    `| Merged (observed) | ${totals.merged} |`,
    `| Merge UNKNOWN | ${totals.mergeUnknown} |`,
    `| First-pass checks | ${totals.firstPass} |`,
    `| First-pass failures | ${totals.firstPassFailed} |`,
    `| Check UNKNOWN | ${totals.checksUnknown} |`,
    `| Rework observed | ${totals.reworkObserved} |`,
    `| Average time to green | ${duration(totals.averageTimeToGreenMs)} |`,
    `| Correction loops | ${totals.correctionLoops} |`,
    `| Retry episodes | ${totals.retryEpisodes} |`,
    "",
    "`UNKNOWN` means the immutable local outcome timeline lacks the required evidence or policy coverage; it is never a zero or a negative outcome.",
    "",
  ];
  return lines.join("\n");
}
