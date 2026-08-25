/**
 * Matched-outcome capacity research guardrails (issue #174).
 *
 * Capacity may be STUDIED only as a supporting context variable under this
 * separate scientific protocol. This module is a pre-registration and
 * disclosure contract, NOT a second matched-outcome engine: it computes no
 * effects, joins no live data, and never replaces the bounded work-unit
 * evidence packets (learning-evidence) as the evidence source.
 *
 * Doctrine enforced here and by scripts/capacity-research-proof.ts:
 * - Work type AND time window must match before any comparison.
 * - Stable outcome evidence, quality, rework, complexity, declared
 *   confounders, and human review are required before inference.
 * - Identity, activity, time, cost, capacity, and outcomes stay separate
 *   fact streams joined only by declared keys; collapsing streams fails.
 * - Minimum sample and the missing-evidence rule are declared BEFORE any
 *   inference; the only allowed missing-evidence policy is UNKNOWN.
 * - Three-machine compatibility observations are excluded from statistical
 *   team conclusions, always.
 * - Insufficient evidence stays literal UNKNOWN — never zero, never a
 *   fabricated effect.
 * - Every research output states scope, source dates, confidence,
 *   limitations, and the decisions it can and cannot support. Capacity can
 *   never feed rankings, coaching scores, compensation, discipline,
 *   interventions, D3/D4 hosted analytics, or individual performance
 *   verdicts; those decisions are structurally unsupported.
 *
 * This module is deliberately capacity-named: everything it exports is
 * capacity doctrine, so the reachability gate treats every consumer of these
 * symbols exactly like a consumer of the planning module.
 */

export const CAPACITY_RESEARCH_SCHEMA_VERSION = 1 as const;
export const CAPACITY_RESEARCH_PROTOCOL_SCHEMA =
  "plimsoll.capacity-research-protocol.v1" as const;
export const CAPACITY_RESEARCH_FINDING_SCHEMA =
  "plimsoll.capacity-research-finding.v1" as const;

export const CAPACITY_RESEARCH_IDENTITY_MAX_LENGTH = 128 as const;
const CAPACITY_IDENTITY_REGEX =
  /^[A-Za-z0-9](?:[A-Za-z0-9._:@/-]{0,126}[A-Za-z0-9])?$/;

const BOUNDED_TEXT_MAX_LENGTH = 2000 as const;

/** The six fact streams are separate joins and are never collapsed. */
export const CAPACITY_RESEARCH_FACT_STREAMS = [
  "identity",
  "activity",
  "time",
  "cost",
  "capacity",
  "outcomes",
] as const;
export type CapacityResearchFactStream =
  (typeof CAPACITY_RESEARCH_FACT_STREAMS)[number];

/**
 * Evidence dimensions required before any matched comparison is inferred.
 * All six must be declared by the protocol; none may be silently skipped.
 */
export const CAPACITY_RESEARCH_REQUIRED_EVIDENCE = [
  "stable_outcome_evidence",
  "quality",
  "rework",
  "complexity",
  "declared_confounders",
  "human_review",
] as const;

/** The population event aligns with metric-registry cohort vocabulary. */
export const CAPACITY_RESEARCH_POPULATION_EVENTS = [
  "submitted",
  "merged",
] as const;
export type CapacityResearchPopulationEvent =
  (typeof CAPACITY_RESEARCH_POPULATION_EVENTS)[number];

/**
 * Why an observation exists. `machine_compatibility` rows exist to prove a
 * runtime works on hardware; they are never statistical team conclusions.
 */
export const CAPACITY_RESEARCH_OBSERVATION_PURPOSES = [
  "work_measurement",
  "machine_compatibility",
] as const;
export type CapacityResearchObservationPurpose =
  (typeof CAPACITY_RESEARCH_OBSERVATION_PURPOSES)[number];

/** Missing evidence stays UNKNOWN. There is no other policy. */
export type CapacityResearchMissingEvidencePolicy = "unknown";

/**
 * Decisions that capacity research output can NEVER support, regardless of
 * sample size or effect strength. Capacity is context, not judgment.
 */
export const CAPACITY_RESEARCH_FORBIDDEN_DECISIONS = [
  "rankings",
  "coaching_scores",
  "compensation",
  "discipline",
  "interventions",
  "d3_d4_hosted_analytics",
  "individual_performance_verdicts",
] as const;
export type CapacityResearchForbiddenDecision =
  (typeof CAPACITY_RESEARCH_FORBIDDEN_DECISIONS)[number];

/** The only role capacity may play in outcome research. */
export const CAPACITY_RESEARCH_CAPACITY_ROLE =
  "supporting_context_variable" as const;

/** Fixed limitations carried on every finding, estimable or not. */
export const CAPACITY_RESEARCH_LIMITATIONS: readonly string[] = [
  "Capacity is a supporting context variable only; it never independently proves productivity or performance.",
  "This protocol yields at most observational association; causality requires a separate pre-registered experiment.",
  "Three-machine compatibility observations are excluded from all statistical team conclusions.",
  "Missing or stale evidence remains UNKNOWN and was never treated as zero, absence of rework, or failure.",
  "Findings bind to the declared work type and time window; they do not transfer to other work types or windows.",
];

export type CapacityResearchProtocolInput = {
  studyId: string;
  /** Matching contract: comparisons are valid only inside this cell. */
  matching: {
    workType: string;
    window: { since: string; until: string };
    populationEvent: CapacityResearchPopulationEvent;
  };
  /** Must cover every entry of CAPACITY_RESEARCH_REQUIRED_EVIDENCE. */
  requiredEvidence: string[];
  /** Declared BEFORE inference; enforced fail-closed at evaluation time. */
  minimumSample: {
    minMatchedPairs: number;
    missingEvidencePolicy: CapacityResearchMissingEvidencePolicy;
  };
  /** One join key per fact stream; keys must differ across streams. */
  factStreamJoins: Array<{
    stream: CapacityResearchFactStream;
    joinKey: string;
  }>;
  /** Must explicitly exclude three-machine compatibility data. */
  exclusions: string[];
  /** Recorded human review approval; absent or unapproved review blocks. */
  humanReview: { approvedBy: string; approvedAt: string };
  /** Evaluation instant; approvals dated after it fail closed. */
  now: string;
};

export type CapacityResearchProtocol = {
  schema: typeof CAPACITY_RESEARCH_PROTOCOL_SCHEMA;
  schemaVersion: typeof CAPACITY_RESEARCH_SCHEMA_VERSION;
  studyId: string;
  matching: {
    workType: string;
    window: { since: string; until: string };
    populationEvent: CapacityResearchPopulationEvent;
  };
  requiredEvidence: readonly string[];
  minimumSample: {
    minMatchedPairs: number;
    missingEvidencePolicy: CapacityResearchMissingEvidencePolicy;
  };
  factStreamJoins: ReadonlyArray<{
    stream: CapacityResearchFactStream;
    joinKey: string;
  }>;
  exclusions: readonly string[];
  humanReview: { approvedBy: string; approvedAt: string };
  capacityRole: typeof CAPACITY_RESEARCH_CAPACITY_ROLE;
};

export type CapacityResearchSampleCounts = {
  /** Total observations considered for this study cell. */
  observations: number;
  /** Subset whose purpose is machine compatibility; always excluded below. */
  machineCompatibilityObservations: number;
  /** Outcome-matched pairs formed inside the declared matching cell. */
  matchedPairs: number;
  /** Matched pairs missing required evidence; visible, never dropped silently. */
  pairsMissingRequiredEvidence: number;
};

export type CapacityResearchReadiness =
  | {
      state: "ESTIMABLE";
      usablePairs: number;
      excludedMachineCompatibilityObservations: number;
      pairsUnknownEvidence: number;
    }
  | {
      state: "UNKNOWN";
      reason: string;
      usablePairs: number;
      excludedMachineCompatibilityObservations: number;
      pairsUnknownEvidence: number;
    };

export type CapacityResearchFindingInput = {
  claimClass: "observed" | "suggestive";
  confidence: "low" | "moderate";
  confidenceBasis: string;
  supportedDecisions: string[];
  asOf: string;
};

export type CapacityResearchFinding = {
  schema: typeof CAPACITY_RESEARCH_FINDING_SCHEMA;
  schemaVersion: typeof CAPACITY_RESEARCH_SCHEMA_VERSION;
  studyId: string;
  status: "estimated" | "UNKNOWN";
  reason: string | null;
  claimClass: "observed" | "suggestive" | null;
  confidence: { level: "low" | "moderate"; basis: string } | null;
  capacityRole: typeof CAPACITY_RESEARCH_CAPACITY_ROLE;
  scope: {
    workType: string;
    window: { since: string; until: string };
    populationEvent: CapacityResearchPopulationEvent;
  };
  sourceDates: {
    windowSince: string;
    windowUntil: string;
    generatedAsOf: string;
  };
  sample: {
    observations: number;
    usablePairs: number;
    excludedMachineCompatibilityObservations: number;
    pairsUnknownEvidence: number;
  } | null;
  limitations: readonly string[];
  supportedDecisions: readonly string[];
  unsupportedDecisions: readonly string[];
};

function requireIdentity(field: string, value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > CAPACITY_RESEARCH_IDENTITY_MAX_LENGTH ||
    !CAPACITY_IDENTITY_REGEX.test(value)
  ) {
    throw new Error(`invalid capacity research ${field}`);
  }
  return value;
}

function requireBoundedText(field: string, value: unknown): string {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > BOUNDED_TEXT_MAX_LENGTH
  ) {
    throw new Error(`invalid capacity research ${field}`);
  }
  return value;
}

function requireIsoTimestamp(field: string, value: unknown): string {
  if (typeof value !== "string" || !Number.isFinite(Date.parse(value))) {
    throw new Error(`invalid capacity research ${field}`);
  }
  return value;
}

function requireNonNegativeInteger(value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error("invalid capacity research non-negative integer");
  }
  return value;
}

/**
 * Pre-register a matched-outcome capacity research protocol. Fails closed
 * unless work type and window are matched, all six evidence dimensions are
 * required, minimum sample and the UNKNOWN missing-evidence rule are fixed,
 * the six fact streams keep separate join keys, three-machine compatibility
 * data is explicitly excluded, and human review approval is recorded.
 */
export function validateCapacityResearchProtocol(
  input: CapacityResearchProtocolInput,
): CapacityResearchProtocol {
  const studyId = requireIdentity("studyId", input.studyId);

  if (!input.matching || typeof input.matching !== "object") {
    throw new Error("capacity research requires an explicit matching contract");
  }
  const workType = requireBoundedText("matching.workType", input.matching.workType);
  const since = requireIsoTimestamp("matching.window.since", input.matching.window?.since);
  const until = requireIsoTimestamp("matching.window.until", input.matching.window?.until);
  if (Date.parse(until) <= Date.parse(since)) {
    throw new Error("capacity research window.until must be after window.since");
  }
  if (
    !input.matching.populationEvent ||
    !CAPACITY_RESEARCH_POPULATION_EVENTS.includes(input.matching.populationEvent)
  ) {
    throw new Error(
      `invalid capacity research population event ${String(input.matching?.populationEvent)}`,
    );
  }

  const declared = new Set(input.requiredEvidence ?? []);
  const missingEvidenceDimensions = CAPACITY_RESEARCH_REQUIRED_EVIDENCE.filter(
    (dimension) => !declared.has(dimension),
  );
  if (missingEvidenceDimensions.length > 0) {
    throw new Error(
      `capacity research protocol is missing required evidence dimensions: ${missingEvidenceDimensions.join(", ")}`,
    );
  }

  const minMatchedPairs = requireNonNegativeInteger(
    input.minimumSample?.minMatchedPairs,
  );
  if (minMatchedPairs < 1) {
    throw new Error(
      "capacity research minMatchedPairs must be >= 1 and declared before inference",
    );
  }
  if (input.minimumSample?.missingEvidencePolicy !== "unknown") {
    throw new Error(
      'capacity research missingEvidencePolicy must be "unknown"; dropping, imputing, or zeroing missing evidence is forbidden',
    );
  }

  // Fact-stream separation: all six streams present, each with its own key,
  // and no key shared between two streams (a shared key is a collapse).
  const joins = input.factStreamJoins ?? [];
  const streamsSeen = new Set<string>();
  const keysByStream = new Map<string, string>();
  for (const join of joins) {
    if (!join || !CAPACITY_RESEARCH_FACT_STREAMS.includes(join.stream)) {
      throw new Error(`invalid capacity research fact stream ${String(join?.stream)}`);
    }
    if (streamsSeen.has(join.stream)) {
      throw new Error(`duplicate capacity research fact stream ${join.stream}`);
    }
    streamsSeen.add(join.stream);
    const joinKey = requireIdentity(`factStreamJoins.${join.stream}.joinKey`, join.joinKey);
    keysByStream.set(join.stream, joinKey);
  }
  for (const stream of CAPACITY_RESEARCH_FACT_STREAMS) {
    if (!streamsSeen.has(stream)) {
      throw new Error(
        `capacity research requires a separate join for fact stream ${stream}`,
      );
    }
  }
  const seenKeys = new Set<string>();
  for (const [stream, joinKey] of keysByStream) {
    if (seenKeys.has(joinKey)) {
      throw new Error(
        `capacity research fact streams ${stream} and another stream share join key ${joinKey}; streams must stay separate joins`,
      );
    }
    seenKeys.add(joinKey);
  }

  const exclusions = [...new Set(input.exclusions ?? [])];
  if (!exclusions.includes("three_machine_compatibility")) {
    throw new Error(
      "capacity research must explicitly exclude three_machine_compatibility data from statistical team conclusions",
    );
  }

  const humanReview = input.humanReview;
  if (!humanReview || typeof humanReview !== "object") {
    throw new Error("capacity research requires recorded human review");
  }
  const approvedBy = requireIdentity("humanReview.approvedBy", humanReview.approvedBy);
  const approvedAt = requireIsoTimestamp("humanReview.approvedAt", humanReview.approvedAt);
  const nowMs = Date.parse(requireIsoTimestamp("now", input.now));
  if (Date.parse(approvedAt) > nowMs) {
    throw new Error("capacity research human review approval cannot be future-dated");
  }

  return {
    schema: CAPACITY_RESEARCH_PROTOCOL_SCHEMA,
    schemaVersion: CAPACITY_RESEARCH_SCHEMA_VERSION,
    studyId,
    matching: {
      workType,
      window: { since, until },
      populationEvent: input.matching.populationEvent,
    },
    requiredEvidence: [...CAPACITY_RESEARCH_REQUIRED_EVIDENCE],
    minimumSample: { minMatchedPairs, missingEvidencePolicy: "unknown" },
    factStreamJoins: CAPACITY_RESEARCH_FACT_STREAMS.map((stream) => ({
      stream,
      joinKey: keysByStream.get(stream)!,
    })),
    exclusions: ["three_machine_compatibility", ...exclusions.filter((e) => e !== "three_machine_compatibility")],
    humanReview: { approvedBy, approvedAt },
    capacityRole: CAPACITY_RESEARCH_CAPACITY_ROLE,
  };
}

/**
 * Gate readiness BEFORE any inference. Machine-compatibility observations
 * are subtracted from consideration entirely; pairs with unknown required
 * evidence stay counted but unusable; a usable-pair count below the
 * protocol's declared minimum leaves the study UNKNOWN — never a small
 * effect estimate, never a zero.
 */
export function evaluateCapacityResearchReadiness(
  protocol: CapacityResearchProtocol,
  counts: CapacityResearchSampleCounts,
): CapacityResearchReadiness {
  if (protocol.schema !== CAPACITY_RESEARCH_PROTOCOL_SCHEMA) {
    throw new Error("unrecognized capacity research protocol schema");
  }
  const observations = requireNonNegativeInteger(counts.observations);
  const machineCompatibilityObservations = requireNonNegativeInteger(
    counts.machineCompatibilityObservations,
  );
  const matchedPairs = requireNonNegativeInteger(counts.matchedPairs);
  const pairsMissingRequiredEvidence = requireNonNegativeInteger(
    counts.pairsMissingRequiredEvidence,
  );
  if (machineCompatibilityObservations > observations) {
    throw new Error(
      "machine-compatibility observations cannot exceed total observations",
    );
  }
  if (pairsMissingRequiredEvidence > matchedPairs) {
    throw new Error(
      "pairs missing required evidence cannot exceed matched pairs",
    );
  }

  const pairsUnknownEvidence = pairsMissingRequiredEvidence;
  const usablePairs = Math.max(0, matchedPairs - pairsUnknownEvidence);
  const base = {
    usablePairs,
    excludedMachineCompatibilityObservations: machineCompatibilityObservations,
    pairsUnknownEvidence,
  };

  if (matchedPairs === 0) {
    return {
      state: "UNKNOWN",
      reason:
        observations > 0 && machineCompatibilityObservations === observations
          ? "only_machine_compatibility_observations: excluded from statistical team conclusions"
          : "no_matched_pairs_within_declared_matching_cell",
      ...base,
    };
  }
  if (usablePairs < protocol.minimumSample.minMatchedPairs) {
    return {
      state: "UNKNOWN",
      reason: `below_declared_minimum_sample: ${usablePairs}/${protocol.minimumSample.minMatchedPairs} usable pairs`,
      ...base,
    };
  }
  return { state: "ESTIMABLE", ...base };
}

function assertNoForbiddenDecisions(supported: string[]): void {
  const forbiddenHit = supported.find((decision) =>
    (CAPACITY_RESEARCH_FORBIDDEN_DECISIONS as readonly string[]).includes(decision),
  );
  if (forbiddenHit !== undefined) {
    throw new Error(
      `capacity research output can never support "${forbiddenHit}"; capacity does not feed rankings, coaching scores, compensation, discipline, interventions, D3/D4 hosted analytics, or individual performance verdicts`,
    );
  }
}

/**
 * Compose the disclosure-complete research finding. Insufficient evidence
 * returns a status-"UNKNOWN" finding that still carries scope, source dates,
 * limitations, and the full unsupported-decision list. Estimable findings
 * cap at observational association ("observed"/"suggestive"); causal claims
 * and forbidden decisions fail closed.
 */
export function buildCapacityResearchFinding(
  protocol: CapacityResearchProtocol,
  counts: CapacityResearchSampleCounts,
  input: CapacityResearchFindingInput,
): CapacityResearchFinding {
  const readiness = evaluateCapacityResearchReadiness(protocol, counts);
  const generatedAsOf = requireIsoTimestamp("asOf", input.asOf);
  const unsupportedDecisions: readonly string[] = [
    ...CAPACITY_RESEARCH_FORBIDDEN_DECISIONS,
    "team_statistical_conclusions_from_compatibility_data",
  ];

  const header = {
    schema: CAPACITY_RESEARCH_FINDING_SCHEMA,
    schemaVersion: CAPACITY_RESEARCH_SCHEMA_VERSION,
    studyId: protocol.studyId,
    capacityRole: CAPACITY_RESEARCH_CAPACITY_ROLE,
    scope: {
      workType: protocol.matching.workType,
      window: protocol.matching.window,
      populationEvent: protocol.matching.populationEvent,
    },
    sourceDates: {
      windowSince: protocol.matching.window.since,
      windowUntil: protocol.matching.window.until,
      generatedAsOf,
    },
    limitations: CAPACITY_RESEARCH_LIMITATIONS,
    supportedDecisions: [] as readonly string[],
    unsupportedDecisions,
  };

  if (readiness.state === "UNKNOWN") {
    return {
      ...header,
      status: "UNKNOWN",
      reason: readiness.reason,
      claimClass: null,
      confidence: null,
      sample: null,
      supportedDecisions: [],
      unsupportedDecisions,
    };
  }

  if (input.claimClass !== "observed" && input.claimClass !== "suggestive") {
    throw new Error(
      `invalid capacity research claim class ${String(input.claimClass)}; only observed or suggestive (observational association) is permitted — causality requires a separate pre-registered experiment`,
    );
  }
  if (input.confidence !== "low" && input.confidence !== "moderate") {
    throw new Error("capacity research confidence level must be low or moderate");
  }
  assertNoForbiddenDecisions(input.supportedDecisions ?? []);

  return {
    ...header,
    status: "estimated",
    reason: null,
    claimClass: input.claimClass,
    confidence: {
      level: input.confidence,
      basis: requireBoundedText("confidenceBasis", input.confidenceBasis),
    },
    sample: {
      observations: requireNonNegativeInteger(counts.observations),
      usablePairs: readiness.usablePairs,
      excludedMachineCompatibilityObservations:
        readiness.excludedMachineCompatibilityObservations,
      pairsUnknownEvidence: readiness.pairsUnknownEvidence,
    },
    supportedDecisions: [...new Set(input.supportedDecisions)].sort(),
    unsupportedDecisions,
  };
}
