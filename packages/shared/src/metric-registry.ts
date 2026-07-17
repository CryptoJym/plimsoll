/**
 * Versioned learning-metric truth contract (issue #97).
 *
 * This module is deliberately pure. It accepts an explicit, bounded analysis
 * manifest and returns deterministic descriptive results. It does not fetch,
 * recommend, score techniques, or silently substitute zero for missing data.
 */

export const ANALYSIS_MANIFEST_VERSION = "1.0.0" as const;
export const METRIC_DEFINITION_VERSION = "1.0.0" as const;

export const EVIDENCE_STATES = [
  "verified",
  "partial",
  "inferred",
  "blocked",
  "excluded",
] as const;
export type EvidenceState = (typeof EVIDENCE_STATES)[number];

export const CLAIM_CLASSES = [
  "observed",
  "suggestive",
  "associated",
  "causal",
  "not_estimable",
] as const;
export type ClaimClass = (typeof CLAIM_CLASSES)[number];

export const ATTRIBUTION_METHODS = [
  "direct",
  "deterministic_linkage",
  "declared_exposure",
  "inferred",
  "none",
] as const;
export type AttributionMethod = (typeof ATTRIBUTION_METHODS)[number];

export const CHECK_STATES = ["passed", "failed", "none", "unknown"] as const;
export type CheckState = (typeof CHECK_STATES)[number];

export const COST_KINDS = ["reported", "estimated", "missing"] as const;
export type CostKind = (typeof COST_KINDS)[number];

export const PROJECT_ALLOCATION_KINDS = ["exact", "apportioned", "unallocated", "unknown"] as const;
export type ProjectAllocationKind = (typeof PROJECT_ALLOCATION_KINDS)[number];

export const METRIC_LIFECYCLE_STATUSES = ["experimental", "active", "deprecated"] as const;
export type MetricLifecycleStatus = (typeof METRIC_LIFECYCLE_STATUSES)[number];

export const METRIC_VALIDATION_STATUSES = [
  "unvalidated",
  "adversarial_fixture_validated",
  "externally_validated",
  "blocked",
] as const;
export type MetricValidationStatus = (typeof METRIC_VALIDATION_STATUSES)[number];

export type MetricPopulationEvent = "submitted" | "merged";

export const METRIC_IDS = [
  "project_allocation_coverage",
  "first_pass_yield",
  "correction_loop",
  "time_tokens_to_first_green",
  "mature_stable_delivery",
  "post_merge_rework",
  "known_cost_coverage",
  "technique_exposure",
] as const;
export type MetricId = (typeof METRIC_IDS)[number];

export type AnalysisWindow = {
  startInclusive: string;
  endExclusive: string;
};

export type CheckAttempt = {
  attemptId: string;
  revisionId: string;
  sequence: number;
  supersedesAttemptId: string | null;
  at: string;
  state: CheckState;
};

export type ProjectAllocation = {
  kind: ProjectAllocationKind;
  shares: readonly {
    projectId: string;
    fraction: number;
  }[];
};

export type TokenDimensions = {
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheWriteTokens: number | null;
};

export type LearningDelivery = {
  id: string;
  submittedAt: string;
  mergedAt: string | null;
  deliveryAttribution: {
    method: AttributionMethod;
    evidenceState: EvidenceState;
  };
  project: {
    /** Legacy singular label retained for exact allocations only. */
    id: string | null;
    allocation: ProjectAllocation;
    attributionMethod: AttributionMethod;
    evidenceState: EvidenceState;
  };
  checks: {
    attempts: readonly CheckAttempt[];
    evidenceState: EvidenceState;
  };
  rework: {
    events: readonly { at: string; kind: string }[];
    evidenceState: EvidenceState;
  };
  tokensToFirstGreen: TokenDimensions & {
    evidenceState: EvidenceState;
  };
  cost: {
    usd: number | null;
    kind: CostKind;
    evidenceState: EvidenceState;
  };
  techniques: {
    ids: readonly string[] | null;
    attributionMethod: AttributionMethod;
    evidenceState: EvidenceState;
  };
};

export type MetricAnalysisManifest = {
  schemaVersion: typeof ANALYSIS_MANIFEST_VERSION;
  definitionVersion: typeof METRIC_DEFINITION_VERSION;
  analysisId: string;
  metricIds: readonly MetricId[];
  window: AnalysisWindow;
  asOf: string;
  stabilityHorizonDays: number;
  deliveries: readonly LearningDelivery[];
};

export const METRIC_UNITS = [
  "delivery",
  "ratio",
  "millisecond",
  "millisecond_per_delivery",
  "input_token",
  "input_token_per_delivery",
  "output_token",
  "output_token_per_delivery",
  "cache_read_token",
  "cache_read_token_per_delivery",
  "cache_write_token",
  "cache_write_token_per_delivery",
  "usd",
  "technique_exposure",
] as const;
export type MetricUnit = (typeof METRIC_UNITS)[number];

export const QUANTITY_KNOWLEDGE_STATES = ["known", "floor", "unknown"] as const;
export type QuantityKnowledge = (typeof QUANTITY_KNOWLEDGE_STATES)[number];

export type MetricQuantity = {
  value: number | null;
  unit: MetricUnit;
  knowledge: QuantityKnowledge;
};

export type NamedMetricQuantity = MetricQuantity & {
  key: string;
};

export type MetricMeasure = {
  key: string;
  numerator: MetricQuantity;
  denominator: MetricQuantity;
  value: MetricQuantity;
};

export type MetricCoverage = {
  verifiedCount: number;
  inferredCount: number;
  partialCount: number;
  blockedCount: number;
  excludedCount: number;
  eligibleCount: number;
  /** Verified source coverage only; inferred evidence is named separately. */
  ratio: number | null;
};

export type MetricSample = {
  eligibleCount: number;
  sampleCount: number;
  excludedCount: number;
  censoredCount: number;
  unknownCount: number;
  exclusionReasons: Readonly<Record<string, number>>;
};

export type MetricMaturity = {
  horizonDays: number | null;
  matureCount: number;
  censoredCount: number;
};

/**
 * Closed set of runtime configuration that changes a metric formula without
 * changing the registry definition itself. Null means the parameter is not
 * part of that metric's formula.
 */
export type MetricFormulaConfig = {
  stabilityHorizonDays: number | null;
};

export type MetricAttribution = {
  methods: readonly AttributionMethod[];
  attributedCount: number;
  eligibleCount: number;
  ratio: number | null;
  allocationMix: {
    exactCount: number;
    apportionedCount: number;
    unallocatedCount: number;
    unknownCount: number;
  };
};

export type MetricBreakdownRow = {
  key: string;
  count: number;
};

export type MetricResult = {
  metricId: MetricId;
  definitionVersion: typeof METRIC_DEFINITION_VERSION;
  lifecycleStatus: MetricLifecycleStatus;
  validationStatus: MetricValidationStatus;
  populationEvent: MetricPopulationEvent;
  formulaConfig: MetricFormulaConfig;
  window: AnalysisWindow;
  asOf: string;
  numerator: readonly NamedMetricQuantity[];
  denominator: readonly NamedMetricQuantity[];
  measures: readonly MetricMeasure[];
  totals: readonly NamedMetricQuantity[];
  sample: MetricSample;
  coverage: MetricCoverage;
  maturity: MetricMaturity;
  attribution: MetricAttribution;
  evidenceState: EvidenceState;
  claimClass: ClaimClass;
  breakdown: readonly MetricBreakdownRow[];
  limitations: readonly string[];
};

export type MetricDefinition = {
  id: MetricId;
  definitionVersion: typeof METRIC_DEFINITION_VERSION;
  label: string;
  lifecycleStatus: MetricLifecycleStatus;
  validationStatus: MetricValidationStatus;
  populationEvent: MetricPopulationEvent;
  eligibility: string;
  numerator: string;
  denominator: string;
  window: string;
  maturity: string;
  attribution: string;
  defaultClaimClass: "observed";
};

export const METRIC_REGISTRY: Readonly<Record<MetricId, MetricDefinition>> = Object.freeze({
  project_allocation_coverage: Object.freeze({
    id: "project_allocation_coverage",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Project allocation coverage",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "submitted",
    eligibility: "Non-excluded deliveries submitted in the analysis window by as-of time.",
    numerator: "Eligible deliveries with an exact or full-sum apportioned project allocation and named attribution method.",
    denominator: "All eligible deliveries; explicit unallocated and unknown allocation remain in the denominator.",
    window: "submittedAt in [startInclusive, min(endExclusive, asOf)].",
    maturity: "Not applicable.",
    attribution: "Direct, deterministic-linkage, or explicitly labeled inferred allocation.",
    defaultClaimClass: "observed",
  }),
  first_pass_yield: Object.freeze({
    id: "first_pass_yield",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "First-pass yield",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "submitted",
    eligibility: "Non-excluded deliveries submitted in the analysis window by as-of time.",
    numerator: "Eligible deliveries whose first lineage-ordered required-check observation is explicitly passed with verified, unambiguous check evidence.",
    denominator: "All eligible deliveries; none, unknown, ambiguous, partial, blocked, and missing checks stay in the denominator and never pass.",
    window: "Only check observations at or before asOf are visible.",
    maturity: "Not applicable.",
    attribution: "Delivery attribution is reported separately and never manufactured from a check result.",
    defaultClaimClass: "observed",
  }),
  correction_loop: Object.freeze({
    id: "correction_loop",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Correction-loop closure",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "submitted",
    eligibility: "Deliveries with a verified first required-check failure in the analysis cohort.",
    numerator: "Eligible failures whose final explicit pass is lineage-linked and carries a different revision by asOf.",
    denominator: "All verified first-attempt failures, including failures not yet corrected.",
    window: "No attempt after asOf is visible; later green results cannot leak backward.",
    maturity: "Unresolved loops are visible as not closed, not as exclusions.",
    attribution: "Delivery attribution is reported independently of correction outcome.",
    defaultClaimClass: "observed",
  }),
  time_tokens_to_first_green: Object.freeze({
    id: "time_tokens_to_first_green",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Time and tokens to first green",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "submitted",
    eligibility: "Non-excluded deliveries submitted in the analysis window by as-of time.",
    numerator: "Separate sums for elapsed milliseconds, input, output, cache-read, and cache-write tokens among observations with a verified first green and known dimension.",
    denominator: "A separate known-observation count for every dimension; unresolved or missing dimensions are never zero-filled.",
    window: "First green must occur by asOf. Future green attempts are ignored.",
    maturity: "Unresolved deliveries are explicitly unknown, not successful samples.",
    attribution: "Delivery attribution is reported; token dimensions are never blended or currency-weighted.",
    defaultClaimClass: "observed",
  }),
  mature_stable_delivery: Object.freeze({
    id: "mature_stable_delivery",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Mature stable delivery",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "merged",
    eligibility: "Merged cohort deliveries whose mergedAt is in the analysis window by as-of time.",
    numerator: "Mature deliveries with a verified, unambiguous final passed check, complete rework evidence, and no rework inside the configured horizon.",
    denominator: "Only deliveries whose full stability horizon has elapsed by asOf.",
    window: "Rework is inspected only in [mergedAt, mergedAt + horizon] and never after asOf.",
    maturity: "Right-censored deliveries stay in eligibleCount but are excluded from numerator and denominator until mature.",
    attribution: "Delivery attribution is explicit; stable outcome alone does not establish causality.",
    defaultClaimClass: "observed",
  }),
  post_merge_rework: Object.freeze({
    id: "post_merge_rework",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Post-merge rework",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "merged",
    eligibility: "Merged cohort deliveries whose full configured rework horizon has elapsed.",
    numerator: "Mature deliveries with an observed rework event inside the configured horizon.",
    denominator: "All mature deliveries; incomplete rework evidence makes the observed rate a floor.",
    window: "Events after the horizon or after asOf are excluded.",
    maturity: "Right-censored deliveries are not in the rate denominator.",
    attribution: "Delivery linkage is reported; a rework association is not a technique effect.",
    defaultClaimClass: "observed",
  }),
  known_cost_coverage: Object.freeze({
    id: "known_cost_coverage",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Known-cost coverage",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "submitted",
    eligibility: "Non-excluded deliveries submitted in the analysis window by as-of time.",
    numerator: "Eligible deliveries with a reported or explicitly estimated non-negative USD cost.",
    denominator: "All eligible deliveries; missing cost remains unknown.",
    window: "Cost observations belong to the bounded delivery cohort.",
    maturity: "Not applicable.",
    attribution: "Reported and estimated cost remain distinguishable in the manifest evidence state.",
    defaultClaimClass: "observed",
  }),
  technique_exposure: Object.freeze({
    id: "technique_exposure",
    definitionVersion: METRIC_DEFINITION_VERSION,
    label: "Technique exposure",
    lifecycleStatus: "experimental",
    validationStatus: "adversarial_fixture_validated",
    populationEvent: "submitted",
    eligibility: "Non-excluded deliveries submitted in the analysis window by as-of time.",
    numerator: "Eligible deliveries with one or more explicitly attributed technique ids.",
    denominator: "All eligible deliveries; unknown exposure stays in the denominator.",
    window: "Exposure is bounded to the delivery cohort and as-of time.",
    maturity: "Not applicable; this is exposure, not outcome effectiveness.",
    attribution: "Declared exposure, deterministic linkage, or labeled inference; per-technique counts remain descriptive.",
    defaultClaimClass: "observed",
  }),
});

const DAY_MS = 24 * 60 * 60 * 1_000;
const TIMEZONE_SUFFIX = /(?:Z|[+-]\d{2}:\d{2})$/;

function parseInstant(value: string, field: string): number {
  if (!TIMEZONE_SUFFIX.test(value)) throw new Error(`${field} must include an explicit timezone`);
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) throw new Error(`${field} must be a valid timestamp`);
  return parsed;
}

function assertNonnegative(value: number | null, field: string): void {
  if (value !== null && (!Number.isFinite(value) || value < 0)) {
    throw new Error(`${field} must be a non-negative finite number or null`);
  }
}

function assertOneOf<T extends string>(value: unknown, allowed: readonly T[], field: string): asserts value is T {
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    throw new Error(`${field} has unsupported value: ${String(value)}`);
  }
}

function assertEvidenceState(value: unknown, field: string): asserts value is EvidenceState {
  assertOneOf(value, EVIDENCE_STATES, field);
}

function assertAttributionMethod(value: unknown, field: string): asserts value is AttributionMethod {
  assertOneOf(value, ATTRIBUTION_METHODS, field);
}

/** Reject identity aliases instead of silently grouping or comparing them. */
function assertCanonicalIdentity(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string") throw new Error(`${field} must be a string identity`);
  const canonical = value.trim().normalize("NFC");
  if (!canonical) throw new Error(`${field} must not be empty`);
  if (value !== canonical) throw new Error(`${field} must use canonical identity form`);
}

export function validateMetricAnalysisManifest(manifest: MetricAnalysisManifest): void {
  if (manifest.schemaVersion !== ANALYSIS_MANIFEST_VERSION) {
    throw new Error(`Unsupported analysis manifest version: ${manifest.schemaVersion}`);
  }
  if (manifest.definitionVersion !== METRIC_DEFINITION_VERSION) {
    throw new Error(`Unsupported metric definition version: ${manifest.definitionVersion}`);
  }
  assertCanonicalIdentity(manifest.analysisId, "analysisId");
  if (manifest.metricIds.length === 0) throw new Error("metricIds must not be empty");
  if (new Set(manifest.metricIds).size !== manifest.metricIds.length) {
    throw new Error("metricIds must not contain duplicates");
  }
  for (const metricId of manifest.metricIds) {
    if (!METRIC_IDS.includes(metricId)) throw new Error(`Unknown metric id: ${metricId}`);
  }
  const startMs = parseInstant(manifest.window.startInclusive, "window.startInclusive");
  const endMs = parseInstant(manifest.window.endExclusive, "window.endExclusive");
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  if (startMs >= endMs) throw new Error("analysis window must have positive duration");
  if (asOfMs < startMs) throw new Error("asOf must not precede the analysis window");
  if (!Number.isInteger(manifest.stabilityHorizonDays) || manifest.stabilityHorizonDays <= 0) {
    throw new Error("stabilityHorizonDays must be a positive integer");
  }

  const ids = new Set<string>();
  for (const delivery of manifest.deliveries) {
    assertCanonicalIdentity(delivery.id, "delivery.id");
    if (ids.has(delivery.id)) throw new Error(`duplicate delivery id: ${delivery.id}`);
    ids.add(delivery.id);
    assertEvidenceState(delivery.deliveryAttribution.evidenceState, `${delivery.id}.deliveryAttribution.evidenceState`);
    assertAttributionMethod(delivery.deliveryAttribution.method, `${delivery.id}.deliveryAttribution.method`);
    assertEvidenceState(delivery.project.evidenceState, `${delivery.id}.project.evidenceState`);
    assertAttributionMethod(delivery.project.attributionMethod, `${delivery.id}.project.attributionMethod`);
    assertOneOf(delivery.project.allocation.kind, PROJECT_ALLOCATION_KINDS, `${delivery.id}.project.allocation.kind`);
    assertEvidenceState(delivery.checks.evidenceState, `${delivery.id}.checks.evidenceState`);
    assertEvidenceState(delivery.rework.evidenceState, `${delivery.id}.rework.evidenceState`);
    assertEvidenceState(delivery.tokensToFirstGreen.evidenceState, `${delivery.id}.tokensToFirstGreen.evidenceState`);
    assertEvidenceState(delivery.cost.evidenceState, `${delivery.id}.cost.evidenceState`);
    assertOneOf(delivery.cost.kind, COST_KINDS, `${delivery.id}.cost.kind`);
    assertEvidenceState(delivery.techniques.evidenceState, `${delivery.id}.techniques.evidenceState`);
    assertAttributionMethod(delivery.techniques.attributionMethod, `${delivery.id}.techniques.attributionMethod`);

    const allocationShares = delivery.project.allocation.shares;
    const allocationIds = new Set<string>();
    if (delivery.project.id !== null) {
      assertCanonicalIdentity(delivery.project.id, `${delivery.id}.project.id`);
    }
    for (const [index, share] of allocationShares.entries()) {
      assertCanonicalIdentity(
        share.projectId,
        `${delivery.id}.project.allocation.shares[${index}].projectId`,
      );
      if (allocationIds.has(share.projectId)) throw new Error(`${delivery.id} project allocation ids must be unique`);
      allocationIds.add(share.projectId);
      if (!Number.isFinite(share.fraction) || share.fraction <= 0 || share.fraction > 1) {
        throw new Error(`${delivery.id}.project.allocation.shares[${index}].fraction must be in (0, 1]`);
      }
    }
    const allocatedFraction = allocationShares.reduce((sum, share) => sum + share.fraction, 0);
    if (delivery.project.allocation.kind === "exact") {
      if (
        allocationShares.length !== 1 ||
        Math.abs(allocatedFraction - 1) > 1e-12 ||
        !delivery.project.id?.trim() ||
        delivery.project.id !== allocationShares[0]?.projectId ||
        delivery.project.attributionMethod === "none"
      ) {
        throw new Error(`${delivery.id} exact allocation requires one full share, matching project id, and attribution`);
      }
    } else if (delivery.project.allocation.kind === "apportioned") {
      if (
        allocationShares.length < 2 ||
        Math.abs(allocatedFraction - 1) > 1e-12 ||
        delivery.project.id !== null ||
        delivery.project.attributionMethod === "none"
      ) {
        throw new Error(`${delivery.id} apportioned allocation requires two or more full-sum shares and no singular project id`);
      }
    } else {
      if (allocationShares.length !== 0 || delivery.project.id !== null || delivery.project.attributionMethod !== "none") {
        throw new Error(`${delivery.id} ${delivery.project.allocation.kind} allocation must have no shares, id, or attribution`);
      }
      if (delivery.project.allocation.kind === "unknown" && delivery.project.evidenceState === "verified") {
        throw new Error(`${delivery.id} unknown project allocation cannot have verified evidence`);
      }
    }

    const submittedMs = parseInstant(delivery.submittedAt, `${delivery.id}.submittedAt`);
    if (delivery.mergedAt !== null) {
      const mergedMs = parseInstant(delivery.mergedAt, `${delivery.id}.mergedAt`);
      if (mergedMs < submittedMs) throw new Error(`${delivery.id}.mergedAt precedes submittedAt`);
    }
    const attemptIds = new Set<string>();
    const attemptSequences = new Set<number>();
    for (const [index, attempt] of delivery.checks.attempts.entries()) {
      assertOneOf(attempt.state, CHECK_STATES, `${delivery.id}.checks.attempts[${index}].state`);
      assertCanonicalIdentity(attempt.attemptId, `${delivery.id}.checks.attempts[${index}].attemptId`);
      assertCanonicalIdentity(attempt.revisionId, `${delivery.id}.checks.attempts[${index}].revisionId`);
      if (attempt.supersedesAttemptId !== null) {
        assertCanonicalIdentity(
          attempt.supersedesAttemptId,
          `${delivery.id}.checks.attempts[${index}].supersedesAttemptId`,
        );
      }
      if (!Number.isInteger(attempt.sequence) || attempt.sequence < 1) {
        throw new Error(`${delivery.id} check sequence must be a positive integer`);
      }
      if (attemptIds.has(attempt.attemptId)) throw new Error(`${delivery.id} check attempt ids must be unique`);
      if (attemptSequences.has(attempt.sequence)) throw new Error(`${delivery.id} check sequences must be unique`);
      attemptIds.add(attempt.attemptId);
      attemptSequences.add(attempt.sequence);
      const attemptMs = parseInstant(attempt.at, `${delivery.id}.checks.attempts[${index}].at`);
      if (attemptMs < submittedMs) throw new Error(`${delivery.id} check attempt precedes submittedAt`);
    }
    const orderedAttempts = delivery.checks.attempts.slice().sort((a, b) => a.sequence - b.sequence);
    for (const [index, attempt] of orderedAttempts.entries()) {
      const previous = orderedAttempts[index - 1];
      if (index === 0 && attempt.supersedesAttemptId !== null) {
        throw new Error(`${delivery.id} first check attempt must not supersede another attempt`);
      }
      if (index > 0 && attempt.supersedesAttemptId !== previous.attemptId) {
        throw new Error(`${delivery.id} check lineage must supersede the immediately previous attempt`);
      }
      if (previous && parseInstant(attempt.at, `${delivery.id}.check.at`) < parseInstant(previous.at, `${delivery.id}.check.at`)) {
        throw new Error(`${delivery.id} check timestamps must not move backward across sequence`);
      }
    }
    for (const [index, event] of delivery.rework.events.entries()) {
      assertCanonicalIdentity(event.kind, `${delivery.id}.rework.events[${index}].kind`);
      parseInstant(event.at, `${delivery.id}.rework.events[${index}].at`);
    }
    for (const [key, value] of Object.entries(delivery.tokensToFirstGreen)) {
      if (key !== "evidenceState") assertNonnegative(value as number | null, `${delivery.id}.${key}`);
    }
    if (
      delivery.tokensToFirstGreen.evidenceState === "verified" &&
      TOKEN_MEASURES.some(([, field]) => delivery.tokensToFirstGreen[field] === null)
    ) {
      throw new Error(`${delivery.id} verified token evidence requires every token dimension`);
    }
    assertNonnegative(delivery.cost.usd, `${delivery.id}.cost.usd`);
    if (delivery.cost.kind === "missing" && delivery.cost.usd !== null) {
      throw new Error(`${delivery.id} missing cost must be null`);
    }
    if (delivery.cost.kind !== "missing" && delivery.cost.usd === null) {
      throw new Error(`${delivery.id} ${delivery.cost.kind} cost must have a value`);
    }
    if (delivery.cost.kind === "missing" && delivery.cost.evidenceState === "verified") {
      throw new Error(`${delivery.id} missing cost cannot have verified evidence`);
    }
    if (delivery.cost.kind === "estimated" && delivery.cost.evidenceState === "verified") {
      throw new Error(`${delivery.id} estimated cost cannot have verified evidence`);
    }
    if (delivery.techniques.ids === null && delivery.techniques.evidenceState === "verified") {
      throw new Error(`${delivery.id} null technique ids cannot have verified evidence`);
    }
    if (delivery.techniques.ids !== null) {
      for (const [index, id] of delivery.techniques.ids.entries()) {
        assertCanonicalIdentity(id, `${delivery.id}.techniques.ids[${index}]`);
      }
      if (new Set(delivery.techniques.ids).size !== delivery.techniques.ids.length) {
        throw new Error(`${delivery.id} technique ids must be unique`);
      }
    }
  }
}

/**
 * Adds quantities only when their dimensions are identical. This is the sole
 * aggregation primitive exported by the contract: USD cannot be combined
 * with tokens, and input/output/cache dimensions cannot be collapsed.
 */
export function sumLikeQuantities(quantities: readonly MetricQuantity[]): MetricQuantity {
  if (quantities.length === 0) throw new Error("cannot sum an empty quantity list");
  for (const [index, quantity] of quantities.entries()) {
    assertOneOf(quantity.unit, METRIC_UNITS, `quantities[${index}].unit`);
    assertOneOf(quantity.knowledge, QUANTITY_KNOWLEDGE_STATES, `quantities[${index}].knowledge`);
    if (quantity.value === null) {
      if (quantity.knowledge !== "unknown") {
        throw new Error(`quantities[${index}] null value must have unknown knowledge`);
      }
    } else {
      if (!Number.isFinite(quantity.value) || quantity.value < 0) {
        throw new Error(`quantities[${index}] value must be a non-negative finite number or null`);
      }
      if (quantity.knowledge === "unknown") {
        throw new Error(`quantities[${index}] numeric value cannot have unknown knowledge`);
      }
    }
  }
  const unit = quantities[0].unit;
  if (quantities.some((quantity) => quantity.unit !== unit)) {
    throw new Error("cannot combine metric quantities with different units");
  }
  if (quantities.some((quantity) => quantity.value === null || quantity.knowledge === "unknown")) {
    return { value: null, unit, knowledge: "unknown" };
  }
  const total = quantities.reduce((sum, quantity) => sum + (quantity.value as number), 0);
  if (!Number.isFinite(total)) throw new Error("combined metric quantity must remain finite");
  return {
    value: total,
    unit,
    knowledge: quantities.some((quantity) => quantity.knowledge === "floor") ? "floor" : "known",
  };
}

export type PersistedMetricIdentity = {
  metricId: string;
  definitionVersion: string;
  lifecycleStatus: string;
  validationStatus: string;
  populationEvent: string;
  formulaConfig: MetricFormulaConfig;
};

const STABILITY_HORIZON_METRICS: readonly MetricId[] = ["mature_stable_delivery", "post_merge_rework"];

function assertFormulaConfig(
  identity: PersistedMetricIdentity,
  side: "left" | "right",
  metricId: MetricId,
): void {
  if (typeof identity.formulaConfig !== "object" || identity.formulaConfig === null) {
    throw new Error(`${side}.formulaConfig must be an object`);
  }
  const keys = Object.keys(identity.formulaConfig).sort();
  if (keys.length !== 1 || keys[0] !== "stabilityHorizonDays") {
    throw new Error(`${side}.formulaConfig must contain only stabilityHorizonDays`);
  }
  const horizon = identity.formulaConfig.stabilityHorizonDays;
  if (STABILITY_HORIZON_METRICS.includes(metricId)) {
    if (!Number.isInteger(horizon) || (horizon as number) <= 0) {
      throw new Error(`${side}.formulaConfig.stabilityHorizonDays must be a positive integer`);
    }
  } else if (horizon !== null) {
    throw new Error(`${side}.formulaConfig.stabilityHorizonDays must be null for ${metricId}`);
  }
}

function assertIdentityMatchesRegistry(
  identity: PersistedMetricIdentity,
  side: "left" | "right",
  metricId: MetricId,
): void {
  const definition = METRIC_REGISTRY[metricId];
  assertCanonicalIdentity(identity.definitionVersion, `${side}.definitionVersion`);
  assertOneOf(identity.lifecycleStatus, METRIC_LIFECYCLE_STATUSES, `${side}.lifecycleStatus`);
  assertOneOf(identity.validationStatus, METRIC_VALIDATION_STATUSES, `${side}.validationStatus`);
  assertOneOf(identity.populationEvent, ["submitted", "merged"] as const, `${side}.populationEvent`);
  if (identity.lifecycleStatus !== definition.lifecycleStatus) {
    throw new Error(`${side}.lifecycleStatus does not match the metric registry`);
  }
  if (identity.validationStatus !== definition.validationStatus) {
    throw new Error(`${side}.validationStatus does not match the metric registry`);
  }
  if (identity.populationEvent !== definition.populationEvent) {
    throw new Error(`${side}.populationEvent does not match the metric registry`);
  }
  assertFormulaConfig(identity, side, metricId);
}

/** Fail closed before comparing persisted results across formula contracts. */
export function assertComparableMetricResults(
  left: PersistedMetricIdentity,
  right: PersistedMetricIdentity,
): void {
  assertOneOf(left.metricId, METRIC_IDS, "left.metricId");
  assertOneOf(right.metricId, METRIC_IDS, "right.metricId");
  if (left.metricId !== right.metricId) {
    throw new Error(`cannot compare different metrics: ${left.metricId} vs ${right.metricId}`);
  }
  assertIdentityMatchesRegistry(left, "left", left.metricId);
  assertIdentityMatchesRegistry(right, "right", right.metricId);
  if (left.definitionVersion !== right.definitionVersion) {
    throw new Error(
      `cannot compare incompatible formula versions: ${left.definitionVersion} vs ${right.definitionVersion}`,
    );
  }
  const currentVersion = METRIC_REGISTRY[left.metricId].definitionVersion;
  if (left.definitionVersion !== currentVersion) {
    throw new Error(`unsupported persisted formula version: ${left.definitionVersion}`);
  }
  if (left.formulaConfig.stabilityHorizonDays !== right.formulaConfig.stabilityHorizonDays) {
    throw new Error(
      `cannot compare incompatible formula configuration: stabilityHorizonDays ${String(left.formulaConfig.stabilityHorizonDays)} vs ${String(right.formulaConfig.stabilityHorizonDays)}`,
    );
  }
  if (left.validationStatus === "blocked" || left.validationStatus === "unvalidated") {
    throw new Error(`left result validation status is not comparable: ${left.validationStatus}`);
  }
  if (right.validationStatus === "blocked" || right.validationStatus === "unvalidated") {
    throw new Error(`right result validation status is not comparable: ${right.validationStatus}`);
  }
}

type EvidenceCounts = Omit<MetricCoverage, "eligibleCount" | "ratio">;

function countEvidence(states: readonly EvidenceState[], eligibleCount = states.length): MetricCoverage {
  const counts: EvidenceCounts = {
    verifiedCount: 0,
    inferredCount: 0,
    partialCount: 0,
    blockedCount: 0,
    excludedCount: 0,
  };
  for (const state of states) counts[`${state}Count` as keyof EvidenceCounts] += 1;
  return {
    ...counts,
    eligibleCount,
    ratio: eligibleCount === 0 ? null : counts.verifiedCount / eligibleCount,
  };
}

function evidenceFromCoverage(coverage: MetricCoverage): EvidenceState {
  if (coverage.eligibleCount === 0) return "excluded";
  if (coverage.excludedCount === coverage.eligibleCount) return "excluded";
  if (coverage.blockedCount === coverage.eligibleCount) return "blocked";
  if (coverage.partialCount > 0 || coverage.blockedCount > 0 || coverage.excludedCount > 0) return "partial";
  if (coverage.inferredCount > 0) return "inferred";
  return "verified";
}

function combineEvidenceStates(states: readonly EvidenceState[]): EvidenceState {
  if (states.includes("blocked")) return "blocked";
  if (states.includes("partial") || states.includes("excluded")) return "partial";
  if (states.includes("inferred")) return "inferred";
  return "verified";
}

function claimFromEvidence(evidenceState: EvidenceState): ClaimClass {
  if (evidenceState === "blocked" || evidenceState === "excluded") return "not_estimable";
  if (evidenceState === "partial" || evidenceState === "inferred") return "suggestive";
  return "observed";
}

function known(value: number, unit: MetricUnit): MetricQuantity {
  return { value, unit, knowledge: "known" };
}

function unknown(unit: MetricUnit): MetricQuantity {
  return { value: null, unit, knowledge: "unknown" };
}

function ratioMeasure(
  key: string,
  numerator: number,
  denominator: number,
  knowledge: QuantityKnowledge = "known",
): MetricMeasure {
  return {
    key,
    numerator: known(numerator, "delivery"),
    denominator: known(denominator, "delivery"),
    value:
      denominator === 0 || knowledge === "unknown"
        ? unknown("ratio")
        : { value: numerator / denominator, unit: "ratio", knowledge },
  };
}

function averageMeasure(options: {
  key: string;
  sum: number | null;
  count: number;
  numeratorUnit: MetricUnit;
  valueUnit: MetricUnit;
}): MetricMeasure {
  return {
    key: options.key,
    numerator: options.sum === null ? unknown(options.numeratorUnit) : known(options.sum, options.numeratorUnit),
    denominator: known(options.count, "delivery"),
    value:
      options.sum === null || options.count === 0
        ? unknown(options.valueUnit)
        : known(options.sum / options.count, options.valueUnit),
  };
}

function reasons(entries: readonly string[]): Readonly<Record<string, number>> {
  const result: Record<string, number> = {};
  for (const entry of entries) result[entry] = (result[entry] ?? 0) + 1;
  return Object.fromEntries(Object.entries(result).sort(([a], [b]) => a.localeCompare(b)));
}

function attributionFor(
  deliveries: readonly LearningDelivery[],
  select: (delivery: LearningDelivery) => AttributionMethod = (delivery) => delivery.deliveryAttribution.method,
): MetricAttribution {
  const methods = deliveries.map(select);
  const attributedCount = methods.filter((method) => method !== "none").length;
  const allocationMix = {
    exactCount: deliveries.filter((delivery) => delivery.project.allocation.kind === "exact").length,
    apportionedCount: deliveries.filter((delivery) => delivery.project.allocation.kind === "apportioned").length,
    unallocatedCount: deliveries.filter((delivery) => delivery.project.allocation.kind === "unallocated").length,
    unknownCount: deliveries.filter((delivery) => delivery.project.allocation.kind === "unknown").length,
  };
  return {
    methods: [...new Set(methods)].sort() as AttributionMethod[],
    attributedCount,
    eligibleCount: deliveries.length,
    ratio: deliveries.length === 0 ? null : attributedCount / deliveries.length,
    allocationMix,
  };
}

function result(options: {
  metricId: MetricId;
  manifest: MetricAnalysisManifest;
  measures: readonly MetricMeasure[];
  totals?: readonly NamedMetricQuantity[];
  sample: MetricSample;
  coverage: MetricCoverage;
  maturity?: MetricMaturity;
  attribution: MetricAttribution;
  breakdown?: readonly MetricBreakdownRow[];
  limitations?: readonly string[];
  evidenceState?: EvidenceState;
  claimClass?: ClaimClass;
}): MetricResult {
  const evidenceState = options.evidenceState ?? evidenceFromCoverage(options.coverage);
  const definition = METRIC_REGISTRY[options.metricId];
  const usesStabilityHorizon = STABILITY_HORIZON_METRICS.includes(options.metricId);
  return {
    metricId: options.metricId,
    definitionVersion: METRIC_DEFINITION_VERSION,
    lifecycleStatus: definition.lifecycleStatus,
    validationStatus: definition.validationStatus,
    populationEvent: definition.populationEvent,
    formulaConfig: {
      stabilityHorizonDays: usesStabilityHorizon ? options.manifest.stabilityHorizonDays : null,
    },
    window: { ...options.manifest.window },
    asOf: options.manifest.asOf,
    numerator: options.measures.map((measure) => ({ key: measure.key, ...measure.numerator })),
    denominator: options.measures.map((measure) => ({ key: measure.key, ...measure.denominator })),
    measures: options.measures,
    totals: options.totals ?? [],
    sample: options.sample,
    coverage: options.coverage,
    maturity: options.maturity ?? { horizonDays: null, matureCount: options.sample.sampleCount, censoredCount: 0 },
    attribution: options.attribution,
    evidenceState,
    claimClass: options.claimClass ?? claimFromEvidence(evidenceState),
    breakdown: options.breakdown ?? [],
    limitations: options.limitations ?? [],
  };
}

function submittedDeliveries(manifest: MetricAnalysisManifest): LearningDelivery[] {
  const startMs = parseInstant(manifest.window.startInclusive, "window.startInclusive");
  const endMs = parseInstant(manifest.window.endExclusive, "window.endExclusive");
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  return manifest.deliveries
    .filter((delivery) => {
      const submittedMs = parseInstant(delivery.submittedAt, `${delivery.id}.submittedAt`);
      return (
        submittedMs >= startMs &&
        submittedMs < endMs &&
        submittedMs <= asOfMs &&
        delivery.deliveryAttribution.evidenceState !== "excluded"
      );
    })
    .slice()
    .sort((a, b) => a.id.localeCompare(b.id));
}

function nonExcludedDeliveries(manifest: MetricAnalysisManifest): LearningDelivery[] {
  return manifest.deliveries
    .filter((delivery) => delivery.deliveryAttribution.evidenceState !== "excluded")
    .slice()
    .sort((a, b) => a.id.localeCompare(b.id));
}

function attemptsByAsOf(delivery: LearningDelivery, asOfMs: number): CheckAttempt[] {
  return delivery.checks.attempts
    .filter((attempt) => parseInstant(attempt.at, `${delivery.id}.check.at`) <= asOfMs)
    .slice()
    .sort((a, b) => a.sequence - b.sequence);
}

function hasAmbiguousAttemptTimestamp(attempts: readonly CheckAttempt[]): boolean {
  const statesByTimestamp = new Map<string, Set<CheckState>>();
  for (const attempt of attempts) {
    const timestampKey = String(parseInstant(attempt.at, "check.at"));
    const states = statesByTimestamp.get(timestampKey) ?? new Set<CheckState>();
    states.add(attempt.state);
    statesByTimestamp.set(timestampKey, states);
  }
  return [...statesByTimestamp.values()].some((states) => states.size > 1);
}

function effectiveCheckEvidence(delivery: LearningDelivery, asOfMs: number): EvidenceState {
  if (delivery.checks.evidenceState !== "verified") return delivery.checks.evidenceState;
  const attempts = attemptsByAsOf(delivery, asOfMs);
  if (
    attempts.length === 0 ||
    hasAmbiguousAttemptTimestamp(attempts) ||
    attempts.some((attempt) => attempt.state === "none" || attempt.state === "unknown")
  ) {
    return "partial";
  }
  return "verified";
}

function verifiedFirstGreen(delivery: LearningDelivery, asOfMs: number): CheckAttempt | undefined {
  if (effectiveCheckEvidence(delivery, asOfMs) !== "verified") return undefined;
  return attemptsByAsOf(delivery, asOfMs).find((attempt) => attempt.state === "passed");
}

function verifiedFinalGreen(delivery: LearningDelivery, asOfMs: number): CheckAttempt | undefined {
  if (effectiveCheckEvidence(delivery, asOfMs) !== "verified") return undefined;
  const attempts = attemptsByAsOf(delivery, asOfMs);
  const final = attempts.at(-1);
  return final?.state === "passed" ? final : undefined;
}

function verifiedCorrectionPass(delivery: LearningDelivery, asOfMs: number): CheckAttempt | undefined {
  if (effectiveCheckEvidence(delivery, asOfMs) !== "verified") return undefined;
  const attempts = attemptsByAsOf(delivery, asOfMs);
  const first = attempts[0];
  const final = attempts.at(-1);
  if (first?.state !== "failed" || final?.state !== "passed" || final.revisionId === first.revisionId) {
    return undefined;
  }
  return final;
}

function standardSample(eligibleCount: number, sampleCount = eligibleCount): MetricSample {
  return {
    eligibleCount,
    sampleCount,
    excludedCount: 0,
    censoredCount: 0,
    unknownCount: Math.max(eligibleCount - sampleCount, 0),
    exclusionReasons: {},
  };
}

function analyzeProjectAllocation(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const allocated = deliveries.filter(
    (delivery) =>
      (delivery.project.allocation.kind === "exact" || delivery.project.allocation.kind === "apportioned") &&
      delivery.project.attributionMethod !== "none" &&
      (delivery.project.evidenceState === "verified" || delivery.project.evidenceState === "inferred"),
  );
  const coverage = countEvidence(deliveries.map((delivery) => delivery.project.evidenceState));
  const unknownCount = deliveries.filter(
    (delivery) =>
      delivery.project.allocation.kind === "unknown" ||
      (delivery.project.evidenceState !== "verified" && delivery.project.evidenceState !== "inferred"),
  ).length;
  return result({
    metricId: "project_allocation_coverage",
    manifest,
    measures: [ratioMeasure("allocation_rate", allocated.length, deliveries.length, coverage.ratio === 1 ? "known" : "floor")],
    sample: {
      ...standardSample(deliveries.length),
      unknownCount,
      exclusionReasons: unknownCount === 0 ? {} : { unverified_project_allocation: unknownCount },
    },
    coverage,
    attribution: attributionFor(deliveries, (delivery) => delivery.project.attributionMethod),
    breakdown: [
      { key: "exact", count: deliveries.filter((delivery) => delivery.project.allocation.kind === "exact").length },
      { key: "apportioned", count: deliveries.filter((delivery) => delivery.project.allocation.kind === "apportioned").length },
      { key: "unallocated", count: deliveries.filter((delivery) => delivery.project.allocation.kind === "unallocated").length },
      { key: "unknown", count: deliveries.filter((delivery) => delivery.project.allocation.kind === "unknown").length },
    ],
    limitations: coverage.ratio === 1 ? [] : ["Unverified project allocation stays in the denominator and cannot inflate coverage."],
  });
}

function analyzeFirstPass(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  const effectiveStates = deliveries.map((delivery) => effectiveCheckEvidence(delivery, asOfMs));
  const passed = deliveries.filter((delivery) => {
    const first = attemptsByAsOf(delivery, asOfMs)[0];
    return effectiveCheckEvidence(delivery, asOfMs) === "verified" && first?.state === "passed";
  });
  const determinate = deliveries.filter((delivery) => {
    const first = attemptsByAsOf(delivery, asOfMs)[0];
    return effectiveCheckEvidence(delivery, asOfMs) === "verified" && (first?.state === "passed" || first?.state === "failed");
  });
  const coverage = countEvidence(effectiveStates);
  const incomplete = effectiveStates.filter((state) => state !== "verified").length;
  const unknownRows = deliveries.filter((delivery) => {
    const first = attemptsByAsOf(delivery, asOfMs)[0];
    return effectiveCheckEvidence(delivery, asOfMs) !== "verified" || !first || first.state === "none" || first.state === "unknown";
  });
  const noneOrUnknown = deliveries.filter((delivery) => {
    const first = attemptsByAsOf(delivery, asOfMs)[0];
    return !first || first.state === "none" || first.state === "unknown";
  }).length;
  return result({
    metricId: "first_pass_yield",
    manifest,
    measures: [
      ratioMeasure(
        "first_pass_yield",
        passed.length,
        deliveries.length,
        determinate.length === 0 && deliveries.length > 0 ? "unknown" : incomplete > 0 ? "floor" : "known",
      ),
    ],
    sample: {
      ...standardSample(deliveries.length),
      unknownCount: unknownRows.length,
      exclusionReasons: reasons([
        ...Array.from({ length: noneOrUnknown }, () => "none_or_unknown_check"),
        ...Array.from({ length: incomplete }, () => "incomplete_check_evidence"),
      ]),
    },
    coverage,
    attribution: attributionFor(deliveries),
    limitations:
      incomplete > 0
        ? ["Unknown, none, ambiguous, partial, blocked, inferred, or excluded check evidence cannot pass validation."]
        : [],
    claimClass: determinate.length === 0 && deliveries.length > 0 ? "not_estimable" : undefined,
  });
}

function analyzeCorrectionLoop(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  const eligible = deliveries.filter((delivery) => {
    const first = attemptsByAsOf(delivery, asOfMs)[0];
    return delivery.checks.evidenceState === "verified" && first?.state === "failed";
  });
  const corrected = eligible.filter((delivery) => Boolean(verifiedCorrectionPass(delivery, asOfMs)));
  const effectiveStates = eligible.map((delivery) => effectiveCheckEvidence(delivery, asOfMs));
  const coverage = countEvidence(effectiveStates);
  const evaluable = effectiveStates.filter((state) => state === "verified").length;
  const incomplete = effectiveStates.length - evaluable;
  return result({
    metricId: "correction_loop",
    manifest,
    measures: [
      ratioMeasure(
        "correction_loop_closure",
        corrected.length,
        eligible.length,
        evaluable === 0 && eligible.length > 0 ? "unknown" : incomplete > 0 ? "floor" : "known",
      ),
    ],
    sample: standardSample(eligible.length),
    coverage,
    attribution: attributionFor(eligible),
    limitations:
      eligible.length === 0
        ? ["No verified first-attempt failures are eligible for a correction-loop rate."]
        : ["A correction closes only when a later, lineage-linked passing attempt uses a different revision."],
    claimClass: evaluable === 0 && eligible.length > 0 ? "not_estimable" : undefined,
  });
}

const TOKEN_MEASURES = [
  ["input_tokens", "inputTokens", "input_token", "input_token_per_delivery"],
  ["output_tokens", "outputTokens", "output_token", "output_token_per_delivery"],
  ["cache_read_tokens", "cacheReadTokens", "cache_read_token", "cache_read_token_per_delivery"],
  ["cache_write_tokens", "cacheWriteTokens", "cache_write_token", "cache_write_token_per_delivery"],
] as const;

function analyzeTimeTokens(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  const withGreen = deliveries
    .map((delivery) => ({ delivery, green: verifiedFirstGreen(delivery, asOfMs) }))
    .filter((row): row is { delivery: LearningDelivery; green: CheckAttempt } => Boolean(row.green));
  const durations = withGreen.map((row) => parseInstant(row.green.at, "green.at") - parseInstant(row.delivery.submittedAt, "submittedAt"));
  const measures: MetricMeasure[] = [
    averageMeasure({
      key: "time_to_first_green_ms",
      sum: durations.length === 0 ? null : durations.reduce((sum, value) => sum + value, 0),
      count: durations.length,
      numeratorUnit: "millisecond",
      valueUnit: "millisecond_per_delivery",
    }),
  ];
  for (const [key, field, numeratorUnit, valueUnit] of TOKEN_MEASURES) {
    const values = withGreen
      .filter(
        (row) =>
          row.delivery.tokensToFirstGreen.evidenceState !== "blocked" &&
          row.delivery.tokensToFirstGreen.evidenceState !== "excluded",
      )
      .map((row) => row.delivery.tokensToFirstGreen[field])
      .filter((value): value is number => value !== null);
    measures.push(
      averageMeasure({
        key,
        sum: values.length === 0 ? null : values.reduce((sum, value) => sum + value, 0),
        count: values.length,
        numeratorUnit,
        valueUnit,
      }),
    );
  }
  const coverage = countEvidence(
    deliveries.map((delivery) =>
      combineEvidenceStates([effectiveCheckEvidence(delivery, asOfMs), delivery.tokensToFirstGreen.evidenceState]),
    ),
  );
  return result({
    metricId: "time_tokens_to_first_green",
    manifest,
    measures,
    sample: {
      eligibleCount: deliveries.length,
      sampleCount: withGreen.length,
      excludedCount: 0,
      censoredCount: 0,
      unknownCount: deliveries.length - withGreen.length,
      exclusionReasons: withGreen.length === deliveries.length ? {} : { no_verified_green_by_as_of: deliveries.length - withGreen.length },
    },
    coverage,
    attribution: attributionFor(deliveries),
    limitations: ["Every token dimension has its own known-observation denominator; dimensions are never blended."],
  });
}

type MatureCohort = {
  merged: LearningDelivery[];
  mature: LearningDelivery[];
  censored: LearningDelivery[];
};

function matureCohort(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MatureCohort {
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  const startMs = parseInstant(manifest.window.startInclusive, "window.startInclusive");
  const endMs = parseInstant(manifest.window.endExclusive, "window.endExclusive");
  const horizonMs = manifest.stabilityHorizonDays * DAY_MS;
  const merged = deliveries.filter((delivery) => {
    if (delivery.mergedAt === null) return false;
    const mergedMs = parseInstant(delivery.mergedAt, `${delivery.id}.mergedAt`);
    return mergedMs >= startMs && mergedMs < endMs && mergedMs <= asOfMs;
  });
  const mature = merged.filter(
    (delivery) => asOfMs >= parseInstant(delivery.mergedAt as string, `${delivery.id}.mergedAt`) + horizonMs,
  );
  const matureIds = new Set(mature.map((delivery) => delivery.id));
  return { merged, mature, censored: merged.filter((delivery) => !matureIds.has(delivery.id)) };
}

function hasReworkInHorizon(delivery: LearningDelivery, manifest: MetricAnalysisManifest): boolean {
  if (delivery.mergedAt === null) return false;
  const mergedMs = parseInstant(delivery.mergedAt, `${delivery.id}.mergedAt`);
  const horizonEnd = mergedMs + manifest.stabilityHorizonDays * DAY_MS;
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  return delivery.rework.events.some((event) => {
    const at = parseInstant(event.at, `${delivery.id}.rework.at`);
    return at >= mergedMs && at <= horizonEnd && at <= asOfMs;
  });
}

function hasVerifiedStableCheck(delivery: LearningDelivery, asOfMs: number): boolean {
  return Boolean(verifiedFinalGreen(delivery, asOfMs));
}

function analyzeMatureStable(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const cohort = matureCohort(manifest, deliveries);
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  const stable = cohort.mature.filter(
    (delivery) =>
      hasVerifiedStableCheck(delivery, asOfMs) &&
      delivery.rework.evidenceState === "verified" &&
      !hasReworkInHorizon(delivery, manifest),
  );
  const coverage = countEvidence(
    cohort.mature.map((delivery) =>
      combineEvidenceStates([effectiveCheckEvidence(delivery, asOfMs), delivery.rework.evidenceState]),
    ),
  );
  const evaluable = cohort.mature.filter(
    (delivery) => effectiveCheckEvidence(delivery, asOfMs) === "verified" && delivery.rework.evidenceState === "verified",
  ).length;
  const incomplete = evaluable < cohort.mature.length;
  const evidenceState =
    cohort.merged.length > 0 && cohort.mature.length === 0
      ? "excluded"
      : evidenceFromCoverage(coverage);
  const unknownCount = cohort.mature.length - evaluable;
  return result({
    metricId: "mature_stable_delivery",
    manifest,
    measures: [
      ratioMeasure(
        "mature_stable_delivery",
        stable.length,
        cohort.mature.length,
        evaluable === 0 && cohort.mature.length > 0 ? "unknown" : incomplete ? "floor" : "known",
      ),
    ],
    sample: {
      eligibleCount: cohort.merged.length,
      sampleCount: cohort.mature.length,
      excludedCount: 0,
      censoredCount: cohort.censored.length,
      unknownCount,
      exclusionReasons: cohort.censored.length === 0 ? {} : { right_censored: cohort.censored.length },
    },
    coverage,
    maturity: {
      horizonDays: manifest.stabilityHorizonDays,
      matureCount: cohort.mature.length,
      censoredCount: cohort.censored.length,
    },
    attribution: attributionFor(cohort.merged),
    limitations: cohort.censored.length === 0 ? [] : ["Right-censored deliveries are excluded from the rate until the full horizon elapses."],
    evidenceState,
    claimClass: evaluable === 0 && cohort.mature.length > 0 ? "not_estimable" : undefined,
  });
}

function analyzeRework(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const cohort = matureCohort(manifest, deliveries);
  const reworked = cohort.mature.filter((delivery) => hasReworkInHorizon(delivery, manifest));
  const coverage = countEvidence(cohort.mature.map((delivery) => delivery.rework.evidenceState));
  const incomplete = cohort.mature.some((delivery) => delivery.rework.evidenceState !== "verified");
  const evidenceState =
    cohort.merged.length > 0 && cohort.mature.length === 0
      ? "excluded"
      : evidenceFromCoverage(coverage);
  return result({
    metricId: "post_merge_rework",
    manifest,
    measures: [ratioMeasure("post_merge_rework", reworked.length, cohort.mature.length, incomplete ? "floor" : "known")],
    sample: {
      eligibleCount: cohort.merged.length,
      sampleCount: cohort.mature.length,
      excludedCount: 0,
      censoredCount: cohort.censored.length,
      unknownCount: cohort.mature.filter((delivery) => delivery.rework.evidenceState !== "verified").length,
      exclusionReasons: cohort.censored.length === 0 ? {} : { right_censored: cohort.censored.length },
    },
    coverage,
    maturity: {
      horizonDays: manifest.stabilityHorizonDays,
      matureCount: cohort.mature.length,
      censoredCount: cohort.censored.length,
    },
    attribution: attributionFor(cohort.merged),
    limitations: incomplete ? ["Observed rework is a floor while any mature delivery has incomplete rework evidence."] : [],
    evidenceState,
  });
}

function analyzeKnownCost(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const withKnownCost = deliveries.filter(
    (delivery) =>
      delivery.cost.kind !== "missing" &&
      delivery.cost.usd !== null &&
      delivery.cost.evidenceState !== "blocked" &&
      delivery.cost.evidenceState !== "excluded",
  );
  const coverage = countEvidence(deliveries.map((delivery) => delivery.cost.evidenceState));
  const knownSum = withKnownCost.reduce((sum, delivery) => sum + (delivery.cost.usd as number), 0);
  const total: NamedMetricQuantity = {
    key: "known_cost_usd",
    value: withKnownCost.length === 0 && deliveries.length > 0 ? null : knownSum,
    unit: "usd",
    knowledge:
      withKnownCost.length === 0 && deliveries.length > 0
        ? "unknown"
        : withKnownCost.length < deliveries.length
          ? "floor"
          : "known",
  };
  return result({
    metricId: "known_cost_coverage",
    manifest,
    measures: [ratioMeasure("known_cost_coverage", withKnownCost.length, deliveries.length)],
    totals: [total],
    sample: {
      ...standardSample(deliveries.length),
      unknownCount: deliveries.length - withKnownCost.length,
      exclusionReasons:
        withKnownCost.length === deliveries.length ? {} : { missing_or_unusable_cost: deliveries.length - withKnownCost.length },
    },
    coverage,
    attribution: attributionFor(deliveries),
    limitations: withKnownCost.length === deliveries.length ? [] : ["Known USD is a floor; missing cost is unknown and never substituted with zero."],
  });
}

function analyzeTechniqueExposure(manifest: MetricAnalysisManifest, deliveries: LearningDelivery[]): MetricResult {
  const usable = deliveries.filter(
    (delivery) =>
      delivery.techniques.ids !== null &&
      delivery.techniques.attributionMethod !== "none" &&
      delivery.techniques.evidenceState !== "blocked" &&
      delivery.techniques.evidenceState !== "excluded",
  );
  const exposed = usable.filter((delivery) => (delivery.techniques.ids as readonly string[]).length > 0);
  const byTechnique = new Map<string, number>();
  for (const delivery of exposed) {
    for (const technique of delivery.techniques.ids as readonly string[]) {
      const key = technique.trim();
      byTechnique.set(key, (byTechnique.get(key) ?? 0) + 1);
    }
  }
  const coverage = countEvidence(deliveries.map((delivery) => delivery.techniques.evidenceState));
  return result({
    metricId: "technique_exposure",
    manifest,
    measures: [ratioMeasure("technique_exposure", exposed.length, deliveries.length, usable.length < deliveries.length ? "floor" : "known")],
    totals: [
      {
        key: "technique_assignments",
        value: [...byTechnique.values()].reduce((sum, value) => sum + value, 0),
        unit: "technique_exposure",
        knowledge: usable.length < deliveries.length ? "floor" : "known",
      },
    ],
    sample: {
      ...standardSample(deliveries.length),
      unknownCount: deliveries.length - usable.length,
      exclusionReasons: usable.length === deliveries.length ? {} : { unknown_exposure: deliveries.length - usable.length },
    },
    coverage,
    attribution: attributionFor(deliveries, (delivery) => delivery.techniques.attributionMethod),
    breakdown: [...byTechnique.entries()]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([key, count]) => ({ key, count })),
    limitations: ["Exposure counts are descriptive and do not estimate a technique's effect."],
  });
}

/** Analyze the requested registry entries in canonical registry order. */
export function analyzeLearningMetrics(manifest: MetricAnalysisManifest): MetricResult[] {
  validateMetricAnalysisManifest(manifest);
  const submitted = submittedDeliveries(manifest);
  const allNonExcluded = nonExcludedDeliveries(manifest);
  const requested = new Set(manifest.metricIds);
  const analyzers: Record<MetricId, () => MetricResult> = {
    project_allocation_coverage: () => analyzeProjectAllocation(manifest, submitted),
    first_pass_yield: () => analyzeFirstPass(manifest, submitted),
    correction_loop: () => analyzeCorrectionLoop(manifest, submitted),
    time_tokens_to_first_green: () => analyzeTimeTokens(manifest, submitted),
    mature_stable_delivery: () => analyzeMatureStable(manifest, allNonExcluded),
    post_merge_rework: () => analyzeRework(manifest, allNonExcluded),
    known_cost_coverage: () => analyzeKnownCost(manifest, submitted),
    technique_exposure: () => analyzeTechniqueExposure(manifest, submitted),
  };
  return METRIC_IDS.filter((metricId) => requested.has(metricId)).map((metricId) => analyzers[metricId]());
}
