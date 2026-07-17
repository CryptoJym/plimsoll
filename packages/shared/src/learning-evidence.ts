/**
 * Deterministic, local-only learning evidence packets (issue #101).
 *
 * This module is deliberately pure. It accepts bounded, explicit prospective
 * technique exposures and matched outcome pairs. It does not fetch, call a
 * model, rank people, recommend a technique, or write/install a skill.
 */
import { createHash } from "node:crypto";
import { homedir } from "node:os";
import { isAbsolute, relative, resolve, sep } from "node:path";

export const LEARNING_EVIDENCE_SCHEMA_VERSION = "1.0.0" as const;
export const LEARNING_ANALYSIS_VERSION = "1.0.0" as const;

export const LEARNING_INVENTORY_DISPOSITIONS = [
  "new_skill",
  "enhance_existing",
  "compose_existing",
  "duplicate",
  "conflict",
  "quarantine",
  "insufficient_evidence",
] as const;
export type LearningInventoryDisposition = (typeof LEARNING_INVENTORY_DISPOSITIONS)[number];

export const SKILL_CANDIDATE_LIFECYCLE = [
  "observed",
  "candidate",
  "reviewed_playbook",
  "owner_approved_pilot",
  "evaluated",
  "skill_proposal",
  "owner_approved_publication",
  "monitored",
  "stale",
  "deprecated",
  "rolled_back",
] as const;
export type SkillCandidateLifecycleState = (typeof SKILL_CANDIDATE_LIFECYCLE)[number];

export const LEARNING_NOT_ESTIMABLE_REASONS = [
  "insufficient_statistical_sample",
  "privacy_minimum_not_met",
  "incomplete_outcome_pairs",
  "attribution_coverage_below_minimum",
  "simpson_reversal",
] as const;
export type LearningNotEstimableReason = (typeof LEARNING_NOT_ESTIMABLE_REASONS)[number];

export type LearningAnalysisWindow = {
  startInclusive: string;
  endExclusive: string;
};

export type LearningSourceIdentity = {
  /** Opaque local snapshot identity. It must not contain raw prompts/content. */
  snapshotId: string;
  /** Hash of the bounded source query/parameters. */
  queryHash: string;
  /** Hash of the exact canonical matched-pair rows. */
  rowDigest: string;
  declaredPairCount: number;
  sourceKind: "local_owned_aggregate";
};

export type LearningMetricVersions = {
  outcomeMetric: string;
  techniqueExposure: string;
  projectAllocation: string;
};

export type LearningCohort = {
  projectId: string;
  workType: string;
  complexityBand: string;
  modelId: string;
  toolVersion: string;
  actorClusterId: string;
  repoClusterId: string;
  epochId: string;
};

export type LearningExposure = {
  state: "exposed" | "control";
  techniqueId: string | null;
  /** Only explicit operator declarations or machine receipts are admissible. */
  evidenceSource: "operator_declared" | "machine_receipt";
  contentOrigin: "operator" | "machine_observation";
  recordedAt: string;
};

export type LearningOutcome = {
  metricId: string;
  metricVersion: string;
  unit: string;
  direction: "higher_is_better" | "lower_is_better";
  /** Null is an honest unknown; it is never converted to zero. */
  value: number | null;
};

export type LearningAttribution = {
  method: "direct" | "deterministic_linkage" | "inferred" | "none";
  projectAllocation: "exact" | "apportioned" | "unallocated" | "unknown";
  coverage: number;
};

export type LearningObservation = {
  observationId: string;
  workStartedAt: string;
  outcomeObservedAt: string;
  cohort: LearningCohort;
  exposure: LearningExposure;
  outcome: LearningOutcome;
  attribution: LearningAttribution;
};

export type LearningOutcomePair = {
  pairId: string;
  exposed: LearningObservation;
  control: LearningObservation;
};

export type LearningHypothesisFamily = {
  familyId: string;
  hypothesisIndex: number;
  hypothesesTested: number;
  selectionPolicy: "pre_registered" | "all_reported";
  correction: "none" | "bonferroni";
  familyWiseAlpha: number;
  registeredAt: string;
};

export type LearningAnalysisGates = {
  /** Analytical power/reliability floor. This is not a privacy threshold. */
  statisticalMinCompletePairs: number;
  /** Disclosure/aggregation floor. This is independent of statistical power. */
  privacyMinCompletePairs: number;
  minimumAttributionCoverage: number;
  maxAbsoluteOutcome: number;
  maxPairs: number;
  maxCounterexamples: number;
  maxRuntimeMs: number;
};

export type LearningEvidenceManifest = {
  schemaVersion: typeof LEARNING_EVIDENCE_SCHEMA_VERSION;
  analysisVersion: typeof LEARNING_ANALYSIS_VERSION;
  analysisId: string;
  source: LearningSourceIdentity;
  metricVersions: LearningMetricVersions;
  window: LearningAnalysisWindow;
  asOf: string;
  techniqueId: string;
  hypothesisFamily: LearningHypothesisFamily;
  gates: LearningAnalysisGates;
  declaredConfounders: readonly string[];
  pairs: readonly LearningOutcomePair[];
};

export type LearningEffectEstimate = {
  method: "paired_mean_difference";
  uncertaintyMethod: "bonferroni_adjusted_normal_approximation";
  rawEstimate: number | null;
  directionAdjustedEstimate: number | null;
  standardError: number | null;
  lowerBound: number | null;
  upperBound: number | null;
  familyWiseConfidenceLevel: number;
  perHypothesisConfidenceLevel: number;
  associationDirection: "favors_exposed" | "favors_control" | "no_observed_difference" | "not_estimable";
  crudePairWeightedEstimate: number | null;
  equalStratumEstimate: number | null;
};

export type LearningEvidencePacket = {
  schemaVersion: typeof LEARNING_EVIDENCE_SCHEMA_VERSION;
  analysisVersion: typeof LEARNING_ANALYSIS_VERSION;
  packetFingerprint: string;
  sourceFingerprint: string;
  analysisId: string;
  source: LearningSourceIdentity;
  metricVersions: LearningMetricVersions;
  window: LearningAnalysisWindow;
  asOf: string;
  techniqueId: string;
  hypothesisFamily: LearningHypothesisFamily & {
    adjustedAlpha: number;
  };
  sample: {
    exposedCount: number;
    controlCount: number;
    completePairCount: number;
    incompletePairCount: number;
    statisticalMinimum: number;
    privacyMinimum: number;
    statisticalMinimumMet: boolean;
    privacyMinimumMet: boolean;
  };
  attribution: {
    observationCount: number;
    coverage: number;
    methods: Record<LearningAttribution["method"], number>;
    allocationMix: Record<LearningAttribution["projectAllocation"], number>;
    unallocatedCount: number;
    unknownCount: number;
    unknownOutcomeCount: number;
  };
  effect: LearningEffectEstimate;
  claimClass: "observational_association" | "not_estimable";
  causalClaim: false;
  prescriptiveClaim: false;
  notEstimableReasons: readonly LearningNotEstimableReason[];
  confounders: readonly string[];
  counterexamples: {
    count: number;
    pairIds: readonly string[];
    privacySuppressed: boolean;
  };
  skillCandidateReview: SkillCandidateReviewArtifact;
  budgets: {
    pairLimit: number;
    runtimeLimitMs: number;
    analysisWorkUnits: number;
    continuousLoop: false;
    modelCalls: 0;
  };
};

export type SkillCandidateReviewArtifact = {
  artifactType: "review_artifact_only";
  currentState: "observed";
  lifecycle: typeof SKILL_CANDIDATE_LIFECYCLE;
  inventoryDispositions: typeof LEARNING_INVENTORY_DISPOSITIONS;
  inventoryDisposition: "insufficient_evidence";
  containsExecutableInstructions: false;
  publicationAuthorized: false;
  installationAuthorized: false;
  ownerApprovalRequired: true;
  independentVerificationRequired: true;
  openWebOrModelTextMayGate: false;
  prohibitedWriteTargets: readonly string[];
};

export type LearningEvidenceRun =
  | {
      status: "computed";
      sourceFingerprint: string;
      analysisWorkUnits: number;
      packet: LearningEvidencePacket;
    }
  | {
      status: "unchanged";
      sourceFingerprint: string;
      analysisWorkUnits: 0;
      packet: null;
    };

export type CompileLearningEvidenceOptions = {
  previousSourceFingerprint?: string | null;
};

const IDENTITY_PATTERN = /^[A-Za-z0-9](?:[A-Za-z0-9._:@/-]{0,126}[A-Za-z0-9])?$/;
const VERSION_PATTERN = /^[0-9]+\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?$/;
const HASH_PATTERN = /^[0-9a-f]{64}$/;
const TIMEZONE_SUFFIX = /(?:Z|[+-]\d{2}:\d{2})$/;
const MAX_HARD_PAIRS = 10_000;
const MAX_HARD_RUNTIME_MS = 10_000;
const MAX_CONFOUNDERS = 32;
const COHORT_KEYS = [
  "projectId",
  "workType",
  "complexityBand",
  "modelId",
  "toolVersion",
  "actorClusterId",
  "repoClusterId",
  "epochId",
] as const;

const PROHIBITED_WRITE_TARGETS = Object.freeze([
  "~/.codex/skills",
  "~/.claude/skills",
  "~/.agents/skills",
  "~/.codex/memories",
  "~/.claude/memory",
  "global MEMORY.md",
  "installed team machines",
]);

function assertObject(value: unknown, field: string): asserts value is Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`${field} must be an object`);
  }
}

function assertExactKeys(value: Record<string, unknown>, keys: readonly string[], field: string): void {
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  if (actual.length !== expected.length || actual.some((key, index) => key !== expected[index])) {
    throw new Error(`${field} must contain exactly: ${expected.join(", ")}`);
  }
}

function assertCanonicalIdentity(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string") throw new Error(`${field} must be a string identity`);
  if (value !== value.trim().normalize("NFKC") || !IDENTITY_PATTERN.test(value)) {
    throw new Error(`${field} must be an already-trimmed bounded ASCII identity`);
  }
}

function assertVersion(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !VERSION_PATTERN.test(value)) {
    throw new Error(`${field} must be an explicit semantic version`);
  }
}

function assertHash(value: unknown, field: string): asserts value is string {
  if (typeof value !== "string" || !HASH_PATTERN.test(value)) {
    throw new Error(`${field} must be a lowercase sha256 hex digest`);
  }
}

function assertOneOf<T extends string>(value: unknown, allowed: readonly T[], field: string): asserts value is T {
  if (typeof value !== "string" || !allowed.includes(value as T)) {
    throw new Error(`${field} has unsupported value: ${String(value)}`);
  }
}

function parseInstant(value: unknown, field: string): number {
  if (typeof value !== "string" || !TIMEZONE_SUFFIX.test(value)) {
    throw new Error(`${field} must include an explicit timezone`);
  }
  const parsed = Date.parse(value);
  if (!Number.isFinite(parsed)) throw new Error(`${field} must be a valid timestamp`);
  return parsed;
}

function assertSafeIntegerInRange(value: unknown, min: number, max: number, field: string): asserts value is number {
  if (!Number.isSafeInteger(value) || (value as number) < min || (value as number) > max) {
    throw new Error(`${field} must be a safe integer in [${min}, ${max}]`);
  }
}

function assertFiniteInRange(value: unknown, min: number, max: number, field: string): asserts value is number {
  if (typeof value !== "number" || !Number.isFinite(value) || value < min || value > max) {
    throw new Error(`${field} must be a finite number in [${min}, ${max}]`);
  }
}

/** Stable JSON for hashing. It rejects undefined, non-finite numbers, and exotic values. */
export function canonicalLearningJson(value: unknown): string {
  if (value === null) return "null";
  if (typeof value === "string" || typeof value === "boolean") return JSON.stringify(value);
  if (typeof value === "number") {
    if (!Number.isFinite(value)) throw new Error("canonical JSON rejects non-finite numbers");
    return JSON.stringify(Object.is(value, -0) ? 0 : value);
  }
  if (Array.isArray(value)) return `[${value.map(canonicalLearningJson).join(",")}]`;
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => {
        if (record[key] === undefined) throw new Error("canonical JSON rejects undefined values");
        return `${JSON.stringify(key)}:${canonicalLearningJson(record[key])}`;
      })
      .join(",")}}`;
  }
  throw new Error(`canonical JSON rejects ${typeof value}`);
}

function sha256(value: unknown): string {
  return createHash("sha256").update(canonicalLearningJson(value)).digest("hex");
}

/** Upstream bounded queries can use this helper to bind the exact supplied rows. */
export function computeLearningPairDigest(pairs: readonly LearningOutcomePair[]): string {
  return sha256([...pairs].sort((left, right) => left.pairId.localeCompare(right.pairId)));
}

function sourceFingerprintMaterial(manifest: LearningEvidenceManifest): unknown {
  return {
    schemaVersion: manifest.schemaVersion,
    analysisVersion: manifest.analysisVersion,
    analysisId: manifest.analysisId,
    source: manifest.source,
    metricVersions: manifest.metricVersions,
    window: manifest.window,
    asOf: manifest.asOf,
    techniqueId: manifest.techniqueId,
    hypothesisFamily: manifest.hypothesisFamily,
    gates: manifest.gates,
    declaredConfounders: manifest.declaredConfounders,
  };
}

/** Header-only fingerprint: rowDigest binds rows, enabling a true zero-analysis no-op. */
export function computeLearningSourceFingerprint(manifest: LearningEvidenceManifest): string {
  validateLearningEvidenceHeader(manifest);
  return sha256(sourceFingerprintMaterial(manifest));
}

function validateLearningEvidenceHeader(manifest: LearningEvidenceManifest): void {
  assertObject(manifest, "manifest");
  assertExactKeys(
    manifest,
    [
      "schemaVersion",
      "analysisVersion",
      "analysisId",
      "source",
      "metricVersions",
      "window",
      "asOf",
      "techniqueId",
      "hypothesisFamily",
      "gates",
      "declaredConfounders",
      "pairs",
    ],
    "manifest",
  );
  if (manifest.schemaVersion !== LEARNING_EVIDENCE_SCHEMA_VERSION) {
    throw new Error(`Unsupported learning evidence schema version: ${String(manifest.schemaVersion)}`);
  }
  if (manifest.analysisVersion !== LEARNING_ANALYSIS_VERSION) {
    throw new Error(`Unsupported learning analysis version: ${String(manifest.analysisVersion)}`);
  }
  assertCanonicalIdentity(manifest.analysisId, "analysisId");
  assertObject(manifest.source, "source");
  assertExactKeys(
    manifest.source,
    ["snapshotId", "queryHash", "rowDigest", "declaredPairCount", "sourceKind"],
    "source",
  );
  assertCanonicalIdentity(manifest.source.snapshotId, "source.snapshotId");
  assertHash(manifest.source.queryHash, "source.queryHash");
  assertHash(manifest.source.rowDigest, "source.rowDigest");
  assertSafeIntegerInRange(manifest.source.declaredPairCount, 0, MAX_HARD_PAIRS, "source.declaredPairCount");
  if (manifest.source.sourceKind !== "local_owned_aggregate") {
    throw new Error("source.sourceKind must be local_owned_aggregate");
  }
  if (!Array.isArray(manifest.pairs)) throw new Error("pairs must be an array");
  if (manifest.pairs.length !== manifest.source.declaredPairCount) {
    throw new Error("source.declaredPairCount must equal pairs.length");
  }

  assertObject(manifest.metricVersions, "metricVersions");
  assertExactKeys(manifest.metricVersions, ["outcomeMetric", "techniqueExposure", "projectAllocation"], "metricVersions");
  assertVersion(manifest.metricVersions.outcomeMetric, "metricVersions.outcomeMetric");
  assertVersion(manifest.metricVersions.techniqueExposure, "metricVersions.techniqueExposure");
  assertVersion(manifest.metricVersions.projectAllocation, "metricVersions.projectAllocation");

  assertObject(manifest.window, "window");
  assertExactKeys(manifest.window, ["startInclusive", "endExclusive"], "window");
  const startMs = parseInstant(manifest.window.startInclusive, "window.startInclusive");
  const endMs = parseInstant(manifest.window.endExclusive, "window.endExclusive");
  const asOfMs = parseInstant(manifest.asOf, "asOf");
  if (startMs >= endMs) throw new Error("analysis window must have positive duration");
  if (asOfMs < startMs) throw new Error("asOf must not precede window.startInclusive");
  assertCanonicalIdentity(manifest.techniqueId, "techniqueId");

  assertObject(manifest.hypothesisFamily, "hypothesisFamily");
  assertExactKeys(
    manifest.hypothesisFamily,
    [
      "familyId",
      "hypothesisIndex",
      "hypothesesTested",
      "selectionPolicy",
      "correction",
      "familyWiseAlpha",
      "registeredAt",
    ],
    "hypothesisFamily",
  );
  assertCanonicalIdentity(manifest.hypothesisFamily.familyId, "hypothesisFamily.familyId");
  assertSafeIntegerInRange(manifest.hypothesisFamily.hypothesesTested, 1, 10_000, "hypothesisFamily.hypothesesTested");
  assertSafeIntegerInRange(
    manifest.hypothesisFamily.hypothesisIndex,
    1,
    manifest.hypothesisFamily.hypothesesTested,
    "hypothesisFamily.hypothesisIndex",
  );
  assertOneOf(manifest.hypothesisFamily.selectionPolicy, ["pre_registered", "all_reported"] as const, "hypothesisFamily.selectionPolicy");
  assertOneOf(manifest.hypothesisFamily.correction, ["none", "bonferroni"] as const, "hypothesisFamily.correction");
  assertFiniteInRange(manifest.hypothesisFamily.familyWiseAlpha, 0.000001, 0.5, "hypothesisFamily.familyWiseAlpha");
  const registeredMs = parseInstant(manifest.hypothesisFamily.registeredAt, "hypothesisFamily.registeredAt");
  if (registeredMs > startMs) throw new Error("hypothesis family must be registered no later than window start");
  if (manifest.hypothesisFamily.hypothesesTested > 1 && manifest.hypothesisFamily.correction !== "bonferroni") {
    throw new Error("multiple hypotheses require an explicit bonferroni correction; winner selection is forbidden");
  }

  assertObject(manifest.gates, "gates");
  assertExactKeys(
    manifest.gates,
    [
      "statisticalMinCompletePairs",
      "privacyMinCompletePairs",
      "minimumAttributionCoverage",
      "maxAbsoluteOutcome",
      "maxPairs",
      "maxCounterexamples",
      "maxRuntimeMs",
    ],
    "gates",
  );
  assertSafeIntegerInRange(manifest.gates.statisticalMinCompletePairs, 2, MAX_HARD_PAIRS, "gates.statisticalMinCompletePairs");
  assertSafeIntegerInRange(manifest.gates.privacyMinCompletePairs, 1, MAX_HARD_PAIRS, "gates.privacyMinCompletePairs");
  assertFiniteInRange(manifest.gates.minimumAttributionCoverage, 0, 1, "gates.minimumAttributionCoverage");
  assertFiniteInRange(manifest.gates.maxAbsoluteOutcome, Number.MIN_VALUE, Number.MAX_SAFE_INTEGER, "gates.maxAbsoluteOutcome");
  assertSafeIntegerInRange(manifest.gates.maxPairs, 1, MAX_HARD_PAIRS, "gates.maxPairs");
  assertSafeIntegerInRange(manifest.gates.maxCounterexamples, 0, 50, "gates.maxCounterexamples");
  assertSafeIntegerInRange(manifest.gates.maxRuntimeMs, 1, MAX_HARD_RUNTIME_MS, "gates.maxRuntimeMs");
  if (manifest.source.declaredPairCount > manifest.gates.maxPairs) {
    throw new Error(`pair budget exceeded: ${manifest.source.declaredPairCount} > ${manifest.gates.maxPairs}`);
  }

  if (!Array.isArray(manifest.declaredConfounders) || manifest.declaredConfounders.length > MAX_CONFOUNDERS) {
    throw new Error(`declaredConfounders must contain at most ${MAX_CONFOUNDERS} codes`);
  }
  for (const [index, confounder] of manifest.declaredConfounders.entries()) {
    assertCanonicalIdentity(confounder, `declaredConfounders[${index}]`);
  }
  if (new Set(manifest.declaredConfounders).size !== manifest.declaredConfounders.length) {
    throw new Error("declaredConfounders must be unique");
  }
}

function validateExposure(
  observation: LearningObservation,
  expectedState: "exposed" | "control",
  techniqueId: string,
  field: string,
): void {
  assertObject(observation.exposure, `${field}.exposure`);
  assertExactKeys(
    observation.exposure,
    ["state", "techniqueId", "evidenceSource", "contentOrigin", "recordedAt"],
    `${field}.exposure`,
  );
  assertOneOf(observation.exposure.state, ["exposed", "control"] as const, `${field}.exposure.state`);
  if (observation.exposure.state !== expectedState) {
    throw new Error(`${field} must carry an explicit ${expectedState} exposure record`);
  }
  if (expectedState === "exposed") {
    assertCanonicalIdentity(observation.exposure.techniqueId, `${field}.exposure.techniqueId`);
    if (observation.exposure.techniqueId !== techniqueId) {
      throw new Error(`${field} exposed technique does not match manifest.techniqueId`);
    }
  } else if (observation.exposure.techniqueId !== null) {
    throw new Error(`${field} control must explicitly declare techniqueId null`);
  }
  assertOneOf(
    observation.exposure.evidenceSource,
    ["operator_declared", "machine_receipt"] as const,
    `${field}.exposure.evidenceSource`,
  );
  assertOneOf(
    observation.exposure.contentOrigin,
    ["operator", "machine_observation"] as const,
    `${field}.exposure.contentOrigin`,
  );
  const recordedMs = parseInstant(observation.exposure.recordedAt, `${field}.exposure.recordedAt`);
  const startedMs = parseInstant(observation.workStartedAt, `${field}.workStartedAt`);
  if (recordedMs > startedMs) {
    throw new Error(`${field} exposure is retrospective; exposure must be explicit before work starts`);
  }
}

function validateObservation(
  observation: LearningObservation,
  expectedState: "exposed" | "control",
  manifest: LearningEvidenceManifest,
  field: string,
  observationIds: Set<string>,
): void {
  assertObject(observation, field);
  assertExactKeys(
    observation,
    ["observationId", "workStartedAt", "outcomeObservedAt", "cohort", "exposure", "outcome", "attribution"],
    field,
  );
  assertCanonicalIdentity(observation.observationId, `${field}.observationId`);
  if (observationIds.has(observation.observationId)) {
    throw new Error(`duplicate observation id: ${observation.observationId}`);
  }
  observationIds.add(observation.observationId);
  const startedMs = parseInstant(observation.workStartedAt, `${field}.workStartedAt`);
  const outcomeMs = parseInstant(observation.outcomeObservedAt, `${field}.outcomeObservedAt`);
  const windowStart = Date.parse(manifest.window.startInclusive);
  const windowEnd = Date.parse(manifest.window.endExclusive);
  const asOfMs = Date.parse(manifest.asOf);
  if (startedMs < windowStart || startedMs >= windowEnd || startedMs > asOfMs) {
    throw new Error(`${field}.workStartedAt is outside the declared analysis window/as-of`);
  }
  if (outcomeMs < startedMs || outcomeMs > asOfMs) {
    throw new Error(`${field}.outcomeObservedAt must be between work start and as-of`);
  }
  validateExposure(observation, expectedState, manifest.techniqueId, field);

  assertObject(observation.cohort, `${field}.cohort`);
  assertExactKeys(observation.cohort, COHORT_KEYS, `${field}.cohort`);
  for (const key of COHORT_KEYS) assertCanonicalIdentity(observation.cohort[key], `${field}.cohort.${key}`);

  assertObject(observation.outcome, `${field}.outcome`);
  assertExactKeys(
    observation.outcome,
    ["metricId", "metricVersion", "unit", "direction", "value"],
    `${field}.outcome`,
  );
  assertCanonicalIdentity(observation.outcome.metricId, `${field}.outcome.metricId`);
  assertVersion(observation.outcome.metricVersion, `${field}.outcome.metricVersion`);
  if (observation.outcome.metricVersion !== manifest.metricVersions.outcomeMetric) {
    throw new Error(`${field}.outcome.metricVersion is incomparable with manifest.metricVersions.outcomeMetric`);
  }
  assertCanonicalIdentity(observation.outcome.unit, `${field}.outcome.unit`);
  assertOneOf(observation.outcome.direction, ["higher_is_better", "lower_is_better"] as const, `${field}.outcome.direction`);
  if (observation.outcome.value !== null) {
    if (!Number.isFinite(observation.outcome.value) || Math.abs(observation.outcome.value) > manifest.gates.maxAbsoluteOutcome) {
      throw new Error(`${field}.outcome.value must be finite and within maxAbsoluteOutcome`);
    }
  }

  assertObject(observation.attribution, `${field}.attribution`);
  assertExactKeys(
    observation.attribution,
    ["method", "projectAllocation", "coverage"],
    `${field}.attribution`,
  );
  assertOneOf(
    observation.attribution.method,
    ["direct", "deterministic_linkage", "inferred", "none"] as const,
    `${field}.attribution.method`,
  );
  assertOneOf(
    observation.attribution.projectAllocation,
    ["exact", "apportioned", "unallocated", "unknown"] as const,
    `${field}.attribution.projectAllocation`,
  );
  assertFiniteInRange(observation.attribution.coverage, 0, 1, `${field}.attribution.coverage`);
  if (observation.attribution.method === "none" && observation.attribution.coverage !== 0) {
    throw new Error(`${field} none attribution must have zero coverage`);
  }
  if (
    (observation.attribution.projectAllocation === "unallocated" || observation.attribution.projectAllocation === "unknown") &&
    observation.attribution.method !== "none"
  ) {
    throw new Error(`${field} unallocated/unknown project evidence cannot claim an attribution method`);
  }
}

function cohortKey(cohort: LearningCohort): string {
  return COHORT_KEYS.map((key) => cohort[key]).join("\u001f");
}

function assertComparablePair(pair: LearningOutcomePair, field: string): void {
  for (const key of COHORT_KEYS) {
    if (pair.exposed.cohort[key] !== pair.control.cohort[key]) {
      throw new Error(`${field} has incomparable ${key} cohorts`);
    }
  }
  for (const key of ["metricId", "metricVersion", "unit", "direction"] as const) {
    if (pair.exposed.outcome[key] !== pair.control.outcome[key]) {
      throw new Error(`${field} has incomparable outcome ${key}`);
    }
  }
}

/** Full validation is intentionally skipped on a trusted unchanged fingerprint. */
export function validateLearningEvidenceManifest(manifest: LearningEvidenceManifest): void {
  validateLearningEvidenceHeader(manifest);
  if (computeLearningPairDigest(manifest.pairs) !== manifest.source.rowDigest) {
    throw new Error("source.rowDigest does not bind the exact supplied pair rows");
  }
  const pairIds = new Set<string>();
  const observationIds = new Set<string>();
  for (const [index, pair] of manifest.pairs.entries()) {
    const field = `pairs[${index}]`;
    assertObject(pair, field);
    assertExactKeys(pair, ["pairId", "exposed", "control"], field);
    assertCanonicalIdentity(pair.pairId, `${field}.pairId`);
    if (pairIds.has(pair.pairId)) throw new Error(`duplicate pair id: ${pair.pairId}`);
    pairIds.add(pair.pairId);
    validateObservation(pair.exposed, "exposed", manifest, `${field}.exposed`, observationIds);
    validateObservation(pair.control, "control", manifest, `${field}.control`, observationIds);
    assertComparablePair(pair, field);
  }
}

function kahanMean(values: readonly number[]): number | null {
  if (values.length === 0) return null;
  let sum = 0;
  let compensation = 0;
  for (const value of values) {
    const adjusted = value - compensation;
    const next = sum + adjusted;
    compensation = next - sum - adjusted;
    sum = next;
  }
  const mean = sum / values.length;
  if (!Number.isFinite(mean)) throw new Error("effect aggregation exceeded finite numeric range");
  return Object.is(mean, -0) ? 0 : mean;
}

function sign(value: number | null, epsilon = 1e-12): -1 | 0 | 1 {
  if (value === null || Math.abs(value) <= epsilon) return 0;
  return value > 0 ? 1 : -1;
}

/** Acklam-style inverse standard-normal CDF approximation. */
function inverseStandardNormal(probability: number): number {
  if (!(probability > 0 && probability < 1)) throw new Error("normal quantile probability must be in (0, 1)");
  const a = [-39.69683028665376, 220.9460984245205, -275.9285104469687, 138.357751867269, -30.66479806614716, 2.506628277459239];
  const b = [-54.47609879822406, 161.5858368580409, -155.6989798598866, 66.80131188771972, -13.28068155288572];
  const c = [-0.007784894002430293, -0.3223964580411365, -2.400758277161838, -2.549732539343734, 4.374664141464968, 2.938163982698783];
  const d = [0.007784695709041462, 0.3224671290700398, 2.445134137142996, 3.754408661907416];
  const low = 0.02425;
  const high = 1 - low;
  if (probability < low) {
    const q = Math.sqrt(-2 * Math.log(probability));
    return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  if (probability > high) {
    const q = Math.sqrt(-2 * Math.log(1 - probability));
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) /
      ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1);
  }
  const q = probability - 0.5;
  const r = q * q;
  return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q /
    (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1);
}

function adjustedAlpha(family: LearningHypothesisFamily): number {
  return family.correction === "bonferroni"
    ? family.familyWiseAlpha / family.hypothesesTested
    : family.familyWiseAlpha;
}

function emptyEffect(family: LearningHypothesisFamily, crude: number | null, stratified: number | null): LearningEffectEstimate {
  const alpha = adjustedAlpha(family);
  return {
    method: "paired_mean_difference",
    uncertaintyMethod: "bonferroni_adjusted_normal_approximation",
    rawEstimate: null,
    directionAdjustedEstimate: null,
    standardError: null,
    lowerBound: null,
    upperBound: null,
    familyWiseConfidenceLevel: 1 - family.familyWiseAlpha,
    perHypothesisConfidenceLevel: 1 - alpha,
    associationDirection: "not_estimable",
    crudePairWeightedEstimate: crude,
    equalStratumEstimate: stratified,
  };
}

function createSkillReviewArtifact(): SkillCandidateReviewArtifact {
  return {
    artifactType: "review_artifact_only",
    currentState: "observed",
    lifecycle: SKILL_CANDIDATE_LIFECYCLE,
    inventoryDispositions: LEARNING_INVENTORY_DISPOSITIONS,
    inventoryDisposition: "insufficient_evidence",
    containsExecutableInstructions: false,
    publicationAuthorized: false,
    installationAuthorized: false,
    ownerApprovalRequired: true,
    independentVerificationRequired: true,
    openWebOrModelTextMayGate: false,
    prohibitedWriteTargets: PROHIBITED_WRITE_TARGETS,
  };
}

function packetWithoutFingerprint(packet: Omit<LearningEvidencePacket, "packetFingerprint">): LearningEvidencePacket {
  return { ...packet, packetFingerprint: sha256(packet) };
}

export function compileLearningEvidencePacket(
  manifest: LearningEvidenceManifest,
  options: CompileLearningEvidenceOptions = {},
): LearningEvidenceRun {
  const sourceFingerprint = computeLearningSourceFingerprint(manifest);
  if (options.previousSourceFingerprint !== undefined && options.previousSourceFingerprint !== null) {
    assertHash(options.previousSourceFingerprint, "previousSourceFingerprint");
    if (options.previousSourceFingerprint === sourceFingerprint) {
      return { status: "unchanged", sourceFingerprint, analysisWorkUnits: 0, packet: null };
    }
  }

  validateLearningEvidenceManifest(manifest);
  const startedAt = performance.now();
  const sortedPairs = [...manifest.pairs].sort((left, right) => left.pairId.localeCompare(right.pairId));
  const differences: { pairId: string; difference: number; stratum: string }[] = [];
  const methodCounts: Record<LearningAttribution["method"], number> = {
    direct: 0,
    deterministic_linkage: 0,
    inferred: 0,
    none: 0,
  };
  const allocationMix: Record<LearningAttribution["projectAllocation"], number> = {
    exact: 0,
    apportioned: 0,
    unallocated: 0,
    unknown: 0,
  };
  let attributionCoverageSum = 0;
  let unknownOutcomeCount = 0;
  for (const [index, pair] of sortedPairs.entries()) {
    if ((index & 127) === 0 && performance.now() - startedAt > manifest.gates.maxRuntimeMs) {
      throw new Error(`analysis time budget exceeded: ${manifest.gates.maxRuntimeMs}ms`);
    }
    for (const observation of [pair.exposed, pair.control]) {
      methodCounts[observation.attribution.method] += 1;
      allocationMix[observation.attribution.projectAllocation] += 1;
      attributionCoverageSum += observation.attribution.coverage;
      if (observation.outcome.value === null) unknownOutcomeCount += 1;
    }
    if (pair.exposed.outcome.value !== null && pair.control.outcome.value !== null) {
      const difference = pair.exposed.outcome.value - pair.control.outcome.value;
      if (!Number.isFinite(difference) || Math.abs(difference) > manifest.gates.maxAbsoluteOutcome * 2) {
        throw new Error(`pair ${pair.pairId} produced an unsafe outcome difference`);
      }
      differences.push({ pairId: pair.pairId, difference, stratum: cohortKey(pair.exposed.cohort) });
    }
  }

  const observationCount = sortedPairs.length * 2;
  const coverage = observationCount === 0 ? 0 : attributionCoverageSum / observationCount;
  if (!Number.isFinite(coverage)) throw new Error("attribution coverage aggregation is not finite");
  const rawDifferences = differences.map((row) => row.difference);
  const crudeEstimate = kahanMean(rawDifferences);
  const strata = new Map<string, number[]>();
  for (const row of differences) {
    const existing = strata.get(row.stratum) ?? [];
    existing.push(row.difference);
    strata.set(row.stratum, existing);
  }
  const stratumMeans = [...strata.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([, values]) => kahanMean(values) as number);
  const equalStratumEstimate = kahanMean(stratumMeans);
  const simpsonReversal = sign(crudeEstimate) !== 0 && sign(equalStratumEstimate) !== 0 && sign(crudeEstimate) !== sign(equalStratumEstimate);

  const statisticalMinimumMet = differences.length >= manifest.gates.statisticalMinCompletePairs;
  const privacyMinimumMet = differences.length >= manifest.gates.privacyMinCompletePairs;
  const reasons = new Set<LearningNotEstimableReason>();
  if (!statisticalMinimumMet) reasons.add("insufficient_statistical_sample");
  if (!privacyMinimumMet) reasons.add("privacy_minimum_not_met");
  if (differences.length !== sortedPairs.length) reasons.add("incomplete_outcome_pairs");
  if (coverage < manifest.gates.minimumAttributionCoverage) reasons.add("attribution_coverage_below_minimum");
  if (simpsonReversal) reasons.add("simpson_reversal");
  const notEstimableReasons = LEARNING_NOT_ESTIMABLE_REASONS.filter((reason) => reasons.has(reason));

  let effect = emptyEffect(manifest.hypothesisFamily, crudeEstimate, equalStratumEstimate);
  if (notEstimableReasons.length === 0 && crudeEstimate !== null && differences.length >= 2) {
    const squaredDeviations = rawDifferences.map((value) => (value - crudeEstimate) ** 2);
    const variance = (kahanMean(squaredDeviations) as number) * differences.length / (differences.length - 1);
    const standardError = Math.sqrt(variance / differences.length);
    if (!Number.isFinite(standardError)) throw new Error("effect uncertainty is not finite");
    const alpha = adjustedAlpha(manifest.hypothesisFamily);
    const critical = inverseStandardNormal(1 - alpha / 2);
    const lower = crudeEstimate - critical * standardError;
    const upper = crudeEstimate + critical * standardError;
    const directionMultiplier = sortedPairs[0]?.exposed.outcome.direction === "lower_is_better" ? -1 : 1;
    const adjusted = crudeEstimate * directionMultiplier;
    effect = {
      method: "paired_mean_difference",
      uncertaintyMethod: "bonferroni_adjusted_normal_approximation",
      rawEstimate: crudeEstimate,
      directionAdjustedEstimate: adjusted,
      standardError,
      lowerBound: lower,
      upperBound: upper,
      familyWiseConfidenceLevel: 1 - manifest.hypothesisFamily.familyWiseAlpha,
      perHypothesisConfidenceLevel: 1 - alpha,
      associationDirection: sign(adjusted) > 0 ? "favors_exposed" : sign(adjusted) < 0 ? "favors_control" : "no_observed_difference",
      crudePairWeightedEstimate: crudeEstimate,
      equalStratumEstimate,
    };
  }

  const outcomeMultiplier = sortedPairs[0]?.exposed.outcome.direction === "lower_is_better" ? -1 : 1;
  const counterexampleDirection = sign(crudeEstimate === null ? null : crudeEstimate * outcomeMultiplier);
  const counterexampleIds = counterexampleDirection === 0
    ? []
    : differences
        .filter((row) => sign(row.difference * outcomeMultiplier) === -counterexampleDirection)
        .map((row) => row.pairId)
        .sort();
  const disclosedCounterexamples = privacyMinimumMet
    ? counterexampleIds.slice(0, manifest.gates.maxCounterexamples)
    : [];
  const confounders = [...new Set([
    "nonrandom_technique_assignment",
    "unmeasured_context",
    "observational_design",
    ...manifest.declaredConfounders,
  ])].sort();
  const analysisWorkUnits = sortedPairs.length;
  const packet = packetWithoutFingerprint({
    schemaVersion: LEARNING_EVIDENCE_SCHEMA_VERSION,
    analysisVersion: LEARNING_ANALYSIS_VERSION,
    sourceFingerprint,
    analysisId: manifest.analysisId,
    source: manifest.source,
    metricVersions: manifest.metricVersions,
    window: manifest.window,
    asOf: manifest.asOf,
    techniqueId: manifest.techniqueId,
    hypothesisFamily: {
      ...manifest.hypothesisFamily,
      adjustedAlpha: adjustedAlpha(manifest.hypothesisFamily),
    },
    sample: {
      exposedCount: sortedPairs.length,
      controlCount: sortedPairs.length,
      completePairCount: differences.length,
      incompletePairCount: sortedPairs.length - differences.length,
      statisticalMinimum: manifest.gates.statisticalMinCompletePairs,
      privacyMinimum: manifest.gates.privacyMinCompletePairs,
      statisticalMinimumMet,
      privacyMinimumMet,
    },
    attribution: {
      observationCount,
      coverage,
      methods: methodCounts,
      allocationMix,
      unallocatedCount: allocationMix.unallocated,
      unknownCount: allocationMix.unknown,
      unknownOutcomeCount,
    },
    effect,
    claimClass: notEstimableReasons.length === 0 ? "observational_association" : "not_estimable",
    causalClaim: false,
    prescriptiveClaim: false,
    notEstimableReasons,
    confounders,
    counterexamples: {
      count: counterexampleIds.length,
      pairIds: disclosedCounterexamples,
      privacySuppressed: !privacyMinimumMet && counterexampleIds.length > 0,
    },
    skillCandidateReview: createSkillReviewArtifact(),
    budgets: {
      pairLimit: manifest.gates.maxPairs,
      runtimeLimitMs: manifest.gates.maxRuntimeMs,
      analysisWorkUnits,
      continuousLoop: false,
      modelCalls: 0,
    },
  });
  return { status: "computed", sourceFingerprint, analysisWorkUnits, packet };
}

/**
 * CLI outputs are review artifacts only and must remain under the caller's
 * explicit workspace root. This prevents skill, memory, mounted fleet, and
 * arbitrary home-directory writes by construction.
 */
export function assertLearningReviewOutputPath(outputPath: string, workspaceRoot = process.cwd()): string {
  if (!outputPath || outputPath.includes("\0")) throw new Error("output path must be a non-empty local path");
  if (/^[a-z][a-z0-9+.-]*:\/\//i.test(outputPath)) throw new Error("network/URL output targets are forbidden");
  const root = resolve(workspaceRoot);
  const resolved = resolve(root, outputPath);
  const relativePath = relative(root, resolved);
  if (relativePath === "" || relativePath === ".." || relativePath.startsWith(`..${sep}`) || isAbsolute(relativePath)) {
    throw new Error("learning evidence output must be a file inside the explicit workspace root");
  }
  const base = resolved.split(sep).at(-1)?.toLowerCase();
  if (base === "skill.md" || base === "memory.md") {
    throw new Error("learning evidence output cannot target executable skills or global memory");
  }
  const home = resolve(homedir());
  const prohibited = [
    resolve(home, ".codex/skills"),
    resolve(home, ".claude/skills"),
    resolve(home, ".agents/skills"),
    resolve(home, ".codex/memories"),
    resolve(home, ".claude/memory"),
  ];
  if (prohibited.some((target) => resolved === target || resolved.startsWith(`${target}${sep}`))) {
    throw new Error("learning evidence output targets a prohibited skill or memory tree");
  }
  return resolved;
}
