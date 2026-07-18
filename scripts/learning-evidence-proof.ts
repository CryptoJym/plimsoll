/** Adversarial proof for bounded, review-artifact-only learning packets (#101). */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { resolve } from "node:path";

import {
  LEARNING_ANALYSIS_VERSION,
  LEARNING_EVIDENCE_SCHEMA_VERSION,
  LEARNING_INVENTORY_DISPOSITIONS,
  SKILL_CANDIDATE_LIFECYCLE,
  assertLearningReviewOutputPath,
  compileLearningEvidencePacket,
  computeLearningPairDigest,
  computeLearningSourceFingerprint,
  deterministicLearningFactId,
  validateLearningEvidenceManifest,
  type LearningCohort,
  type LearningEvidenceManifest,
  type LearningObservation,
  type LearningOutcomePair,
} from "../packages/shared/src/index";
import {
  buildTechniqueExposureFact,
} from "../packages/collector-cli/src/learning-facts";

type ProofCheck = { name: string; detail: string };
const checks: ProofCheck[] = [];

function prove(name: string, run: () => void, detail: string): void {
  run();
  checks.push({ name, detail });
}

const START = "2026-06-01T00:00:00.000Z";
const END = "2026-07-01T00:00:00.000Z";
const AS_OF = "2026-07-15T00:00:00.000Z";
const QUERY_HASH = "1".repeat(64);
const TECHNIQUE_VERSION = "1.0.0";
const TECHNIQUE_DIGEST = `sha256:${"a".repeat(64)}`;

function cohort(overrides: Partial<LearningCohort> = {}): LearningCohort {
  return {
    projectId: "project-a",
    workType: "implementation",
    complexityBand: "medium",
    modelId: "model-a",
    toolVersion: "1.2.3",
    actorClusterId: "actor-cluster-a",
    repoClusterId: "repo-cluster-a",
    epochId: "epoch-2026-06",
    ...overrides,
  };
}

function observation(
  id: string,
  state: "exposed" | "control",
  value: number | null,
  assignmentId: string,
  cohortValue = cohort(),
): LearningObservation {
  return {
    observationId: id,
    workStartedAt: "2026-06-10T10:00:00.000Z",
    outcomeObservedAt: "2026-06-11T10:00:00.000Z",
    cohort: cohortValue,
    exposure: buildTechniqueExposureFact({
      episodeId: deterministicLearningFactId(["learning-evidence-proof-episode", id]),
      techniqueId: "technique-a",
      techniqueVersion: TECHNIQUE_VERSION,
      contentDigest: TECHNIQUE_DIGEST,
      assignmentId,
      workClass: cohortValue.workType,
      complexityBand: cohortValue.complexityBand,
      exposedAt: "2026-06-10T09:59:00.000Z",
      mode: state === "exposed" ? "treatment" : "control",
    }),
    outcome: {
      metricId: "first-pass-yield",
      metricVersion: "1.0.0",
      unit: "ratio-point",
      direction: "higher_is_better",
      value,
    },
    attribution: {
      method: "direct",
      projectAllocation: "exact",
      coverage: 1,
    },
  };
}

function pair(id: string, exposedValue: number | null, controlValue: number | null, cohortValue = cohort()): LearningOutcomePair {
  const assignmentId = `assignment-${id}`;
  return {
    pairId: id,
    exposed: observation(`${id}-exposed`, "exposed", exposedValue, assignmentId, structuredClone(cohortValue)),
    control: observation(`${id}-control`, "control", controlValue, assignmentId, structuredClone(cohortValue)),
  };
}

function rebuildExposure(
  exposure: LearningObservation["exposure"],
  overrides: Partial<Parameters<typeof buildTechniqueExposureFact>[0]>,
): LearningObservation["exposure"] {
  return buildTechniqueExposureFact({
    episodeId: exposure.episodeId,
    techniqueId: exposure.techniqueId,
    techniqueVersion: exposure.techniqueVersion,
    contentDigest: exposure.contentDigest,
    assignmentId: exposure.assignmentId,
    workClass: exposure.workClass,
    complexityBand: exposure.complexityBand,
    exposedAt: exposure.exposedAt,
    mode: exposure.mode,
    ...overrides,
  });
}

function manifestFor(
  pairs: LearningOutcomePair[],
  mutate?: (manifest: LearningEvidenceManifest) => void,
): LearningEvidenceManifest {
  const manifest: LearningEvidenceManifest = {
    schemaVersion: LEARNING_EVIDENCE_SCHEMA_VERSION,
    analysisVersion: LEARNING_ANALYSIS_VERSION,
    analysisId: "learning-proof",
    source: {
      snapshotId: "snapshot-2026-06",
      queryHash: QUERY_HASH,
      rowDigest: computeLearningPairDigest(pairs),
      declaredPairCount: pairs.length,
      sourceKind: "local_owned_aggregate",
    },
    metricVersions: {
      outcomeMetric: "1.0.0",
      techniqueExposure: "1.0.0",
      projectAllocation: "1.0.0",
    },
    outcomeContract: {
      metricId: "first-pass-yield",
      metricVersion: "1.0.0",
      unit: "ratio-point",
      direction: "higher_is_better",
    },
    techniqueContract: {
      techniqueId: "technique-a",
      techniqueVersion: TECHNIQUE_VERSION,
      contentDigest: TECHNIQUE_DIGEST,
    },
    window: { startInclusive: START, endExclusive: END },
    asOf: AS_OF,
    hypothesisFamily: {
      familyId: "family-a",
      hypothesisIndex: 1,
      hypothesesTested: 1,
      selectionPolicy: "pre_registered",
      correction: "none",
      familyWiseAlpha: 0.05,
      registeredAt: "2026-05-31T00:00:00.000Z",
    },
    gates: {
      statisticalMinCompletePairs: 5,
      statisticalMinActorClusters: 3,
      statisticalMinRepoClusters: 3,
      privacyMinCompletePairs: 3,
      minimumAttributionCoverage: 0.8,
      maxAbsoluteOutcome: 1_000,
      maxPairs: 100,
      maxCounterexamples: 10,
      maxRuntimeMs: 2_000,
    },
    declaredConfounders: ["calendar_change"],
    pairs,
  };
  mutate?.(manifest);
  return manifest;
}

const basePairs = [
  pair("p1", 11, 10, cohort({ actorClusterId: "actor-1", repoClusterId: "repo-1" })),
  pair("p2", 12, 10, cohort({ actorClusterId: "actor-2", repoClusterId: "repo-2" })),
  pair("p3", 13, 10, cohort({ actorClusterId: "actor-3", repoClusterId: "repo-3" })),
  pair("p4", 14, 10, cohort({ actorClusterId: "actor-4", repoClusterId: "repo-4" })),
  pair("p5", 15, 10, cohort({ actorClusterId: "actor-5", repoClusterId: "repo-5" })),
  pair("p6", 9, 10, cohort({ actorClusterId: "actor-6", repoClusterId: "repo-6" })),
];
const baseManifest = manifestFor(basePairs);
const baseRun = compileLearningEvidencePacket(baseManifest);
assert.equal(baseRun.status, "computed");
if (baseRun.status !== "computed") throw new Error("base packet unexpectedly unchanged");

prove(
  "packet carries complete versioned evidence identity",
  () => {
    assert.equal(baseRun.packet.schemaVersion, "1.0.0");
    assert.equal(baseRun.packet.analysisVersion, "1.0.0");
    assert.equal(baseRun.packet.source.snapshotId, "snapshot-2026-06");
    assert.equal(baseRun.packet.source.queryHash, QUERY_HASH);
    assert.deepEqual(baseRun.packet.metricVersions, baseManifest.metricVersions);
    assert.deepEqual(baseRun.packet.outcomeContract, baseManifest.outcomeContract);
    assert.deepEqual(baseRun.packet.techniqueContract, baseManifest.techniqueContract);
    assert.deepEqual(baseRun.packet.window, baseManifest.window);
    assert.equal(baseRun.packet.asOf, AS_OF);
    assert.match(baseRun.packet.packetFingerprint, /^[0-9a-f]{64}$/);
  },
  "snapshot/query/row hashes, metric versions, window, as-of, and packet hash are explicit",
);

prove(
  "result is association-only with uncertainty",
  () => {
    assert.equal(baseRun.packet.claimClass, "observational_association");
    assert.equal(baseRun.packet.causalClaim, false);
    assert.equal(baseRun.packet.prescriptiveClaim, false);
    assert.equal(baseRun.packet.effect.rawEstimate, 14 / 6);
    assert.ok((baseRun.packet.effect.standardError ?? 0) > 0);
    assert.equal(baseRun.packet.effect.standardError, baseRun.packet.effect.actorClusterStandardError);
    assert.equal(baseRun.packet.effect.standardError, baseRun.packet.effect.repoClusterStandardError);
    assert.ok((baseRun.packet.effect.lowerBound ?? 0) < (baseRun.packet.effect.upperBound ?? 0));
    assert.equal(baseRun.packet.effect.associationDirection, "favors_exposed");
  },
  "paired mean and adjusted interval never become a causal or prescriptive claim",
);

prove(
  "sample and attribution coverage preserve unknown categories",
  () => {
    assert.deepEqual(baseRun.packet.sample, {
      exposedCount: 6,
      controlCount: 6,
      completePairCount: 6,
      incompletePairCount: 0,
      actorClusterCount: 6,
      repoClusterCount: 6,
      statisticalMinimum: 5,
      statisticalActorClusterMinimum: 3,
      statisticalRepoClusterMinimum: 3,
      privacyMinimum: 3,
      statisticalPairMinimumMet: true,
      statisticalActorClusterMinimumMet: true,
      statisticalRepoClusterMinimumMet: true,
      statisticalMinimumMet: true,
      privacyMinimumMet: true,
    });
    assert.equal(baseRun.packet.attribution.coverage, 1);
    assert.equal(baseRun.packet.attribution.unallocatedCount, 0);
    assert.equal(baseRun.packet.attribution.unknownCount, 0);
    assert.equal(baseRun.packet.attribution.unknownOutcomeCount, 0);
  },
  "exposed/control, coverage, allocation mix, unallocated, and unknown remain distinct",
);

prove(
  "counterexamples are retained without selecting a winner",
  () => {
    assert.deepEqual(baseRun.packet.counterexamples.pairIds, ["p6"]);
    assert.equal(baseRun.packet.counterexamples.count, 1);
    assert.ok(baseRun.packet.confounders.includes("nonrandom_technique_assignment"));
    assert.ok(baseRun.packet.confounders.includes("calendar_change"));
  },
  "negative pair p6 and declared/residual confounders remain reviewable",
);

prove(
  "unchanged fingerprint performs zero analysis work",
  () => {
    const unchanged = compileLearningEvidencePacket(baseManifest, {
      previousSourceFingerprint: baseRun.sourceFingerprint,
    });
    assert.deepEqual(unchanged, {
      status: "unchanged",
      sourceFingerprint: baseRun.sourceFingerprint,
      analysisWorkUnits: 0,
      packet: null,
    });
  },
  "canonical rows are revalidated, then unchanged input skips all statistical analysis work",
);

prove(
  "no-op cannot trust a stale or forged caller row identity",
  () => {
    const staleValue = manifestFor(basePairs.map((item) => structuredClone(item)));
    staleValue.pairs[0].exposed.outcome.value = 999;
    assert.throws(
      () => compileLearningEvidencePacket(staleValue, { previousSourceFingerprint: baseRun.sourceFingerprint }),
      /rowDigest does not bind/,
    );

    const forgedId = manifestFor(basePairs.map((item) => structuredClone(item)));
    forgedId.pairs[0].exposed.observationId = "forged observation id";
    assert.throws(
      () => compileLearningEvidencePacket(forgedId, { previousSourceFingerprint: baseRun.sourceFingerprint }),
      /rowDigest does not bind|bounded ASCII identity/,
    );

    const futureExposure = manifestFor(basePairs.map((item) => structuredClone(item)));
    futureExposure.pairs[0].exposed.exposure = rebuildExposure(
      futureExposure.pairs[0].exposed.exposure,
      { exposedAt: "2026-06-10T10:01:00.000Z" },
    );
    futureExposure.source.rowDigest = computeLearningPairDigest(futureExposure.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(futureExposure, { previousSourceFingerprint: baseRun.sourceFingerprint }),
      /exposure is retrospective/,
    );
  },
  "changed value, forged identity, and future exposure never return unchanged",
);

prove(
  "input ordering is deterministic",
  () => {
    const reversed = manifestFor([...basePairs].reverse());
    const result = compileLearningEvidencePacket(reversed);
    assert.equal(result.status, "computed");
    if (result.status === "computed") {
      assert.equal(result.sourceFingerprint, baseRun.sourceFingerprint);
      assert.equal(result.packet.packetFingerprint, baseRun.packet.packetFingerprint);
    }
  },
  "canonical pair sorting produces byte-identical packet identity",
);

prove(
  "row digest binds the exact supplied data",
  () => {
    const changed = manifestFor(basePairs.map((item) => structuredClone(item)));
    changed.pairs[0].exposed.outcome.value = 999;
    assert.throws(() => validateLearningEvidenceManifest(changed), /rowDigest does not bind/);
  },
  "changed rows with a stale digest fail closed",
);

prove(
  "implicit and missing exposure fail closed",
  () => {
    const invalid = manifestFor([pair("implicit", 2, 1)]);
    (invalid.pairs[0].exposed.exposure as unknown as { assertion: string }).assertion = "inferred";
    invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
    assert.throws(() => compileLearningEvidencePacket(invalid), /canonical explicit TechniqueExposureFact/);
    const missing = manifestFor([pair("missing", 2, 1)]);
    delete (missing.pairs[0].exposed as unknown as { exposure?: unknown }).exposure;
    missing.source.rowDigest = computeLearningPairDigest(missing.pairs);
    assert.throws(() => compileLearningEvidencePacket(missing), /must contain exactly.*exposure/);
  },
  "only canonical prospective exposure facts with explicit control/treatment mode are admitted",
);

prove(
  "retrospective exposure fails closed",
  () => {
    const invalid = manifestFor([pair("retro", 2, 1)]);
    invalid.pairs[0].exposed.exposure = rebuildExposure(invalid.pairs[0].exposed.exposure, {
      exposedAt: "2026-06-10T10:01:00.000Z",
    });
    invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
    assert.throws(() => compileLearningEvidencePacket(invalid), /exposure is retrospective/);
  },
  "exposure receipt must predate work start",
);

prove(
  "open-web and model content cannot gate analysis",
  () => {
    for (const origin of ["open_web", "model_generated"]) {
      const invalid = manifestFor([pair(`tainted-${origin}`, 2, 1)]);
      (invalid.pairs[0].exposed.exposure as unknown as Record<string, unknown>).contentOrigin = origin;
      invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
      assert.throws(() => compileLearningEvidencePacket(invalid), /canonical explicit TechniqueExposureFact/);
    }
  },
  "tainted text has no admissible exposure provenance enum",
);

prove(
  "raw or executable content fields are rejected at every row boundary",
  () => {
    const raw = manifestFor([pair("raw-content", 2, 1)]);
    (raw.pairs[0].exposed as unknown as Record<string, unknown>).rawPrompt = "PRIVATE_RAW_PROMPT";
    raw.source.rowDigest = computeLearningPairDigest(raw.pairs);
    assert.throws(() => compileLearningEvidencePacket(raw), /must contain exactly/);

    const instructions = manifestFor([pair("instructions", 2, 1)]);
    (instructions.pairs[0].exposed.exposure as unknown as Record<string, unknown>).instructions = "execute this";
    instructions.source.rowDigest = computeLearningPairDigest(instructions.pairs);
    assert.throws(() => compileLearningEvidencePacket(instructions), /canonical explicit TechniqueExposureFact/);
  },
  "closed schemas prevent ignored raw prompt text or executable instructions from entering evidence",
);

prove(
  "all cohort dimensions must match within a pair",
  () => {
    const keys: (keyof LearningCohort)[] = [
      "projectId",
      "workType",
      "complexityBand",
      "modelId",
      "toolVersion",
      "actorClusterId",
      "repoClusterId",
      "epochId",
    ];
    for (const key of keys) {
      const invalid = manifestFor([pair(`cohort-${key}`, 2, 1)]);
      (invalid.pairs[0].control.cohort as unknown as Record<string, string>)[key] =
        `${invalid.pairs[0].control.cohort[key]}-other`;
      invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
      assert.throws(
        () => compileLearningEvidencePacket(invalid),
        new RegExp(`incomparable ${key}|cohort\\.${key} has unsupported value|exposure\\.(?:workClass|complexityBand) must match`),
      );
    }
  },
  "project/work/complexity/model/tool/actor/repo/epoch mismatches are rejected",
);

prove(
  "one canonical technique identity governs every pair",
  () => {
    const mixedVersion = manifestFor(basePairs.map((item) => structuredClone(item)));
    mixedVersion.pairs[0].control.exposure = rebuildExposure(
      mixedVersion.pairs[0].control.exposure,
      { techniqueVersion: "2.0.0" },
    );
    mixedVersion.source.rowDigest = computeLearningPairDigest(mixedVersion.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(mixedVersion),
      /incomparable with manifest.techniqueContract/,
    );

    const mixedAssignment = manifestFor(basePairs.map((item) => structuredClone(item)));
    mixedAssignment.pairs[0].control.exposure = rebuildExposure(
      mixedAssignment.pairs[0].control.exposure,
      { assignmentId: "assignment-other" },
    );
    mixedAssignment.source.rowDigest = computeLearningPairDigest(mixedAssignment.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(mixedAssignment),
      /incomparable exposure assignmentId/,
    );
  },
  "canonical fact id/version/content and matched intervention assignment cannot drift inside one estimate",
);

prove(
  "exposure semantic identity and work-unit reuse fail closed",
  () => {
    const forged = manifestFor(basePairs.map((item) => structuredClone(item)));
    forged.pairs[0].exposed.exposure.exposureId = deterministicLearningFactId([
      "unrelated-valid-exposure-id",
    ]);
    forged.source.rowDigest = computeLearningPairDigest(forged.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(forged),
      /exposureId does not bind its canonical semantic fields/,
    );

    const sameEpisode = manifestFor([pair("same-episode", 2, 1)]);
    sameEpisode.pairs[0].control.exposure = rebuildExposure(
      sameEpisode.pairs[0].control.exposure,
      { episodeId: sameEpisode.pairs[0].exposed.exposure.episodeId },
    );
    sameEpisode.source.rowDigest = computeLearningPairDigest(sameEpisode.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(sameEpisode),
      /duplicate technique exposure episodeId/,
    );

    const reusedEpisode = manifestFor([
      pair("episode-reuse-a", 2, 1),
      pair("episode-reuse-b", 3, 1),
    ]);
    reusedEpisode.pairs[1].exposed.exposure = rebuildExposure(
      reusedEpisode.pairs[1].exposed.exposure,
      { episodeId: reusedEpisode.pairs[0].exposed.exposure.episodeId },
    );
    reusedEpisode.source.rowDigest = computeLearningPairDigest(reusedEpisode.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(reusedEpisode),
      /duplicate technique exposure episodeId/,
    );

    const reusedObservation = manifestFor([
      pair("observation-reuse-a", 2, 1),
      pair("observation-reuse-b", 3, 1),
    ]);
    reusedObservation.pairs[1].exposed.observationId =
      reusedObservation.pairs[0].exposed.observationId;
    reusedObservation.source.rowDigest = computeLearningPairDigest(reusedObservation.pairs);
    assert.throws(
      () => compileLearningEvidencePacket(reusedObservation),
      /duplicate observation id/,
    );

    const duplicateRows = manifestFor([
      pair("duplicate-row", 2, 1),
      pair("duplicate-row", 2, 1),
    ]);
    assert.throws(
      () => compileLearningEvidencePacket(duplicateRows),
      /duplicate pair id/,
    );
  },
  "a valid UUID shape cannot forge a fact, and one episode/observation cannot enter multiple arms or pairs",
);

prove(
  "outcome identity and metric versions must be comparable",
  () => {
    for (const key of ["metricId", "metricVersion", "unit", "direction"] as const) {
      const invalid = manifestFor([pair(`outcome-${key}`, 2, 1)]);
      (invalid.pairs[0].control.outcome as unknown as Record<string, string>)[key] =
        key === "direction" ? "lower_is_better" : key === "metricVersion" ? "2.0.0" : "other";
      invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
      assert.throws(() => compileLearningEvidencePacket(invalid), /incomparable|metricVersion is incomparable/);
    }
  },
  "formula version, metric, unit, and direction cannot be mixed",
);

prove(
  "one declared outcome contract governs every pair",
  () => {
    const keys = ["metricId", "unit", "direction"] as const;
    for (const key of keys) {
      const drift = manifestFor(basePairs.map((item) => structuredClone(item)));
      const replacement = key === "direction" ? "lower_is_better" : key === "unit" ? "usd" : "cost-usd";
      (drift.pairs[1].exposed.outcome as unknown as Record<string, string>)[key] = replacement;
      (drift.pairs[1].control.outcome as unknown as Record<string, string>)[key] = replacement;
      drift.source.rowDigest = computeLearningPairDigest(drift.pairs);
      assert.throws(() => compileLearningEvidencePacket(drift), /incomparable with manifest.outcomeContract/);
    }
  },
  "internally matched pairs still cannot mix ratios, dollars, or effect direction across the run",
);

prove(
  "statistical and privacy gates remain separate",
  () => {
    const statisticallySmall = manifestFor([pair("small-1", 2, 1), pair("small-2", 3, 1)]);
    statisticallySmall.gates.privacyMinCompletePairs = 2;
    const small = compileLearningEvidencePacket(statisticallySmall);
    assert.equal(small.status, "computed");
    if (small.status === "computed") {
      assert.equal(small.packet.sample.statisticalMinimumMet, false);
      assert.equal(small.packet.sample.privacyMinimumMet, true);
      assert.ok(small.packet.notEstimableReasons.includes("insufficient_statistical_sample"));
      assert.ok(small.packet.notEstimableReasons.includes("insufficient_actor_clusters"));
      assert.ok(small.packet.notEstimableReasons.includes("insufficient_repo_clusters"));
    }

    const privacySmall = manifestFor(basePairs);
    privacySmall.gates.statisticalMinCompletePairs = 5;
    privacySmall.gates.privacyMinCompletePairs = 7;
    const privacy = compileLearningEvidencePacket(privacySmall);
    assert.equal(privacy.status, "computed");
    if (privacy.status === "computed") {
      assert.equal(privacy.packet.sample.statisticalMinimumMet, true);
      assert.equal(privacy.packet.sample.privacyMinimumMet, false);
      assert.ok(privacy.packet.notEstimableReasons.includes("privacy_minimum_not_met"));
      assert.deepEqual(privacy.packet.counterexamples.pairIds, []);
      assert.equal(privacy.packet.counterexamples.privacySuppressed, true);
    }
  },
  "power and disclosure floors emit distinct not_estimable evidence",
);

prove(
  "few actor clusters cannot masquerade as eleven independent pairs",
  () => {
    const clusteredPairs = Array.from({ length: 11 }, (_, index) =>
      pair(
        `clustered-${index}`,
        20 + index,
        20,
        cohort({
          actorClusterId: index === 10 ? "actor-b" : "actor-a",
          repoClusterId: `repo-${index}`,
        }),
      ),
    );
    const result = compileLearningEvidencePacket(manifestFor(clusteredPairs));
    assert.equal(result.status, "computed");
    if (result.status === "computed") {
      assert.equal(result.packet.sample.completePairCount, 11);
      assert.equal(result.packet.sample.actorClusterCount, 2);
      assert.equal(result.packet.sample.repoClusterCount, 11);
      assert.equal(result.packet.sample.statisticalPairMinimumMet, true);
      assert.equal(result.packet.sample.statisticalActorClusterMinimumMet, false);
      assert.equal(result.packet.claimClass, "not_estimable");
      assert.ok(result.packet.notEstimableReasons.includes("insufficient_actor_clusters"));
      assert.equal(result.packet.effect.standardError, null);
    }
  },
  "actor and repo cluster floors are explicit and cluster-aware uncertainty is required",
);

prove(
  "unknown outcomes never become zero",
  () => {
    const incompletePairs = basePairs.map((item) => structuredClone(item));
    incompletePairs[0].exposed.outcome.value = null;
    const result = compileLearningEvidencePacket(manifestFor(incompletePairs));
    assert.equal(result.status, "computed");
    if (result.status === "computed") {
      assert.equal(result.packet.sample.completePairCount, 5);
      assert.equal(result.packet.sample.incompletePairCount, 1);
      assert.equal(result.packet.attribution.unknownOutcomeCount, 1);
      assert.ok(result.packet.notEstimableReasons.includes("incomplete_outcome_pairs"));
      assert.equal(result.packet.effect.rawEstimate, null);
    }
  },
  "incomplete pairs remain counted and the effect is not estimable",
);

prove(
  "unallocated and unknown attribution remain visible",
  () => {
    const inputs = basePairs.map((item) => structuredClone(item));
    inputs[0].exposed.attribution = { method: "none", projectAllocation: "unallocated", coverage: 0 };
    inputs[1].control.attribution = { method: "none", projectAllocation: "unknown", coverage: 0 };
    const result = compileLearningEvidencePacket(manifestFor(inputs, (value) => {
      value.gates.minimumAttributionCoverage = 0.9;
    }));
    assert.equal(result.status, "computed");
    if (result.status === "computed") {
      assert.equal(result.packet.attribution.unallocatedCount, 1);
      assert.equal(result.packet.attribution.unknownCount, 1);
      assert.ok(result.packet.notEstimableReasons.includes("attribution_coverage_below_minimum"));
    }
  },
  "coverage loss cannot disappear into an attributed total",
);

prove(
  "Simpson reversal fails closed",
  () => {
    const simpsonPairs = [
      ...Array.from({ length: 5 }, (_, index) => pair(`simpson-a-${index}`, 12, 10, cohort({ projectId: "project-a" }))),
      pair("simpson-b-0", 4, 10, cohort({ projectId: "project-b" })),
    ];
    const result = compileLearningEvidencePacket(manifestFor(simpsonPairs));
    assert.equal(result.status, "computed");
    if (result.status === "computed") {
      assert.ok((result.packet.effect.crudePairWeightedEstimate ?? 0) > 0);
      assert.ok((result.packet.effect.equalStratumEstimate ?? 0) < 0);
      assert.ok(result.packet.notEstimableReasons.includes("simpson_reversal"));
      assert.equal(result.packet.effect.rawEstimate, null);
    }
  },
  "pair-weighted and equal-stratum sign reversal blocks association",
);

prove(
  "non-finite and unsafe outcomes fail closed",
  () => {
    for (const unsafe of [Number.NaN, Number.POSITIVE_INFINITY, 1_001]) {
      assert.throws(
        () => {
          const invalid = manifestFor([pair("unsafe", unsafe, 1)]);
          compileLearningEvidencePacket(invalid);
        },
        /canonical JSON rejects non-finite|within maxAbsoluteOutcome/,
      );
    }
  },
  "NaN, Infinity, and values above the declared safety bound cannot enter a packet",
);

prove(
  "multiple hypotheses require correction and forbid winner selection",
  () => {
    const uncorrected = manifestFor(basePairs);
    uncorrected.hypothesisFamily.hypothesesTested = 10;
    assert.throws(() => compileLearningEvidencePacket(uncorrected), /require an explicit bonferroni correction/);
    const winner = manifestFor(basePairs);
    (winner.hypothesisFamily as unknown as { selectionPolicy: string }).selectionPolicy = "winner_only";
    assert.throws(() => compileLearningEvidencePacket(winner), /unsupported value: winner_only/);
    const corrected = manifestFor(basePairs);
    corrected.hypothesisFamily.hypothesesTested = 10;
    corrected.hypothesisFamily.correction = "bonferroni";
    const result = compileLearningEvidencePacket(corrected);
    assert.equal(result.status, "computed");
    if (result.status === "computed") assert.equal(result.packet.hypothesisFamily.adjustedAlpha, 0.005);
  },
  "family-wide error is explicit; chance-winner-only mode is absent",
);

prove(
  "hypotheses must be preregistered before the analysis epoch",
  () => {
    const invalid = manifestFor(basePairs);
    invalid.hypothesisFamily.registeredAt = "2026-06-02T00:00:00.000Z";
    assert.throws(() => compileLearningEvidencePacket(invalid), /registered no later than window start/);
  },
  "retrospective hypothesis registration cannot masquerade as prospective evidence",
);

prove(
  "row and runtime budgets are hard bounded",
  () => {
    const rowBound = manifestFor(basePairs);
    rowBound.gates.maxPairs = 5;
    assert.throws(() => compileLearningEvidencePacket(rowBound), /pair budget exceeded/);
    const unsafeRuntime = manifestFor(basePairs);
    unsafeRuntime.gates.maxRuntimeMs = 10_001;
    assert.throws(() => compileLearningEvidencePacket(unsafeRuntime), /safe integer in/);
    assert.equal(baseRun.packet.budgets.analysisWorkUnits, 6);
    assert.equal(baseRun.packet.budgets.continuousLoop, false);
    assert.equal(baseRun.packet.budgets.modelCalls, 0);
  },
  "10k hard row/runtime ceilings and exact work-unit receipt prevent continuous loops",
);

prove(
  "skill lifecycle is review-only and owner gated",
  () => {
    const review = baseRun.packet.skillCandidateReview;
    assert.deepEqual(review.lifecycle, SKILL_CANDIDATE_LIFECYCLE);
    assert.deepEqual(review.inventoryDispositions, LEARNING_INVENTORY_DISPOSITIONS);
    assert.equal(review.currentState, "observed");
    assert.equal(review.inventoryDisposition, "insufficient_evidence");
    assert.equal(review.containsExecutableInstructions, false);
    assert.equal(review.publicationAuthorized, false);
    assert.equal(review.installationAuthorized, false);
    assert.equal(review.ownerApprovalRequired, true);
    assert.equal(review.independentVerificationRequired, true);
    assert.equal(review.openWebOrModelTextMayGate, false);
  },
  "analysis can only produce an observed review artifact, never an executable skill",
);

prove(
  "output path guard blocks skill, memory, URL, and workspace escape writes",
  () => {
    const root = resolve(process.cwd());
    assert.equal(assertLearningReviewOutputPath("evidence/learning.json", root), resolve(root, "evidence/learning.json"));
    assert.throws(() => assertLearningReviewOutputPath("../outside.json", root), /inside the explicit workspace/);
    assert.throws(() => assertLearningReviewOutputPath("https://example.com/skill.json", root), /network\/URL/);
    assert.throws(() => assertLearningReviewOutputPath("reports/SKILL.md", root), /executable skills/);
    assert.throws(() => assertLearningReviewOutputPath("reports/MEMORY.md", root), /global memory/);
    assert.throws(
      () => assertLearningReviewOutputPath("fixtures/.codex/skills/candidate.json", root),
      /prohibited skill or memory tree/,
    );
    assert.throws(
      () => assertLearningReviewOutputPath(resolve(homedir(), ".codex/skills/x.json"), root),
      /inside the explicit workspace|prohibited skill/,
    );
    const outside = resolve(root, `../plimsoll-learning-guard-outside-${process.pid}`);
    const link = resolve(root, `evidence/.learning-guard-link-${process.pid}`);
    const forbiddenTarget = resolve(root, `evidence/.learning-guard-case-${process.pid}/.CoDeX/SkIlLs`);
    const skillAlias = resolve(root, `evidence/.learning-skill-alias-${process.pid}`);
    const chainAlias = resolve(root, `evidence/.learning-skill-chain-${process.pid}`);
    mkdirSync(outside, { recursive: true });
    mkdirSync(forbiddenTarget, { recursive: true });
    mkdirSync(resolve(root, "evidence"), { recursive: true });
    try {
      symlinkSync(outside, link, "dir");
      assert.throws(
        () => assertLearningReviewOutputPath(`${link}/packet.json`, root),
        /cannot escape through a workspace symlink/,
      );
      symlinkSync(forbiddenTarget, skillAlias, "dir");
      symlinkSync(skillAlias, chainAlias, "dir");
      assert.throws(
        () => assertLearningReviewOutputPath(`${skillAlias}/candidate.json`, root),
        /resolves into a prohibited skill or memory tree/,
      );
      for (let attempt = 0; attempt < 2; attempt += 1) {
        assert.throws(
          () => assertLearningReviewOutputPath(`${chainAlias}/candidate.json`, root),
          /resolves into a prohibited skill or memory tree/,
        );
      }
      rmSync(chainAlias, { force: true });
      symlinkSync(skillAlias, chainAlias, "dir");
      assert.throws(
        () => assertLearningReviewOutputPath(`${chainAlias}/candidate.json`, root),
        /resolves into a prohibited skill or memory tree/,
      );
    } finally {
      rmSync(chainAlias, { force: true });
      rmSync(skillAlias, { force: true });
      rmSync(link, { force: true });
      rmSync(outside, { recursive: true, force: true });
      rmSync(resolve(root, `evidence/.learning-guard-case-${process.pid}`), { recursive: true, force: true });
    }
  },
  "CLI output resolves symlink chains on every run and rejects canonical mixed-case skill/memory targets",
);

prove(
  "source fingerprint changes on analytical identity changes",
  () => {
    const changed = manifestFor(basePairs);
    changed.gates.statisticalMinCompletePairs = 6;
    assert.notEqual(computeLearningSourceFingerprint(changed), baseRun.sourceFingerprint);
    const versionChanged = manifestFor(basePairs);
    versionChanged.metricVersions.projectAllocation = "2.0.0";
    assert.notEqual(computeLearningSourceFingerprint(versionChanged), baseRun.sourceFingerprint);
    const techniqueChanged = manifestFor(basePairs);
    techniqueChanged.techniqueContract.techniqueVersion = "2.0.0";
    assert.notEqual(computeLearningSourceFingerprint(techniqueChanged), baseRun.sourceFingerprint);

    const confounderOrderA = manifestFor(basePairs);
    confounderOrderA.declaredConfounders = ["actor_selection", "calendar_change"];
    const confounderOrderB = manifestFor(basePairs);
    confounderOrderB.declaredConfounders = ["calendar_change", "actor_selection"];
    assert.equal(
      computeLearningSourceFingerprint(confounderOrderA),
      computeLearningSourceFingerprint(confounderOrderB),
    );
  },
  "gate/version drift invalidates the key while set ordering does not",
);

prove(
  "CLI writes once and leaves an unchanged artifact untouched",
  () => {
    const directory = resolve(process.cwd(), `evidence/.learning-cli-proof-${process.pid}`);
    const input = resolve(directory, "manifest.json");
    const output = resolve(directory, "packet.json");
    mkdirSync(directory, { recursive: true });
    try {
      writeFileSync(input, `${JSON.stringify(baseManifest)}\n`, { mode: 0o600 });
      const command = [
        "exec",
        "tsx",
        "scripts/learning-evidence-packet.ts",
        "--input",
        input,
        "--out",
        output,
      ];
      const first = spawnSync("pnpm", command, { cwd: process.cwd(), encoding: "utf8" });
      assert.equal(first.status, 0, first.stderr);
      const firstReceipt = JSON.parse(first.stdout.trim()) as { status: string; outputWritten: boolean };
      assert.deepEqual(firstReceipt, {
        status: "computed",
        sourceFingerprint: baseRun.sourceFingerprint,
        analysisWorkUnits: 6,
        output,
        outputWritten: true,
      });
      const packet = JSON.parse(readFileSync(output, "utf8")) as { packetFingerprint: string };
      assert.equal(packet.packetFingerprint, baseRun.packet.packetFingerprint);
      const inode = statSync(output).ino;

      const second = spawnSync("pnpm", command, { cwd: process.cwd(), encoding: "utf8" });
      assert.equal(second.status, 0, second.stderr);
      const secondReceipt = JSON.parse(second.stdout.trim()) as { status: string; outputWritten: boolean };
      assert.equal(secondReceipt.status, "unchanged");
      assert.equal(secondReceipt.outputWritten, false);
      assert.equal(statSync(output).ino, inode);
    } finally {
      rmSync(directory, { recursive: true, force: true });
    }
  },
  "second run reports zero work and does not replace/rewrite the JSON packet",
);

console.log(`learning evidence proof: ${checks.length}/${checks.length} checks passed`);
for (const check of checks) console.log(`  ✓ ${check.name} — ${check.detail}`);
