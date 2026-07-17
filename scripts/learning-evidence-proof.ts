/** Adversarial proof for bounded, review-artifact-only learning packets (#101). */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
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
  validateLearningEvidenceManifest,
  type LearningCohort,
  type LearningEvidenceManifest,
  type LearningObservation,
  type LearningOutcomePair,
} from "../packages/shared/src/index";

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
  cohortValue = cohort(),
): LearningObservation {
  return {
    observationId: id,
    workStartedAt: "2026-06-10T10:00:00.000Z",
    outcomeObservedAt: "2026-06-11T10:00:00.000Z",
    cohort: cohortValue,
    exposure: {
      state,
      techniqueId: state === "exposed" ? "technique-a" : null,
      evidenceSource: "machine_receipt",
      contentOrigin: "machine_observation",
      recordedAt: "2026-06-10T09:59:00.000Z",
    },
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
  return {
    pairId: id,
    exposed: observation(`${id}-exposed`, "exposed", exposedValue, structuredClone(cohortValue)),
    control: observation(`${id}-control`, "control", controlValue, structuredClone(cohortValue)),
  };
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
    window: { startInclusive: START, endExclusive: END },
    asOf: AS_OF,
    techniqueId: "technique-a",
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
  pair("p1", 11, 10),
  pair("p2", 12, 10),
  pair("p3", 13, 10),
  pair("p4", 14, 10),
  pair("p5", 15, 10),
  pair("p6", 9, 10),
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
      statisticalMinimum: 5,
      privacyMinimum: 3,
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
  "header/row digest identity returns before cohort validation or pair analysis",
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
    (invalid.pairs[0].exposed.exposure as unknown as { evidenceSource: string }).evidenceSource = "inferred";
    invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
    assert.throws(() => compileLearningEvidencePacket(invalid), /unsupported value: inferred/);
    const missing = manifestFor([pair("missing", 2, 1)]);
    delete (missing.pairs[0].exposed as unknown as { exposure?: unknown }).exposure;
    missing.source.rowDigest = computeLearningPairDigest(missing.pairs);
    assert.throws(() => compileLearningEvidencePacket(missing), /exposure must be an object/);
  },
  "only operator declarations or machine receipts with explicit arm state are admitted",
);

prove(
  "retrospective exposure fails closed",
  () => {
    const invalid = manifestFor([pair("retro", 2, 1)]);
    invalid.pairs[0].exposed.exposure.recordedAt = "2026-06-10T10:01:00.000Z";
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
      (invalid.pairs[0].exposed.exposure as unknown as { contentOrigin: string }).contentOrigin = origin;
      invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
      assert.throws(() => compileLearningEvidencePacket(invalid), /unsupported value/);
    }
  },
  "tainted text has no admissible exposure provenance enum",
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
      invalid.pairs[0].control.cohort[key] = `${invalid.pairs[0].control.cohort[key]}-other`;
      invalid.source.rowDigest = computeLearningPairDigest(invalid.pairs);
      assert.throws(() => compileLearningEvidencePacket(invalid), new RegExp(`incomparable ${key}`));
    }
  },
  "project/work/complexity/model/tool/actor/repo/epoch mismatches are rejected",
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
  "statistical and privacy gates remain separate",
  () => {
    const statisticallySmall = manifestFor([pair("small-1", 2, 1), pair("small-2", 3, 1)]);
    statisticallySmall.gates.privacyMinCompletePairs = 2;
    const small = compileLearningEvidencePacket(statisticallySmall);
    assert.equal(small.status, "computed");
    if (small.status === "computed") {
      assert.equal(small.packet.sample.statisticalMinimumMet, false);
      assert.equal(small.packet.sample.privacyMinimumMet, true);
      assert.deepEqual(small.packet.notEstimableReasons, ["insufficient_statistical_sample"]);
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
      () => assertLearningReviewOutputPath(resolve(homedir(), ".codex/skills/x.json"), root),
      /inside the explicit workspace|prohibited skill/,
    );
  },
  "CLI artifact output stays local to one explicit workspace and cannot install/publish",
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
  },
  "gate and metric-version drift invalidates the no-op key",
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
