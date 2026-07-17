/** Adversarial, pure proof for the versioned learning-metric contract (#97). */
import assert from "node:assert/strict";

import {
  ANALYSIS_MANIFEST_VERSION,
  CLAIM_CLASSES,
  EVIDENCE_STATES,
  METRIC_DEFINITION_VERSION,
  METRIC_IDS,
  METRIC_REGISTRY,
  analyzeLearningMetrics,
  assertComparableMetricResults,
  sumLikeQuantities,
  validateMetricAnalysisManifest,
  type LearningDelivery,
  type MetricAnalysisManifest,
  type MetricId,
  type MetricResult,
} from "../packages/shared/src/index";

type ProofCheck = { name: string; detail: string };
const checks: ProofCheck[] = [];

function prove(name: string, run: () => void, detail: string): void {
  run();
  checks.push({ name, detail });
}

function exactProject(
  id: string,
  attributionMethod: "direct" | "deterministic_linkage" = "direct",
): LearningDelivery["project"] {
  return {
    id,
    allocation: { kind: "exact", shares: [{ projectId: id, fraction: 1 }] },
    attributionMethod,
    evidenceState: "verified",
  };
}

function unallocatedProject(): LearningDelivery["project"] {
  return {
    id: null,
    allocation: { kind: "unallocated", shares: [] },
    attributionMethod: "none",
    evidenceState: "verified",
  };
}

function unknownProject(evidenceState: "partial" | "blocked"): LearningDelivery["project"] {
  return {
    id: null,
    allocation: { kind: "unknown", shares: [] },
    attributionMethod: "none",
    evidenceState,
  };
}

function attempt(
  attemptId: string,
  revisionId: string,
  sequence: number,
  at: string,
  state: "passed" | "failed" | "none" | "unknown",
  supersedesAttemptId: string | null = null,
): LearningDelivery["checks"]["attempts"][number] {
  return { attemptId, revisionId, sequence, supersedesAttemptId, at, state };
}

function delivery(id: string, overrides: Partial<LearningDelivery> = {}): LearningDelivery {
  return {
    id,
    submittedAt: "2026-06-01T00:00:00.000Z",
    mergedAt: null,
    deliveryAttribution: { method: "direct", evidenceState: "verified" },
    project: exactProject("project-default"),
    checks: { attempts: [], evidenceState: "verified" },
    rework: { events: [], evidenceState: "verified" },
    tokensToFirstGreen: {
      inputTokens: null,
      outputTokens: null,
      cacheReadTokens: null,
      cacheWriteTokens: null,
      evidenceState: "partial",
    },
    cost: { usd: null, kind: "missing", evidenceState: "partial" },
    techniques: {
      ids: [],
      attributionMethod: "declared_exposure",
      evidenceState: "verified",
    },
    ...overrides,
  };
}

const deliveries: LearningDelivery[] = [
  delivery("a-stable-first-pass", {
    submittedAt: "2026-06-01T00:00:00.000Z",
    mergedAt: "2026-06-02T00:00:00.000Z",
    project: exactProject("project-a"),
    checks: {
      attempts: [attempt("a1", "a-rev-1", 1, "2026-06-01T01:00:00.000Z", "passed")],
      evidenceState: "verified",
    },
    // This is deliberately after both asOf and the 14-day horizon.
    rework: {
      events: [{ at: "2026-07-02T00:00:00.000Z", kind: "future-revert" }],
      evidenceState: "verified",
    },
    tokensToFirstGreen: {
      inputTokens: 100,
      outputTokens: 20,
      cacheReadTokens: 30,
      cacheWriteTokens: 4,
      evidenceState: "verified",
    },
    cost: { usd: 10, kind: "reported", evidenceState: "verified" },
    techniques: {
      ids: ["alpha"],
      attributionMethod: "declared_exposure",
      evidenceState: "verified",
    },
  }),
  delivery("b-corrected-reworked", {
    submittedAt: "2026-06-03T00:00:00.000Z",
    mergedAt: "2026-06-05T00:00:00.000Z",
    project: unallocatedProject(),
    checks: {
      attempts: [
        attempt("b1", "b-rev-1", 1, "2026-06-03T01:00:00.000Z", "failed"),
        attempt("b2", "b-rev-2", 2, "2026-06-04T01:00:00.000Z", "passed", "b1"),
      ],
      evidenceState: "verified",
    },
    rework: {
      events: [{ at: "2026-06-10T00:00:00.000Z", kind: "revert" }],
      evidenceState: "verified",
    },
    tokensToFirstGreen: {
      inputTokens: 200,
      outputTokens: 50,
      cacheReadTokens: null,
      cacheWriteTokens: null,
      evidenceState: "partial",
    },
  }),
  delivery("c-young-first-pass", {
    submittedAt: "2026-06-25T00:00:00.000Z",
    mergedAt: "2026-06-26T00:00:00.000Z",
    project: exactProject("project-c", "deterministic_linkage"),
    checks: {
      attempts: [attempt("c1", "c-rev-1", 1, "2026-06-25T02:00:00.000Z", "passed")],
      evidenceState: "verified",
    },
    tokensToFirstGreen: {
      inputTokens: 300,
      outputTokens: 60,
      cacheReadTokens: 90,
      cacheWriteTokens: 8,
      evidenceState: "verified",
    },
    cost: { usd: 5, kind: "reported", evidenceState: "verified" },
    techniques: {
      ids: ["beta"],
      attributionMethod: "declared_exposure",
      evidenceState: "verified",
    },
  }),
  delivery("d-missing-check-page", {
    submittedAt: "2026-06-06T00:00:00.000Z",
    mergedAt: "2026-06-07T00:00:00.000Z",
    project: unknownProject("partial"),
    // A visible pass on an incomplete page must not validate the delivery.
    checks: {
      attempts: [attempt("d1", "d-rev-1", 1, "2026-06-06T01:00:00.000Z", "passed")],
      evidenceState: "partial",
    },
    techniques: { ids: null, attributionMethod: "none", evidenceState: "partial" },
  }),
  delivery("e-no-required-checks", {
    submittedAt: "2026-06-07T00:00:00.000Z",
    mergedAt: "2026-06-08T00:00:00.000Z",
    project: exactProject("project-e"),
    checks: {
      attempts: [attempt("e1", "e-rev-1", 1, "2026-06-07T01:00:00.000Z", "none")],
      evidenceState: "verified",
    },
    // Explicitly reported zero is valid; missing cost is what must not become zero.
    cost: { usd: 0, kind: "reported", evidenceState: "verified" },
  }),
  delivery("f-future-green", {
    submittedAt: "2026-06-09T00:00:00.000Z",
    project: exactProject("project-f", "deterministic_linkage"),
    checks: {
      attempts: [
        attempt("f1", "f-rev-1", 1, "2026-06-09T01:00:00.000Z", "failed"),
        attempt("f2", "f-rev-2", 2, "2026-07-01T01:00:00.000Z", "passed", "f1"),
      ],
      evidenceState: "verified",
    },
    tokensToFirstGreen: {
      inputTokens: 999,
      outputTokens: 999,
      cacheReadTokens: 999,
      cacheWriteTokens: 999,
      evidenceState: "verified",
    },
    cost: { usd: 2, kind: "estimated", evidenceState: "inferred" },
    techniques: { ids: ["alpha", "beta"], attributionMethod: "inferred", evidenceState: "inferred" },
  }),
  delivery("g-unknown-check", {
    submittedAt: "2026-06-10T00:00:00.000Z",
    mergedAt: "2026-06-11T00:00:00.000Z",
    project: unknownProject("blocked"),
    checks: {
      attempts: [attempt("g1", "g-rev-1", 1, "2026-06-10T01:00:00.000Z", "unknown")],
      evidenceState: "verified",
    },
    techniques: { ids: null, attributionMethod: "none", evidenceState: "blocked" },
  }),
];

const manifest: MetricAnalysisManifest = {
  schemaVersion: ANALYSIS_MANIFEST_VERSION,
  definitionVersion: METRIC_DEFINITION_VERSION,
  analysisId: "metric-truth-adversarial-fixture",
  metricIds: METRIC_IDS,
  window: {
    startInclusive: "2026-06-01T00:00:00.000Z",
    endExclusive: "2026-07-01T00:00:00.000Z",
  },
  asOf: "2026-06-30T00:00:00.000Z",
  stabilityHorizonDays: 14,
  deliveries,
};

function metric(results: readonly MetricResult[], id: MetricId): MetricResult {
  const found = results.find((result) => result.metricId === id);
  assert.ok(found, `missing metric result: ${id}`);
  return found;
}

function measure(result: MetricResult, key: string) {
  const found = result.measures.find((candidate) => candidate.key === key);
  assert.ok(found, `missing measure ${result.metricId}.${key}`);
  return found;
}

validateMetricAnalysisManifest(manifest);
const results = analyzeLearningMetrics(manifest);

prove(
  "registry is complete and versioned",
  () => {
    assert.deepEqual(Object.keys(METRIC_REGISTRY), METRIC_IDS);
    assert.ok(Object.values(METRIC_REGISTRY).every((definition) => definition.definitionVersion === "1.0.0"));
    assert.ok(Object.values(METRIC_REGISTRY).every((definition) => definition.lifecycleStatus === "experimental"));
    assert.ok(
      Object.values(METRIC_REGISTRY).every(
        (definition) => definition.validationStatus === "adversarial_fixture_validated",
      ),
    );
    assert.equal(METRIC_REGISTRY.mature_stable_delivery.populationEvent, "merged");
    assert.equal(METRIC_REGISTRY.post_merge_rework.populationEvent, "merged");
    assert.equal(METRIC_REGISTRY.first_pass_yield.populationEvent, "submitted");
    assert.deepEqual(EVIDENCE_STATES, ["verified", "partial", "inferred", "blocked", "excluded"]);
    assert.deepEqual(CLAIM_CLASSES, ["observed", "suggestive", "associated", "causal", "not_estimable"]);
  },
  "8 versioned definitions with explicit lifecycle, validation, and population event",
);

prove(
  "every result carries the full truth envelope",
  () => {
    assert.equal(results.length, 8);
    for (const result of results) {
      assert.equal(result.definitionVersion, METRIC_DEFINITION_VERSION);
      assert.equal(result.lifecycleStatus, "experimental");
      assert.equal(result.validationStatus, "adversarial_fixture_validated");
      assert.ok(result.populationEvent === "submitted" || result.populationEvent === "merged");
      assert.deepEqual(result.formulaConfig, {
        stabilityHorizonDays:
          result.metricId === "mature_stable_delivery" || result.metricId === "post_merge_rework"
            ? manifest.stabilityHorizonDays
            : null,
      });
      assert.deepEqual(result.window, manifest.window);
      assert.equal(result.asOf, manifest.asOf);
      assert.equal(result.numerator.length, result.measures.length);
      assert.equal(result.denominator.length, result.measures.length);
      assert.ok(Number.isInteger(result.sample.eligibleCount));
      assert.ok("ratio" in result.coverage);
      assert.ok("horizonDays" in result.maturity);
      assert.ok("methods" in result.attribution);
      assert.ok("allocationMix" in result.attribution);
      assert.ok(EVIDENCE_STATES.includes(result.evidenceState));
      assert.ok(CLAIM_CLASSES.includes(result.claimClass));
    }
  },
  "version/formula config/window/asOf/numerator/denominator/sample/coverage/maturity/attribution/evidence/claim present",
);

prove(
  "project allocation keeps unknown allocation in the denominator",
  () => {
    const allocation = metric(results, "project_allocation_coverage");
    const rate = measure(allocation, "allocation_rate");
    assert.equal(rate.numerator.value, 4);
    assert.equal(rate.denominator.value, 7);
    assert.equal(allocation.sample.eligibleCount, 7);
    assert.equal(allocation.evidenceState, "partial");
    assert.deepEqual(allocation.attribution.allocationMix, {
      exactCount: 4,
      apportionedCount: 0,
      unallocatedCount: 1,
      unknownCount: 2,
    });
    assert.deepEqual(allocation.breakdown, [
      { key: "exact", count: 4 },
      { key: "apportioned", count: 0 },
      { key: "unallocated", count: 1 },
      { key: "unknown", count: 2 },
    ]);
  },
  "4 explicitly allocated / 7 eligible; partial and blocked allocation remain visible",
);

prove(
  "none, unknown, and incomplete required checks never pass",
  () => {
    const firstPass = metric(results, "first_pass_yield");
    const rate = measure(firstPass, "first_pass_yield");
    assert.equal(rate.numerator.value, 2);
    assert.equal(rate.denominator.value, 7);
    assert.equal(firstPass.sample.eligibleCount, 7);
    assert.equal(rate.value.knowledge, "floor");
    assert.equal(firstPass.claimClass, "suggestive");
  },
  "only 2 verified first passes; all 7 deliveries remain in the denominator",
);

prove(
  "correction loop and first-green samples reject look-ahead",
  () => {
    const correction = metric(results, "correction_loop");
    assert.equal(measure(correction, "correction_loop_closure").numerator.value, 1);
    assert.equal(measure(correction, "correction_loop_closure").denominator.value, 2);
    const firstGreen = metric(results, "time_tokens_to_first_green");
    assert.equal(firstGreen.sample.sampleCount, 3);
    assert.equal(measure(firstGreen, "time_to_first_green_ms").denominator.value, 3);
    assert.equal(measure(firstGreen, "input_tokens").denominator.value, 3);
    assert.equal(measure(firstGreen, "cache_read_tokens").denominator.value, 2);
    assert.equal(measure(firstGreen, "cache_write_tokens").denominator.value, 2);
  },
  "future green excluded; cache dimensions use their own known-count denominators",
);

prove(
  "right-censored work cannot look stable",
  () => {
    const stable = metric(results, "mature_stable_delivery");
    assert.equal(stable.sample.eligibleCount, 6);
    assert.equal(stable.sample.sampleCount, 5);
    assert.equal(stable.sample.censoredCount, 1);
    assert.equal(measure(stable, "mature_stable_delivery").numerator.value, 1);
    assert.equal(measure(stable, "mature_stable_delivery").denominator.value, 5);
    const rework = metric(results, "post_merge_rework");
    assert.equal(measure(rework, "post_merge_rework").numerator.value, 1);
    assert.equal(measure(rework, "post_merge_rework").denominator.value, 5);
  },
  "1 young merge censored; 1 stable and 1 reworked among 5 mature deliveries",
);

prove(
  "events beyond as-of or the stability horizon do not leak backward",
  () => {
    const stable = metric(results, "mature_stable_delivery");
    assert.equal(measure(stable, "mature_stable_delivery").numerator.value, 1);
    const rework = metric(results, "post_merge_rework");
    assert.equal(measure(rework, "post_merge_rework").numerator.value, 1);
  },
  "future rework on stable delivery ignored; in-window rework retained",
);

prove(
  "missing cost remains unknown and the known total is a floor",
  () => {
    const cost = metric(results, "known_cost_coverage");
    assert.equal(measure(cost, "known_cost_coverage").numerator.value, 4);
    assert.equal(measure(cost, "known_cost_coverage").denominator.value, 7);
    assert.deepEqual(cost.totals[0], { key: "known_cost_usd", value: 17, unit: "usd", knowledge: "floor" });

    const noKnownCost = analyzeLearningMetrics({
      ...manifest,
      analysisId: "all-cost-missing",
      metricIds: ["known_cost_coverage"],
      deliveries: [delivery("missing-cost-only")],
    })[0];
    assert.deepEqual(noKnownCost.totals[0], {
      key: "known_cost_usd",
      value: null,
      unit: "usd",
      knowledge: "unknown",
    });
  },
  "known USD = $17 floor; all-missing fixture emits null/unknown, never $0",
);

prove(
  "token and dollar dimensions cannot be weighted together",
  () => {
    assert.throws(
      () =>
        sumLikeQuantities([
          { value: 1, unit: "usd", knowledge: "known" },
          { value: 1, unit: "input_token", knowledge: "known" },
        ]),
      /different units/,
    );
    assert.throws(
      () =>
        sumLikeQuantities([
          { value: 1, unit: "input_token", knowledge: "known" },
          { value: 1, unit: "output_token", knowledge: "known" },
        ]),
      /different units/,
    );
    assert.deepEqual(
      sumLikeQuantities([
        { value: 2, unit: "cache_read_token", knowledge: "known" },
        { value: 3, unit: "cache_read_token", knowledge: "floor" },
      ]),
      { value: 5, unit: "cache_read_token", knowledge: "floor" },
    );
    for (const value of [-1, Number.NaN, Number.POSITIVE_INFINITY]) {
      assert.throws(
        () => sumLikeQuantities([{ value, unit: "input_token", knowledge: "known" }]),
        /non-negative finite/,
      );
    }
    assert.throws(
      () => sumLikeQuantities([{ value: null, unit: "usd", knowledge: "known" }]),
      /null value must have unknown knowledge/,
    );
    assert.throws(
      () =>
        sumLikeQuantities([
          { value: Number.MAX_VALUE, unit: "usd", knowledge: "known" },
          { value: Number.MAX_VALUE, unit: "usd", knowledge: "known" },
        ]),
      /must remain finite/,
    );
  },
  "mixed units, negative, NaN, Infinity, and dishonest null knowledge rejected",
);

prove(
  "technique exposure is attributed, sorted, and explicitly non-causal",
  () => {
    const exposure = metric(results, "technique_exposure");
    assert.equal(measure(exposure, "technique_exposure").numerator.value, 3);
    assert.equal(measure(exposure, "technique_exposure").denominator.value, 7);
    assert.deepEqual(exposure.breakdown, [
      { key: "alpha", count: 2 },
      { key: "beta", count: 2 },
    ]);
    assert.match(exposure.limitations.join(" "), /do not estimate/);
    assert.notEqual(exposure.claimClass, "causal");
  },
  "3 exposed deliveries; alpha=2, beta=2; exposure does not claim effect",
);

prove(
  "blocked and fully censored analyses are not estimable",
  () => {
    const blocked = analyzeLearningMetrics({
      ...manifest,
      analysisId: "blocked-checks",
      metricIds: ["first_pass_yield"],
      deliveries: [
        delivery("blocked-check", {
          checks: { attempts: [], evidenceState: "blocked" },
        }),
      ],
    })[0];
    assert.equal(blocked.evidenceState, "blocked");
    assert.equal(blocked.claimClass, "not_estimable");

    const censored = analyzeLearningMetrics({
      ...manifest,
      analysisId: "all-censored",
      metricIds: ["mature_stable_delivery"],
      deliveries: [
        delivery("young-only", {
          submittedAt: "2026-06-28T00:00:00.000Z",
          mergedAt: "2026-06-29T00:00:00.000Z",
          checks: {
            attempts: [attempt("young1", "young-rev-1", 1, "2026-06-28T01:00:00.000Z", "passed")],
            evidenceState: "verified",
          },
        }),
      ],
    })[0];
    assert.equal(censored.sample.censoredCount, 1);
    assert.equal(censored.evidenceState, "excluded");
    assert.equal(censored.claimClass, "not_estimable");
  },
  "blocked source and all-young cohort both fail closed as not_estimable",
);

prove(
  "merge-window metrics use mergedAt for the full cohort",
  () => {
    const crossWindow = delivery("submitted-before-window", {
      submittedAt: "2026-05-31T00:00:00.000Z",
      mergedAt: "2026-06-01T00:00:00.000Z",
      checks: {
        attempts: [attempt("cross1", "cross-rev-1", 1, "2026-05-31T01:00:00.000Z", "passed")],
        evidenceState: "verified",
      },
    });
    const mergeResults = analyzeLearningMetrics({
      ...manifest,
      analysisId: "merge-event-window",
      metricIds: ["mature_stable_delivery", "post_merge_rework"],
      deliveries: [crossWindow],
    });
    const stable = metric(mergeResults, "mature_stable_delivery");
    const rework = metric(mergeResults, "post_merge_rework");
    assert.equal(stable.populationEvent, "merged");
    assert.equal(stable.sample.eligibleCount, 1);
    assert.equal(measure(stable, "mature_stable_delivery").numerator.value, 1);
    assert.equal(measure(stable, "mature_stable_delivery").denominator.value, 1);
    assert.equal(measure(rework, "post_merge_rework").denominator.value, 1);
  },
  "May-submitted/June-merged delivery remains in both June merge-based denominators",
);

prove(
  "unknown-only checks remain incomplete and cannot become stable",
  () => {
    const unknownOnly = delivery("unknown-only", {
      mergedAt: "2026-06-02T00:00:00.000Z",
      checks: {
        attempts: [attempt("unknown1", "unknown-rev-1", 1, "2026-06-01T01:00:00.000Z", "unknown")],
        evidenceState: "verified",
      },
    });
    const unknownResults = analyzeLearningMetrics({
      ...manifest,
      analysisId: "unknown-only-check",
      metricIds: ["first_pass_yield", "mature_stable_delivery"],
      deliveries: [unknownOnly],
    });
    for (const [id, key] of [
      ["first_pass_yield", "first_pass_yield"],
      ["mature_stable_delivery", "mature_stable_delivery"],
    ] as const) {
      const output = metric(unknownResults, id);
      assert.equal(measure(output, key).numerator.value, 0);
      assert.equal(measure(output, key).denominator.value, 1);
      assert.equal(measure(output, key).value.value, null);
      assert.equal(measure(output, key).value.knowledge, "unknown");
      assert.equal(output.evidenceState, "partial");
      assert.equal(output.claimClass, "not_estimable");
    }
  },
  "unknown source outcome emits null/unknown, partial evidence, and not_estimable",
);

prove(
  "same-timestamp conflicting checks are conservatively ambiguous",
  () => {
    const ambiguous = delivery("same-time-ambiguous", {
      mergedAt: "2026-06-02T00:00:00.000Z",
      checks: {
        attempts: [
          attempt("amb1", "amb-rev-1", 1, "2026-06-01T01:00:00.000Z", "unknown"),
          attempt("amb2", "amb-rev-2", 2, "2026-05-31T19:00:00.000-06:00", "passed", "amb1"),
        ],
        evidenceState: "verified",
      },
    });
    const output = analyzeLearningMetrics({
      ...manifest,
      analysisId: "same-time-conflict",
      metricIds: ["mature_stable_delivery"],
      deliveries: [ambiguous],
    })[0];
    assert.equal(measure(output, "mature_stable_delivery").numerator.value, 0);
    assert.equal(measure(output, "mature_stable_delivery").value.value, null);
    assert.equal(output.sample.unknownCount, 1);
    assert.equal(output.claimClass, "not_estimable");
  },
  "explicit sequence does not launder conflicting observations at the same instant",
);

prove(
  "correction closure requires revision and attempt lineage",
  () => {
    const sameRevision = delivery("same-revision-correction", {
      checks: {
        attempts: [
          attempt("corr1", "corr-rev-1", 1, "2026-06-01T01:00:00.000Z", "failed"),
          attempt("corr2", "corr-rev-1", 2, "2026-06-02T01:00:00.000Z", "passed", "corr1"),
        ],
        evidenceState: "verified",
      },
    });
    const correction = analyzeLearningMetrics({
      ...manifest,
      analysisId: "same-revision-does-not-close",
      metricIds: ["correction_loop"],
      deliveries: [sameRevision],
    })[0];
    assert.equal(measure(correction, "correction_loop_closure").numerator.value, 0);
    assert.equal(measure(correction, "correction_loop_closure").denominator.value, 1);

    const brokenLineage = delivery("broken-lineage", {
      checks: {
        attempts: [
          attempt("broken1", "broken-rev-1", 1, "2026-06-01T01:00:00.000Z", "failed"),
          attempt("broken2", "broken-rev-2", 2, "2026-06-02T01:00:00.000Z", "passed"),
        ],
        evidenceState: "verified",
      },
    });
    assert.throws(
      () => validateMetricAnalysisManifest({ ...manifest, analysisId: "broken-lineage", deliveries: [brokenLineage] }),
      /check lineage must supersede/,
    );
  },
  "same revision yields 0/1; missing supersession edge is rejected",
);

prove(
  "allocation mix carries exact, apportioned, and unallocated states",
  () => {
    const apportioned = delivery("apportioned", {
      project: {
        id: null,
        allocation: {
          kind: "apportioned",
          shares: [
            { projectId: "project-one", fraction: 0.4 },
            { projectId: "project-two", fraction: 0.6 },
          ],
        },
        attributionMethod: "deterministic_linkage",
        evidenceState: "verified",
      },
    });
    const output = analyzeLearningMetrics({
      ...manifest,
      analysisId: "allocation-mix",
      metricIds: ["project_allocation_coverage"],
      deliveries: [apportioned, delivery("exact"), delivery("unallocated", { project: unallocatedProject() })],
    })[0];
    assert.deepEqual(output.attribution.allocationMix, {
      exactCount: 1,
      apportionedCount: 1,
      unallocatedCount: 1,
      unknownCount: 0,
    });
    assert.equal(measure(output, "allocation_rate").numerator.value, 2);
    assert.equal(measure(output, "allocation_rate").denominator.value, 3);
  },
  "allocation result reports exact=1, apportioned=1, unallocated=1",
);

prove(
  "project allocation ids cannot alias through whitespace",
  () => {
    const aliasedAllocation = delivery("aliased-allocation", {
      project: {
        id: null,
        allocation: {
          kind: "apportioned",
          shares: [
            { projectId: "project-a", fraction: 0.5 },
            { projectId: " project-a ", fraction: 0.5 },
          ],
        },
        attributionMethod: "deterministic_linkage",
        evidenceState: "verified",
      },
    });
    assert.throws(
      () =>
        validateMetricAnalysisManifest({
          ...manifest,
          analysisId: "aliased-allocation",
          deliveries: [aliasedAllocation],
        }),
      /projectId must use canonical identity form/,
    );
  },
  "project-a plus whitespace-wrapped project-a is rejected before uniqueness or allocation math",
);

prove(
  "revision ids cannot fake a correction through whitespace",
  () => {
    const aliasedRevision = delivery("aliased-revision", {
      checks: {
        attempts: [
          attempt("revision-attempt-1", "revision-a", 1, "2026-06-01T01:00:00.000Z", "failed"),
          attempt(
            "revision-attempt-2",
            " revision-a ",
            2,
            "2026-06-02T01:00:00.000Z",
            "passed",
            "revision-attempt-1",
          ),
        ],
        evidenceState: "verified",
      },
    });
    assert.throws(
      () =>
        validateMetricAnalysisManifest({
          ...manifest,
          analysisId: "aliased-revision",
          deliveries: [aliasedRevision],
        }),
      /revisionId must use canonical identity form/,
    );
  },
  "revision-a followed by whitespace-wrapped revision-a is rejected rather than counted as a new revision",
);

prove(
  "runtime enum validation rejects malformed plain-JS manifests",
  () => {
    const base = delivery("bad-enum");
    const invalidDeliveries = [
      { ...base, deliveryAttribution: { ...base.deliveryAttribution, evidenceState: "mystery" } },
      { ...base, deliveryAttribution: { ...base.deliveryAttribution, method: "mystery" } },
      {
        ...base,
        checks: {
          evidenceState: "verified",
          attempts: [{ ...attempt("bad1", "bad-rev-1", 1, "2026-06-01T01:00:00.000Z", "passed"), state: "green" }],
        },
      },
      { ...base, cost: { ...base.cost, kind: "mystery" } },
      { ...base, project: { ...base.project, allocation: { ...base.project.allocation, kind: "mystery" } } },
    ];
    for (const [index, invalid] of invalidDeliveries.entries()) {
      assert.throws(
        () =>
          validateMetricAnalysisManifest({
            ...manifest,
            analysisId: `invalid-enum-${index}`,
            deliveries: [invalid],
          } as unknown as MetricAnalysisManifest),
        /unsupported value/,
      );
    }
  },
  "unknown evidence, attribution, check, cost, and allocation values all fail closed",
);

prove(
  "verified evidence cannot carry impossible null measures or ids",
  () => {
    const base = delivery("verified-null");
    const invalidDeliveries = [
      {
        ...base,
        tokensToFirstGreen: { ...base.tokensToFirstGreen, evidenceState: "verified" },
      },
      {
        ...base,
        cost: { usd: null, kind: "missing", evidenceState: "verified" },
      },
      {
        ...base,
        techniques: { ids: null, attributionMethod: "none", evidenceState: "verified" },
      },
      {
        ...base,
        project: {
          id: null,
          allocation: { kind: "unknown", shares: [] },
          attributionMethod: "none",
          evidenceState: "verified",
        },
      },
    ];
    for (const [index, invalid] of invalidDeliveries.entries()) {
      assert.throws(() =>
        validateMetricAnalysisManifest({
          ...manifest,
          analysisId: `verified-null-${index}`,
          deliveries: [invalid],
        } as MetricAnalysisManifest),
      );
    }
  },
  "verified null tokens, cost, techniques, and unknown project allocation are rejected",
);

prove(
  "persisted metric comparisons require compatible formula versions",
  () => {
    assert.doesNotThrow(() => assertComparableMetricResults(results[0], results[0]));
    assert.throws(
      () =>
        assertComparableMetricResults(results[0], {
          ...results[0],
          definitionVersion: "0.9.0",
        }),
      /incompatible formula versions/,
    );
  },
  "current/current comparison passes; 1.0.0/0.9.0 comparison fails closed",
);

prove(
  "persisted lifecycle and validation claims bind to the registry",
  () => {
    assert.throws(
      () =>
        assertComparableMetricResults(results[0], {
          ...results[0],
          lifecycleStatus: "active",
        }),
      /lifecycleStatus does not match the metric registry/,
    );
    assert.throws(
      () =>
        assertComparableMetricResults(results[0], {
          ...results[0],
          validationStatus: "externally_validated",
        }),
      /validationStatus does not match the metric registry/,
    );
  },
  "allowed enum values still fail when they forge a status the current registry does not declare",
);

prove(
  "formula identity includes the configured stability horizon",
  () => {
    const stable14 = analyzeLearningMetrics({
      ...manifest,
      analysisId: "stable-horizon-14",
      metricIds: ["mature_stable_delivery"],
      stabilityHorizonDays: 14,
    })[0];
    const stable7 = analyzeLearningMetrics({
      ...manifest,
      analysisId: "stable-horizon-7",
      metricIds: ["mature_stable_delivery"],
      stabilityHorizonDays: 7,
    })[0];
    assert.deepEqual(stable14.formulaConfig, { stabilityHorizonDays: 14 });
    assert.deepEqual(stable7.formulaConfig, { stabilityHorizonDays: 7 });
    assert.throws(
      () => assertComparableMetricResults(stable14, stable7),
      /incompatible formula configuration: stabilityHorizonDays 14 vs 7/,
    );
  },
  "mature-stability results calculated at 14 and 7 days cannot be silently compared",
);

prove(
  "manifest validation rejects dishonest cost and ambiguous time",
  () => {
    assert.throws(
      () =>
        validateMetricAnalysisManifest({
          ...manifest,
          analysisId: "invalid-cost",
          deliveries: [delivery("bad-cost", { cost: { usd: 0, kind: "missing", evidenceState: "verified" } })],
        }),
      /missing cost must be null/,
    );
    assert.throws(
      () => validateMetricAnalysisManifest({ ...manifest, asOf: "2026-06-30T00:00:00" }),
      /explicit timezone/,
    );
  },
  "missing-as-zero and timezone-free asOf are rejected",
);

prove(
  "analysis is deterministic across input ordering",
  () => {
    const reversed = analyzeLearningMetrics({ ...manifest, deliveries: [...deliveries].reverse() });
    assert.equal(JSON.stringify(reversed), JSON.stringify(results));
  },
  "byte-identical JSON for forward and reverse delivery order",
);

console.log(`metric truth proof: ${checks.length}/${checks.length} checks passed`);
for (const check of checks) console.log(`  ✓ ${check.name} — ${check.detail}`);
