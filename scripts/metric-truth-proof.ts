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

function delivery(id: string, overrides: Partial<LearningDelivery> = {}): LearningDelivery {
  return {
    id,
    submittedAt: "2026-06-01T00:00:00.000Z",
    mergedAt: null,
    deliveryAttribution: { method: "direct", evidenceState: "verified" },
    project: {
      id: "project-default",
      attributionMethod: "direct",
      evidenceState: "verified",
    },
    checks: { attempts: [], evidenceState: "verified" },
    rework: { events: [], evidenceState: "verified" },
    tokensToFirstGreen: {
      inputTokens: null,
      outputTokens: null,
      cacheReadTokens: null,
      cacheWriteTokens: null,
      evidenceState: "partial",
    },
    cost: { usd: null, kind: "missing", evidenceState: "verified" },
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
    project: { id: "project-a", attributionMethod: "direct", evidenceState: "verified" },
    checks: {
      attempts: [{ at: "2026-06-01T01:00:00.000Z", state: "passed" }],
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
    project: { id: null, attributionMethod: "none", evidenceState: "verified" },
    checks: {
      attempts: [
        { at: "2026-06-03T01:00:00.000Z", state: "failed" },
        { at: "2026-06-04T01:00:00.000Z", state: "passed" },
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
    project: { id: "project-c", attributionMethod: "deterministic_linkage", evidenceState: "verified" },
    checks: {
      attempts: [{ at: "2026-06-25T02:00:00.000Z", state: "passed" }],
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
    project: { id: null, attributionMethod: "none", evidenceState: "partial" },
    // A visible pass on an incomplete page must not validate the delivery.
    checks: {
      attempts: [{ at: "2026-06-06T01:00:00.000Z", state: "passed" }],
      evidenceState: "partial",
    },
    techniques: { ids: null, attributionMethod: "none", evidenceState: "partial" },
  }),
  delivery("e-no-required-checks", {
    submittedAt: "2026-06-07T00:00:00.000Z",
    mergedAt: "2026-06-08T00:00:00.000Z",
    project: { id: "project-e", attributionMethod: "direct", evidenceState: "verified" },
    checks: {
      attempts: [{ at: "2026-06-07T01:00:00.000Z", state: "none" }],
      evidenceState: "verified",
    },
    // Explicitly reported zero is valid; missing cost is what must not become zero.
    cost: { usd: 0, kind: "reported", evidenceState: "verified" },
  }),
  delivery("f-future-green", {
    submittedAt: "2026-06-09T00:00:00.000Z",
    project: { id: "project-f", attributionMethod: "deterministic_linkage", evidenceState: "verified" },
    checks: {
      attempts: [
        { at: "2026-06-09T01:00:00.000Z", state: "failed" },
        { at: "2026-07-01T01:00:00.000Z", state: "passed" },
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
    project: { id: null, attributionMethod: "none", evidenceState: "blocked" },
    checks: {
      attempts: [{ at: "2026-06-10T01:00:00.000Z", state: "unknown" }],
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
    assert.deepEqual(EVIDENCE_STATES, ["verified", "partial", "inferred", "blocked", "excluded"]);
    assert.deepEqual(CLAIM_CLASSES, ["observed", "suggestive", "associated", "causal", "not_estimable"]);
  },
  "8 definitions; 5 evidence states; 5 claim classes",
);

prove(
  "every result carries the full truth envelope",
  () => {
    assert.equal(results.length, 8);
    for (const result of results) {
      assert.equal(result.definitionVersion, METRIC_DEFINITION_VERSION);
      assert.deepEqual(result.window, manifest.window);
      assert.equal(result.asOf, manifest.asOf);
      assert.equal(result.numerator.length, result.measures.length);
      assert.equal(result.denominator.length, result.measures.length);
      assert.ok(Number.isInteger(result.sample.eligibleCount));
      assert.ok("ratio" in result.coverage);
      assert.ok("horizonDays" in result.maturity);
      assert.ok("methods" in result.attribution);
      assert.ok(EVIDENCE_STATES.includes(result.evidenceState));
      assert.ok(CLAIM_CLASSES.includes(result.claimClass));
    }
  },
  "version/window/asOf/numerator/denominator/sample/coverage/maturity/attribution/evidence/claim present",
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
  },
  "mixed USD/input and input/output aggregation rejected; like-dimension floor preserved",
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
            attempts: [{ at: "2026-06-28T01:00:00.000Z", state: "passed" }],
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
