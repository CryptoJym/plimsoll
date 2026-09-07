import assert from "node:assert/strict";
import test from "node:test";

import {
  buildProjectUsageExport,
  serializeProjectUsageExport,
  type ProjectUsageExportInput,
} from "../packages/shared/src/finance-project-usage-export";
import type { CostUsageRecord } from "../packages/shared/src/schemas";

const refs = {
  tenant: "11111111-1111-4111-8111-111111111111",
  installation: "22222222-2222-4222-8222-222222222222",
  pool: "33333333-3333-4333-8333-333333333333",
  company: "44444444-4444-4444-8444-444444444444",
  project: "55555555-5555-4555-8555-555555555555",
  excludedCompany: "66666666-6666-4666-8666-666666666666",
  excludedProject: "77777777-7777-4777-8777-777777777777",
};

const period = {
  start: "2026-08-01T00:00:00.000Z",
  end: "2026-09-01T00:00:00.000Z",
};

function record(overrides: Partial<CostUsageRecord> = {}): CostUsageRecord {
  return {
    id: "raw-id",
    tenantId: refs.tenant,
    source: "codex",
    periodStart: "2026-08-01T08:00:00.000Z",
    periodEnd: "2026-08-01T08:01:00.000Z",
    sourceRecordKey: "source-key",
    metadata: {},
    ...overrides,
  };
}

function input(
  records: readonly CostUsageRecord[],
  options: {
    expectedRecordCount?: number;
    complete?: boolean;
    coveredThrough?: string;
    projectMappings?: ProjectUsageExportInput["source"]["projectMappings"];
    billingPoolBySource?: ProjectUsageExportInput["source"]["billingPoolBySource"];
  } = {},
): ProjectUsageExportInput {
  return {
    envelope: {
      tenantRef: refs.tenant,
      installationRef: refs.installation,
      registryVersion: "registry.v1",
      sourceVersion: "projection.v1",
      generation: 1,
      generatedAt: "2026-09-04T20:05:00.000Z",
      sourceUpdatedAt: "2026-09-04T20:00:00.000Z",
      period,
      coverage: {
        complete: options.complete ?? true,
        coveredThrough: options.coveredThrough ?? period.end,
        expectedRecordCount: options.expectedRecordCount ?? records.length,
      },
    },
    source: {
      records,
      billingPoolBySource: options.billingPoolBySource ?? { codex: refs.pool },
      projectMappings: options.projectMappings ?? [],
    },
  };
}

function approvedMapping(projectKey = "project-key") {
  return {
    projectKey,
    companyRef: refs.company,
    projectRef: refs.project,
    effectiveFrom: period.start,
    effectiveTo: period.end,
    attribution: "APPROVED_MAPPING" as const,
  };
}

test("builds a deterministic privacy-safe aggregate and deduplicates replay rows", () => {
  const first = record({
    id: "raw-id-one",
    sourceRecordKey: "private-source-one",
    projectKey: "private-project-one",
    inputTokens: 10,
    outputTokens: 20,
    actualCostUsd: 0.000001,
    estimatedCostUsd: 0.000002,
    model: "private-model",
    metadata: { prompt: "PRIVATE_PROMPT_SENTINEL", email: "private@example.invalid" },
  });
  const replay = record({
    id: "different-raw-id",
    sourceRecordKey: "private-source-one",
    projectKey: "private-project-one",
    inputTokens: 10,
    outputTokens: 20,
    actualCostUsd: 0.000001,
    estimatedCostUsd: 0.000002,
    model: "different-private-model",
    metadata: { response: "PRIVATE_RESPONSE_SENTINEL" },
  });
  const second = record({
    id: "raw-id-two",
    sourceRecordKey: "private-source-two",
    inputTokens: 3,
    outputTokens: 4,
    actualCostUsd: 0.1,
  });
  const value = input([first, replay, second], {
    expectedRecordCount: 2,
    projectMappings: [approvedMapping("private-project-one")],
  });
  const before = structuredClone(value);
  const exported = buildProjectUsageExport(value);
  const serialized = serializeProjectUsageExport(exported);

  assert.deepEqual(value, before, "producer must not mutate caller input");
  assert.equal(exported.totals.recordCount, 2);
  assert.equal(exported.totals.inputTokens, 13);
  assert.equal(exported.totals.outputTokens, 24);
  assert.equal(exported.totals.reportedCostMicros, "100001");
  assert.equal(exported.totals.estimatedCostMicros, null, "missing estimate null-propagates");
  assert.equal(exported.totals.unpricedRecordCount, 0);
  assert.equal(exported.rows.length, 2);
  assert.ok(!serialized.includes("PRIVATE_PROMPT_SENTINEL"));
  assert.ok(!serialized.includes("PRIVATE_RESPONSE_SENTINEL"));
  assert.ok(!serialized.includes("private-model"));
  assert.ok(!serialized.includes("private-source-one"));
  assert.ok(!serialized.includes("private-project-one"));

  const reordered = input([second, first, replay], {
    expectedRecordCount: 2,
    projectMappings: [approvedMapping("private-project-one")],
  });
  assert.equal(serializeProjectUsageExport(buildProjectUsageExport(reordered)), serialized);
});

test("keeps excluded and unallocated groups distinct while preserving null metrics", () => {
  const excluded = record({
    sourceRecordKey: "excluded-source",
    projectKey: "excluded-project",
    outputTokens: 5,
  });
  const unmatched = record({
    sourceRecordKey: "unmatched-source",
    projectKey: "not-in-registry",
    inputTokens: 7,
    actualCostUsd: 2.5,
    estimatedCostUsd: 1,
  });
  const exported = buildProjectUsageExport(input([excluded, unmatched], {
    projectMappings: [{
      projectKey: "excluded-project",
      companyRef: refs.excludedCompany,
      projectRef: refs.excludedProject,
      effectiveFrom: period.start,
      effectiveTo: period.end,
      attribution: "EXCLUDED",
    }],
  }));

  assert.equal(exported.rows.length, 2);
  assert.equal(exported.totals.recordCount, 2);
  assert.equal(exported.totals.inputTokens, null);
  assert.equal(exported.totals.outputTokens, null);
  assert.equal(exported.totals.reportedCostMicros, null);
  assert.equal(exported.totals.estimatedCostMicros, null);
  assert.equal(exported.totals.unpricedRecordCount, 1);
  const excludedRow = exported.rows.find((row) => row.attribution === "EXCLUDED");
  const unallocatedRow = exported.rows.find((row) => row.attribution === "UNALLOCATED");
  assert.deepEqual(
    excludedRow && { companyRef: excludedRow.companyRef, projectRef: excludedRow.projectRef },
    { companyRef: null, projectRef: null },
  );
  assert.deepEqual(
    unallocatedRow && { companyRef: unallocatedRow.companyRef, projectRef: unallocatedRow.projectRef },
    { companyRef: null, projectRef: null },
  );
});

test("rejects conflicts, out-of-window rows, ambiguous mappings, and missing pools", () => {
  const duplicate = record({ sourceRecordKey: "same-source", inputTokens: 1 });
  const conflicting = record({ sourceRecordKey: "same-source", inputTokens: 2 });
  assert.throws(
    () => buildProjectUsageExport(input([duplicate, conflicting], { expectedRecordCount: 1 })),
    /conflicting_duplicate/,
  );

  const outside = record({
    periodStart: "2026-07-31T23:59:00.000Z",
    periodEnd: "2026-08-01T00:01:00.000Z",
  });
  assert.throws(() => buildProjectUsageExport(input([outside])), /record_outside_period/);

  const overlap = [
    approvedMapping("ambiguous"),
    {
      ...approvedMapping("ambiguous"),
      projectRef: refs.excludedProject,
      effectiveFrom: "2026-08-15T00:00:00.000Z",
    },
  ];
  assert.throws(
    () => buildProjectUsageExport(input([record({ projectKey: "ambiguous" })], { projectMappings: overlap })),
    /overlapping_project_mappings/,
  );

  assert.throws(
    () => buildProjectUsageExport(input([record()], { billingPoolBySource: {} })),
    /missing_billing_pool/,
  );
});

test("requires exact six-decimal USD precision and safe values", () => {
  const exact = buildProjectUsageExport(input([record({ actualCostUsd: 0.1 })]));
  assert.equal(exact.totals.reportedCostMicros, "100000");
  assert.throws(
    () => buildProjectUsageExport(input([record({ actualCostUsd: 0.0000001 })])),
    /cost_precision/,
  );
  assert.throws(
    () => buildProjectUsageExport(input([record({ actualCostUsd: 0.1 + 0.2 })])),
    /cost_precision/,
  );
  assert.throws(
    () => buildProjectUsageExport(input([record({ actualCostUsd: 10_000_000_000_000_000 })])),
    /unsafe_cost/,
  );
});

test("preserves partial coverage and rejects an overstated complete envelope", () => {
  const partial = buildProjectUsageExport(input([record()], {
    complete: false,
    expectedRecordCount: 2,
    coveredThrough: "2026-08-15T00:00:00.000Z",
  }));
  assert.equal(partial.coverage.complete, false);
  assert.equal(partial.coverage.expectedRecordCount, 2);
  assert.equal(partial.totals.recordCount, 1);

  assert.throws(
    () => buildProjectUsageExport(input([record()], {
      complete: true,
      expectedRecordCount: 2,
    })),
    /complete_coverage_mismatch/,
  );
});

test("serializer verifies strict shape and hashes", () => {
  const exported = buildProjectUsageExport(input([record()]));
  const tampered = structuredClone(exported) as typeof exported & { unexpected?: string };
  tampered.unexpected = "raw-value-must-not-be-accepted";
  assert.throws(() => serializeProjectUsageExport(tampered), /unknown_or_missing_property/);

  const badHash = structuredClone(exported);
  badHash.totals.recordCount = 7;
  assert.throws(() => serializeProjectUsageExport(badHash), /totals_mismatch/);
});

test("rejects source and aggregate-row bounds without truncating output", () => {
  const tooManySourceRows = Array.from({ length: 10_001 }, (_, index) =>
    record({ sourceRecordKey: `source-${index}` }),
  );
  assert.throws(
    () => buildProjectUsageExport(input(tooManySourceRows, { expectedRecordCount: 10_001 })),
    /source_record_limit/,
  );

  const manyRecords = Array.from({ length: 2_001 }, (_, index) =>
    record({ sourceRecordKey: `project-source-${index}`, projectKey: `project-${index}` }),
  );
  const manyMappings = manyRecords.map((item, index) => ({
    projectKey: item.projectKey!,
    companyRef: refs.company,
    projectRef: `a0000000-0000-4000-8000-${index.toString(16).padStart(12, "0")}`,
    effectiveFrom: period.start,
    effectiveTo: period.end,
    attribution: "APPROVED_MAPPING" as const,
  }));
  assert.throws(
    () => buildProjectUsageExport(input(manyRecords, {
      expectedRecordCount: manyRecords.length,
      projectMappings: manyMappings,
    })),
    /aggregate_row_limit/,
  );
});
