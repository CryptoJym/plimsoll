import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import {
  buildFinanceProjectUsageExportFromProjection,
  readFinanceProjectUsageProjection,
  type FinanceProjectionRequest,
} from "../packages/collector-cli/src/finance-project-usage-projection";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { runCodexReconciliationMaintenance } from "../packages/collector-cli/src/codex-reconciliation";
import {
  invalidateFinanceSourceCoverage,
  recordFinanceCaptureActivity,
  recordFinanceFullHistoryAttempt,
} from "../packages/collector-cli/src/history-coverage";
import { runRepricingMaintenance } from "../packages/collector-cli/src/maintenance";
import { runRetentionDeletionStage } from "../packages/collector-cli/src/maintenance-stage-primitives";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import { normalizeHookPayload } from "../packages/collector-cli/src/normalizer";
import { attachRepoContextSidecar, REPO_CONTEXT_RESOLVER_VERSION } from "../packages/collector-cli/src/repo-context";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { serializeProjectUsageExport } from "../packages/shared/src/finance-project-usage-export";
import { admittedCost, aiInteractionEventSchema, type AiInteractionEvent } from "../packages/shared/src/index";

const refs = {
  tenant: "11111111-1111-4111-8111-111111111111",
  installation: "22222222-2222-4222-8222-222222222222",
  codexPool: "33333333-3333-4333-8333-333333333333",
  claudePool: "44444444-4444-4444-8444-444444444444",
  company: "55555555-5555-4555-8555-555555555555",
  project: "66666666-6666-4666-8666-666666666666",
};

const period = {
  start: "2026-08-31T00:00:00.000Z",
  end: "2026-09-01T00:00:00.000Z",
};
const snapshotAt = "2026-09-04T20:00:00.000Z";
const now = "2026-09-04T21:00:00.000Z";
const repoKey = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const secondRepoKey = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

let eventCounter = 1;

function eventId(): string {
  const suffix = (eventCounter++).toString(16).padStart(12, "0");
  return `00000000-0000-4000-8000-${suffix}`;
}

function event(
  observedAt: string,
  options: Partial<AiInteractionEvent> = {},
): AiInteractionEvent {
  return {
    id: eventId(),
    tenantId: refs.tenant,
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt,
    actionClass: "other",
    metadata: { costUsd: 0.001, ...(options.metadata ?? {}) },
    inputTokens: 10,
    outputTokens: 20,
    costUsd: 0.001,
    projectKey: repoKey,
    ...options,
    intent: options.intent ?? "unknown",
  };
}

function historyMarker(source: "codex" | "claude_code") {
  const completedAt = "2026-09-04T19:00:00.000Z";
  const counters = {
    filesSeen: 1,
    filesRead: 1,
    bytesRead: 1,
    bytesDeferred: 0,
    eventsAppended: 1,
    parseErrors: 0,
    discoveryErrors: 0,
    statErrors: 0,
    readErrors: 0,
  };
  return JSON.stringify({
    version: 3,
    source,
    completion: { completedAt, ...counters },
    latestFullAttempt: {
      attemptedAt: completedAt,
      status: "complete",
      reason: null,
      exhaustive: true,
      truncated: false,
      ...counters,
    },
    invalidation: null,
  });
}

function healthyBuffer(events: readonly AiInteractionEvent[] = [
  event("2026-08-31T12:00:00.000Z"),
]): LocalEventBuffer {
  const buffer = new LocalEventBuffer(":memory:", {
    workspaceId: "workspace-a",
    delivery: { enabled: false },
  });
  for (const item of events) buffer.append(item);
  for (let index = 0; index < 4; index += 1) {
    buffer.projection.runMaintenance(new Date(snapshotAt));
  }

  const database = buffer.database;
  const nativeScanAt = new Date().toISOString();
  database.prepare(
    `update dashboard_projection_control set
       schema_version=1, ready=1, parity_ready=1, generation=7, dirty=0,
       degraded_reason=null, last_success_at=?, backfill_complete=1,
       parity_complete=1, metric_backfill_complete=1, repair_backlog=0,
       dirty_session_backlog=0, account_invalidation_backlog=0,
       compact_mutation_backlog=0, compact_gc_backlog=0`,
  ).run(snapshotAt);
  database.prepare(`update dashboard_window_control set target_cutoff_at=null`).run();
  for (const source of ["codex", "claude_code"] as const) {
    database.prepare(
      `insert into maintenance_state (key,value,updated_at) values (?,?,?)
       on conflict(key) do update set value=excluded.value,updated_at=excluded.updated_at`,
    ).run(`history_coverage_v2_${source}`, historyMarker(source), snapshotAt);
    database.prepare(
      `insert into capture_activity_state
       (source,last_activity_at,files_today,discovery_entries,last_scan_at,last_error_code,truncated)
       values (?,?,?,?,?,?,0)
       on conflict(source) do update set last_activity_at=excluded.last_activity_at,
        files_today=excluded.files_today,discovery_entries=excluded.discovery_entries,
        last_scan_at=excluded.last_scan_at,last_error_code=null,truncated=0`,
    ).run(source, "2026-09-04T18:00:00.000Z", 1, 1, nativeScanAt, null);
    database.prepare(
      `update finance_source_coverage set retained_from=?, covered_through=?,
         latest_full_attempt_at=?, latest_full_complete=1, invalidated_at=null,
         last_scan_at=?, last_scan_ok=1, last_scan_truncated=0,
         state_revision=1, published_revision=0
       where workspace_id=? and installation_epoch_id=? and source=?`,
    ).run(
      period.start,
      period.end,
      nativeScanAt,
      nativeScanAt,
      "workspace-a",
      buffer.workspaceBinding()?.currentInstallationEpochId,
      source,
    );
  }
  database.prepare(
    `update finance_publication_control set dirty=1, revision=0,
       workspace_id=null, installation_epoch_id=null, projection_generation=null,
       published_at=null, updated_at=? where singleton=1`,
  ).run(snapshotAt);
  // The in-memory constructor may have a wall clock later than the fixture's
  // snapshot. Use a forward native maintenance instant so expiry cannot mark
  // the dashboard as a clock rollback while the publication is settled.
  buffer.projection.runMaintenance(fixtureMaintenanceDate());
  return buffer;
}

function request(overrides: Partial<FinanceProjectionRequest> = {}): FinanceProjectionRequest {
  return {
    tenantRef: refs.tenant,
    installationRef: refs.installation,
    expectedWorkspaceId: "workspace-a",
    installationScopeAsserted: true,
    period,
    now,
    maxSourceAgeMs: 86_400_000,
    retentionCutoff: "2026-08-01T00:00:00.000Z",
    requiredSources: ["codex", "claude_code"],
    registryVersion: "registry.v1",
    sourceVersion: "finance_exact_period.v2",
    billingPoolBySource: { codex: refs.codexPool, claude_code: refs.claudePool },
    projectMappings: [{
      projectKey: repoKey,
      companyRef: refs.company,
      projectRef: refs.project,
      effectiveFrom: period.start,
      effectiveTo: period.end,
      attribution: "APPROVED_MAPPING",
    }],
    ...overrides,
  };
}

function close(buffer: LocalEventBuffer): void {
  buffer.close();
}

function fixtureMaintenanceDate(): Date {
  return new Date(Date.now() + 1_000);
}

test("reads native facts for the exact period and keeps reported and estimated channels separate", () => {
  const buffer = healthyBuffer([
    event("2026-08-31T00:00:00.000Z", {
      inputTokens: 0,
      outputTokens: undefined,
      costKind: "reported",
      metadata: { costUsd: 0.001 },
    }),
    event("2026-08-31T00:00:00.001Z", {
      inputTokens: undefined,
      outputTokens: 20,
      costUsd: 0.002,
      costKind: "estimated",
      metadata: { costEstimated: true },
      projectKey: secondRepoKey,
    }),
    event("2026-09-01T00:00:00.000Z"),
  ]);
  try {
    const result = buildFinanceProjectUsageExportFromProjection(buffer.database, request());
    assert.equal(result.reasons.includes("PROVENANCE_COVERAGE_UNAVAILABLE"), false);
    assert.equal(result.reasons.includes("SOURCE_REVISION_UNBOUND"), false);
    assert.equal(result.exported.coverage.complete, true);
    assert.equal(result.exported.coverage.expectedRecordCount, 2);
    assert.equal(result.exported.totals.recordCount, 2);
    assert.equal(result.exported.totals.inputTokens, null);
    assert.equal(result.exported.totals.outputTokens, null);
    assert.equal(result.exported.totals.reportedCostMicros, null);
    assert.equal(result.exported.totals.estimatedCostMicros, null);
    assert.equal(result.exported.totals.unpricedRecordCount, 0);
    assert.equal(result.exported.rows.length, 2);
    assert.ok(result.exported.rows.some((row) => row.metrics.reportedCostMicros === "1000"));
    assert.ok(result.exported.rows.some((row) => row.metrics.estimatedCostMicros === "2000"));
  } finally {
    close(buffer);
  }
});

test("uses start-inclusive/end-exclusive facts and one millisecond source intervals", () => {
  const buffer = healthyBuffer([
    event(period.start, { inputTokens: 1, outputTokens: 2, costUsd: undefined }),
    event("2026-08-31T23:59:59.999Z", { inputTokens: 3, outputTokens: 4, costUsd: undefined }),
    event(period.end, { inputTokens: 100, outputTokens: 100, costUsd: 1 }),
  ]);
  try {
    const projection = readFinanceProjectUsageProjection(buffer.database, request());
    assert.equal(projection.input.source.records.length, 2);
    assert.deepEqual(
      projection.input.source.records.map((record) => [record.periodStart, record.periodEnd]),
      [
        [period.start, "2026-08-31T00:00:00.001Z"],
        ["2026-08-31T23:59:59.999Z", period.end],
      ],
    );
  } finally {
    close(buffer);
  }
});

test("uses the native typed cost kind and ignores metadata cost aliases", () => {
  const buffer = healthyBuffer([event("2026-08-31T12:00:00.000Z", {
    metadata: { costUsd: undefined, "plimsoll.estimated_cost_usd": 0.003 },
    costUsd: 0.003,
    costKind: "estimated",
  })]);
  try {
    const result = buildFinanceProjectUsageExportFromProjection(buffer.database, request());
    assert.equal(result.reasons.includes("PROVENANCE_COVERAGE_UNAVAILABLE"), false);
    assert.equal(result.reasons.includes("SOURCE_REVISION_UNBOUND"), false);
    assert.equal(result.reasons.includes("UNKNOWN_COST_PROVENANCE"), false);
    assert.equal(result.exported.totals.reportedCostMicros, null);
    assert.equal(result.exported.totals.estimatedCostMicros, "3000");
  } finally {
    close(buffer);
  }
});

test("filters the persisted workspace and rejects a binding mismatch before facts", () => {
  const buffer = healthyBuffer([
    event("2026-08-31T12:00:00.000Z"),
    event("2026-08-31T13:00:00.000Z"),
  ]);
  try {
    const database = buffer.database;
    const secondFact = database.prepare(
      `select projection_id as projectionId from dashboard_event_facts order by observed_at_ms limit 1 offset 1`,
    ).get() as { projectionId: string };
    database.prepare(`update dashboard_event_facts set workspace_id=? where projection_id=?`)
      .run("workspace-b", secondFact.projectionId);
    const projection = readFinanceProjectUsageProjection(database, request());
    assert.equal(projection.input.source.records.length, 1);
    database.prepare(`update collector_workspace_binding set current_workspace_id=? where singleton=1`).run("workspace-b");
    assert.throws(
      () => readFinanceProjectUsageProjection(database, request()),
      /workspace_binding_mismatch/,
    );
  } finally {
    close(buffer);
  }
});

test("consumes projected suppression without turning it into an unpriced record", () => {
  const buffer = healthyBuffer([event("2026-08-31T12:00:00.000Z")]);
  try {
    const database = buffer.database;
    const fact = database.prepare(
      `select projection_id as projectionId from dashboard_event_facts limit 1`,
    ).get() as { projectionId: string };
    database.prepare(
      `update dashboard_event_facts set input_tokens=null,output_tokens=null,
       cache_read_tokens=null,cache_creation_tokens=null,cost_nanos=null where projection_id=?`,
    ).run(fact.projectionId);
    const projection = readFinanceProjectUsageProjection(database, request());
    assert.equal(projection.input.source.records.length, 0);
    assert.equal(projection.input.envelope.coverage.expectedRecordCount, 0);
    assert.equal(projection.reasons.includes("CACHE_ONLY_USAGE_UNREPRESENTABLE"), false);
  } finally {
    close(buffer);
  }
});

test("holds cache-only facts and unknown or unsafe cost channels while preserving nulls and zero", () => {
  const buffer = healthyBuffer([
    event("2026-08-31T12:00:00.000Z", { inputTokens: 0, outputTokens: undefined, costUsd: undefined, metadata: {} }),
    event("2026-08-31T13:00:00.000Z", { inputTokens: 1, outputTokens: 2, costUsd: 0.003, metadata: {} }),
    event("2026-08-31T14:00:00.000Z", { inputTokens: 3, outputTokens: 4, costUsd: 0.004, metadata: { costEstimated: true } }),
    event("2026-08-31T15:00:00.000Z", {
      inputTokens: 5,
      outputTokens: 6,
      costUsd: 0.005,
      costKind: "reported",
      metadata: { costUsd: 0.005 },
    }),
    event("2026-08-31T16:00:00.000Z", { inputTokens: undefined, outputTokens: undefined, cacheReadTokens: 9, costUsd: undefined, metadata: {} }),
  ]);
  try {
    const database = buffer.database;
    const extraPrecision = database.prepare(
      `select projection_id as projectionId from dashboard_event_facts where observed_at=?`,
    ).get("2026-08-31T15:00:00.000Z") as { projectionId: string };
    database.prepare(`update dashboard_event_facts set cost_nanos=1 where projection_id=?`).run(extraPrecision.projectionId);
    const result = buildFinanceProjectUsageExportFromProjection(database, request());
    assert.equal(result.exported.coverage.complete, false);
    assert.equal(result.exported.coverage.expectedRecordCount, 4);
    assert.equal(result.exported.totals.inputTokens, 9);
    assert.equal(result.exported.totals.outputTokens, null);
    assert.equal(result.exported.totals.reportedCostMicros, null);
    assert.equal(result.exported.totals.estimatedCostMicros, null);
    assert.equal(result.exported.totals.unpricedRecordCount, 4);
    assert.ok(result.reasons.includes("CACHE_ONLY_USAGE_UNREPRESENTABLE"));
    assert.ok(result.reasons.includes("CACHE_USAGE_UNREPRESENTABLE"));
    assert.ok(result.reasons.includes("UNKNOWN_COST_PROVENANCE"));
    assert.ok(result.reasons.includes("COST_PRECISION_UNSAFE"));
  } finally {
    close(buffer);
  }
});

test("blocks pending and conflicting repo context and never emits stored sentinels", () => {
  const pendingEvent = event("2026-08-31T12:00:00.000Z", {
    metadata: {
      costUsd: 0.001,
      prompt: "PRIVATE_PROMPT_SENTINEL",
      path: "/private/secret/path",
      email: "person@example.invalid",
      git: { remoteUrlHash: repoKey },
    },
  });
  const conflictingEvent = event("2026-08-31T13:00:00.000Z", {
    projectKey: secondRepoKey,
    metadata: {
      costUsd: 0.001,
      model: "PRIVATE_MODEL_SENTINEL",
      git: { remoteUrlHash: secondRepoKey },
    },
  });
  const buffer = healthyBuffer([pendingEvent, conflictingEvent]);
  try {
    const database = buffer.database;
    const contextId = `repoctx:v1:${"c".repeat(64)}`;
    const conflictContextId = `repoctx:v1:${"d".repeat(64)}`;
    database.prepare(
      `insert into repo_context_event_links (event_id,context_id,fill_pending,context_conflict,suppression_cleaned)
       values (?,?,1,0,0),(?,?,0,1,0)`,
    ).run(pendingEvent.id, contextId, conflictingEvent.id, conflictContextId);
    const result = buildFinanceProjectUsageExportFromProjection(database, request({
      projectMappings: [
        {
          projectKey: repoKey,
          companyRef: refs.company,
          projectRef: refs.project,
          effectiveFrom: period.start,
          effectiveTo: period.end,
          attribution: "APPROVED_MAPPING",
        },
      ],
    }));
    assert.equal(result.exported.rows.length, 2);
    assert.ok(result.exported.rows.some((row) => row.attribution === "UNALLOCATED"));
    const serialized = serializeProjectUsageExport(result.exported);
    assert.ok(!serialized.includes("PRIVATE_PROMPT_SENTINEL"));
    assert.ok(!serialized.includes("/private/secret/path"));
    assert.ok(!serialized.includes("person@example.invalid"));
    assert.ok(!serialized.includes("PRIVATE_MODEL_SENTINEL"));
    assert.ok(!JSON.stringify(result).includes("PRIVATE_PROMPT_SENTINEL"));
  } finally {
    close(buffer);
  }
});

test("uses the native workspace epoch and fails closed on legacy lineage", () => {
  const oldEvent = event("2026-08-31T11:00:00.000Z", { inputTokens: 7, outputTokens: 8, costUsd: undefined });
  const buffer = healthyBuffer([oldEvent]);
  try {
    const database = buffer.database;
    const oldFact = database.prepare(
      `select projection_id as projectionId, workspace_id as workspaceId,
              installation_epoch_id as installationEpochId
       from dashboard_event_facts limit 1`,
    ).get() as { projectionId: string; workspaceId: string; installationEpochId: string };
    assert.equal(oldFact.workspaceId, "workspace-a");
    assert.ok(oldFact.installationEpochId);
    assert.throws(
      () => database.prepare(`update buffered_events set installation_epoch_id=?`).run("forged-epoch"),
      /installation_epoch_id_is_immutable/,
    );

    buffer.transitionWorkspace("workspace-a", "workspace-b");
    recordFinanceFullHistoryAttempt(database, "codex", snapshotAt, true);
    recordFinanceCaptureActivity(database, "codex", snapshotAt, true, false);
    const staleReceipt = database.prepare(
      `select covered_through as coveredThrough, latest_full_complete as latestFullComplete,
              last_scan_at as lastScanAt
       from finance_source_coverage
       where workspace_id='workspace-b' and source='codex'`,
    ).get() as { coveredThrough: string | null; latestFullComplete: number; lastScanAt: string | null };
    assert.equal(staleReceipt.coveredThrough, null);
    assert.equal(staleReceipt.latestFullComplete, 0);
    assert.equal(staleReceipt.lastScanAt, null);
    const transitionScanAt = new Date().toISOString();
    const newEvent = event("2026-08-31T12:00:00.000Z", {
      inputTokens: 9,
      outputTokens: 10,
      costUsd: undefined,
    });
    assert.equal(buffer.append(newEvent), true);
    for (let index = 0; index < 4; index += 1) {
      buffer.projection.runMaintenance(new Date(snapshotAt));
    }
    database.prepare(
      `update dashboard_projection_control set
       schema_version=1, ready=1, parity_ready=1, generation=8, dirty=0,
       degraded_reason=null, last_success_at=?, backfill_complete=1,
       parity_complete=1, metric_backfill_complete=1, repair_backlog=0,
       dirty_session_backlog=0, account_invalidation_backlog=0,
       compact_mutation_backlog=0, compact_gc_backlog=0`,
    ).run(snapshotAt);
    database.prepare(
      `update dashboard_window_control set cutoff_at=?, target_cutoff_at=null,
         expiry_cursor_at=null, expiry_cursor_id=null,
         compact_expiry_high_water=null, compact_expiry_cursor_segment=null,
         compact_expiry_cursor_offset=null`,
    ).run("2020-01-01T00:00:00.000Z");

    const binding = database.prepare(
      `select current_workspace_id as workspaceId,
              current_installation_epoch_id as installationEpochId
       from collector_workspace_binding where singleton=1`,
    ).get() as { workspaceId: string; installationEpochId: string };
    assert.equal(binding.workspaceId, "workspace-b");
    assert.notEqual(binding.installationEpochId, oldFact.installationEpochId);
    for (const source of ["codex", "claude_code"] as const) {
      database.prepare(
        `update finance_source_coverage set retained_from=?, covered_through=?,
           latest_full_attempt_at=?, latest_full_complete=1, invalidated_at=null,
           last_scan_at=?, last_scan_ok=1, last_scan_truncated=0,
           state_revision=state_revision+1, published_revision=0
         where workspace_id=? and installation_epoch_id=? and source=?`,
      ).run(
        period.start,
        period.end,
        transitionScanAt,
        transitionScanAt,
        binding.workspaceId,
        binding.installationEpochId,
        source,
      );
    }
    database.prepare(
      `update finance_publication_control set dirty=1, revision=0,
         workspace_id=null, installation_epoch_id=null, projection_generation=null,
         published_at=null, updated_at=? where singleton=1`,
    ).run(snapshotAt);
    buffer.projection.runMaintenance(new Date(transitionScanAt));
    const result = readFinanceProjectUsageProjection(database, request({ expectedWorkspaceId: "workspace-b" }));
    assert.equal(result.input.envelope.installationRef, binding.installationEpochId);
    assert.equal(result.input.source.records.length, 1);
    assert.equal(result.input.source.records[0]?.inputTokens, 9);
    assert.notEqual(result.input.source.records[0]?.sourceRecordKey, oldFact.projectionId);

    const currentFact = database.prepare(
      `select projection_id as projectionId from dashboard_event_facts
       where workspace_id='workspace-b' limit 1`,
    ).get() as { projectionId: string };
    database.prepare(`update dashboard_event_facts set installation_epoch_id=null where projection_id=?`)
      .run(currentFact.projectionId);
    const legacy = readFinanceProjectUsageProjection(database, request({ expectedWorkspaceId: "workspace-b" }));
    assert.equal(legacy.input.source.records.length, 0);
    assert.equal(legacy.reasons.includes("PROVENANCE_LINEAGE_UNAVAILABLE"), false);
    database.prepare(
      `update dashboard_event_facts set installation_epoch_id=?, raw_generation=null where projection_id=?`,
    ).run(binding.installationEpochId, currentFact.projectionId);
    const malformedCurrent = readFinanceProjectUsageProjection(
      database,
      request({ expectedWorkspaceId: "workspace-b" }),
    );
    assert.equal(malformedCurrent.input.source.records.length, 0);
    assert.ok(malformedCurrent.reasons.includes("PROVENANCE_LINEAGE_UNAVAILABLE"));
  } finally {
    close(buffer);
  }
});

test("does not strand a current epoch after an A-B-A transition", () => {
  const buffer = healthyBuffer([event("2026-08-31T11:00:00.000Z", { inputTokens: 1, outputTokens: 2 })]);
  try {
    const database = buffer.database;
    const first = database.prepare(
      `select projection_id as projectionId, installation_epoch_id as installationEpochId
       from dashboard_event_facts where workspace_id='workspace-a' limit 1`,
    ).get() as { projectionId: string; installationEpochId: string };

    buffer.transitionWorkspace("workspace-a", "workspace-b");
    assert.equal(buffer.append(event("2026-08-31T12:00:00.000Z", { inputTokens: 3, outputTokens: 4 })), true);
    buffer.transitionWorkspace("workspace-b", "workspace-a");
    const current = buffer.workspaceBinding();
    assert.ok(current?.currentInstallationEpochId);
    assert.notEqual(current.currentInstallationEpochId, first.installationEpochId);
    assert.equal(buffer.append(event("2026-08-31T13:00:00.000Z", { inputTokens: 5, outputTokens: 6 })), true);

    const scanAt = new Date().toISOString();
    for (let index = 0; index < 4; index += 1) {
      buffer.projection.runMaintenance(new Date(snapshotAt));
    }
    database.prepare(
      `update dashboard_projection_control set
       schema_version=1, ready=1, parity_ready=1, generation=9, dirty=0,
       degraded_reason=null, last_success_at=?, backfill_complete=1,
       parity_complete=1, metric_backfill_complete=1, repair_backlog=0,
       dirty_session_backlog=0, account_invalidation_backlog=0,
       compact_mutation_backlog=0, compact_gc_backlog=0`,
    ).run(snapshotAt);
    database.prepare(
      `update dashboard_window_control set cutoff_at=?, target_cutoff_at=null,
         expiry_cursor_at=null, expiry_cursor_id=null,
         compact_expiry_high_water=null, compact_expiry_cursor_segment=null,
         compact_expiry_cursor_offset=null`,
    ).run("2020-01-01T00:00:00.000Z");
    for (const source of ["codex", "claude_code"] as const) {
      database.prepare(
        `update finance_source_coverage set retained_from=?, covered_through=?,
           latest_full_attempt_at=?, latest_full_complete=1, invalidated_at=null,
           last_scan_at=?, last_scan_ok=1, last_scan_truncated=0,
           state_revision=state_revision+1, published_revision=0
         where workspace_id=? and installation_epoch_id=? and source=?`,
      ).run(
        period.start,
        period.end,
        scanAt,
        scanAt,
        current.currentWorkspaceId,
        current.currentInstallationEpochId,
        source,
      );
    }
    database.prepare(
      `update finance_publication_control set dirty=1, revision=0,
         workspace_id=null, installation_epoch_id=null, projection_generation=null,
         published_at=null, updated_at=? where singleton=1`,
    ).run(snapshotAt);
    buffer.projection.runMaintenance(new Date(scanAt));

    const result = readFinanceProjectUsageProjection(database, request({ expectedWorkspaceId: "workspace-a" }));
    assert.equal(result.input.source.records.length, 1);
    assert.equal(result.input.source.records[0]?.inputTokens, 5);
    assert.notEqual(result.input.source.records[0]?.sourceRecordKey, first.projectionId);
    const currentFacts = database.prepare(
      `select count(*) as count from dashboard_event_facts
       where workspace_id='workspace-a' and installation_epoch_id=?`,
    ).get(current.currentInstallationEpochId) as { count: number };
    assert.equal(currentFacts.count, 1);
  } finally {
    close(buffer);
  }
});

test("upgrades a legacy binding without relabeling prior facts", () => {
  const buffer = healthyBuffer([event("2026-08-31T11:00:00.000Z", { inputTokens: 1, outputTokens: 2 })]);
  try {
    const database = buffer.database;
    const before = buffer.workspaceBinding();
    assert.ok(before?.currentInstallationEpochId);
    const legacyFact = database.prepare(
      `select projection_id as projectionId from dashboard_event_facts limit 1`,
    ).get() as { projectionId: string };
    database.prepare(`update dashboard_event_facts set installation_epoch_id=null where projection_id=?`)
      .run(legacyFact.projectionId);
    database.prepare(
      `update collector_workspace_binding set current_installation_epoch_id=null,
         current_installation_epoch_started_at=null where singleton=1`,
    ).run();

    buffer.useWorkspace("workspace-a");
    const after = buffer.workspaceBinding();
    assert.ok(after?.currentInstallationEpochId);
    assert.notEqual(after.currentInstallationEpochId, before.currentInstallationEpochId);
    assert.equal(
      (database.prepare(`select installation_epoch_id as installationEpochId from dashboard_event_facts where projection_id=?`)
        .get(legacyFact.projectionId) as { installationEpochId: string | null }).installationEpochId,
      null,
    );
    assert.equal(buffer.append(event("2026-08-31T12:00:00.000Z", { inputTokens: 3, outputTokens: 4 })), true);
    const currentFact = database.prepare(
      `select installation_epoch_id as installationEpochId from dashboard_event_facts
       where observed_at='2026-08-31T12:00:00.000Z' limit 1`,
    ).get() as { installationEpochId: string };
    assert.equal(currentFact.installationEpochId, after.currentInstallationEpochId);
  } finally {
    close(buffer);
  }
});

test("compares native milliseconds for offset timestamps at both period boundaries", () => {
  const buffer = healthyBuffer([
    event("2026-08-30T18:00:00.000-06:00", { inputTokens: 1, outputTokens: 2, costUsd: undefined }),
    event("2026-08-31T17:59:59.999-06:00", { inputTokens: 3, outputTokens: 4, costUsd: undefined }),
    event("2026-08-31T18:00:00.000-06:00", { inputTokens: 100, outputTokens: 100, costUsd: undefined }),
  ]);
  try {
    const projection = readFinanceProjectUsageProjection(buffer.database, request());
    assert.deepEqual(
      projection.input.source.records.map((record) => [record.periodStart, record.periodEnd]),
      [
        [period.start, "2026-08-31T00:00:00.001Z"],
        ["2026-08-31T23:59:59.999Z", period.end],
      ],
    );
  } finally {
    close(buffer);
  }
});

test("every source-health gate produces a finite hold", () => {
  const cases: Array<{
    name: string;
    mutate: (database: LocalEventBuffer["database"]) => void;
    reason: string;
    request?: Partial<FinanceProjectionRequest>;
  }> = [
    { name: "dirty", mutate: (db) => db.prepare(`update dashboard_projection_control set dirty=1`).run(), reason: "PROJECTION_DIRTY" },
    { name: "parity", mutate: (db) => db.prepare(`update dashboard_projection_control set parity_ready=0`).run(), reason: "PROJECTION_PARITY_NOT_READY" },
    { name: "backlog", mutate: (db) => db.prepare(`update dashboard_projection_control set repair_backlog=1`).run(), reason: "PROJECTION_BACKLOG" },
    { name: "expiry backlog", mutate: (db) => db.prepare(`update dashboard_window_control set target_cutoff_at=?`).run("2026-09-04T19:00:00.000Z"), reason: "PROJECTION_BACKLOG" },
    { name: "history", mutate: (db) => db.prepare(`delete from maintenance_state where key=?`).run("history_coverage_v2_codex"), reason: "HISTORY_COVERAGE_INCOMPLETE" },
    { name: "invalidated", mutate: (db) => db.prepare(`update maintenance_state set value=? where key=?`).run(historyMarker("codex").replace('"invalidation":null', '"invalidation":{"reason":"excluded_generation_grew_after_completion","invalidatedAt":"2026-09-04T19:30:00.000Z"}'), "history_coverage_v2_codex"), reason: "HISTORY_COVERAGE_INVALIDATED" },
    { name: "activity stale", mutate: (db) => db.prepare(`update capture_activity_state set last_scan_at=? where source='codex'`).run("2026-08-31T00:00:00.000Z"), reason: "CAPTURE_ACTIVITY_STALE" },
    { name: "activity error", mutate: (db) => db.prepare(`update capture_activity_state set last_error_code=? where source='codex'`).run("opaque-error"), reason: "CAPTURE_ACTIVITY_ERROR" },
    { name: "activity truncated", mutate: (db) => db.prepare(`update capture_activity_state set truncated=1 where source='codex'`).run(), reason: "CAPTURE_ACTIVITY_TRUNCATED" },
    { name: "open period", mutate: (db) => db.prepare(`update finance_publication_control set published_at=?`).run("2026-08-31T12:00:00.000Z"), reason: "OPEN_PERIOD" },
    { name: "retention", mutate: (db) => db.prepare(`update finance_source_coverage set retained_from=?`).run(period.end), reason: "RETENTION_GAP" },
    {
      name: "installation binding",
      mutate: (db) => db.prepare(`delete from collector_workspace_binding`).run(),
      reason: "WORKSPACE_BINDING_UNPROVEN",
    },
  ];
  for (const current of cases) {
    const buffer = healthyBuffer();
    try {
      current.mutate(buffer.database);
      const result = readFinanceProjectUsageProjection(buffer.database, request(current.request));
      assert.ok(result.reasons.includes(current.reason as never), current.name);
      assert.equal(result.input.envelope.coverage.complete, false, current.name);
      if (["dirty", "parity", "backlog", "expiry backlog"].includes(current.name)) {
        assert.equal(result.input.source.records.length, 0, current.name);
      }
    } finally {
      close(buffer);
    }
  }
});

test("initializes coverage per epoch and preserves a prior watermark on failed scans", () => {
  const buffer = new LocalEventBuffer(":memory:", {
    workspaceId: "workspace-a",
    delivery: { enabled: false },
  });
  try {
    const database = buffer.database;
    const first = buffer.workspaceBinding();
    assert.ok(first?.currentInstallationEpochId);
    assert.ok(first?.currentInstallationEpochStartedAt);
    const initial = database.prepare(
      `select source, workspace_id as workspaceId, installation_epoch_id as installationEpochId,
         retained_from as retainedFrom, covered_through as coveredThrough,
         latest_full_complete as latestFullComplete
       from finance_source_coverage where workspace_id=? and installation_epoch_id=?
       order by source`,
    ).all("workspace-a", first.currentInstallationEpochId) as Array<Record<string, unknown>>;
    assert.equal(initial.length, 2);
    assert.ok(initial.every((row) => row.coveredThrough === null && row.latestFullComplete === 0));

    const firstScanAt = first.currentInstallationEpochStartedAt;
    recordFinanceFullHistoryAttempt(database, "codex", firstScanAt, true);
    const completed = database.prepare(
      `select covered_through as coveredThrough, latest_full_complete as latestFullComplete,
         state_revision as stateRevision from finance_source_coverage
       where workspace_id=? and installation_epoch_id=? and source='codex'`,
    ).get("workspace-a", first.currentInstallationEpochId) as {
      coveredThrough: string; latestFullComplete: number; stateRevision: number;
    };
    assert.equal(completed.coveredThrough, firstScanAt);
    assert.equal(completed.latestFullComplete, 1);
    const priorRevision = completed.stateRevision;
    const failedScanAt = new Date(Date.parse(firstScanAt) + 1).toISOString();
    recordFinanceFullHistoryAttempt(database, "codex", failedScanAt, false);
    const failed = database.prepare(
      `select covered_through as coveredThrough, latest_full_complete as latestFullComplete,
         latest_full_attempt_at as latestFullAttemptAt, state_revision as stateRevision
       from finance_source_coverage where workspace_id=? and installation_epoch_id=? and source='codex'`,
    ).get("workspace-a", first.currentInstallationEpochId) as {
      coveredThrough: string; latestFullComplete: number;
      latestFullAttemptAt: string; stateRevision: number;
    };
    assert.equal(failed.coveredThrough, completed.coveredThrough);
    assert.equal(failed.latestFullComplete, 0);
    assert.equal(failed.latestFullAttemptAt, failedScanAt);
    assert.equal(failed.stateRevision, priorRevision + 1);

    buffer.transitionWorkspace("workspace-a", "workspace-b");
    const second = buffer.workspaceBinding();
    assert.ok(second?.currentInstallationEpochId);
    recordFinanceFullHistoryAttempt(database, "codex", firstScanAt, true);
    recordFinanceCaptureActivity(database, "codex", firstScanAt, true, false);
    const oldRows = database.prepare(
      `select count(*) as count from finance_source_coverage
       where workspace_id='workspace-a' and installation_epoch_id=?`,
    ).get(first.currentInstallationEpochId) as { count: number };
    const newRows = database.prepare(
      `select count(*) as count from finance_source_coverage
       where workspace_id='workspace-b' and installation_epoch_id=? and covered_through is null
         and latest_full_complete=0`,
    ).get(second.currentInstallationEpochId) as { count: number };
    assert.equal(oldRows.count, 2);
    assert.equal(newRows.count, 2);
  } finally {
    close(buffer);
  }
});

test("native publication increments once and dirty or mismatched revisions emit no records", () => {
  const buffer = healthyBuffer();
  try {
    const database = buffer.database;
    const first = database.prepare(
      `select revision, dirty from finance_publication_control where singleton=1`,
    ).get() as { revision: number; dirty: number };
    assert.equal(first.revision, 1);
    assert.equal(first.dirty, 0);
    buffer.projection.runMaintenance(fixtureMaintenanceDate());
    const replay = database.prepare(
      `select revision, dirty from finance_publication_control where singleton=1`,
    ).get() as { revision: number; dirty: number };
    assert.deepEqual(replay, first);

    database.prepare(`update finance_publication_control set dirty=1`).run();
    const dirty = readFinanceProjectUsageProjection(database, request());
    assert.equal(dirty.input.source.records.length, 0);
    assert.ok(dirty.reasons.includes("FINANCE_PUBLICATION_DIRTY"));

    database.prepare(
      `update finance_publication_control set dirty=0, workspace_id='wrong-workspace'`,
    ).run();
    const scopeMismatch = readFinanceProjectUsageProjection(database, request());
    assert.equal(scopeMismatch.input.source.records.length, 0);
    assert.ok(scopeMismatch.reasons.includes("FINANCE_PUBLICATION_SCOPE_MISMATCH"));
  } finally {
    close(buffer);
  }
});

test("publisher validates structure while readers gate requested source health", () => {
  const blocked = healthyBuffer();
  try {
    const database = blocked.database;
    database.prepare(
      `update finance_source_coverage set covered_through=null, latest_full_complete=0,
         last_scan_at=null, last_scan_ok=0 where source='claude_code'`,
    ).run();
    database.prepare(`update finance_publication_control set dirty=1`).run();
    blocked.projection.runMaintenance(fixtureMaintenanceDate());
    const published = database.prepare(
      `select revision, dirty from finance_publication_control where singleton=1`,
    ).get() as { revision: number; dirty: number };
    assert.deepEqual(published, { revision: 2, dirty: 0 });

    const codexOnly = readFinanceProjectUsageProjection(
      database,
      request({ requiredSources: ["codex"] }),
    );
    assert.equal(codexOnly.input.source.records.length, 1);
    assert.equal(codexOnly.reasons.includes("FINANCE_COVERAGE_INCOMPLETE"), false);
    const incompleteClaude = readFinanceProjectUsageProjection(
      database,
      request({ requiredSources: ["claude_code"] }),
    );
    assert.equal(incompleteClaude.input.source.records.length, 0);
    assert.ok(incompleteClaude.reasons.includes("FINANCE_COVERAGE_INCOMPLETE"));
  } finally {
    close(blocked);
  }

  const reader = healthyBuffer();
  try {
    const database = reader.database;
    database.prepare(`delete from finance_source_coverage where source='claude_code'`).run();
    const claudeRequired = readFinanceProjectUsageProjection(
      database,
      request({ requiredSources: ["claude_code"] }),
    );
    assert.equal(claudeRequired.input.source.records.length, 0);
    assert.ok(claudeRequired.reasons.includes("FINANCE_COVERAGE_UNAVAILABLE"));
  } finally {
    close(reader);
  }
});

test("terminal suppression invalidates clean exports until bounded cleanup finishes", () => {
  const buffer = healthyBuffer();
  try {
    const database = buffer.database;
    const suppressedEvents = [
      event("2026-08-31T14:00:00.000Z", { inputTokens: 13, outputTokens: 14, projectKey: undefined }),
      event("2026-08-31T15:00:00.000Z", { inputTokens: 17, outputTokens: 18, projectKey: undefined }),
    ];
    for (const [index, item] of suppressedEvents.entries()) {
      assert.equal(attachRepoContextSidecar(item, `suppression-${index}`, "/tmp"), true);
      assert.equal(buffer.append(item), true);
    }
    const requests = buffer.takeRepoContextBatch();
    assert.equal(requests.length, suppressedEvents.length);
    buffer.beginRepoContextResolution(requests);
    buffer.applyRepoContextResults(requests.map((request) => ({
      contextId: request.contextId,
      repoHash: repoKey,
      branchHash: null,
      headSha: null,
      resolvedAt: new Date().toISOString(),
      resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
    })));
    buffer.projection.runMaintenance(fixtureMaintenanceDate());

    const clean = readFinanceProjectUsageProjection(database, request());
    const cleanAttributed = clean.input.source.records.filter((record) =>
      record.inputTokens === 13 || record.inputTokens === 17,
    );
    assert.equal(cleanAttributed.length, 2);
    assert.ok(cleanAttributed.every((record) => record.projectKey === repoKey));
    const contextIds = requests.map((request) => request.contextId);
    buffer.transactionWithRepoContextHandoffs(() => {
      for (const contextId of contextIds) assert.equal(buffer.suppressRepoContextId(contextId), true);
    });
    const suppressionState = database.prepare(
      `select count(*) as pending from repo_context_suppressions where cleanup_complete=0`,
    ).get() as { pending: number };
    const dirty = database.prepare(
      `select dirty from finance_publication_control where singleton=1`,
    ).get() as { dirty: number };
    assert.equal(suppressionState.pending, 2);
    assert.equal(dirty.dirty, 1);

    const immediatelyHeld = readFinanceProjectUsageProjection(database, request());
    assert.equal(immediatelyHeld.input.source.records.length, 0);
    assert.ok(immediatelyHeld.reasons.includes("FINANCE_PUBLICATION_DIRTY"));

    const firstSlice = buffer.drainRepoContextSuppressions(1);
    assert.equal(firstSlice.rowsVisited, 1);
    buffer.projection.runMaintenance(fixtureMaintenanceDate());
    const betweenSlices = readFinanceProjectUsageProjection(database, request());
    assert.equal(betweenSlices.input.source.records.length, 0);
    assert.ok(betweenSlices.reasons.includes("FINANCE_PUBLICATION_DIRTY"));
    const pendingBetween = database.prepare(
      `select count(*) as pending from repo_context_suppressions where cleanup_complete=0`,
    ).get() as { pending: number };
    assert.equal(pendingBetween.pending, 1);

    const secondSlice = buffer.drainRepoContextSuppressions(1);
    assert.equal(secondSlice.rowsVisited, 1);
    buffer.projection.runMaintenance(fixtureMaintenanceDate());
    const afterCleanup = readFinanceProjectUsageProjection(database, request());
    const unallocated = afterCleanup.input.source.records.filter((record) =>
      record.inputTokens === 13 || record.inputTokens === 17,
    );
    assert.equal(unallocated.length, 2);
    assert.ok(unallocated.every((record) => record.projectKey === undefined));
  } finally {
    close(buffer);
  }
});

test("capture health does not advance historic coverage and retention advances exact deleted watermarks", () => {
  const buffer = new LocalEventBuffer(":memory:", {
    workspaceId: "workspace-a",
    delivery: { enabled: false },
  });
  try {
    const database = buffer.database;
    const binding = buffer.workspaceBinding();
    assert.ok(binding?.currentInstallationEpochId);
    assert.ok(binding?.currentInstallationEpochStartedAt);
    const scanAt = binding.currentInstallationEpochStartedAt;
    recordFinanceCaptureActivity(database, "codex", scanAt, true, false);
    const afterActivity = database.prepare(
      `select covered_through as coveredThrough, last_scan_at as lastScanAt
       from finance_source_coverage where workspace_id=? and installation_epoch_id=? and source='codex'`,
    ).get("workspace-a", binding.currentInstallationEpochId) as {
      coveredThrough: string | null; lastScanAt: string;
    };
    assert.equal(afterActivity.coveredThrough, null);
    assert.equal(afterActivity.lastScanAt, scanAt);

    const old = event("2026-08-01T00:00:00.000Z", { costUsd: undefined, inputTokens: 1, outputTokens: 1 });
    assert.equal(buffer.append(old), true);
    database.prepare(`update buffered_events set uploaded_at=? where id=?`).run("2026-09-05T00:00:00.000Z", old.id);
    database.prepare(
      `update finance_source_coverage set retained_from=?
       where workspace_id=? and installation_epoch_id=? and source='codex'`,
    ).run("2026-07-01T00:00:00.000Z", "workspace-a", binding.currentInstallationEpochId);
    const retention = runRetentionDeletionStage(database, {
      remainingMs: 1_000,
      batchSize: 64,
      retentionDays: 30,
      parityReady: true,
      wallNow: () => Date.parse("2026-09-05T00:00:00.000Z"),
    });
    assert.equal(retention.rows, 1);
    const watermark = database.prepare(
      `select retained_from as retainedFrom from finance_source_coverage
       where workspace_id=? and installation_epoch_id=? and source='codex'`,
    ).get("workspace-a", binding.currentInstallationEpochId) as { retainedFrom: string };
    assert.equal(watermark.retainedFrom, "2026-08-01T00:00:00.001Z");

    const invalidatedAt = new Date().toISOString();
    invalidateFinanceSourceCoverage(database, "codex", invalidatedAt);
    const invalidated = database.prepare(
      `select invalidated_at as invalidatedAt, retained_from as retainedFrom
       from finance_source_coverage where workspace_id=? and installation_epoch_id=? and source='codex'`,
    ).get("workspace-a", binding.currentInstallationEpochId) as {
      invalidatedAt: string; retainedFrom: string;
    };
    assert.equal(invalidated.invalidatedAt, invalidatedAt);
    assert.equal(invalidated.retainedFrom, watermark.retainedFrom);
  } finally {
    close(buffer);
  }
});

test("rejects unsupported sources and more than 10,000 eligible facts without truncation", () => {
  const sourceBuffer = healthyBuffer();
  try {
    assert.throws(
      () => readFinanceProjectUsageProjection(sourceBuffer.database, request({ requiredSources: ["github"] as never })),
      /unsupported_required_source/,
    );
  } finally {
    close(sourceBuffer);
  }

  const buffer = healthyBuffer([]);
  try {
    const database = buffer.database;
    const binding = database.prepare(
      `select current_installation_epoch_id as installationEpochId
       from collector_workspace_binding where singleton=1`,
    ).get() as { installationEpochId: string };
    const insertRaw = database.prepare(
      `insert into buffered_events
       (id,source,event_type,data_mode,observed_at,payload_json,suppressed_fields_json,created_at,
        workspace_id,installation_epoch_id,privacy_generation)
       values (?,?,?,?,?,?,?,?,?,?,?)`,
    );
    const insertFact = database.prepare(
      `insert into dashboard_event_facts
       (projection_id,raw_rowid,source,event_type,observed_at,input_tokens,output_tokens,cost_nanos,
        repo_hash,raw_generation,workspace_id,installation_epoch_id,observed_at_ms,project_key,cost_kind)
       values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
    );
    const write = database.transaction(() => {
      for (let index = 0; index < 10_001; index += 1) {
        const observedMs = Date.parse(period.start) + index;
        const observedAt = new Date(observedMs).toISOString();
        const raw = insertRaw.run(
          `raw-${index}`, "codex", "assistant_response", "metadata", observedAt, "{}", "[]",
          snapshotAt, "workspace-a", binding.installationEpochId, `privacy-${index}`,
        );
        const projectionId = `sha256:${index.toString(16).padStart(64, "0")}`;
        insertFact.run(
          projectionId, Number(raw.lastInsertRowid), "codex", "assistant_response", observedAt,
          1, null, null, null, `generation-${index}`, "workspace-a", binding.installationEpochId,
          observedMs, null, null,
        );
      }
    });
    write();
    database.prepare(
      `update dashboard_projection_control set dirty=0, degraded_reason=null, repair_backlog=0,
       dirty_session_backlog=0, account_invalidation_backlog=0,
       compact_mutation_backlog=0, compact_gc_backlog=0`,
    ).run();
    database.prepare(
      `update finance_publication_control set dirty=0, updated_at=? where singleton=1`,
    ).run(snapshotAt);
    assert.throws(
      () => readFinanceProjectUsageProjection(database, request()),
      /source_record_limit/,
    );
  } finally {
    close(buffer);
  }
});

test("replays an unchanged source snapshot byte-for-byte", () => {
  const firstEvents = [
    event("2026-08-31T12:00:00.000Z", { inputTokens: 10, outputTokens: 20, metadata: { costUsd: 0.001 } }),
    event("2026-08-31T13:00:00.000Z", { inputTokens: 3, outputTokens: 4, metadata: { costEstimated: true }, costUsd: 0.002 }),
  ];
  const buffer = healthyBuffer(firstEvents);
  try {
    const first = serializeProjectUsageExport(
      buildFinanceProjectUsageExportFromProjection(buffer.database, request()).exported,
    );
    const second = serializeProjectUsageExport(
      buildFinanceProjectUsageExportFromProjection(buffer.database, request()).exported,
    );
    assert.equal(first, second);
  } finally {
    close(buffer);
  }
});

test("does not read or return raw JSON payloads", () => {
  const buffer = healthyBuffer([event("2026-08-31T12:00:00.000Z", {
    model: "PRIVATE_MODEL_SENTINEL",
    sessionId: "PRIVATE_SESSION_SENTINEL",
    metadata: {
      prompt: "PRIVATE_PROMPT_SENTINEL",
      path: "/PRIVATE_PATH_SENTINEL",
      email: "PRIVATE_EMAIL_SENTINEL",
      costUsd: 0.001,
    },
  })]);
  try {
    const result = readFinanceProjectUsageProjection(buffer.database, request());
    const serialized = serializeProjectUsageExport(buildFinanceProjectUsageExportFromProjection(buffer.database, request()).exported);
    assert.ok(!JSON.stringify(result).includes("PRIVATE_MODEL_SENTINEL"));
    assert.ok(!JSON.stringify(result).includes("PRIVATE_SESSION_SENTINEL"));
    assert.ok(!serialized.includes("PRIVATE_PROMPT_SENTINEL"));
    assert.ok(!serialized.includes("PRIVATE_PATH_SENTINEL"));
    assert.ok(!serialized.includes("PRIVATE_EMAIL_SENTINEL"));
  } finally {
    close(buffer);
  }
});

test("fails closed on malformed stored JSON without leaking it", () => {
  const buffer = healthyBuffer([event("2026-08-31T12:00:00.000Z")]);
  try {
    buffer.database.prepare(`update buffered_events set payload_json=?`).run("MALFORMED_PRIVATE_JSON_SENTINEL");
    const result = readFinanceProjectUsageProjection(buffer.database, request());
    assert.equal(result.input.source.records.length, 1);
    assert.ok(result.reasons.includes("UNKNOWN_COST_PROVENANCE"));
    assert.ok(!JSON.stringify(result).includes("MALFORMED_PRIVATE_JSON_SENTINEL"));
  } finally {
    close(buffer);
  }
});

test("captures cost provenance once and fails closed on ambiguous markers", () => {
  assert.deepEqual(admittedCost([]), { value: undefined, kind: undefined });
  assert.deepEqual(admittedCost([{ costUsd: 0 }]), { value: 0, kind: "reported" });
  assert.deepEqual(admittedCost([{ estimated_cost_usd: 0 }]), { value: 0, kind: "estimated" });
  assert.deepEqual(
    admittedCost([{ costUsd: 0.5, estimated_cost_usd: 0.5 }]),
    { value: 0.5, kind: "unknown" },
  );
  assert.deepEqual(
    admittedCost([{ costUsd: 0.5 }, { cost_usd: 0.6 }]),
    { value: 0.5, kind: "unknown" },
  );
  assert.deepEqual(admittedCost([{ costUsd: 0.5 }], 0.5), { value: 0.5, kind: "reported" });
  assert.deepEqual(admittedCost([{ costUsd: 0.5 }], 0.6), { value: 0.6, kind: "unknown" });
  assert.deepEqual(admittedCost([{ costUsd: null }]), { value: undefined, kind: "unknown" });

  const legacy = aiInteractionEventSchema.parse({
    id: eventId(),
    source: "codex",
    dataMode: "metadata",
    eventType: "assistant_response",
    observedAt: "2026-09-04T00:00:00.000Z",
    actionClass: "other",
    costUsd: 0,
    metadata: {},
  });
  assert.equal(legacy.costKind, undefined);
  assert.throws(
    () => aiInteractionEventSchema.parse({ ...legacy, costUsd: undefined, costKind: "unknown" }),
    /costKind requires costUsd/,
  );

  const normalizedReported = normalizeHookPayload({
    eventType: "assistant_response",
    costUsd: 0,
  }).event;
  const normalizedEstimated = normalizeHookPayload({
    eventType: "assistant_response",
    estimated_cost_usd: 0,
  }).event;
  const normalizedMixed = normalizeHookPayload({
    eventType: "assistant_response",
    costUsd: 0.5,
    estimated_cost_usd: 0.5,
  }).event;
  assert.deepEqual(
    [normalizedReported.costUsd, normalizedReported.costKind],
    [0, "reported"],
  );
  assert.deepEqual(
    [normalizedEstimated.costUsd, normalizedEstimated.costKind],
    [0, "estimated"],
  );
  assert.equal(normalizedMixed.costUsd, 0.5);
  assert.equal(normalizedMixed.costKind, "unknown");

  const otlpAttribute = (key: string, value: number) => ({
    key,
    value: { doubleValue: value },
  });
  const otlpEvent = (attributes: unknown[]) => explodeOtlpPayload({
    resourceLogs: [{ scopeLogs: [{ logRecords: [{ attributes }] }] }],
  }, { source: "codex" }).events[0]?.event;
  const otlpReported = otlpEvent([otlpAttribute("costUsd", 0)]);
  const otlpEstimated = otlpEvent([otlpAttribute("estimated_cost_usd", 0)]);
  const otlpMixed = otlpEvent([
    otlpAttribute("costUsd", 0.5),
    otlpAttribute("estimated_cost_usd", 0.5),
  ]);
  assert.deepEqual([otlpReported?.costUsd, otlpReported?.costKind], [0, "reported"]);
  assert.deepEqual([otlpEstimated?.costUsd, otlpEstimated?.costKind], [0, "estimated"]);
  assert.equal(otlpMixed?.costUsd, 0.5);
  assert.equal(otlpMixed?.costKind, "unknown");
});

test("defaults present legacy cost to unknown and reprices atomically as estimated", () => {
  const buffer = new LocalEventBuffer(":memory:", {
    workspaceId: "workspace-a",
    delivery: { enabled: false },
  });
  try {
    const legacy = event("2026-08-31T12:00:00.000Z", {
      costUsd: 0,
      metadata: {},
    });
    const unpricedRollout = event("2026-08-31T12:01:00.000Z", {
      eventType: "usage_rollout",
      model: "gpt-5.5",
      inputTokens: 100,
      outputTokens: 100,
      costUsd: undefined,
      metadata: {},
    });
    assert.equal(buffer.append(legacy), true);
    assert.equal(buffer.append(unpricedRollout), true);
    const before = buffer.database.prepare(
      `select cost_usd as costUsd, cost_kind as costKind from buffered_events where id=?`,
    ).get(legacy.id) as { costUsd: number; costKind: string | null };
    assert.deepEqual(before, { costUsd: 0, costKind: "unknown" });

    const result = runRepricingMaintenance(buffer.database);
    assert.equal(result.repriced, 1);
    const after = buffer.database.prepare(
      `select cost_usd as costUsd, cost_kind as costKind, payload_json as payloadJson
       from buffered_events where id=?`,
    ).get(unpricedRollout.id) as { costUsd: number; costKind: string; payloadJson: string };
    assert.equal(after.costKind, "estimated");
    assert.equal(after.costUsd, 0.0035);
    assert.equal(JSON.parse(after.payloadJson).costKind, "estimated");
  } finally {
    close(buffer);
  }
});

test("codex reconciliation writes estimated provenance with its repriced amount", () => {
  const buffer = new LocalEventBuffer(":memory:", {
    workspaceId: "workspace-a",
    delivery: { enabled: false },
  });
  try {
    const unpricedCodex = event("2026-08-31T12:02:00.000Z", {
      source: "codex",
      eventType: "assistant_response",
      model: "gpt-5.5",
      inputTokens: 100,
      outputTokens: 100,
      costUsd: undefined,
      metadata: {},
    });
    assert.equal(buffer.append(unpricedCodex), true);
    const result = runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    assert.equal(result.priced, 1);
    const row = buffer.database.prepare(
      `select cost_usd as costUsd, cost_kind as costKind, payload_json as payloadJson
       from buffered_events where id=?`,
    ).get(unpricedCodex.id) as { costUsd: number; costKind: string; payloadJson: string };
    assert.deepEqual(
      { costUsd: row.costUsd, costKind: row.costKind, payloadCostKind: JSON.parse(row.payloadJson).costKind },
      { costUsd: 0.0035, costKind: "estimated", payloadCostKind: "estimated" },
    );
  } finally {
    close(buffer);
  }
});

test("local pricing tailers stamp estimated provenance at capture", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-cost-kind-"));
  const buffer = new LocalEventBuffer(":memory:", {
    workspaceId: "workspace-a",
    delivery: { enabled: false },
  });
  const rolloutSession = "019e1111-2222-7333-8444-555555555555";
  const transcriptSession = "44445555-6666-4777-8888-99990000aaaa";
  try {
    const rolloutRoot = path.join(root, "rollouts", "2026", "09", "04");
    fs.mkdirSync(rolloutRoot, { recursive: true });
    const rolloutFile = path.join(rolloutRoot, `rollout-2026-09-04T10-00-00-${rolloutSession}.jsonl`);
    const rolloutLine = (timestamp: string, type: string, payload: Record<string, unknown>) =>
      JSON.stringify({ timestamp, type, payload });
    fs.writeFileSync(rolloutFile, [
      rolloutLine("2026-09-04T10:00:00.000Z", "session_meta", { id: rolloutSession }),
      rolloutLine("2026-09-04T10:00:01.000Z", "turn_context", { model: "gpt-5.5" }),
      // Main requires an observed zero before marginal consumption is validated.
      rolloutLine("2026-09-04T10:00:01.500Z", "event_msg", {
        type: "token_count", info: { total_token_usage: {
          input_tokens: 0, cached_input_tokens: 0, output_tokens: 0,
          reasoning_output_tokens: 0, total_tokens: 0,
        } },
      }),
      rolloutLine("2026-09-04T10:00:02.000Z", "event_msg", {
        type: "token_count",
        info: {
          total_token_usage: {
            input_tokens: 100,
            cached_input_tokens: 0,
            output_tokens: 10,
            reasoning_output_tokens: 0,
            total_tokens: 110,
          },
        },
      }),
    ].join("\n") + "\n");
    await new RolloutTailer(buffer, path.join(root, "rollouts"), () => []).scan({ scope: "full" });

    const transcriptRoot = path.join(root, "transcripts");
    const transcriptProject = path.join(transcriptRoot, "project");
    fs.mkdirSync(transcriptProject, { recursive: true });
    fs.writeFileSync(
      path.join(transcriptProject, `${transcriptSession}.jsonl`),
      JSON.stringify({
        type: "assistant",
        sessionId: transcriptSession,
        timestamp: "2026-09-04T10:00:03.000Z",
        message: {
          id: "message-1",
          model: "claude-sonnet-5",
          usage: {
            input_tokens: 100,
            cache_read_input_tokens: 0,
            cache_creation_input_tokens: 0,
            output_tokens: 10,
          },
        },
      }) + "\n",
    );
    await new TranscriptTailer(buffer, transcriptRoot).scan({ scope: "full" });

    const rows = buffer.database.prepare(
      `select event_type as eventType, cost_usd as costUsd, cost_kind as costKind
       from buffered_events where event_type in ('usage_rollout','usage_transcript')
       order by event_type`,
    ).all() as Array<{ eventType: string; costUsd: number | null; costKind: string | null }>;
    assert.equal(rows.length, 2);
    assert.ok(rows.every((row) => row.costUsd !== null && row.costKind === "estimated"));
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
});
