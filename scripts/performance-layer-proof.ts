import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { guardProofCompletion } from "./lib/proof-completion";
import { OutcomeTimelineStore } from "../packages/collector-cli/src/outcome-timeline-store";
import type { OutcomeTimelineCoverage, PullTimelineFact } from "../packages/shared/src/index";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-performance-layer-proof-"));
const databasePath = path.join(root, "outcomes.sqlite");
const store = new OutcomeTimelineStore(databasePath);
const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function prove(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

// Refuses the two silent-green modes — an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts for why each is needed.
const guard = guardProofCompletion({
  countChecks: () => checks.length,
});

const repository = "github:repository:fixture/performance";
const sha = "a".repeat(40);

try {
  const empty = store.materializePerformance({ now: "2026-08-20T00:00:00.000Z" });
  const emptySummary = store.performanceSummary(7, "2026-08-20T00:00:00.000Z");
  prove(
    "empty_history_materializes_nothing",
    empty.inserted === 0 && empty.updated === 0 && empty.unchanged === 0 && emptySummary.totals.pulls === 0,
    { empty, totals: emptySummary.totals },
  );

  const partial: PullTimelineFact = {
    schemaVersion: 1,
    externalId: "github:pull:partial",
    repositoryExternalId: repository,
    pullExternalId: "github:pull:partial",
    pullNumber: 1,
    kind: "pull",
    createdAt: "2026-08-16T00:00:00.000Z",
  };
  store.appendFacts([partial], "2026-08-20T00:00:00.000Z");
  store.materializePerformance({ repositoryExternalId: repository, now: "2026-08-20T00:00:00.000Z" });
  const partialRecord = store.performanceRecords(repository).find((record) => record.pullNumber === 1)!;
  prove(
    "partial_evidence_is_literal_unknown_never_negative",
    partialRecord.mergeOutcome === "UNKNOWN" &&
      partialRecord.checkOutcome === "UNKNOWN" &&
      partialRecord.reworkOutcome === "UNKNOWN" &&
      partialRecord.coverage === "UNKNOWN" &&
      partialRecord.retryEpisodes === "UNKNOWN" &&
      partialRecord.correctionLoops === "UNKNOWN",
    { partialRecord },
  );

  const completeFacts: PullTimelineFact[] = [
    {
      schemaVersion: 1,
      externalId: "github:pull:complete",
      repositoryExternalId: repository,
      pullExternalId: "github:pull:complete",
      pullNumber: 2,
      kind: "pull",
      createdAt: "2026-08-17T00:00:00.000Z",
    },
    {
      schemaVersion: 1,
      externalId: `github:pull:complete:revision:${sha}`,
      repositoryExternalId: repository,
      pullExternalId: "github:pull:complete",
      pullNumber: 2,
      kind: "pull_revision",
      sha,
      committedAt: "2026-08-17T01:00:00.000Z",
    },
    {
      schemaVersion: 1,
      externalId: "github:check-run:complete",
      repositoryExternalId: repository,
      pullExternalId: "github:pull:complete",
      pullNumber: 2,
      kind: "check_attempt",
      checkRunExternalId: "github:check-run:complete",
      sha,
      name: "ci",
      conclusion: "success",
      startedAt: "2026-08-17T01:01:00.000Z",
      completedAt: "2026-08-17T01:02:00.000Z",
    },
    {
      schemaVersion: 1,
      externalId: `github:pull:complete:merge:${sha}`,
      repositoryExternalId: repository,
      pullExternalId: "github:pull:complete",
      pullNumber: 2,
      kind: "merge",
      mergeSha: sha,
      mergedAt: "2026-08-17T02:00:00.000Z",
    },
  ];
  const coverage: OutcomeTimelineCoverage[] = ["pull", "revisions", "checks", "required_checks", "reviews"].map((dimension) => ({
    runId: "performance-proof",
    repositoryExternalId: repository,
    pullExternalId: "github:pull:complete",
    dimension: dimension as OutcomeTimelineCoverage["dimension"],
    status: "complete",
    reason: "complete",
  }));
  store.appendFacts(completeFacts, "2026-08-20T00:00:00.000Z");
  store.recordCoverage(coverage, "2026-08-20T00:00:00.000Z");
  const first = store.materializePerformance({
    repositoryExternalId: repository,
    requiredChecks: { names: ["ci"] },
    now: "2026-08-20T00:00:00.000Z",
  });
  const generation = first.generation;
  const second = store.materializePerformance({
    repositoryExternalId: repository,
    requiredChecks: { names: ["ci"] },
    now: "2026-08-20T00:01:00.000Z",
  });
  const complete = store.performanceRecords(repository).find((record) => record.pullNumber === 2)!;
  prove(
    "already_backfilled_materialization_is_idempotent",
    first.inserted >= 1 && second.inserted === 0 && second.updated === 0 && second.unchanged === 2 &&
      second.generation === generation && complete.mergeOutcome === "MERGED" && complete.checkOutcome === "FIRST_PASS",
    { first, second, complete },
  );

  const summary = store.performanceSummary(7, "2026-08-20T00:00:00.000Z");
  prove(
    "same_materialized_rows_feed_weekly_summary",
    summary.totals.pulls === 2 && summary.totals.merged === 1 && summary.totals.checksUnknown === 1,
    { totals: summary.totals, daily: summary.daily },
  );

  const dashboard = fs.readFileSync(path.join(process.cwd(), "packages/collector-cli/src/dashboard.html"), "utf8");
  const server = fs.readFileSync(path.join(process.cwd(), "packages/collector-cli/src/server.ts"), "utf8");
  prove(
    "dashboard_panel_uses_the_existing_single_snapshot_surface",
    dashboard.includes('id="outcome-plate"') &&
      dashboard.includes("renderOutcomePerformance(d.outcomePerformance||null)") &&
      (dashboard.match(/fetch\("\/api\/snapshot/g) ?? []).length === 1 &&
      server.includes("outcomePerformance") && !server.includes('"/api/outcome'),
    { snapshotFetches: (dashboard.match(/fetch\("\/api\/snapshot/g) ?? []).length },
  );
} finally {
  store.close();
  fs.rmSync(root, { recursive: true, force: true });
}

console.log(JSON.stringify({ schema: "plimsoll.performance-layer-proof.v1", passed: checks.length, checks }, null, 2));
guard.complete();
