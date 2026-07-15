#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
  runRepoEnrichmentMaintenance,
  runRepricingMaintenance,
  type CollectorMaintenanceRunResult,
} from "../packages/collector-cli/src/maintenance";
import { RolloutTailer, type RolloutScanResult } from "../packages/collector-cli/src/rollout-tailer";
import {
  TranscriptTailer,
  type TranscriptScanResult,
} from "../packages/collector-cli/src/transcript-tailer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { MODEL_PRICING } from "../packages/shared/src/pricing";

const checks: Array<{ name: string; detail: Record<string, unknown> }> = [];

function check(name: string, condition: unknown, detail: Record<string, unknown>) {
  assert.ok(condition, `${name}: ${JSON.stringify(detail)}`);
  checks.push({ name, detail });
}

function emptyRollout(): RolloutScanResult {
  return {
    filesSeen: 0,
    filesRead: 0,
    filesParsed: 0,
    filesReset: 0,
    legacyRebuilds: 0,
    checkpointRebuilds: 0,
    bytesRead: 0,
    bytesDeferred: 0,
    sessionsSkippedOtlpCovered: 0,
    eventsAppended: 0,
    tokensAppended: { input: 0, cachedInput: 0, output: 0 },
    parseErrors: 0,
  };
}

function emptyTranscript(): TranscriptScanResult {
  return {
    filesSeen: 0,
    filesRead: 0,
    filesParsed: 0,
    filesReset: 0,
    legacyRebuilds: 0,
    checkpointRebuilds: 0,
    bytesRead: 0,
    bytesDeferred: 0,
    sessionsSkippedLiveCovered: 0,
    eventsAppended: 0,
    tokensAppended: { input: 0, cacheRead: 0, output: 0 },
    parseErrors: 0,
  };
}

function fakeRun(recentOnly: boolean): CollectorMaintenanceRunResult {
  const multiplier = recentOnly ? 1 : 10;
  const rollout = emptyRollout();
  const transcript = emptyTranscript();
  rollout.filesRead = multiplier;
  transcript.filesRead = 2 * multiplier;
  return {
    recentOnly,
    rollout,
    transcript,
    repricing: {
      catalogFingerprint: "proof",
      catalogChanged: false,
      backfillComplete: true,
      legacyRowsVisited: 0,
      candidateRowsVisited: 4 * multiplier,
      rowsVisited: 4 * multiplier,
      repriced: 0,
    },
    enrichment: {
      backfillComplete: true,
      legacyRowsVisited: 0,
      sessionsVisited: 0,
      candidateRowsVisited: 5 * multiplier,
      rowsVisited: 5 * multiplier,
      backward: 0,
      forward: 0,
    },
    rawEventWrites: 3 * multiplier,
  };
}

async function proveCoalescing() {
  let releaseFirst!: () => void;
  let firstStarted!: () => void;
  const firstStartedPromise = new Promise<void>((resolve) => {
    firstStarted = resolve;
  });
  const firstReleasePromise = new Promise<void>((resolve) => {
    releaseFirst = resolve;
  });
  const calls: boolean[] = [];
  let active = 0;
  let maxActive = 0;
  const scheduler = new CoalescingMaintenanceScheduler(async (recentOnly) => {
    calls.push(recentOnly);
    active += 1;
    maxActive = Math.max(maxActive, active);
    if (calls.length === 1) {
      firstStarted();
      await firstReleasePromise;
    }
    active -= 1;
    return fakeRun(recentOnly);
  });

  const first = scheduler.trigger(true);
  await firstStartedPromise;
  const second = scheduler.trigger(true);
  const fullWhileActive = scheduler.trigger(false);
  const activeStatus = scheduler.status();
  check(
    "active_trigger_is_coalesced_and_visible",
    activeStatus.inFlight &&
      activeStatus.pending &&
      activeStatus.triggerCount === 3 &&
      activeStatus.coalescedTriggerCount === 2,
    activeStatus,
  );
  releaseFirst();
  await Promise.all([first, second, fullWhileActive]);
  const finalStatus = scheduler.status();
  check(
    "full_request_dominates_pending_recent_without_overlap",
    calls.length === 2 &&
      calls[0] === true &&
      calls[1] === false &&
      maxActive === 1 &&
      finalStatus.runCount === 2 &&
      finalStatus.overlappingJobs === 0 &&
      finalStatus.maxConcurrentJobs === 1 &&
      finalStatus.rolloutFilesRead === 11 &&
      finalStatus.transcriptFilesRead === 22 &&
      finalStatus.rawEventWrites === 33 &&
      finalStatus.repriceRowsVisited === 44 &&
      finalStatus.enrichmentRowsVisited === 55 &&
      !finalStatus.inFlight &&
      !finalStatus.pending,
    { calls, maxActive, ...finalStatus },
  );
}

function event(options: {
  id: string;
  sessionId: string;
  observedAt: string;
  model?: string;
  inputTokens?: number;
  outputTokens?: number;
  costUsd?: number;
  repoHash?: string;
  branchHash?: string;
}) {
  return aiInteractionEventSchema.parse({
    id: options.id,
    tenantId: "local",
    source: "codex",
    dataMode: "metadata",
    eventType: options.inputTokens === undefined ? "tool_use" : "usage_rollout",
    observedAt: options.observedAt,
    sessionId: options.sessionId,
    model: options.model,
    actionClass: options.inputTokens === undefined ? "shell" : "other",
    inputTokens: options.inputTokens,
    outputTokens: options.outputTokens,
    costUsd: options.costUsd,
    metadata: options.repoHash
      ? {
          git: {
            remoteUrlHash: options.repoHash,
            branchHash: options.branchHash,
          },
        }
      : {},
  });
}

function dirtyCount(buffer: LocalEventBuffer, table: string) {
  return (buffer.database.prepare(`select count(*) as n from ${table}`).get() as { n: number }).n;
}

function proveBackfillIndexes(buffer: LocalEventBuffer) {
  const pricingPlan = buffer.database
    .prepare(
      `explain query plan
       select id from buffered_events indexed by idx_events_unpriced_usage
       where id > '' and event_type in ('usage_rollout','usage_transcript')
         and cost_usd is null and model is not null
       order by id limit 1`,
    )
    .all() as Array<{ detail: string }>;
  const enrichmentPlan = buffer.database
    .prepare(
      `explain query plan
       select id, session_id from buffered_events indexed by idx_events_repo_enrichment_seed
       where id > '' and session_id is not null and (
         repo_hash is not null or input_tokens is not null or
         output_tokens is not null or cost_usd is not null
       )
       order by id limit 1`,
    )
    .all() as Array<{ detail: string }>;
  const pricingUsesPartialIndex = pricingPlan.some((row) =>
    row.detail.includes("idx_events_unpriced_usage"),
  );
  const enrichmentUsesPartialIndex = enrichmentPlan.some((row) =>
    row.detail.includes("idx_events_repo_enrichment_seed"),
  );
  check(
    "legacy_backfills_use_bounded_partial_indexes",
    pricingUsesPartialIndex && enrichmentUsesPartialIndex,
    {
      pricingPlan: pricingPlan.map((row) => row.detail).join(" | "),
      enrichmentPlan: enrichmentPlan.map((row) => row.detail).join(" | "),
    },
  );
}

function proveDirtyMaintenance(buffer: LocalEventBuffer) {
  const priceModel = "plimsoll-maintenance-proof-price";
  const priceSession = "019e3000-0000-7000-8000-000000000001";
  buffer.append(
    event({
      id: "proof-price-dirty",
      sessionId: priceSession,
      observedAt: "2026-07-15T12:00:00.000Z",
      model: priceModel,
      inputTokens: 1_000,
      outputTokens: 100,
    }),
  );

  const initialPricingRuns = [];
  for (let iteration = 0; iteration < 10; iteration += 1) {
    const result = runRepricingMaintenance(buffer.database, {
      backfillLimit: 1,
      candidateLimit: 1,
    });
    initialPricingRuns.push(result);
    assert.ok(result.legacyRowsVisited <= 1 && result.candidateRowsVisited <= 1);
    if (result.backfillComplete && dirtyCount(buffer, "reprice_dirty_events") === 0) break;
  }
  const pricingIdle = runRepricingMaintenance(buffer.database, {
    backfillLimit: 1,
    candidateLimit: 1,
  });
  check(
    "unchanged_pricing_catalog_visits_zero_event_rows",
    pricingIdle.rowsVisited === 0 && pricingIdle.repriced === 0 && !pricingIdle.catalogChanged,
    pricingIdle,
  );

  MODEL_PRICING[priceModel] = {
    input: 2,
    cachedInput: 0.2,
    output: 20,
    vendor: "openai",
    asOf: "maintenance-proof",
  };
  try {
    let repriced = 0;
    const catalogChangeRuns = [];
    for (let iteration = 0; iteration < 10; iteration += 1) {
      const result = runRepricingMaintenance(buffer.database, {
        backfillLimit: 1,
        candidateLimit: 1,
      });
      catalogChangeRuns.push(result);
      repriced += result.repriced;
      assert.ok(result.legacyRowsVisited <= 1 && result.candidateRowsVisited <= 1);
      if (result.backfillComplete && dirtyCount(buffer, "reprice_dirty_events") === 0) break;
    }
    const priced = buffer.database
      .prepare(`select cost_usd as cost from buffered_events where id = ?`)
      .get("proof-price-dirty") as { cost: number | null };
    check(
      "catalog_change_bounded_backfill_reprices_legacy_row",
      repriced === 1 && priced.cost !== null && priced.cost > 0,
      { repriced, cost: priced.cost, runs: catalogChangeRuns.length },
    );
    const pricingIdleAfterChange = runRepricingMaintenance(buffer.database, {
      backfillLimit: 1,
      candidateLimit: 1,
    });
    check(
      "completed_pricing_backfill_returns_to_zero_work",
      pricingIdleAfterChange.rowsVisited === 0 && pricingIdleAfterChange.repriced === 0,
      pricingIdleAfterChange,
    );
  } finally {
    delete MODEL_PRICING[priceModel];
  }

  const backwardSession = "019e3000-0000-7000-8000-000000000002";
  buffer.append(
    event({
      id: "proof-backward-link",
      sessionId: backwardSession,
      observedAt: "2026-07-15T13:00:00.000Z",
      repoHash: "sha256:repo-backward",
      branchHash: "sha256:branch-backward",
    }),
  );
  buffer.append(
    event({
      id: "proof-backward-token",
      sessionId: backwardSession,
      observedAt: "2026-07-15T13:01:00.000Z",
      model: "gpt-5.4",
      inputTokens: 25,
      outputTokens: 5,
      costUsd: 0.01,
    }),
  );

  let backward = 0;
  const boundedEnrichmentRuns = [];
  for (let iteration = 0; iteration < 20; iteration += 1) {
    const result = runRepoEnrichmentMaintenance(buffer.database, {
      legacyBackfillLimit: 1,
      sessionLimit: 1,
      eventLimit: 1,
    });
    boundedEnrichmentRuns.push(result);
    backward += result.backward;
    assert.ok(result.legacyRowsVisited <= 1 && result.candidateRowsVisited <= 1);
    if (result.backfillComplete && dirtyCount(buffer, "repo_enrichment_dirty") === 0) break;
  }
  const backwardRepo = buffer.database
    .prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
    .get("proof-backward-token") as { repoHash: string | null };
  check(
    "dirty_session_backfill_is_bounded_and_stitches_backward",
    backward >= 1 && backwardRepo.repoHash === "sha256:repo-backward",
    { backward, repoHash: backwardRepo.repoHash, runs: boundedEnrichmentRuns.length },
  );
  const enrichmentIdle = runRepoEnrichmentMaintenance(buffer.database, {
    legacyBackfillLimit: 1,
    sessionLimit: 1,
    eventLimit: 1,
  });
  check(
    "unchanged_repo_inputs_visit_zero_event_rows",
    enrichmentIdle.rowsVisited === 0 && enrichmentIdle.sessionsVisited === 0,
    enrichmentIdle,
  );

  const forwardSession = "019e3000-0000-7000-8000-000000000003";
  buffer.append(
    event({
      id: "proof-forward-token",
      sessionId: forwardSession,
      observedAt: "2026-07-15T14:00:00.000Z",
      model: "gpt-5.4",
      inputTokens: 30,
      outputTokens: 6,
      costUsd: 0.02,
    }),
  );
  const unresolved = runRepoEnrichmentMaintenance(buffer.database, {
    legacyBackfillLimit: 1,
    sessionLimit: 1,
    eventLimit: 10,
  });
  const unresolvedIdle = runRepoEnrichmentMaintenance(buffer.database, {
    legacyBackfillLimit: 1,
    sessionLimit: 1,
    eventLimit: 10,
  });
  check(
    "unresolved_dirty_session_does_not_spin",
    unresolved.candidateRowsVisited === 1 &&
      unresolvedIdle.rowsVisited === 0 &&
      dirtyCount(buffer, "repo_enrichment_dirty") === 0,
    { unresolved, unresolvedIdle },
  );
  buffer.append(
    event({
      id: "proof-forward-link",
      sessionId: forwardSession,
      observedAt: "2026-07-15T14:05:00.000Z",
      repoHash: "sha256:repo-forward",
      branchHash: "sha256:branch-forward",
    }),
  );
  const reactivated = runRepoEnrichmentMaintenance(buffer.database, {
    legacyBackfillLimit: 1,
    sessionLimit: 1,
    eventLimit: 10,
  });
  const forwardRepo = buffer.database
    .prepare(`select repo_hash as repoHash from buffered_events where id = ?`)
    .get("proof-forward-token") as { repoHash: string | null };
  check(
    "later_linkage_reactivates_and_stitches_unresolved_session",
    reactivated.forward === 1 && forwardRepo.repoHash === "sha256:repo-forward",
    { reactivated, repoHash: forwardRepo.repoHash },
  );
}

async function proveIntegratedIdle(
  buffer: LocalEventBuffer,
  rolloutRoot: string,
  transcriptRoot: string,
) {
  // Drain any receipts created by the final proof linkage row.
  while (dirtyCount(buffer, "repo_enrichment_dirty") > 0) {
    runRepoEnrichmentMaintenance(buffer.database, {
      legacyBackfillLimit: 25_000,
      sessionLimit: 500,
      eventLimit: 25_000,
    });
  }
  while (dirtyCount(buffer, "reprice_dirty_events") > 0) {
    runRepricingMaintenance(buffer.database, {
      backfillLimit: 25_000,
      candidateLimit: 25_000,
    });
  }
  const maintenance = new CollectorMaintenance(
    buffer,
    new RolloutTailer(buffer, rolloutRoot, () => []),
    new TranscriptTailer(buffer, transcriptRoot),
  );
  const idle = await maintenance.run(false);
  check(
    "integrated_unchanged_cycle_has_zero_parse_write_and_row_visits",
    idle.rollout.filesRead === 0 &&
      idle.transcript.filesRead === 0 &&
      idle.rawEventWrites === 0 &&
      idle.repricing.rowsVisited === 0 &&
      idle.enrichment.rowsVisited === 0,
    idle,
  );
}

async function main() {
  await proveCoalescing();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-maintenance-proof-"));
  const ledger = path.join(root, "ledger.sqlite");
  const rolloutRoot = path.join(root, "rollouts");
  const transcriptRoot = path.join(root, "transcripts");
  fs.mkdirSync(rolloutRoot, { recursive: true });
  fs.mkdirSync(transcriptRoot, { recursive: true });
  let buffer = new LocalEventBuffer(ledger);
  try {
    // Reopening proves additive schema/trigger creation is idempotent.
    buffer.close();
    buffer = new LocalEventBuffer(ledger);
    check("maintenance_schema_is_idempotent", true, { reopened: true });
    proveBackfillIndexes(buffer);
    proveDirtyMaintenance(buffer);
    await proveIntegratedIdle(buffer, rolloutRoot, transcriptRoot);
    const rawContentRows = (
      buffer.database
        .prepare(
          `select count(*) as n from buffered_events
           where payload_json like '%maintenance-proof-raw-content-sentinel%'`,
        )
        .get() as { n: number }
    ).n;
    check("metadata_fixtures_persist_no_raw_content_sentinel", rawContentRows === 0, {
      rawContentRows,
    });
  } finally {
    buffer.close();
    fs.rmSync(root, { recursive: true, force: true });
  }

  process.stdout.write(
    `${JSON.stringify(
      {
        status: "pass",
        proof: "issue-77-serialized-dirty-maintenance",
        checks: checks.length,
        names: checks.map((entry) => entry.name),
      },
      null,
      2,
    )}\n`,
  );
}

main().catch((error) => {
  process.stderr.write(
    `${JSON.stringify({
      status: "fail",
      proof: "issue-77-serialized-dirty-maintenance",
      error: error instanceof Error ? error.message : String(error),
    })}\n`,
  );
  process.exitCode = 1;
});
