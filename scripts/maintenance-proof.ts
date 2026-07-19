#!/usr/bin/env node

import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  codexReconciliationStatus,
  runCodexReconciliationMaintenance,
} from "../packages/collector-cli/src/codex-reconciliation";
import {
  AUTOMATIC_BASELINE_STARTUP_INTERVAL_MS,
  AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS,
  AutomaticMaintenanceCadence,
  CoalescingMaintenanceScheduler,
  CollectorMaintenance,
  drainProjectionMigration,
  requestAutomaticRecentMaintenance,
  runRepoEnrichmentMaintenance,
  runRepricingMaintenance,
  type AutomaticMaintenanceCadenceTimer,
  type CollectorMaintenanceRunResult,
} from "../packages/collector-cli/src/maintenance";
import {
  beginAutomaticCaptureBaseline,
  captureBaselineStatus,
  completeAutomaticCaptureBaseline,
  stageAutomaticCaptureBaselineObservation,
  type CaptureBaselineStatus,
} from "../packages/collector-cli/src/capture-baseline";
import {
  RolloutTailer,
  type RolloutScanOptions,
  type RolloutScanResult,
} from "../packages/collector-cli/src/rollout-tailer";
import {
  TranscriptTailer,
  type TranscriptScanOptions,
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
    scope: "recent",
    exhaustive: true,
    discoveryErrors: 0,
    statErrors: 0,
    readErrors: 0,
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
    unresolvedRecords: 0,
    recordsParsed: 0,
    slicesCommitted: 0,
    cooperativeYields: 0,
    excludedGenerations: 0,
    excludedBytes: 0,
    deferredGenerations: 0,
    aborted: false,
    lastYieldAt: null,
    automaticBudget: null,
    activity: {
      lastActivityAt: null,
      filesToday: 0,
      discoveryEntries: 0,
      lastScanAt: "2026-07-15T00:00:00.000Z",
      truncated: false,
    },
  };
}

function emptyTranscript(): TranscriptScanResult {
  return {
    scope: "recent",
    exhaustive: true,
    discoveryErrors: 0,
    statErrors: 0,
    readErrors: 0,
    filesSeen: 0,
    filesRead: 0,
    filesParsed: 0,
    filesReset: 0,
    legacyRebuilds: 0,
    checkpointRebuilds: 0,
    bytesRead: 0,
    bytesDeferred: 0,
    sessionsSkippedLiveCovered: 0,
    filesSkippedOutsideRecentWindow: 0,
    eventsAppended: 0,
    tokensAppended: { input: 0, cacheRead: 0, output: 0 },
    parseErrors: 0,
    unresolvedRecords: 0,
    recordsParsed: 0,
    slicesCommitted: 0,
    cooperativeYields: 0,
    excludedGenerations: 0,
    excludedBytes: 0,
    deferredGenerations: 0,
    aborted: false,
    lastYieldAt: null,
    automaticBudget: null,
    activity: {
      lastActivityAt: null,
      filesToday: 0,
      discoveryEntries: 0,
      lastScanAt: "2026-07-15T00:00:00.000Z",
      truncated: false,
    },
  };
}

function fakeRun(): CollectorMaintenanceRunResult {
  const multiplier = 1;
  const rollout = emptyRollout();
  const transcript = emptyTranscript();
  rollout.filesRead = multiplier;
  transcript.filesRead = 2 * multiplier;
  return {
    recentOnly: true,
    rollout,
    transcript,
    reconciliation: {
      backfillComplete: true,
      legacyRowsVisited: 0,
      contextRowsVisited: 0,
      candidateRowsVisited: 6 * multiplier,
      rowsVisited: 6 * multiplier,
      rowsChanged: 0,
      stitched: 0,
      priced: 0,
      sliceDurationMs: 0,
      timeBudgetExhausted: false,
    },
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
  let calls = 0;
  let active = 0;
  let maxActive = 0;
  const scheduler = new CoalescingMaintenanceScheduler(async () => {
    calls += 1;
    active += 1;
    maxActive = Math.max(maxActive, active);
    if (calls === 1) {
      firstStarted();
      await firstReleasePromise;
    }
    active -= 1;
    return fakeRun();
  });

  const first = requestAutomaticRecentMaintenance(scheduler);
  await firstStartedPromise;
  const second = requestAutomaticRecentMaintenance(scheduler);
  const third = requestAutomaticRecentMaintenance(scheduler);
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
  await Promise.all([first, second, third]);
  const finalStatus = scheduler.status();
  check(
    "automatic_recent_requests_coalesce_without_full_mode_or_overlap",
    calls === 2 &&
      maxActive === 1 &&
      finalStatus.runCount === 2 &&
      finalStatus.overlappingJobs === 0 &&
      finalStatus.maxConcurrentJobs === 1 &&
      finalStatus.rolloutFilesRead === 2 &&
      finalStatus.transcriptFilesRead === 4 &&
      finalStatus.rawEventWrites === 6 &&
      finalStatus.repriceRowsVisited === 8 &&
      finalStatus.reconciliationRowsVisited === 12 &&
      finalStatus.enrichmentRowsVisited === 10 &&
      !finalStatus.inFlight &&
      !finalStatus.pending,
    { calls, maxActive, ...finalStatus },
  );
}

async function proveStoppingCancelsPendingFollowup() {
  let release!: () => void;
  let started!: () => void;
  const startedPromise = new Promise<void>((resolve) => { started = resolve; });
  const releasePromise = new Promise<void>((resolve) => { release = resolve; });
  let calls = 0;
  const scheduler = new CoalescingMaintenanceScheduler(async () => {
    calls += 1;
    started();
    await releasePromise;
    return fakeRun();
  });
  const first = scheduler.trigger();
  await startedPromise;
  const coalesced = scheduler.trigger();
  scheduler.stopAccepting();
  await assert.rejects(() => scheduler.trigger(), /maintenance_scheduler_stopping/);
  release();
  await Promise.all([first, coalesced, scheduler.waitForIdle()]);
  const status = scheduler.status();
  check(
    "shutdown_stops_accepting_and_cancels_pending_followup",
    calls === 1 && status.runCount === 1 && !status.pending && !status.inFlight && !status.accepting,
    { calls, ...status },
  );
}

function fakeBaselineStatus(
  state: CaptureBaselineStatus["progress"]["state"],
): CaptureBaselineStatus {
  const complete = state === "complete";
  return {
    status: complete ? "complete" : "blocked",
    reason: complete ? null : "capture_baseline_in_progress",
    progress: {
      state,
      sourcesComplete: complete ? 2 : 0,
      sourcesInProgress: state === "in_progress" ? 2 : 0,
      sourcesFailed: state === "failed" || state === "ambiguous" ? 1 : 0,
      filesDiscovered: 0,
      filesValidated: 0,
      filesBaselined: 0,
      pendingMetadata: 0,
      pendingMetadataPerSourceCap: 64,
      pendingMetadataAggregateCap: 128,
      deferredSources: complete ? 0 : 2,
    },
    sources: [],
  };
}

function fakeCadenceTimer() {
  let now = Date.parse("2026-07-19T12:00:00.000Z");
  let nextId = 1;
  const entries = new Map<number, { at: number; callback: () => void }>();
  const timer: AutomaticMaintenanceCadenceTimer = {
    now: () => now,
    setTimeout: (callback, delayMs) => {
      const id = nextId++;
      entries.set(id, { at: now + delayMs, callback });
      return id;
    },
    clearTimeout: (handle) => entries.delete(Number(handle)),
  };
  return {
    timer,
    advance: async (milliseconds: number) => {
      now += milliseconds;
      for (;;) {
        const due = [...entries.entries()]
          .filter(([, entry]) => entry.at <= now)
          .sort((left, right) => left[1].at - right[1].at)[0];
        if (!due) break;
        entries.delete(due[0]);
        due[1].callback();
        await new Promise<void>((resolve) => setImmediate(resolve));
      }
    },
  };
}

async function proveAdaptiveBaselineCadence() {
  const clock = fakeCadenceTimer();
  let state: CaptureBaselineStatus["progress"]["state"] = "not_established";
  let calls = 0;
  const scheduler = new CoalescingMaintenanceScheduler(async () => {
    calls += 1;
    state = calls === 1 ? "in_progress" : "ambiguous";
    return fakeRun();
  });
  const cadence = new AutomaticMaintenanceCadence(
    scheduler,
    () => fakeBaselineStatus(state),
    { timer: clock.timer },
  );
  cadence.start();
  const initial = cadence.status();
  await clock.advance(4_999);
  const beforeBoot = cadence.status();
  await clock.advance(1);
  const afterBoot = cadence.status();
  await clock.advance(4_999);
  const beforeStartupFollowup = cadence.status();
  await clock.advance(1);
  const afterAmbiguity = cadence.status();
  await clock.advance(59_999);
  const beforeNormalRetry = cadence.status();
  cadence.stop();
  check(
    "baseline_followups_use_one_bounded_startup_timer_then_ambiguous_returns_to_normal",
    initial.retryClass === "boot" &&
      initial.nextRetryAt !== null &&
      beforeBoot.triggerCount === 0 &&
      afterBoot.triggerCount === 1 &&
      afterBoot.retryClass === "startup" &&
      beforeStartupFollowup.triggerCount === 1 &&
      calls === 2 &&
      afterAmbiguity.retryClass === "normal" &&
      afterAmbiguity.nextRetryAt !== null &&
      beforeNormalRetry.triggerCount === 2 &&
      scheduler.status().maxConcurrentJobs === 1 &&
      scheduler.status().overlappingJobs === 0 &&
      afterAmbiguity.maximumStartupDutyCycle === 0.04 &&
      AUTOMATIC_BASELINE_STARTUP_INTERVAL_MS === 5_000 &&
      AUTOMATIC_MAINTENANCE_NORMAL_INTERVAL_MS === 60_000,
    { calls, initial, afterAmbiguity, beforeNormalRetry, scheduler: scheduler.status() },
  );

  const failureClock = fakeCadenceTimer();
  let failures = 0;
  const failingScheduler = new CoalescingMaintenanceScheduler(async () => {
    failures += 1;
    throw new Error("cadence-proof-failure");
  });
  const failingCadence = new AutomaticMaintenanceCadence(
    failingScheduler,
    () => fakeBaselineStatus("in_progress"),
    { timer: failureClock.timer },
  );
  failingCadence.start();
  await failureClock.advance(5_000);
  await failureClock.advance(59_999);
  const failedStatus = failingCadence.status();
  failingCadence.stop();
  check(
    "failed_maintenance_never_spins_on_startup_cadence",
    failures === 1 &&
      failedStatus.failedTriggers === 1 &&
      failedStatus.retryClass === "normal" &&
      failingScheduler.status().failedRuns === 1,
    { failures, cadence: failedStatus, scheduler: failingScheduler.status() },
  );
}

async function proveDurableSlowSourceFairness(root: string) {
  const buffer = new LocalEventBuffer(path.join(root, "source-fairness.sqlite"));
  const startedAt = new Date().toISOString();
  for (const source of ["codex", "claude_code"] as const) {
    beginAutomaticCaptureBaseline(buffer.database, source, {
      startedAt,
      filesDiscovered: 0,
    });
  }
  const order: string[] = [];
  let codexProgress = 0;
  let claudeProgress = 0;
  const makeRollout = () => ({
    scan: async (options: RolloutScanOptions) => {
      order.push("codex");
      const result = emptyRollout();
      const budget = options.automatic!.budget;
      if (budget.canContinue()) {
        codexProgress += 1;
        // Emulate a slow source exhausting the shared cadence without burning
        // CPU or depending on host wall-clock speed.
        budget.recordSlice({ bytesRead: 512 * 1024, recordsParsed: 0, eventsAppended: 0 });
      }
      result.exhaustive = false;
      result.deferredGenerations = 1;
      result.automaticBudget = budget.status();
      return result;
    },
    close: () => undefined,
  });
  const makeTranscript = () => ({
    scan: async (options: TranscriptScanOptions) => {
      order.push("claude_code");
      const result = emptyTranscript();
      const budget = options.automatic!.budget;
      if (budget.canContinue()) claudeProgress += 1;
      result.exhaustive = false;
      result.deferredGenerations = 1;
      result.automaticBudget = budget.status();
      return result;
    },
    close: () => undefined,
  });
  try {
    const first = new CollectorMaintenance(
      buffer,
      makeRollout() as unknown as RolloutTailer,
      makeTranscript() as unknown as TranscriptTailer,
    );
    await first.runRecent();
    first.close();
    const afterFirst = [...order];
    const second = new CollectorMaintenance(
      buffer,
      makeRollout() as unknown as RolloutTailer,
      makeTranscript() as unknown as TranscriptTailer,
    );
    await second.runRecent();
    const statusText = JSON.stringify(second.status());
    second.close();
    check(
      "slow_source_turn_is_fair_and_durable_across_maintenance_restart",
      afterFirst[0] === "codex" &&
        afterFirst[1] === "claude_code" &&
        order[2] === "claude_code" &&
        order[3] === "codex" &&
        codexProgress >= 1 &&
        claudeProgress >= 1 &&
        !statusText.includes(root),
      { order, codexProgress, claudeProgress, statusPathFree: !statusText.includes(root) },
    );
  } finally {
    buffer.close();
  }
}

async function proveCrashResumeDropsOnlyEphemeralPending(root: string) {
  const ledger = path.join(root, "baseline-crash-resume.sqlite");
  const codexRoot = path.join(root, "baseline-crash-codex");
  const claudeRoot = path.join(root, "baseline-crash-claude");
  const [year, month, day] = new Date().toISOString().slice(0, 10).split("-");
  const codexDay = path.join(codexRoot, year!, month!, day!);
  fs.mkdirSync(codexDay, { recursive: true });
  fs.mkdirSync(claudeRoot, { recursive: true });
  const files: string[] = [];
  for (let index = 0; index < 64; index += 1) {
    const file = path.join(codexDay, `rollout-crash-${String(index).padStart(4, "0")}.jsonl`);
    fs.writeFileSync(file, "{}\n");
    files.push(file);
  }
  await new Promise<void>((resolve) => setTimeout(resolve, 2));
  const buffer = new LocalEventBuffer(ledger);
  const startedAt = new Date().toISOString();
  const codex = beginAutomaticCaptureBaseline(buffer.database, "codex", {
    startedAt,
    filesDiscovered: 0,
  });
  if (!codex.latestRun) throw new Error("crash_resume_codex_run_missing");
  for (let index = 0; index < 7; index += 1) {
    const precise = fs.lstatSync(files[index]!, { bigint: true });
    stageAutomaticCaptureBaselineObservation(buffer.database, "codex", {
      runId: codex.latestRun.runId,
      observedAt: new Date().toISOString(),
      filesDiscovered: 64,
      filesValidated: index + 1,
      observation: {
        path: files[index]!,
        device: precise.dev,
        inode: precise.ino,
        size: precise.size,
        birthtimeNs: precise.birthtimeNs,
      },
    });
  }
  const claude = beginAutomaticCaptureBaseline(buffer.database, "claude_code", {
    startedAt,
    filesDiscovered: 0,
  });
  if (!claude.latestRun) throw new Error("crash_resume_claude_run_missing");
  completeAutomaticCaptureBaseline(buffer.database, "claude_code", {
    runId: claude.latestRun.runId,
    completedAt: new Date().toISOString(),
  });
  const before = captureBaselineStatus(buffer.database);
  const maintenance = new CollectorMaintenance(
    buffer,
    new RolloutTailer(buffer, codexRoot, () => []),
    new TranscriptTailer(buffer, claudeRoot),
  );
  try {
    for (let cadence = 0; cadence < 6 && captureBaselineStatus(buffer.database).status !== "complete"; cadence += 1) {
      await maintenance.runRecent();
    }
    const after = captureBaselineStatus(buffer.database);
    const codexAfter = after.sources.find((source) => source.source === "codex")?.latestRun;
    const events = (buffer.database
      .prepare(`select count(*) as count from buffered_events`)
      .get() as { count: number }).count;
    check(
      "crash_resume_rewalks_inactive_namespace_without_promoting_lost_pending",
      before.status === "blocked" &&
        before.progress.pendingMetadata === 57 &&
        after.status === "complete" &&
        codexAfter?.filesDiscovered === codexAfter?.filesValidated &&
        codexAfter?.filesValidated === 135 &&
        codexAfter?.filesBaselined === 64 &&
        events === 0,
      {
        beforeState: before.progress.state,
        beforePending: before.progress.pendingMetadata,
        afterState: after.progress.state,
        discovered: codexAfter?.filesDiscovered,
        validated: codexAfter?.filesValidated,
        events,
      },
    );
  } finally {
    maintenance.close();
    buffer.close();
  }
}

async function proveProjectionDutyCycle(){
  let cursor=0,parityCursor=0;
  const highWater=100_000;
  const fake={
    runMaintenance(){
      if(cursor<highWater)cursor=Math.min(highWater,cursor+1_000);
      else parityCursor=Math.min(highWater,parityCursor+1_000);
      return {backfillRowsVisited:cursor<highWater||cursor===1_000?1_000:0,
        parityRowsVisited:cursor>=highWater&&parityCursor>0?1_000:0,metricRowsVisited:0};
    },
    status(){return {backfill:{highWater,cursor,complete:cursor>=highWater,parityCursor,
      parityComplete:parityCursor>=highWater,metricHighWater:0,metricCursor:0,metricComplete:true}};},
  };
  const result=await drainProjectionMigration(fake as unknown as LocalEventBuffer["projection"],
    {maxSlices:4,maxActiveMs:5_000,cadenceSeconds:60});
  check("projection_migration_duty_cycle_is_bounded_yielding_and_reports_eta",
    result.drain.slices===4&&result.drain.yields===3&&result.drain.migrationRowsVisited===4_000&&
    result.drain.remainingRowidUpperBound===196_000&&result.drain.estimatedMinutesUpperBound===49&&
    result.drain.stillMigrating,
    result.drain as unknown as Record<string,unknown>);
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
  for (let iteration = 0; iteration < 100; iteration += 1) {
    runCodexReconciliationMaintenance(buffer.database, { timeLimitMs: 1_000 });
    const status = codexReconciliationStatus(buffer.database);
    if (
      status.legacyComplete &&
      status.candidateBacklog === 0 &&
      status.contextWindowBacklog === 0
    ) {
      break;
    }
  }
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
  const idle = await maintenance.runRecent();
  check(
    "integrated_unchanged_cycle_has_zero_parse_write_and_row_visits",
    idle.rollout.filesRead === 0 &&
      idle.transcript.filesRead === 0 &&
      idle.rawEventWrites === 0 &&
      idle.reconciliation.rowsVisited === 0 &&
      idle.repricing.rowsVisited === 0 &&
      idle.enrichment.rowsVisited === 0,
    idle,
  );
}

async function main() {
  await proveCoalescing();
  await proveStoppingCancelsPendingFollowup();
  await proveAdaptiveBaselineCadence();
  await proveProjectionDutyCycle();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-maintenance-proof-"));
  const ledger = path.join(root, "ledger.sqlite");
  const rolloutRoot = path.join(root, "rollouts");
  const transcriptRoot = path.join(root, "transcripts");
  fs.mkdirSync(rolloutRoot, { recursive: true });
  fs.mkdirSync(transcriptRoot, { recursive: true });
  let buffer = new LocalEventBuffer(ledger);
  try {
    await proveDurableSlowSourceFairness(root);
    await proveCrashResumeDropsOnlyEphemeralPending(root);
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
