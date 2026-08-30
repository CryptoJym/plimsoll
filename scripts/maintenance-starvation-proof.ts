#!/usr/bin/env node
/**
 * Failure-hunting proof for issue #181: maintenance deadline starvation.
 *
 * Three defects are pinned here:
 *   1. RESUMABILITY — a fixture enrichment batch larger than one deadline
 *      window must make measurable, disjoint progress across consecutive
 *      runs, and a deadline kill must durably record what the killed run had
 *      reached instead of discarding the whole batch silently.
 *   2. QUARANTINE MISATTRIBUTION — only a candidate whose own measured
 *      time-on-stage proves slowness may be quarantined. A fast candidate
 *      that merely happened to be on stage at kill time is recorded as
 *      UNKNOWN and never applied as quarantine.
 *   3. NO SILENT STARVATION — the durable starvation receipt must reflect a
 *      seeded pending-enrichment backlog and the deadline-kill rate.
 *
 * The proof uses only temporary homes, ledgers, injected child adapters, and
 * a manual clock. It never reads the live collector ledger, starts a
 * LaunchAgent, or changes installed config.
 *
 * Run with Node 22:
 *   pnpm exec tsx scripts/maintenance-starvation-proof.ts
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { guardProofCompletion } from "./lib/proof-completion";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import {
  MaintenanceProcessBoundary,
  type MaintenanceBoundaryChild,
  type MaintenanceBoundaryOptions,
} from "../packages/collector-cli/src/maintenance-boundary";
import {
  maintenanceCandidateHash,
  type MaintenanceProgressStage,
} from "../packages/collector-cli/src/maintenance-progress";
import {
  MAINTENANCE_PROTOCOL_SCHEMA,
  type MaintenanceRunRequest,
} from "../packages/collector-cli/src/maintenance-protocol";
import {
  maintenanceBacklogSnapshot,
  maintenanceStarvationReceipt,
  recordGitContextBatchProgress,
  recordMaintenanceDeadlineBlame,
  recordMaintenanceDeadlineKill,
} from "../packages/collector-cli/src/maintenance-starvation";
import {
  boundRepoContextCarryOver,
  gitContextBudgetMs,
  resolveRepoContextBatch,
} from "../packages/collector-cli/src/maintenance-worker";
import {
  REPO_CONTEXT_RESOLVER_VERSION,
  type RepoContextRequest,
  type RepoContextResult,
} from "../packages/collector-cli/src/repo-context";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import type { MaintenanceRunOutcome } from "../packages/collector-cli/src/maintenance";

type Check = { name: string; passed: true; detail: Record<string, unknown> };
type TimerHandle = ReturnType<typeof setTimeout>;

const checks: Check[] = [];
const SENTINEL = "maintenance-starvation-private-path-sentinel";

function pass(name: string, detail: Record<string, unknown>) {
  checks.push({ name, passed: true, detail });
}

function tick() {
  return new Promise<void>((resolve) => setImmediate(resolve));
}

async function rejectsWith(promise: Promise<unknown>, message: string) {
  await assert.rejects(promise, (error: unknown) => (
    error instanceof Error && error.message === message
  ));
}

function outcome(rawEventWrites = 1): MaintenanceRunOutcome {
  return {
    recentOnly: true,
    rollout: {
      filesRead: 1,
      parseErrors: 0,
      eventsAppended: rawEventWrites,
      activity: { discoveryEntries: 1 },
    },
    transcript: {
      filesRead: 0,
      parseErrors: 0,
      eventsAppended: 0,
      activity: { discoveryEntries: 0 },
    },
    reconciliation: { rowsChanged: 0, rowsVisited: 0 },
    repricing: { repriced: 0, rowsVisited: 0 },
    enrichment: { backward: 0, forward: 0, rowsVisited: 0 },
    rawEventWrites,
    stageTimings: {
      codexCaptureMs: rawEventWrites,
      claudeCaptureMs: 2,
      reconciliationMs: 3,
      repricingMs: 4,
      enrichmentMs: 5,
      projectionDrainMs: 6,
      totalMs: 21 + rawEventWrites,
    },
  };
}

class FakeChild {
  readonly pid: number;
  connected = true;
  readonly sent: unknown[] = [];
  readonly signals: NodeJS.Signals[] = [];
  onSend: ((message: unknown) => void) | null = null;
  private listeners = new Map<string, Array<(...args: unknown[]) => void>>();
  private closed = false;

  constructor(pid: number) {
    this.pid = pid;
  }

  on(event: string, listener: (...args: unknown[]) => void) {
    const list = this.listeners.get(event) ?? [];
    list.push(listener);
    this.listeners.set(event, list);
    return this;
  }

  removeListener(event: string, listener: (...args: unknown[]) => void) {
    this.listeners.set(
      event,
      (this.listeners.get(event) ?? []).filter((entry) => entry !== listener),
    );
    return this;
  }

  emit(event: string, ...args: unknown[]) {
    for (const listener of [...(this.listeners.get(event) ?? [])]) listener(...args);
    return true;
  }

  send(message: unknown, callback?: (error: Error | null) => void) {
    this.sent.push(message);
    this.onSend?.(message);
    // A real worker closes when told to shut down. Without this the boundary
    // waits on its termination grace timer, and under a manual clock that
    // timer never fires — the proof would hang instead of asserting.
    if ((message as { type?: string })?.type === "shutdown") {
      queueMicrotask(() => this.close());
    }
    queueMicrotask(() => callback?.(null));
    return true;
  }

  kill(signal: NodeJS.Signals = "SIGTERM") {
    this.signals.push(signal);
    queueMicrotask(() => this.close());
    return true;
  }

  close() {
    if (this.closed) return;
    this.closed = true;
    this.connected = false;
    this.emit("close");
  }
}

function asBoundaryChild(child: FakeChild) {
  return child as unknown as MaintenanceBoundaryChild;
}

function ready(child: FakeChild, spawnNonce: string) {
  queueMicrotask(() => child.emit("message", {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "ready",
    spawnNonce,
  }));
}

class ManualClock {
  nowMs = Date.parse("2026-08-22T12:00:00.000Z");
  private ordinal = 0;
  private timers = new Map<object, { due: number; ordinal: number; callback: () => void }>();

  readonly now = () => this.nowMs;

  readonly setTimer = (callback: () => void, delayMs: number) => {
    const handle = {};
    this.timers.set(handle, {
      due: this.nowMs + delayMs,
      ordinal: this.ordinal++,
      callback,
    });
    return handle as TimerHandle;
  };

  readonly clearTimer = (handle: TimerHandle) => {
    this.timers.delete(handle as unknown as object);
  };

  advanceBy(ms: number) {
    this.nowMs += ms;
    for (;;) {
      const due = [...this.timers.entries()]
        .filter(([, timer]) => timer.due <= this.nowMs)
        .sort((a, b) => a[1].due - b[1].due || a[1].ordinal - b[1].ordinal)[0];
      if (!due) break;
      this.timers.delete(due[0]);
      due[1].callback();
    }
  }
}

function fakeBoundary(
  factory: (spawnNonce: string, spawnIndex: number) => FakeChild,
  overrides: Partial<MaintenanceBoundaryOptions> = {},
) {
  let spawns = 0;
  const children: FakeChild[] = [];
  const boundary = new MaintenanceProcessBoundary({
    entryPath: "unused-by-injected-child",
    deadlineMs: 100,
    readyDeadlineMs: 100,
    termGraceMs: 10,
    killGraceMs: 10,
    initialCircuitMs: 50,
    escalatedCircuitMs: 200,
    fingerprint: async (pid) => `proof-fingerprint-${pid}`,
    ...overrides,
    spawnChild: (spawnNonce) => {
      const child = factory(spawnNonce, spawns++);
      children.push(child);
      return asBoundaryChild(child);
    },
  });
  return { boundary, children, spawnCount: () => spawns };
}

function progressFrame(
  request: MaintenanceRunRequest,
  sequence: number,
  stage: MaintenanceProgressStage,
  source: "codex" | "claude_code",
  candidateHash: string | null,
) {
  return {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "progress" as const,
    generation: request.generation,
    nonce: request.nonce,
    sequence,
    stage,
    source,
    candidateHash,
  };
}

function resultReceipt(request: MaintenanceRunRequest, value = outcome()) {
  return {
    schema: MAINTENANCE_PROTOCOL_SCHEMA,
    type: "result" as const,
    generation: request.generation,
    nonce: request.nonce,
    sequence: 1,
    result: value,
    repoContexts: [],
  };
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
      ? { git: { remoteUrlHash: options.repoHash, branchHash: options.branchHash } }
      : {},
  });
}

/**
 * Scenario 1a: a fixture batch larger than one window makes measurable,
 * disjoint progress across consecutive runs — never repeating from zero.
 */
async function resumableBatchProgressProof(root: string) {
  const ledger = path.join(root, "resume-batch.sqlite");
  const emptyRoot = path.join(root, "empty-scan-roots");
  fs.mkdirSync(emptyRoot, { recursive: true });
  const buffer = new LocalEventBuffer(ledger);

  // Fixture batch: 10 stitchable sessions. One automatic cycle enriches at
  // most 4 sessions / 32 events, so the batch spans multiple windows by
  // construction.
  const sessions = Array.from({ length: 10 }, (_, index) =>
    `019e3000-0000-7000-8000-${String(index + 1).padStart(12, "0")}`);
  for (let index = 0; index < sessions.length; index += 1) {
    buffer.append(event({
      id: `resume-link-${index}`,
      sessionId: sessions[index]!,
      observedAt: `2026-08-01T10:${String(index).padStart(2, "0")}:00.000Z`,
      repoHash: `sha256:resume-repo-${index}`,
      branchHash: `sha256:resume-branch-${index}`,
    }));
    buffer.append(event({
      id: `resume-token-${index}`,
      sessionId: sessions[index]!,
      observedAt: `2026-08-01T10:${String(index).padStart(2, "0")}:30.000Z`,
      model: "gpt-5.4",
      inputTokens: 100,
      outputTokens: 10,
      costUsd: 0.01,
    }));
  }

  const maintenance = new CollectorMaintenance(
    buffer,
    new RolloutTailer(buffer, emptyRoot, () => []),
    new TranscriptTailer(buffer, emptyRoot),
  );

  const stitchedPerRun: number[] = [];
  const stitchedSessionsPerRun: string[][] = [];
  let previouslyStitched = new Set<string>();
  for (let run = 0; run < 4; run += 1) {
    const result = await maintenance.runRecent();
    const ids = buffer.database.prepare(
      `select session_id as sessionId from buffered_events
       where id like 'resume-token-%' and repo_hash is not null`,
    ).all() as Array<{ sessionId: string }>;
    // Count only sessions newly committed by THIS run; already-stitched rows
    // persist in the ledger, so the cumulative query alone would double-count.
    const newlyStitched = ids
      .map((row) => row.sessionId)
      .filter((sessionId) => !previouslyStitched.has(sessionId))
      .sort();
    stitchedPerRun.push(result.enrichment.backward + result.enrichment.forward);
    stitchedSessionsPerRun.push(newlyStitched);
    previouslyStitched = new Set(ids.map((row) => row.sessionId));
  }
  const totalStitched = stitchedPerRun.reduce((total, value) => total + value, 0);
  assert.equal(totalStitched, sessions.length, "every seeded token row must stitch");

  // Progress is disjoint across runs: later runs continue where earlier ones
  // committed, they do not repeat already-stitched work from zero.
  const seen = new Set<string>();
  let overlap = 0;
  for (const runSessions of stitchedSessionsPerRun) {
    for (const sessionId of runSessions) {
      if (seen.has(sessionId)) overlap += 1;
      seen.add(sessionId);
    }
  }
  const firstRunCount = stitchedSessionsPerRun[0]!.length;
  assert.ok(firstRunCount >= 1 && firstRunCount < sessions.length,
    `first run must be bounded below the full batch (stitched ${firstRunCount})`);
  assert.equal(seen.size, sessions.length);
  assert.equal(overlap, 0, "no session may be restitched across runs");

  const dirtyAfter = (buffer.database.prepare(
    `select count(*) as n from repo_enrichment_dirty`,
  ).get() as { n: number }).n;
  assert.equal(dirtyAfter, 0, "the seeded batch must fully drain");
  assert.equal(totalStitched > firstRunCount, true,
    "consecutive runs must make measurable progress beyond one window");
  pass("fixture_batch_larger_than_one_window_makes_disjoint_progress_across_runs", {
    seededSessions: sessions.length,
    stitchedPerRun,
    firstRunCount,
    totalStitched,
    overlap,
    dirtyQueueAfter: dirtyAfter,
  });
  maintenance.close();
  buffer.close();
}

/**
 * Scenario 1b: the `git_context` batch — the one all-or-nothing unit left in
 * the cycle — is bounded, committed per context, and resumable.
 *
 * Pre-#181 the whole batch was resolved into an array and committed once at
 * the end, so a kill anywhere inside the loop discarded every context the run
 * had already resolved and the next cycle restarted the same batch from zero.
 */
async function resumableGitContextBatchProof(root: string) {
  const ledger = path.join(root, "git-context-resume.sqlite");
  let buffer = new LocalEventBuffer(ledger);
  const CONTEXTS = 6;
  const PER_CONTEXT_MS = 10;
  // Deliberately smaller than the whole batch: this fixture is larger than
  // one window by construction.
  const BUDGET_MS = 25;
  const EXPECTED_FIRST_PASS = 3;

  const contextId = (index: number) => `repoctx:v1:${index.toString(16).padStart(64, "0")}`;
  const repoHash = (index: number) => `sha256:${(index + 1).toString(16).padStart(64, "0")}`;
  const requests: RepoContextRequest[] = Array.from({ length: CONTEXTS }, (_, index) => ({
    contextId: contextId(index),
    source: "codex" as const,
    cwd: path.join(root, `git-context-repo-${index}`),
  }));
  for (const request of requests) {
    buffer.database.prepare(
      `insert into repo_context_inflight (context_id, started_at, owner)
       values (?, ?, 'child')`,
    ).run(request.contextId, "2026-08-03T09:00:00.000Z");
  }

  const resultRows = () => (buffer.database.prepare(
    `select count(*) as n from repo_context_results`,
  ).get() as { n: number }).n;
  const inflightIds = () => (buffer.database.prepare(
    `select context_id as contextId from repo_context_inflight order by context_id`,
  ).all() as Array<{ contextId: string }>).map((row) => row.contextId);

  let clockMs = 0;
  const now = () => clockMs;
  let resolverCalls = 0;
  const resolvedIds: string[] = [];
  // Observed at the moment each context begins resolving, BEFORE its own
  // result exists. Under an end-of-batch commit every observation is zero.
  const durableResultsWhileResolving: number[] = [];
  const resolveRequests = (batch: readonly RepoContextRequest[]): RepoContextResult[] => {
    resolverCalls += 1;
    durableResultsWhileResolving.push(resultRows());
    clockMs += PER_CONTEXT_MS;
    return batch.map((request) => {
      const index = requests.findIndex((candidate) => candidate.contextId === request.contextId);
      resolvedIds.push(request.contextId);
      return {
        contextId: request.contextId,
        repoHash: repoHash(index),
        branchHash: null,
        headSha: null,
        resolvedAt: "2026-08-03T09:00:00.000Z",
        resolverVersion: REPO_CONTEXT_RESOLVER_VERSION,
      };
    });
  };
  const passOptions = () => ({
    quarantine: null,
    reportProgress: () => true,
    recordRepoLabel: () => undefined,
    resolveRequests,
    budgetMs: BUDGET_MS,
    now,
    commit: (committed: RepoContextResult[]) => {
      buffer.applyRepoContextResults(committed);
    },
    deferrable: true,
  });

  const passOne = resolveRepoContextBatch(requests, passOptions());
  recordGitContextBatchProgress(buffer.database, {
    committed: passOne.results.length,
    deferred: passOne.deferred.length,
  });

  assert.equal(passOne.resolved, EXPECTED_FIRST_PASS,
    "one budget window must resolve only part of the batch");
  assert.equal(passOne.budgetExhausted, true, "the window must be the thing that stopped it");
  assert.equal(passOne.deferred.length, CONTEXTS - EXPECTED_FIRST_PASS,
    "the remainder must be deferred for the next cycle");
  assert.deepEqual(durableResultsWhileResolving, [0, 1, 2],
    "each resolved context must be durable before the next one starts");
  assert.equal(resultRows(), EXPECTED_FIRST_PASS,
    "the partial batch must already be committed when the window closes");
  assert.deepEqual(
    inflightIds(),
    passOne.deferred.map((request) => request.contextId).sort(),
    "deferred contexts keep their inflight rows: deferral is not a burn",
  );
  assert.equal(
    passOne.results.some((result) => result.repoHash === null),
    false,
    "a deferred context must never be committed as an UNKNOWN result",
  );
  const afterPassOne = maintenanceStarvationReceipt(buffer.database);
  assert.equal(afterPassOne.gitContext.committedTotal, EXPECTED_FIRST_PASS);
  assert.equal(afterPassOne.gitContext.lastDeferred, CONTEXTS - EXPECTED_FIRST_PASS);

  // Model the deadline kill: the worker dies here. Reopen the ledger and prove
  // the committed half survived rather than vanishing with the process.
  buffer.close();
  buffer = new LocalEventBuffer(ledger);
  assert.equal(resultRows(), EXPECTED_FIRST_PASS,
    "committed contexts must survive the process that resolved them");

  const passTwo = resolveRepoContextBatch(passOne.deferred, passOptions());
  recordGitContextBatchProgress(buffer.database, {
    committed: passTwo.results.length,
    deferred: passTwo.deferred.length,
  });

  assert.equal(passTwo.resolved, CONTEXTS - EXPECTED_FIRST_PASS);
  assert.equal(passTwo.deferred.length, 0, "the second window must drain the batch");
  assert.equal(resultRows(), CONTEXTS);
  assert.equal(resolverCalls, CONTEXTS,
    "a resumed batch must not re-resolve what it already committed");
  assert.equal(new Set(resolvedIds).size, CONTEXTS, "every context resolved exactly once");
  assert.deepEqual(durableResultsWhileResolving, [0, 1, 2, 3, 4, 5],
    "the durable committed count must rise monotonically across both windows");
  assert.deepEqual(inflightIds(), [], "a drained batch leaves no inflight row behind");
  const afterPassTwo = maintenanceStarvationReceipt(buffer.database);
  assert.equal(afterPassTwo.gitContext.committedTotal, CONTEXTS,
    "the committed total must accumulate across windows, not reset");
  assert.equal(afterPassTwo.gitContext.lastDeferred, 0);

  // The budget is a share of the EXISTING deadline. #181 is not fixed by
  // enlarging the 30s the child overran.
  assert.equal(gitContextBudgetMs(30_000), 10_000);
  assert.equal(gitContextBudgetMs(100), 50);
  assert.equal(gitContextBudgetMs(1), 1, "the budget may never exceed the deadline");
  const cliSource = fs.readFileSync(
    path.resolve("packages/collector-cli/src/cli.ts"),
    "utf8",
  );
  assert.match(cliSource, /deadlineMs:\s*30_000/,
    "the production deadline must still be 30s: resumability is the fix, not a bigger constant");

  // The carry-over cursor is bounded, deduplicated and order-preserving.
  const carried = requests.slice(0, 4);
  const carryOver = boundRepoContextCarryOver(carried, [requests[3]!, requests[4]!], 5);
  assert.deepEqual(
    carryOver.kept.map((request) => request.contextId),
    [...carried, requests[4]!].map((request) => request.contextId),
    "already-waiting contexts keep their place and duplicates collapse",
  );
  const overBound = boundRepoContextCarryOver(requests, [], 2);
  assert.equal(overBound.kept.length, 2);
  assert.equal(overBound.overflow.length, CONTEXTS - 2,
    "the carry-over is bounded and returns its overflow for exact retirement");

  pass("git_context_batch_is_bounded_committed_per_context_and_resumable", {
    seededContexts: CONTEXTS,
    firstWindowResolved: passOne.resolved,
    firstWindowDeferred: passOne.deferred.length,
    durableResultsWhileResolving,
    resolverCalls,
    committedTotal: afterPassTwo.gitContext.committedTotal,
    budgetMsForProductionDeadline: gitContextBudgetMs(30_000),
    productionDeadlineUnchangedMs: 30_000,
  });
  buffer.close();
}

/**
 * Scenario 1c: a deadline kill durably records what the killed run had
 * reached (checkpoint + kill counter) instead of discarding everything.
 */
async function deadlineKillRecordsProgressProof(root: string) {
  const ledger = path.join(root, "deadline-checkpoint.sqlite");
  const buffer = new LocalEventBuffer(ledger);
  const clock = new ManualClock();
  const slowCandidate = maintenanceCandidateHash(`${SENTINEL}/slow.jsonl`);

  const harness = fakeBoundary((spawnNonce, index) => {
    const child = new FakeChild(30_000 + index);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      queueMicrotask(() => child.emit("message",
        progressFrame(request, 1, "jsonl_open", "codex", slowCandidate)));
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: clock.now,
    setTimer: clock.setTimer,
    clearTimer: clock.clearTimer,
    deadlineMs: 100,
    initialCircuitMs: 50,
    escalatedCircuitMs: 200,
    onDeadline: (info) => {
      recordMaintenanceDeadlineKill(buffer.database);
      recordMaintenanceDeadlineBlame(buffer.database, {
        at: new Date(clock.now()).toISOString(),
        source: info.progress?.source ?? null,
        stage: info.progress?.stage ?? null,
        heldMs: info.heldMs,
        attribution: info.attribution,
      });
    },
  });

  const failed = harness.boundary.run();
  void failed.catch(() => undefined);
  await tick();
  clock.advanceBy(100);
  await tick();
  await rejectsWith(failed, "maintenance_deadline_exceeded");

  const receipt = maintenanceStarvationReceipt(buffer.database);
  assert.equal(receipt.deadlineKills, 1,
    "the deadline kill must be counted durably");
  assert.ok(receipt.lastCheckpoint, "the killed run must leave a checkpoint");
  assert.equal(receipt.lastCheckpoint!.stage, "jsonl_open");
  assert.equal(receipt.lastCheckpoint!.source, "codex");
  assert.equal(receipt.lastCheckpoint!.attribution, "proven");
  assert.ok((receipt.lastCheckpoint!.heldMs ?? 0) >= 50,
    "the slow candidate held the stage past half the window");
  assert.equal(JSON.stringify(receipt).includes(SENTINEL), false,
    "starvation receipts must remain path-free");

  await harness.boundary.shutdown();
  pass("deadline_kill_records_partial_progress_checkpoint_and_kill_rate", {
    deadlineKills: receipt.deadlineKills,
    checkpointStage: receipt.lastCheckpoint!.stage,
    attribution: receipt.lastCheckpoint!.attribution,
    heldMs: receipt.lastCheckpoint!.heldMs,
    pathFree: true,
  });
  buffer.close();
}

/**
 * Scenario 2: quarantine blames only proven slowness.
 */
async function quarantineProvableBlameProof() {
  // Proven case: one candidate holds the stage for the whole window.
  const slowClock = new ManualClock();
  const slowCandidate = maintenanceCandidateHash(`${SENTINEL}/genuinely-slow.jsonl`);
  let slowRunRequest: MaintenanceRunRequest | null = null;
  const slowHarness = fakeBoundary((spawnNonce, index) => {
    const child = new FakeChild(31_000 + index);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      slowRunRequest = request;
      // The first child stalls after naming its candidate, which is what the
      // deadline kills. Its replacement completes, so the recovery run can be
      // observed instead of hanging the proof.
      queueMicrotask(() => child.emit("message", index === 0
        ? progressFrame(request, 1, "jsonl_open", "codex", slowCandidate)
        : resultReceipt(request)));
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: slowClock.now,
    setTimer: slowClock.setTimer,
    clearTimer: slowClock.clearTimer,
    initialCircuitMs: 50,
    escalatedCircuitMs: 200,
  });
  const slowFailed = slowHarness.boundary.run();
  void slowFailed.catch(() => undefined);
  await tick();
  slowClock.advanceBy(100);
  await tick();
  await rejectsWith(slowFailed, "maintenance_deadline_exceeded");
  const slowStatus = slowHarness.boundary.status();
  assert.equal(slowStatus.quarantine.candidateHash, slowCandidate,
    "the genuinely slow candidate must be quarantined");
  assert.equal(slowStatus.quarantine.lastBlame?.attribution, "proven");
  assert.equal(slowStatus.quarantine.unknownBlames, 0);

  // After the circuit cools, the proven quarantine crosses the recovery
  // boundary exactly once so the slow probe is skipped, not retried blindly.
  slowClock.advanceBy(50);
  const recovered = await slowHarness.boundary.run();
  assert.equal(recovered.rawEventWrites, 1);
  const recoveryQuarantine =
    (slowRunRequest as MaintenanceRunRequest | null)?.quarantine ?? null;
  assert.deepEqual(recoveryQuarantine, {
    source: "codex",
    stage: "jsonl_open",
    candidateHash: slowCandidate,
  });

  // Misattribution case: a busy capture fills the window; a FAST candidate
  // merely happens to be on stage when the kill lands. Pre-fix behavior
  // quarantined that innocent candidate; the fix records UNKNOWN instead.
  const fastClock = new ManualClock();
  const busyCandidate = maintenanceCandidateHash(`${SENTINEL}/busy.jsonl`);
  const fastCandidate = maintenanceCandidateHash(`${SENTINEL}/innocent-fast.jsonl`);
  const fastHarness = fakeBoundary((spawnNonce, index) => {
    const child = new FakeChild(32_000 + index);
    child.onSend = (raw) => {
      const request = raw as MaintenanceRunRequest;
      if (request.type !== "run") return;
      if (index > 0) {
        queueMicrotask(() => child.emit("message", resultReceipt(request)));
        return;
      }
      // One busy candidate consumes the window; the fast candidate arrives
      // 5ms before the kill and is merely the one on stage when it lands.
      queueMicrotask(() => child.emit("message",
        progressFrame(request, 1, "candidate_metadata", "codex", busyCandidate)));
      fastClock.setTimer(() => {
        child.emit("message",
          progressFrame(request, 2, "jsonl_open", "codex", fastCandidate));
      }, 95);
    };
    ready(child, spawnNonce);
    return child;
  }, {
    now: fastClock.now,
    setTimer: fastClock.setTimer,
    clearTimer: fastClock.clearTimer,
    initialCircuitMs: 50,
    escalatedCircuitMs: 200,
  });
  const fastFailed = fastHarness.boundary.run();
  void fastFailed.catch(() => undefined);
  await tick();
  // Step to 95ms so the fast candidate takes the stage BEFORE the kill lands,
  // then let the deadline fire: it held the stage for a measured 5ms.
  fastClock.advanceBy(95);
  await tick();
  fastClock.advanceBy(5);
  await tick();
  await rejectsWith(fastFailed, "maintenance_deadline_exceeded");
  const fastStatus = fastHarness.boundary.status();
  assert.equal(fastStatus.quarantine.candidateHash, null,
    "the fast candidate must NOT be quarantined merely for being on stage");
  assert.equal(fastStatus.quarantine.lastBlame?.candidateHash, fastCandidate,
    "the unproven blame must still be recorded, naming its subject");
  assert.equal(fastStatus.quarantine.lastBlame?.attribution, "unknown");
  const fastHeldMs = fastStatus.quarantine.lastBlame?.heldMs ?? Infinity;
  assert.ok(fastHeldMs > 0,
    "the fast candidate really was the one on stage when the kill landed");
  assert.ok(fastHeldMs < 50,
    "the fast candidate demonstrably held the stage far less than the window");
  assert.equal(fastStatus.quarantine.unknownBlames, 1);

  // The unproven blame must not be APPLIED: no quarantine crosses recovery.
  fastClock.advanceBy(50);
  let recoverySawQuarantine: unknown = "no-request";
  await fastHarness.boundary.run(); // completes against a fresh child
  const secondChild = fastHarness.children[1]!;
  for (const message of secondChild.sent) {
    const request = message as MaintenanceRunRequest;
    if (request.type === "run") recoverySawQuarantine = request.quarantine;
  }
  assert.equal(recoverySawQuarantine, null,
    "an UNKNOWN blame must never cross the recovery boundary as quarantine");
  assert.equal(JSON.stringify(fastStatus.quarantine).includes(SENTINEL), false,
    "quarantine receipts must remain path-free");

  await slowHarness.boundary.shutdown();
  await fastHarness.boundary.shutdown();
  pass("quarantine_blames_only_provable_slow_candidates", {
    slowCandidateQuarantined: slowStatus.quarantine.candidateHash === slowCandidate,
    slowAttribution: slowStatus.quarantine.lastBlame?.attribution,
    provenQuarantineCrossedRecoveryOnce: recoveryQuarantine?.candidateHash === slowCandidate,
    fastCandidateQuarantined: false,
    fastAttribution: fastStatus.quarantine.lastBlame?.attribution,
    fastHeldMs: fastStatus.quarantine.lastBlame?.heldMs,
    unknownBlames: fastStatus.quarantine.unknownBlames,
    pathFree: true,
  });
}

/**
 * Scenario 3: the starvation receipt reflects reality on a seeded backlog.
 */
async function starvationReceiptReflectsBacklogProof(root: string) {
  const ledger = path.join(root, "backlog.sqlite");
  const buffer = new LocalEventBuffer(ledger);
  const contextId = (hex: string) => `repoctx:v1:${hex.padEnd(64, "0").slice(0, 64)}`;
  const FILL_PENDING_SEEDED = 6;
  const DIRTY_SESSIONS_SEEDED = 3;

  for (let index = 0; index < FILL_PENDING_SEEDED; index += 1) {
    buffer.append(event({
      id: `backlog-token-${index}`,
      sessionId: `019e4000-0000-7000-8000-${String(index).padStart(12, "0")}`,
      observedAt: "2026-08-02T09:00:00.000Z",
      model: "gpt-5.4",
      inputTokens: 10,
      outputTokens: 1,
      costUsd: 0.001,
    }));
    buffer.database.prepare(
      `insert into repo_context_event_links (event_id, context_id) values (?, ?)`,
    ).run(`backlog-token-${index}`, contextId(index.toString(16)));
  }
  // Appending a session-bearing event enqueues its own dirty row, so the
  // seeded rows are counted on top of what the fixture already produced.
  const autoDirtySessions = (buffer.database.prepare(
    `select count(*) as n from repo_enrichment_dirty`,
  ).get() as { n: number }).n;
  for (let index = 0; index < DIRTY_SESSIONS_SEEDED; index += 1) {
    buffer.database.prepare(
      `insert into repo_enrichment_dirty
         (session_id, cursor_rowid, queued_at, updated_at)
       values (?, 0, ?, ?)`,
    ).run(
      `019e5000-0000-7000-8000-${String(index).padStart(12, "0")}`,
      "2026-08-02T09:00:00.000Z",
      "2026-08-02T09:00:00.000Z",
    );
  }

  const before = maintenanceStarvationReceipt(buffer.database);
  assert.equal(before.deadlineKills, 0);
  assert.equal(before.backlog.fillPendingEventLinks, FILL_PENDING_SEEDED,
    "receipt must count the seeded fill_pending backlog");
  assert.equal(before.backlog.dirtyEnrichmentSessions,
    autoDirtySessions + DIRTY_SESSIONS_SEEDED,
    "receipt must count the seeded dirty-session backlog");
  assert.ok(autoDirtySessions > 0, "the fixture must produce a real dirty queue");
  assert.equal(before.starving, false,
    "no kills yet: backlog alone is not starvation");

  const snapshot = maintenanceBacklogSnapshot(buffer.database);
  assert.deepEqual(snapshot, before.backlog);

  recordMaintenanceDeadlineKill(buffer.database, new Date(0));
  recordMaintenanceDeadlineKill(buffer.database, new Date(1_000));
  const during = maintenanceStarvationReceipt(buffer.database);
  assert.equal(during.deadlineKills, 2, "kill rate must accumulate");
  assert.ok(during.lastDeadlineKillAt);
  assert.equal(during.starving, true,
    "kills over a live backlog must surface as starvation");

  // Drain the queues; the receipt must follow reality, not stick.
  buffer.database.exec(`
    update repo_context_event_links set fill_pending = 0 where fill_pending = 1;
    delete from repo_enrichment_dirty;
  `);
  const drained = maintenanceStarvationReceipt(buffer.database);
  assert.equal(drained.backlog.fillPendingEventLinks, 0);
  assert.equal(drained.backlog.dirtyEnrichmentSessions, 0);
  assert.equal(drained.starving, false,
    "draining the backlog clears the starvation flag");
  assert.equal(drained.deadlineKills, 2,
    "kill history survives the backlog drain");
  pass("starvation_receipt_reflects_seeded_backlog_and_kill_rate", {
    fillPendingSeeded: FILL_PENDING_SEEDED,
    dirtySessionsSeeded: DIRTY_SESSIONS_SEEDED,
    kills: during.deadlineKills,
    starvingDuringBacklog: during.starving,
    starvingAfterDrain: drained.starving,
  });
  buffer.close();
}

/** Every scenario above must register exactly one check. */
const EXPECTED_CHECKS = 5;

// Refuses the two silent-green modes — an early event-loop drain and a hang
// that never exits. See scripts/lib/proof-completion.ts for why each is needed.
const guard = guardProofCompletion({
  expectedChecks: EXPECTED_CHECKS,
  countChecks: () => checks.length,
});

async function main() {
  // Bound to the range package.json actually declares. Pinning one exact
  // major would turn a supported runtime into a fake red.
  const nodeMajor = Number(process.versions.node.split(".")[0]);
  assert.ok(nodeMajor >= 20 && nodeMajor < 25,
    `proof requires the declared engines range (>=20 <25), got ${process.versions.node}`);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), `${SENTINEL}-`));
  try {
    await resumableBatchProgressProof(root);
    await resumableGitContextBatchProof(root);
    await deadlineKillRecordsProgressProof(root);
    await quarantineProvableBlameProof();
    await starvationReceiptReflectsBacklogProof(root);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  guard.complete();
  const receipt = { status: "pass", checks };
  assert.equal(JSON.stringify(receipt).includes(SENTINEL), false,
    "the receipt must remain path-free");
  console.log(JSON.stringify(receipt, null, 2));
}

main().catch((error) => {
  console.error(JSON.stringify({
    status: "fail",
    error: error instanceof Error ? error.message : String(error),
    stack: error instanceof Error ? error.stack : undefined,
  }, null, 2));
  process.exitCode = 1;
});
