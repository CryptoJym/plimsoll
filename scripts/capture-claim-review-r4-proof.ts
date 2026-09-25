/**
 * eco-6hoxj.163.18 r4 — the collector findings of the r3 re-review, and the
 * Grok usage files collector 0.7.37 began to tail, each as one check that
 * fails on r3 and passes on r4. It uses only what the versions share: the
 * maintenance loop's own coverage turn (`checkCaptureCoverage`), the spool
 * state and the claim; the maintenance constructor and the claim are called by
 * their shape.
 *
 * - R3-S2: the spool loss log keeps its oldest losses, merged, as gaps.
 * - R3-S4: an append to a pre-enrollment file is uncovered from the last
 *   complete check, not from the epoch start.
 * - R3-S5: a busy file the tailer keeps up with never becomes a gap.
 * - R3-N4: every coverage turn stays within 250 ms on a large tree (60,000
 *   files per source, half of them unread), and the walk resumes until it
 *   completes.
 * - Grok: a usage file Grok wrote that the collector has not read is never
 *   attested, and reading it clears the gap.
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import {
  advanceCaptureFrontier,
  CAPTURE_COVERAGE_MAX_WORK_PER_TURN,
  CAPTURE_FRONTIER_SOURCES,
  CAPTURE_WRITE_LAG_MS,
  captureFrontier,
  CaptureCoverageWalk,
} from "../packages/collector-cli/src/capture-frontier";
import type { CaptureRoot } from "../packages/collector-cli/src/capture-root-inventory";
import { captureSpoolState, type CaptureSpoolState } from "../packages/collector-cli/src/capture-spool-state";
import { CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { hookSpoolDirectory } from "../packages/collector-cli/src/hook-spool";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { recordSpoolLoss } from "../packages/collector-cli/src/spool-losses";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { aiInteractionEventSchema } from "../packages/shared/src/index";
import { createProofCompletion } from "./lib/proof-completion";
import { incrementalCoverageChecks } from "./lib/capture-coverage-incremental";

// maintenance.ts checkCaptureCoverage steps one coverage walk per source in a
// turn: codex, claude_code and grok.
const COVERAGE_SOURCES = 3;
/** The 0.7.40 release's per-source coverage work ceiling, written here on
 * purpose instead of read from the product: the checks below hold
 * CAPTURE_COVERAGE_MAX_WORK_PER_TURN to it, so changing that constant fails
 * this proof until the new ceiling is reviewed and written here. */
const RELEASE_MAX_WORK_PER_TURN = 4_096;

const completion = createProofCompletion("capture-claim-review-r4", 11);
const results: Array<{ name: string; passed: boolean; detail: Record<string, unknown> }> = [];
const check = (name: string, passed: boolean, detail: Record<string, unknown>) => {
  completion.check(name, passed);
  results.push({ name, passed, detail });
};

type Claim = {
  through: string | null;
  unattested?: string;
  pending: number;
  dead: number;
  gaps: Array<{ from: string; to: string }>;
};

// A wall clock that can be moved forward, for "time passes" without waiting.
const RealDate = Date;
let clockShiftMs = 0;
class ShiftedDate extends RealDate {
  constructor(...args: unknown[]) {
    if (args.length === 0) super(RealDate.now() + clockShiftMs);
    else super(...(args as [string | number | Date]));
  }
  static now() {
    return RealDate.now() + clockShiftMs;
  }
}
globalThis.Date = ShiftedDate as unknown as DateConstructor;

const MINUTE = 60_000;
const HOUR = 60 * MINUTE;
const DAY = 24 * HOUR;
const iso = (ms: number) => new Date(ms).toISOString();
const TENANT = "00000000-0000-4000-8000-0000000000c1";
const root = fs.realpathSync(fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-capture-review-r4-proof-")));
let ledgerIndex = 0;
const EMPTY_SPOOL: CaptureSpoolState = { pendingFiles: 0, oldestPendingMs: null, losses: [], unreadable: false };
let eventIndex = 0;

function event(observedMs: number) {
  eventIndex += 1;
  return aiInteractionEventSchema.parse({
    id: `00000000-0000-4000-8000-${String(710_000 + eventIndex).padStart(12, "0")}`,
    sessionId: `00000000-0000-4000-8000-${String(610_000 + eventIndex).padStart(12, "0")}`,
    source: "codex", dataMode: "metadata", eventType: "assistant_response", observedAt: iso(observedMs),
    actionClass: "other", inputTokens: 3, outputTokens: 1, metadata: { proof: "capture-claim-review-r4" },
  });
}

function ledger(epochStartMs: number) {
  return new LocalEventBuffer(path.join(root, `ledger-${++ledgerIndex}.sqlite`), {
    workspaceId: TENANT, delivery: { enabled: true, limits: { maxOldestAgeDays: 3650 } },
    enrollmentNow: () => new Date(epochStartMs),
  });
}

function claimOf(buffer: LocalEventBuffer, spool: CaptureSpoolState = EMPTY_SPOOL): Claim {
  buffer.delivery.migrateLegacy({ now: new Date() });
  const ids = (buffer.database.prepare(`select delivery_id as id from upload_outbox`).all() as Array<{ id: string }>).map((row) => row.id);
  const claim = (buffer.delivery as unknown as { captureClaim(...args: unknown[]): Claim | null }).captureClaim(ids, spool);
  if (!claim) throw new Error("no claim");
  return claim;
}
const inGap = (claim: Claim, at: string) => claim.gaps.some((gap) => gap.from <= at && at <= gap.to);
/** The claim vouches for `at` without naming it as missing. */
const attests = (claim: Claim, at: string) => claim.through !== null && claim.through > at && !inGap(claim, at);

/** The maintenance loop, built for either constructor shape (0.7.37 put a Grok tailer before the options). */
function maintenanceFor(buffer: LocalEventBuffer, rollout: RolloutTailer, transcript: TranscriptTailer,
  options: { captureCoverageIntervalMs: number }, grok?: unknown) {
  return CollectorMaintenance.length >= 5
    ? new CollectorMaintenance(buffer, rollout, transcript, undefined, grok as never, options)
    : new CollectorMaintenance(buffer, rollout, transcript, undefined, options as never);
}
/** One coverage turn of the maintenance loop: the whole check on r3, one budgeted turn on r4. */
const coverageTurn = (maintenance: CollectorMaintenance) =>
  (maintenance as unknown as { checkCaptureCoverage(): void }).checkCaptureCoverage();

function roots(buffer: LocalEventBuffer, base: string) {
  const epochId = buffer.workspaceBinding()!.currentInstallationEpochId!;
  const list: CaptureRoot[] = [
    { source: "codex", rootId: "codex-0", profileId: "profile-codex-0", directory: path.join(base, "codex"), installationEpochId: epochId },
    { source: "claude_code", rootId: "claude-0", profileId: "profile-claude-0", directory: path.join(base, "claude"), installationEpochId: epochId },
  ];
  for (const captureRoot of list) fs.mkdirSync(captureRoot.directory, { recursive: true });
  const rollout = new RolloutTailer(buffer, undefined, () => [], DEFAULT_JSONL_TAILER_IO, list.filter((r) => r.source === "codex"));
  const transcript = new TranscriptTailer(buffer, undefined, DEFAULT_JSONL_TAILER_IO, list.filter((r) => r.source === "claude_code"));
  return { codex: list[0]!.directory, claude: list[1]!.directory, rollout, transcript };
}

async function completeBaseline(maintenance: CollectorMaintenance, buffer: LocalEventBuffer) {
  for (let cadence = 0; cadence < 40 && captureBaselineStatus(buffer.database).status !== "complete"; cadence += 1) {
    await maintenance.runRecent({ onDurableCommit: () => true, onProgress: () => true });
  }
}

function r3s2() {
  // A week the ledger could not be written: 1,200 spooled files rejected or
  // expired together. The log holds 1,000 lines.
  const now = Date.now();
  const home = path.join(root, "s2-home");
  const buffer = ledger(now - 10 * DAY);
  buffer.append(event(now - 2 * HOUR));
  for (const source of CAPTURE_FRONTIER_SOURCES) {
    advanceCaptureFrontier(buffer.database, source, { complete: true, files: [] }, iso(now - 30 * MINUTE + CAPTURE_WRITE_LAG_MS));
  }
  const firstLossMs = now - 3 * DAY;
  const losses = 1_200;
  for (let index = 0; index < losses; index += 1) {
    recordSpoolLoss(hookSpoolDirectory(home), { atMs: firstLossMs + index * 3 * MINUTE, reason: "spool_untrusted" });
  }
  const claim = claimOf(buffer, captureSpoolState(home));
  buffer.close();
  const firstLoss = iso(firstLossMs);
  const lastLoss = iso(firstLossMs + (losses - 1) * 3 * MINUTE);
  check("R3_S2_older_spool_losses_stay_gaps_when_the_log_fills",
    claim.dead === losses && inGap(claim, firstLoss) && inGap(claim, lastLoss),
    { losses, firstLoss, lastLoss, dead: claim.dead, gaps: claim.gaps, through: claim.through });
}

async function r3s4() {
  // A Claude session last written twelve days ago, two days before enrollment:
  // the baseline excludes it. It is resumed in place with one new line.
  const now = Date.now();
  const epochStartMs = now - 10 * DAY;
  const base = path.join(root, "s4");
  const buffer = ledger(epochStartMs);
  const world = roots(buffer, base);
  const session = "33333333-3333-4333-8333-333333333333";
  const project = path.join(world.claude, "project-old");
  fs.mkdirSync(project, { recursive: true });
  const file = path.join(project, `${session}.jsonl`);
  const oldAt = new Date(now - 12 * DAY);
  fs.writeFileSync(file, `${JSON.stringify({ type: "assistant", timestamp: oldAt.toISOString(), sessionId: session,
    message: { id: `${session}-1`, model: "claude-opus-5", usage: { input_tokens: 100, output_tokens: 20 } } })}\n`);
  fs.utimesSync(file, oldAt, oldAt);
  const cadences = maintenanceFor(buffer, world.rollout, world.transcript, { captureCoverageIntervalMs: 365 * DAY });
  const checks = maintenanceFor(buffer, world.rollout, world.transcript, { captureCoverageIntervalMs: 0 });
  await completeBaseline(cadences, buffer);
  for (let turn = 0; turn < 8; turn += 1) coverageTurn(checks);
  const lastCheck = (buffer.database.prepare(
    `select checked_at as checkedAt from capture_coverage_state where source = 'claude_code'`,
  ).get() as { checkedAt: string } | undefined)?.checkedAt ?? null;
  const appendAt = new Date().toISOString();
  fs.appendFileSync(file, `${JSON.stringify({ type: "assistant", timestamp: appendAt, sessionId: session,
    message: { id: `${session}-2`, model: "claude-opus-5", usage: { input_tokens: 300, output_tokens: 40 } } })}\n`);
  for (let turn = 0; turn < 8; turn += 1) coverageTurn(checks);
  const row = buffer.database.prepare(
    `select uncovered_since as since from capture_uncovered_files where source = 'claude_code'`,
  ).get() as { since: string } | undefined;
  const claim = claimOf(buffer);
  const captured = (buffer.database.prepare(`select count(*) as n from buffered_events where payload_json like ?`)
    .get(`%${session}-2%`) as { n: number }).n;
  cadences.close();
  buffer.close();
  check("R3_S4_append_to_pre_enrollment_file_is_uncovered_from_the_last_check",
    captured === 0 && lastCheck !== null && row?.since === lastCheck && !attests(claim, appendAt) &&
      !claim.gaps.some((gap) => gap.from === iso(epochStartMs)),
    { epochStartedAt: iso(epochStartMs), lastCompleteCheckBeforeAppend: lastCheck, appendAt, uncoveredSince: row?.since ?? null,
      appendedLineCaptured: captured > 0, claim });
}

async function r3s5() {
  // A live Codex session: each round the tailer reads it to the end, then the
  // session writes one more line before the coverage check looks at it.
  const buffer = ledger(Date.now() - 10 * DAY);
  const world = roots(buffer, path.join(root, "s5"));
  const cadences = maintenanceFor(buffer, world.rollout, world.transcript, { captureCoverageIntervalMs: 365 * DAY });
  const checks = maintenanceFor(buffer, world.rollout, world.transcript, { captureCoverageIntervalMs: 0 });
  const cadence = () => cadences.runRecent({ onDurableCommit: () => true, onProgress: () => true });
  await completeBaseline(cadences, buffer);
  const session = "44444444-4444-4444-8444-444444444444";
  const dayDir = path.join(world.codex, ...new Date().toISOString().slice(0, 10).split("-"));
  fs.mkdirSync(dayDir, { recursive: true });
  const file = path.join(dayDir, `rollout-${new Date().toISOString().slice(0, 19).replace(/:/g, "-")}-${session}.jsonl`);
  let tokens = 0;
  const usage = () => {
    tokens += 500;
    return { type: "event_msg", timestamp: new Date().toISOString(), payload: { type: "token_count", info: {
      total_token_usage: { input_tokens: tokens, cached_input_tokens: 0, output_tokens: tokens, reasoning_output_tokens: 0 } } } };
  };
  const write = (lines: unknown[]) => fs.appendFileSync(file, lines.map((line) => JSON.stringify(line)).join("\n") + "\n");
  write([{ type: "session_meta", timestamp: new Date().toISOString(), payload: { id: session } },
    { type: "turn_context", timestamp: new Date().toISOString(), payload: { model: "gpt-5.5" } }]);
  await cadence();
  coverageTurn(checks);
  const rounds: Array<Record<string, unknown>> = [];
  for (let round = 1; round <= 6; round += 1) {
    clockShiftMs += 15 * MINUTE;
    write([usage()]);
    for (let index = 0; index < 2; index += 1) await cadence();
    write([usage()]);
    coverageTurn(checks);
    const frontier = captureFrontier(buffer.database)!;
    rounds.push({
      round, through: frontier.capturedThrough, gaps: frontier.gaps.map((gap) => ({ from: iso(gap.fromMs), to: iso(gap.toMs) })),
      captured: (buffer.database.prepare(`select count(*) as n from buffered_events where payload_json like ?`).get(`%${session}%`) as { n: number }).n,
    });
  }
  clockShiftMs = 0;
  cadences.close();
  buffer.close();
  const throughs = rounds.map((round) => round.through as string | null);
  check("R3_S5_busy_file_the_tailer_keeps_up_with_is_never_a_gap",
    rounds.every((round) => (round.gaps as unknown[]).length === 0 && round.captured === 2 * (round.round as number) - 1) &&
      throughs.every((through) => through !== null) && throughs.at(-1)! > throughs[0]!,
    { rounds });
}

function coverageTurnBudget() {
  const files = Array.from({ length: RELEASE_MAX_WORK_PER_TURN * 2 + 17 },
    (_, index) => `file-${index}`);
  let checked = 0;
  let next = 0;
  const walk = new CaptureCoverageWalk({
    roots: ["root"],
    open: () => ({
      read: () => next < files.length ? { path: files[next++]!, kind: "file" as const } : null,
      unchanged: () => true,
      close: () => undefined,
    }),
    check: () => {
      checked += 1;
      return null;
    },
    checkLink: () => null,
  });
  const firstWork = walk.step(1_000, () => undefined, () => 0);
  const secondWork = walk.step(1_000, () => undefined, () => 0);
  const afterBudgetTurn = checked;
  let virtualNow = 0;
  const deadlineWork = walk.step(8, () => undefined, () => virtualNow++);
  const afterDeadlineTurn = checked - afterBudgetTurn;
  check("R3_N4_coverage_walk_enforces_the_deterministic_turn_work_budget_and_deadline",
    CAPTURE_COVERAGE_MAX_WORK_PER_TURN === RELEASE_MAX_WORK_PER_TURN &&
      firstWork === RELEASE_MAX_WORK_PER_TURN && secondWork === RELEASE_MAX_WORK_PER_TURN &&
      afterBudgetTurn === RELEASE_MAX_WORK_PER_TURN - 1 &&
      deadlineWork === 8 && afterDeadlineTurn === 4 &&
      !walk.done,
    { maxWorkPerTurn: CAPTURE_COVERAGE_MAX_WORK_PER_TURN, releaseMaxWorkPerTurn: RELEASE_MAX_WORK_PER_TURN,
      firstWork, secondWork, deadlineWork, afterBudgetTurn, afterDeadlineTurn, done: walk.done });
  walk.close();
}

async function r3n4() {
  // A large tree: files per source, half written before the epoch began and
  // half written since and never read (every one of those becomes a row).
  const perSource = 60_000;
  const now = Date.now();
  const base = path.join(root, "n4");
  const buffer = ledger(now - 10 * DAY);
  const world = roots(buffer, base);
  const old = new Date(now - 60 * DAY);
  const built = performance.now();
  for (let index = 0; index < perSource; index += 1) {
    const day = new Date(now - (1 + Math.floor(index / 100)) * DAY).toISOString().slice(0, 10).split("-");
    const codexDir = path.join(world.codex, ...day);
    const claudeDir = path.join(world.claude, `project-${Math.floor(index / 100)}`);
    if (index % 100 === 0) {
      fs.mkdirSync(codexDir, { recursive: true });
      fs.mkdirSync(claudeDir, { recursive: true });
    }
    const codexFile = path.join(codexDir, `rollout-2026-01-01T00-00-00-${String(index).padStart(12, "0")}.jsonl`);
    const claudeFile = path.join(claudeDir, `${String(index).padStart(8, "0")}-0000-4000-8000-000000000000.jsonl`);
    fs.writeFileSync(codexFile, "{}\n");
    fs.writeFileSync(claudeFile, "{}\n");
    if (index % 2 === 0) {
      fs.utimesSync(codexFile, old, old);
      fs.utimesSync(claudeFile, old, old);
    }
  }
  // Grok sessions, where this version tails them.
  const grokModule = await import("../packages/collector-cli/src/grok-usage-tailer").catch(() => null);
  const grokSessions = grokModule ? Math.ceil(perSource / 20) : 0;
  const grokHome = path.join(base, "grok");
  for (let index = 0; index < grokSessions; index += 1) {
    const session = path.join(grokHome, "sessions", `group-${Math.floor(index / 100)}`, `session-${index}`);
    fs.mkdirSync(session, { recursive: true });
    fs.writeFileSync(path.join(session, "usage.json"), "{}\n");
  }
  const buildSeconds = Number(((performance.now() - built) / 1000).toFixed(1));
  const grok = grokModule ? new grokModule.GrokUsageTailer(buffer, grokHome) : undefined;
  const checks = maintenanceFor(buffer, world.rollout, world.transcript, { captureCoverageIntervalMs: 0 }, grok);
  const turns: number[] = [];
  const rowsPerTurn: number[] = [];
  // The table appears with the first coverage check; before it, no rows.
  const uncoveredRows = () =>
    buffer.database.prepare(`select 1 from sqlite_master where type = 'table' and name = 'capture_uncovered_files'`).get()
      ? (buffer.database.prepare(`select count(*) as n from capture_uncovered_files`).get() as { n: number }).n
      : 0;
  let frontier: { capturedThrough: string | null } | null = null;
  for (let turn = 0; turn < 400; turn += 1) {
    const started = performance.now();
    const before = uncoveredRows();
    coverageTurn(checks);
    turns.push(Number((performance.now() - started).toFixed(1)));
    rowsPerTurn.push(uncoveredRows() - before);
    frontier = captureFrontier(buffer.database);
    if (frontier?.capturedThrough) break;
  }
  const rows = uncoveredRows();
  const unreadFiles = 2 * Math.floor(perSource / 2) + grokSessions;
  const maxTurnMs = Math.max(...turns);
  // One coverage turn steps each source's walk once (maintenance.ts
  // checkCaptureCoverage), each capped at the release's RELEASE_MAX_WORK_PER_TURN.
  const perTurnWorkCap = COVERAGE_SOURCES * RELEASE_MAX_WORK_PER_TURN;
  const minimumResumableTurns = Math.ceil(unreadFiles / perTurnWorkCap);
  const maxRowsPerTurn = Math.max(...rowsPerTurn);
  checks.close();
  buffer.close();
  fs.rmSync(base, { recursive: true, force: true });
  check("R3_N4_large_tree_walk_uses_resumable_work_budgets_and_completes",
    frontier?.capturedThrough != null && rows === unreadFiles && turns.length >= minimumResumableTurns &&
      maxRowsPerTurn <= perTurnWorkCap,
    { filesPerSource: perSource, grokSessions, buildSeconds, turns: turns.length, minimumResumableTurns,
      maxRowsPerTurn, perTurnWorkCap,
      maxWorkPerTurn: CAPTURE_COVERAGE_MAX_WORK_PER_TURN, maxTurnMs,
      totalMs: Number(turns.reduce((total, ms) => total + ms, 0).toFixed(1)), uncoveredRows: rows, unreadFiles,
      frontier: frontier?.capturedThrough ?? null });
}

async function grokCoverage() {
  const grokModule = await import("../packages/collector-cli/src/grok-usage-tailer").catch(() => null);
  if (!grokModule) {
    check("Grok_usage_file_not_yet_read_is_never_attested", false, { reason: "this collector has no Grok usage tailer" });
    return;
  }
  const { grokUsageDocument } = await import("./lib/grok-usage-fixture");
  const now = Date.now();
  const base = path.join(root, "grok");
  const buffer = ledger(now - 10 * DAY);
  const world = roots(buffer, base);
  const grokHome = path.join(base, "grok-home");
  fs.mkdirSync(path.join(grokHome, "sessions"), { recursive: true });
  const grok = new grokModule.GrokUsageTailer(buffer, grokHome);
  const checks = maintenanceFor(buffer, world.rollout, world.transcript, { captureCoverageIntervalMs: 0 }, grok);
  coverageTurn(checks);
  // Grok records a finished turn; no Grok sweep has reached it yet.
  const endedAt = new Date().toISOString();
  const session = path.join(grokHome, "sessions", "%2FUsers%2Ffixture%2Fproject", "55555555-5555-4555-8555-555555555555");
  fs.mkdirSync(session, { recursive: true });
  fs.writeFileSync(path.join(session, "usage.json"), JSON.stringify(grokUsageDocument({
    sessionId: "55555555-5555-4555-8555-555555555555", updatedAt: endedAt, shape: "modern",
    turns: [{ turnNumber: 1, endedAt, models: [{ model: "grok-4", input: 1_000, cachedRead: 0, cacheCreation: 0,
      output: 200, reasoning: 0, modelCalls: 1, costTicks: 5_000_000 }] }],
  })));
  // Three hours on, still unread: the checks keep running.
  clockShiftMs = 3 * HOUR;
  coverageTurn(checks);
  const unread = claimOf(buffer);
  // The sweep reads it; the next check clears the gap.
  await grok.scan({ budget: new CaptureWorkBudget(), now: new Date() });
  const grokEvents = (buffer.database.prepare(`select count(*) as n from buffered_events where source = 'grok'`).get() as { n: number }).n;
  clockShiftMs += MINUTE;
  coverageTurn(checks);
  const read = claimOf(buffer);
  clockShiftMs = 0;
  checks.close();
  buffer.close();
  check("Grok_usage_file_not_yet_read_is_never_attested",
    !attests(unread, endedAt) && unread.through !== null && unread.through > endedAt && inGap(unread, endedAt) &&
      grokEvents > 0 && !inGap(read, endedAt),
    { endedAt, unread, grokEvents, afterRead: read });
}

async function main() {
  for (const step of [r3s2, r3s4, r3s5, coverageTurnBudget, () => incrementalCoverageChecks(check), r3n4, grokCoverage]) {
    try {
      await step();
    } catch (error) {
      clockShiftMs = 0;
      check(step.name, false, { error: error instanceof Error ? `${error.name}: ${error.message}` : String(error) });
    }
  }
  console.log(JSON.stringify({ proof: "capture-claim-review-r4", results }, null, 2));
  fs.rmSync(root, { recursive: true, force: true });
  if (results.every((result) => result.passed)) completion.complete();
  else process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
