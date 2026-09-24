/**
 * Busy-host capture proof (eco-6hoxj.163.42, rounds 2 to 5).
 *
 * Each scenario is a regression for a reviewer finding, built from the
 * reviewer's own construction:
 *
 *   progress   A source must commit within a stated bound: 2S cadences for
 *              S capture sources (6 with Grok). A leader that is admitted,
 *              spends the whole allowance and commits nothing must pass
 *              the lead on (round 3), including with a repair overrun at the
 *              front of every cadence; three busy real sources behind slow
 *              reads all commit. Bookkeeping before capture that spends the
 *              allowance on every capture-first cadence cannot keep the
 *              leaders out (round 4), and the baseline status it spends is
 *              answered from a covering index (round 5).
 *   ceiling    200 ms is an admission ceiling: no capture or repair unit
 *              starts after it, and a cadence ends within one bounded unit
 *              of it. Grok discovery of a 2,000-session group with a slow
 *              lstat stays inside the budget on every scan of a sweep.
 *   lossless   Every session is reached within a stated bound: a group
 *              larger than one pass is finished even when the worker is
 *              replaced every pass, and churn that keeps adding entries
 *              ahead of an old session cannot keep it out (round 3). Caps
 *              are reported, the durable walk state names no path, and
 *              today's sessions come first.
 *
 * Time is virtual (scripts/lib/virtual-clock.ts): the fixture charges what
 * a slow call costs and real work costs nothing, so the result is the same
 * on a loaded host and on a CI runner. The receipt's `deterministicDigest`
 * covers every measured value; repeated runs print the same digest.
 * Fixtures are temporary; nothing reads an installed collector or a real
 * provider home.
 *
 *   pnpm proof:busy-host-capture [-- --scenario=progress|ceiling|lossless]
 */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import {
  CAPTURE_BASELINE_GENERATION_STATUS_SQL,
  captureBaselineStatus,
  ensureCaptureBaselineSchema,
} from "../packages/collector-cli/src/capture-baseline";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { GROK_USAGE_LIMITS, GrokUsageTailer, ensureGrokUsageState } from "../packages/collector-cli/src/grok-usage-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";
import { installVirtualClock, spend, virtualNow } from "./lib/virtual-clock";

const scenario = process.argv.find((arg) => arg.startsWith("--scenario="))?.split("=", 2)[1] ?? "all";
const checks: Array<{ scenario: string; name: string; passed: boolean; detail: unknown }> = [];
const receipts: Record<string, unknown> = {};
const MAX_WALL_MS = AUTOMATIC_CAPTURE_LIMITS.maxWallMs;
/** The stated service bound: every source leads within 2S cadences. */
const SERVICE_BOUND_CADENCES = 2 * 3;

function check(group: string, name: string, passed: boolean, detail: unknown = null) {
  checks.push({ scenario: group, name, passed, detail });
}

/** A real pause, used only to order file creation times; it never times work. */
const pause = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
const uuid = (lead: string, index: number) =>
  `${lead.padEnd(8, "0").slice(0, 8)}-0000-4000-8000-${String(index).padStart(12, "0")}`;
const walkHash = (name: string) =>
  crypto.createHash("sha256").update(`plimsoll-grok-walk-v1\0${name}`).digest("hex").slice(0, 32);

type Source = "codex" | "claude_code" | "grok";

function grokUsage(sessionId: string, endedAt = new Date().toISOString()) {
  return {
    sessionId,
    updatedAt: endedAt,
    session: { inputTokens: 11, outputTokens: 3, primaryModelId: "grok-4.7-build" },
    turns: [{
      turnNumber: 1, endedAt, inputTokens: 11, outputTokens: 3, reasoningTokens: 1,
      cachedReadTokens: 2, cacheCreationTokens: 0, costUsdTicks: 100, primaryModelId: "grok-4.7-build",
    }],
  };
}

/** One Grok session; `at` sets the usage file, session and group mtimes. */
function writeGrokSession(home: string, group: string, sessionId: string, at?: Date, withUsage = true) {
  const directory = path.join(home, "sessions", group, sessionId);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, "usage.json");
  if (withUsage) fs.writeFileSync(file, JSON.stringify(grokUsage(sessionId)), { mode: 0o600 });
  if (at) {
    if (withUsage) fs.utimesSync(file, at, at);
    fs.utimesSync(directory, at, at);
    fs.utimesSync(path.dirname(directory), at, at);
  }
  return file;
}

/** A ledger with the Grok usage tables, so it can be queried before its first scan. */
function grokLedger(file: string) {
  const buffer = new LocalEventBuffer(file);
  ensureGrokUsageState(buffer.database);
  return buffer;
}

function grokEvents(buffer: LocalEventBuffer) {
  return Number((buffer.database.prepare(
    "select count(*) as count from buffered_events where source = 'grok' and input_tokens is not null",
  ).get() as { count: number }).count);
}

function grokTurnSeen(buffer: LocalEventBuffer, sessionId: string) {
  return Boolean(buffer.database.prepare(
    "select 1 from grok_usage_turn_state where session_id = ? limit 1",
  ).get(sessionId));
}

function state(buffer: LocalEventBuffer, key: string) {
  return (buffer.database.prepare("select value from maintenance_state where key = ?").get(key) as
    | { value: string } | undefined)?.value ?? null;
}

function setState(buffer: LocalEventBuffer, key: string, value: string) {
  buffer.database.prepare(
    `insert into maintenance_state(key, value, updated_at) values (?, ?, ?)
     on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
  ).run(key, value, new Date().toISOString());
}

function grokSweeps(buffer: LocalEventBuffer) {
  const marker = JSON.parse(state(buffer, "grok_usage_backfill_v1") ?? "null") as { sweeps: number } | null;
  return marker?.sweeps ?? 0;
}

/** Scan with a wall the fixture never reaches, so its only limits are the ones it sets. */
function roomyBudget() {
  return new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 1_000_000 });
}

function repairStages(cycles: number) {
  return JSON.stringify({ next: 0, cycles, stages: Object.fromEntries([
    "projection", "reconciliation", "repricing", "repo_context_suppression",
  ].map((stage) => [stage, { attempts: 0, completed: 0, failures: 0, rowsVisited: 0, lastSuccessAt: null }])) });
}

/** Pin every cadence repair-first (cycles 0 -> 1) or capture-first (1 -> 2). */
function pinCadence(buffer: LocalEventBuffer, first: "repair" | "capture") {
  const raw = state(buffer, "automatic_repair_service_v1");
  const repair = raw ? JSON.parse(raw) as Record<string, unknown> : JSON.parse(repairStages(0)) as Record<string, unknown>;
  repair.cycles = first === "repair" ? 0 : 1;
  setState(buffer, "automatic_repair_service_v1", JSON.stringify(repair));
}

function codexDocument(id: string, padding = 0) {
  const timestamp = new Date().toISOString();
  return [
    { type: "session_meta", timestamp, payload: { id } },
    { type: "turn_context", timestamp, payload: { model: "gpt-5.5" } },
    ...Array.from({ length: padding }, () => ({ type: "fixture_ignored", padding: "x".repeat(600) })),
    { type: "event_msg", timestamp, payload: { type: "token_count", info: { total_token_usage: {
      input_tokens: 2, cached_input_tokens: 0, output_tokens: 1, reasoning_output_tokens: 0,
    } } } },
  ].map((row) => JSON.stringify(row)).join("\n") + "\n";
}

function claudeDocument(id: string, padding = 0) {
  const timestamp = new Date().toISOString();
  return [
    ...Array.from({ length: padding }, () => ({ type: "fixture_ignored", padding: "x".repeat(600) })),
    { type: "assistant", timestamp, sessionId: id, message: {
      id: `${id}-message`, model: "claude-opus-5", usage: { input_tokens: 3, output_tokens: 1 },
    } },
  ].map((row) => JSON.stringify(row)).join("\n") + "\n";
}

/** The shapes the maintenance loop reads from a source result. */
function jsonlResult(deferred: boolean, progressed: boolean, readErrors = 0) {
  return {
    scope: "recent", exhaustive: !deferred, discoveryErrors: 0, statErrors: 0, readErrors,
    parseErrors: 0, unresolvedRecords: 0, filesSeen: progressed ? 1 : 0, filesRead: progressed ? 1 : 0,
    filesReset: 0, bytesRead: progressed ? 2_048 : 0, bytesDeferred: deferred || !progressed ? 2_048 : 0,
    recordsParsed: progressed ? 1 : 0, recordsCommitted: progressed ? 1 : 0, eventsAppended: progressed ? 1 : 0,
    excludedGenerations: 0, deferredGenerations: deferred || !progressed ? 1 : 0, cooperativeYields: 0,
    lastYieldAt: null, aborted: false, slicesCommitted: progressed ? 1 : 0, continuationBytesAdvanced: 0,
    activity: { lastActivityAt: null, filesToday: 0, discoveryEntries: 0, lastScanAt: new Date().toISOString(),
      error: null, truncated: deferred || !progressed, scan: null },
  };
}

function grokResult(deferred: boolean, progressed: boolean) {
  return {
    scope: "automatic", home: "ready", exhaustive: !deferred, discoveryErrors: 0, statErrors: 0, readErrors: 0,
    parseErrors: 0, unresolvedRecords: 0, filesSeen: progressed ? 1 : 0, filesUnchanged: 0,
    filesRead: progressed ? 1 : 0, filesParsed: progressed ? 1 : 0, filesDeferred: deferred ? 1 : 0,
    filesOversized: 0, bytesRead: progressed ? 2_048 : 0, bytesDeferred: deferred ? 2_048 : 0,
    recordsParsed: progressed ? 1 : 0, recordsCommitted: progressed ? 1 : 0, eventsAppended: progressed ? 1 : 0,
    turnsRevised: 0, turnRewritesRefused: 0, incompleteEvents: 0, sessionsSkippedLiveCovered: 0,
    enrollmentExcludedEvents: 0, futureTimestampClampedEvents: 0,
    tokensAppended: { input: 0, cachedRead: 0, cacheCreation: 0, output: 0, reasoning: 0 },
    costUsdTicksAppended: 0, deferredGenerations: deferred ? 1 : 0, excludedGenerations: 0,
    cooperativeYields: 0, lastYieldAt: null, aborted: false, automaticBudget: null,
    activity: { lastActivityAt: null, filesToday: 0, discoveryEntries: 0, lastScanAt: new Date().toISOString(),
      error: null, truncated: deferred, scan: null },
  };
}

/**
 * Round 3, blocker 1 (the reviewer's construction): Codex is admitted every
 * time it leads, spends more than the whole allowance and commits nothing;
 * Claude and Grok commit whenever they are admitted. The lead must pass on.
 */
async function failingLeader(root: string, front: "capture" | "repair") {
  const TICKS = 12;
  const database = path.join(root, `failing-leader-${front}.sqlite`);
  const counters = Object.fromEntries((["codex", "claude_code", "grok"] as Source[])
    .map((source) => [source, { ioStarts: 0, commits: 0 }])) as Record<Source, { ioStarts: number; commits: number }>;
  const make = (buffer: LocalEventBuffer) => {
    const codex = {
      async scan(options: { deferredBeforeIo?: boolean }) {
        if (options.deferredBeforeIo) return jsonlResult(true, false);
        counters.codex.ioStarts += 1;
        spend(230); // one synchronous call longer than the whole allowance, then a read error
        return jsonlResult(false, false, 1);
      },
      close() {},
    };
    const committing = (source: "claude_code") => ({
      async scan(options: { deferredBeforeIo?: boolean; automatic: { budget: CaptureWorkBudget } }) {
        if (options.deferredBeforeIo) return jsonlResult(true, false);
        counters[source].ioStarts += 1;
        options.automatic.budget.recordSlice({ bytesRead: 2_048, recordsParsed: 1, eventsAppended: 1 });
        counters[source].commits += 1;
        return jsonlResult(false, true);
      },
      close() {},
    });
    const grok = {
      async scan(options: { deferredBeforeIo?: boolean; budget: CaptureWorkBudget }) {
        if (options.deferredBeforeIo) return grokResult(true, false);
        counters.grok.ioStarts += 1;
        options.budget.recordSlice({ bytesRead: 2_048, recordsParsed: 1, eventsAppended: 1 });
        counters.grok.commits += 1;
        return grokResult(false, true);
      },
      close() {},
    };
    if (front === "repair") {
      const projection = buffer.projection as unknown as { runMaintenance: (...args: unknown[]) => unknown };
      const real = projection.runMaintenance.bind(projection);
      projection.runMaintenance = (...args: unknown[]) => {
        spend(230);
        return real(...args);
      };
    }
    return new CollectorMaintenance(buffer, codex as never, committing("claude_code") as never, undefined, grok as never);
  };
  let buffer = new LocalEventBuffer(database);
  // The round-2 debt ledger, seeded as the reviewer seeded it: Codex one point ahead.
  setState(buffer, "automatic_capture_fairness_v1",
    JSON.stringify({ version: 1, owed: { codex: 1, claude_code: 0, grok: 0 }, captureFirst: front === "capture" }));
  setState(buffer, "automatic_capture_source_turn", "codex");
  let maintenance = make(buffer);
  const ticks: Array<{ tick: number; leader: Source; leaderAdmitted: boolean; commits: Record<Source, number> }> = [];
  try {
    for (let tick = 0; tick < TICKS; tick += 1) {
      if (tick === 3) {
        // A replaced worker and a reopened ledger keep the service order.
        maintenance.close();
        buffer.close();
        buffer = new LocalEventBuffer(database);
        maintenance = make(buffer);
      }
      pinCadence(buffer, front);
      const before = { codex: counters.codex.commits, claude_code: counters.claude_code.commits, grok: counters.grok.commits };
      const result = await maintenance.runRecent();
      const order = (result as { captureTurn?: { order: Source[]; admitted: Partial<Record<Source, boolean>> } }).captureTurn;
      ticks.push({
        tick,
        leader: order!.order[0]!,
        leaderAdmitted: order!.admitted[order!.order[0]!] === true,
        commits: {
          codex: counters.codex.commits - before.codex,
          claude_code: counters.claude_code.commits - before.claude_code,
          grok: counters.grok.commits - before.grok,
        },
      });
    }
  } finally {
    maintenance.close();
    buffer.close();
  }
  const windows = Array.from({ length: TICKS - SERVICE_BOUND_CADENCES + 1 }, (_, start) =>
    ticks.slice(start, start + SERVICE_BOUND_CADENCES));
  const everyWindowLedByEveryone = windows.every((window) =>
    (["codex", "claude_code", "grok"] as Source[]).every((source) =>
      window.some((row) => row.leader === source && row.leaderAdmitted)));
  const everyWindowCommits = windows.every((window) =>
    (["claude_code", "grok"] as Source[]).every((source) => window.some((row) => row.commits[source] > 0)));
  receipts[`failingLeader_${front}First`] = { ticks, counters };
  check("progress", `a_failing_leader_passes_the_lead_on_${front}_first`,
    everyWindowLedByEveryone,
    { boundCadences: SERVICE_BOUND_CADENCES, leaders: ticks.map((row) => `${row.leader}${row.leaderAdmitted ? "" : "(held)"}`) });
  check("progress", `busy_sources_behind_a_failing_leader_commit_within_the_bound_${front}_first`,
    everyWindowCommits && counters.claude_code.ioStarts > 0 && counters.grok.ioStarts > 0,
    { boundCadences: SERVICE_BOUND_CADENCES, counters });
}

/**
 * Blockers 1 and 2 of round 1 at the scheduler, with real tailers: the
 * reviewer's all-source fixture, every unit start timed against its cadence.
 */
/**
 * Charge `ms` of virtual time to each capture-baseline status query (one per
 * JSONL source), as a large ledger's generation aggregate would take it.
 */
function slowBaselineStatus(buffer: LocalEventBuffer, ms: number) {
  const database = buffer.database as unknown as { prepare: (sql: string) => unknown };
  const prepare = database.prepare.bind(database);
  database.prepare = (sql: string) => {
    const statement = prepare(sql) as { get: (...args: unknown[]) => unknown };
    if (!/count\(\*\) as excludedGenerations/.test(sql)) return statement;
    return new Proxy(statement, {
      get(target, property) {
        if (property === "get") {
          return (...args: unknown[]) => {
            spend(ms);
            return target.get(...args);
          };
        }
        const value = Reflect.get(target, property, target) as unknown;
        return typeof value === "function" ? (value as (...args: unknown[]) => unknown).bind(target) : value;
      },
    });
  };
}

/**
 * Round 4 (should-fix): the bookkeeping before capture spends the whole
 * allowance on every cadence, and every cadence is capture-first, so no
 * repair-first cadence ever hands capture the next turn. A leader the clock
 * denies starts on the next cadence whatever the clock, so every source
 * still leads, and commits, within 2S cadences.
 */
async function slowBookkeeping(root: string) {
  const TICKS = 12;
  const counters = Object.fromEntries((["codex", "claude_code", "grok"] as Source[])
    .map((source) => [source, { ioStarts: 0, commits: 0 }])) as Record<Source, { ioStarts: number; commits: number }>;
  const jsonl = (source: "codex" | "claude_code") => ({
    async scan(options: { deferredBeforeIo?: boolean; automatic: { budget: CaptureWorkBudget } }) {
      if (options.deferredBeforeIo) return jsonlResult(true, false);
      counters[source].ioStarts += 1;
      options.automatic.budget.recordSlice({ bytesRead: 2_048, recordsParsed: 1, eventsAppended: 1 });
      counters[source].commits += 1;
      return jsonlResult(false, true);
    },
    close() {},
  });
  const grok = {
    async scan(options: { deferredBeforeIo?: boolean; budget: CaptureWorkBudget }) {
      if (options.deferredBeforeIo) return grokResult(true, false);
      counters.grok.ioStarts += 1;
      options.budget.recordSlice({ bytesRead: 2_048, recordsParsed: 1, eventsAppended: 1 });
      counters.grok.commits += 1;
      return grokResult(false, true);
    },
    close() {},
  };
  const buffer = new LocalEventBuffer(path.join(root, "slow-bookkeeping.sqlite"));
  const maintenance = new CollectorMaintenance(buffer, jsonl("codex") as never, jsonl("claude_code") as never,
    undefined, grok as never);
  slowBaselineStatus(buffer, 110);
  const ticks: Array<{ tick: number; leader: Source; leaderServed: boolean; leaderOverride: boolean;
    preCaptureMs: number; commits: Record<Source, number> }> = [];
  let fairness: { deniedCadences?: number } | null = null;
  try {
    for (let tick = 0; tick < TICKS; tick += 1) {
      pinCadence(buffer, "capture");
      const before = { codex: counters.codex.commits, claude_code: counters.claude_code.commits, grok: counters.grok.commits };
      const result = await maintenance.runRecent();
      const turn = (result as { captureTurn?: { order: Source[]; leaderServed: boolean; leaderOverride?: boolean;
        preCaptureMs?: number } }).captureTurn!;
      ticks.push({
        tick, leader: turn.order[0]!, leaderServed: turn.leaderServed, leaderOverride: turn.leaderOverride === true,
        preCaptureMs: turn.preCaptureMs ?? -1,
        commits: {
          codex: counters.codex.commits - before.codex,
          claude_code: counters.claude_code.commits - before.claude_code,
          grok: counters.grok.commits - before.grok,
        },
      });
    }
    fairness = JSON.parse(state(buffer, "automatic_capture_fairness_v1") ?? "null") as { deniedCadences?: number } | null;
  } finally {
    maintenance.close();
    buffer.close();
  }
  const windows = Array.from({ length: TICKS - SERVICE_BOUND_CADENCES + 1 }, (_, start) =>
    ticks.slice(start, start + SERVICE_BOUND_CADENCES));
  const sources = ["codex", "claude_code", "grok"] as Source[];
  receipts.slowBookkeeping = { ticks, counters, deniedCadences: fairness?.deniedCadences ?? null };
  check("progress", "bookkeeping_that_spends_the_allowance_still_lets_every_source_lead_within_the_bound",
    windows.every((window) => sources.every((source) => window.some((row) => row.leader === source && row.leaderServed))),
    { boundCadences: SERVICE_BOUND_CADENCES,
      leaders: ticks.map((row) => `${row.leader}${row.leaderServed ? row.leaderOverride ? "(override)" : "" : "(denied)"}`) });
  check("progress", "every_source_commits_within_the_bound_behind_slow_bookkeeping",
    windows.every((window) => sources.every((source) => window.some((row) => row.commits[source] > 0))),
    { boundCadences: SERVICE_BOUND_CADENCES, counters });
  check("progress", "a_denied_leader_is_recorded_with_the_time_spent_before_capture",
    ticks.every((row) => row.preCaptureMs >= AUTOMATIC_CAPTURE_LIMITS.maxWallMs) && (fairness?.deniedCadences ?? 0) > 0,
    { preCaptureMs: ticks.map((row) => row.preCaptureMs), deniedCadences: fairness?.deniedCadences ?? null });
}

/**
 * Round 5 (should-fix): the capture-baseline status aggregate runs before
 * every capture leader and on the collector parent's status refresh. It is
 * answered from its covering index instead of reading every generation row,
 * which took about 0.6 s at 200,000 generations per source.
 */
function baselineStatusPlan(root: string) {
  const buffer = new LocalEventBuffer(path.join(root, "baseline-plan.sqlite"));
  let plan: string[] = [];
  let error: string | null = null;
  try {
    ensureCaptureBaselineSchema(buffer.database);
    plan = (buffer.database.prepare(`explain query plan ${CAPTURE_BASELINE_GENERATION_STATUS_SQL}`).all("codex", "run") as Array<{ detail: string }>)
      .map((row) => row.detail);
  } catch (caught) {
    error = (caught as Error).message;
  } finally {
    buffer.close();
  }
  receipts.baselineStatusPlan = { plan, error };
  check("progress", "the_baseline_status_aggregate_reads_only_its_covering_index",
    plan.length === 1 && plan[0]!.includes("USING COVERING INDEX idx_capture_baseline_generation_status"), { plan, error });
}

async function busyHost(root: string) {
  const REPAIR_MS = 220;
  const READ_MS = 120;
  const GROK_READ_MS = 2;
  const TICKS = 12;
  const codexRoot = path.join(root, ".codex", "sessions");
  const claudeRoot = path.join(root, ".claude", "projects");
  const grokHome = path.join(root, ".grok");
  const codexDay = path.join(codexRoot, ...new Date().toISOString().slice(0, 10).split("-"));
  const claudeProject = path.join(claudeRoot, "project");
  fs.mkdirSync(codexDay, { recursive: true, mode: 0o700 });
  fs.mkdirSync(claudeProject, { recursive: true, mode: 0o700 });
  for (let group = 0; group < 100; group += 1) {
    for (let session = 0; session < 40; session += 1) {
      writeGrokSession(grokHome, `group-${String(group).padStart(3, "0")}`, uuid("7a", group * 40 + session));
    }
  }
  for (let index = 0; index < 12; index += 1) {
    const codexId = uuid("8a", index);
    const claudeId = uuid("9a", index);
    fs.writeFileSync(path.join(codexDay, `rollout-${codexId}.jsonl`), codexDocument(codexId), { mode: 0o600 });
    fs.writeFileSync(path.join(claudeProject, `${claudeId}.jsonl`), claudeDocument(claudeId), { mode: 0o600 });
  }

  const buffer = new LocalEventBuffer(path.join(root, "busy.sqlite"));
  let tickStartedAt = 0;
  const unitStarts: Array<{ unit: string; offsetMs: number }> = [];
  const mark = (unit: string) => unitStarts.push({ unit, offsetMs: virtualNow() - tickStartedAt });
  const io = {
    ...DEFAULT_JSONL_TAILER_IO,
    readTail: (...args: Parameters<typeof DEFAULT_JSONL_TAILER_IO.readTail>) => {
      mark(args[0].startsWith(codexRoot) ? "codex_read" : "claude_read");
      spend(READ_MS);
      return DEFAULT_JSONL_TAILER_IO.readTail(...args);
    },
  };
  const rollout = new RolloutTailer(buffer, codexRoot, undefined, io);
  const transcript = new TranscriptTailer(buffer, claudeRoot, io);
  const grok = new GrokUsageTailer(buffer, grokHome);
  const projection = buffer.projection as unknown as { runMaintenance: (...args: unknown[]) => unknown };
  const realProjectionMaintenance = projection.runMaintenance.bind(projection);
  projection.runMaintenance = (...args: unknown[]) => {
    mark("repair");
    spend(REPAIR_MS);
    return realProjectionMaintenance(...args);
  };
  const originalOpen = fs.openSync;
  (fs as unknown as { openSync: typeof fs.openSync }).openSync = ((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target).startsWith(grokHome)) {
      mark("grok_read");
      spend(GROK_READ_MS);
    }
    return (originalOpen as (...values: unknown[]) => number)(target, ...rest);
  }) as typeof fs.openSync;
  const maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, grok);
  const observations: Array<Record<string, any>> = [];
  let liveWrittenAfterTick: number | null = null;
  try {
    for (let tick = 0; tick < TICKS; tick += 1) {
      // Repairs lead every cadence and overrun it: the parity alternation is
      // pinned off, so only the scheduler's own rules can serve capture.
      pinCadence(buffer, "repair");
      const firstUnit = unitStarts.length;
      tickStartedAt = virtualNow();
      const result = await maintenance.runRecent();
      const source = (scan: { recordsCommitted?: number; filesRead: number; deferredGenerations: number;
        activity: { scan?: { deferredBeforeIo?: boolean } | null } } | undefined) => ({
        admitted: scan?.activity.scan?.deferredBeforeIo === false,
        committed: scan?.recordsCommitted ?? 0,
        filesRead: scan?.filesRead ?? 0,
        deferredGenerations: scan?.deferredGenerations ?? 0,
      });
      observations.push({
        tick,
        wallMs: virtualNow() - tickStartedAt,
        baseline: captureBaselineStatus(buffer.database).status,
        codex: source(result.rollout),
        claude: source(result.transcript),
        grok: source(result.grok),
        units: unitStarts.slice(firstUnit),
        budget: maintenance.status().budget,
        order: (result as { captureTurn?: { order: Source[] } }).captureTurn?.order ?? null,
      });
      if (liveWrittenAfterTick === null && captureBaselineStatus(buffer.database).status === "complete") {
        // Post-enrollment work arrives: twenty large generations per source,
        // far more than any cadence can commit.
        pause(25);
        rollout.close();
        transcript.close();
        for (let index = 0; index < 20; index += 1) {
          const codexId = uuid("aa", index);
          const claudeId = uuid("ba", index);
          fs.writeFileSync(path.join(codexDay, `rollout-${codexId}.jsonl`), codexDocument(codexId, 200), { mode: 0o600 });
          fs.writeFileSync(path.join(claudeProject, `${claudeId}.jsonl`), claudeDocument(claudeId, 200), { mode: 0o600 });
        }
        liveWrittenAfterTick = tick;
      }
    }
  } finally {
    (fs as unknown as { openSync: typeof fs.openSync }).openSync = originalOpen;
    maintenance.close();
    buffer.close();
  }

  const busy = observations.filter((row) => liveWrittenAfterTick !== null && row.tick > liveWrittenAfterTick);
  const longestWait = (name: "codex" | "claude" | "grok") => {
    let longest = 0;
    let current = 0;
    for (const row of busy) {
      current = row[name].committed > 0 ? 0 : current + 1;
      longest = Math.max(longest, current);
    }
    return longest;
  };
  const waits = { codex: longestWait("codex"), claude: longestWait("claude"), grok: longestWait("grok") };
  const committed = {
    codex: busy.reduce((total, row) => total + row.codex.committed, 0),
    claude: busy.reduce((total, row) => total + row.claude.committed, 0),
    grok: busy.reduce((total, row) => total + row.grok.committed, 0),
  };
  const lateUnits = observations.flatMap((row) => row.units
    .filter((unit: { offsetMs: number }) => unit.offsetMs >= MAX_WALL_MS)
    .map((unit: { unit: string; offsetMs: number }) => ({ tick: row.tick, ...unit })));
  // The last unit is admitted before 200 ms and one unit costs at most the
  // repair's 220 ms here, so no cadence can pass 420 virtual ms.
  const tickCeilingMs = MAX_WALL_MS + REPAIR_MS;
  const walls = observations.map((row) => row.wallMs as number);
  receipts.busyHost = {
    fixture: { grokSessions: 4_000, codexLiveFiles: 20, claudeLiveFiles: 20, repairDelayMs: REPAIR_MS,
      jsonlReadDelayMs: READ_MS, grokReadMs: GROK_READ_MS, ticks: TICKS, repairFirstEveryTick: true,
      liveWrittenAfterTick },
    committedAfterLiveWork: committed,
    longestConsecutiveCadencesWithoutCommit: waits,
    walls,
    tickCeilingMs,
    lateUnits,
    observations,
  };
  check("progress", "post_enrollment_work_arrives_with_ticks_left_to_observe",
    liveWrittenAfterTick !== null && busy.length >= 6, { liveWrittenAfterTick, busyTicks: busy.length });
  check("progress", "codex_commits_under_a_read_slower_than_any_source_share",
    committed.codex > 0, { committed: committed.codex });
  check("progress", "claude_commits_under_a_read_slower_than_any_source_share",
    committed.claude > 0, { committed: committed.claude });
  check("progress", "grok_commits_alongside_the_slow_sources", committed.grok > 0, { committed: committed.grok });
  check("progress", "no_busy_source_waits_longer_than_the_stated_bound",
    Object.values(waits).every((wait) => wait <= SERVICE_BOUND_CADENCES),
    { boundCadences: SERVICE_BOUND_CADENCES, waits });
  check("progress", "aggregate_byte_record_and_event_ceilings_hold_every_tick",
    observations.every((row) => row.budget.bytesRead <= row.budget.maxBytes &&
      row.budget.recordsParsed <= row.budget.maxRecords && row.budget.eventsAppended <= row.budget.maxEvents));
  check("ceiling", "no_capture_or_repair_unit_starts_after_the_aggregate_wall",
    lateUnits.length === 0, { lateUnits: lateUnits.slice(0, 8), maxWallMs: MAX_WALL_MS, units: unitStarts.length });
  check("ceiling", "every_cadence_ends_within_one_bounded_unit_of_the_ceiling",
    walls.every((wall) => wall <= tickCeilingMs), { walls, tickCeilingMs });
}

/** Blocker 2 of round 1 inside Grok: discovery of one large group with a slow lstat. */
async function grokDiscoveryBound(root: string) {
  const SESSIONS = 2_000;
  const home = path.join(root, "bound", ".grok");
  const group = path.join(home, "sessions", "group");
  const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000);
  for (let index = 0; index < SESSIONS; index += 1) writeGrokSession(home, "group", uuid("ab", index), old);
  fs.utimesSync(group, old, old);
  const buffer = grokLedger(path.join(root, "bound.sqlite"));
  const tailer = new GrokUsageTailer(buffer, home);
  const originalLstat = fs.lstatSync;
  let delayedStats = 0;
  const slowScan = async () => {
    (fs as unknown as { lstatSync: typeof fs.lstatSync }).lstatSync = ((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target).startsWith(group + path.sep)) {
        delayedStats += 1;
        spend(1);
      }
      return (originalLstat as (...values: unknown[]) => fs.Stats)(target, ...rest);
    }) as typeof fs.lstatSync;
    delayedStats = 0;
    const started = virtualNow();
    try {
      const result = await tailer.scan({ budget: new CaptureWorkBudget() });
      return { result, wallMs: virtualNow() - started, delayedStats };
    } finally {
      (fs as unknown as { lstatSync: typeof fs.lstatSync }).lstatSync = originalLstat;
    }
  };
  const firstSlow = await slowScan();
  const first = firstSlow.result;
  let scans = 1;
  for (; scans < 400 && grokEvents(buffer) < SESSIONS; scans += 1) await tailer.scan({ budget: new CaptureWorkBudget() });
  const events = grokEvents(buffer);
  // Steady state: every file is committed and unchanged, so nothing queues
  // and only the budget ends a pass over 2,000 slow lstats. Time every scan
  // of one whole sweep.
  const sweepsBefore = grokSweeps(buffer);
  const steadyScans: Array<{ wallMs: number; delayedStats: number; unchanged: number }> = [];
  for (let scan = 0; scan < 400 && grokSweeps(buffer) === sweepsBefore; scan += 1) {
    const slow = await slowScan();
    steadyScans.push({ wallMs: slow.wallMs, delayedStats: slow.delayedStats, unchanged: slow.result.filesUnchanged });
  }
  const steadyUnchanged = steadyScans.reduce((total, row) => total + row.unchanged, 0);
  tailer.close();
  buffer.close();
  receipts.discovery = {
    fixture: { sessions: SESSIONS, injectedLstatDelayMs: 1 },
    first: { wallMs: firstSlow.wallMs, delayedStats: firstSlow.delayedStats, seen: first.filesSeen,
      committed: first.recordsCommitted, entries: first.activity.discoveryEntries },
    scansToCommitAll: scans,
    events,
    steady: { scans: steadyScans.length, unchanged: steadyUnchanged,
      maxWallMs: Math.max(...steadyScans.map((row) => row.wallMs)),
      maxDelayedStats: Math.max(...steadyScans.map((row) => row.delayedStats)) },
  };
  check("ceiling", "grok_discovery_of_a_large_group_stays_inside_the_budget",
    firstSlow.wallMs <= MAX_WALL_MS, { wallMs: firstSlow.wallMs, maxWallMs: MAX_WALL_MS });
  check("ceiling", "grok_discovery_never_stats_a_whole_directory_in_one_scan",
    firstSlow.delayedStats < SESSIONS / 2, { delayedStats: firstSlow.delayedStats, sessions: SESSIONS });
  check("ceiling", "the_first_bounded_grok_scan_makes_progress",
    first.activity.scan.deferredBeforeIo === false && first.filesSeen > 0 && first.recordsCommitted > 0,
    { seen: first.filesSeen, committed: first.recordsCommitted });
  check("ceiling", "bounded_grok_scans_reach_every_session_exactly_once",
    events === SESSIONS, { events, scans });
  check("ceiling", "every_scan_of_an_unchanged_sweep_stays_inside_the_budget",
    steadyScans.length > 1 && steadyUnchanged === SESSIONS &&
      steadyScans.every((row) => row.wallMs <= MAX_WALL_MS && row.delayedStats < SESSIONS / 2),
    { scans: steadyScans.length, unchanged: steadyUnchanged,
      maxWallMs: Math.max(...steadyScans.map((row) => row.wallMs)) });
}

/**
 * Names in the order a directory stream returns them, which the walk follows.
 * (fs.readdirSync sorts; a stream does not.)
 */
function streamOrder(directory: string) {
  const handle = fs.opendirSync(directory);
  const names: string[] = [];
  try {
    for (let entry = handle.readSync(); entry; entry = handle.readSync()) names.push(entry.name);
  } finally {
    handle.closeSync();
  }
  return names;
}

/**
 * A group of empty sessions whose last entry in directory order holds the
 * one usage file: a walk reaches it only after it has covered all the rest.
 */
function groupEndingInTarget(home: string, group: string, prefix: string, sessions: number) {
  for (let index = 0; index < sessions; index += 1) {
    writeGrokSession(home, group, `${prefix}-${String(index).padStart(4, "0")}`, undefined, false);
  }
  const target = streamOrder(path.join(home, "sessions", group)).at(-1)!;
  writeGrokSession(home, group, target);
  return target;
}

/**
 * Round 3, blocker 2 (the reviewer's constructions): a group larger than
 * one pass, with a new worker every pass.
 */
async function restartEveryPass(root: string) {
  // Production defaults: 2,101 sessions against 2,048 steps a pass.
  const largeHome = path.join(root, "large-restart", ".grok");
  const largeGroup = "large-old-group";
  const largeTarget = groupEndingInTarget(largeHome, largeGroup, "large-session", 2_101);
  const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000);
  fs.utimesSync(path.join(largeHome, "sessions", largeGroup), old, old);
  const largeBuffer = grokLedger(path.join(root, "large-restart.sqlite"));
  const largePasses: Array<{ pass: number; entries: number; targetSeen: boolean }> = [];
  for (let pass = 0; pass < 3 && !grokTurnSeen(largeBuffer, largeTarget); pass += 1) {
    const tailer = new GrokUsageTailer(largeBuffer, largeHome);
    const scan = await tailer.scan({ budget: roomyBudget() });
    largePasses.push({ pass, entries: scan.activity.discoveryEntries, targetSeen: grokTurnSeen(largeBuffer, largeTarget) });
    tailer.close();
  }
  const largeSeen = grokTurnSeen(largeBuffer, largeTarget);
  largeBuffer.close();
  check("lossless", "a_new_worker_every_pass_finishes_a_group_larger_than_one_pass",
    largeSeen, { sessions: 2_101, entriesPerPass: GROK_USAGE_LIMITS.entriesPerPass, boundPasses: 3, passes: largePasses });

  // The scaled construction: five steps a pass, 41 sessions, recent lane off.
  const scaledHome = path.join(root, "scaled-restart", ".grok");
  const scaledGroup = "private-group-name";
  const scaledTarget = groupEndingInTarget(scaledHome, scaledGroup, "private-session", 41);
  const scaledBuffer = grokLedger(path.join(root, "scaled-restart.sqlite"));
  const scaledLimits = { ...GROK_USAGE_LIMITS, entriesPerPass: 5, discoveryWallMs: 1_000, recentGroups: 0,
    recentSessions: 0, maxSessionsPerGroup: 100 };
  // Each pass examines at least three sessions: ceil(41 / 3) passes finish the group.
  const scaledBound = Math.ceil(41 / 3);
  let scaledPasses = 0;
  for (; scaledPasses < scaledBound && !grokTurnSeen(scaledBuffer, scaledTarget); scaledPasses += 1) {
    const tailer = new GrokUsageTailer(scaledBuffer, scaledHome, scaledLimits);
    await tailer.scan({ budget: roomyBudget() });
    tailer.close();
  }
  const scaledSeen = grokTurnSeen(scaledBuffer, scaledTarget);
  scaledBuffer.close();
  check("lossless", "a_new_worker_every_pass_keeps_every_pass_of_progress",
    scaledSeen, { sessions: 41, entriesPerPass: 5, boundPasses: scaledBound, passes: scaledPasses });
  receipts.restartEveryPass = { large: largePasses, scaled: { passes: scaledPasses, seen: scaledSeen } };
}

/**
 * Round 3, blocker 3: bounded add/remove churn. Population three, two
 * sessions a pass per group, two fresh entries added ahead of an old
 * session before every pass and removed after it.
 */
async function boundedChurn(root: string, order: "name-hash" | "directory") {
  const identifyingGroup = `customer-secret-project-directory-${order}`;
  const home = path.join(root, `churn-${order}`, ".grok");
  const groupDirectory = path.join(home, "sessions", identifyingGroup);
  fs.mkdirSync(groupDirectory, { recursive: true, mode: 0o700 });
  const names = Array.from({ length: 80 }, (_, index) => `identifying-session-${String(index).padStart(3, "0")}`);
  let ordered: string[];
  if (order === "name-hash") {
    // The reviewer's choice: ahead of the old session in round 2's hash order.
    ordered = [...names].sort((left, right) => walkHash(left).localeCompare(walkHash(right)));
  } else {
    // The same adversary against this walk: ahead of it in directory order.
    for (const name of names) fs.mkdirSync(path.join(groupDirectory, name), { mode: 0o700 });
    ordered = streamOrder(groupDirectory).filter((name) => names.includes(name));
    for (const name of names) fs.rmSync(path.join(groupDirectory, name), { recursive: true, force: true });
  }
  const target = ordered.at(-1)!;
  const churn = ordered.slice(0, 20);
  writeGrokSession(home, identifyingGroup, target);
  const buffer = grokLedger(path.join(root, `churn-${order}.sqlite`));
  const tailer = new GrokUsageTailer(buffer, home, {
    ...GROK_USAGE_LIMITS, maxSessionsPerGroup: 2, entriesPerPass: 1_000, discoveryWallMs: 1_000,
    recentGroups: 0, recentSessions: 0,
  });
  // Three present sessions at two a pass: the sweep they began takes two passes.
  const boundPasses = 2;
  const passes: Array<{ pass: number; targetSeen: boolean; sessionsOverLimit: number }> = [];
  let seenAtPass: number | null = null;
  for (let pass = 0; pass < 10; pass += 1) {
    const active = churn.slice(pass * 2, pass * 2 + 2);
    for (const name of active) fs.mkdirSync(path.join(groupDirectory, name), { mode: 0o700 });
    const scan = await tailer.scan({ budget: roomyBudget() });
    const files = scan.activity.scan.usageFiles as { sessionsOverLimit?: number };
    const seen = grokTurnSeen(buffer, target);
    if (seen && seenAtPass === null) seenAtPass = pass + 1;
    passes.push({ pass, targetSeen: seen, sessionsOverLimit: files.sessionsOverLimit ?? 0 });
    for (const name of active) fs.rmSync(path.join(groupDirectory, name), { recursive: true, force: true });
    // Order creation times after this pass unambiguously (ms resolution).
    pause(3);
  }
  tailer.close();
  buffer.close();
  receipts[`churn_${order}`] = { population: 3, cap: 2, boundPasses, seenAtPass, passes };
  check("lossless", `bounded_churn_ahead_in_${order.replace("-", "_")}_order_cannot_keep_an_old_session_out`,
    seenAtPass !== null && seenAtPass <= boundPasses, { boundPasses, seenAtPass, passes: passes.slice(0, 4) });
}

/** Caps, recency, restarts with legacy state, and the durable state's privacy. */
async function lossless(root: string) {
  const old = (offsetMs: number) => new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000 + offsetMs);

  // Two sessions a pass per group, three sessions: one sweep, two passes.
  const cappedHome = path.join(root, "capped", ".grok");
  const cappedBuffer = grokLedger(path.join(root, "capped.sqlite"));
  const cappedGroup = encodeURIComponent("/Users/example/capped-project");
  const sessions = [uuid("d0", 1), uuid("e0", 2), uuid("f0", 3)];
  sessions.forEach((sessionId, index) => writeGrokSession(cappedHome, cappedGroup, sessionId, old(3_000 - index * 1_000)));
  const cappedTailer = new GrokUsageTailer(cappedBuffer, cappedHome,
    { ...GROK_USAGE_LIMITS, maxSessionsPerGroup: 2, entriesPerPass: 100, discoveryWallMs: 1_000 });
  let sessionsOverLimit = 0;
  let cappedScans = 0;
  for (; cappedScans < 8 && grokSweeps(cappedBuffer) < 1; cappedScans += 1) {
    const result = await cappedTailer.scan({ budget: roomyBudget() });
    sessionsOverLimit += Number((result.activity.scan.usageFiles as { sessionsOverLimit?: number }).sessionsOverLimit ?? 0);
  }
  const cappedSeen = sessions.map((sessionId) => grokTurnSeen(cappedBuffer, sessionId));
  const cappedEvents = grokEvents(cappedBuffer);
  cappedTailer.close();
  cappedBuffer.close();
  check("lossless", "a_capped_group_is_finished_within_its_first_sweep",
    cappedSeen.every(Boolean) && cappedEvents === sessions.length,
    { seen: cappedSeen, events: cappedEvents, scans: cappedScans });
  check("lossless", "a_capped_group_is_reported_not_silently_skipped", sessionsOverLimit > 0, { sessionsOverLimit });

  // Two groups a pass, three groups.
  const groupsHome = path.join(root, "groups", ".grok");
  const groupsBuffer = grokLedger(path.join(root, "groups.sqlite"));
  const groupSessions = ["alpha", "beta", "gamma"].map((name, index) => {
    const sessionId = uuid("c1", index);
    writeGrokSession(groupsHome, `%2Ftmp%2F${name}`, sessionId, old(3_000 - index * 1_000));
    return sessionId;
  });
  const groupsTailer = new GrokUsageTailer(groupsBuffer, groupsHome,
    { ...GROK_USAGE_LIMITS, maxGroups: 2, entriesPerPass: 100, discoveryWallMs: 1_000 });
  let groupsOverLimit = 0;
  for (let scan = 0; scan < 8 && grokSweeps(groupsBuffer) < 1; scan += 1) {
    const result = await groupsTailer.scan({ budget: roomyBudget() });
    groupsOverLimit += Number((result.activity.scan.usageFiles as { groupsOverLimit?: number }).groupsOverLimit ?? 0);
  }
  const groupsSeen = groupSessions.map((sessionId) => grokTurnSeen(groupsBuffer, sessionId));
  const groupsEvents = grokEvents(groupsBuffer);
  groupsTailer.close();
  groupsBuffer.close();
  check("lossless", "capped_groups_are_all_reached_within_the_first_sweep",
    groupsSeen.every(Boolean) && groupsEvents === groupSessions.length, { groupsSeen, events: groupsEvents });
  check("lossless", "capped_groups_are_reported_not_silently_skipped", groupsOverLimit > 0, { groupsOverLimit });

  // Capped passes over empty and unreadable groups, a new worker every pass.
  const sparseHome = path.join(root, "sparse", ".grok");
  const sparseBuffer = grokLedger(path.join(root, "sparse.sqlite"));
  const sparseSessions: string[] = [];
  const unreadable = path.join(sparseHome, "sessions", "unreadable");
  for (let group = 0; group < 9; group += 1) {
    const name = `sparse-${group}`;
    if (group % 3 === 0) {
      const sessionId = uuid("3d", group);
      writeGrokSession(sparseHome, name, sessionId, old(group));
      sparseSessions.push(sessionId);
    } else {
      fs.mkdirSync(path.join(sparseHome, "sessions", name), { recursive: true, mode: 0o700 });
    }
  }
  fs.mkdirSync(unreadable, { recursive: true, mode: 0o700 });
  fs.chmodSync(unreadable, 0o000);
  let sparseScans = 0;
  try {
    for (; sparseScans < 40 && sparseSessions.some((sessionId) => !grokTurnSeen(sparseBuffer, sessionId)); sparseScans += 1) {
      const tailer = new GrokUsageTailer(sparseBuffer, sparseHome,
        { ...GROK_USAGE_LIMITS, maxGroups: 2, entriesPerPass: 100, discoveryWallMs: 1_000 });
      await tailer.scan({ budget: roomyBudget() });
      tailer.close();
    }
  } finally {
    fs.chmodSync(unreadable, 0o700);
  }
  const sparseSeen = sparseSessions.filter((sessionId) => grokTurnSeen(sparseBuffer, sessionId)).length;
  const sparseEvents = grokEvents(sparseBuffer);
  sparseBuffer.close();
  check("lossless", "capped_passes_move_past_empty_and_unreadable_groups",
    sparseSeen === sparseSessions.length && sparseEvents === sparseSessions.length,
    { seen: sparseSeen, sessions: sparseSessions.length, events: sparseEvents, scans: sparseScans });

  // Recent first: today's sessions in a recently active group come before
  // 1,200 older sessions, in the first bounded scan.
  const recentHome = path.join(root, "recent", ".grok");
  const recentBuffer = grokLedger(path.join(root, "recent.sqlite"));
  for (let group = 0; group < 30; group += 1) {
    for (let session = 0; session < 40; session += 1) {
      writeGrokSession(recentHome, `group-${String(group).padStart(3, "0")}`, uuid("5a", group * 40 + session),
        old(group * 40 + session));
    }
  }
  const recentAt = new Date(Date.now() - 60 * 60 * 1_000);
  const recentSessions = Array.from({ length: 5 }, (_, index) => {
    const sessionId = uuid("6b", index);
    writeGrokSession(recentHome, "group-015", sessionId, recentAt);
    return sessionId;
  });
  const recentTailer = new GrokUsageTailer(recentBuffer, recentHome);
  const firstRecent = await recentTailer.scan({ budget: new CaptureWorkBudget() });
  const recentSeen = recentSessions.map((sessionId) => grokTurnSeen(recentBuffer, sessionId));
  recentTailer.close();
  recentBuffer.close();
  check("lossless", "recent_sessions_are_committed_in_the_first_bounded_scan",
    recentSeen.every(Boolean), { recentSeen, firstScanEvents: firstRecent.eventsAppended });

  // A new worker every scan, a tiny pass allowance and cursor values left by
  // 0.7.37 and by round 2: every session exactly once. Then the durable walk
  // state must name no group or session.
  const restartHome = path.join(root, "restart", ".grok");
  const restartBuffer = grokLedger(path.join(root, "restart.sqlite"));
  const restartSessions: string[] = [];
  const restartGroups: string[] = [];
  for (let group = 0; group < 4; group += 1) {
    const groupName = encodeURIComponent(`/Users/example/private-${group}`);
    restartGroups.push(groupName);
    for (let session = 0; session < 5; session += 1) {
      const sessionId = uuid("4c", group * 5 + session);
      writeGrokSession(restartHome, groupName, sessionId, old(group * 5 + session));
      restartSessions.push(sessionId);
    }
  }
  setState(restartBuffer, "grok_usage_sweep_resume_v1", "1");
  const stateSnapshots: string[] = [];
  let restartScans = 0;
  for (; restartScans < 60 && restartSessions.some((sessionId) => !grokTurnSeen(restartBuffer, sessionId)); restartScans += 1) {
    if (restartScans === 3) {
      setState(restartBuffer, "grok_usage_sweep_resume_v1",
        JSON.stringify({ version: 2, group: walkHash(restartGroups[2]!), session: walkHash(restartSessions[11]!) }));
    }
    const tailer = new GrokUsageTailer(restartBuffer, restartHome,
      { ...GROK_USAGE_LIMITS, entriesPerPass: 6, discoveryWallMs: 1_000 });
    await tailer.scan({ budget: roomyBudget() });
    tailer.close();
    stateSnapshots.push(state(restartBuffer, "grok_usage_walk_round_v1") ?? "");
  }
  const restartSeen = restartSessions.filter((sessionId) => grokTurnSeen(restartBuffer, sessionId)).length;
  const restartEvents = grokEvents(restartBuffer);
  const coveredNamesTable = restartBuffer.database.prepare(
    "select 1 from sqlite_master where type = 'table' and name = 'grok_usage_walk_visits'",
  ).get() !== undefined;
  restartBuffer.close();
  check("lossless", "a_walk_restarted_every_scan_reaches_every_session_once",
    restartSeen === restartSessions.length && restartEvents === restartSessions.length,
    { seen: restartSeen, sessions: restartSessions.length, events: restartEvents, scans: restartScans });
  const leaks = stateSnapshots.filter((snapshot) =>
    snapshot.includes("private-") || snapshot.includes("Users") || snapshot.includes(path.sep) ||
    restartSessions.some((sessionId) => snapshot.includes(sessionId)));
  // The walk state is one cursor of name hashes; round 3's table of covered names is gone.
  const shapes = stateSnapshots.every((snapshot) => {
    const cursor = (JSON.parse(snapshot || "null") as { cursor?: { group: string; session: string } | null } | null)
      ?.cursor ?? null;
    return cursor === null || (/^[0-9a-f]{32}$/.test(cursor.group) && /^([0-9a-f]{32}|~)?$/.test(cursor.session));
  }) && !coveredNamesTable;
  check("lossless", "the_durable_walk_state_names_no_group_or_session",
    stateSnapshots.length > 0 && leaks.length === 0 && shapes, { snapshots: stateSnapshots.length, leaks: leaks.length });
  receipts.lossless = { cappedScans, sessionsOverLimit, groupsOverLimit, sparseScans, recentSeen,
    restart: { scans: restartScans, seen: restartSeen, events: restartEvents } };
}

async function main() {
  installVirtualClock();
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "plimsoll-busy-host-proof-"));
  try {
    if (scenario === "all" || scenario === "progress") {
      await failingLeader(root, "capture");
      await failingLeader(root, "repair");
      await slowBookkeeping(root);
      baselineStatusPlan(root);
    }
    if (scenario === "all" || scenario === "progress" || scenario === "ceiling") await busyHost(path.join(root, "busy"));
    if (scenario === "all" || scenario === "ceiling") await grokDiscoveryBound(root);
    if (scenario === "all" || scenario === "lossless") {
      await restartEveryPass(root);
      await boundedChurn(root, "name-hash");
      await boundedChurn(root, "directory");
      await lossless(root);
    }
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  const failed = checks.filter((entry) => !entry.passed);
  const deterministicDigest = `sha256:${crypto.createHash("sha256")
    .update(JSON.stringify({ checks, receipts })).digest("hex")}`;
  console.log(JSON.stringify({
    schema: "eco-6hoxj.163.42.busy-host-capture-proof.v3",
    scenario,
    ok: failed.length === 0 && checks.length > 0,
    counts: { total: checks.length, passed: checks.length - failed.length, failed: failed.length },
    failed: failed.map((entry) => `${entry.scenario}:${entry.name}`),
    deterministicDigest,
    checks,
    receipts,
  }, null, 2));
  if (failed.length > 0 || checks.length === 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
