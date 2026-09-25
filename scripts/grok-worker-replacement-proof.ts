/**
 * Grok worker replacement proof (eco-6hoxj.163.42, rounds 4 and 5).
 *
 * The walk visits groups, then each group's sessions, in the order of the
 * hashes of their names, from one durable cursor. Each check below is built
 * from a reviewer's construction:
 *
 *   studio0     Round 3: 4,000 sessions in 20 groups, 12 ms per group
 *               lstat, production limits and cadence budget, a new worker
 *               every pass. The last session in walk order is reached within
 *               the stated bound, every pass moves the cursor, and the
 *               durable walk state is one cursor.
 *   early-end   Round 4, blocker 1: a walk-state row cap once ended a sweep
 *               early and restarted it from the first group, so a session
 *               beyond the cap was never read. The reviewer's 17-session case
 *               and a Studio0-scale case both finish within their bounds.
 *   read-cap    Round 4, blocker 2: a new worker once had to read past every
 *               session already walked, and stopped at the pass's read
 *               allowance. Now every pass of a replaced worker commits a new
 *               session.
 *   clock       Round 4: a clock 24 hours behind the file system, or jumping
 *               forward 11 minutes a pass, delays nothing past its bound and
 *               marks nothing clean that was not read.
 *   slow        A group lstat slower than a whole pass: every new worker
 *               still moves the cursor, so the tree finishes within N + G
 *               passes.
 *   churn       Five new groups before every pass, each removed after it,
 *               with a new worker every pass: the state stays one cursor and
 *               every old session is committed once while the churn lasts.
 *   honest      A sweep finished by a later worker is not reported clean
 *               when an earlier worker of it left a queued file unread or met
 *               an unreadable group; the next sweep reads what was missed,
 *               once.
 *
 * Time is virtual (scripts/lib/virtual-clock.ts): only the slow lstats the
 * fixture declares cost time, so every count is the same on any host.
 *
 *   pnpm proof:grok-worker-replacement [-- --scenario=studio0|early-end|read-cap|clock|slow|churn|honest]
 */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import {
  GROK_USAGE_LIMITS, GrokUsageTailer, ensureGrokUsageState, type GrokUsageLimits,
} from "../packages/collector-cli/src/grok-usage-tailer";
import { GROK_USAGE_BACKFILL_KEY, type GrokUsageBackfillMarker } from "../packages/collector-cli/src/history-coverage";
import { installVirtualClock, spend } from "./lib/virtual-clock";

const scenario = process.argv.find((arg) => arg.startsWith("--scenario="))?.split("=", 2)[1] ?? "all";
const checks: Array<{ scenario: string; name: string; passed: boolean; detail: unknown }> = [];
const receipts: Record<string, unknown> = {};

function check(group: string, name: string, passed: boolean, detail: unknown = null) {
  checks.push({ scenario: group, name, passed, detail });
}

const walkHash = (name: string) =>
  crypto.createHash("sha256").update(`plimsoll-grok-walk-v1\0${name}`).digest("hex").slice(0, 32);
const byHash = (left: string, right: string) => walkHash(left) < walkHash(right) ? -1 : 1;

function grokUsage(sessionId: string) {
  const endedAt = new Date().toISOString();
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

function names(directory: string) {
  return fs.readdirSync(directory).filter((name) => fs.lstatSync(path.join(directory, name)).isDirectory());
}

type Tree = { home: string; sessions: string; target: string; groups: Map<string, string[]> };

function streamOrder(directory: string) {
  const handle = fs.opendirSync(directory);
  const listed: string[] = [];
  try {
    for (let entry = handle.readSync(); entry; entry = handle.readSync()) listed.push(entry.name);
  } finally {
    handle.closeSync();
  }
  return listed;
}

/**
 * Groups of old sessions. The last session of the last group in walk order
 * holds a usage file (every session does with `everySession`): a walk
 * reaches it only after everything else.
 */
function tree(home: string, groups: number, sessionsPerGroup: number, everySession = false,
  targetOrder: "walk" | "directory" = "walk"): Tree {
  const sessions = path.join(home, "sessions");
  const layout = new Map<string, string[]>();
  for (let group = 0; group < groups; group += 1) {
    const name = `private-project-${String(group).padStart(2, "0")}`;
    const members: string[] = [];
    for (let session = 0; session < sessionsPerGroup; session += 1) {
      const id = `session-${String(group).padStart(2, "0")}-${String(session).padStart(3, "0")}`;
      fs.mkdirSync(path.join(sessions, name, id), { recursive: true, mode: 0o700 });
      if (everySession) fs.writeFileSync(path.join(sessions, name, id, "usage.json"), JSON.stringify(grokUsage(id)), { mode: 0o600 });
      members.push(id);
    }
    layout.set(name, members.sort(byHash));
  }
  // Last in walk order, or (the reviewers' constructions for round 4) last
  // in directory order, the order round 4 walked in.
  const lastGroup = targetOrder === "walk" ? [...layout.keys()].sort(byHash).at(-1)! : streamOrder(sessions).at(-1)!;
  const target = targetOrder === "walk"
    ? layout.get(lastGroup)!.at(-1)!
    : streamOrder(path.join(sessions, lastGroup)).at(-1)!;
  if (!everySession) {
    fs.writeFileSync(path.join(sessions, lastGroup, target, "usage.json"), JSON.stringify(grokUsage(target)), { mode: 0o600 });
  }
  const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000);
  for (const group of layout.keys()) fs.utimesSync(path.join(sessions, group), old, old);
  return { home, sessions, target, groups: layout };
}

/** Charge `ms` of virtual time for every lstat of a group directory. */
function slowGroupStats(sessions: string, ms: number) {
  const original = fs.lstatSync;
  const replace = (value: typeof fs.lstatSync) =>
    Object.defineProperty(fs, "lstatSync", { configurable: true, writable: true, value });
  replace(((target: fs.PathLike, options?: fs.StatSyncOptions) => {
    if (path.dirname(String(target)) === sessions) spend(ms);
    return original(target, options as never);
  }) as typeof fs.lstatSync);
  return () => replace(original);
}

function ledger(file: string) {
  const buffer = new LocalEventBuffer(file);
  ensureGrokUsageState(buffer.database);
  return buffer;
}

function seen(buffer: LocalEventBuffer, sessionId: string) {
  return Boolean(buffer.database.prepare(
    "select 1 from grok_usage_turn_state where session_id = ? limit 1",
  ).get(sessionId));
}

function events(buffer: LocalEventBuffer) {
  return Number((buffer.database.prepare(
    "select count(*) as count from buffered_events where source = 'grok' and input_tokens is not null",
  ).get() as { count: number }).count);
}

function distinctEventSessions(buffer: LocalEventBuffer) {
  return Number((buffer.database.prepare(
    "select count(distinct session_id) as count from buffered_events where source = 'grok' and input_tokens is not null",
  ).get() as { count: number }).count);
}

function marker(buffer: LocalEventBuffer) {
  const row = buffer.database.prepare("select value from maintenance_state where key = ?")
    .get(GROK_USAGE_BACKFILL_KEY) as { value: string } | undefined;
  return row ? JSON.parse(row.value) as GrokUsageBackfillMarker : null;
}

/** The durable walk state, as stored. */
function walkState(buffer: LocalEventBuffer) {
  const row = buffer.database.prepare("select value from maintenance_state where key = 'grok_usage_walk_round_v1'")
    .get() as { value: string } | undefined;
  return row?.value ?? "";
}

type Cursor = { group: string; session: string } | null;

/**
 * Entries the walk's cursor has moved past: a group counts itself and its
 * sessions once finished. A finished sweep has moved past them all.
 */
function position(buffer: LocalEventBuffer, layout: Map<string, string[]>, round: number) {
  const total = [...layout.values()].reduce((sum, sessions) => sum + sessions.length + 1, 0);
  const stored = JSON.parse(walkState(buffer) || "null") as { round: number; cursor?: Cursor } | null;
  if (!stored || stored.round > round) return total;
  if (stored.cursor === undefined) return coveredNames(buffer, layout);
  const cursor = stored.cursor;
  if (cursor === null) return 0;
  let passed = 0;
  for (const [group, sessions] of layout) {
    const hash = walkHash(group);
    if (hash < cursor.group || (hash === cursor.group && cursor.session === "~")) passed += sessions.length + 1;
    else if (hash === cursor.group) passed += sessions.filter((session) => walkHash(session) <= cursor.session).length;
  }
  return passed;
}

/**
 * Round 4 kept a table of covered names instead of a cursor: a finished
 * group counts itself and its sessions, a group being walked its covered
 * sessions. Used only to report round 4's progress.
 */
function coveredNames(buffer: LocalEventBuffer, layout: Map<string, string[]>) {
  const sizes = new Map([...layout].map(([group, sessions]) => [walkHash(group), sessions.length + 1]));
  const rows = buffer.database.prepare("select group_hash as g, session_hash as s from grok_usage_walk_visits")
    .all() as Array<{ g: string; s: string }>;
  return rows.reduce((sum, row) => sum + (row.s === "" ? sizes.get(row.g) ?? 1 : 1), 0);
}

/** The durable walk state is one cursor of hashes: small, and naming no path. */
function stateIsOneCursor(buffer: LocalEventBuffer, layout: Map<string, string[]>) {
  const value = walkState(buffer);
  const parsed = JSON.parse(value || "null") as { cursor?: Cursor } | null;
  const cursor = parsed?.cursor ?? null;
  const hashes = cursor === null ||
    (/^[0-9a-f]{32}$/.test(cursor.group) && /^([0-9a-f]{32}|~)?$/.test(cursor.session));
  const leaks = [...layout.keys(), ...[...layout.values()].flat()].some((name) => value.includes(name)) ||
    value.includes(path.sep);
  const table = Boolean(buffer.database.prepare(
    "select 1 from sqlite_master where type = 'table' and name = 'grok_usage_walk_visits'",
  ).get());
  return { bytes: Buffer.byteLength(value), hashes, leaks, table };
}

type PassLog = { pass: number; steps: number; position: number; events: number; stateBytes: number; recentLaneStarted: boolean };

async function passes(options: {
  buffer: LocalEventBuffer;
  tree: Tree;
  limit: number;
  replace: (pass: number) => boolean;
  limits?: GrokUsageLimits;
  budget?: () => CaptureWorkBudget;
  now?: (pass: number) => Date | undefined;
  until?: () => boolean;
}) {
  const log: PassLog[] = [];
  let worker: GrokUsageTailer | null = null;
  let reachedAt: number | null = null;
  let stateMaxBytes = 0;
  let stateOk = true;
  try {
    for (let pass = 1; pass <= options.limit && reachedAt === null; pass += 1) {
      const round = (JSON.parse(walkState(options.buffer) || "null") as { round?: number } | null)?.round ?? 1;
      worker ??= new GrokUsageTailer(options.buffer, options.tree.home, options.limits);
      // The production cadence budget: 200 ms of admission, of which a
      // discovery pass takes at most its 50 ms allowance.
      const scan = await worker.scan({
        budget: options.budget?.() ?? new CaptureWorkBudget(AUTOMATIC_CAPTURE_LIMITS),
        now: options.now?.(pass),
      });
      if (options.replace(pass)) {
        worker.close();
        worker = null;
      }
      const state = stateIsOneCursor(options.buffer, options.tree.groups);
      stateMaxBytes = Math.max(stateMaxBytes, state.bytes);
      stateOk = stateOk && state.hashes && !state.leaks && !state.table;
      log.push({
        pass, steps: scan.activity.discoveryEntries, position: position(options.buffer, options.tree.groups, round),
        events: events(options.buffer), stateBytes: state.bytes,
        recentLaneStarted: (JSON.parse(walkState(options.buffer) || "null") as { recentAtMs?: number | null } | null)
          ?.recentAtMs != null,
      });
      if (options.until ? options.until() : seen(options.buffer, options.tree.target)) reachedAt = pass;
    }
  } finally {
    worker?.close();
  }
  return { reachedAt, log, stateMaxBytes, stateOk };
}

const recentLaneOff: GrokUsageLimits = { ...GROK_USAGE_LIMITS, recentGroups: 0, recentSessions: 0 };
const roomy = () => new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 1_000_000 });
const movesEveryPass = (log: PassLog[]) => log.every((row, index) =>
  row.position > (index === 0 ? 0 : log[index - 1]!.position));

/** Round 3's construction at Studio0's scale, with a new worker every pass. */
async function studio0(root: string, fixture: Tree) {
  const GROUPS = fixture.groups.size;
  const GROUP_STAT_MS = 12;
  // The first pass of a sweep finishes at least one group beside the recent
  // lane's half of the allowance; every later pass of a new worker finishes
  // at least floor(50 / 12) = 4, because the recent lane is not started
  // again within recentRefreshMs of its last start.
  const perPass = Math.floor(GROK_USAGE_LIMITS.discoveryWallMs / GROUP_STAT_MS);
  const bound = 1 + Math.ceil((GROUPS - 1) / perPass);
  const firstGroup = (fixture.groups.values().next().value as string[]).length + 1;
  const restore = slowGroupStats(fixture.sessions, GROUP_STAT_MS);
  try {
    const replacedBuffer = ledger(path.join(root, "studio0-replaced.sqlite"));
    const replaced = await passes({ buffer: replacedBuffer, tree: fixture, limit: 40, replace: () => true });
    replacedBuffer.close();
    const persistentBuffer = ledger(path.join(root, "studio0-persistent.sqlite"));
    const persistent = await passes({ buffer: persistentBuffer, tree: fixture, limit: 40, replace: () => false });
    persistentBuffer.close();
    const total = [...fixture.groups.values()].reduce((sum, sessions) => sum + sessions.length + 1, 0);
    // Until the sweep is finished: the next sweep starts its own recent lane.
    const recentStarts = new Set(replaced.log.filter((row) => row.position < total).map((row) => row.recentLaneStarted));
    receipts.studio0 = {
      fixture: { groups: GROUPS, sessionsPerGroup: firstGroup - 1, groupStatMs: GROUP_STAT_MS,
        discoveryWallMs: GROK_USAGE_LIMITS.discoveryWallMs, cadenceAdmissionMs: AUTOMATIC_CAPTURE_LIMITS.maxWallMs },
      bound, replaced: { reachedAt: replaced.reachedAt, log: replaced.log },
      persistent: { reachedAt: persistent.reachedAt }, stateMaxBytes: Math.max(replaced.stateMaxBytes, persistent.stateMaxBytes),
    };
    check("studio0", "a_worker_replaced_every_pass_reaches_the_last_session_within_the_stated_bound",
      replaced.reachedAt !== null && replaced.reachedAt <= bound, { bound, reachedAt: replaced.reachedAt });
    check("studio0", "the_first_pass_finishes_a_group_beside_the_recent_lane",
      (replaced.log[0]?.position ?? 0) >= firstGroup, { firstPass: replaced.log[0]?.position ?? 0, group: firstGroup });
    check("studio0", "every_pass_of_a_replaced_worker_moves_the_cursor",
      movesEveryPass(replaced.log), { positions: replaced.log.map((row) => row.position) });
    check("studio0", "a_replaced_worker_does_not_start_the_recent_lane_again_before_it_is_due",
      recentStarts.size === 1 && recentStarts.has(true), { recentLaneStarted: [...recentStarts] });
    check("studio0", "a_persistent_worker_reaches_it_too",
      persistent.reachedAt !== null && persistent.reachedAt <= 2 * bound, { bound: 2 * bound, reachedAt: persistent.reachedAt });
    check("studio0", "the_durable_walk_state_is_one_cursor_of_hashes",
      replaced.stateOk && persistent.stateOk && Math.max(replaced.stateMaxBytes, persistent.stateMaxBytes) <= 256,
      { stateMaxBytes: Math.max(replaced.stateMaxBytes, persistent.stateMaxBytes), limit: 256 });
  } finally {
    restore();
  }
}

/**
 * Round 4, blocker 1. A walk-state row cap (`maxWalkStateRows`, which the
 * cursor walk no longer has) ended a sweep early and began the next at the
 * first group again, so a session beyond the cap was never read. The
 * reviewer's case: 17 sessions with the last in directory order
 * token-bearing, a 5-row cap, 7 steps a pass and a new worker every pass. At
 * Studio0's scale: 4,000 sessions, a 50-row cap and 100 steps a pass.
 */
async function earlyEnd(root: string) {
  const small = tree(path.join(root, "early-end", ".grok"), 1, 17, false, "directory");
  const smallBuffer = ledger(path.join(root, "early-end.sqlite"));
  // Each pass lists sessions/ and the group and examines five sessions.
  const smallBound = Math.ceil(17 / 5);
  const smallRun = await passes({ buffer: smallBuffer, tree: small, limit: 12, replace: () => true,
    limits: { ...recentLaneOff, entriesPerPass: 7, maxWalkStateRows: 5 } as GrokUsageLimits });
  const smallMarker = marker(smallBuffer);
  smallBuffer.close();
  const large = tree(path.join(root, "early-end-studio0", ".grok"), 20, 200, false, "directory");
  const largeBuffer = ledger(path.join(root, "early-end-studio0.sqlite"));
  // A pass lists sessions/ and at most two groups: 96 more entries at least.
  const largeBound = Math.ceil((20 * 200 + 20) / (100 - 4));
  const largeRun = await passes({ buffer: largeBuffer, tree: large, limit: 2 * largeBound, replace: () => true,
    budget: roomy, limits: { ...recentLaneOff, entriesPerPass: 100, discoveryWallMs: 1_000_000,
      maxWalkStateRows: 50 } as GrokUsageLimits });
  const largeMarker = marker(largeBuffer);
  largeBuffer.close();
  receipts.earlyEnd = {
    small: { sessions: 17, cap: 5, entriesPerPass: 7, bound: smallBound, reachedAt: smallRun.reachedAt,
      sweeps: smallMarker?.sweeps ?? 0, log: smallRun.log },
    large: { sessions: 4_000, cap: 50, entriesPerPass: 100, bound: largeBound, reachedAt: largeRun.reachedAt,
      sweeps: largeMarker?.sweeps ?? 0 },
  };
  check("early-end", "the_reviewers_17_session_case_reaches_the_last_session_within_the_bound",
    smallRun.reachedAt !== null && smallRun.reachedAt <= smallBound, { bound: smallBound, reachedAt: smallRun.reachedAt });
  check("early-end", "a_studio0_scale_tree_reaches_the_last_session_within_the_bound",
    largeRun.reachedAt !== null && largeRun.reachedAt <= largeBound, { bound: largeBound, reachedAt: largeRun.reachedAt });
  check("early-end", "no_sweep_ends_before_its_walk_is_done",
    [smallMarker, largeMarker].every((current) => (current?.sweeps ?? 0) === 0 || current?.lastSweep?.clean === true),
    { sweeps: [smallMarker?.sweeps ?? 0, largeMarker?.sweeps ?? 0] });
  check("early-end", "the_walk_state_stays_one_cursor_while_it_walks",
    smallRun.stateOk && largeRun.stateOk && Math.max(smallRun.stateMaxBytes, largeRun.stateMaxBytes) <= 256,
    { stateMaxBytes: Math.max(smallRun.stateMaxBytes, largeRun.stateMaxBytes) });
}

/**
 * Round 4, blocker 2. A worker kept alive walks the first 100 of 110
 * token-bearing sessions; then a new worker comes every pass with 3 steps
 * and 96 reads a pass. Each new worker must commit a new session.
 */
async function readCap(root: string) {
  const fixture = tree(path.join(root, "read-cap", ".grok"), 1, 110, true);
  const buffer = ledger(path.join(root, "read-cap.sqlite"));
  const limits: GrokUsageLimits = { ...recentLaneOff, entriesPerPass: 3 };
  const warm = await passes({ buffer, tree: fixture, limit: 200, replace: () => false, limits,
    until: () => events(buffer) >= 100 });
  const before = events(buffer);
  const fresh = await passes({ buffer, tree: fixture, limit: 20, replace: () => true, limits,
    until: () => events(buffer) >= 110 });
  const commits = fresh.log.map((row, index) => row.events - (index === 0 ? before : fresh.log[index - 1]!.events));
  const bound = 110 - before;
  receipts.readCap = { sessions: 110, readsPerPass: limits.entriesPerPass * 32, warmPasses: warm.reachedAt,
    eventsBefore: before, freshPasses: fresh.reachedAt, commits, events: events(buffer),
    distinctSessions: distinctEventSessions(buffer) };
  check("read-cap", "every_pass_of_a_new_worker_commits_a_new_session",
    fresh.log.length > 0 && commits.every((count) => count >= 1), { commits });
  check("read-cap", "the_rest_of_a_group_larger_than_the_read_allowance_is_committed_within_the_bound",
    fresh.reachedAt !== null && fresh.reachedAt <= bound && events(buffer) === 110 && distinctEventSessions(buffer) === 110,
    { bound, reachedAt: fresh.reachedAt, events: events(buffer) });
  buffer.close();
}

/**
 * Round 4: the scan's clock 24 hours behind the file system's creation
 * times, and a clock that jumps forward 11 minutes before every pass, with a
 * new worker every pass on the Studio0-scale tree.
 */
async function clock(root: string, fixture: Tree) {
  const GROUP_STAT_MS = 12;
  const restore = slowGroupStats(fixture.sessions, GROUP_STAT_MS);
  try {
    const backwardBuffer = ledger(path.join(root, "clock-backward.sqlite"));
    const backward = await passes({ buffer: backwardBuffer, tree: fixture, limit: 12, replace: () => true,
      now: () => new Date(Date.now() - 24 * 60 * 60 * 1_000) });
    const backwardMarker = marker(backwardBuffer);
    backwardBuffer.close();
    const forwardBuffer = ledger(path.join(root, "clock-forward.sqlite"));
    const forward = await passes({ buffer: forwardBuffer, tree: fixture, limit: 40, replace: () => true,
      now: (pass) => new Date(Date.now() + pass * 11 * 60 * 1_000) });
    forwardBuffer.close();
    // Forward: the recent lane is due every pass and takes half of it; the
    // walk still finishes at least one 12 ms group a pass.
    const forwardBound = fixture.groups.size + 1;
    // Timestamps stay out of the receipt: its digest depends on the fixture alone.
    const completed = backwardMarker?.completedAt != null;
    receipts.clock = { backward: { reachedAt: backward.reachedAt, completed,
      clean: backwardMarker?.lastSweep?.clean ?? null }, forward: { reachedAt: forward.reachedAt, bound: forwardBound } };
    check("clock", "a_clock_24_hours_behind_the_file_system_delays_nothing_past_the_bound",
      backward.reachedAt !== null && backward.reachedAt <= 6, { bound: 6, reachedAt: backward.reachedAt });
    check("clock", "no_sweep_is_marked_complete_before_its_sessions_are_read",
      !completed || backward.reachedAt !== null, { completed, reachedAt: backward.reachedAt });
    check("clock", "a_clock_jumping_forward_every_pass_delays_nothing_past_the_bound",
      forward.reachedAt !== null && forward.reachedAt <= forwardBound, { bound: forwardBound, reachedAt: forward.reachedAt });
  } finally {
    restore();
  }
}

/** One group lstat slower than a whole pass: each new worker still moves the cursor. */
async function slow(root: string) {
  const fixture = tree(path.join(root, "slow", ".grok"), 3, 4);
  const bound = 3 * 4 + 3;
  const restore = slowGroupStats(fixture.sessions, 60);
  try {
    const buffer = ledger(path.join(root, "slow.sqlite"));
    const run = await passes({ buffer, tree: fixture, limit: 2 * bound, replace: () => true });
    buffer.close();
    receipts.slow = { fixture: { groups: 3, sessionsPerGroup: 4, groupStatMs: 60 }, bound, reachedAt: run.reachedAt,
      positions: run.log.map((row) => row.position) };
    check("slow", "a_metadata_read_slower_than_a_pass_still_lets_every_new_worker_move_the_cursor",
      movesEveryPass(run.log), { positions: run.log.map((row) => row.position) });
    check("slow", "the_tree_is_finished_within_n_plus_g_passes",
      run.reachedAt !== null && run.reachedAt <= bound, { bound, reachedAt: run.reachedAt });
  } finally {
    restore();
  }
}

/**
 * Five new groups before every pass, each removed after it, with a new
 * worker every pass and 12 steps a pass; the old group holds 12
 * token-bearing sessions.
 */
async function churn(root: string) {
  const fixture = tree(path.join(root, "churn", ".grok"), 1, 12, true);
  const buffer = ledger(path.join(root, "churn.sqlite"));
  const limits: GrokUsageLimits = { ...recentLaneOff, entriesPerPass: 12 };
  let previous: string[] = [];
  let committedAt: number | null = null;
  let stateMaxBytes = 0;
  let stateOk = true;
  const pause = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
  const CHURN_PASSES = 16;
  try {
    for (let pass = 1; pass <= CHURN_PASSES; pass += 1) {
      for (const name of previous) fs.rmSync(path.join(fixture.sessions, name), { recursive: true, force: true });
      previous = Array.from({ length: 5 }, (_, index) => `churn-${String(pass).padStart(2, "0")}-${index}`);
      for (const name of previous) fs.mkdirSync(path.join(fixture.sessions, name), { mode: 0o700 });
      pause(2);
      const tailer = new GrokUsageTailer(buffer, fixture.home, limits);
      await tailer.scan({ budget: roomy() });
      tailer.close();
      const state = stateIsOneCursor(buffer, fixture.groups);
      stateMaxBytes = Math.max(stateMaxBytes, state.bytes);
      stateOk = stateOk && state.hashes && !state.leaks && !state.table;
      if (committedAt === null && distinctEventSessions(buffer) === 12) committedAt = pass;
    }
  } finally {
    for (const name of previous) fs.rmSync(path.join(fixture.sessions, name), { recursive: true, force: true });
  }
  receipts.churn = { oldSessions: 12, churnPerPass: 5, entriesPerPass: 12, churnPasses: CHURN_PASSES, committedAt,
    events: events(buffer), distinctSessions: distinctEventSessions(buffer), stateMaxBytes };
  check("churn", "churn_cannot_grow_the_walk_state", stateOk && stateMaxBytes <= 256, { stateMaxBytes });
  check("churn", "every_old_session_is_committed_once_while_the_churn_lasts",
    committedAt !== null && events(buffer) === 12, { committedAt, churnPasses: CHURN_PASSES, events: events(buffer) });
  buffer.close();
}

/**
 * A sweep finished by a later worker must not report itself clean when an
 * earlier worker of the same sweep left a queued usage file unread, or met a
 * group it could not read. The next sweep reads what was missed, once.
 */
async function honest(root: string) {
  const cases: Record<string, unknown> = {};
  for (const kind of ["unread", "unreadable"] as const) {
    const home = path.join(root, `honest-${kind}`, ".grok");
    const sessions = path.join(home, "sessions");
    const files: string[] = [];
    for (let index = 0; index < 4; index += 1) {
      const group = `private-group-${index % 2}`;
      const session = `honest-session-${index}`;
      fs.mkdirSync(path.join(sessions, group, session), { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(sessions, group, session, "usage.json"), JSON.stringify(grokUsage(session)), { mode: 0o600 });
      files.push(session);
    }
    // The group the walk meets first is the one the first worker cannot open.
    const flaky = path.join(sessions, names(sessions).sort(byHash)[0]!);
    const buffer = ledger(path.join(root, `honest-${kind}.sqlite`));
    try {
      // The first worker reads one file (its budget admits one record) and
      // closes with the rest queued, or meets a group it cannot open and
      // stops three steps in. Either way it does not finish the sweep.
      if (kind === "unreadable") fs.chmodSync(flaky, 0o000);
      const first = new GrokUsageTailer(buffer, home, kind === "unread" ? recentLaneOff : { ...recentLaneOff, entriesPerPass: 3 });
      await first.scan({
        budget: kind === "unread" ? new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxRecords: 1 }) : roomy(),
      });
      first.close();
      if (kind === "unreadable") fs.chmodSync(flaky, 0o700);
      const sweepsAfterFirstWorker = marker(buffer)?.sweeps ?? 0;
      // New workers finish that sweep, then run the next one.
      let sweeps = sweepsAfterFirstWorker;
      let firstSweepClean: boolean | null = null;
      for (let pass = 0; pass < 20 && (marker(buffer)?.sweeps ?? 0) < 2; pass += 1) {
        const worker = new GrokUsageTailer(buffer, home, recentLaneOff);
        await worker.scan({ budget: roomy() });
        worker.close();
        const current = marker(buffer);
        if ((current?.sweeps ?? 0) > sweeps && firstSweepClean === null) firstSweepClean = current?.lastSweep?.clean ?? null;
        sweeps = current?.sweeps ?? 0;
      }
      const secondSweepClean = marker(buffer)?.lastSweep?.clean ?? null;
      cases[kind] = { sweepsAfterFirstWorker, firstSweepClean, secondSweepClean, sweeps, events: events(buffer),
        files: files.length };
      check("honest", `a_sweep_another_worker_left_${kind === "unread" ? "a_queued_file_unread" : "a_group_unreadable"}_in_is_not_reported_clean`,
        sweepsAfterFirstWorker === 0 && firstSweepClean === false, { sweepsAfterFirstWorker, firstSweepClean });
      check("honest", `the_next_sweep_reads_what_was_missed_once_${kind}`,
        secondSweepClean === true && events(buffer) === files.length,
        { secondSweepClean, events: events(buffer), files: files.length });
    } finally {
      if (fs.existsSync(flaky)) fs.chmodSync(flaky, 0o700);
      buffer.close();
    }
  }
  receipts.honest = cases;
}

async function main() {
  installVirtualClock();
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "plimsoll-grok-replacement-proof-"));
  const run = (name: string) => scenario === "all" || scenario === name;
  try {
    const studio0Tree = run("studio0") || run("clock") ? tree(path.join(root, "studio0", ".grok"), 20, 200) : null;
    if (run("studio0")) await studio0(root, studio0Tree!);
    if (run("early-end")) await earlyEnd(root);
    if (run("read-cap")) await readCap(root);
    if (run("clock")) await clock(root, studio0Tree!);
    if (run("slow")) await slow(root);
    if (run("churn")) await churn(root);
    if (run("honest")) await honest(root);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  const failed = checks.filter((entry) => !entry.passed);
  const deterministicDigest = `sha256:${crypto.createHash("sha256")
    .update(JSON.stringify({ checks, receipts })).digest("hex")}`;
  console.log(JSON.stringify({
    schema: "eco-6hoxj.163.42.grok-worker-replacement-proof.v2",
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
