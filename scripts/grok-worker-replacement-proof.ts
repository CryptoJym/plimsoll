/**
 * Grok worker replacement proof (eco-6hoxj.163.42, round 4).
 *
 * The round-3 review replaced the Grok worker after every pass on a
 * Studio0-scale tree with slow group-directory metadata: the recent lane was
 * rebuilt by every new worker and took each pass's whole discovery allowance,
 * so the durable walk never started and an old session could wait forever.
 * Each check below is built from that construction:
 *
 *   studio0     4,000 sessions in 20 groups, 12 ms per group lstat, the
 *               production limits and cadence budget, a new worker every
 *               pass. The last session of the last group in directory order
 *               is reached within the stated bound, every pass covers more,
 *               and the durable walk state stays within one group's
 *               sessions plus one row per finished group.
 *   slow        One group lstat slower than a whole pass (60 ms against the
 *               50 ms allowance): a new worker every pass still covers at
 *               least one entry, so the tree is finished within N + G passes.
 *   state       Churn that adds and removes groups on every pass keeps the
 *               durable walk state within its row limit plus one pass; the
 *               sweep that reaches the limit ends early, reported
 *               incomplete, and once the churn stops a clean sweep reaches
 *               the old session.
 *   honest      A sweep finished by a later worker is not reported clean
 *               when an earlier worker of it left a queued file unread or
 *               met an unreadable group; the next sweep reads what was
 *               missed, once.
 *
 * Time is virtual (scripts/lib/virtual-clock.ts): only the slow lstats the
 * fixture declares cost time, so every count is the same on any host.
 *
 *   pnpm proof:grok-worker-replacement [-- --scenario=studio0|slow|state|honest]
 */
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { GROK_USAGE_LIMITS, GrokUsageTailer, ensureGrokUsageState } from "../packages/collector-cli/src/grok-usage-tailer";
import { GROK_USAGE_BACKFILL_KEY, type GrokUsageBackfillMarker } from "../packages/collector-cli/src/history-coverage";
import { installVirtualClock, spend } from "./lib/virtual-clock";

const scenario = process.argv.find((arg) => arg.startsWith("--scenario="))?.split("=", 2)[1] ?? "all";
const checks: Array<{ scenario: string; name: string; passed: boolean; detail: unknown }> = [];
const receipts: Record<string, unknown> = {};

function check(group: string, name: string, passed: boolean, detail: unknown = null) {
  checks.push({ scenario: group, name, passed, detail });
}

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
 * Groups of empty sessions, all modified long ago; the last session of the
 * last group in directory order holds the one usage file. A walk reaches it
 * only after everything else.
 */
function tree(home: string, groups: number, sessionsPerGroup: number) {
  const sessions = path.join(home, "sessions");
  for (let group = 0; group < groups; group += 1) {
    const name = `private-project-${String(group).padStart(2, "0")}`;
    for (let session = 0; session < sessionsPerGroup; session += 1) {
      fs.mkdirSync(path.join(sessions, name, `session-${String(group).padStart(2, "0")}-${String(session).padStart(3, "0")}`),
        { recursive: true, mode: 0o700 });
    }
  }
  const lastGroup = streamOrder(sessions).at(-1)!;
  const target = streamOrder(path.join(sessions, lastGroup)).at(-1)!;
  fs.writeFileSync(path.join(sessions, lastGroup, target, "usage.json"), JSON.stringify(grokUsage(target)), { mode: 0o600 });
  const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000);
  for (const group of streamOrder(sessions)) fs.utimesSync(path.join(sessions, group), old, old);
  return { sessions, target };
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

function marker(buffer: LocalEventBuffer) {
  const row = buffer.database.prepare("select value from maintenance_state where key = ?")
    .get(GROK_USAGE_BACKFILL_KEY) as { value: string } | undefined;
  return row ? JSON.parse(row.value) as GrokUsageBackfillMarker : null;
}

function walkRound(buffer: LocalEventBuffer) {
  const row = buffer.database.prepare("select value from maintenance_state where key = 'grok_usage_walk_round_v1'")
    .get() as { value: string } | undefined;
  return row ? JSON.parse(row.value) as { round: number; recentAtMs?: number | null } : null;
}

/** Rows of the durable walk state: finished-group rows and covered-session rows. */
function walkRows(buffer: LocalEventBuffer) {
  const row = buffer.database.prepare(
    `select coalesce(sum(case when session_hash = '' then 1 else 0 end), 0) as groups,
       coalesce(sum(case when session_hash <> '' then 1 else 0 end), 0) as sessions
     from grok_usage_walk_visits`,
  ).get() as { groups: number; sessions: number };
  return { groups: Number(row.groups), sessions: Number(row.sessions) };
}

/**
 * Entries the sweep has covered so far: a finished group counts itself and
 * its sessions, a group still being walked its covered sessions. A finished
 * sweep covered them all.
 */
function coveredEntries(buffer: LocalEventBuffer, round: number, sessionsPerGroup: number, total: number) {
  if ((walkRound(buffer)?.round ?? round) > round) return total;
  const rows = walkRows(buffer);
  return rows.groups * (sessionsPerGroup + 1) + rows.sessions;
}

async function passes(options: {
  buffer: LocalEventBuffer;
  home: string;
  target: string;
  limit: number;
  persistent: boolean;
  sessionsPerGroup: number;
  total: number;
  limits?: typeof GROK_USAGE_LIMITS;
}) {
  const log: Array<{ pass: number; steps: number; covered: number; rows: number; recentAtMs: number | null }> = [];
  let tailer = options.persistent ? new GrokUsageTailer(options.buffer, options.home, options.limits) : null;
  let reachedAt: number | null = null;
  try {
    for (let pass = 1; pass <= options.limit && reachedAt === null; pass += 1) {
      const round = walkRound(options.buffer)?.round ?? 1;
      const worker = tailer ?? new GrokUsageTailer(options.buffer, options.home, options.limits);
      // The production cadence budget: 200 ms of admission, of which a
      // discovery pass takes at most its 50 ms allowance.
      const scan = await worker.scan({ budget: new CaptureWorkBudget(AUTOMATIC_CAPTURE_LIMITS) });
      if (!tailer) worker.close();
      const rows = walkRows(options.buffer);
      log.push({
        pass,
        steps: scan.activity.discoveryEntries,
        covered: coveredEntries(options.buffer, round, options.sessionsPerGroup, options.total),
        rows: rows.groups + rows.sessions,
        recentAtMs: walkRound(options.buffer)?.recentAtMs ?? null,
      });
      if (seen(options.buffer, options.target)) reachedAt = pass;
    }
  } finally {
    tailer?.close();
    tailer = null;
  }
  return { reachedAt, log };
}

/** The review's construction at Studio0's scale, with a new worker every pass. */
async function studio0(root: string) {
  const GROUPS = 20;
  const SESSIONS_PER_GROUP = 200;
  const GROUP_STAT_MS = 12;
  const home = path.join(root, "studio0", ".grok");
  const { sessions, target } = tree(home, GROUPS, SESSIONS_PER_GROUP);
  const total = GROUPS * (SESSIONS_PER_GROUP + 1);
  // The first pass of a sweep finishes at least one group beside the recent
  // lane's half of the allowance; every later pass of a new worker finishes
  // at least floor(50 / 12) = 4, because the recent lane is not started
  // again within recentRefreshMs of its last start.
  const perPass = Math.floor(GROK_USAGE_LIMITS.discoveryWallMs / GROUP_STAT_MS);
  const bound = 1 + Math.ceil((GROUPS - 1) / perPass);
  const restore = slowGroupStats(sessions, GROUP_STAT_MS);
  try {
    const replacedBuffer = ledger(path.join(root, "studio0-replaced.sqlite"));
    const replaced = await passes({ buffer: replacedBuffer, home, target, limit: 40, persistent: false,
      sessionsPerGroup: SESSIONS_PER_GROUP, total });
    replacedBuffer.close();
    const persistentBuffer = ledger(path.join(root, "studio0-persistent.sqlite"));
    const persistent = await passes({ buffer: persistentBuffer, home, target, limit: 40, persistent: true,
      sessionsPerGroup: SESSIONS_PER_GROUP, total });
    persistentBuffer.close();
    const moved = replaced.log.every((row, index) => row.covered > (index === 0 ? 0 : replaced.log[index - 1]!.covered));
    const maxRows = Math.max(0, ...replaced.log.map((row) => row.rows));
    const recentStarts = new Set(replaced.log.filter((row) => row.covered < total).map((row) => row.recentAtMs));
    receipts.studio0 = {
      fixture: { groups: GROUPS, sessionsPerGroup: SESSIONS_PER_GROUP, groupStatMs: GROUP_STAT_MS,
        discoveryWallMs: GROK_USAGE_LIMITS.discoveryWallMs, cadenceAdmissionMs: AUTOMATIC_CAPTURE_LIMITS.maxWallMs },
      bound,
      replaced: { reachedAt: replaced.reachedAt, log: replaced.log.map(({ recentAtMs, ...row }) =>
        ({ ...row, recentLaneStarted: recentAtMs !== null })) },
      persistent: { reachedAt: persistent.reachedAt },
      maxRows,
    };
    check("studio0", "a_worker_replaced_every_pass_reaches_the_last_session_within_the_stated_bound",
      replaced.reachedAt !== null && replaced.reachedAt <= bound,
      { bound, reachedAt: replaced.reachedAt });
    check("studio0", "the_first_pass_finishes_a_group_beside_the_recent_lane",
      (replaced.log[0]?.covered ?? 0) >= SESSIONS_PER_GROUP + 1,
      { firstPassCovered: replaced.log[0]?.covered ?? 0, group: SESSIONS_PER_GROUP + 1 });
    check("studio0", "every_pass_of_a_replaced_worker_covers_more_of_the_tree",
      moved && replaced.log.length > 0, { covered: replaced.log.map((row) => row.covered) });
    check("studio0", "a_replaced_worker_does_not_start_the_recent_lane_again_before_it_is_due",
      recentStarts.size === 1 && !recentStarts.has(null), { recentLaneStarts: recentStarts.size });
    check("studio0", "a_persistent_worker_reaches_it_too",
      persistent.reachedAt !== null && persistent.reachedAt <= 2 * bound,
      { bound: 2 * bound, reachedAt: persistent.reachedAt });
    check("studio0", "the_walk_state_holds_one_group_of_sessions_plus_a_row_per_finished_group",
      maxRows <= GROUPS + SESSIONS_PER_GROUP,
      { maxRows, limit: GROUPS + SESSIONS_PER_GROUP, sessions: GROUPS * SESSIONS_PER_GROUP });
  } finally {
    restore();
  }
}

/** One group lstat slower than a whole pass: each new worker still covers an entry. */
async function slow(root: string) {
  const GROUPS = 3;
  const SESSIONS_PER_GROUP = 4;
  const GROUP_STAT_MS = 60;
  const home = path.join(root, "slow", ".grok");
  const { sessions, target } = tree(home, GROUPS, SESSIONS_PER_GROUP);
  const total = GROUPS * (SESSIONS_PER_GROUP + 1);
  const bound = total;
  const restore = slowGroupStats(sessions, GROUP_STAT_MS);
  try {
    const buffer = ledger(path.join(root, "slow.sqlite"));
    const replaced = await passes({ buffer, home, target, limit: 2 * bound, persistent: false,
      sessionsPerGroup: SESSIONS_PER_GROUP, total });
    buffer.close();
    const progress = replaced.log.map((row) => row.covered);
    const moved = replaced.log.every((row, index) => row.covered > (index === 0 ? 0 : replaced.log[index - 1]!.covered));
    receipts.slow = { fixture: { groups: GROUPS, sessionsPerGroup: SESSIONS_PER_GROUP, groupStatMs: GROUP_STAT_MS },
      bound, reachedAt: replaced.reachedAt, progress };
    check("slow", "a_metadata_read_slower_than_a_pass_still_lets_every_new_worker_cover_an_entry",
      moved && replaced.log.length > 0, { progress });
    check("slow", "the_tree_is_finished_within_n_plus_g_passes",
      replaced.reachedAt !== null && replaced.reachedAt <= bound, { bound, reachedAt: replaced.reachedAt });
  } finally {
    restore();
  }
}

/**
 * Churn that grows the walk state: five new groups before every pass, each
 * removed after it, so a sweep cannot finish while the churn lasts. The
 * state stays bounded, the sweep that reaches the limit ends early and is
 * reported, and once the churn stops a clean sweep commits every old
 * session's usage exactly once.
 */
async function state(root: string) {
  const home = path.join(root, "state", ".grok");
  const { sessions } = tree(home, 1, 12);
  const oldGroup = streamOrder(sessions)[0]!;
  const oldSessions = streamOrder(path.join(sessions, oldGroup));
  for (const session of oldSessions) {
    fs.writeFileSync(path.join(sessions, oldGroup, session, "usage.json"), JSON.stringify(grokUsage(session)), { mode: 0o600 });
  }
  fs.utimesSync(path.join(sessions, oldGroup), new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000),
    new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000));
  const limits = {
    ...GROK_USAGE_LIMITS, entriesPerPass: 6, discoveryWallMs: 1_000, recentGroups: 0, recentSessions: 0,
    maxWalkStateRows: 16,
  };
  const buffer = ledger(path.join(root, "state.sqlite"));
  const churnRows: number[] = [];
  let earlyEnds = 0;
  let sweepsBefore = 0;
  let previous: string[] = [];
  const pause = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
  const roomy = () => new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 1_000_000 });
  try {
    for (let pass = 0; pass < 16; pass += 1) {
      for (const name of previous) fs.rmSync(path.join(sessions, name), { recursive: true, force: true });
      previous = Array.from({ length: 5 }, (_, index) => `churn-${String(pass).padStart(2, "0")}-${index}`);
      for (const name of previous) fs.mkdirSync(path.join(sessions, name), { mode: 0o700 });
      pause(2);
      const tailer = new GrokUsageTailer(buffer, home, limits);
      await tailer.scan({ budget: roomy() });
      tailer.close();
      const rows = walkRows(buffer);
      churnRows.push(rows.groups + rows.sessions);
      const current = marker(buffer);
      if ((current?.sweeps ?? 0) > sweepsBefore && current?.lastSweep?.walkStateLimitReached === 1 &&
        current.lastSweep.clean === false) earlyEnds += 1;
      sweepsBefore = current?.sweeps ?? 0;
    }
    for (const name of previous) fs.rmSync(path.join(sessions, name), { recursive: true, force: true });
    let quietPasses = 0;
    let cleanSweep = false;
    while (quietPasses < 12 && !cleanSweep) {
      const tailer = new GrokUsageTailer(buffer, home, limits);
      await tailer.scan({ budget: roomy() });
      tailer.close();
      quietPasses += 1;
      cleanSweep = marker(buffer)?.lastSweep?.clean === true;
    }
    const committed = oldSessions.filter((session) => seen(buffer, session)).length;
    const maxRows = Math.max(0, ...churnRows);
    receipts.state = { limit: limits.maxWalkStateRows, entriesPerPass: limits.entriesPerPass, churnRows, earlyEnds,
      quietPasses, cleanSweep, committed, events: events(buffer), sessions: oldSessions.length };
    check("state", "churn_keeps_the_walk_state_within_its_limit_plus_one_pass",
      maxRows <= limits.maxWalkStateRows + limits.entriesPerPass,
      { maxRows, bound: limits.maxWalkStateRows + limits.entriesPerPass });
    check("state", "a_sweep_that_reaches_the_limit_ends_early_and_is_reported_incomplete", earlyEnds > 0, { earlyEnds });
    check("state", "once_churn_stops_a_clean_sweep_commits_every_old_session_once",
      cleanSweep && committed === oldSessions.length && events(buffer) === oldSessions.length,
      { quietPasses, committed, events: events(buffer), sessions: oldSessions.length });
  } finally {
    buffer.close();
  }
}

function events(buffer: LocalEventBuffer) {
  return Number((buffer.database.prepare(
    "select count(*) as count from buffered_events where source = 'grok' and input_tokens is not null",
  ).get() as { count: number }).count);
}

/**
 * A sweep finished by a later worker must not report itself clean when an
 * earlier worker of the same sweep left a queued usage file unread, or saw a
 * group it could not read. The next sweep reads what was missed, once.
 */
async function honest(root: string) {
  const roomy = () => new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 1_000_000 });
  const limits = { ...GROK_USAGE_LIMITS, recentGroups: 0, recentSessions: 0 };
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
    // The group the first worker meets first is the one it cannot open.
    const flaky = path.join(sessions, streamOrder(sessions)[0]!);
    const buffer = ledger(path.join(root, `honest-${kind}.sqlite`));
    try {
      // The first worker reads one file (its budget admits one record) and
      // closes with the rest queued, or meets a group it cannot open and
      // stops three steps in. Either way it does not finish the sweep.
      if (kind === "unreadable") fs.chmodSync(flaky, 0o000);
      const first = new GrokUsageTailer(buffer, home, kind === "unread" ? limits : { ...limits, entriesPerPass: 3 });
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
        const worker = new GrokUsageTailer(buffer, home, limits);
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
  try {
    if (scenario === "all" || scenario === "studio0") await studio0(root);
    if (scenario === "all" || scenario === "slow") await slow(root);
    if (scenario === "all" || scenario === "state") await state(root);
    if (scenario === "all" || scenario === "honest") await honest(root);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  const failed = checks.filter((entry) => !entry.passed);
  const deterministicDigest = `sha256:${crypto.createHash("sha256")
    .update(JSON.stringify({ checks, receipts })).digest("hex")}`;
  console.log(JSON.stringify({
    schema: "eco-6hoxj.163.42.grok-worker-replacement-proof.v1",
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
