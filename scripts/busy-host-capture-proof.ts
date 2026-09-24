/**
 * Busy-host capture proof (eco-6hoxj.163.42 round 2).
 *
 * The round-1 review found three release blockers. Each scenario here is a
 * regression for one of them, built from the reviewer's probes:
 *
 *   progress   Admission is not progress. Repairs overrun the cadence by
 *              220 ms on every tick and every JSONL read stalls 120 ms, longer
 *              than any per-source slice; Codex, Claude and Grok must each
 *              still commit, within a bounded number of cadences.
 *   ceiling    The 200 ms aggregate wall is an admission ceiling: no capture
 *              or repair unit starts after it, and a tick ends within one
 *              bounded unit of it. Grok discovery of a 2,000-session group
 *              with a slow lstat stays inside the budget.
 *   lossless   Caps and recency never exclude a session for good: capped
 *              listings are visited in later windows and reported, a resumed
 *              walk reaches everything exactly once, and its cursor names no
 *              path.
 *
 * Fixtures are temporary; nothing reads an installed collector or a real
 * provider home. Timings are fault injection, not measurements of a host.
 *
 *   pnpm proof:busy-host-capture [-- --scenario=progress|ceiling|lossless]
 */
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { captureBaselineStatus } from "../packages/collector-cli/src/capture-baseline";
import { AUTOMATIC_CAPTURE_LIMITS, CaptureWorkBudget } from "../packages/collector-cli/src/capture-work-budget";
import { GROK_USAGE_LIMITS, GrokUsageTailer, ensureGrokUsageState } from "../packages/collector-cli/src/grok-usage-tailer";
import { DEFAULT_JSONL_TAILER_IO } from "../packages/collector-cli/src/jsonl-byte-tailer";
import { CollectorMaintenance } from "../packages/collector-cli/src/maintenance";
import { RolloutTailer } from "../packages/collector-cli/src/rollout-tailer";
import { TranscriptTailer } from "../packages/collector-cli/src/transcript-tailer";

const scenario = process.argv.find((arg) => arg.startsWith("--scenario="))?.split("=", 2)[1] ?? "all";
const checks: Array<{ scenario: string; name: string; passed: boolean; detail: unknown }> = [];
const receipts: Record<string, unknown> = {};

function check(group: string, name: string, passed: boolean, detail: unknown = null) {
  checks.push({ scenario: group, name, passed, detail });
}

const sleep = (ms: number) => Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
const uuid = (lead: string, index: number) =>
  `${lead.padEnd(8, "0").slice(0, 8)}-0000-4000-8000-${String(index).padStart(12, "0")}`;

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
function writeGrokSession(home: string, group: string, sessionId: string, at?: Date) {
  const directory = path.join(home, "sessions", group, sessionId);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, "usage.json");
  fs.writeFileSync(file, JSON.stringify(grokUsage(sessionId)), { mode: 0o600 });
  if (at) {
    fs.utimesSync(file, at, at);
    fs.utimesSync(directory, at, at);
    fs.utimesSync(path.dirname(directory), at, at);
  }
  return file;
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

function grokSweeps(buffer: LocalEventBuffer) {
  const marker = JSON.parse(state(buffer, "grok_usage_backfill_v1") ?? "null") as
    | { sweeps: number; lastSweep: { clean: boolean } | null } | null;
  return { sweeps: marker?.sweeps ?? 0, lastClean: marker?.lastSweep?.clean ?? null };
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

/**
 * Blockers 1 and 2 at the scheduler: the reviewer's all-source fixture, with
 * every unit start timed against the cadence it belongs to.
 */
async function busyHost(root: string) {
  const REPAIR_MS = 220;
  const READ_MS = 120;
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
  const mark = (unit: string) => unitStarts.push({ unit, offsetMs: performance.now() - tickStartedAt });
  const io = {
    ...DEFAULT_JSONL_TAILER_IO,
    readTail: (...args: Parameters<typeof DEFAULT_JSONL_TAILER_IO.readTail>) => {
      mark(args[0].startsWith(codexRoot) ? "codex_read" : "claude_read");
      sleep(READ_MS);
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
    sleep(REPAIR_MS);
    return realProjectionMaintenance(...args);
  };
  const originalOpen = fs.openSync;
  (fs as unknown as { openSync: typeof fs.openSync }).openSync = ((target: fs.PathLike, ...rest: unknown[]) => {
    if (String(target).startsWith(grokHome)) mark("grok_read");
    return (originalOpen as (...values: unknown[]) => number)(target, ...rest);
  }) as typeof fs.openSync;
  const maintenance = new CollectorMaintenance(buffer, rollout, transcript, undefined, grok);
  const observations: Array<Record<string, any>> = [];
  let liveWrittenAfterTick: number | null = null;
  try {
    for (let tick = 0; tick < TICKS; tick += 1) {
      // Pin the repair consumer to the front of every cadence: the parity
      // alternation alone must not be what serves capture.
      const repairRaw = state(buffer, "automatic_repair_service_v1");
      if (repairRaw) {
        const repair = JSON.parse(repairRaw) as Record<string, unknown>;
        repair.cycles = 0;
        buffer.database.prepare(
          "update maintenance_state set value = ?, updated_at = ? where key = 'automatic_repair_service_v1'",
        ).run(JSON.stringify(repair), new Date().toISOString());
      }
      const firstUnit = unitStarts.length;
      tickStartedAt = performance.now();
      const result = await maintenance.runRecent();
      const wallMs = performance.now() - tickStartedAt;
      const source = (scan: { recordsCommitted?: number; filesRead: number; deferredGenerations: number;
        activity: { scan?: { deferredBeforeIo?: boolean } | null } } | undefined) => ({
        admitted: scan?.activity.scan?.deferredBeforeIo === false,
        committed: scan?.recordsCommitted ?? 0,
        filesRead: scan?.filesRead ?? 0,
        deferredGenerations: scan?.deferredGenerations ?? 0,
      });
      observations.push({
        tick,
        wallMs: Number(wallMs.toFixed(3)),
        baseline: captureBaselineStatus(buffer.database).status,
        codex: source(result.rollout),
        claude: source(result.transcript),
        grok: source(result.grok),
        units: unitStarts.slice(firstUnit).map((unit) => ({ ...unit, offsetMs: Number(unit.offsetMs.toFixed(3)) })),
        budget: maintenance.status().budget,
        captureTurn: (result as { captureTurn?: unknown }).captureTurn ?? null,
      });
      if (liveWrittenAfterTick === null && captureBaselineStatus(buffer.database).status === "complete") {
        // Post-enrollment work arrives: twenty large generations per source,
        // far more than any cadence can commit.
        sleep(25);
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

  const maxWallMs = AUTOMATIC_CAPTURE_LIMITS.maxWallMs;
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
  // A unit's own bookkeeping (binding and path checks) runs between its
  // admission and its read; 10 ms bounds that. A unit admitted after the
  // aggregate wall starts much later: the repair alone overruns by 20 ms.
  const lateUnits = observations.flatMap((row) => row.units
    .filter((unit: { offsetMs: number }) => unit.offsetMs > maxWallMs + 10)
    .map((unit: { unit: string; offsetMs: number }) => ({ tick: row.tick, ...unit })));
  // One unit may cross the ceiling; 150 ms covers the fixed per-cadence
  // bookkeeping outside the capture budget on a loaded runner.
  const tickCeilingMs = maxWallMs + REPAIR_MS + 150;
  const walls = observations.map((row) => row.wallMs as number);
  receipts.progress = {
    fixture: { grokSessions: 4_000, codexLiveFiles: 20, claudeLiveFiles: 20, repairDelayMs: REPAIR_MS,
      jsonlReadDelayMs: READ_MS, ticks: TICKS, repairFirstEveryTick: true, liveWrittenAfterTick },
    committedAfterLiveWork: committed,
    longestConsecutiveCadencesWithoutCommit: waits,
    maxWallMs: Math.max(...walls),
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
  check("progress", "no_busy_source_waits_more_than_four_cadences_for_a_commit",
    Object.values(waits).every((wait) => wait <= 4), waits);
  check("progress", "aggregate_byte_record_and_event_ceilings_hold_every_tick",
    observations.every((row) => row.budget.bytesRead <= row.budget.maxBytes &&
      row.budget.recordsParsed <= row.budget.maxRecords && row.budget.eventsAppended <= row.budget.maxEvents));
  check("ceiling", "no_capture_or_repair_unit_starts_after_the_aggregate_wall",
    lateUnits.length === 0, { lateUnits: lateUnits.slice(0, 8), maxWallMs });
  check("ceiling", "every_tick_ends_within_one_bounded_unit_of_the_ceiling",
    walls.every((wall) => wall <= tickCeilingMs), { walls, tickCeilingMs });
}

/** Blocker 2 inside Grok: discovery of one large group with a slow lstat. */
async function grokDiscoveryBound(root: string) {
  const SESSIONS = 2_000;
  const home = path.join(root, "bound", ".grok");
  const group = path.join(home, "sessions", "group");
  const old = new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000);
  for (let index = 0; index < SESSIONS; index += 1) writeGrokSession(home, "group", uuid("ab", index), old);
  fs.utimesSync(group, old, old);
  const buffer = new LocalEventBuffer(path.join(root, "bound.sqlite"));
  const tailer = new GrokUsageTailer(buffer, home);
  const originalLstat = fs.lstatSync;
  let delayedStats = 0;
  const slowScan = async () => {
    (fs as unknown as { lstatSync: typeof fs.lstatSync }).lstatSync = ((target: fs.PathLike, ...rest: unknown[]) => {
      if (String(target).startsWith(group + path.sep)) {
        delayedStats += 1;
        sleep(1);
      }
      return (originalLstat as (...values: unknown[]) => fs.Stats)(target, ...rest);
    }) as typeof fs.lstatSync;
    delayedStats = 0;
    const started = performance.now();
    try {
      const result = await tailer.scan({ budget: new CaptureWorkBudget() });
      return { result, wallMs: performance.now() - started, delayedStats };
    } finally {
      (fs as unknown as { lstatSync: typeof fs.lstatSync }).lstatSync = originalLstat;
    }
  };
  const firstSlow = await slowScan();
  const first = firstSlow.result;
  const firstWallMs = firstSlow.wallMs;
  const firstStats = firstSlow.delayedStats;
  let scans = 1;
  for (; scans < 400 && grokEvents(buffer) < SESSIONS; scans += 1) {
    await tailer.scan({ budget: new CaptureWorkBudget() });
  }
  const events = grokEvents(buffer);
  // Steady state: every file is committed and unchanged, so nothing queues
  // and only the budget ends a pass over 2,000 slow lstats. Time every scan
  // of one whole sweep: after the first lists the group, each later scan
  // could otherwise stat the rest of it in one go.
  const sweepsBefore = grokSweeps(buffer).sweeps;
  const steadyScans: Array<{ wallMs: number; delayedStats: number; unchanged: number }> = [];
  for (let scan = 0; scan < 200 && grokSweeps(buffer).sweeps === sweepsBefore; scan += 1) {
    const slow = await slowScan();
    steadyScans.push({ wallMs: Number(slow.wallMs.toFixed(3)), delayedStats: slow.delayedStats,
      unchanged: slow.result.filesUnchanged });
  }
  const steadyUnchanged = steadyScans.reduce((total, row) => total + row.unchanged, 0);
  tailer.close();
  buffer.close();
  const ceilingMs = AUTOMATIC_CAPTURE_LIMITS.maxWallMs + 60;
  receipts.discovery = {
    fixture: { sessions: SESSIONS, injectedLstatDelayMs: 1 },
    first: { wallMs: Number(firstWallMs.toFixed(3)), delayedStats: firstStats, seen: first.filesSeen,
      committed: first.recordsCommitted, entries: first.activity.discoveryEntries,
      deferredBeforeIo: first.activity.scan.deferredBeforeIo },
    scansToCommitAll: scans,
    events,
    ceilingMs,
    steady: { scans: steadyScans.length, unchanged: steadyUnchanged,
      maxWallMs: Math.max(...steadyScans.map((row) => row.wallMs)),
      maxDelayedStats: Math.max(...steadyScans.map((row) => row.delayedStats)) },
  };
  check("ceiling", "grok_discovery_of_a_large_group_stays_inside_the_budget",
    firstWallMs <= ceilingMs, { wallMs: Number(firstWallMs.toFixed(3)), ceilingMs });
  check("ceiling", "grok_discovery_never_stats_a_whole_directory_in_one_scan",
    firstStats < SESSIONS / 2, { delayedStats: firstStats, sessions: SESSIONS });
  check("ceiling", "the_first_bounded_grok_scan_makes_progress",
    first.activity.scan.deferredBeforeIo === false && first.filesSeen > 0 && first.recordsCommitted > 0,
    { seen: first.filesSeen, committed: first.recordsCommitted });
  check("ceiling", "bounded_grok_scans_resume_to_every_session_exactly_once",
    events === SESSIONS, { events, scans });
  check("ceiling", "every_scan_of_an_unchanged_steady_state_sweep_stays_inside_the_budget",
    steadyScans.length > 1 && steadyUnchanged === SESSIONS &&
      steadyScans.every((row) => row.wallMs <= ceilingMs && row.delayedStats < SESSIONS / 2),
    { scans: steadyScans.length, unchanged: steadyUnchanged, ceilingMs,
      slowest: [...steadyScans].sort((left, right) => right.wallMs - left.wallMs).slice(0, 3) });
}

/** Scan with a generous wall so a scenario's only limits are the ones it sets. */
function roomyBudget() {
  return new CaptureWorkBudget({ ...AUTOMATIC_CAPTURE_LIMITS, maxWallMs: 2_000 });
}

/** Blocker 3: capped listings, recency, restarts and the cursor. */
async function lossless(root: string) {
  const old = (offsetMs: number) => new Date(Date.now() - 30 * 24 * 60 * 60 * 1_000 + offsetMs);

  // The reviewer's capped case: two slots, three stable sessions.
  const cappedHome = path.join(root, "capped", ".grok");
  const cappedBuffer = new LocalEventBuffer(path.join(root, "capped.sqlite"));
  const sessions = [uuid("d0", 1), uuid("e0", 2), uuid("f0", 3)];
  // Grok names a group after the URL-encoded working directory.
  const cappedGroup = encodeURIComponent("/Users/example/capped-project");
  sessions.forEach((sessionId, index) => writeGrokSession(cappedHome, cappedGroup, sessionId, old(3_000 - index * 1_000)));
  const cappedTailer = new GrokUsageTailer(cappedBuffer, cappedHome,
    { ...GROK_USAGE_LIMITS, maxSessionsPerGroup: 2, entriesPerPass: 100, discoveryWallMs: 1_000 });
  let sessionsOverLimit = 0;
  const cursors: string[] = [];
  const cappedSweeps: Array<{ scan: number; sweeps: number; seen: boolean[] }> = [];
  for (let scan = 0; scan < 8 && grokSweeps(cappedBuffer).sweeps < 2; scan += 1) {
    const result = await cappedTailer.scan({ budget: roomyBudget() });
    sessionsOverLimit += Number((result.activity.scan.usageFiles as { sessionsOverLimit?: number }).sessionsOverLimit ?? 0);
    const cursor = state(cappedBuffer, "grok_usage_sweep_resume_v1");
    if (cursor) cursors.push(cursor);
    cappedSweeps.push({ scan, sweeps: grokSweeps(cappedBuffer).sweeps,
      seen: sessions.map((sessionId) => grokTurnSeen(cappedBuffer, sessionId)) });
  }
  const cappedSeen = sessions.map((sessionId) => grokTurnSeen(cappedBuffer, sessionId));
  const cappedEvents = grokEvents(cappedBuffer);
  cappedTailer.close();
  cappedBuffer.close();
  check("lossless", "a_capped_group_reaches_its_oldest_session_within_two_sweeps",
    cappedSeen.every(Boolean) && cappedEvents === sessions.length, { cappedSweeps, events: cappedEvents });
  check("lossless", "a_capped_group_is_reported_not_silently_skipped",
    sessionsOverLimit > 0, { sessionsOverLimit });

  // Capped groups: three groups, room for two.
  const groupsHome = path.join(root, "groups", ".grok");
  const groupsBuffer = new LocalEventBuffer(path.join(root, "groups.sqlite"));
  const groupSessions = ["alpha", "beta", "gamma"].map((name, index) => {
    const sessionId = uuid("c1", index);
    writeGrokSession(groupsHome, `%2Ftmp%2F${name}`, sessionId, old(3_000 - index * 1_000));
    return sessionId;
  });
  const groupsTailer = new GrokUsageTailer(groupsBuffer, groupsHome,
    { ...GROK_USAGE_LIMITS, maxGroups: 2, entriesPerPass: 100, discoveryWallMs: 1_000 });
  let groupsOverLimit = 0;
  for (let scan = 0; scan < 8 && grokSweeps(groupsBuffer).sweeps < 2; scan += 1) {
    const result = await groupsTailer.scan({ budget: roomyBudget() });
    groupsOverLimit += Number((result.activity.scan.usageFiles as { groupsOverLimit?: number }).groupsOverLimit ?? 0);
  }
  const groupsSeen = groupSessions.map((sessionId) => grokTurnSeen(groupsBuffer, sessionId));
  const groupsEvents = grokEvents(groupsBuffer);
  groupsTailer.close();
  groupsBuffer.close();
  check("lossless", "capped_groups_are_all_reached_within_two_sweeps",
    groupsSeen.every(Boolean) && groupsEvents === groupSessions.length, { groupsSeen, events: groupsEvents });
  check("lossless", "capped_groups_are_reported_not_silently_skipped", groupsOverLimit > 0, { groupsOverLimit });

  // Capped windows over empty and unreadable groups: the cursor must move
  // past every group it finishes, or a window ending on one repeats forever.
  const sparseHome = path.join(root, "sparse", ".grok");
  const sparseBuffer = new LocalEventBuffer(path.join(root, "sparse.sqlite"));
  ensureGrokUsageState(sparseBuffer.database);
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
  check("lossless", "capped_windows_move_past_empty_and_unreadable_groups",
    sparseSeen === sparseSessions.length && sparseEvents === sparseSessions.length,
    { seen: sparseSeen, sessions: sparseSessions.length, events: sparseEvents, scans: sparseScans });

  // Recent first: today's sessions in a recently active group come before
  // 1,200 older sessions, in the first bounded scan.
  const recentHome = path.join(root, "recent", ".grok");
  const recentBuffer = new LocalEventBuffer(path.join(root, "recent.sqlite"));
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

  // Restarts: a tiny lifetime limit ends every sweep early, and every scan
  // runs in a fresh tailer, as after a worker restart. Legacy cursors (the
  // 0.7.37 numeric origin and an unshipped name cursor) must not strand it.
  const restartHome = path.join(root, "restart", ".grok");
  const restartBuffer = new LocalEventBuffer(path.join(root, "restart.sqlite"));
  ensureGrokUsageState(restartBuffer.database);
  const restartSessions: string[] = [];
  for (let group = 0; group < 4; group += 1) {
    for (let session = 0; session < 5; session += 1) {
      const sessionId = uuid("4c", group * 5 + session);
      writeGrokSession(restartHome, `grp-${group}`, sessionId, old(group * 5 + session));
      restartSessions.push(sessionId);
    }
  }
  restartBuffer.database.prepare(
    `insert into maintenance_state(key, value, updated_at) values ('grok_usage_sweep_resume_v1', '1', ?)
     on conflict(key) do update set value = excluded.value`,
  ).run(new Date().toISOString());
  const restartLimits = { ...GROK_USAGE_LIMITS, lifetimeEntryLimit: 3, entriesPerPass: 100, discoveryWallMs: 1_000 };
  const restartCursors: string[] = [];
  let restartScans = 0;
  for (; restartScans < 60 && restartSessions.some((sessionId) => !grokTurnSeen(restartBuffer, sessionId)); restartScans += 1) {
    if (restartScans === 3) {
      restartBuffer.database.prepare(
        "update maintenance_state set value = ? where key = 'grok_usage_sweep_resume_v1'",
      ).run(JSON.stringify({ version: 1, groupName: "grp-2", sessionName: restartSessions[11] }));
    }
    const tailer = new GrokUsageTailer(restartBuffer, restartHome, restartLimits);
    await tailer.scan({ budget: roomyBudget() });
    tailer.close();
    const cursor = state(restartBuffer, "grok_usage_sweep_resume_v1");
    if (cursor && restartScans !== 3) restartCursors.push(cursor);
  }
  const restartSeen = restartSessions.filter((sessionId) => grokTurnSeen(restartBuffer, sessionId)).length;
  const restartEvents = grokEvents(restartBuffer);
  restartBuffer.close();
  // The same restarts, with a group window smaller than the tree as well.
  const windowBuffer = new LocalEventBuffer(path.join(root, "restart-window.sqlite"));
  ensureGrokUsageState(windowBuffer.database);
  let windowScans = 0;
  for (; windowScans < 80 && restartSessions.some((sessionId) => !grokTurnSeen(windowBuffer, sessionId)); windowScans += 1) {
    const tailer = new GrokUsageTailer(windowBuffer, restartHome, { ...restartLimits, maxGroups: 2 });
    await tailer.scan({ budget: roomyBudget() });
    tailer.close();
  }
  const windowSeen = restartSessions.filter((sessionId) => grokTurnSeen(windowBuffer, sessionId)).length;
  const windowEvents = grokEvents(windowBuffer);
  windowBuffer.close();
  check("lossless", "a_capped_window_cut_short_and_restarted_every_scan_reaches_every_session_once",
    windowSeen === restartSessions.length && windowEvents === restartSessions.length,
    { seen: windowSeen, sessions: restartSessions.length, events: windowEvents, scans: windowScans });
  check("lossless", "a_walk_cut_short_and_restarted_every_scan_reaches_every_session_once",
    restartSeen === restartSessions.length && restartEvents === restartSessions.length,
    { seen: restartSeen, sessions: restartSessions.length, events: restartEvents, scans: restartScans });
  // The ledger never holds the working directory a group name encodes. A
  // small pass allowance leaves a sweep mid-walk, so its cursor persists.
  const privacyHome = path.join(root, "privacy", ".grok");
  const privacyBuffer = new LocalEventBuffer(path.join(root, "privacy.sqlite"));
  const privacyGroups = ["alpha", "beta", "gamma"].map((name) => encodeURIComponent(`/Users/example/private-${name}`));
  const privacySessions: string[] = [];
  privacyGroups.forEach((group, groupIndex) => {
    for (let session = 0; session < 4; session += 1) {
      const sessionId = uuid("2e", groupIndex * 4 + session);
      writeGrokSession(privacyHome, group, sessionId, old(groupIndex * 4 + session));
      privacySessions.push(sessionId);
    }
  });
  const privacyTailer = new GrokUsageTailer(privacyBuffer, privacyHome,
    { ...GROK_USAGE_LIMITS, entriesPerPass: 6, discoveryWallMs: 1_000 });
  const privacyCursors: string[] = [];
  for (let scan = 0; scan < 12 && grokSweeps(privacyBuffer).sweeps === 0; scan += 1) {
    await privacyTailer.scan({ budget: roomyBudget() });
    const cursor = state(privacyBuffer, "grok_usage_sweep_resume_v1");
    if (cursor && cursor !== "0") privacyCursors.push(cursor);
  }
  privacyTailer.close();
  privacyBuffer.close();
  const resumable = [...privacyCursors, ...cursors, ...restartCursors].filter((cursor) => cursor !== "0");
  check("lossless", "the_resume_cursor_names_no_group_or_session",
    privacyCursors.length > 0 && resumable.every((cursor) => !cursor.includes("private-") &&
      !cursor.includes("capped-project") && !cursor.includes("grp-") &&
      [...privacySessions, ...sessions, ...restartSessions].every((sessionId) => !cursor.includes(sessionId))),
    { cursors: resumable.slice(0, 4) });
  receipts.lossless = { cappedSweeps, sessionsOverLimit, groupsOverLimit, cursors, recentSeen,
    restart: { scans: restartScans, seen: restartSeen, events: restartEvents, cursors: restartCursors } };
}

async function main() {
  const root = fs.mkdtempSync(path.join(fs.realpathSync(os.tmpdir()), "plimsoll-busy-host-proof-"));
  try {
    if (scenario === "all" || scenario === "progress" || scenario === "ceiling") await busyHost(path.join(root, "busy"));
    if (scenario === "all" || scenario === "ceiling") await grokDiscoveryBound(root);
    if (scenario === "all" || scenario === "lossless") await lossless(root);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
  const failed = checks.filter((entry) => !entry.passed);
  console.log(JSON.stringify({
    schema: "eco-6hoxj.163.42.busy-host-capture-proof.v2",
    scenario,
    ok: failed.length === 0 && checks.length > 0,
    counts: { total: checks.length, passed: checks.length - failed.length, failed: failed.length },
    failed: failed.map((entry) => `${entry.scenario}:${entry.name}`),
    checks,
    receipts,
  }, null, 2));
  if (failed.length > 0 || checks.length === 0) process.exitCode = 1;
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
