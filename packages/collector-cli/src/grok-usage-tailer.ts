import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";

import type Database from "better-sqlite3";

import {
  aiInteractionEventSchema,
  type AiInteractionEvent,
} from "../../shared/src/index";
import type { LocalEventBuffer } from "./buffer";
import { captureScanProgress, type CaptureScanProgress } from "./capture-baseline";
import {
  GROK_USAGE_BACKFILL_KEY,
  grokUsageBackfillMarker,
  type GrokUsageBackfillMarker,
  type GrokUsageSweepCounters,
} from "./history-coverage";
import type { CaptureBudgetStatus, CaptureWorkBudget } from "./capture-work-budget";
import { CAPTURE_COVERAGE_MAX_ENTRIES, CaptureCoverageWalk, linkCoverageFile } from "./capture-frontier";
import { clampFutureObservedAt, deterministicEventId } from "./normalizer";
import { attachRepoContextId, canonicalRepoContextCwd } from "./repo-context";

/**
 * Grok usage tailer (bead eco-6hoxj.163.20).
 *
 * Grok Build reaches the collector through hooks whose payloads carry no token
 * fields, so every Grok event in the ledger had zero tokens. Grok itself
 * records what it billed in `<GROK_HOME>/sessions/<url-encoded cwd>/<session
 * id>/usage.json`: session totals plus one record per completed turn (input,
 * output, reasoning, cached-read and cache-creation tokens, cost ticks, model
 * calls, per-model usage, `turnNumber`, `endedAt`, `usageIsIncomplete`).
 *
 * Mechanics:
 *  - Metadata only. The only file this tailer ever opens is `usage.json`, and
 *    it never lists a session directory: chat_history.jsonl, events.jsonl,
 *    prompt_context.json, system_prompt.txt, updates.jsonl and every other
 *    sibling are never opened, read or stat'ed. Group directories are listed
 *    with entry types, so a file beside the session directories is skipped
 *    without a system call on it. No symlink is followed.
 *  - One event per (turn, model). The ledger's typed columns (model, tokens,
 *    cost) are what every aggregate, price and upload reads, so a turn that
 *    used two models becomes two events whose tokens and cost sum exactly to
 *    the turn. A turn whose per-model rows do not add up to its own totals
 *    (or has none) becomes one event under its primary model instead, so a
 *    turn's total is never split inexactly. The choice is fixed per turn.
 *  - Token convention: Grok follows the OpenAI convention Codex rollouts use —
 *    `inputTokens` includes `cachedReadTokens` and `outputTokens` includes
 *    `reasoningTokens` (`totalTokens` = input + output). The mapping is the
 *    rollout tailer's: input → inputTokens, cached reads → cacheReadTokens,
 *    output → outputTokens, reasoning → metadata.reasoningOutputTokens, and
 *    cache writes → cacheCreationTokens.
 *  - Cost: `costUsdTicks` is Grok's own billed cost in 1e-10 USD, recorded as
 *    a reported cost. A row Grok marks `usageIsIncomplete` keeps its tokens
 *    but no cost (a partial bill is never presented as the bill), and every
 *    event of an incomplete turn, model row or session is labelled
 *    `usageSource: "grok_usage_incomplete"`.
 *  - Identity and rewrites: event ids are deterministic over (session id,
 *    turn number, model), and a durable per-turn record holds what has been
 *    counted. The ledger is append-only and first-writer-wins, like Claude
 *    transcript usage, so a rewrite that raises a turn's numbers appends one
 *    revision event carrying only the increase, and a rewrite that lowers any
 *    counted number is refused and counted: a turn is never counted twice.
 *  - Change detection: an unchanged usage.json (same device, inode, size,
 *    mtime and ctime) costs one lstat and no read per sweep (a second one
 *    when its group is among the recently modified ones the recent lane
 *    checks); a changed one whose bytes hash to the last committed document
 *    costs a read but no parse.
 *  - Walk order: a sweep first checks the most recently modified groups for
 *    usage files written in the last two days, so today's tokens arrive
 *    first. It walks every group, then each group's sessions, in the order
 *    of the hashes of their names, one session per step, each step admitted
 *    by the shared budget. Its only durable walk state is a cursor of name
 *    hashes, so a restarted worker lists the cursor's group again and goes
 *    on right after it, and every pass moves the cursor however often the
 *    worker is replaced. A per-pass cap only moves work to the next pass.
 *  - Bounds: discovery entries and wall time per pass, pending files, a
 *    per-document byte ceiling and the shared automatic capture budget
 *    (bytes, records = turns, events, wall). Work left over is deferred, never
 *    waited on; turn records make a partly processed document safe to resume.
 */

export const GROK_USAGE_FILE_NAME = "usage.json";
/** xAI bills in ticks: 1 USD = 10^10 ticks (docs.x.ai, cost tracking). */
export const GROK_USD_TICKS_PER_USD = 10_000_000_000;

export const GROK_USAGE_LIMITS = Object.freeze({
  /**
   * A document is read whole. One larger than a cadence's whole byte
   * allowance could never be read, so it is reported instead of retried.
   */
  maxFileBytes: 512 * 1024,
  maxTurnsPerFile: 10_000,
  maxModelsPerTurn: 64,
  /**
   * Groups a pass opens, and sessions of one group a pass observes. A cap
   * leaves the rest to a later pass of the same sweep and is counted as over
   * limit; nothing is dropped.
   */
  maxGroups: 4_096,
  maxSessionsPerGroup: 100_000,
  /**
   * Discovery steps per pass: listing a directory, or examining one session
   * (at most two lstats). Listings read at most 32 entries a step, except in
   * the walk's first unit of a pass, which lists what it must to move.
   */
  entriesPerPass: 2_048,
  discoveryWallMs: 50,
  pendingFiles: 64,
  /** Reported with the scan; a sweep is bounded by the entries it began with. */
  lifetimeEntryLimit: 200_000,
  /** The recent lane: groups modified within the window, newest first. */
  recentGroups: 16,
  recentSessions: 4_096,
  recentWindowMs: 48 * 60 * 60 * 1_000,
  /**
   * The recent lane runs when a sweep begins and again once this long has
   * passed since it last started, whichever worker started it.
   */
  recentRefreshMs: 10 * 60 * 1_000,
});

export type GrokUsageLimits = { readonly [Key in keyof typeof GROK_USAGE_LIMITS]: number };

const FILE_STATE_TABLE = "grok_usage_file_state";
const TURN_STATE_TABLE = "grok_usage_turn_state";
/** Which sweep is in progress, when it began and how far it has walked. Path- and content-free. */
const WALK_ROUND_KEY = "grok_usage_walk_round_v1";
/** The cursor's session before a group's first session, and once the group is finished. */
const GROUP_STARTED = "";
const GROUP_FINISHED = "~";
/** Directory entries a pass may list, per step of its allowance. */
const READS_PER_STEP = 32;

const MODEL_ID = /^[A-Za-z0-9][A-Za-z0-9._:+-]{0,127}$/;
const SESSION_ID = /^[A-Za-z0-9][A-Za-z0-9._:+-]{0,127}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const TOKEN_KEYS = ["input", "cachedRead", "cacheCreation", "output", "reasoning"] as const;

type TokenKey = (typeof TOKEN_KEYS)[number];
type Usage = Record<TokenKey, number> & { costTicks: number | null };
/** `billIncomplete`: Grok marked this row partial. `labelIncomplete`: the row or its session. */
type Stream = Usage & { model: string | null; billIncomplete: boolean; labelIncomplete: boolean };
type CountedStream = Usage & { model: string | null };
type TurnState = {
  splitMode: "model" | "turn";
  streams: Record<string, CountedStream>;
};

type ParsedTurn = {
  turnNumber: number;
  endedAt: string | null;
  totals: Usage;
  incomplete: boolean;
  primaryModel: string | null;
  /** Per-model rows, or null when absent or unusable. */
  models: Map<string, Usage & { incomplete: boolean }> | null;
};

type ParsedDocument = {
  sessionId: string;
  updatedAt: string | null;
  sessionIncomplete: boolean;
  sessionPrimaryModel: string | null;
  /**
   * Input plus output tokens in Grok's session totals that no turn record
   * carries (for example an interrupted turn). Never emitted: a later turn
   * record could claim them. Reported so hosts can compare with Grok.
   */
  sessionOnlyTokens: number;
  turns: ParsedTurn[];
  /** Turns refused as malformed or ambiguous (duplicate turn numbers). */
  invalidTurns: number;
};

type Identity = {
  device: string;
  inode: string;
  size: number;
  mtimeNs: string;
  ctimeNs: string;
  mtimeMs: number;
};

type Candidate = {
  file: string;
  fileKey: string;
  groupName: string;
  sessionName: string;
  sessionDirectory: string;
  identity: Identity;
};

type SweepCounters = GrokUsageSweepCounters;

type RecentGroup = { name: string; mtimeMs: number };

/** A directory the walk visits: its name, and its place in walk order. */
type Listed = { name: string; hash: string };

/** The group a walk is visiting: its sessions in walk order, and the next to examine. */
type WalkGroup = { name: string; hash: string; sessions: Listed[]; next: number };

/**
 * How far a sweep has walked, durably. Groups are walked in the order of the
 * hashes of their names, and each group's sessions likewise. Every group
 * before `group` is finished, and so is every session of `group` up to and
 * including `session` (GROUP_STARTED before its first, GROUP_FINISHED once
 * the whole group is done). Null before the first group.
 */
type WalkCursor = { group: string; session: string } | null;

type Sweep = {
  /** The sweep in progress and when it began; newer entries wait for the next. */
  round: number;
  startedAtMs: number;
  startedAt: string;
  /** When a recent lane last started in this sweep, by any worker. Durable. */
  recentAtMs: number | null;
  /**
   * Durable: a pass of this sweep saw an error or a worker closed with a
   * queued file unread. Counters live in one worker; this does not.
   */
  unclean: boolean;
  /** Durable: where the walk stands. */
  cursor: WalkCursor;
  recent: {
    root: fs.Dir | null;
    rootOpened: boolean;
    ranked: boolean;
    groups: RecentGroup[];
    index: number;
    handle: fs.Dir | null;
    observed: number;
    done: boolean;
  };
  /** This worker's listing of sessions/, in walk order; a new worker lists it again. */
  groups: Listed[] | null;
  group: WalkGroup | null;
  /** The cursor, `recentAtMs` or `unclean` changed and is written with the pass. */
  stateChanged: boolean;
  entries: number;
  done: boolean;
  counters: SweepCounters;
};

/** One discovery pass: its allowances and per-pass caps. */
type WalkPass = {
  steps: number;
  reads: number;
  groupsOpened: number;
  observedByGroup: Map<string, number>;
  stalled: boolean;
  /** Groups and sessions the walk's cursor moved past this pass. */
  covered: number;
  /** The recent lane's share of this pass so far. */
  recentSteps: number;
  recentMs: number;
};

export type GrokUsageFileCounters = {
  sweepStartedAt: string | null;
  seen: number;
  unchanged: number;
  parsed: number;
  deferred: number;
  oversized: number;
  unresolved: number;
  errors: number;
  /** Tokens Grok's session totals hold beyond its turn records, host-wide. */
  sessionOnlyTokens: number;
  /** Groups and sessions a per-pass cap left to a later pass of the sweep. */
  groupsOverLimit: number;
  sessionsOverLimit: number;
  /** Usage files the recent lane queued ahead of the walk this sweep. */
  recentFiles: number;
};

export type GrokUsageScanReceipt = CaptureScanProgress & { usageFiles: GrokUsageFileCounters };

export type GrokUsageScanResult = {
  scope: "automatic";
  /** Path-free state of the Grok root this pass used. */
  home: "ready" | "absent" | "invalid";
  exhaustive: boolean;
  discoveryErrors: number;
  statErrors: number;
  readErrors: number;
  parseErrors: number;
  unresolvedRecords: number;
  filesSeen: number;
  filesUnchanged: number;
  filesRead: number;
  filesParsed: number;
  filesDeferred: number;
  filesOversized: number;
  bytesRead: number;
  bytesDeferred: number;
  /** Turns examined. */
  recordsParsed: number;
  /** Turns whose durable record advanced. */
  recordsCommitted: number;
  eventsAppended: number;
  turnsRevised: number;
  turnRewritesRefused: number;
  incompleteEvents: number;
  sessionsSkippedLiveCovered: number;
  enrollmentExcludedEvents: number;
  futureTimestampClampedEvents: number;
  tokensAppended: Record<TokenKey, number>;
  costUsdTicksAppended: number;
  deferredGenerations: number;
  excludedGenerations: number;
  cooperativeYields: number;
  lastYieldAt: string | null;
  aborted: boolean;
  automaticBudget: CaptureBudgetStatus | null;
  activity: {
    lastActivityAt: string | null;
    filesToday: number;
    discoveryEntries: number;
    lastScanAt: string;
    truncated: boolean;
    scan: GrokUsageScanReceipt;
  };
};

function zeroCounters(): SweepCounters {
  return {
    filesSeen: 0, filesUnchanged: 0, filesParsed: 0, filesUnresolved: 0,
    filesOversized: 0, filesDeferred: 0, bytesRead: 0, eventsAppended: 0,
    parseErrors: 0, discoveryErrors: 0, statErrors: 0, readErrors: 0,
    groupsOverLimit: 0, sessionsOverLimit: 0, recentFiles: 0,
  };
}

/** No error, and nothing left unread or unresolved: the sweep read every usage file it found. */
function sweepClean(counters: SweepCounters) {
  return counters.filesUnresolved === 0 && counters.filesDeferred === 0 && counters.parseErrors === 0 &&
    counters.discoveryErrors === 0 && counters.statErrors === 0 && counters.readErrors === 0 &&
    counters.filesOversized === 0;
}

function zeroUsage(): Usage {
  return { input: 0, cachedRead: 0, cacheCreation: 0, output: 0, reasoning: 0, costTicks: null };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function count(value: unknown): number | null {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0 ? value : null;
}

function modelId(value: unknown): string | null {
  return typeof value === "string" && MODEL_ID.test(value) ? value : null;
}

function isoStamp(value: unknown): string | null {
  if (typeof value !== "string" || !/(?:Z|[+-]\d{2}:\d{2})$/.test(value.trim())) return null;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? new Date(parsed).toISOString() : null;
}

/** Read one usage aggregate. Absent token keys are zero; a present key must be exact. */
function parseUsage(value: unknown): (Usage & { incomplete: boolean }) | null {
  if (!isRecord(value)) return null;
  const keys: Record<TokenKey, string> = {
    input: "inputTokens",
    cachedRead: "cachedReadTokens",
    cacheCreation: "cacheCreationTokens",
    output: "outputTokens",
    reasoning: "reasoningTokens",
  };
  const usage = zeroUsage();
  for (const key of TOKEN_KEYS) {
    const raw = value[keys[key]];
    if (raw === undefined) continue;
    const parsed = count(raw);
    if (parsed === null) return null;
    usage[key] = parsed;
  }
  if (value.costUsdTicks !== undefined) {
    const ticks = count(value.costUsdTicks);
    if (ticks === null) return null;
    usage.costTicks = ticks;
  }
  if (value.usageIsIncomplete !== undefined && typeof value.usageIsIncomplete !== "boolean") return null;
  return { ...usage, incomplete: value.usageIsIncomplete === true };
}

function parseModelUsage(value: unknown): Map<string, Usage & { incomplete: boolean }> | null {
  if (!isRecord(value)) return null;
  const entries = Object.entries(value);
  if (entries.length === 0 || entries.length > GROK_USAGE_LIMITS.maxModelsPerTurn) return null;
  const models = new Map<string, Usage & { incomplete: boolean }>();
  for (const [key, row] of entries) {
    const id = modelId(key);
    const usage = parseUsage(row);
    if (!id || !usage) return null;
    models.set(id, usage);
  }
  return models;
}

/** Parse the value-typed fields this tailer reads; every other key is ignored. */
export function parseGrokUsageDocument(value: unknown, directorySessionId: string): ParsedDocument | null {
  if (!isRecord(value) || !Array.isArray(value.turns) ||
    value.turns.length > GROK_USAGE_LIMITS.maxTurnsPerFile) return null;
  const declared = typeof value.sessionId === "string" && SESSION_ID.test(value.sessionId)
    ? value.sessionId
    : null;
  if (value.sessionId !== undefined && declared === null) return null;
  const fromDirectory = SESSION_ID.test(directorySessionId) ? directorySessionId : null;
  // A copied or moved document must not be counted as another session.
  if (declared && fromDirectory && declared.toLowerCase() !== fromDirectory.toLowerCase()) return null;
  const rawSessionId = declared ?? fromDirectory;
  if (!rawSessionId) return null;
  const sessionId = UUID.test(rawSessionId) ? rawSessionId.toLowerCase() : rawSessionId;
  const session = isRecord(value.session) ? value.session : {};
  const turns: ParsedTurn[] = [];
  const seen = new Map<number, number>();
  let invalidTurns = 0;
  for (const raw of value.turns) {
    const totals = parseUsage(raw);
    const turnNumber = isRecord(raw) ? count(raw.turnNumber) : null;
    if (!isRecord(raw) || !totals || turnNumber === null) {
      invalidTurns += 1;
      continue;
    }
    const models = raw.modelUsage === undefined ? null : parseModelUsage(raw.modelUsage);
    seen.set(turnNumber, (seen.get(turnNumber) ?? 0) + 1);
    turns.push({
      turnNumber,
      endedAt: isoStamp(raw.endedAt),
      totals: { ...totals },
      incomplete: totals.incomplete,
      primaryModel: modelId(raw.primaryModelId),
      models,
    });
  }
  // Two records claiming one turn number cannot be told apart by identity.
  const unique = turns.filter((turn) => seen.get(turn.turnNumber) === 1);
  invalidTurns += turns.length - unique.length;
  unique.sort((left, right) => left.turnNumber - right.turnNumber);
  const sessionTotals = parseUsage(session);
  const turnTokens = turns.reduce((total, turn) => total + turn.totals.input + turn.totals.output, 0);
  return {
    sessionId,
    updatedAt: isoStamp(value.updatedAt),
    sessionIncomplete: session.usageIsIncomplete === true,
    sessionPrimaryModel: modelId(session.primaryModelId),
    sessionOnlyTokens: sessionTotals
      ? Math.max(0, sessionTotals.input + sessionTotals.output - turnTokens)
      : 0,
    turns: unique,
    invalidTurns,
  };
}

/**
 * Grok names a session's group directory by URL-encoding its working
 * directory. Decode once; `+` stays literal. A malformed escape, a relative or
 * non-canonical path, or the filesystem root yields no project.
 */
export function grokGroupWorkingDirectory(groupName: string): string | null {
  // Grok encodes process.cwd(), which is already canonical; anything that
  // decodes to another spelling is not a directory this session ran in.
  let decoded: string;
  try {
    decoded = decodeURIComponent(groupName);
  } catch {
    return null;
  }
  const canonical = canonicalRepoContextCwd(decoded);
  if (!canonical || canonical !== decoded) return null;
  return canonical === path.parse(canonical).root ? null : canonical;
}

function sumModels(models: Map<string, Usage & { incomplete: boolean }>): Usage {
  const total = zeroUsage();
  let ticks = 0;
  let allTicks = true;
  for (const row of models.values()) {
    for (const key of TOKEN_KEYS) total[key] += row[key];
    if (row.costTicks === null) allTicks = false;
    else ticks += row.costTicks;
  }
  total.costTicks = allTicks ? ticks : null;
  return total;
}

/** Per-model rows are used only when they reproduce the turn exactly. */
function modelsReconcile(turn: ParsedTurn) {
  if (!turn.models) return false;
  const summed = sumModels(turn.models);
  if (!TOKEN_KEYS.every((key) => summed[key] === turn.totals[key])) return false;
  return turn.totals.costTicks === null || summed.costTicks === turn.totals.costTicks;
}

function turnStreams(turn: ParsedTurn, mode: "model" | "turn", fallbackModel: string | null,
  sessionIncomplete: boolean): Map<string, Stream> | null {
  const streams = new Map<string, Stream>();
  if (mode === "model") {
    if (!turn.models) return null;
    for (const [model, row] of turn.models) {
      const billIncomplete = row.incomplete || turn.incomplete;
      streams.set(model, {
        ...row,
        model,
        billIncomplete,
        labelIncomplete: billIncomplete || sessionIncomplete,
      });
    }
    return streams;
  }
  // One stream under a fixed key: the turn's model is decided on first sight.
  const soleModel = turn.models && turn.models.size === 1 ? [...turn.models.keys()][0]! : null;
  const billIncomplete = turn.incomplete || [...(turn.models?.values() ?? [])].some((row) => row.incomplete);
  streams.set("turn", {
    ...turn.totals,
    model: turn.primaryModel ?? soleModel ?? fallbackModel,
    billIncomplete,
    labelIncomplete: billIncomplete || sessionIncomplete,
  });
  return streams;
}

type Emit = { stream: Stream; delta: Usage; absolute: Usage; revision: boolean };

/**
 * Compare a turn with what is already counted. A stream seen for the first
 * time emits whole. A rewrite emits only increases; any decrease — a lower
 * number, a lower bill, or a counted per-model row that disappeared — is
 * refused, so an append-only ledger never counts a turn twice.
 */
export function planTurn(streams: Map<string, Stream>, previous: TurnState | null):
  "refused" | { emits: Emit[]; counted: Record<string, CountedStream> } {
  const counted: Record<string, CountedStream> = { ...(previous?.streams ?? {}) };
  for (const [key, prior] of Object.entries(previous?.streams ?? {})) {
    const nonzero = TOKEN_KEYS.some((tokenKey) => prior[tokenKey] > 0) || (prior.costTicks ?? 0) > 0;
    if (nonzero && !streams.has(key)) return "refused";
  }
  const emits: Emit[] = [];
  for (const [key, stream] of streams) {
    const prior = previous?.streams[key];
    const delta = zeroUsage();
    for (const tokenKey of TOKEN_KEYS) {
      const before = prior?.[tokenKey] ?? 0;
      if (stream[tokenKey] < before) return "refused";
      delta[tokenKey] = stream[tokenKey] - before;
    }
    // A bill is counted only from a row Grok does not mark incomplete; a
    // later complete bill adds what was not yet counted.
    const bill = stream.billIncomplete ? null : stream.costTicks;
    const countedBill = prior?.costTicks ?? null;
    if (bill !== null && countedBill !== null && bill < countedBill) return "refused";
    delta.costTicks = bill === null ? null : bill - (countedBill ?? 0);
    const model = prior ? prior.model : stream.model;
    const absolute: Usage = {
      input: stream.input,
      cachedRead: stream.cachedRead,
      cacheCreation: stream.cacheCreation,
      output: stream.output,
      reasoning: stream.reasoning,
      costTicks: bill ?? countedBill,
    };
    counted[key] = { ...absolute, model };
    if (TOKEN_KEYS.some((tokenKey) => delta[tokenKey] > 0) || (delta.costTicks ?? 0) > 0) {
      emits.push({ stream: { ...stream, model }, delta, absolute, revision: prior !== undefined });
    }
  }
  return { emits, counted };
}

/**
 * The first sight of (session, turn, model) takes the stable id; a revision's
 * id also binds the new absolute numbers, so replaying either never appends.
 */
export function buildGrokUsageEvent(sessionId: string, turnNumber: number, observedAt: string,
  emit: Emit): AiInteractionEvent {
  const model = emit.stream.model ?? undefined;
  const id = emit.revision
    ? deterministicEventId([
        "grok-usage-revision", sessionId, String(turnNumber), model ?? "",
        ...TOKEN_KEYS.map((key) => String(emit.absolute[key])),
        String(emit.absolute.costTicks ?? ""),
      ])
    : deterministicEventId(["grok-usage", sessionId, String(turnNumber), model ?? ""]);
  const costTicks = emit.delta.costTicks ?? 0;
  return aiInteractionEventSchema.parse({
    id,
    tenantId: "local",
    source: "grok",
    dataMode: "metadata",
    eventType: "usage_transcript",
    observedAt,
    sessionId,
    model,
    actionClass: "other",
    inputTokens: emit.delta.input,
    outputTokens: emit.delta.output,
    cacheReadTokens: emit.delta.cachedRead,
    ...(emit.delta.cacheCreation > 0 ? { cacheCreationTokens: emit.delta.cacheCreation } : {}),
    ...(costTicks > 0 ? { costUsd: costTicks / GROK_USD_TICKS_PER_USD, costKind: "reported" as const } : {}),
    metadata: {
      usageSource: emit.stream.labelIncomplete ? "grok_usage_incomplete" : "grok_usage",
      turnIndex: turnNumber,
      ...(emit.delta.reasoning > 0 ? { reasoningOutputTokens: emit.delta.reasoning } : {}),
    },
  });
}

type MutationSnapshot = {
  counters: Pick<GrokUsageSweepCounters, "eventsAppended">;
  result: Pick<GrokUsageScanResult, "eventsAppended" | "recordsParsed" | "recordsCommitted" |
    "turnsRevised" | "turnRewritesRefused" | "incompleteEvents" | "sessionsSkippedLiveCovered" |
    "enrollmentExcludedEvents" | "futureTimestampClampedEvents" | "costUsdTicksAppended" | "tokensAppended">;
};

function mutationSnapshot(result: GrokUsageScanResult, counters: GrokUsageSweepCounters): MutationSnapshot {
  return {
    counters: { eventsAppended: counters.eventsAppended },
    result: {
      eventsAppended: result.eventsAppended,
      recordsParsed: result.recordsParsed,
      recordsCommitted: result.recordsCommitted,
      turnsRevised: result.turnsRevised,
      turnRewritesRefused: result.turnRewritesRefused,
      incompleteEvents: result.incompleteEvents,
      sessionsSkippedLiveCovered: result.sessionsSkippedLiveCovered,
      enrollmentExcludedEvents: result.enrollmentExcludedEvents,
      futureTimestampClampedEvents: result.futureTimestampClampedEvents,
      costUsdTicksAppended: result.costUsdTicksAppended,
      tokensAppended: { ...result.tokensAppended },
    },
  };
}

function restoreMutationSnapshot(result: GrokUsageScanResult, counters: GrokUsageSweepCounters,
  snapshot: MutationSnapshot) {
  counters.eventsAppended = snapshot.counters.eventsAppended;
  Object.assign(result, snapshot.result, { tokensAppended: { ...snapshot.result.tokensAppended } });
}

function sha256(value: string | Buffer) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function identityOf(stat: fs.BigIntStats): Identity {
  return {
    device: stat.dev.toString(),
    inode: stat.ino.toString(),
    size: Number(stat.size),
    mtimeNs: stat.mtimeNs.toString(),
    ctimeNs: stat.ctimeNs.toString(),
    mtimeMs: Number(stat.mtimeNs / 1_000_000n),
  };
}

function sameIdentity(left: Identity, right: fs.BigIntStats) {
  return left.device === right.dev.toString() && left.inode === right.ino.toString() &&
    left.size === Number(right.size) && left.mtimeNs === right.mtimeNs.toString() &&
    left.ctimeNs === right.ctimeNs.toString();
}

function realDirectory(directory: string): fs.BigIntStats | null {
  try {
    const stat = fs.lstatSync(directory, { bigint: true });
    return stat.isDirectory() && !stat.isSymbolicLink() ? stat : null;
  } catch {
    return null;
  }
}

function isSymlink(file: string) {
  try {
    return fs.lstatSync(file).isSymbolicLink();
  } catch {
    return false;
  }
}

/** The durable, path-free key of one usage file (unchanged since 0.7.37). */
function grokUsageFileKey(file: string) {
  return sha256(`plimsoll-grok-usage-v1\0${file}`);
}

/** Walk order key: a hash of the directory name, never the name itself. */
function walkHash(name: string) {
  return sha256(`plimsoll-grok-walk-v1\0${name}`).slice(0, 32);
}

function closeDirectory(handle: fs.Dir | null) {
  try {
    handle?.closeSync();
  } catch {
    // Discovery already recorded what it saw; a close failure must not throw.
  }
}

/**
 * The durable round: its number, when it began (null until its first pass),
 * when a recent lane last started in it, whether any pass of it saw an
 * error or left a queued file unread, and how far its walk has come.
 * Version 2 replaced the round-3 table of covered names with the cursor.
 */
type WalkRound = {
  round: number;
  startedAtMs: number | null;
  recentAtMs: number | null;
  unclean: boolean;
  cursor: WalkCursor;
};

const WALK_HASH = /^[0-9a-f]{32}$/;

function readWalkRound(database: Database.Database): WalkRound | null {
  try {
    const parsed = JSON.parse(readMaintenanceState(database, WALK_ROUND_KEY) ?? "null") as {
      version?: unknown; round?: unknown; startedAtMs?: unknown; recentAtMs?: unknown; unclean?: unknown;
      cursor?: { group?: unknown; session?: unknown } | null;
    } | null;
    if (parsed?.version !== 2 || !Number.isSafeInteger(parsed.round) || (parsed.round as number) < 1) return null;
    const time = (value: unknown) => Number.isSafeInteger(value) && (value as number) >= 0 ? value as number : null;
    const startedAtMs = time(parsed.startedAtMs);
    if (parsed.startedAtMs !== null && startedAtMs === null) return null;
    const cursor = parsed.cursor ?? null;
    if (cursor !== null && !(typeof cursor.group === "string" && WALK_HASH.test(cursor.group) &&
      typeof cursor.session === "string" && (cursor.session === GROUP_STARTED ||
        cursor.session === GROUP_FINISHED || WALK_HASH.test(cursor.session)))) return null;
    return {
      round: parsed.round as number, startedAtMs, recentAtMs: time(parsed.recentAtMs),
      unclean: parsed.unclean === true,
      cursor: cursor === null ? null : { group: cursor.group as string, session: cursor.session as string },
    };
  } catch {
    return null;
  }
}

function writeWalkRound(database: Database.Database, round: WalkRound) {
  writeMaintenanceState(database, WALK_ROUND_KEY, JSON.stringify({ version: 2, ...round }));
}

export function ensureGrokUsageState(database: Database.Database) {
  database.exec(`
    create table if not exists ${FILE_STATE_TABLE} (
      file_key text primary key,
      device text not null,
      inode text not null,
      size integer not null,
      mtime_ns text not null,
      ctime_ns text not null,
      mtime_ms integer not null,
      content_sha256 text,
      turns integer not null default 0,
      token_bearing integer not null default 0 check (token_bearing in (0,1)),
      session_only_tokens integer not null default 0,
      status text not null check (status in ('committed','partial','parse_error','oversized')),
      updated_at text not null
    ) without rowid;
    create table if not exists ${TURN_STATE_TABLE} (
      session_id text not null,
      turn_number integer not null,
      split_mode text not null check (split_mode in ('model','turn')),
      streams_json text not null,
      ended_at text,
      revisions integer not null default 0,
      updated_at text not null,
      primary key (session_id, turn_number)
    ) without rowid;
    create table if not exists maintenance_state (
      key text primary key,
      value text not null,
      updated_at text not null
    );
  `);
}

function readMaintenanceState(database: Database.Database, key: string) {
  return (database.prepare(`select value from maintenance_state where key = ?`).get(key) as
    | { value: string }
    | undefined)?.value;
}

function writeMaintenanceState(database: Database.Database, key: string, value: string) {
  database.prepare(
    `insert into maintenance_state (key, value, updated_at) values (?, ?, ?)
     on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
  ).run(key, value, new Date().toISOString());
}

export class GrokUsageTailer {
  private sweep: Sweep | null = null;
  private pending: Candidate[] = [];
  /** The recent lane and the walk can meet the same file; queue it once. */
  private readonly pendingKeys = new Set<string>();
  private readonly sessionsRoot: string | null;

  constructor(
    private readonly buffer: LocalEventBuffer,
    /** The resolved Grok home, or null when its configuration is invalid. */
    private readonly grokHome: string | null,
    private readonly limits: GrokUsageLimits = GROK_USAGE_LIMITS,
    private readonly now: () => number = () => Date.now(),
  ) {
    this.sessionsRoot = grokHome ? path.resolve(grokHome, "sessions") : null;
    ensureGrokUsageState(this.buffer.database);
  }

  close() {
    // A queued file this worker never read may belong to a session the
    // cursor has passed: this sweep can no longer claim it read everything.
    if (this.sweep && this.pending.length > 0 && !this.sweep.unclean) {
      this.sweep.unclean = true;
      this.sweep.stateChanged = true;
    }
    this.closeSweep(this.sweep);
    this.sweep = null;
    this.pending = [];
    this.pendingKeys.clear();
  }

  private closeSweep(sweep: Sweep | null) {
    if (!sweep) return;
    try {
      this.writeState(sweep);
    } catch {
      // A cursor that was not written is walked again from the last one written.
    }
    this.closeHandles(sweep);
  }

  /** Close a sweep's directory streams without writing its walk state. */
  private closeHandles(sweep: Sweep) {
    closeDirectory(sweep.recent.root);
    closeDirectory(sweep.recent.handle);
    sweep.recent.root = null;
    sweep.recent.handle = null;
  }

  /**
   * eco-6hoxj.163.18: every usage file the capture frontier must cover, walked
   * the way discover() walks: group and session directories are listed with
   * entry types, usage.json is lstat'ed and never opened, and a session
   * directory is never listed. A file counts as read when the tailer
   * committed this exact generation (same device, inode, size, mtime and
   * ctime). An invalid GROK_HOME makes the walk incomplete, as it stops the
   * scan; no sessions directory means no files. The tailer follows no
   * symlink, so a symlinked sessions directory, group, session or usage.json
   * is reported as a link, never covered, and never followed (review r4, S1).
   */
  coverageWalk(maxEntries = CAPTURE_COVERAGE_MAX_ENTRIES): CaptureCoverageWalk {
    const root = this.sessionsRoot;
    if (!root) return new CaptureCoverageWalk(null);
    const state = this.buffer.database.prepare(
      `select device, inode, size, mtime_ns as mtimeNs, ctime_ns as ctimeNs, mtime_ms as mtimeMs, status
       from ${FILE_STATE_TABLE} where file_key = ?`,
    );
    const directories = (directory: string, limit: number) => {
      const names: string[] = [];
      const links: string[] = [];
      for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
        if (entry.name.startsWith(".")) continue;
        if (entry.isDirectory()) names.push(entry.name);
        else if (entry.isSymbolicLink()) links.push(path.join(directory, entry.name));
      }
      // The sweep never reaches past its own limits, so neither can the check.
      if (names.length > limit) throw new Error("grok_usage_coverage_over_limit");
      return { names, links };
    };
    const linkVerdict = (link: string, stat: fs.BigIntStats) =>
      linkCoverageFile(sha256(`plimsoll-grok-usage-v1\0${link}\0symlink`), Number(stat.birthtimeNs / 1_000_000n));
    return new CaptureCoverageWalk({
      roots: [root],
      maxEntries,
      list: (directory, depth) => {
        if (depth === 0) {
          if (!realDirectory(directory)) {
            // The scan reads nothing through a symlinked sessions directory.
            return { directories: [], files: [], links: isSymlink(directory) ? [directory] : [] };
          }
          const groups = directories(directory, this.limits.maxGroups);
          return { directories: groups.names.map((name) => path.join(directory, name)), files: [], links: groups.links };
        }
        if (!realDirectory(directory)) throw new Error("grok_usage_group_not_directory");
        const sessions = directories(directory, this.limits.maxSessionsPerGroup);
        return {
          directories: [],
          files: sessions.names.map((name) => path.join(directory, name, GROK_USAGE_FILE_NAME)),
          links: sessions.links,
        };
      },
      check: (file) => {
        let stat: fs.BigIntStats;
        try {
          stat = fs.lstatSync(file, { bigint: true });
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") return null; // no usage yet
          throw error;
        }
        if (stat.isSymbolicLink()) return linkVerdict(file, stat);
        if (!stat.isFile()) return null;
        // The same key observeSession() records the file under.
        const key = sha256(`plimsoll-grok-usage-v1\0${file}`);
        const row = state.get(key) as (Identity & { status: string }) | undefined;
        const committed = row !== undefined && row.status === "committed";
        const mtimeMs = Number(stat.mtimeNs / 1_000_000n);
        return {
          key,
          mtimeMs,
          birthtimeMs: Number(stat.birthtimeNs / 1_000_000n),
          extent: mtimeMs,
          progress: committed ? row.mtimeMs : -1,
          fullyRead: committed && sameIdentity(row, stat),
        };
      },
      checkLink: (link) => {
        let stat: fs.BigIntStats;
        try {
          stat = fs.lstatSync(link, { bigint: true });
        } catch (error) {
          if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
          throw error;
        }
        return stat.isSymbolicLink() ? linkVerdict(link, stat) : null;
      },
    });
  }

  async scan(options: {
    budget: CaptureWorkBudget;
    now?: Date;
    signal?: AbortSignal;
    deferredBeforeIo?: boolean;
  }): Promise<GrokUsageScanResult> {
    const scanNow = options.now ?? new Date(this.now());
    const result = this.emptyResult(scanNow);
    const budget = options.budget;
    let entriesThisTick = 0;
    try {
      if (!this.sessionsRoot) {
        // An invalid GROK_HOME is doctor's to name; the scan cannot run.
        result.discoveryErrors += 1;
        result.activity.truncated = true;
        return result;
      }
      if (options.deferredBeforeIo || !budget.canContinue()) {
        result.activity.truncated = true;
        result.deferredGenerations = Math.max(1, this.pending.length);
        return result;
      }
      this.beginSweepIfNeeded(scanNow);
      await this.drainPending(budget, result, options.signal);
      if (!result.aborted && this.pending.length < this.limits.pendingFiles) {
        entriesThisTick = this.discover(budget, result, scanNow, options.signal);
        await this.drainPending(budget, result, options.signal);
      }
      if (this.sweep && this.sweep.done && this.pending.length === 0) this.finishSweep(scanNow, result);
      return result;
    } finally {
      result.deferredGenerations = Math.max(result.deferredGenerations, this.pending.length);
      result.filesDeferred += this.pending.length;
      result.activity.truncated = result.activity.truncated || this.sweep !== null || this.pending.length > 0;
      result.automaticBudget = budget.status();
      this.publishActivity(result, scanNow, entriesThisTick, options.deferredBeforeIo === true);
    }
  }

  private emptyResult(scanNow: Date): GrokUsageScanResult {
    return {
      scope: "automatic",
      home: "ready",
      exhaustive: false,
      discoveryErrors: 0,
      statErrors: 0,
      readErrors: 0,
      parseErrors: 0,
      unresolvedRecords: 0,
      filesSeen: 0,
      filesUnchanged: 0,
      filesRead: 0,
      filesParsed: 0,
      filesDeferred: 0,
      filesOversized: 0,
      bytesRead: 0,
      bytesDeferred: 0,
      recordsParsed: 0,
      recordsCommitted: 0,
      eventsAppended: 0,
      turnsRevised: 0,
      turnRewritesRefused: 0,
      incompleteEvents: 0,
      sessionsSkippedLiveCovered: 0,
      enrollmentExcludedEvents: 0,
      futureTimestampClampedEvents: 0,
      tokensAppended: { input: 0, cachedRead: 0, cacheCreation: 0, output: 0, reasoning: 0 },
      costUsdTicksAppended: 0,
      deferredGenerations: 0,
      excludedGenerations: 0,
      cooperativeYields: 0,
      lastYieldAt: null,
      aborted: false,
      automaticBudget: null,
      activity: {
        lastActivityAt: null,
        filesToday: 0,
        discoveryEntries: 0,
        lastScanAt: scanNow.toISOString(),
        truncated: false,
        scan: null as unknown as GrokUsageScanReceipt,
      },
    };
  }

  private beginSweepIfNeeded(scanNow: Date) {
    if (this.sweep) return;
    const database = this.buffer.database;
    if (!grokUsageBackfillMarker(database)) {
      const marker: GrokUsageBackfillMarker = {
        version: 1, startedAt: scanNow.toISOString(), completedAt: null,
        sweeps: 0, lastSweep: null, completion: null,
      };
      writeMaintenanceState(database, GROK_USAGE_BACKFILL_KEY, JSON.stringify(marker));
    }
    // A restarted worker continues the sweep in progress: the round, its
    // cursor and when its recent lane last started are durable, so the walk
    // goes on after the cursor and a replaced worker does not rebuild the
    // recent lane before it is due.
    let round = readWalkRound(database);
    if (round?.startedAtMs == null) {
      const fresh: WalkRound = {
        round: round?.round ?? 1, startedAtMs: scanNow.getTime(), recentAtMs: null, unclean: false, cursor: null,
      };
      writeWalkRound(database, fresh);
      round = fresh;
    }
    const startedAtMs = round.startedAtMs!;
    this.sweep = {
      round: round.round,
      startedAtMs,
      startedAt: new Date(startedAtMs).toISOString(),
      recentAtMs: round.recentAtMs,
      unclean: round.unclean,
      cursor: round.cursor,
      recent: this.recentLane(!this.recentDue(round.recentAtMs, scanNow.getTime())),
      groups: null,
      group: null,
      stateChanged: false,
      entries: 0,
      done: false,
      counters: zeroCounters(),
    };
  }

  /** The recent lane is due when it has not run, after `recentRefreshMs`, or when the clock moved back since. */
  private recentDue(recentAtMs: number | null, nowMs: number) {
    return recentAtMs === null || nowMs < recentAtMs || nowMs - recentAtMs >= this.limits.recentRefreshMs;
  }

  private recentLane(done = false): Sweep["recent"] {
    return {
      root: null, rootOpened: false, ranked: false, groups: [], index: 0, handle: null, observed: 0,
      done: done || this.limits.recentGroups <= 0,
    };
  }

  /**
   * Bounded discovery. A step lists a directory or examines one session (at
   * most two lstats); no step stats a whole directory tree. A pass is
   * admitted while the shared budget and the pending queue have room, and it
   * starts with the walk's progress unit: the walk runs until its cursor has
   * moved past one group or session, whatever this pass's wall, allowances
   * or the cadence clock, so every pass moves the sweep however often its
   * worker is replaced (eco-6hoxj.163.42 rounds 4 and 5). That unit lists at
   * most sessions/ and one group and examines at most one session. After it,
   * every step is admitted only while the budget, the pass's wall and
   * allowances and the pending queue all have room, and the recent lane gets
   * at most half of the pass's wall and of its steps.
   */
  private discover(budget: CaptureWorkBudget, result: GrokUsageScanResult, scanNow: Date, signal?: AbortSignal) {
    const sweep = this.sweep!;
    const root = this.sessionsRoot!;
    const nowMs = scanNow.getTime();
    if (!budget.canContinue() || this.pending.length >= this.limits.pendingFiles) return 0;
    const started = performance.now();
    const wallMs = Math.min(this.limits.discoveryWallMs, Math.max(1, budget.remainingWallMs()));
    const pass: WalkPass = {
      steps: 0, reads: 0, groupsOpened: 0, observedByGroup: new Map(), stalled: false,
      covered: 0, recentSteps: 0, recentMs: 0,
    };
    if (sweep.recent.done && sweep.recentAtMs !== null && this.recentDue(sweep.recentAtMs, nowMs)) {
      // A long sweep checks the newest groups again, so fresh usage keeps
      // arriving ahead of the walk.
      sweep.recent = this.recentLane();
    }
    try {
      while (!sweep.done && !pass.stalled && !signal?.aborted &&
        (pass.covered === 0 || (pass.steps < this.limits.entriesPerPass &&
          pass.reads < this.limits.entriesPerPass * READS_PER_STEP &&
          this.pending.length < this.limits.pendingFiles && budget.canContinue() &&
          performance.now() - started < wallMs))) {
        if (pass.covered > 0 && !sweep.recent.done && pass.recentSteps * 2 < this.limits.entriesPerPass &&
          pass.recentMs * 2 < wallMs) {
          const stepsBefore = pass.steps;
          const recentStarted = performance.now();
          this.recentStep(root, pass, nowMs);
          pass.recentSteps += pass.steps - stepsBefore;
          pass.recentMs += performance.now() - recentStarted;
        } else {
          this.walkStep(root, result, pass);
        }
      }
    } finally {
      this.writeState(sweep);
    }
    sweep.entries += pass.steps;
    result.activity.discoveryEntries += pass.steps;
    return pass.steps;
  }

  /** Record the sweep's cursor and state durably with the pass. */
  private writeState(sweep: Sweep) {
    if (!sweep.unclean && !sweepClean(sweep.counters)) {
      sweep.unclean = true;
      sweep.stateChanged = true;
    }
    if (!sweep.stateChanged) return;
    sweep.stateChanged = false;
    writeWalkRound(this.buffer.database, {
      round: sweep.round, startedAtMs: sweep.startedAtMs, recentAtMs: sweep.recentAtMs, unclean: sweep.unclean,
      cursor: sweep.cursor,
    });
  }

  /** Move the walk's cursor past a group or session: one unit of progress. */
  private advance(sweep: Sweep, pass: WalkPass, group: string, session: string) {
    sweep.cursor = { group, session };
    sweep.stateChanged = true;
    pass.covered += 1;
  }

  /**
   * The recent lane: rank the groups modified within the recent window
   * (one lstat each), then check the newest for changed usage files written
   * inside the window and queue them ahead of the walk. It is bounded and
   * counts only what it queued: the walk still covers and counts every one
   * of these sessions. Its only durable state is when it last started, so a
   * replaced worker does not start it again before `recentRefreshMs`.
   */
  private recentStep(root: string, pass: WalkPass, nowMs: number) {
    const sweep = this.sweep!;
    const lane = sweep.recent;
    const finish = () => {
      closeDirectory(lane.root);
      closeDirectory(lane.handle);
      lane.root = null;
      lane.handle = null;
      lane.done = true;
    };
    if (!lane.ranked) {
      if (!lane.rootOpened) {
        lane.rootOpened = true;
        sweep.recentAtMs = nowMs;
        sweep.stateChanged = true;
        pass.steps += 1;
        try {
          lane.root = realDirectory(root) ? fs.opendirSync(root) : null;
        } catch {
          lane.root = null;
        }
        if (!lane.root) finish();
        return;
      }
      let entry: fs.Dirent | null = null;
      pass.reads += 1;
      try {
        entry = lane.root!.readSync();
      } catch {
        entry = null;
      }
      if (!entry) {
        closeDirectory(lane.root);
        lane.root = null;
        lane.ranked = true;
        return;
      }
      if (!entry.isDirectory() || entry.name.startsWith(".")) return;
      pass.steps += 1;
      const stat = realDirectory(path.join(root, entry.name));
      if (!stat || Number(stat.mtimeMs) < nowMs - this.limits.recentWindowMs) return;
      lane.groups.push({ name: entry.name, mtimeMs: Number(stat.mtimeMs) });
      lane.groups.sort((left, right) => right.mtimeMs - left.mtimeMs ||
        (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));
      if (lane.groups.length > this.limits.recentGroups) lane.groups.length = this.limits.recentGroups;
      return;
    }
    if (lane.index >= lane.groups.length || lane.observed >= this.limits.recentSessions) {
      finish();
      return;
    }
    const groupName = lane.groups[lane.index]!.name;
    if (!lane.handle) {
      pass.steps += 1;
      const directory = path.join(root, groupName);
      try {
        lane.handle = realDirectory(directory) ? fs.opendirSync(directory) : null;
      } catch {
        lane.handle = null;
      }
      // An unreadable group is the walk's to report.
      if (!lane.handle) lane.index += 1;
      return;
    }
    let entry: fs.Dirent | null = null;
    pass.reads += 1;
    try {
      entry = lane.handle.readSync();
    } catch {
      entry = null;
    }
    if (!entry) {
      closeDirectory(lane.handle);
      lane.handle = null;
      lane.index += 1;
      return;
    }
    if (!entry.isDirectory() || entry.name.startsWith(".")) return;
    pass.steps += 1;
    lane.observed += 1;
    const file = path.join(root, groupName, entry.name, GROK_USAGE_FILE_NAME);
    let stat: fs.BigIntStats;
    try {
      stat = fs.lstatSync(file, { bigint: true });
    } catch {
      return;
    }
    if (stat.isSymbolicLink() || !stat.isFile() || Number(stat.mtimeMs) < nowMs - this.limits.recentWindowMs) return;
    const fileKey = grokUsageFileKey(file);
    if (this.pendingKeys.has(fileKey) || this.unchanged(fileKey, stat)) return;
    this.enqueue({ file, fileKey, groupName, sessionName: entry.name,
      sessionDirectory: path.dirname(file), identity: identityOf(stat) });
    this.sweep!.counters.recentFiles += 1;
  }

  /**
   * One directory's subdirectories in walk order (the hashes of their
   * names), or null when it cannot be read. Outside the walk's first unit, a
   * listing that would take the pass past its read allowance is given up,
   * and the next pass's first unit lists the directory whole.
   */
  private list(directory: string, pass: WalkPass): Listed[] | null | "over_allowance" {
    const allowance = pass.covered === 0
      ? Number.POSITIVE_INFINITY
      : this.limits.entriesPerPass * READS_PER_STEP - pass.reads;
    let handle: fs.Dir;
    try {
      handle = fs.opendirSync(directory);
    } catch {
      return null;
    }
    const listed: Listed[] = [];
    try {
      for (let read = 0, entry = handle.readSync(); entry; entry = handle.readSync()) {
        read += 1;
        pass.reads += 1;
        if (read > allowance) return "over_allowance";
        // The walk follows no symlink and reads no hidden entry.
        if (entry.isDirectory() && !entry.name.startsWith(".")) listed.push({ name: entry.name, hash: walkHash(entry.name) });
      }
    } catch {
      return null;
    } finally {
      closeDirectory(handle);
    }
    return listed.sort((left, right) => left.hash < right.hash ? -1 : left.hash > right.hash ? 1 : 0);
  }

  /**
   * The walk: every group, then every session of each group, in the order
   * of the hashes of their names, from the sweep's durable cursor. A step
   * lists a directory (sessions/, or a group) or examines one session (at
   * most two lstats). A new worker lists sessions/ and its group again and
   * goes on after the cursor; a worker that stays keeps its listings for the
   * sweep. Every entry present when the sweep began is examined once in the
   * sweep; one created later is examined if it sorts after the cursor, and
   * otherwise waits for the next sweep. The durable state is one cursor,
   * whatever the size of the tree.
   */
  private walkStep(root: string, result: GrokUsageScanResult, pass: WalkPass) {
    const sweep = this.sweep!;
    if (sweep.group) {
      this.sessionStep(root, result, pass, sweep.group);
      return;
    }
    if (!sweep.groups) {
      pass.steps += 1;
      if (!realDirectory(root)) {
        // No Grok sessions on this host: an empty, finished sweep.
        sweep.done = true;
        return;
      }
      const groups = this.list(root, pass);
      if (groups === "over_allowance") {
        pass.stalled = true;
        return;
      }
      if (groups === null) {
        this.discoveryError(result);
        sweep.done = true;
        return;
      }
      sweep.groups = groups;
      return;
    }
    // The cursor's group while it is unfinished, else the first after it.
    const cursor = sweep.cursor;
    const index = sweep.groups.findIndex((group) => cursor === null || group.hash > cursor.group ||
      (group.hash === cursor.group && cursor.session !== GROUP_FINISHED));
    if (index < 0) {
      sweep.done = true;
      return;
    }
    if (pass.groupsOpened >= this.limits.maxGroups) {
      // The per-pass cap: count what is left, then leave it to a later pass.
      sweep.counters.groupsOverLimit += sweep.groups.length - index;
      pass.stalled = true;
      return;
    }
    const next = sweep.groups[index]!;
    pass.steps += 1;
    pass.groupsOpened += 1;
    const directory = path.join(root, next.name);
    let sessions: Listed[] | null | "over_allowance" = null;
    try {
      const stat = fs.lstatSync(directory);
      if (stat.isDirectory() && !stat.isSymbolicLink()) sessions = this.list(directory, pass);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        // Removed since the listing: nothing of it is left to read.
        this.advance(sweep, pass, next.hash, GROUP_FINISHED);
        return;
      }
    }
    if (sessions === "over_allowance") {
      pass.stalled = true;
      return;
    }
    if (sessions === null) {
      // Counted: the sweep is not clean, and the next sweep reads the group again.
      this.discoveryError(result);
      this.advance(sweep, pass, next.hash, GROUP_FINISHED);
      return;
    }
    const after = cursor !== null && cursor.group === next.hash ? cursor.session : GROUP_STARTED;
    const first = sessions.findIndex((session) => session.hash > after);
    sweep.group = { name: next.name, hash: next.hash, sessions, next: first < 0 ? sessions.length : first };
  }

  private sessionStep(root: string, result: GrokUsageScanResult, pass: WalkPass, group: WalkGroup) {
    const sweep = this.sweep!;
    if (group.next >= group.sessions.length) {
      // Every session examined: the group is finished for this sweep.
      sweep.group = null;
      this.advance(sweep, pass, group.hash, GROUP_FINISHED);
      return;
    }
    const observed = pass.observedByGroup.get(group.hash) ?? 0;
    if (observed >= this.limits.maxSessionsPerGroup) {
      // The per-pass cap: count what is left, then leave it to a later pass.
      sweep.counters.sessionsOverLimit += group.sessions.length - group.next;
      pass.stalled = true;
      return;
    }
    // One step: the session directory, then its usage file.
    const session = group.sessions[group.next]!;
    group.next += 1;
    pass.steps += 1;
    this.advance(sweep, pass, group.hash, session.hash);
    if (!realDirectory(path.join(root, group.name, session.name))) return;
    pass.observedByGroup.set(group.hash, observed + 1);
    this.observeSession(root, group.name, session.name, result);
  }

  private discoveryError(result: GrokUsageScanResult) {
    this.sweep!.counters.discoveryErrors += 1;
    result.discoveryErrors += 1;
  }

  private unchanged(fileKey: string, stat: fs.BigIntStats) {
    const state = this.buffer.database.prepare(
      `select device, inode, size, mtime_ns as mtimeNs, ctime_ns as ctimeNs, status
       from ${FILE_STATE_TABLE} where file_key = ?`,
    ).get(fileKey) as (Omit<Identity, "mtimeMs"> & { status: string }) | undefined;
    return state && sameIdentity({ ...state, mtimeMs: 0 }, stat) ? state : null;
  }

  private enqueue(candidate: Candidate) {
    this.pending.push(candidate);
    this.pendingKeys.add(candidate.fileKey);
  }

  private observeSession(root: string, groupName: string, sessionName: string, result: GrokUsageScanResult) {
    const counters = this.sweep!.counters;
    const sessionDirectory = path.join(root, groupName, sessionName);
    const file = path.join(sessionDirectory, GROK_USAGE_FILE_NAME);
    let stat: fs.BigIntStats;
    try {
      // The listing's entry type already excluded symlinked session
      // directories; the read re-validates every ancestor before opening.
      stat = fs.lstatSync(file, { bigint: true });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return; // no usage yet
      counters.statErrors += 1;
      result.statErrors += 1;
      return;
    }
    if (stat.isSymbolicLink() || !stat.isFile()) {
      counters.statErrors += 1;
      result.statErrors += 1;
      return;
    }
    counters.filesSeen += 1;
    result.filesSeen += 1;
    const fileKey = grokUsageFileKey(file);
    // Queued by the recent lane and not yet read: it is this sweep's already.
    if (this.pendingKeys.has(fileKey)) return;
    const state = this.unchanged(fileKey, stat);
    if (state) {
      counters.filesUnchanged += 1;
      result.filesUnchanged += 1;
      if (state.status !== "committed") counters.filesUnresolved += 1;
      return;
    }
    this.enqueue({ file, fileKey, groupName, sessionName, sessionDirectory, identity: identityOf(stat) });
  }

  private async drainPending(budget: CaptureWorkBudget, result: GrokUsageScanResult, signal?: AbortSignal) {
    let sinceYield = 0;
    while (this.pending.length > 0) {
      if (signal?.aborted) {
        result.aborted = true;
        return;
      }
      if (!budget.canContinue()) return;
      const candidate = this.pending[0]!;
      const outcome = this.processFile(candidate, budget, result);
      if (outcome === "deferred") return;
      this.pending.shift();
      this.pendingKeys.delete(candidate.fileKey);
      sinceYield += 1;
      if (sinceYield >= 8 && this.pending.length > 0) {
        await new Promise<void>((resolve) => setImmediate(resolve));
        sinceYield = 0;
        result.cooperativeYields += 1;
        result.lastYieldAt = new Date(this.now()).toISOString();
        budget.recordYield();
      }
    }
  }

  /**
   * Read, parse and commit one document inside the shared budget. Returns
   * `deferred` when the budget cannot admit it now (it stays pending), and
   * `done` otherwise, including every refusal that is recorded durably.
   */
  private processFile(candidate: Candidate, budget: CaptureWorkBudget, result: GrokUsageScanResult): "done" | "deferred" {
    const counters = this.sweep!.counters;
    const size = candidate.identity.size;
    if (size > this.limits.maxFileBytes) {
      counters.filesOversized += 1;
      counters.filesUnresolved += 1;
      result.filesOversized += 1;
      result.unresolvedRecords += 1;
      result.bytesDeferred += size;
      this.recordFileState(candidate, null, "oversized");
      return "done";
    }
    const slice = budget.remainingSlice(true);
    if (!slice || size > slice.maxBytes || budget.remainingEventSlots() === 0) {
      result.bytesDeferred += size;
      return "deferred";
    }
    let bytes: Buffer;
    try {
      const read = this.readDocument(candidate);
      if (!read) {
        // Replaced or rewritten while being read: a later sweep rediscovers it.
        counters.filesDeferred += 1;
        result.filesDeferred += 1;
        return "done";
      }
      bytes = read;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return "done";
      counters.readErrors += 1;
      counters.filesUnresolved += 1;
      result.readErrors += 1;
      return "done";
    }
    result.filesRead += 1;
    result.bytesRead += bytes.length;
    counters.bytesRead += bytes.length;
    const digest = sha256(bytes);
    const previous = this.buffer.database.prepare(
      `select content_sha256 as digest, status, turns, token_bearing as tokenBearing,
         session_only_tokens as sessionOnlyTokens
       from ${FILE_STATE_TABLE} where file_key = ?`,
    ).get(candidate.fileKey) as {
      digest: string | null; status: string; turns: number; tokenBearing: number; sessionOnlyTokens: number;
    } | undefined;
    if (previous?.digest === digest && previous.status === "committed") {
      // The same bytes under a new mtime: nothing to parse.
      this.recordFileState(candidate, digest, "committed", {
        turns: previous.turns,
        tokenBearing: previous.tokenBearing === 1,
        sessionOnlyTokens: previous.sessionOnlyTokens,
      });
      budget.recordSlice({ bytesRead: bytes.length, recordsParsed: 0, eventsAppended: 0 });
      counters.filesParsed += 1;
      result.filesParsed += 1;
      return "done";
    }
    let document: ParsedDocument | null = null;
    try {
      document = parseGrokUsageDocument(JSON.parse(bytes.toString("utf8")), candidate.sessionName);
    } catch {
      document = null;
    }
    if (!document) {
      counters.parseErrors += 1;
      counters.filesUnresolved += 1;
      result.parseErrors += 1;
      this.recordFileState(candidate, digest, "parse_error");
      budget.recordSlice({ bytesRead: bytes.length, recordsParsed: 0, eventsAppended: 0 });
      return "done";
    }
    const before = mutationSnapshot(result, counters);
    let applied: ReturnType<GrokUsageTailer["applyDocument"]>;
    try {
      applied = this.applyDocument(candidate, document, digest, slice.maxRecords, budget, result);
    } catch {
      // The transaction rolled back. Like the JSONL tailers, contain the
      // failure to this document: its state did not advance, so a later
      // sweep retries it, and the other sources keep their cadence.
      restoreMutationSnapshot(result, counters, before);
      counters.readErrors += 1;
      counters.filesUnresolved += 1;
      result.readErrors += 1;
      budget.recordSlice({ bytesRead: bytes.length, recordsParsed: 0, eventsAppended: 0 });
      return "done";
    }
    budget.recordSlice({
      bytesRead: bytes.length,
      recordsParsed: applied.turnsWorked,
      eventsAppended: applied.eventsAppended,
    });
    if (!applied.complete) {
      // The budget ended inside this document. Its turn records are durable,
      // so the next pass reads it again and continues after committed turns.
      return "deferred";
    }
    if (document.invalidTurns > 0) {
      counters.parseErrors += document.invalidTurns;
      counters.filesUnresolved += 1;
      result.parseErrors += document.invalidTurns;
    }
    counters.filesParsed += 1;
    result.filesParsed += 1;
    return "done";
  }

  /** Read exactly the discovered generation through a no-follow descriptor. */
  private readDocument(candidate: Candidate): Buffer | null {
    if (!realDirectory(path.dirname(candidate.sessionDirectory)) || !realDirectory(candidate.sessionDirectory)) {
      return null;
    }
    const root = fs.realpathSync(this.sessionsRoot!);
    if (!fs.realpathSync(candidate.file).startsWith(`${root}${path.sep}`)) return null;
    const descriptor = fs.openSync(candidate.file, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW);
    try {
      const before = fs.fstatSync(descriptor, { bigint: true });
      if (!before.isFile() || !sameIdentity(candidate.identity, before)) return null;
      const size = candidate.identity.size;
      const bytes = Buffer.alloc(size);
      let offset = 0;
      while (offset < size) {
        const read = fs.readSync(descriptor, bytes, offset, size - offset, offset);
        if (read === 0) break;
        offset += read;
      }
      const after = fs.fstatSync(descriptor, { bigint: true });
      return offset === size && sameIdentity(candidate.identity, after) ? bytes : null;
    } finally {
      fs.closeSync(descriptor);
    }
  }

  private applyDocument(candidate: Candidate, document: ParsedDocument, digest: string,
    maxTurnsWorked: number, budget: CaptureWorkBudget, result: GrokUsageScanResult) {
    const database = this.buffer.database;
    let turnsWorked = 0;
    let eventsAppended = 0;
    let complete = true;
    const facts = {
      turns: document.turns.length,
      tokenBearing: document.turns.some((turn) => TOKEN_KEYS.some((key) => turn.totals[key] > 0)),
      sessionOnlyTokens: document.sessionOnlyTokens,
    };
    const liveCovered = this.buffer.sessionUsageAuthority("grok", document.sessionId) === "live";
    const cwd = grokGroupWorkingDirectory(candidate.groupName);
    const contextRequest = cwd
      ? this.buffer.repoContextOccurrenceRequest("grok", `grok-usage:${document.sessionId}`, cwd)
      : null;
    const selectTurn = database.prepare(
      `select split_mode as splitMode, streams_json as streamsJson
       from ${TURN_STATE_TABLE} where session_id = ? and turn_number = ?`,
    );
    const upsertTurn = database.prepare(
      `insert into ${TURN_STATE_TABLE}
         (session_id, turn_number, split_mode, streams_json, ended_at, revisions, updated_at)
       values (@sessionId, @turnNumber, @splitMode, @streamsJson, @endedAt, 0, @updatedAt)
       on conflict(session_id, turn_number) do update set
         streams_json = excluded.streams_json,
         ended_at = excluded.ended_at,
         revisions = ${TURN_STATE_TABLE}.revisions + 1,
         updated_at = excluded.updated_at`,
    );
    const receivedAtMs = this.now();
    const fallback = clampFutureObservedAt(
      document.updatedAt ?? new Date(candidate.identity.mtimeMs).toISOString(),
      receivedAtMs,
    );
    this.buffer.transactionWithRepoContextHandoffs(() => {
      if (liveCovered) {
        // Live usage already owns this session (first writer wins, as for
        // the Codex and Claude tailers); file rows would only be suppressed.
        result.sessionsSkippedLiveCovered += 1;
        this.recordFileState(candidate, digest, "committed", facts);
        return;
      }
      let contextStaged = false;
      for (const turn of document.turns) {
        result.recordsParsed += 1;
        const stored = selectTurn.get(document.sessionId, turn.turnNumber) as
          | { splitMode: "model" | "turn"; streamsJson: string }
          | undefined;
        let previous: TurnState | null = null;
        if (stored) {
          try {
            previous = {
              splitMode: stored.splitMode,
              streams: JSON.parse(stored.streamsJson) as Record<string, CountedStream>,
            };
          } catch {
            // An unreadable record cannot prove what was counted; refuse.
            result.turnRewritesRefused += 1;
            continue;
          }
        }
        const mode = previous?.splitMode ?? (modelsReconcile(turn) ? "model" : "turn");
        const streams = turnStreams(turn, mode, document.sessionPrimaryModel, document.sessionIncomplete);
        const plan = streams ? planTurn(streams, previous) : "refused";
        if (plan === "refused") {
          // A rewrite that lowers a counted number, or a per-model turn
          // rewritten without per-model rows, cannot be mapped onto what was
          // counted. The earlier count stands and the refusal is reported.
          result.turnRewritesRefused += 1;
          continue;
        }
        if (previous && plan.emits.length === 0) continue; // already counted
        if (turnsWorked >= maxTurnsWorked ||
          budget.remainingEventSlots() - eventsAppended < plan.emits.length) {
          complete = false;
          break;
        }
        turnsWorked += 1;
        const observed = clampFutureObservedAt(turn.endedAt ?? undefined, receivedAtMs);
        const observedAt = observed.observedAt ?? fallback.observedAt!;
        const clamped = observed.observedAt === undefined ? fallback.clamped : observed.clamped;
        if (contextRequest && plan.emits.length > 0 && !contextStaged) {
          this.buffer.stageRepoContextRequest(contextRequest);
          contextStaged = true;
        }
        for (const emit of plan.emits) {
          const event = buildGrokUsageEvent(document.sessionId, turn.turnNumber, observedAt, emit);
          if (contextRequest && !attachRepoContextId(event, contextRequest.contextId)) {
            throw new Error("grok_usage_repo_context_binding_failed");
          }
          const appended = this.buffer.append(event, [], { integrityReceipt: true });
          if (appended.enrollmentRejected) {
            // Counter state still advances: history outside the enrolled
            // window is never retried into a later observation.
            result.enrollmentExcludedEvents += 1;
            continue;
          }
          if (!appended.appended) continue;
          eventsAppended += 1;
          result.eventsAppended += 1;
          this.sweep!.counters.eventsAppended += 1;
          if (emit.stream.labelIncomplete) result.incompleteEvents += 1;
          if (clamped) result.futureTimestampClampedEvents += 1;
          for (const key of TOKEN_KEYS) result.tokensAppended[key] += emit.delta[key];
          result.costUsdTicksAppended += emit.delta.costTicks ?? 0;
        }
        if (previous) result.turnsRevised += 1;
        upsertTurn.run({
          sessionId: document.sessionId,
          turnNumber: turn.turnNumber,
          splitMode: mode,
          streamsJson: JSON.stringify(plan.counted),
          endedAt: turn.endedAt,
          updatedAt: new Date(receivedAtMs).toISOString(),
        });
        result.recordsCommitted += 1;
      }
      this.recordFileState(
        candidate,
        complete ? digest : null,
        !complete ? "partial" : document.invalidTurns > 0 ? "parse_error" : "committed",
        facts,
      );
    });
    return { turnsWorked, eventsAppended, complete };
  }

  private recordFileState(candidate: Candidate, digest: string | null,
    status: "committed" | "partial" | "parse_error" | "oversized",
    facts: { turns: number; tokenBearing: boolean; sessionOnlyTokens: number } =
      { turns: 0, tokenBearing: false, sessionOnlyTokens: 0 }) {
    this.buffer.database.prepare(
      `insert into ${FILE_STATE_TABLE}
         (file_key, device, inode, size, mtime_ns, ctime_ns, mtime_ms, content_sha256,
          turns, token_bearing, session_only_tokens, status, updated_at)
       values (@fileKey, @device, @inode, @size, @mtimeNs, @ctimeNs, @mtimeMs, @digest,
          @turns, @tokenBearing, @sessionOnlyTokens, @status, @updatedAt)
       on conflict(file_key) do update set
         device = excluded.device, inode = excluded.inode, size = excluded.size,
         mtime_ns = excluded.mtime_ns, ctime_ns = excluded.ctime_ns, mtime_ms = excluded.mtime_ms,
         content_sha256 = excluded.content_sha256, turns = excluded.turns,
         token_bearing = excluded.token_bearing, session_only_tokens = excluded.session_only_tokens,
         status = excluded.status, updated_at = excluded.updated_at`,
    ).run({
      fileKey: candidate.fileKey,
      device: candidate.identity.device,
      inode: candidate.identity.inode,
      size: candidate.identity.size,
      // A partial document keeps an identity no discovery will match, so the
      // next sweep reads it again and resumes after its committed turns.
      mtimeNs: status === "partial" ? "partial" : candidate.identity.mtimeNs,
      ctimeNs: candidate.identity.ctimeNs,
      mtimeMs: candidate.identity.mtimeMs,
      digest,
      turns: facts.turns,
      tokenBearing: facts.tokenBearing ? 1 : 0,
      sessionOnlyTokens: facts.sessionOnlyTokens,
      status,
      updatedAt: new Date(this.now()).toISOString(),
    });
  }

  private finishSweep(scanNow: Date, result: GrokUsageScanResult) {
    const sweep = this.sweep!;
    const counters = sweep.counters;
    // Per-pass caps only move work to a later pass of the same sweep, and
    // errors other workers of this sweep saw are durable in `unclean`.
    const clean = !sweep.unclean && sweepClean(counters);
    const database = this.buffer.database;
    const marker = grokUsageBackfillMarker(database) ?? {
      version: 1 as const, startedAt: sweep.startedAt, completedAt: null, sweeps: 0, lastSweep: null, completion: null,
    };
    const completedAt = scanNow.toISOString();
    const next: GrokUsageBackfillMarker = {
      ...marker,
      sweeps: marker.sweeps + 1,
      lastSweep: { ...counters, completedAt, clean },
      completedAt: marker.completedAt ?? (clean ? completedAt : null),
      completion: marker.completion ?? (clean ? { ...counters, completedAt } : null),
    };
    database.transaction(() => {
      writeMaintenanceState(database, GROK_USAGE_BACKFILL_KEY, JSON.stringify(next));
      // The next sweep begins at its first pass, from the first group.
      writeWalkRound(database, { round: sweep.round + 1, startedAtMs: null, recentAtMs: null, unclean: false, cursor: null });
    })();
    sweep.stateChanged = false;
    result.exhaustive = clean;
    // The round has moved on: writing this sweep's state now would undo it.
    this.closeHandles(sweep);
    this.lastSweep = sweep;
    this.sweep = null;
  }

  /** The sweep most recently finished by this tailer, for its receipt. */
  private lastSweep: Sweep | null = null;

  private publishActivity(result: GrokUsageScanResult, scanNow: Date, entriesThisTick: number, deferredBeforeIo: boolean) {
    const dayStart = Date.parse(`${scanNow.toISOString().slice(0, 10)}T00:00:00.000Z`);
    let activity: { filesToday: number; lastMs: number | null } = { filesToday: 0, lastMs: null };
    let sessionOnlyTokens = 0;
    try {
      // Local activity is every usage file that carries tokens or that the
      // tailer could not account for; a committed file without tokens is not.
      activity = this.buffer.database.prepare(
        `select coalesce(sum(case when mtime_ms >= ? then 1 else 0 end), 0) as filesToday,
           max(mtime_ms) as lastMs
         from ${FILE_STATE_TABLE} where token_bearing = 1 or status <> 'committed'`,
      ).get(dayStart) as { filesToday: number; lastMs: number | null };
      sessionOnlyTokens = (this.buffer.database.prepare(
        `select coalesce(sum(session_only_tokens), 0) as total from ${FILE_STATE_TABLE}`,
      ).get() as { total: number }).total;
    } catch {
      // Activity is advisory; the scan result already carries its counters.
    }
    result.activity.filesToday = activity.filesToday;
    result.activity.lastActivityAt = activity.lastMs === null
      ? null
      : new Date(Math.min(activity.lastMs, scanNow.getTime())).toISOString();
    const sweep = this.sweep ?? this.lastSweep;
    const counters = sweep?.counters ?? zeroCounters();
    const rootReady = this.sessionsRoot !== null && realDirectory(this.sessionsRoot) !== null;
    result.home = this.sessionsRoot === null ? "invalid" : rootReady ? "ready" : "absent";
    const progress = captureScanProgress({
      // No sweep yet is no receipt: it must not read as a finished sweep.
      discovery: sweep ? {
        rootsTotal: 1,
        rootsStarted: sweep.entries > 0 || sweep.done ? 1 : 0,
        openDirectories: [sweep.recent.root, sweep.recent.handle].filter(Boolean).length,
        entriesVisited: sweep.entries,
        lifetimeEntryLimit: this.limits.lifetimeEntryLimit,
        limitReached: false,
        finished: sweep.done,
        origin: 0,
        nextRootIndex: 0,
      } : null,
      configuredRoots: 1,
      eligibleRoots: rootReady ? 1 : 0,
      pendingFiles: this.pending.length,
      entriesThisTick,
      deferredBeforeIo,
      lifetimeEntryLimit: this.limits.lifetimeEntryLimit,
    });
    result.activity.scan = {
      ...progress,
      // This walk's own allowance, not the JSONL cursor's corpus-sized one.
      entryBudgetPerTick: this.limits.entriesPerPass,
      wallBudgetMsPerTick: this.limits.discoveryWallMs,
      usageFiles: {
        sweepStartedAt: sweep?.startedAt ?? null,
        seen: counters.filesSeen,
        unchanged: counters.filesUnchanged,
        parsed: counters.filesParsed,
        deferred: counters.filesDeferred + this.pending.length,
        oversized: counters.filesOversized,
        unresolved: counters.filesUnresolved,
        errors: counters.parseErrors + counters.discoveryErrors + counters.statErrors + counters.readErrors,
        sessionOnlyTokens,
        groupsOverLimit: counters.groupsOverLimit,
        sessionsOverLimit: counters.sessionsOverLimit,
        recentFiles: counters.recentFiles,
      },
    };
  }
}
