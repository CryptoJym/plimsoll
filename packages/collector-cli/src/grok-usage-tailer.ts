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
 *  - Walk order: a sweep is a round over every group and session that
 *    existed when it began. It first checks the most recently modified
 *    groups for usage files written in the last two days, so today's tokens
 *    arrive first. It then streams sessions/ and each unfinished group in
 *    directory order, one entry per step, each step admitted by the shared
 *    budget. Every session it observes is recorded in a durable visited set
 *    keyed by name hashes, so a restarted worker skips what the round
 *    already covered and continues, and a per-pass cap only moves work to
 *    the next pass. An entry created after the round began waits for the
 *    next round, so churn cannot keep crowding out an older session.
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
   * moves the rest to a later pass of the same sweep and is counted as over
   * limit; nothing is dropped.
   */
  maxGroups: 4_096,
  maxSessionsPerGroup: 100_000,
  /**
   * Discovery steps per pass: opening a directory, or examining one entry
   * the sweep has not yet covered (at most two lstats). Reading past covered
   * entries costs no step; it is bounded by 32 reads a step and, after the
   * walk's first unit of the pass, by the wall.
   */
  entriesPerPass: 2_048,
  discoveryWallMs: 50,
  pendingFiles: 64,
  /** Reported with the scan; a sweep is bounded by the entries it began with. */
  lifetimeEntryLimit: 200_000,
  /**
   * Rows the durable walk state may hold: one per finished group, and one per
   * covered session of a group still being walked. A sweep that reaches it
   * ends early, reported incomplete, and the next sweep starts over.
   */
  maxWalkStateRows: 200_000,
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
/** Which sweep is in progress, and when it began. Path- and content-free. */
const WALK_ROUND_KEY = "grok_usage_walk_round_v1";
/** What the sweep in progress has covered, as hashes of group and session names. */
const WALK_VISITS_TABLE = "grok_usage_walk_visits";
/** The session hash that records a whole group as covered for the sweep. */
const GROUP_COVERED = "";
/** Entries a pass may read past, per step of its allowance. */
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

/** The group a walk is streaming: its covered sessions and this pass's count. */
type WalkGroup = {
  name: string;
  hash: string;
  handle: fs.Dir;
  covered: Set<string>;
  /** Stopped by the per-pass cap: the rest of the listing is only counted. */
  capped: boolean;
};

type Sweep = {
  /** The sweep in progress and when it began; newer entries wait for the next. */
  round: number;
  startedAtMs: number;
  startedAt: string;
  /** When a recent lane last started in this sweep, by any worker. Durable. */
  recentAtMs: number | null;
  /** Rows of the durable walk state. */
  rows: number;
  /**
   * Durable: a pass of this sweep saw an error or a worker closed with a
   * queued file unread. Counters live in one worker; this does not.
   */
  unclean: boolean;
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
  root: fs.Dir | null;
  group: WalkGroup | null;
  /** This stream of sessions/ left a group unfinished, so the sweep goes on. */
  streamIncomplete: boolean;
  /** Steps since sessions/ was last opened: a stream without any is stalled. */
  streamSteps: number;
  coveredGroups: Set<string>;
  /** Coverage recorded this pass, written with the pass. */
  unwritten: Array<[string, string]>;
  /** `recentAtMs` or `unclean` changed and is written with the pass. */
  stateChanged: boolean;
  /** The walk state passed `maxWalkStateRows`: the scan ends this sweep early. */
  walkStateFull: boolean;
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
  /** Groups and sessions the walk covered this pass. */
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
  /** Groups and sessions created after the sweep began, left to the next sweep. */
  groupsDeferred: number;
  sessionsDeferred: number;
  /** Usage files the recent lane queued ahead of the walk this sweep. */
  recentFiles: number;
  /** Rows the durable walk state holds now (bounded by `maxWalkStateRows`). */
  walkStateRows: number;
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
    groupsOverLimit: 0, sessionsOverLimit: 0, groupsDeferred: 0, sessionsDeferred: 0, recentFiles: 0,
    walkStateLimitReached: 0,
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
 * when a recent lane last started in it, and how many rows its walk state
 * holds (null when a round-3 state did not count them).
 */
type WalkRound = {
  round: number;
  startedAtMs: number | null;
  recentAtMs: number | null;
  rows: number | null;
  /** A pass of this sweep, by any worker, saw an error or left a queued file unread. */
  unclean: boolean;
};

function readWalkRound(database: Database.Database): WalkRound | null {
  try {
    const parsed = JSON.parse(readMaintenanceState(database, WALK_ROUND_KEY) ?? "null") as
      { version?: unknown; round?: unknown; startedAtMs?: unknown; recentAtMs?: unknown; rows?: unknown; unclean?: unknown } | null;
    if (parsed?.version !== 1 || !Number.isSafeInteger(parsed.round) || (parsed.round as number) < 1) return null;
    const count = (value: unknown) => Number.isSafeInteger(value) && (value as number) >= 0 ? value as number : null;
    const startedAtMs = count(parsed.startedAtMs);
    if (parsed.startedAtMs !== null && startedAtMs === null) return null;
    return {
      round: parsed.round as number, startedAtMs, recentAtMs: count(parsed.recentAtMs), rows: count(parsed.rows),
      unclean: parsed.unclean === true,
    };
  } catch {
    return null;
  }
}

function writeWalkRound(database: Database.Database, round: WalkRound) {
  writeMaintenanceState(database, WALK_ROUND_KEY, JSON.stringify({ version: 1, ...round }));
}

/** When a directory was created, in ms; 0 where the filesystem does not say. */
function bornAtMs(stat: fs.BigIntStats) {
  return Number(stat.birthtimeNs / 1_000_000n);
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
    create table if not exists ${WALK_VISITS_TABLE} (
      group_hash text not null,
      session_hash text not null,
      primary key (group_hash, session_hash)
    ) without rowid;
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
    // A queued file this worker never read may belong to a session the walk
    // already covered: this sweep can no longer claim it read everything.
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
      this.writeCoverage(sweep);
    } catch {
      // Coverage that was not written is walked again; nothing is lost.
    }
    this.closeHandles(sweep);
  }

  /** Close a sweep's directory streams without writing its walk state. */
  private closeHandles(sweep: Sweep) {
    closeDirectory(sweep.root);
    closeDirectory(sweep.recent.root);
    closeDirectory(sweep.recent.handle);
    closeDirectory(sweep.group?.handle ?? null);
    sweep.root = null;
    sweep.recent.root = null;
    sweep.recent.handle = null;
    sweep.group = null;
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
      // A sweep whose walk state is full ends early, reported incomplete;
      // what it left queued is read under the next one.
      if (this.sweep?.walkStateFull) this.finishSweep(scanNow, result, true);
      else if (this.sweep && this.sweep.done && this.pending.length === 0) this.finishSweep(scanNow, result);
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
    // A restarted worker continues the sweep in progress: the round, what it
    // covered and when its recent lane last started are durable, so nothing
    // already covered is walked again and a replaced worker does not rebuild
    // the recent lane before it is due. A sweep begins at its first pass, and
    // everything created before then belongs to it.
    let round = readWalkRound(database);
    if (round?.startedAtMs == null) {
      const fresh: WalkRound = {
        round: round?.round ?? 1, startedAtMs: scanNow.getTime(), recentAtMs: null, rows: 0, unclean: false,
      };
      database.transaction(() => {
        database.prepare(`delete from ${WALK_VISITS_TABLE}`).run();
        writeWalkRound(database, fresh);
      })();
      round = fresh;
    }
    const startedAtMs = round.startedAtMs!;
    const recentDue = round.recentAtMs === null ||
      scanNow.getTime() - round.recentAtMs >= this.limits.recentRefreshMs;
    this.sweep = {
      round: round.round,
      startedAtMs,
      startedAt: new Date(startedAtMs).toISOString(),
      recentAtMs: round.recentAtMs,
      rows: round.rows ?? Number((database.prepare(
        `select count(*) as count from ${WALK_VISITS_TABLE}`,
      ).get() as { count: number }).count),
      unclean: round.unclean,
      recent: this.recentLane(!recentDue),
      root: null,
      group: null,
      streamIncomplete: false,
      streamSteps: 0,
      coveredGroups: new Set((database.prepare(
        `select group_hash as hash from ${WALK_VISITS_TABLE} where session_hash = ?`,
      ).all(GROUP_COVERED) as Array<{ hash: string }>).map((row) => row.hash)),
      unwritten: [],
      stateChanged: round.rows === null,
      walkStateFull: false,
      entries: 0,
      done: false,
      counters: zeroCounters(),
    };
  }

  private recentLane(done = false): Sweep["recent"] {
    return {
      root: null, rootOpened: false, ranked: false, groups: [], index: 0, handle: null, observed: 0,
      done: done || this.limits.recentGroups <= 0,
    };
  }

  /**
   * Bounded discovery. A step opens a directory or examines one entry the
   * sweep has not covered (at most two lstats); reading past covered
   * entries costs no step. No step lists, stats or sorts a whole directory
   * tree. A pass is admitted while the shared budget and the pending queue
   * have room, and it starts with the walk's progress unit: the walk runs
   * until it has covered one group or session, whatever this pass's wall or
   * the cadence clock, so a worker replaced after every pass still moves the
   * sweep (eco-6hoxj.163.42 round 4). That unit opens at most the root and
   * one group and examines at most one session, within the pass's step and
   * read allowances. After it, every step and read is admitted only while
   * the budget, the pass's wall and allowances and the pending queue all
   * have room, and the recent lane gets at most half of the pass's wall and
   * of its steps.
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
    if (sweep.recent.done && sweep.recentAtMs !== null && nowMs - sweep.recentAtMs >= this.limits.recentRefreshMs) {
      // A long sweep checks the newest groups again, so fresh usage keeps
      // arriving ahead of the walk.
      sweep.recent = this.recentLane();
    }
    try {
      while (!sweep.done && !pass.stalled && pass.steps < this.limits.entriesPerPass &&
        pass.reads < this.limits.entriesPerPass * READS_PER_STEP && !signal?.aborted &&
        (pass.covered === 0 || (this.pending.length < this.limits.pendingFiles && budget.canContinue() &&
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
      this.writeCoverage(sweep);
    }
    sweep.entries += pass.steps;
    result.activity.discoveryEntries += pass.steps;
    // A sweep that churn or a vast tree has grown past the walk state's
    // limit ends early, once this scan has read what it queued.
    if (sweep.rows > this.limits.maxWalkStateRows) sweep.walkStateFull = true;
    return pass.steps;
  }

  /**
   * Record this pass's coverage durably, in one transaction, with the
   * round. A finished group keeps only its own row, since its sessions are
   * not read again this sweep: the walk state holds one row per finished
   * group plus the covered sessions of groups still being walked.
   */
  private writeCoverage(sweep: Sweep) {
    if (!sweep.unclean && !sweepClean(sweep.counters)) {
      sweep.unclean = true;
      sweep.stateChanged = true;
    }
    if (sweep.unwritten.length === 0 && !sweep.stateChanged) return;
    const rows = sweep.unwritten;
    sweep.unwritten = [];
    sweep.stateChanged = false;
    const database = this.buffer.database;
    const insert = database.prepare(
      `insert or ignore into ${WALK_VISITS_TABLE} (group_hash, session_hash) values (?, ?)`,
    );
    const compact = database.prepare(
      `delete from ${WALK_VISITS_TABLE} where group_hash = ? and session_hash <> ?`,
    );
    sweep.rows = database.transaction(() => {
      let count = sweep.rows;
      for (const [group, session] of rows) count += insert.run(group, session).changes;
      for (const [group, session] of rows) {
        if (session === GROUP_COVERED) count -= compact.run(group, GROUP_COVERED).changes;
      }
      writeWalkRound(database, {
        round: sweep.round, startedAtMs: sweep.startedAtMs, recentAtMs: sweep.recentAtMs, rows: count,
        unclean: sweep.unclean,
      });
      return count;
    })();
  }

  private cover(sweep: Sweep, pass: WalkPass, group: string, session: string) {
    sweep.unwritten.push([group, session]);
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
   * The walk. It streams sessions/ and every group the sweep has not
   * covered in directory order. A group's covered sessions are skipped with
   * one read each; an uncovered session costs one step. A group or session
   * created after the sweep began waits for the next sweep. When a stream
   * of sessions/ reaches its end having left nothing unfinished, the sweep
   * is complete.
   */
  private walkStep(root: string, result: GrokUsageScanResult, pass: WalkPass) {
    const sweep = this.sweep!;
    if (sweep.group) {
      this.walkGroupStep(root, result, pass, sweep.group);
      return;
    }
    if (!sweep.root) {
      pass.steps += 1;
      if (!realDirectory(root)) {
        // No Grok sessions on this host: an empty, finished sweep.
        sweep.done = true;
        return;
      }
      try {
        sweep.root = fs.opendirSync(root);
      } catch {
        this.discoveryError(result);
        sweep.done = true;
        return;
      }
      sweep.streamIncomplete = false;
      sweep.streamSteps = 0;
      return;
    }
    let entry: fs.Dirent | null = null;
    pass.reads += 1;
    try {
      entry = sweep.root.readSync();
    } catch {
      this.discoveryError(result);
      sweep.streamIncomplete = true;
    }
    if (!entry) {
      closeDirectory(sweep.root);
      sweep.root = null;
      if (!sweep.streamIncomplete) sweep.done = true;
      // Nothing more this pass can do: every group left is waiting on a
      // per-pass cap, or this stream found no step to take.
      else if (sweep.streamSteps === 0 || pass.groupsOpened >= this.limits.maxGroups) pass.stalled = true;
      return;
    }
    if (!entry.isDirectory() || entry.name.startsWith(".")) return;
    const hash = walkHash(entry.name);
    if (sweep.coveredGroups.has(hash)) return;
    if (pass.groupsOpened >= this.limits.maxGroups) {
      sweep.counters.groupsOverLimit += 1;
      sweep.streamIncomplete = true;
      return;
    }
    if (pass.observedByGroup.get(hash) === -1) {
      // Capped earlier in this pass: the rest of it belongs to a later pass.
      sweep.streamIncomplete = true;
      return;
    }
    pass.steps += 1;
    sweep.streamSteps += 1;
    const directory = path.join(root, entry.name);
    const stat = realDirectory(directory);
    if (stat && bornAtMs(stat) > sweep.startedAtMs) {
      sweep.counters.groupsDeferred += 1;
      sweep.coveredGroups.add(hash);
      this.cover(sweep, pass, hash, GROUP_COVERED);
      return;
    }
    let handle: fs.Dir | null = null;
    try {
      if (!stat) throw new Error("grok_usage_group_not_directory");
      handle = fs.opendirSync(directory);
    } catch {
      this.discoveryError(result);
      sweep.coveredGroups.add(hash);
      this.cover(sweep, pass, hash, GROUP_COVERED);
      return;
    }
    pass.groupsOpened += 1;
    sweep.group = {
      name: entry.name,
      hash,
      handle,
      covered: new Set((this.buffer.database.prepare(
        `select session_hash as hash from ${WALK_VISITS_TABLE} where group_hash = ?`,
      ).all(hash) as Array<{ hash: string }>).map((row) => row.hash)),
      capped: false,
    };
  }

  private walkGroupStep(root: string, result: GrokUsageScanResult, pass: WalkPass, group: WalkGroup) {
    const sweep = this.sweep!;
    let entry: fs.Dirent | null = null;
    pass.reads += 1;
    try {
      entry = group.handle.readSync();
    } catch {
      this.discoveryError(result);
    }
    if (!entry) {
      closeDirectory(group.handle);
      sweep.group = null;
      if (group.capped) {
        sweep.streamIncomplete = true;
      } else {
        // Covered for this sweep. A listing error was counted: the sweep is
        // not clean, and the next sweep reads the group again.
        sweep.coveredGroups.add(group.hash);
        this.cover(sweep, pass, group.hash, GROUP_COVERED);
      }
      return;
    }
    if (!entry.isDirectory() || entry.name.startsWith(".")) return;
    const hash = walkHash(entry.name);
    if (group.covered.has(hash)) return;
    if (group.capped) {
      sweep.counters.sessionsOverLimit += 1;
      return;
    }
    const observed = pass.observedByGroup.get(group.hash) ?? 0;
    if (observed >= this.limits.maxSessionsPerGroup) {
      // The per-pass cap: count what is left, then leave it to a later pass.
      group.capped = true;
      pass.observedByGroup.set(group.hash, -1);
      sweep.counters.sessionsOverLimit += 1;
      return;
    }
    // One step: when the session directory was created, then its usage file.
    pass.steps += 1;
    sweep.streamSteps += 1;
    group.covered.add(hash);
    this.cover(sweep, pass, group.hash, hash);
    const stat = realDirectory(path.join(root, group.name, entry.name));
    if (!stat) return;
    if (bornAtMs(stat) > sweep.startedAtMs) {
      sweep.counters.sessionsDeferred += 1;
      return;
    }
    pass.observedByGroup.set(group.hash, observed + 1);
    this.observeSession(root, group.name, entry.name, result);
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

  /**
   * End the sweep: when its walk is done, or early when its walk state has
   * reached `maxWalkStateRows`. An early end is reported incomplete
   * (`walkStateLimitReached`), and the next sweep starts over with every
   * entry present at its first pass.
   */
  private finishSweep(scanNow: Date, result: GrokUsageScanResult, walkStateLimitReached = false) {
    const sweep = this.sweep!;
    const counters = sweep.counters;
    if (walkStateLimitReached) counters.walkStateLimitReached = 1;
    // Per-pass caps only move work to a later pass of the same sweep, and
    // entries created after it began belong to the next: neither leaves an
    // entry the sweep began with unexamined.
    // Errors other workers saw in this sweep are durable in `unclean`.
    const clean = !walkStateLimitReached && !sweep.unclean && sweepClean(counters);
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
    writeMaintenanceState(database, GROK_USAGE_BACKFILL_KEY, JSON.stringify(next));
    // The next sweep begins at its first pass, so everything created until
    // then is part of it. Its coverage starts empty.
    sweep.unwritten = [];
    sweep.stateChanged = false;
    sweep.rows = 0;
    database.transaction(() => {
      database.prepare(`delete from ${WALK_VISITS_TABLE}`).run();
      writeWalkRound(database, { round: sweep.round + 1, startedAtMs: null, recentAtMs: null, rows: 0, unclean: false });
    })();
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
        openDirectories: [sweep.root, sweep.recent.root, sweep.recent.handle, sweep.group?.handle].filter(Boolean).length,
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
        groupsDeferred: counters.groupsDeferred,
        sessionsDeferred: counters.sessionsDeferred,
        recentFiles: counters.recentFiles,
        walkStateRows: this.sweep?.rows ?? 0,
      },
    };
  }
}
