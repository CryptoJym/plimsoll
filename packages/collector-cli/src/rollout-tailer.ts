import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { LocalEventBuffer } from "./buffer";
import {
  attachRepoContextId,
  validRepoContextId,
  type RepoContextRequest,
} from "./repo-context";
import {
  DEFAULT_JSONL_TAILER_IO,
  ensureJsonlScanState,
  loadJsonlScanCursor,
  rememberJsonlScanCursor,
  type JsonlScanCursor,
  type JsonlTailerIo,
} from "./jsonl-byte-tailer";
import {
  AUTOMATIC_DISCOVERY_ENTRY_CAP,
  AUTOMATIC_DISCOVERY_WALL_MS,
  AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP,
  beginAutomaticCaptureBaseline,
  captureBaselineStatus,
  classifyCaptureBaselineFile,
  completeAutomaticCaptureBaseline,
  recordAutomaticCaptureBaselineProgress,
  resolveAutomaticCaptureBaselinePending,
  stageAutomaticCaptureBaselineObservation,
  stageAutomaticCaptureBaselinePending,
  type CaptureBaselineFileObservation,
} from "./capture-baseline";
import { CaptureWorkBudget, type CaptureBudgetStatus } from "./capture-work-budget";
import { IncrementalJsonlDiscovery } from "./incremental-jsonl-discovery";
import {
  maintenanceCandidateHash,
  type MaintenanceProgressStage,
} from "./maintenance-progress";
import { readLocalIdentities, type LocalIdentity, type LocalIdentityPaths } from "./local-identity";
import { deterministicEventId } from "./normalizer";
import {
  aiInteractionEventSchema,
  estimateCostUsd,
  type AiInteractionEvent,
} from "../../shared/src/index";

/**
 * Codex rollout tailer (issue 0022). OTLP usage spans only arrive from some
 * codex frontends (`codex exec` yes; app-server-driven sessions no) — on
 * 2026-06-10 that meant 1 of 11 sessions captured. Codex always writes
 * rollouts (`~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<uuid>.jsonl`) whose
 * `token_count` lines carry cumulative `total_token_usage`, so the rollout
 * file is the codex source of truth and OTLP stays as the low-latency path.
 *
 * Mechanics, verified against live codex 0.137 rollouts:
 *  - Per-event deltas come from TELESCOPING the cumulative totals (totals are
 *    strictly monotonic across 328 token_counts in a 35MB live file, while
 *    sum(last_token_usage) overcounts by ~128K tokens on the same file —
 *    retries/parallel requests double-count in `last`). Sum of deltas equals
 *    the final total by construction.
 *  - Event ids are deterministic over (conversation id, token_count index),
 *    and deterministic duplicate ids are ignored, so rescans are idempotent.
 *  - Privacy: only `session_meta` / `turn_context` / `token_count` lines are
 *    even JSON-parsed; message/reasoning lines are skipped by prefilter and
 *    no content field is ever read or persisted.
 *  - Dedupe vs OTLP is first-writer-wins per session: spans arrive within
 *    seconds while the tailer lags a scan interval, so a span-emitting
 *    session always has token-bearing non-rollout events by the time the
 *    tailer sees its rollout — those sessions are skipped (and counted).
 */

export type RolloutScanResult = {
  scope: "recent" | "full";
  exhaustive: boolean;
  discoveryErrors: number;
  statErrors: number;
  readErrors: number;
  filesSeen: number;
  filesRead: number;
  filesParsed: number;
  filesReset: number;
  legacyRebuilds: number;
  checkpointRebuilds: number;
  bytesRead: number;
  bytesDeferred: number;
  sessionsSkippedOtlpCovered: number;
  eventsAppended: number;
  tokensAppended: { input: number; cachedInput: number; output: number };
  parseErrors: number;
  unresolvedRecords: number;
  recordsParsed: number;
  slicesCommitted: number;
  cooperativeYields: number;
  excludedGenerations: number;
  excludedBytes: number;
  deferredGenerations: number;
  baselinePendingMetadataPeak?: number;
  aborted: boolean;
  lastYieldAt: string | null;
  automaticBudget: CaptureBudgetStatus | null;
  activity: {
    lastActivityAt: string | null;
    filesToday: number;
    discoveryEntries: number;
    lastScanAt: string;
    truncated: boolean;
  };
};

type TokenTotals = { input: number; cachedInput: number; output: number; reasoningOutput: number };

export type RolloutScanOptions = {
  scope: "recent" | "full";
  now?: Date;
  discoveryLimit?: number;
  automatic?: {
    phase: "baseline" | "capture";
    budget: CaptureWorkBudget;
  };
  signal?: AbortSignal;
  quarantine?: { stage: MaintenanceProgressStage; candidateHash: string };
  onProgress?: (progress: { stage: MaintenanceProgressStage; candidateHash: string | null }) => boolean;
  deferredBeforeIo?: boolean;
};

type RolloutParserState = {
  parserKind: "codex-rollout-v2";
  checkpointVersion: 2;
  conversationId?: string;
  sessionStartedAt?: string;
  originator?: string;
  cliVersion?: string;
  model?: string;
  planType?: string;
  previous: TokenTotals;
  tokenCountIndex: number;
  contextOccurrenceIndex: number;
  activeRepoContextId?: string;
};

const PARSER_KIND = "codex-rollout-v2";
const CHECKPOINT_VERSION = 2;

const ZERO: TokenTotals = { input: 0, cachedInput: 0, output: 0, reasoningOutput: 0 };

function totalsFrom(usage: Record<string, unknown> | undefined): TokenTotals | undefined {
  if (!usage || typeof usage !== "object") return undefined;
  const n = (key: string) => {
    const value = (usage as Record<string, unknown>)[key];
    return typeof value === "number" && Number.isFinite(value) ? value : 0;
  };
  return {
    input: n("input_tokens"),
    cachedInput: n("cached_input_tokens"),
    output: n("output_tokens"),
    reasoningOutput: n("reasoning_output_tokens"),
  };
}

function diff(current: TokenTotals, previous: TokenTotals): TokenTotals {
  // Negative steps never appeared in live data; clamp anyway so a rollout
  // rewrite can only under-count, never fabricate usage.
  return {
    input: Math.max(0, current.input - previous.input),
    cachedInput: Math.max(0, current.cachedInput - previous.cachedInput),
    output: Math.max(0, current.output - previous.output),
    reasoningOutput: Math.max(0, current.reasoningOutput - previous.reasoningOutput),
  };
}

function validateRolloutParserState(value: unknown): RolloutParserState | undefined {
  if (!isRecord(value)) return undefined;
  if (
    !hasOnlyKeys(value, [
      "parserKind",
      "checkpointVersion",
      "conversationId",
      "sessionStartedAt",
      "originator",
      "cliVersion",
      "model",
      "planType",
      "previous",
      "tokenCountIndex",
      "contextOccurrenceIndex",
      "activeRepoContextId",
      // Accepted only to migrate old checkpoints without rebuilding or
      // carrying their parser-wide attribution forward.
      "git",
    ])
  ) {
    return undefined;
  }
  if (value.parserKind !== PARSER_KIND || value.checkpointVersion !== CHECKPOINT_VERSION) {
    return undefined;
  }
  if (!isTokenTotals(value.previous)) return undefined;
  if (
    typeof value.tokenCountIndex !== "number" ||
    !Number.isSafeInteger(value.tokenCountIndex) ||
    value.tokenCountIndex < -1
  ) {
    return undefined;
  }
  const contextOccurrenceIndex = value.contextOccurrenceIndex === undefined
    ? -1
    : value.contextOccurrenceIndex;
  if (
    typeof contextOccurrenceIndex !== "number" ||
    !Number.isSafeInteger(contextOccurrenceIndex) ||
    contextOccurrenceIndex < -1
  ) {
    return undefined;
  }
  const conversationId = optionalString(value.conversationId);
  const sessionStartedAt = optionalString(value.sessionStartedAt);
  const originator = optionalString(value.originator);
  const cliVersion = optionalString(value.cliVersion);
  const model = optionalString(value.model);
  const planType = optionalString(value.planType);
  if ([conversationId, sessionStartedAt, originator, cliVersion, model, planType].includes(null)) {
    return undefined;
  }
  if (conversationId && !UUID_EXACT_RE.test(conversationId)) return undefined;
  if (value.git !== undefined && !validLegacyPersistedGit(value.git)) return undefined;
  if (value.activeRepoContextId !== undefined && !validRepoContextId(value.activeRepoContextId)) {
    return undefined;
  }

  return {
    parserKind: PARSER_KIND,
    checkpointVersion: CHECKPOINT_VERSION,
    previous: { ...value.previous },
    tokenCountIndex: value.tokenCountIndex,
    contextOccurrenceIndex,
    ...(conversationId ? { conversationId: conversationId.toLowerCase() } : {}),
    ...(sessionStartedAt ? { sessionStartedAt } : {}),
    ...(originator ? { originator } : {}),
    ...(cliVersion ? { cliVersion } : {}),
    ...(model ? { model } : {}),
    ...(planType ? { planType } : {}),
    ...(typeof value.activeRepoContextId === "string"
      ? { activeRepoContextId: value.activeRepoContextId }
      : {}),
  };
}

function optionalString(value: unknown): string | undefined | null {
  if (value === undefined) return undefined;
  return typeof value === "string" ? value : null;
}

function isTokenTotals(value: unknown): value is TokenTotals {
  if (!isRecord(value)) return false;
  if (!hasOnlyKeys(value, ["input", "cachedInput", "output", "reasoningOutput"])) return false;
  return ["input", "cachedInput", "output", "reasoningOutput"].every((key) => {
    const total = value[key];
    return typeof total === "number" && Number.isSafeInteger(total) && total >= 0;
  });
}

function validLegacyPersistedGit(value: unknown) {
  if (!isRecord(value) || !hasOnlyKeys(value, ["remoteUrlHash", "branchHash", "headSha"])) {
    return false;
  }
  return ["remoteUrlHash", "branchHash", "headSha"].every(
    (key) => value[key] === undefined || typeof value[key] === "string",
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]) {
  const names = new Set(allowed);
  return Object.keys(value).every((key) => names.has(key));
}

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const UUID_EXACT_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function conversationIdFromFilename(file: string) {
  const base = path.basename(file).replace(/\.jsonl$/, "");
  const match = base.match(UUID_RE);
  return match ? match[0].toLowerCase() : undefined;
}

function baselineObservation(
  file: string,
  stat: fs.Stats,
  precise?: fs.BigIntStats,
): CaptureBaselineFileObservation {
  const identity = precise ?? fs.lstatSync(file, { bigint: true });
  return {
    path: file,
    device: identity.dev,
    inode: identity.ino,
    size: identity.size,
    birthtimeNs: identity.birthtimeNs,
  };
}

function resultMutationSnapshot(result: RolloutScanResult) {
  return {
    parseErrors: result.parseErrors,
    eventsAppended: result.eventsAppended,
    filesParsed: result.filesParsed,
    sessionsSkippedOtlpCovered: result.sessionsSkippedOtlpCovered,
    tokens: { ...result.tokensAppended },
  };
}

function restoreResultMutationSnapshot(
  result: RolloutScanResult,
  snapshot: ReturnType<typeof resultMutationSnapshot>,
) {
  result.parseErrors = snapshot.parseErrors;
  result.eventsAppended = snapshot.eventsAppended;
  result.filesParsed = snapshot.filesParsed;
  result.sessionsSkippedOtlpCovered = snapshot.sessionsSkippedOtlpCovered;
  result.tokensAppended = { ...snapshot.tokens };
}

export class RolloutTailer {
  private activeBoundaryOptions: Pick<RolloutScanOptions, "quarantine" | "onProgress"> = {};
  private baselineAttempt: {
    discovery: IncrementalJsonlDiscovery;
    pendingFiles: Array<{ file: string; stat: fs.Stats; precise: fs.BigIntStats }>;
    runId: string;
    filesDiscovered: number;
    filesValidated: number;
    cutoffMs: number;
    sweepsCompleted: number;
    newGenerationsThisSweep: number;
    capacityDeferredThisSweep: boolean;
  } | null = null;
  private captureAttempt: {
    discovery: IncrementalJsonlDiscovery;
    pendingFiles: Array<{ file: string; stat: fs.Stats; precise: fs.BigIntStats }>;
    discoveryDone: boolean;
  } | null = null;

  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly sessionsDir = path.join(os.homedir(), ".codex", "sessions"),
    private readonly identityProvider: () => LocalIdentity[] = readLocalIdentities,
    private readonly io: JsonlTailerIo = DEFAULT_JSONL_TAILER_IO,
  ) {
    ensureJsonlScanState(this.buffer.database);
  }

  close() {
    this.baselineAttempt?.discovery.close();
    this.captureAttempt?.discovery.close();
    this.baselineAttempt = null;
    this.captureAttempt = null;
  }

  private codexIdentity: LocalIdentity | undefined;

  /**
   * scope=recent limits discovery to today+yesterday (UTC) day directories.
   * Async on purpose: the collector serves HTTP on the same event loop, and
   * the first full-history walk reads thousands of files — yielding between
   * files keeps the dashboard responsive while a scan runs (owner-reported
   * freeze, sounding 0026).
   */
  async scan(options: RolloutScanOptions): Promise<RolloutScanResult> {
    this.activeBoundaryOptions = {
      quarantine: options.quarantine,
      onProgress: options.onProgress,
    };
    const scanNow = options.now ?? new Date();
    const today = scanNow.toISOString().slice(0, 10);
    const result: RolloutScanResult = {
      scope: options.scope,
      exhaustive: false,
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
        lastScanAt: scanNow.toISOString(),
        truncated: false,
      },
    };
    if (options.deferredBeforeIo) {
      result.activity.truncated = true;
      result.deferredGenerations = 1;
      result.automaticBudget = options.automatic?.budget.status() ?? null;
      return result;
    }
    try {
      this.codexIdentity = this.identityProvider().find((entry) => entry.source === "codex");
    } catch {
      this.codexIdentity = undefined;
    }
    if (this.codexIdentity?.actorHash && this.codexIdentity.email) {
      this.buffer.setAccountEmail(this.codexIdentity.actorHash, this.codexIdentity.email);
    }
    if (options.scope === "recent" && !options.automatic) {
      throw new Error("recent_rollout_scan_requires_automatic_control");
    }
    const automatic = options.scope === "recent" ? options.automatic! : null;
    const baselineBefore = captureBaselineStatus(this.buffer.database);
    const codexBaseline = baselineBefore.sources.find((source) => source.source === "codex")!;
    if (automatic?.phase === "baseline" && codexBaseline.status === "complete") {
      this.baselineAttempt?.discovery.close();
      this.baselineAttempt = null;
      result.excludedGenerations = codexBaseline.excludedGenerations;
      result.excludedBytes = codexBaseline.currentExcludedBytes;
      result.exhaustive = true;
      result.automaticBudget = automatic.budget.status();
      return result;
    }

    if (automatic?.phase === "baseline") {
      if (!this.baselineAttempt) {
        const began = beginAutomaticCaptureBaseline(this.buffer.database, "codex", {
          startedAt: scanNow.toISOString(),
          filesDiscovered: 0,
        });
        if (began.status !== "in_progress" || !began.latestRun) {
          result.exhaustive = false;
          result.automaticBudget = automatic.budget.status();
          return result;
        }
        this.baselineAttempt = {
          discovery: this.recentDiscovery(scanNow, options.discoveryLimit, options),
          pendingFiles: [],
          runId: began.latestRun.runId,
          filesDiscovered: began.latestRun.filesDiscovered,
          filesValidated: began.latestRun.filesValidated,
          cutoffMs: Date.parse(began.latestRun.startedAt),
          sweepsCompleted: 0,
          newGenerationsThisSweep: 0,
          capacityDeferredThisSweep: false,
        };
      }

      const attempt = this.baselineAttempt;
      const chunk = attempt.pendingFiles.length === 0
        ? await attempt.discovery.collect(automatic.budget, {
            signal: options.signal,
            maxFiles: AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP,
            maxEntries: AUTOMATIC_DISCOVERY_ENTRY_CAP,
            maxWallMs: AUTOMATIC_DISCOVERY_WALL_MS,
          })
        : {
            files: [], entriesVisited: 0, errors: 0, done: false,
            limitReached: false, yields: 0, lastYieldAt: null,
          };
      if (chunk.files.length > 0) {
        const pending = stageAutomaticCaptureBaselinePending(
          this.buffer.database,
          "codex",
          {
            runId: attempt.runId,
            observedAt: new Date().toISOString(),
            observations: chunk.files.map((discovered) =>
              baselineObservation(discovered.file, discovered.stat, discovered.precise)
            ),
          },
        );
        attempt.filesDiscovered = pending.filesDiscovered;
        const acceptedFiles = chunk.files.filter((_, index) => pending.accepted[index]);
        if (pending.deferred.some(Boolean)) {
          attempt.capacityDeferredThisSweep = true;
        }
        attempt.pendingFiles.push(...acceptedFiles);
      }
      result.baselinePendingMetadataPeak = Math.max(
        result.baselinePendingMetadataPeak ?? 0,
        attempt.pendingFiles.length,
      );
      if (attempt.pendingFiles.length > AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP) {
        throw new Error("automatic_baseline_pending_metadata_cap_exceeded");
      }
      result.filesSeen = chunk.files.length;
      result.activity.discoveryEntries = chunk.entriesVisited;
      result.cooperativeYields += chunk.yields;
      result.lastYieldAt = chunk.lastYieldAt;
      result.discoveryErrors += chunk.errors;
      while (attempt.pendingFiles.length > 0) {
        if (options.signal?.aborted || !automatic.budget.canContinue()) break;
        const discovered = attempt.pendingFiles[0]!;
        const file = discovered.file;
        try {
          const stat = discovered.stat;
          const cutoffFloor = BigInt(attempt.cutoffMs) * 1_000_000n;
          if (
            discovered.precise.birthtimeNs >= cutoffFloor &&
            discovered.precise.birthtimeNs < cutoffFloor + 1_000_000n
          ) throw new Error("capture_baseline_cutoff_ambiguous");
          const mtime = stat.mtime.toISOString();
          if (!result.activity.lastActivityAt || mtime > result.activity.lastActivityAt) {
            result.activity.lastActivityAt = mtime;
          }
          if (mtime.slice(0, 10) === today) result.activity.filesToday += 1;
          if (discovered.precise.birthtimeNs < cutoffFloor) {
            if (stageAutomaticCaptureBaselineObservation(this.buffer.database, "codex", {
              runId: attempt.runId,
              observedAt: new Date().toISOString(),
              observation: baselineObservation(file, stat, discovered.precise),
              filesDiscovered: attempt.filesDiscovered,
              filesValidated: attempt.filesValidated + 1,
              resolvePending: true,
            })) attempt.newGenerationsThisSweep += 1;
          } else {
            if (!resolveAutomaticCaptureBaselinePending(this.buffer.database, "codex", {
              runId: attempt.runId,
              observedAt: new Date().toISOString(),
              observation: baselineObservation(file, stat, discovered.precise),
              filesDiscovered: attempt.filesDiscovered,
              filesValidated: attempt.filesValidated + 1,
            })) throw new Error("capture_baseline_pending_identity_mismatch");
          }
          attempt.filesValidated += 1;
          attempt.pendingFiles.shift();
        } catch {
          result.statErrors += 1;
          recordAutomaticCaptureBaselineProgress(this.buffer.database, "codex", {
            runId: attempt.runId,
            updatedAt: new Date().toISOString(),
            filesDiscovered: attempt.filesDiscovered,
            filesValidated: attempt.filesValidated,
            statErrors: result.statErrors,
          });
          attempt.discovery.close();
          this.baselineAttempt = null;
          result.exhaustive = false;
          result.automaticBudget = automatic.budget.status();
          return result;
        }
      }

      if (chunk.errors > 0 || chunk.limitReached) {
        recordAutomaticCaptureBaselineProgress(this.buffer.database, "codex", {
          runId: attempt.runId,
          updatedAt: new Date().toISOString(),
          filesDiscovered: attempt.filesDiscovered,
          filesValidated: attempt.filesValidated,
          discoveryErrors: result.discoveryErrors || 1,
        });
        attempt.discovery.close();
        this.baselineAttempt = null;
        result.activity.truncated = true;
        result.automaticBudget = automatic.budget.status();
        return result;
      }

      if (!chunk.done || attempt.pendingFiles.length > 0) {
        result.aborted = Boolean(options.signal?.aborted);
        result.activity.truncated = true;
        result.deferredGenerations = Math.max(1, attempt.pendingFiles.length);
        recordAutomaticCaptureBaselineProgress(this.buffer.database, "codex", {
          runId: attempt.runId,
          updatedAt: new Date().toISOString(),
          filesDiscovered: attempt.filesDiscovered,
          filesValidated: attempt.filesValidated,
        });
        result.automaticBudget = automatic.budget.status();
        return result;
      }

      if (attempt.capacityDeferredThisSweep) {
        attempt.discovery.close();
        attempt.discovery = this.recentDiscovery(scanNow, options.discoveryLimit, options);
        attempt.capacityDeferredThisSweep = false;
        attempt.newGenerationsThisSweep = 0;
        result.activity.truncated = true;
        result.deferredGenerations = 1;
        result.automaticBudget = automatic.budget.status();
        return result;
      }

      attempt.sweepsCompleted += 1;
      if (attempt.sweepsCompleted < 2 || attempt.newGenerationsThisSweep > 0) {
        attempt.discovery.close();
        attempt.discovery = this.recentDiscovery(scanNow, options.discoveryLimit, options);
        attempt.newGenerationsThisSweep = 0;
        result.activity.truncated = true;
        result.deferredGenerations = 1;
        result.automaticBudget = automatic.budget.status();
        return result;
      }

      const completed = completeAutomaticCaptureBaseline(this.buffer.database, "codex", {
        runId: attempt.runId,
        completedAt: new Date().toISOString(),
      });
      attempt.discovery.close();
      this.baselineAttempt = null;
      result.excludedGenerations = completed.excludedGenerations;
      result.excludedBytes = completed.currentExcludedBytes;
      result.exhaustive = completed.status === "complete";
      result.automaticBudget = automatic.budget.status();
      return result;
    }

    const automaticDiscovery = automatic
      ? await this.collectAutomaticCaptureFiles(options, automatic.budget)
      : null;
    const explicitDiscovery = automatic ? null : this.discover(options);
    const discovery = automaticDiscovery ?? explicitDiscovery!;
    const discoveredFiles: Array<{ file: string; stat?: fs.Stats; precise?: fs.BigIntStats }> = automaticDiscovery
      ? automaticDiscovery.files
      : explicitDiscovery!.files.map((file) => ({ file }));
    result.activity.truncated = discovery.truncated;
    result.discoveryErrors = discovery.errors;
    result.filesSeen = discovery.files.length;
    result.activity.discoveryEntries = discovery.files.length;
    const candidates: Array<{
      file: string;
      stat: fs.Stats;
      cursor: JsonlScanCursor<RolloutParserState> | undefined;
    }> = [];
    const automaticFilesConsumed = new Set<string>();
    const consumeAutomaticFile = (file: string) => {
      if (automatic) automaticFilesConsumed.add(file);
    };

    for (let index = 0; index < discoveredFiles.length; index += 1) {
      const discovered = discoveredFiles[index]!;
      const file = discovered.file;
      if (options.signal?.aborted) {
        result.aborted = true;
        result.activity.truncated = true;
        break;
      }
      if (automatic && !automatic.budget.canContinue()) {
        result.activity.truncated = true;
        result.deferredGenerations += discoveredFiles.length - index;
        break;
      }
      const candidateHash = maintenanceCandidateHash(file);
      if (options.quarantine?.stage === "candidate_metadata" &&
        options.quarantine.candidateHash === candidateHash) {
        result.deferredGenerations += 1;
        consumeAutomaticFile(file);
        continue;
      }
      if (!discovered.stat && options.onProgress?.({ stage: "candidate_metadata", candidateHash }) === false) {
        result.deferredGenerations += discoveredFiles.length - index;
        break;
      }
      let stat: fs.Stats;
      try {
        stat = discovered.stat ?? this.regularFileStat(file);
      } catch {
        result.statErrors += 1;
        consumeAutomaticFile(file);
        continue;
      }
      const mtime = stat.mtime.toISOString();
      if (!result.activity.lastActivityAt || mtime > result.activity.lastActivityAt) {
        result.activity.lastActivityAt = mtime;
      }
      if (mtime.slice(0, 10) === today) result.activity.filesToday += 1;
      const observation = baselineObservation(file, stat, discovered.precise);
      if (automatic?.phase === "capture") {
        const decision = classifyCaptureBaselineFile(
          this.buffer.database,
          "codex",
          observation,
          { mode: "automatic", observedAt: scanNow.toISOString() },
        );
        if (decision.decision === "exclude") {
          result.excludedGenerations += 1;
          result.excludedBytes += stat.size;
          consumeAutomaticFile(file);
          continue;
        }
        if (decision.decision === "block") {
          result.statErrors += 1;
          consumeAutomaticFile(file);
          continue;
        }
      }
      if (options.scope === "full" && baselineBefore.status === "complete") {
        const decision = classifyCaptureBaselineFile(
          this.buffer.database,
          "codex",
          observation,
          { mode: "explicit_full", observedAt: scanNow.toISOString() },
        );
        if (decision.decision === "block") {
          result.statErrors += 1;
          consumeAutomaticFile(file);
          continue;
        }
      }
      const cursor = loadJsonlScanCursor<RolloutParserState>(
        this.buffer.database,
        file,
        PARSER_KIND,
        CHECKPOINT_VERSION,
        validateRolloutParserState,
      );
      candidates.push({ file, stat, cursor });
    }

    // Resume already-cursored growth first, then newest new generations. A
    // large older deferred file cannot outrank a currently growing session.
    const preferNewest = automatic ? this.nextCandidatePreference() === "newest" : false;
    candidates.sort((left, right) => {
      const leftCursor = left.cursor?.checkpointStatus === "valid" ? 1 : 0;
      const rightCursor = right.cursor?.checkpointStatus === "valid" ? 1 : 0;
      return preferNewest
        ? right.stat.mtimeMs - left.stat.mtimeMs || rightCursor - leftCursor || left.file.localeCompare(right.file)
        : rightCursor - leftCursor || right.stat.mtimeMs - left.stat.mtimeMs || left.file.localeCompare(right.file);
    });

    for (let candidateIndex = 0; candidateIndex < candidates.length; candidateIndex += 1) {
      const candidate = candidates[candidateIndex]!;
      const candidateHash = maintenanceCandidateHash(candidate.file);
      if ((options.quarantine?.stage === "jsonl_open" || options.quarantine?.stage === "jsonl_validation") &&
        options.quarantine.candidateHash === candidateHash) {
        result.deferredGenerations += 1;
        consumeAutomaticFile(candidate.file);
        continue;
      }
      if (options.onProgress?.({ stage: "jsonl_open", candidateHash }) === false) {
        result.deferredGenerations += candidates.length - candidateIndex;
        break;
      }
      if (options.signal?.aborted) {
        result.aborted = true;
        break;
      }
      if (automatic && !automatic.budget.canContinue()) break;
      let cursor = candidate.cursor;
      let countedFile = false;
      while (true) {
        if (options.signal?.aborted) {
          result.aborted = true;
          break;
        }
        const limits = automatic
          ? automatic.budget.remainingSlice()
          : { maxBytes: 128 * 1024, maxRecords: 64 };
        if (!limits) break;
        let read: NonNullable<ReturnType<JsonlTailerIo["readTail"]>>;
        try {
          const next = this.io.readTail(candidate.file, candidate.stat, cursor, limits);
          if (!next) {
            // A stable no-more-data observation is terminal for this
            // discovery candidate even though no cursor write is needed.
            consumeAutomaticFile(candidate.file);
            break;
          }
          read = next;
        } catch {
          result.readErrors += 1;
          // The failure is explicit and surfaced; a later discovery sweep may
          // retry it without pinning the current bounded candidate batch.
          consumeAutomaticFile(candidate.file);
          break;
        }
        if (!countedFile) {
          result.filesRead += 1;
          countedFile = true;
        }
        result.bytesRead += read.bytesRead;
        result.recordsParsed += read.lines.length;
        if (read.reset) result.filesReset += 1;
        if (read.legacyRebuild) result.legacyRebuilds += 1;
        if (read.checkpointRebuild) result.checkpointRebuilds += 1;

        const before = resultMutationSnapshot(result);
        let parseFailure = false;
        let committed = false;
        let validationDeferred = false;
        try {
          const initialState = read.reset || !cursor?.parserState
            ? this.initialParserState(candidate.file)
            : cursor.parserState;
          // Git work is deferred until after capture. Validate the open
          // generation immediately before the event/cursor transaction so a
          // changed source never advances durable truth.
          if (options.onProgress?.({ stage: "jsonl_validation", candidateHash }) === false) {
            validationDeferred = true;
            throw new Error("maintenance_progress_budget_exhausted");
          }
          const fallbackObservedAt = this.fallbackObservedAt(read.mtimeMs);
          read.assertStableForCommit();
          this.buffer.transactionWithRepoContextHandoffs(() => {
            if (read.unresolvedRecord) {
              rememberJsonlScanCursor(
                this.buffer.database,
                candidate.file,
                PARSER_KIND,
                CHECKPOINT_VERSION,
                read,
                initialState,
              );
              return;
            }
            const parseErrorsBefore = result.parseErrors;
            const parserState = this.ingestLines(
              read.lines,
              result,
              initialState,
              fallbackObservedAt,
              read.fileIdentity,
            );
            if (result.parseErrors !== parseErrorsBefore) {
              parseFailure = true;
              throw new Error("rollout_slice_parse_failed");
            }
            rememberJsonlScanCursor(
              this.buffer.database,
              candidate.file,
              PARSER_KIND,
              CHECKPOINT_VERSION,
              read,
              parserState,
            );
          });
          committed = true;
          // Only a durable cursor commit transfers ownership away from the
          // active discovery attempt. Validation deferral keeps it pending.
          consumeAutomaticFile(candidate.file);
          result.slicesCommitted += 1;
          if (read.unresolvedRecord) result.unresolvedRecords += 1;
        } catch {
          const parseErrors = result.parseErrors - before.parseErrors;
          restoreResultMutationSnapshot(result, before);
          if (parseFailure) {
            result.parseErrors += Math.max(1, parseErrors);
            consumeAutomaticFile(candidate.file);
          } else if (!validationDeferred) {
            result.readErrors += 1;
            consumeAutomaticFile(candidate.file);
          } else {
            result.activity.truncated = true;
          }
        } finally {
          read.close();
        }
        const appended = committed ? result.eventsAppended - before.eventsAppended : 0;
        automatic?.budget.recordSlice({
          bytesRead: read.bytesRead,
          recordsParsed: read.lines.length,
          eventsAppended: appended,
        });
        if (!committed || read.unresolvedRecord || parseFailure || !read.workRemaining) break;

        await new Promise<void>((resolve) => setImmediate(resolve));
        result.cooperativeYields += 1;
        result.lastYieldAt = new Date().toISOString();
        automatic?.budget.recordYield();
        cursor = loadJsonlScanCursor<RolloutParserState>(
          this.buffer.database,
          candidate.file,
          PARSER_KIND,
          CHECKPOINT_VERSION,
          validateRolloutParserState,
        );
        if (automatic && !automatic.budget.canContinue()) break;
      }
    }
    if (automatic) this.consumeAutomaticCaptureFiles(automaticFilesConsumed);

    // Deferred truth is computed once from durable cursors, never summed per
    // slice. Excluded generations are reported separately and are not fake
    // cursor backlog.
    for (const candidate of candidates) {
      const cursor = loadJsonlScanCursor<RolloutParserState>(
        this.buffer.database,
        candidate.file,
        PARSER_KIND,
        CHECKPOINT_VERSION,
        validateRolloutParserState,
      );
      const deferred = Math.max(0, candidate.stat.size - (cursor?.committedOffset ?? 0));
      result.bytesDeferred += deferred;
      if (deferred > 0) result.deferredGenerations += 1;
    }
    result.exhaustive =
      !result.activity.truncated &&
      result.discoveryErrors === 0 &&
      result.statErrors === 0 &&
      result.readErrors === 0 &&
      result.parseErrors === 0 &&
      result.unresolvedRecords === 0 &&
      result.bytesDeferred === 0 &&
      !result.aborted;
    result.automaticBudget = automatic?.budget.status() ?? null;
    if (automatic && discovery.truncated) result.exhaustive = false;
    return result;
  }

  private recentDiscovery(now: Date, limit?: number, _options?: RolloutScanOptions) {
    const roots = [0, 1].map((offset) => {
      const day = new Date(now.getTime() - offset * 24 * 60 * 60 * 1000);
      return path.join(this.sessionsDir, ...day.toISOString().slice(0, 10).split("-"));
    });
    return new IncrementalJsonlDiscovery(roots, {
      recursive: false,
      matches: (name) => name.startsWith("rollout-") && name.endsWith(".jsonl"),
      maxEntries: Math.max(1, limit ?? 100_000),
      missingRootsAreEmpty: true,
      isCandidateQuarantined: (candidateHash) => {
        const quarantine = this.activeBoundaryOptions.quarantine;
        return Boolean(
          quarantine &&
          (quarantine.stage.startsWith("discovery_") || quarantine.stage === "candidate_metadata") &&
          quarantine.candidateHash === candidateHash,
        );
      },
      beforeFilesystemStep: (progress) =>
        this.activeBoundaryOptions.onProgress?.(progress) ?? true,
    });
  }

  private async collectAutomaticCaptureFiles(
    options: RolloutScanOptions,
    budget: CaptureWorkBudget,
  ) {
    if (!this.captureAttempt) {
      this.captureAttempt = {
        discovery: this.recentDiscovery(options.now ?? new Date(), options.discoveryLimit, options),
        pendingFiles: [],
        discoveryDone: false,
      };
    }
    const attempt = this.captureAttempt;
    let entries = 0;
    let errors = 0;
    let truncated = false;
    if (attempt.pendingFiles.length === 0 && !attempt.discoveryDone) {
      const chunk = await attempt.discovery.collect(budget, {
        signal: options.signal,
        maxFiles: AUTOMATIC_DISCOVERY_PENDING_METADATA_CAP,
        maxEntries: AUTOMATIC_DISCOVERY_ENTRY_CAP,
        maxWallMs: AUTOMATIC_DISCOVERY_WALL_MS,
      });
      attempt.pendingFiles.push(...chunk.files);
      attempt.discoveryDone = chunk.done;
      entries = chunk.entriesVisited;
      errors = chunk.errors;
      truncated = !chunk.done || chunk.limitReached;
      if (chunk.errors > 0 || chunk.limitReached) attempt.discoveryDone = true;
    }
    const files = [...attempt.pendingFiles];
    const done = attempt.discoveryDone;
    return { files, errors, truncated: truncated || !done, discoveryEntries: entries };
  }

  private consumeAutomaticCaptureFiles(files: ReadonlySet<string>) {
    const attempt = this.captureAttempt;
    if (!attempt) return;
    attempt.pendingFiles = attempt.pendingFiles.filter((pending) => !files.has(pending.file));
    if (attempt.discoveryDone && attempt.pendingFiles.length === 0) {
      attempt.discovery.close();
      this.captureAttempt = null;
    }
  }

  private regularFileStat(file: string) {
    const metadata = this.io.lstat(file);
    if (metadata.isSymbolicLink() || !metadata.isFile()) {
      throw new Error("capture_non_regular_file");
    }
    const compatibility = this.io.stat(file);
    if (
      compatibility.dev !== metadata.dev ||
      compatibility.ino !== metadata.ino ||
      compatibility.birthtimeMs !== metadata.birthtimeMs
    ) throw new Error("capture_file_alias_changed");
    return metadata;
  }

  private nextCandidatePreference(): "newest" | "cursor" {
    const key = "capture_candidate_preference_codex";
    const row = this.buffer.database.prepare(
      `select value from maintenance_state where key = ?`,
    ).get(key) as { value: string } | undefined;
    const current = row?.value === "cursor" ? "cursor" : "newest";
    this.buffer.database.prepare(
      `insert into maintenance_state (key, value, updated_at) values (?, ?, ?)
       on conflict(key) do update set value = excluded.value, updated_at = excluded.updated_at`,
    ).run(key, current === "newest" ? "cursor" : "newest", new Date().toISOString());
    return current;
  }

  private discover(options: RolloutScanOptions): {
    files: string[];
    truncated: boolean;
    errors: number;
  } {
    const now = options.now ?? new Date();
    const files: string[] = [];
    const dayDirs: string[] = [];
    const limit = Math.max(1, options.discoveryLimit ?? Number.MAX_SAFE_INTEGER);
    let truncated = false;
    let errors = 0;
    const listDirs = (dir: string, root = false) => {
      try {
        return this.io
          .readDirents(dir)
          .filter((entry) => entry.isDirectory())
          .map((entry) => path.join(dir, entry.name));
      } catch (error) {
        if (!(root && (error as NodeJS.ErrnoException).code === "ENOENT")) errors += 1;
        return [];
      }
    };
    if (options.scope === "recent") {
      for (const offset of [0, 1]) {
        const day = new Date(now.getTime() - offset * 24 * 60 * 60 * 1000);
        const iso = day.toISOString().slice(0, 10);
        dayDirs.push(path.join(this.sessionsDir, ...iso.split("-")));
      }
    } else {
      // Full walk: sessions/YYYY/MM/DD — three bounded levels.
      for (const year of listDirs(this.sessionsDir, true)) {
        for (const month of listDirs(year)) {
          dayDirs.push(...listDirs(month));
        }
      }
    }
    for (const dir of dayDirs) {
      let entries: string[];
      try {
        entries = this.io.readNames(dir);
      } catch (error) {
        // A recent day directory commonly does not exist yet. In a full walk,
        // however, day directories came from discovery, so disappearance or
        // inaccessibility means the walk was not exhaustive.
        if (
          !(
            options.scope === "recent" &&
            (error as NodeJS.ErrnoException).code === "ENOENT"
          )
        ) {
          errors += 1;
        }
        continue;
      }
      for (const entry of entries) {
        if (entry.startsWith("rollout-") && entry.endsWith(".jsonl")) {
          if (files.length >= limit) {
            truncated = true;
            break;
          }
          files.push(path.join(dir, entry));
        }
      }
      if (truncated) break;
    }
    return { files: files.sort(), truncated, errors };
  }

  private sessionHasNonRolloutTokens(sessionId: string) {
    return this.buffer.sessionUsageAuthority("codex", sessionId) === "live";
  }

  private initialParserState(file: string): RolloutParserState {
    return {
      parserKind: PARSER_KIND,
      checkpointVersion: CHECKPOINT_VERSION,
      conversationId: conversationIdFromFilename(file),
      previous: { ...ZERO },
      tokenCountIndex: -1,
      contextOccurrenceIndex: -1,
    };
  }

  private fallbackObservedAt(mtimeMs: number) {
    const observed = new Date(mtimeMs);
    return Number.isFinite(mtimeMs) && !Number.isNaN(observed.getTime())
      ? observed.toISOString()
      : new Date().toISOString();
  }

  private ingestLines(
    lines: string[],
    result: RolloutScanResult,
    state: RolloutParserState,
    fallbackObservedAt: string,
    fileIdentity: string,
  ) {
    type ActiveContext =
      | { kind: "persisted"; contextId: string }
      | { kind: "request"; request: RepoContextRequest }
      | undefined;
    const pending: Array<{
      index: number;
      observedAt: string | undefined;
      delta: TokenTotals;
      model: string | undefined;
      repoContext: ActiveContext;
    }> = [];
    let activeRepoContext: ActiveContext = state.activeRepoContextId
      ? { kind: "persisted", contextId: state.activeRepoContextId }
      : undefined;

    const observeContext = (
      type: "session_meta" | "turn_context",
      cwd: unknown,
    ): ActiveContext => {
      state.contextOccurrenceIndex += 1;
      state.activeRepoContextId = undefined;
      if (typeof cwd !== "string") return undefined;
      const occurrence = [
        "codex-rollout",
        fileIdentity,
        state.conversationId ?? "unknown",
        type,
        String(state.contextOccurrenceIndex),
      ].join(":");
      const request = this.buffer.repoContextOccurrenceRequest("codex", occurrence, cwd);
      return request ? { kind: "request", request } : undefined;
    };

    for (const line of lines) {
      // Privacy prefilter: message/reasoning lines are never JSON-parsed.
      const isMeta = line.includes('"session_meta"');
      const isTurn = line.includes('"turn_context"');
      const isCount = line.includes('"token_count"');
      if (!isMeta && !isTurn && !isCount) continue;
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(line) as Record<string, unknown>;
      } catch {
        result.parseErrors += 1; // partial trailing bytes are deferred before this parser
        continue;
      }
      const type = parsed.type;
      const payload = (parsed.payload ?? {}) as Record<string, unknown>;
      if (type === "session_meta") {
        if (typeof payload.id === "string" && UUID_RE.test(payload.id)) {
          state.conversationId = payload.id.toLowerCase();
        }
        if (typeof parsed.timestamp === "string") state.sessionStartedAt = parsed.timestamp;
        else if (typeof payload.timestamp === "string") state.sessionStartedAt = payload.timestamp as string;
        activeRepoContext = observeContext("session_meta", payload.cwd);
        if (typeof payload.originator === "string") state.originator = payload.originator;
        if (typeof payload.cli_version === "string") state.cliVersion = payload.cli_version;
      } else if (type === "turn_context") {
        if (typeof payload.model === "string" && payload.model) state.model = payload.model;
        activeRepoContext = observeContext("turn_context", payload.cwd);
      } else if (type === "event_msg" && payload.type === "token_count") {
        state.tokenCountIndex += 1;
        const info = (payload.info ?? {}) as Record<string, unknown>;
        const totals = totalsFrom(info.total_token_usage as Record<string, unknown> | undefined);
        if (!totals) continue;
        const rateLimits = (payload.rate_limits ?? {}) as Record<string, unknown>;
        if (typeof rateLimits.plan_type === "string") state.planType = rateLimits.plan_type;
        const delta = diff(totals, state.previous);
        state.previous = totals;
        if (delta.input === 0 && delta.output === 0) continue; // periodic no-op emission
        pending.push({
          index: state.tokenCountIndex,
          observedAt: typeof parsed.timestamp === "string" ? parsed.timestamp : undefined,
          delta,
          model: state.model,
          repoContext: activeRepoContext,
        });
      }
    }

    const sessionCovered = state.conversationId
      ? this.sessionHasNonRolloutTokens(state.conversationId)
      : false;
    if (sessionCovered) {
      if (pending.length > 0) result.sessionsSkippedOtlpCovered += 1;
      if (activeRepoContext?.kind === "request") state.activeRepoContextId = undefined;
      return state;
    }

    // Only contexts that can affect a token or the next slice consume bounded
    // resolver capacity. This prevents context-only amplification from
    // displacing the final context that actually owns usage.
    const requests = new Map<string, RepoContextRequest>();
    for (const entry of pending) {
      if (entry.repoContext?.kind === "request") {
        requests.set(entry.repoContext.request.contextId, entry.repoContext.request);
      }
    }
    if (activeRepoContext?.kind === "request") {
      requests.set(activeRepoContext.request.contextId, activeRepoContext.request);
    }
    for (const request of requests.values()) {
      this.buffer.stageRepoContextRequest(request);
    }
    if (activeRepoContext?.kind === "persisted") {
      state.activeRepoContextId = activeRepoContext.contextId;
    } else if (activeRepoContext?.kind === "request") {
      // Persist the path-free occurrence identity even when resolver capacity
      // rejects ownership. Later token rows then receive a terminal sidecar
      // receipt and can never fall back into legacy session stitching.
      state.activeRepoContextId = activeRepoContext.request.contextId;
    }

    if (!state.conversationId || pending.length === 0) return state;
    result.filesParsed += 1;

    // Identity window: only sessions that started at/after the current
    // login's last_refresh provably ran under this account. History stays
    // unattributed rather than guessed (issue 0028).
    const identity = this.codexIdentity;
    const actorId =
      identity?.actorHash &&
      identity.validFrom &&
      state.sessionStartedAt &&
      Date.parse(state.sessionStartedAt) >= Date.parse(identity.validFrom)
        ? identity.actorHash
        : undefined;
    for (const entry of pending) {
      const priced = estimateCostUsd({
        model: entry.model,
        inputTokens: entry.delta.input,
        outputTokens: entry.delta.output,
        cacheReadTokens: entry.delta.cachedInput,
      });
      const metadata: Record<string, unknown> = {
        usageSource: "rollout",
        turnIndex: entry.index,
      };
      if (state.originator) metadata.originator = state.originator;
      if (state.cliVersion) metadata.cliVersion = state.cliVersion;
      if (state.planType) metadata.planType = state.planType;
      if (entry.delta.reasoningOutput > 0) metadata.reasoningOutputTokens = entry.delta.reasoningOutput;
      if (priced) metadata.costEstimated = true;

      const event: AiInteractionEvent = aiInteractionEventSchema.parse({
        id: deterministicEventId(["codex-rollout", state.conversationId, String(entry.index)]),
        tenantId: "local",
        source: "codex",
        dataMode: "metadata",
        eventType: "usage_rollout",
        observedAt: entry.observedAt ?? fallbackObservedAt,
        actorId,
        sessionId: state.conversationId,
        model: entry.model,
        actionClass: "other",
        inputTokens: entry.delta.input,
        outputTokens: entry.delta.output,
        cacheReadTokens: entry.delta.cachedInput,
        costUsd: priced?.costUsd,
        metadata,
      });
      const repoContextId = entry.repoContext?.kind === "persisted"
        ? entry.repoContext.contextId
        : entry.repoContext?.kind === "request"
          ? entry.repoContext.request.contextId
          : undefined;
      if (repoContextId && !attachRepoContextId(event, repoContextId)) {
        throw new Error("rollout_repo_context_binding_failed");
      }
      const inserted = this.buffer.append(event, []);
      if (inserted) {
        result.eventsAppended += 1;
        result.tokensAppended.input += entry.delta.input;
        result.tokensAppended.cachedInput += entry.delta.cachedInput;
        result.tokensAppended.output += entry.delta.output;
      }
    }
    return state;
  }
}
