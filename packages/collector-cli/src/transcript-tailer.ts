import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { LocalEventBuffer } from "./buffer";
import { resolveGitContext } from "./git-context";
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
import { deterministicEventId } from "./normalizer";
import {
  aiInteractionEventSchema,
  estimateCostUsd,
  type AiInteractionEvent,
} from "../../shared/src/index";

/**
 * Claude Code transcript tailer (owner ask 2026-06-10: "it isn't tracking far
 * enough in the past — should be a lot more if the data exists"). It does:
 * ~/.claude/projects/** transcripts carry per-message usage (tokens, model,
 * timestamp) back to the oldest file on disk — weeks before OTLP capture
 * existed, and for any session launched without telemetry env.
 *
 * Mirrors the codex rollout tailer:
 *  - Deterministic ids over (session, message id): retries/streams collapse,
 *    resumed/forked session files collapse, rescans are idempotent.
 *  - First-writer-wins per session vs live capture: sessions that already
 *    have non-tailer token events (OTLP) are skipped entirely.
 *  - Cost is estimated from sourced Anthropic rates and flagged
 *    costEstimated; cache WRITES have no column yet (issue 0024), so the
 *    estimate is a floor — cacheCreationTokens rides metadata for later.
 *  - Privacy: lines are prefiltered (assistant + usage only); only numbers,
 *    ids, model and timestamps are persisted — never message content.
 *  - Repo linkage from the entry cwd via hashed git context.
 *  - Identity: NOT stamped. Claude's local config has no login-window
 *    equivalent of codex's last_refresh, so history stays unattributed
 *    rather than guessed.
 */

export type TranscriptScanResult = {
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
  sessionsSkippedLiveCovered: number;
  filesSkippedOutsideRecentWindow: number;
  eventsAppended: number;
  tokensAppended: { input: number; cacheRead: number; output: number };
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

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;
const UUID_EXACT_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

type PersistedGitContext = {
  remoteUrlHash?: string;
  branchHash?: string;
  headSha?: string;
};

type TranscriptParserState = {
  parserKind: "claude-transcript-v3";
  checkpointVersion: 3;
  sessionId?: string;
  git?: PersistedGitContext;
  pending?: TranscriptPendingUsage;
  usageRevisions?: TranscriptUsageRevision[];
};

type TranscriptUsageRevision = Pick<
  TranscriptPendingUsage,
  "messageId" | "input" | "cacheRead" | "cacheCreation" | "output"
>;

type TranscriptPendingUsage = {
  messageId: string;
  observedAt?: string;
  model?: string;
  input: number;
  cacheRead: number;
  cacheCreation: number;
  output: number;
};

const PARSER_KIND = "claude-transcript-v3";
const CHECKPOINT_VERSION = 3;

export type TranscriptScanOptions = {
  scope: "recent" | "full";
  now?: Date;
  discoveryLimit?: number;
  automatic?: {
    phase: "baseline" | "capture";
    budget: CaptureWorkBudget;
  };
  signal?: AbortSignal;
};

function validateTranscriptParserState(value: unknown): TranscriptParserState | undefined {
  if (!isRecord(value)) return undefined;
  if (!hasOnlyKeys(value, ["parserKind", "checkpointVersion", "sessionId", "git", "pending", "usageRevisions"])) {
    return undefined;
  }
  if (value.parserKind !== PARSER_KIND || value.checkpointVersion !== CHECKPOINT_VERSION) {
    return undefined;
  }
  if (value.sessionId !== undefined) {
    if (typeof value.sessionId !== "string" || !UUID_EXACT_RE.test(value.sessionId)) return undefined;
  }
  const git = validatePersistedGit(value.git);
  if (value.git !== undefined && !git) return undefined;
  const pending = validatePendingUsage(value.pending);
  if (value.pending !== undefined && !pending) return undefined;
  const usageRevisions = validateUsageRevisions(value.usageRevisions);
  if (value.usageRevisions !== undefined && !usageRevisions) return undefined;
  return {
    parserKind: PARSER_KIND,
    checkpointVersion: CHECKPOINT_VERSION,
    ...(value.sessionId ? { sessionId: value.sessionId.toLowerCase() } : {}),
    ...(git ? { git } : {}),
    ...(pending ? { pending } : {}),
    ...(usageRevisions?.length ? { usageRevisions } : {}),
  };
}

function validateUsageRevisions(value: unknown): TranscriptUsageRevision[] | undefined {
  if (value === undefined) return [];
  if (!Array.isArray(value) || value.length > 64) return undefined;
  const revisions: TranscriptUsageRevision[] = [];
  const ids = new Set<string>();
  for (const entry of value) {
    const normalized = validatePendingUsage(entry);
    if (!normalized || ids.has(normalized.messageId)) return undefined;
    ids.add(normalized.messageId);
    revisions.push({
      messageId: normalized.messageId,
      input: normalized.input,
      cacheRead: normalized.cacheRead,
      cacheCreation: normalized.cacheCreation,
      output: normalized.output,
    });
  }
  return revisions;
}

function validatePendingUsage(value: unknown): TranscriptPendingUsage | undefined {
  if (!isRecord(value)) return undefined;
  if (
    !hasOnlyKeys(value, [
      "messageId",
      "observedAt",
      "model",
      "input",
      "cacheRead",
      "cacheCreation",
      "output",
    ]) ||
    typeof value.messageId !== "string" ||
    value.messageId.length === 0 ||
    value.messageId.length > 512
  ) {
    return undefined;
  }
  for (const key of ["input", "cacheRead", "cacheCreation", "output"] as const) {
    if (!Number.isSafeInteger(value[key]) || Number(value[key]) < 0) return undefined;
  }
  if (value.observedAt !== undefined && typeof value.observedAt !== "string") return undefined;
  if (value.model !== undefined && typeof value.model !== "string") return undefined;
  return {
    messageId: value.messageId,
    ...(typeof value.observedAt === "string" ? { observedAt: value.observedAt } : {}),
    ...(typeof value.model === "string" ? { model: value.model } : {}),
    input: Number(value.input),
    cacheRead: Number(value.cacheRead),
    cacheCreation: Number(value.cacheCreation),
    output: Number(value.output),
  };
}

function validatePersistedGit(value: unknown): PersistedGitContext | undefined {
  if (!isRecord(value)) return undefined;
  if (!hasOnlyKeys(value, ["remoteUrlHash", "branchHash", "headSha"])) return undefined;
  for (const key of ["remoteUrlHash", "branchHash", "headSha"] as const) {
    if (value[key] !== undefined && typeof value[key] !== "string") return undefined;
  }
  const git = {
    ...(typeof value.remoteUrlHash === "string" ? { remoteUrlHash: value.remoteUrlHash } : {}),
    ...(typeof value.branchHash === "string" ? { branchHash: value.branchHash } : {}),
    ...(typeof value.headSha === "string" ? { headSha: value.headSha } : {}),
  };
  return git.remoteUrlHash || git.branchHash || git.headSha ? git : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function hasOnlyKeys(value: Record<string, unknown>, allowed: readonly string[]) {
  const names = new Set(allowed);
  return Object.keys(value).every((key) => names.has(key));
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

function resultMutationSnapshot(result: TranscriptScanResult) {
  return {
    parseErrors: result.parseErrors,
    eventsAppended: result.eventsAppended,
    filesParsed: result.filesParsed,
    sessionsSkippedLiveCovered: result.sessionsSkippedLiveCovered,
    tokens: { ...result.tokensAppended },
  };
}

function restoreResultMutationSnapshot(
  result: TranscriptScanResult,
  snapshot: ReturnType<typeof resultMutationSnapshot>,
) {
  result.parseErrors = snapshot.parseErrors;
  result.eventsAppended = snapshot.eventsAppended;
  result.filesParsed = snapshot.filesParsed;
  result.sessionsSkippedLiveCovered = snapshot.sessionsSkippedLiveCovered;
  result.tokensAppended = { ...snapshot.tokens };
}

export class TranscriptTailer {
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
    private readonly projectsDir = path.join(os.homedir(), ".claude", "projects"),
    private readonly io: JsonlTailerIo = DEFAULT_JSONL_TAILER_IO,
  ) {
    ensureJsonlScanState(this.buffer.database);
    this.buffer.database.exec(`
      create table if not exists transcript_usage_revision_state (
        source text not null check (source = 'claude_code'),
        session_id text not null,
        message_key text not null,
        input_tokens integer not null check (input_tokens >= 0),
        cache_read_tokens integer not null check (cache_read_tokens >= 0),
        cache_creation_tokens integer not null check (cache_creation_tokens >= 0),
        output_tokens integer not null check (output_tokens >= 0),
        updated_at text not null,
        primary key (source, session_id, message_key)
      )
    `);
  }

  close() {
    this.baselineAttempt?.discovery.close();
    this.captureAttempt?.discovery.close();
    this.baselineAttempt = null;
    this.captureAttempt = null;
  }

  private sessionHasLiveTokens(sessionId: string) {
    return this.buffer.sessionUsageAuthority("claude_code", sessionId) === "live";
  }

  async scan(options: TranscriptScanOptions): Promise<TranscriptScanResult> {
    const scanNow = options.now ?? new Date();
    const today = scanNow.toISOString().slice(0, 10);
    const result: TranscriptScanResult = {
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
        lastScanAt: scanNow.toISOString(),
        truncated: false,
      },
    };
    const now = scanNow;
    const recentCutoff = now.getTime() - 48 * 60 * 60 * 1000;
    if (options.scope === "recent" && !options.automatic) {
      throw new Error("recent_transcript_scan_requires_automatic_control");
    }
    const automatic = options.scope === "recent" ? options.automatic! : null;
    const baselineBefore = captureBaselineStatus(this.buffer.database);
    const claudeBaseline = baselineBefore.sources.find(
      (source) => source.source === "claude_code",
    )!;
    if (automatic?.phase === "baseline" && claudeBaseline.status === "complete") {
      this.baselineAttempt?.discovery.close();
      this.baselineAttempt = null;
      result.excludedGenerations = claudeBaseline.excludedGenerations;
      result.excludedBytes = claudeBaseline.currentExcludedBytes;
      result.exhaustive = true;
      result.automaticBudget = automatic.budget.status();
      return result;
    }

    if (automatic?.phase === "baseline") {
      if (!this.baselineAttempt) {
        const began = beginAutomaticCaptureBaseline(this.buffer.database, "claude_code", {
          startedAt: scanNow.toISOString(),
          filesDiscovered: 0,
        });
        if (began.status !== "in_progress" || !began.latestRun) {
          result.exhaustive = false;
          result.automaticBudget = automatic.budget.status();
          return result;
        }
        this.baselineAttempt = {
          discovery: this.recentDiscovery(options.discoveryLimit),
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
          "claude_code",
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
        if (acceptedFiles.length < chunk.files.length) {
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
            if (stageAutomaticCaptureBaselineObservation(this.buffer.database, "claude_code", {
              runId: attempt.runId,
              observedAt: new Date().toISOString(),
              observation: baselineObservation(file, stat, discovered.precise),
              filesDiscovered: attempt.filesDiscovered,
              filesValidated: attempt.filesValidated + 1,
              resolvePending: true,
            })) attempt.newGenerationsThisSweep += 1;
          } else {
            if (!resolveAutomaticCaptureBaselinePending(this.buffer.database, "claude_code", {
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
          recordAutomaticCaptureBaselineProgress(this.buffer.database, "claude_code", {
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
        recordAutomaticCaptureBaselineProgress(this.buffer.database, "claude_code", {
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
        recordAutomaticCaptureBaselineProgress(this.buffer.database, "claude_code", {
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
        attempt.discovery = this.recentDiscovery(options.discoveryLimit);
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
        attempt.discovery = this.recentDiscovery(options.discoveryLimit);
        attempt.newGenerationsThisSweep = 0;
        result.activity.truncated = true;
        result.deferredGenerations = 1;
        result.automaticBudget = automatic.budget.status();
        return result;
      }

      const completed = completeAutomaticCaptureBaseline(this.buffer.database, "claude_code", {
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
    const explicitDiscovery = automatic ? null : this.discover(options.discoveryLimit);
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
      cursor: JsonlScanCursor<TranscriptParserState> | undefined;
    }> = [];
    let automaticFilesConsumed = 0;
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
      if (automatic) automaticFilesConsumed = index + 1;
      let stat: fs.Stats;
      try {
        stat = discovered.stat ?? this.regularFileStat(file);
      } catch {
        result.statErrors += 1;
        continue;
      }
      const mtime = stat.mtime.toISOString();
      if (!result.activity.lastActivityAt || mtime > result.activity.lastActivityAt) {
        result.activity.lastActivityAt = mtime;
      }
      if (mtime.slice(0, 10) === today) result.activity.filesToday += 1;
      if (options.scope === "recent" && stat.mtime.getTime() < recentCutoff) {
        result.filesSkippedOutsideRecentWindow += 1;
        continue;
      }
      const observation = baselineObservation(file, stat, discovered.precise);
      if (automatic?.phase === "capture") {
        const decision = classifyCaptureBaselineFile(
          this.buffer.database,
          "claude_code",
          observation,
          { mode: "automatic", observedAt: scanNow.toISOString() },
        );
        if (decision.decision === "exclude") {
          result.excludedGenerations += 1;
          result.excludedBytes += stat.size;
          continue;
        }
        if (decision.decision === "block") {
          result.statErrors += 1;
          continue;
        }
      }
      if (options.scope === "full" && baselineBefore.status === "complete") {
        const decision = classifyCaptureBaselineFile(
          this.buffer.database,
          "claude_code",
          observation,
          { mode: "explicit_full", observedAt: scanNow.toISOString() },
        );
        if (decision.decision === "block") {
          result.statErrors += 1;
          continue;
        }
      }
      const cursor = loadJsonlScanCursor<TranscriptParserState>(
        this.buffer.database,
        file,
        PARSER_KIND,
        CHECKPOINT_VERSION,
        validateTranscriptParserState,
      );
      candidates.push({ file, stat, cursor });
    }
    if (automatic) this.consumeAutomaticCaptureFiles(automaticFilesConsumed);

    const preferNewest = automatic ? this.nextCandidatePreference() === "newest" : false;
    candidates.sort((left, right) => {
      const leftCursor = left.cursor?.checkpointStatus === "valid" ? 1 : 0;
      const rightCursor = right.cursor?.checkpointStatus === "valid" ? 1 : 0;
      return preferNewest
        ? right.stat.mtimeMs - left.stat.mtimeMs || rightCursor - leftCursor || left.file.localeCompare(right.file)
        : rightCursor - leftCursor || right.stat.mtimeMs - left.stat.mtimeMs || left.file.localeCompare(right.file);
    });

    for (const candidate of candidates) {
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
        let limits = automatic
          ? automatic.budget.remainingSlice()
          : { maxBytes: 128 * 1024, maxRecords: 64 };
        if (!limits) break;
        if (automatic && cursor?.parserState?.pending) {
          const slots = automatic.budget.remainingEventSlots();
          if (slots <= 1) break;
          limits = { ...limits, maxRecords: Math.min(limits.maxRecords, slots - 1) };
        }
        let read: NonNullable<ReturnType<JsonlTailerIo["readTail"]>>;
        try {
          const next = this.io.readTail(candidate.file, candidate.stat, cursor, limits);
          if (!next) break;
          read = next;
        } catch {
          result.readErrors += 1;
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
        try {
          const initialState = read.reset || !cursor?.parserState
            ? this.initialParserState(candidate.file)
            : cursor.parserState;
          this.buffer.database.transaction(() => {
            if (read.unresolvedRecord) {
              read.assertStableForCommit();
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
              candidate.file,
              read.lines,
              result,
              initialState,
              !read.workRemaining && read.deferredBytes === 0,
            );
            if (result.parseErrors !== parseErrorsBefore) {
              parseFailure = true;
              throw new Error("transcript_slice_parse_failed");
            }
            read.assertStableForCommit();
            rememberJsonlScanCursor(
              this.buffer.database,
              candidate.file,
              PARSER_KIND,
              CHECKPOINT_VERSION,
              read,
              parserState,
            );
          })();
          committed = true;
          result.slicesCommitted += 1;
          if (read.unresolvedRecord) result.unresolvedRecords += 1;
        } catch {
          const parseErrors = result.parseErrors - before.parseErrors;
          restoreResultMutationSnapshot(result, before);
          if (parseFailure) result.parseErrors += Math.max(1, parseErrors);
          else result.readErrors += 1;
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
        cursor = loadJsonlScanCursor<TranscriptParserState>(
          this.buffer.database,
          candidate.file,
          PARSER_KIND,
          CHECKPOINT_VERSION,
          validateTranscriptParserState,
        );
        if (automatic && !automatic.budget.canContinue()) break;
      }
    }
    for (const candidate of candidates) {
      const cursor = loadJsonlScanCursor<TranscriptParserState>(
        this.buffer.database,
        candidate.file,
        PARSER_KIND,
        CHECKPOINT_VERSION,
        validateTranscriptParserState,
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
    return result;
  }

  private recentDiscovery(limit?: number) {
    return new IncrementalJsonlDiscovery([this.projectsDir], {
      recursive: true,
      matches: (name) => name.endsWith(".jsonl"),
      maxEntries: Math.max(1, limit ?? 100_000),
      missingRootsAreEmpty: true,
    });
  }

  private async collectAutomaticCaptureFiles(
    options: TranscriptScanOptions,
    budget: CaptureWorkBudget,
  ) {
    if (!this.captureAttempt) {
      this.captureAttempt = {
        discovery: this.recentDiscovery(options.discoveryLimit),
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
    return {
      files: [...attempt.pendingFiles],
      errors,
      truncated: truncated || !attempt.discoveryDone,
      discoveryEntries: entries,
    };
  }

  private consumeAutomaticCaptureFiles(count: number) {
    const attempt = this.captureAttempt;
    if (!attempt) return;
    attempt.pendingFiles.splice(0, Math.max(0, count));
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
    const key = "capture_candidate_preference_claude";
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

  private discover(limit = 100_000): {
    files: string[];
    truncated: boolean;
    errors: number;
  } {
    const files: string[] = [];
    const stack = [this.projectsDir];
    let seen = 0;
    let truncated = false;
    let errors = 0;
    const boundedLimit = Math.max(1, limit);
    while (stack.length > 0) {
      const dir = stack.pop()!;
      let entries: fs.Dirent[];
      try {
        entries = this.io.readDirents(dir);
      } catch (error) {
        if (!(dir === this.projectsDir && (error as NodeJS.ErrnoException).code === "ENOENT")) {
          errors += 1;
        }
        continue;
      }
      for (const entry of entries) {
        if (seen >= boundedLimit) {
          truncated = true;
          break;
        }
        seen += 1;
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) stack.push(full);
        else if (entry.name.endsWith(".jsonl")) files.push(full);
      }
      if (truncated) break;
    }
    return { files: files.sort(), truncated, errors };
  }

  private initialParserState(file: string): TranscriptParserState {
    return {
      parserKind: PARSER_KIND,
      checkpointVersion: CHECKPOINT_VERSION,
      sessionId: path.basename(file).replace(/\.jsonl$/, "").match(UUID_RE)?.[0]?.toLowerCase(),
    };
  }

  private safeGitContext(cwd: string): PersistedGitContext | undefined {
    const git = resolveGitContext(cwd);
    if (git?.remoteUrlHash && git.remoteLabel) {
      this.buffer.recordRepoLabel(git.remoteUrlHash, git.remoteLabel);
    }
    if (!git) return undefined;
    const safe = {
      remoteUrlHash: git.remoteUrlHash,
      branchHash: git.branchHash,
      headSha: git.headSha,
    };
    return safe.remoteUrlHash || safe.branchHash || safe.headSha ? safe : undefined;
  }

  private ingestLines(
    file: string,
    lines: string[],
    result: TranscriptScanResult,
    state: TranscriptParserState,
    flushAtStableEof: boolean,
  ) {
    const fallbackObservedAt = (() => {
      try {
        return fs.statSync(file).mtime.toISOString();
      } catch {
        return new Date().toISOString();
      }
    })();
    // Upgrade an old v3 pending snapshot into the durable revision model
    // before processing new bytes. This keeps existing cursors compatible.
    if (state.pending) {
      const pending = state.pending;
      delete state.pending;
      this.applyUsageRevision(state, pending, result, fallbackObservedAt);
    }
    for (const line of lines) {
      // Prefilter: only assistant entries with usage are ever parsed.
      if (!line.includes('"assistant"') || !line.includes('"usage"')) continue;
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(line) as Record<string, unknown>;
      } catch {
        result.parseErrors += 1;
        continue;
      }
      if (parsed.type !== "assistant") continue;
      if (!state.sessionId && typeof parsed.sessionId === "string") {
        state.sessionId = parsed.sessionId.match(UUID_RE)?.[0]?.toLowerCase();
      }
      if (typeof parsed.cwd === "string") state.git = this.safeGitContext(parsed.cwd);
      const message = (parsed.message ?? {}) as Record<string, unknown>;
      const usage = (message.usage ?? {}) as Record<string, unknown>;
      const messageId = typeof message.id === "string" ? message.id : undefined;
      if (!messageId) continue;
      const num = (key: string) => {
        const value = usage[key];
        return typeof value === "number" && Number.isFinite(value) ? value : 0;
      };
      const next: TranscriptPendingUsage = {
        messageId,
        observedAt: typeof parsed.timestamp === "string" ? parsed.timestamp : undefined,
        model: typeof message.model === "string" ? message.model : undefined,
        input: num("input_tokens"),
        cacheRead: num("cache_read_input_tokens"),
        cacheCreation: num("cache_creation_input_tokens"),
        output: num("output_tokens"),
      };
      this.applyUsageRevision(state, next, result, fallbackObservedAt);
    }
    void flushAtStableEof;
    return state;
  }

  private applyUsageRevision(
    state: TranscriptParserState,
    entry: TranscriptPendingUsage,
    result: TranscriptScanResult,
    fallbackObservedAt: string,
  ) {
    if (!state.sessionId) {
      result.parseErrors += 1;
      return;
    }
    if (this.sessionHasLiveTokens(state.sessionId)) {
      result.sessionsSkippedLiveCovered += 1;
      return;
    }
    const revisions = state.usageRevisions ?? [];
    const statePrevious = revisions.find((candidate) => candidate.messageId === entry.messageId);
    const eventBaseId = deterministicEventId(["claude-transcript", state.sessionId, entry.messageId]);
    const previous =
      this.durableUsageRevision(state.sessionId, entry.messageId) ??
      statePrevious ??
      this.persistedUsageRevision(eventBaseId, entry.messageId);
    if (
      previous &&
      (entry.input < previous.input ||
        entry.cacheRead < previous.cacheRead ||
        entry.cacheCreation < previous.cacheCreation ||
        entry.output < previous.output)
    ) {
      result.parseErrors += 1;
      return;
    }
    const delta = {
      input: entry.input - (previous?.input ?? 0),
      cacheRead: entry.cacheRead - (previous?.cacheRead ?? 0),
      cacheCreation: entry.cacheCreation - (previous?.cacheCreation ?? 0),
      output: entry.output - (previous?.output ?? 0),
    };
    state.usageRevisions = [
      ...revisions.filter((candidate) => candidate.messageId !== entry.messageId),
      {
        messageId: entry.messageId,
        input: entry.input,
        cacheRead: entry.cacheRead,
        cacheCreation: entry.cacheCreation,
        output: entry.output,
      },
    ].slice(-64);
    this.buffer.database.prepare(
      `insert into transcript_usage_revision_state (
         source, session_id, message_key, input_tokens, cache_read_tokens,
         cache_creation_tokens, output_tokens, updated_at
       ) values ('claude_code', ?, ?, ?, ?, ?, ?, ?)
       on conflict(source, session_id, message_key) do update set
         input_tokens = excluded.input_tokens,
         cache_read_tokens = excluded.cache_read_tokens,
         cache_creation_tokens = excluded.cache_creation_tokens,
         output_tokens = excluded.output_tokens,
         updated_at = excluded.updated_at`,
    ).run(
      state.sessionId,
      this.messageKey(entry.messageId),
      entry.input,
      entry.cacheRead,
      entry.cacheCreation,
      entry.output,
      new Date().toISOString(),
    );
    if (delta.input === 0 && delta.output === 0 && delta.cacheRead === 0 && delta.cacheCreation === 0) return;
    result.filesParsed += 1;
    const priced = estimateCostUsd({
      model: entry.model,
      inputTokens: delta.input,
      outputTokens: delta.output,
      cacheReadTokens: delta.cacheRead,
      cacheCreationTokens: delta.cacheCreation,
    });
    const metadata: Record<string, unknown> = { usageSource: "transcript" };
    if (priced) metadata.costEstimated = true;
    if (state.git) {
      metadata.git = {
        remoteUrlHash: state.git.remoteUrlHash,
        branchHash: state.git.branchHash,
        headSha: state.git.headSha,
      };
    }
    const event: AiInteractionEvent = aiInteractionEventSchema.parse({
      id: previous
        ? deterministicEventId([
            "claude-transcript-revision",
            state.sessionId,
            entry.messageId,
            String(entry.input),
            String(entry.cacheRead),
            String(entry.cacheCreation),
            String(entry.output),
          ])
        : eventBaseId,
      tenantId: "local",
      source: "claude_code",
      dataMode: "metadata",
      eventType: "usage_transcript",
      observedAt: entry.observedAt ?? fallbackObservedAt,
      sessionId: state.sessionId,
      model: entry.model,
      actionClass: "other",
      inputTokens: delta.input,
      outputTokens: delta.output,
      cacheReadTokens: delta.cacheRead,
      cacheCreationTokens: delta.cacheCreation > 0 ? delta.cacheCreation : undefined,
      costUsd: priced?.costUsd,
      metadata,
    });
    const inserted = this.buffer.append(event, []);
    if (inserted) {
      result.eventsAppended += 1;
      result.tokensAppended.input += delta.input;
      result.tokensAppended.cacheRead += delta.cacheRead;
      result.tokensAppended.output += delta.output;
    }
  }

  private persistedUsageRevision(eventId: string, messageId: string): TranscriptUsageRevision | undefined {
    const row = this.buffer.database.prepare(
      `select input_tokens as input, cache_read_tokens as cacheRead,
         output_tokens as output, payload_json as payloadJson
       from buffered_events where id = ?`,
    ).get(eventId) as {
      input: number | null;
      cacheRead: number | null;
      output: number | null;
      payloadJson: string;
    } | undefined;
    if (!row) return undefined;
    let cacheCreation = 0;
    try {
      const payload = JSON.parse(row.payloadJson) as { cacheCreationTokens?: unknown };
      if (Number.isSafeInteger(payload.cacheCreationTokens) && Number(payload.cacheCreationTokens) >= 0) {
        cacheCreation = Number(payload.cacheCreationTokens);
      }
    } catch {
      return undefined;
    }
    return {
      messageId,
      input: row.input ?? 0,
      cacheRead: row.cacheRead ?? 0,
      cacheCreation,
      output: row.output ?? 0,
    };
  }

  private durableUsageRevision(sessionId: string, messageId: string): TranscriptUsageRevision | undefined {
    const row = this.buffer.database.prepare(
      `select input_tokens as input, cache_read_tokens as cacheRead,
         cache_creation_tokens as cacheCreation, output_tokens as output
       from transcript_usage_revision_state
       where source = 'claude_code' and session_id = ? and message_key = ?`,
    ).get(sessionId, this.messageKey(messageId)) as Omit<TranscriptUsageRevision, "messageId"> | undefined;
    return row ? { messageId, ...row } : undefined;
  }

  private messageKey(messageId: string) {
    return crypto.createHash("sha256").update(messageId).digest("hex");
  }
}
