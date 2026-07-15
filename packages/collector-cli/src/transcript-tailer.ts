import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { LocalEventBuffer } from "./buffer";
import { resolveGitContext } from "./git-context";
import {
  ensureJsonlScanState,
  loadJsonlScanCursor,
  readJsonlTail,
  rememberJsonlScanCursor,
} from "./jsonl-byte-tailer";
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
  filesSeen: number;
  filesRead: number;
  filesParsed: number;
  filesReset: number;
  legacyRebuilds: number;
  checkpointRebuilds: number;
  bytesRead: number;
  bytesDeferred: number;
  sessionsSkippedLiveCovered: number;
  eventsAppended: number;
  tokensAppended: { input: number; cacheRead: number; output: number };
  parseErrors: number;
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
  parserKind: "claude-transcript-v2";
  checkpointVersion: 2;
  sessionId?: string;
  git?: PersistedGitContext;
};

const PARSER_KIND = "claude-transcript-v2";
const CHECKPOINT_VERSION = 2;

function validateTranscriptParserState(value: unknown): TranscriptParserState | undefined {
  if (!isRecord(value)) return undefined;
  if (!hasOnlyKeys(value, ["parserKind", "checkpointVersion", "sessionId", "git"])) {
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
  return {
    parserKind: PARSER_KIND,
    checkpointVersion: CHECKPOINT_VERSION,
    ...(value.sessionId ? { sessionId: value.sessionId.toLowerCase() } : {}),
    ...(git ? { git } : {}),
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

export class TranscriptTailer {
  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly projectsDir = path.join(os.homedir(), ".claude", "projects"),
  ) {
    ensureJsonlScanState(this.buffer.database);
  }

  private sessionHasLiveTokens(sessionId: string) {
    const row = this.buffer.database
      .prepare(
        `select 1 from buffered_events
         where source = 'claude_code' and session_id = ?
           and event_type not in ('usage_rollout','usage_transcript')
           and (input_tokens is not null or output_tokens is not null)
         limit 1`,
      )
      .get(sessionId);
    return Boolean(row);
  }

  async scan(options: { recentOnly?: boolean; now?: Date } = {}): Promise<TranscriptScanResult> {
    const scanNow = options.now ?? new Date();
    const today = scanNow.toISOString().slice(0, 10);
    const result: TranscriptScanResult = {
      filesSeen: 0,
      filesRead: 0,
      filesParsed: 0,
      filesReset: 0,
      legacyRebuilds: 0,
      checkpointRebuilds: 0,
      bytesRead: 0,
      bytesDeferred: 0,
      sessionsSkippedLiveCovered: 0,
      eventsAppended: 0,
      tokensAppended: { input: 0, cacheRead: 0, output: 0 },
      parseErrors: 0,
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
    for (const file of this.discover()) {
      result.filesSeen += 1;
      result.activity.discoveryEntries += 1;
      let stat: fs.Stats;
      try {
        stat = fs.statSync(file);
      } catch {
        continue;
      }
      const mtime = stat.mtime.toISOString();
      if (!result.activity.lastActivityAt || mtime > result.activity.lastActivityAt) {
        result.activity.lastActivityAt = mtime;
      }
      if (mtime.slice(0, 10) === today) result.activity.filesToday += 1;
      if (options.recentOnly && stat.mtime.getTime() < recentCutoff) continue;
      const cursor = loadJsonlScanCursor<TranscriptParserState>(
        this.buffer.database,
        file,
        PARSER_KIND,
        CHECKPOINT_VERSION,
        validateTranscriptParserState,
      );
      let read: ReturnType<typeof readJsonlTail>;
      try {
        read = readJsonlTail(file, stat, cursor);
      } catch {
        // Rotation may remove a file between stat and open. One vanished file
        // must not abort the rest of the discovery set.
        continue;
      }
      if (!read) {
        result.bytesDeferred += cursor?.deferredBytes ?? 0;
        continue;
      }
      result.filesRead += 1;
      result.bytesRead += read.bytesRead;
      result.bytesDeferred += read.deferredBytes;
      if (read.reset) result.filesReset += 1;
      if (read.legacyRebuild) result.legacyRebuilds += 1;
      if (read.checkpointRebuild) result.checkpointRebuilds += 1;

      const initialState = read.reset || !cursor?.parserState
        ? this.initialParserState(file)
        : cursor.parserState;
      const commit = this.buffer.database.transaction(() => {
        const parserState = this.ingestLines(file, read.lines, result, initialState);
        rememberJsonlScanCursor(
          this.buffer.database,
          file,
          PARSER_KIND,
          CHECKPOINT_VERSION,
          read,
          parserState,
        );
      });
      commit();
      await new Promise((resolve) => setImmediate(resolve));
    }
    return result;
  }

  private discover(limit = 100_000): string[] {
    const files: string[] = [];
    const stack = [this.projectsDir];
    let seen = 0;
    while (stack.length > 0) {
      const dir = stack.pop()!;
      let entries: fs.Dirent[];
      try {
        entries = fs.readdirSync(dir, { withFileTypes: true });
      } catch {
        continue;
      }
      for (const entry of entries) {
        if (seen++ > limit) return files.sort();
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) stack.push(full);
        else if (entry.name.endsWith(".jsonl")) files.push(full);
      }
    }
    return files.sort();
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
  ) {
    // message id → usage snapshot (last wins: streamed/retried entries repeat ids)
    const usageById = new Map<
      string,
      { observedAt?: string; model?: string; input: number; cacheRead: number; cacheCreation: number; output: number }
    >();

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
      usageById.set(messageId, {
        observedAt: typeof parsed.timestamp === "string" ? parsed.timestamp : undefined,
        model: typeof message.model === "string" ? message.model : undefined,
        input: num("input_tokens"),
        cacheRead: num("cache_read_input_tokens"),
        cacheCreation: num("cache_creation_input_tokens"),
        output: num("output_tokens"),
      });
    }

    if (!state.sessionId || usageById.size === 0) return state;
    if (this.sessionHasLiveTokens(state.sessionId)) {
      result.sessionsSkippedLiveCovered += 1;
      return state;
    }
    result.filesParsed += 1;

    const fallbackObservedAt = (() => {
      try {
        return fs.statSync(file).mtime.toISOString();
      } catch {
        return new Date().toISOString();
      }
    })();

    for (const [messageId, entry] of usageById) {
      if (entry.input === 0 && entry.output === 0 && entry.cacheRead === 0) continue;
      const priced = estimateCostUsd({
        model: entry.model,
        inputTokens: entry.input,
        outputTokens: entry.output,
        cacheReadTokens: entry.cacheRead,
        cacheCreationTokens: entry.cacheCreation,
      });
      const metadata: Record<string, unknown> = {
        usageSource: "transcript",
        transcriptFile: path.basename(file),
      };
      if (priced) metadata.costEstimated = true;
      if (state.git) {
        metadata.git = {
          remoteUrlHash: state.git.remoteUrlHash,
          branchHash: state.git.branchHash,
          headSha: state.git.headSha,
        };
      }
      const event: AiInteractionEvent = aiInteractionEventSchema.parse({
        id: deterministicEventId(["claude-transcript", state.sessionId, messageId]),
        tenantId: "local",
        source: "claude_code",
        dataMode: "metadata",
        eventType: "usage_transcript",
        observedAt: entry.observedAt ?? fallbackObservedAt,
        sessionId: state.sessionId,
        model: entry.model,
        actionClass: "other",
        inputTokens: entry.input,
        outputTokens: entry.output,
        cacheReadTokens: entry.cacheRead,
        cacheCreationTokens: entry.cacheCreation > 0 ? entry.cacheCreation : undefined,
        costUsd: priced?.costUsd,
        metadata,
      });
      this.buffer.append(event, []);
      result.eventsAppended += 1;
      result.tokensAppended.input += entry.input;
      result.tokensAppended.cacheRead += entry.cacheRead;
      result.tokensAppended.output += entry.output;
    }
    return state;
  }
}
