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
 *    and writes are insert-or-replace, so rescans are idempotent.
 *  - Privacy: only `session_meta` / `turn_context` / `token_count` lines are
 *    even JSON-parsed; message/reasoning lines are skipped by prefilter and
 *    no content field is ever read or persisted.
 *  - Dedupe vs OTLP is first-writer-wins per session: spans arrive within
 *    seconds while the tailer lags a scan interval, so a span-emitting
 *    session always has token-bearing non-rollout events by the time the
 *    tailer sees its rollout — those sessions are skipped (and counted).
 */

export type RolloutScanResult = {
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
  repriced: number;
};

type TokenTotals = { input: number; cachedInput: number; output: number; reasoningOutput: number };

type PersistedGitContext = {
  remoteUrlHash?: string;
  branchHash?: string;
  headSha?: string;
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
  git?: PersistedGitContext;
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
  const git = validatePersistedGit(value.git);
  if (value.git !== undefined && !git) return undefined;

  return {
    parserKind: PARSER_KIND,
    checkpointVersion: CHECKPOINT_VERSION,
    previous: { ...value.previous },
    tokenCountIndex: value.tokenCountIndex,
    ...(conversationId ? { conversationId: conversationId.toLowerCase() } : {}),
    ...(sessionStartedAt ? { sessionStartedAt } : {}),
    ...(originator ? { originator } : {}),
    ...(cliVersion ? { cliVersion } : {}),
    ...(model ? { model } : {}),
    ...(planType ? { planType } : {}),
    ...(git ? { git } : {}),
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

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const UUID_EXACT_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function conversationIdFromFilename(file: string) {
  const base = path.basename(file).replace(/\.jsonl$/, "");
  const match = base.match(UUID_RE);
  return match ? match[0].toLowerCase() : undefined;
}

export class RolloutTailer {
  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly sessionsDir = path.join(os.homedir(), ".codex", "sessions"),
    private readonly identityProvider: () => LocalIdentity[] = readLocalIdentities,
  ) {
    ensureJsonlScanState(this.buffer.database);
  }

  private codexIdentity: LocalIdentity | undefined;

  /**
   * Rate-table updates land after events exist (gpt-5.2 carried 12.5M
   * unpriced tokens before its rate was sourced — issue 0025). Reprice
   * usage_rollout rows whose cost is still null whenever their model becomes
   * priceable. Only null-cost rows are touched: vendor-reported and
   * previously estimated costs are never rewritten.
   */
  private repriceUnpriced(): number {
    const rows = this.buffer.database
      .prepare(
        `select id, model, input_tokens as inputTokens, output_tokens as outputTokens,
           cache_read_tokens as cacheReadTokens
         from buffered_events
         where event_type in ('usage_rollout','usage_transcript') and cost_usd is null and model is not null`,
      )
      .all() as Array<{
      id: string;
      model: string;
      inputTokens: number | null;
      outputTokens: number | null;
      cacheReadTokens: number | null;
    }>;
    if (rows.length === 0) return 0;
    const apply = this.buffer.database.prepare(
      `update buffered_events set cost_usd = @costUsd,
         payload_json = json_set(payload_json, '$.costUsd', @costUsd, '$.metadata.costEstimated', json('true'))
       where id = @id`,
    );
    let repriced = 0;
    for (const row of rows) {
      const priced = estimateCostUsd({
        model: row.model,
        inputTokens: row.inputTokens ?? 0,
        outputTokens: row.outputTokens ?? 0,
        cacheReadTokens: row.cacheReadTokens ?? 0,
      });
      if (!priced) continue;
      apply.run({ id: row.id, costUsd: priced.costUsd });
      repriced += 1;
    }
    return repriced;
  }

  /**
   * recentOnly limits discovery to today+yesterday (UTC) day directories.
   * Async on purpose: the collector serves HTTP on the same event loop, and
   * the first full-history walk reads thousands of files — yielding between
   * files keeps the dashboard responsive while a scan runs (owner-reported
   * freeze, sounding 0026).
   */
  async scan(options: { recentOnly?: boolean; now?: Date } = {}): Promise<RolloutScanResult> {
    const result: RolloutScanResult = {
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
      repriced: this.repriceUnpriced(),
    };
    try {
      this.codexIdentity = this.identityProvider().find((entry) => entry.source === "codex");
    } catch {
      this.codexIdentity = undefined;
    }
    if (this.codexIdentity?.actorHash && this.codexIdentity.email) {
      this.buffer.setAccountEmail(this.codexIdentity.actorHash, this.codexIdentity.email);
    }
    for (const file of this.discover(options)) {
      result.filesSeen += 1;
      let stat: fs.Stats;
      try {
        stat = fs.statSync(file);
      } catch {
        continue;
      }
      const cursor = loadJsonlScanCursor<RolloutParserState>(
        this.buffer.database,
        file,
        PARSER_KIND,
        CHECKPOINT_VERSION,
        validateRolloutParserState,
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

  private discover(options: { recentOnly?: boolean; now?: Date }): string[] {
    const now = options.now ?? new Date();
    const files: string[] = [];
    const dayDirs: string[] = [];
    if (options.recentOnly) {
      for (const offset of [0, 1]) {
        const day = new Date(now.getTime() - offset * 24 * 60 * 60 * 1000);
        const iso = day.toISOString().slice(0, 10);
        dayDirs.push(path.join(this.sessionsDir, ...iso.split("-")));
      }
    } else {
      // Full walk: sessions/YYYY/MM/DD — three bounded levels.
      for (const year of listDirs(this.sessionsDir)) {
        for (const month of listDirs(year)) {
          dayDirs.push(...listDirs(month));
        }
      }
    }
    for (const dir of dayDirs) {
      let entries: string[];
      try {
        entries = fs.readdirSync(dir);
      } catch {
        continue;
      }
      for (const entry of entries) {
        if (entry.startsWith("rollout-") && entry.endsWith(".jsonl")) {
          files.push(path.join(dir, entry));
        }
      }
    }
    return files.sort();
  }

  private sessionHasNonRolloutTokens(sessionId: string) {
    const row = this.buffer.database
      .prepare(
        `select 1 from buffered_events
         where source = 'codex' and session_id = ?
           and event_type != 'usage_rollout'
           and (input_tokens is not null or output_tokens is not null)
         limit 1`,
      )
      .get(sessionId);
    return Boolean(row);
  }

  private initialParserState(file: string): RolloutParserState {
    return {
      parserKind: PARSER_KIND,
      checkpointVersion: CHECKPOINT_VERSION,
      conversationId: conversationIdFromFilename(file),
      previous: { ...ZERO },
      tokenCountIndex: -1,
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
    result: RolloutScanResult,
    state: RolloutParserState,
  ) {
    const pending: Array<{
      index: number;
      observedAt: string | undefined;
      delta: TokenTotals;
      model: string | undefined;
    }> = [];

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
        result.parseErrors += 1; // usually a partial trailing line mid-write
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
        if (typeof payload.cwd === "string") state.git = this.safeGitContext(payload.cwd);
        if (typeof payload.originator === "string") state.originator = payload.originator;
        if (typeof payload.cli_version === "string") state.cliVersion = payload.cli_version;
      } else if (type === "turn_context") {
        if (typeof payload.model === "string" && payload.model) state.model = payload.model;
        if (typeof payload.cwd === "string") state.git = this.safeGitContext(payload.cwd);
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
        });
      }
    }

    if (!state.conversationId || pending.length === 0) return state;
    if (this.sessionHasNonRolloutTokens(state.conversationId)) {
      result.sessionsSkippedOtlpCovered += 1;
      return state;
    }
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
    const fallbackObservedAt = (() => {
      try {
        return fs.statSync(file).mtime.toISOString();
      } catch {
        return new Date().toISOString();
      }
    })();

    for (const entry of pending) {
      const priced = estimateCostUsd({
        model: entry.model,
        inputTokens: entry.delta.input,
        outputTokens: entry.delta.output,
        cacheReadTokens: entry.delta.cachedInput,
      });
      const metadata: Record<string, unknown> = {
        usageSource: "rollout",
        rolloutFile: path.basename(file),
        turnIndex: entry.index,
      };
      if (state.originator) metadata.originator = state.originator;
      if (state.cliVersion) metadata.cliVersion = state.cliVersion;
      if (state.planType) metadata.planType = state.planType;
      if (entry.delta.reasoningOutput > 0) metadata.reasoningOutputTokens = entry.delta.reasoningOutput;
      if (priced) metadata.costEstimated = true;
      // Hashed linkage keys ONLY — GitLinkageContext.remoteLabel is local
      // display data and must never enter event metadata (upload-proofed).
      if (state.git) {
        metadata.git = {
          remoteUrlHash: state.git.remoteUrlHash,
          branchHash: state.git.branchHash,
          headSha: state.git.headSha,
        };
      }

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
      this.buffer.append(event, []);
      result.eventsAppended += 1;
      result.tokensAppended.input += entry.delta.input;
      result.tokensAppended.cachedInput += entry.delta.cachedInput;
      result.tokensAppended.output += entry.delta.output;
    }
    return state;
  }
}

function listDirs(dir: string): string[] {
  try {
    return fs
      .readdirSync(dir, { withFileTypes: true })
      .filter((entry) => entry.isDirectory())
      .map((entry) => path.join(dir, entry.name));
  } catch {
    return [];
  }
}
