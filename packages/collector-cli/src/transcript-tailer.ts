import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import type { LocalEventBuffer } from "./buffer";
import { resolveGitContext } from "./git-context";
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
  filesParsed: number;
  sessionsSkippedLiveCovered: number;
  eventsAppended: number;
  tokensAppended: { input: number; cacheRead: number; output: number };
  parseErrors: number;
};

const UUID_RE = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/i;

export class TranscriptTailer {
  constructor(
    private readonly buffer: LocalEventBuffer,
    private readonly projectsDir = path.join(os.homedir(), ".claude", "projects"),
  ) {
    this.buffer.database.exec(
      `create table if not exists rollout_scan_state (
        file text primary key,
        size integer not null,
        scanned_at text not null
      )`,
    );
  }

  private parsedSize(file: string): number | undefined {
    const row = this.buffer.database
      .prepare(`select size from rollout_scan_state where file = ?`)
      .get(file) as { size: number } | undefined;
    return row?.size;
  }

  private rememberSize(file: string, size: number) {
    this.buffer.database
      .prepare(
        `insert into rollout_scan_state (file, size, scanned_at) values (?, ?, ?)
         on conflict(file) do update set size = excluded.size, scanned_at = excluded.scanned_at`,
      )
      .run(file, size, new Date().toISOString());
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
    const result: TranscriptScanResult = {
      filesSeen: 0,
      filesParsed: 0,
      sessionsSkippedLiveCovered: 0,
      eventsAppended: 0,
      tokensAppended: { input: 0, cacheRead: 0, output: 0 },
      parseErrors: 0,
    };
    const now = options.now ?? new Date();
    const recentCutoff = now.getTime() - 48 * 60 * 60 * 1000;
    for (const file of this.discover()) {
      result.filesSeen += 1;
      let stat: fs.Stats;
      try {
        stat = fs.statSync(file);
      } catch {
        continue;
      }
      if (options.recentOnly && stat.mtime.getTime() < recentCutoff) continue;
      if (this.parsedSize(file) === stat.size) continue;
      this.ingestFile(file, result);
      this.rememberSize(file, stat.size);
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

  private ingestFile(file: string, result: TranscriptScanResult) {
    let raw: string;
    try {
      raw = fs.readFileSync(file, "utf8");
    } catch {
      return;
    }
    let sessionId = path.basename(file).replace(/\.jsonl$/, "").match(UUID_RE)?.[0]?.toLowerCase();
    let cwd: string | undefined;
    // message id → usage snapshot (last wins: streamed/retried entries repeat ids)
    const usageById = new Map<
      string,
      { observedAt?: string; model?: string; input: number; cacheRead: number; cacheCreation: number; output: number }
    >();

    for (const line of raw.split("\n")) {
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
      if (!sessionId && typeof parsed.sessionId === "string") {
        sessionId = parsed.sessionId.match(UUID_RE)?.[0]?.toLowerCase();
      }
      if (!cwd && typeof parsed.cwd === "string") cwd = parsed.cwd;
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

    if (!sessionId || usageById.size === 0) return;
    if (this.sessionHasLiveTokens(sessionId)) {
      result.sessionsSkippedLiveCovered += 1;
      return;
    }
    result.filesParsed += 1;

    const git = cwd ? resolveGitContext(cwd) : undefined;
    if (git?.remoteUrlHash && git.remoteLabel) {
      this.buffer.recordRepoLabel(git.remoteUrlHash, git.remoteLabel); // local-only
    }
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
      if (git) {
        metadata.git = {
          remoteUrlHash: git.remoteUrlHash,
          branchHash: git.branchHash,
          headSha: git.headSha,
        };
      }
      const event: AiInteractionEvent = aiInteractionEventSchema.parse({
        id: deterministicEventId(["claude-transcript", sessionId, messageId]),
        tenantId: "local",
        source: "claude_code",
        dataMode: "metadata",
        eventType: "usage_transcript",
        observedAt: entry.observedAt ?? fallbackObservedAt,
        sessionId,
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
  }
}
