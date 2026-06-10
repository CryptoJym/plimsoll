/**
 * Backfill the plimsoll ledger from an archived v1 spool database.
 *
 * The v1 collector stored each OTLP HTTP POST as a single `otel_span` row whose
 * payload embeds the entire envelope. That flattening discarded per-record token
 * usage (audit 2026-06-10, finding F1). Because the envelopes were retained, the
 * history is recoverable: this script re-parses every archived envelope through
 * the v2 exploder and writes per-record events + metric samples into the live
 * ledger. Hook-origin rows are re-normalized so `actionClass` derives from
 * `tool_name` (finding F4) and codex `conversation.id` maps to sessionId.
 *
 * Only signal rows are kept from envelopes (usage-bearing records, tool events,
 * prompts/session boundaries, metric datapoints). Transport plumbing rows
 * (e.g. codex /models pings) are counted but skipped to keep the ledger small.
 *
 * Usage:
 *   tsx scripts/backfill-plimsoll-claude-envelopes.ts \
 *     --archive "/path/to/work-ledger-archive.sqlite" \
 *     [--target "/path/to/work-ledger.sqlite"] [--dry-run]
 *
 * Re-runs are idempotent: exploded rows use deterministic content-derived ids.
 */
import fs from "node:fs";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { collectorBufferPath } from "../packages/collector-cli/src/config";
import { normalizeHookPayload } from "../packages/collector-cli/src/normalizer";
import { explodeOtlpPayload } from "../packages/collector-cli/src/otlp";
import {
  DEFAULT_POLICY,
  type AiInteractionEvent,
  type ToolSource,
} from "../packages/shared/src/index";

const KEEP_EVENT_TYPES = new Set([
  "assistant_response",
  "session_start",
  "session_stop",
  "tool_result",
  "tool_use",
  "user_prompt_submit",
]);

const KEEP_METRIC_PREFIXES = [
  "claude_code.token.usage",
  "claude_code.cost.usage",
  "claude_code.lines_of_code",
  "claude_code.commit",
  "claude_code.pull_request",
  "claude_code.active_time",
  "claude_code.session.count",
  "codex.tool.call",
  "codex.turn",
];

function optionValue(name: string) {
  const index = process.argv.indexOf(name);
  return index === -1 ? undefined : process.argv[index + 1];
}

function isHookOriginRow(eventType: string) {
  return eventType !== "otel_span";
}

async function main() {
  const archivePath = optionValue("--archive");
  if (!archivePath || !fs.existsSync(archivePath)) {
    throw new Error("Pass --archive /path/to/archived-work-ledger.sqlite");
  }

  const targetPath = optionValue("--target") ?? collectorBufferPath();
  const dryRun = process.argv.includes("--dry-run");
  const policy = DEFAULT_POLICY;

  const archive = new Database(archivePath, { readonly: true });
  const target = dryRun ? null : new LocalEventBuffer(targetPath);

  const totals = {
    archivedRows: 0,
    envelopeRows: 0,
    hookRows: 0,
    keptEvents: 0,
    keptMetricSamples: 0,
    skippedPlumbingRecords: 0,
    parseFailures: 0,
    tokenAttributedEvents: 0,
    inputTokens: 0,
    outputTokens: 0,
    cacheReadTokens: 0,
    costUsd: 0,
    bySource: {} as Record<string, { events: number; inputTokens: number; outputTokens: number; costUsd: number }>,
  };

  const tally = (event: AiInteractionEvent) => {
    totals.keptEvents += 1;
    const bucket = (totals.bySource[event.source] ??= {
      events: 0,
      inputTokens: 0,
      outputTokens: 0,
      costUsd: 0,
    });
    bucket.events += 1;
    if (event.inputTokens !== undefined || event.outputTokens !== undefined) {
      totals.tokenAttributedEvents += 1;
      totals.inputTokens += event.inputTokens ?? 0;
      totals.outputTokens += event.outputTokens ?? 0;
      totals.cacheReadTokens += event.cacheReadTokens ?? 0;
      bucket.inputTokens += event.inputTokens ?? 0;
      bucket.outputTokens += event.outputTokens ?? 0;
    }
    if (event.costUsd !== undefined) {
      totals.costUsd += event.costUsd;
      bucket.costUsd += event.costUsd;
    }
  };

  const rows = archive
    .prepare(
      `select id, source, event_type as eventType, observed_at as observedAt, payload_json as payloadJson
       from buffered_events order by created_at asc`,
    )
    .iterate() as IterableIterator<{
    id: string;
    source: string;
    eventType: string;
    observedAt: string;
    payloadJson: string;
  }>;

  let pendingEvents: Array<{ event: AiInteractionEvent; suppressedFields: string[] }> = [];
  let pendingMetrics: NonNullable<Parameters<LocalEventBuffer["appendMany"]>[1]> = [];
  const flush = () => {
    if (!target) {
      pendingEvents = [];
      pendingMetrics = [];
      return;
    }
    if (pendingEvents.length === 0 && pendingMetrics.length === 0) return;
    target.appendMany(pendingEvents, pendingMetrics);
    pendingEvents = [];
    pendingMetrics = [];
  };

  for (const row of rows) {
    totals.archivedRows += 1;
    let payload: AiInteractionEvent;
    try {
      payload = JSON.parse(row.payloadJson) as AiInteractionEvent;
    } catch {
      totals.parseFailures += 1;
      continue;
    }

    if (isHookOriginRow(row.eventType)) {
      totals.hookRows += 1;
      const metadata = (payload.metadata ?? {}) as Record<string, unknown>;
      try {
        const renormalized = normalizeHookPayload(
          { ...metadata, id: payload.id },
          { policy, source: payload.source as ToolSource },
        );
        const event: AiInteractionEvent = {
          ...renormalized.event,
          observedAt: payload.observedAt,
          sessionId: renormalized.event.sessionId ?? payload.sessionId,
        };
        pendingEvents.push({ event, suppressedFields: [] });
        tally(event);
      } catch {
        totals.parseFailures += 1;
      }
    } else {
      totals.envelopeRows += 1;
      const envelope = (payload.metadata ?? {}) as Record<string, unknown>;
      const exploded = explodeOtlpPayload(envelope, {
        policy,
        source: payload.source as ToolSource,
        transportPath:
          typeof envelope.transport_path === "string" ? envelope.transport_path : undefined,
        // Never resolve git from archived workdirs: replaying old paths against
        // current git state would stamp today's HEAD onto historical sessions.
        resolveGit: false,
      });
      totals.parseFailures += exploded.parseFailures;
      for (const entry of exploded.events) {
        if (!KEEP_EVENT_TYPES.has(entry.event.eventType)) {
          totals.skippedPlumbingRecords += 1;
          continue;
        }
        pendingEvents.push(entry);
        tally(entry.event);
      }
      for (const sample of exploded.metricSamples) {
        if (!KEEP_METRIC_PREFIXES.some((prefix) => sample.metricName.startsWith(prefix))) {
          continue;
        }
        pendingMetrics.push(sample);
        totals.keptMetricSamples += 1;
      }
    }

    if (pendingEvents.length >= 2000 || pendingMetrics.length >= 2000) {
      flush();
    }
    if (totals.archivedRows % 10000 === 0) {
      console.error(
        JSON.stringify({
          progress: totals.archivedRows,
          keptEvents: totals.keptEvents,
          tokenAttributedEvents: totals.tokenAttributedEvents,
        }),
      );
    }
  }

  flush();
  archive.close();
  target?.close();

  const summary = {
    archivePath: path.resolve(archivePath),
    targetPath: dryRun ? null : path.resolve(targetPath),
    dryRun,
    completedAt: new Date().toISOString(),
    totals,
  };
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
