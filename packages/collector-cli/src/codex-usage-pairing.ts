import type Database from "better-sqlite3";

import { estimateCostUsd } from "../../shared/src/index";
import { refreshUnsentRawDelivery, retirePairedSpanDelivery } from "./outbox";

export const CODEX_USAGE_DUPLICATE_REASON = "codex_sse_event_span";
const PAIR_WINDOW_MS = 30_000;
const MAX_SPAN_DURATION_MS = 10 * 60_000;
const MAX_NEARBY_ROWS = 128;

type UsageRow = {
  rowid: number;
  id: string;
  source: string;
  eventType: string;
  observedAt: string;
  sessionId: string | null;
  model: string | null;
  inputTokens: number | null;
  outputTokens: number | null;
  cacheReadTokens: number | null;
  cacheCreationTokens: number | null;
  costUsd: number | null;
  costKind: string | null;
  accountHash: string | null;
  workspaceId: string | null;
  deviceId: string | null;
  payloadJson: string;
  usagePairedEventId: string | null;
};

type Shape = {
  kind: "log" | "span";
  traceId: string | null;
  spanEndAt: string | null;
};

function shape(row: UsageRow): Shape | null {
  try {
    const payload = JSON.parse(row.payloadJson) as {
      metadata?: Record<string, unknown>;
    };
    const metadata = payload.metadata ?? {};
    const name = metadata.otelEventName;
    const kind = name === "codex.sse_event" ? "log"
      : name === "handle_responses" ? "span" : null;
    if (!kind) return null;
    return {
      kind,
      traceId: typeof metadata.traceId === "string" ? metadata.traceId : null,
      spanEndAt: typeof metadata.otelSpanEndAt === "string" ? metadata.otelSpanEndAt : null,
    };
  } catch {
    return null;
  }
}

function isUsageRow(row: UsageRow) {
  return row.source === "codex" && row.eventType === "assistant_response" &&
    row.inputTokens !== null && row.outputTokens !== null &&
    row.usagePairedEventId === null;
}

function compatible(log: UsageRow, span: UsageRow, logShape: Shape, spanShape: Shape) {
  if (logShape.kind !== "log" || spanShape.kind !== "span" || !log.accountHash) return false;
  if (log.workspaceId && span.workspaceId && log.workspaceId !== span.workspaceId) return false;
  if (log.deviceId && span.deviceId && log.deviceId !== span.deviceId) return false;
  if (log.cacheReadTokens !== null && span.cacheReadTokens !== null &&
      log.cacheReadTokens !== span.cacheReadTokens) return false;
  if (logShape.traceId && spanShape.traceId) {
    if (logShape.traceId !== spanShape.traceId) return false;
  }
  // Real Codex usage logs have no trace context and response spans have no
  // session. Do not make the time_window session guess a pairing prerequisite.
  // Mutual uniqueness, exact counts and the bounded time window guard this key.
  const logTime = Date.parse(log.observedAt);
  const spanTime = Date.parse(span.observedAt);
  const endTime = spanShape.spanEndAt ? Date.parse(spanShape.spanEndAt) : NaN;
  if (!Number.isFinite(logTime) || !Number.isFinite(spanTime)) return false;
  return Math.min(Math.abs(logTime - spanTime),
    Number.isFinite(endTime) ? Math.abs(logTime - endTime) : Infinity) <= PAIR_WINDOW_MS;
}

const ROW_COLUMNS = `rowid, id, source, event_type as eventType,
  observed_at as observedAt, session_id as sessionId, model,
  input_tokens as inputTokens, output_tokens as outputTokens,
  cache_read_tokens as cacheReadTokens,
  cache_creation_tokens as cacheCreationTokens, cost_usd as costUsd,
  cost_kind as costKind, account_hash as accountHash,
  workspace_id as workspaceId, device_id as deviceId, payload_json as payloadJson,
  usage_paired_event_id as usagePairedEventId`;

function nearby(db: Database.Database, row: UsageRow, wanted: "log" | "span") {
  const time = Date.parse(row.observedAt);
  if (!Number.isFinite(time)) return [];
  const rowShape = shape(row);
  if (!rowShape) return [];
  // The span's observed_at is its start, while the SSE log is emitted near
  // completion. Include bounded long-running responses, then check the exact
  // span end and mutual uniqueness below.
  const spanEnd = rowShape.spanEndAt ? Date.parse(rowShape.spanEndAt) : NaN;
  const start = wanted === "span" ? time - MAX_SPAN_DURATION_MS : time - PAIR_WINDOW_MS;
  const end = wanted === "log" && Number.isFinite(spanEnd)
    ? Math.min(Math.max(time, spanEnd), time + MAX_SPAN_DURATION_MS) + PAIR_WINDOW_MS
    : time + PAIR_WINDOW_MS;
  const rows = db.prepare(
    `select ${ROW_COLUMNS} from buffered_events indexed by idx_events_observed
     where observed_at >= @start and observed_at <= @end
       and source = 'codex' and event_type = 'assistant_response'
       and input_tokens = @inputTokens and output_tokens = @outputTokens
       and account_hash is ${wanted === "span" ? "null" : "not null"}
       and usage_duplicate_reason is null and usage_paired_event_id is null
       and id != @id
     order by observed_at, id limit ${MAX_NEARBY_ROWS + 1}`,
  ).all({
    start: new Date(start).toISOString(),
    end: new Date(end).toISOString(),
    inputTokens: row.inputTokens,
    outputTokens: row.outputTokens,
    id: row.id,
  }) as UsageRow[];
  // Never pick a plausible-looking row from a truncated collision set.
  if (rows.length > MAX_NEARBY_ROWS) return [];
  return rows.filter((candidate) => {
    const candidateShape = shape(candidate);
    if (!candidateShape || candidateShape.kind !== wanted) return false;
    const log = wanted === "log" ? candidate : row;
    const span = wanted === "span" ? candidate : row;
    const logShape = wanted === "log" ? candidateShape : rowShape;
    const spanShape = wanted === "span" ? candidateShape : rowShape;
    return compatible(log, span, logShape, spanShape);
  });
}

export type CodexUsagePair = { logId: string; spanId: string };

/**
 * Pair only mutually unique candidates. If two genuine responses have equal
 * counts in one window and no shared trace, both stay eligible instead of
 * silently assigning a span to the wrong response.
 */
export function pairCodexUsageEvent(db: Database.Database, eventId: string): CodexUsagePair | null {
  const row = db.prepare(`select ${ROW_COLUMNS} from buffered_events where id = ?`)
    .get(eventId) as UsageRow | undefined;
  if (!row || !isUsageRow(row)) return null;
  const rowShape = shape(row);
  if (!rowShape) return null;
  const candidates = nearby(db, row, rowShape.kind === "log" ? "span" : "log");
  if (candidates.length !== 1) return null;
  const other = candidates[0]!;
  const reciprocal = nearby(db, other, rowShape.kind);
  if (reciprocal.length !== 1 || reciprocal[0]!.id !== row.id) return null;
  const log = rowShape.kind === "log" ? row : other;
  const span = rowShape.kind === "span" ? row : other;

  const cacheReadTokens = log.cacheReadTokens ?? span.cacheReadTokens;
  const cacheCreationTokens = log.cacheCreationTokens ?? span.cacheCreationTokens;
  let costUsd = log.costUsd;
  if (log.costKind === "estimated" && log.model) {
    costUsd = estimateCostUsd({
      model: log.model,
      inputTokens: log.inputTokens ?? 0,
      outputTokens: log.outputTokens ?? 0,
      cacheReadTokens: cacheReadTokens ?? 0,
      cacheCreationTokens: cacheCreationTokens ?? 0,
    })?.costUsd ?? costUsd;
  }
  const logPayload = JSON.parse(log.payloadJson) as Record<string, unknown>;
  if (cacheReadTokens !== null) logPayload.cacheReadTokens = cacheReadTokens;
  if (cacheCreationTokens !== null) logPayload.cacheCreationTokens = cacheCreationTokens;
  if (costUsd !== null && costUsd !== log.costUsd) logPayload.costUsd = costUsd;
  const pairedLog = db.prepare(
    `update buffered_events set usage_paired_event_id = @spanId,
       cache_read_tokens = @cacheReadTokens,
       cache_creation_tokens = @cacheCreationTokens,
       cost_usd = @costUsd, payload_json = @payloadJson
     where id = @logId and usage_paired_event_id is null`,
  ).run({
    logId: log.id, spanId: span.id, cacheReadTokens,
    cacheCreationTokens, costUsd, payloadJson: JSON.stringify(logPayload),
  }).changes;
  if (pairedLog !== 1) return null;
  const pairedSpan = db.prepare(
    `update buffered_events set usage_paired_event_id = @logId,
       usage_duplicate_reason = @reason, event_type = 'otel_span',
       input_tokens = null, output_tokens = null,
       cache_read_tokens = null, cache_creation_tokens = null,
       cost_usd = null
     where id = @spanId and usage_paired_event_id is null`,
  ).run({
    logId: log.id, spanId: span.id, reason: CODEX_USAGE_DUPLICATE_REASON,
  }).changes;
  if (pairedSpan !== 1) throw new Error("codex_usage_pair_lost_span");
  retirePairedSpanDelivery(db, span.id);
  refreshUnsentRawDelivery(db, log.id);
  return { logId: log.id, spanId: span.id };
}

/** Open work is O(1); historical rows are visited by rowid in maintenance. */
export function ensureCodexUsagePairingSchema(db: Database.Database) {
  db.exec(`create table if not exists codex_usage_pairing_control (
    singleton integer primary key check (singleton = 1),
    cursor_rowid integer not null default 0,
    target_rowid integer not null default 0,
    complete integer not null default 0,
    visited integer not null default 0,
    paired integer not null default 0
  );
  insert into codex_usage_pairing_control (singleton, target_rowid, complete)
    select 1, coalesce(max(rowid), 0),
      case when max(rowid) is null then 1 else 0 end from buffered_events
    where 1 = 1
    on conflict(singleton) do nothing;`);
}

export function runCodexUsagePairingBackfill(
  db: Database.Database,
  limit = 128,
  deadline: () => boolean = () => false,
) {
  const control = db.prepare(
    `select cursor_rowid as cursor, target_rowid as target, complete
     from codex_usage_pairing_control where singleton = 1`,
  ).get() as { cursor: number; target: number; complete: number };
  if (control.complete) return { visited: 0, paired: 0, complete: true };
  const rows = db.prepare(
    `select rowid, id, source, event_type as eventType,
       input_tokens as inputTokens, output_tokens as outputTokens
     from buffered_events
     where rowid > ? and rowid <= ? order by rowid limit ?`,
  ).all(control.cursor, control.target, Math.max(1, Math.min(limit, 512))) as
    Array<{ rowid: number; id: string; source: string; eventType: string;
      inputTokens: number | null; outputTokens: number | null }>;
  let cursor = control.cursor;
  let visited = 0;
  let paired = 0;
  for (const row of rows) {
    if (deadline()) break;
    if (row.source === "codex" && row.eventType === "assistant_response" &&
        row.inputTokens !== null && row.outputTokens !== null) {
      paired += pairCodexUsageEvent(db, row.id) ? 1 : 0;
    }
    cursor = row.rowid;
    visited += 1;
  }
  const complete = cursor >= control.target || rows.length === 0;
  db.prepare(
    `update codex_usage_pairing_control set cursor_rowid = ?,
       complete = ?, visited = visited + ?, paired = paired + ?
     where singleton = 1`,
  ).run(cursor, complete ? 1 : 0, visited, paired);
  return { visited, paired, complete };
}
