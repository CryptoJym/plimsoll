import type Database from "better-sqlite3";

import { estimateCostUsd } from "../../shared/src/index";
import { refreshUnsentRawDelivery, retirePairedSpanDelivery } from "./outbox";
import { isSqliteContentionError } from "./sqlite-contention";

export const CODEX_USAGE_DUPLICATE_REASON = "codex_sse_event_span";
const PAIR_WINDOW_MS = 30_000;
const MAX_SPAN_DURATION_MS = 10 * 60_000;
const MAX_NEARBY_ROWS = 128;
const HISTORICAL_DUPLICATE_START = "2026-09-11T00:00:00.000Z";
const BACKFILL_SPAN_PREDICATE = `case when source = 'codex'
  and event_type = 'assistant_response' and account_hash is null
  and input_tokens is not null and output_tokens is not null
  and usage_duplicate_reason is null then
    case when json_valid(payload_json)
      then json_extract(payload_json, '$.metadata.otelEventName') end
  end = 'handle_responses'`;
const LOG_MATCH_PREDICATE = `case when source = 'codex'
  and event_type = 'assistant_response'
  and input_tokens is not null and output_tokens is not null then
    case when json_valid(payload_json)
      then json_extract(payload_json, '$.metadata.otelEventName') end
  end = 'codex.sse_event'`;
const PAIRING_INDEXES = [
  "idx_codex_usage_span_backfill",
  "idx_codex_usage_span_match",
  "idx_codex_usage_log_match",
] as const;
const pairingReady = new WeakMap<Database.Database, boolean>();

/** Cheap status only: ordinary opens never build indexes or scan buffered_events. */
export function codexUsagePairingStatus(db: Database.Database) {
  const present = new Set((db.prepare(`select name from sqlite_master
    where type = 'index' and name in (?, ?, ?)`).all(...PAIRING_INDEXES) as
    Array<{ name: string }>).map((row) => row.name));
  const missingIndexes = PAIRING_INDEXES.filter((name) => !present.has(name));
  if (missingIndexes.length > 0) {
    return { enabled: false, reason: "pairing_indexes_missing_run_lifecycle_pairing_indexes",
      missingIndexes };
  }
  const controlExists = db.prepare(`select 1 from sqlite_master
    where type = 'table' and name = 'codex_usage_pairing_control'`).get();
  if (!controlExists) {
    return { enabled: false, reason: "pairing_control_missing_run_lifecycle_pairing_indexes",
      missingIndexes };
  }
  const control = db.prepare(`select target_rowid as targetRowid, complete
    from codex_usage_pairing_control where singleton = 1`).get() as
    { targetRowid: number; complete: number } | undefined;
  if (!control) {
    return { enabled: false, reason: "pairing_control_missing_run_lifecycle_pairing_indexes",
      missingIndexes };
  }
  // MAX(rowid) uses the rowid B-tree's right edge; it never scans payloads.
  // A prior 0.7.41 rollback can append usage after the last completed pass.
  const currentRowid = (db.prepare(`select coalesce(max(rowid), 0) as value
    from buffered_events`).get() as { value: number }).value;
  return {
    enabled: true,
    reason: control.targetRowid < currentRowid
      ? "historical_target_stale_run_lifecycle_pairing_indexes"
      : control.complete === 0 ? "historical_backfill_pending" : "ready",
    missingIndexes,
  };
}

function indexesReady(db: Database.Database) {
  let ready = pairingReady.get(db);
  if (ready === undefined) {
    ready = codexUsagePairingStatus(db).enabled;
    pairingReady.set(db, ready);
  }
  return ready;
}

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
  authoritativeSession: boolean;
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
      authoritativeSession: metadata.sessionLinkBasis === "otel_trace" ||
        metadata.sessionLinkBasis === "span_attribute",
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
  if (logShape.kind !== "log" || spanShape.kind !== "span") return false;
  if (log.workspaceId && span.workspaceId && log.workspaceId !== span.workspaceId) return false;
  if (log.deviceId && span.deviceId && log.deviceId !== span.deviceId) return false;
  if (log.cacheReadTokens !== null && span.cacheReadTokens !== null &&
      log.cacheReadTokens !== span.cacheReadTokens) return false;
  if (logShape.traceId && spanShape.traceId) {
    if (logShape.traceId !== spanShape.traceId) return false;
  }
  if (spanShape.authoritativeSession && span.sessionId && log.sessionId &&
      span.sessionId !== log.sessionId) return false;
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
  const index = wanted === "span" ? "idx_codex_usage_span_match"
    : "idx_codex_usage_log_match";
  const predicate = wanted === "span" ? BACKFILL_SPAN_PREDICATE : LOG_MATCH_PREDICATE;
  const windows = wanted === "span"
    ? [[time - MAX_SPAN_DURATION_MS, time + PAIR_WINDOW_MS]]
    : Number.isFinite(spanEnd) && spanEnd >= time &&
        spanEnd - time <= MAX_SPAN_DURATION_MS && spanEnd - time > PAIR_WINDOW_MS
      ? [[time - PAIR_WINDOW_MS, time + PAIR_WINDOW_MS],
         [spanEnd - PAIR_WINDOW_MS, spanEnd + PAIR_WINDOW_MS]]
      : [[time - PAIR_WINDOW_MS, time + PAIR_WINDOW_MS]];
  const query = db.prepare(
    `select ${ROW_COLUMNS} from buffered_events indexed by ${index}
     where observed_at >= @start and observed_at <= @end
       and ${predicate}
       and input_tokens = @inputTokens and output_tokens = @outputTokens
       and usage_duplicate_reason is null and usage_paired_event_id is null
       and id != @id
     order by observed_at, id limit ${MAX_NEARBY_ROWS + 1}`,
  );
  const found = new Map<string, UsageRow>();
  for (const [start, end] of windows) {
    for (const candidate of query.all({
      start: new Date(start!).toISOString(),
      end: new Date(end!).toISOString(),
      inputTokens: row.inputTokens,
      outputTokens: row.outputTokens,
      id: row.id,
    }) as UsageRow[]) found.set(candidate.id, candidate);
  }
  const rows = [...found.values()].sort((a, b) =>
    a.observedAt.localeCompare(b.observedAt) || a.id.localeCompare(b.id));
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

function commitPair(db: Database.Database, log: UsageRow, span: UsageRow): CodexUsagePair {
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
  if (pairedLog !== 1) throw new Error("codex_usage_pair_lost_log");
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

const sameIds = (left: UsageRow[], right: UsageRow[]) =>
  left.length === right.length &&
  left.every((row) => right.some((other) => other.id === row.id));

/** Pair a unique match, or an equal-size complete collision cluster in time order. */
export function pairCodexUsageEvent(
  db: Database.Database,
  eventId: string,
): (CodexUsagePair & { pairCount: number }) | null {
  if (!indexesReady(db)) return null;
  const row = db.prepare(`select ${ROW_COLUMNS} from buffered_events where id = ?`)
    .get(eventId) as UsageRow | undefined;
  if (!row || !isUsageRow(row)) return null;
  const rowShape = shape(row);
  if (!rowShape) return null;
  const otherKind = rowShape.kind === "log" ? "span" : "log";
  const candidates = nearby(db, row, otherKind);
  if (candidates.length === 0) return null;
  const reciprocal = nearby(db, candidates[0]!, rowShape.kind);
  let pairs: CodexUsagePair[];
  if (candidates.length === 1 && reciprocal.length === 1 && reciprocal[0]!.id === row.id) {
    pairs = [commitPair(db, rowShape.kind === "log" ? row : candidates[0]!,
      rowShape.kind === "span" ? row : candidates[0]!)];
  } else {
    // Every member must see the same complete bipartite candidate set. A
    // missing report makes the sizes differ, so no uncertain row is dropped.
    if (candidates.length < 2 || candidates.length > 16 ||
        reciprocal.length !== candidates.length ||
        !reciprocal.some((candidate) => candidate.id === row.id) ||
        !candidates.every((candidate) => sameIds(nearby(db, candidate, rowShape.kind), reciprocal)) ||
        !reciprocal.every((candidate) => sameIds(nearby(db, candidate, otherKind), candidates))) {
      return null;
    }
    const byTime = (a: UsageRow, b: UsageRow) =>
      a.observedAt.localeCompare(b.observedAt) || a.id.localeCompare(b.id);
    const rows = [...reciprocal].sort(byTime);
    const others = [...candidates].sort(byTime);
    pairs = rows.map((member, index) => {
      const other = others[index]!;
      return commitPair(db, rowShape.kind === "log" ? member : other,
        rowShape.kind === "span" ? member : other);
    });
  }
  const own = pairs.find((pair) => pair.logId === eventId || pair.spanId === eventId);
  return own ? { ...own, pairCount: pairs.length } : null;
}

/** Ordinary open only creates the O(1) control row, never a history index. */
export function ensureCodexUsagePairingSchema(db: Database.Database) {
  const eventColumns = new Set((db.pragma("table_info(buffered_events)") as
    Array<{ name: string }>).map((column) => column.name));
  if (!eventColumns.has("usage_duplicate_reason")) {
    db.exec("alter table buffered_events add column usage_duplicate_reason text");
  }
  if (!eventColumns.has("usage_paired_event_id")) {
    db.exec("alter table buffered_events add column usage_paired_event_id text");
  }
  db.exec(`create table if not exists codex_usage_pairing_control (
    singleton integer primary key check (singleton = 1),
    cursor_observed_at text not null default '',
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
  const columns = db.pragma("table_info(codex_usage_pairing_control)") as Array<{ name: string }>;
  if (!columns.some((column) => column.name === "cursor_observed_at")) {
    db.exec(`alter table codex_usage_pairing_control
      add column cursor_observed_at text not null default '';
      update codex_usage_pairing_control
      set cursor_rowid = 0, complete = 0;`);
  }
}

/** Run only on a new empty ledger or during the stopped-service upgrade window. */
export function buildCodexUsagePairingIndexes(db: Database.Database) {
  ensureCodexUsagePairingSchema(db);
  // The explicit upgrade also runs after a rollback to 0.7.41. Existing
  // indexes do not imply that its last historical target included legacy
  // writes, so refresh the bounded snapshot and revisit unmatched spans.
  db.exec(`update codex_usage_pairing_control
    set cursor_observed_at = '', cursor_rowid = 0,
        target_rowid = (select coalesce(max(rowid), 0) from buffered_events),
        complete = case when (select max(rowid) from buffered_events) is null then 1 else 0 end
    where singleton = 1`);
  const timings: Array<{ name: string; elapsedMs: number; created: boolean }> = [];
  // An accountless SSE log can be genuine usage; only the span side needs
  // historical discovery. The ordinary account index includes many unrelated
  // null-account rows and cannot make the upgrade bounded on a large ledger.
  const statements = [
    `create index if not exists idx_codex_usage_span_backfill
      on buffered_events (observed_at) where ${BACKFILL_SPAN_PREDICATE}`,
  // Keep the long-span lookback cheap at ingest while the time-first index
  // above preserves an ordered, resumable historical scan.
    `create index if not exists idx_codex_usage_span_match
      on buffered_events (input_tokens, output_tokens, observed_at)
      where ${BACKFILL_SPAN_PREDICATE}`,
    `create index if not exists idx_codex_usage_log_match
      on buffered_events (input_tokens, output_tokens, observed_at)
      where ${LOG_MATCH_PREDICATE}`,
  ];
  for (const [index, name] of PAIRING_INDEXES.entries()) {
    const existed = Boolean(db.prepare(`select 1 from sqlite_master where type = 'index' and name = ?`).get(name));
    const started = performance.now();
    db.exec(statements[index]!);
    timings.push({ name, elapsedMs: performance.now() - started, created: !existed });
  }
  pairingReady.set(db, true);
  return timings;
}

/** O(1) progress for the daemon's existing bounded repair-followup cadence. */
export function codexUsagePairingProgress(db: Database.Database) {
  if (!indexesReady(db)) return { pending: false, units: 0 };
  const row = db.prepare(`select complete, visited from codex_usage_pairing_control
    where singleton = 1`).get() as { complete: number; visited: number } | undefined;
  return { pending: row?.complete === 0, units: row?.visited ?? 0 };
}

export function runCodexUsagePairingBackfill(
  db: Database.Database,
  limit = 128,
  deadline: () => boolean = () => false,
) {
  if (!indexesReady(db)) return { visited: 0, paired: 0, complete: false };
  const control = db.prepare(
    `select cursor_observed_at as cursorAt, cursor_rowid as cursorRowid,
       target_rowid as target, complete
     from codex_usage_pairing_control where singleton = 1`,
  ).get() as { cursorAt: string; cursorRowid: number; target: number; complete: number };
  if (control.complete) return { visited: 0, paired: 0, complete: true };
  const rowLimit = Math.max(1, Math.min(limit, 512));
  const rows = db.prepare(
    `select rowid, id, observed_at as observedAt
     from buffered_events indexed by idx_codex_usage_span_backfill
     where ${BACKFILL_SPAN_PREDICATE} and observed_at >= @start
       and (observed_at > @cursorAt or
         (observed_at = @cursorAt and rowid > @cursorRowid))
       and rowid <= @target
     order by observed_at, rowid limit @limit`,
  ).all({
    start: HISTORICAL_DUPLICATE_START,
    cursorAt: control.cursorAt,
    cursorRowid: control.cursorRowid,
    target: control.target,
    limit: rowLimit + 1,
  }) as Array<{ rowid: number; id: string; observedAt: string }>;
  let cursorAt = control.cursorAt;
  let cursorRowid = control.cursorRowid;
  let visited = 0;
  let paired = 0;
  for (const row of rows.slice(0, rowLimit)) {
    if (deadline()) break;
    paired += pairCodexUsageEvent(db, row.id)?.pairCount ?? 0;
    cursorAt = row.observedAt;
    cursorRowid = row.rowid;
    visited += 1;
  }
  const complete = rows.length === 0 ||
    (visited === Math.min(rows.length, rowLimit) && rows.length <= rowLimit);
  db.prepare(
    `update codex_usage_pairing_control set cursor_observed_at = ?, cursor_rowid = ?,
       complete = ?, visited = visited + ?, paired = paired + ?
     where singleton = 1`,
  ).run(cursorAt, cursorRowid, complete ? 1 : 0, visited, paired);
  return { visited, paired, complete };
}

/** A cadence may visit several candidates, but releases the writer after each one. */
export function runCodexUsagePairingWriterSlice(
  db: Database.Database,
  options: { maxMs?: number; maxCandidates?: number; canStart?: () => boolean;
    clock?: () => number } = {},
) {
  if (db.inTransaction) throw new Error("codex_pairing_writer_slice_requires_outer_commit");
  const clock = options.clock ?? (() => performance.now());
  const started = clock();
  const deadline = started + Math.max(1, Math.min(options.maxMs ?? 15, 100));
  const maxCandidates = Math.max(1, Math.min(options.maxCandidates ?? 32, 128));
  let visited = 0;
  let paired = 0;
  let transactions = 0;
  let maxTransactionMs = 0;
  let maxWriterHoldMs = 0;
  let maxLockWaitMs = 0;
  let complete = false;
  let deferredForWriter = false;
  const priorBusyTimeout = db.pragma("busy_timeout", { simple: true }) as number;
  const priorAutoCheckpoint = db.pragma("wal_autocheckpoint", { simple: true }) as number;
  db.pragma("busy_timeout = 0");
  // A checkpoint can turn one tiny candidate commit into a large writer hold.
  // The collector's separate WAL checkpoint stage owns that work.
  db.pragma("wal_autocheckpoint = 0");
  try {
    while (visited < maxCandidates && clock() < deadline && (options.canStart?.() ?? true)) {
      const transactionStarted = clock();
      let lockAcquired = transactionStarted;
      let result: ReturnType<typeof runCodexUsagePairingBackfill>;
      try {
        result = db.transaction(() => {
          lockAcquired = clock();
          return runCodexUsagePairingBackfill(db, 1, () => clock() >= deadline);
        }).immediate();
      } catch (error) {
        if (!isSqliteContentionError(error)) throw error;
        deferredForWriter = true;
        break;
      }
      const transactionEnded = clock();
      maxTransactionMs = Math.max(maxTransactionMs, transactionEnded - transactionStarted);
      maxLockWaitMs = Math.max(maxLockWaitMs, lockAcquired - transactionStarted);
      maxWriterHoldMs = Math.max(maxWriterHoldMs, transactionEnded - lockAcquired);
      transactions += 1;
      visited += result.visited;
      paired += result.paired;
      complete = result.complete;
      if (result.visited === 0 || complete) break;
    }
  } finally {
    db.pragma(`wal_autocheckpoint = ${priorAutoCheckpoint}`);
    db.pragma(`busy_timeout = ${priorBusyTimeout}`);
  }
  return { visited, paired, complete, transactions,
    elapsedMs: clock() - started, maxTransactionMs, maxWriterHoldMs, maxLockWaitMs,
    deferredForWriter };
}
