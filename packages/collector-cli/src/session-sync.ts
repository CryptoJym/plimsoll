import Database from "better-sqlite3";

import type { CollectorConfig } from "./config";
import { collectorBufferPath } from "./config";
import { deterministicEventId } from "./normalizer";
import { hasUnsafeOutboundString, sealOutboundSessionRow } from "./outbound-envelope";
import { chunkHistoryEnvelopes, postHistoryBatch } from "./upload-history";
import {
  aiWorkSessionSyncBatchSchema,
  type AiWorkIngestBatch,
  type AiWorkSessionSyncRow,
} from "../../shared/src/index";

/**
 * Session sync (issue 0037 / cloud Phase D1): the ledger stitches sessions
 * from events (session_id on every row — issue 0008's repo stitching rides
 * the same ids), but the hosted workspace's AiWorkSession table held 0 rows,
 * so per-session and per-person analytics had nothing to stand on. This
 * module recomputes one SNAPSHOT per session from the ledger and pushes it
 * over the existing ingest transport (postHistoryBatch — same auth, retries,
 * fail-closed semantics) as a `kind: "session_sync"` batch.
 *
 * Invariants (the upload-history house rules):
 * - The ledger is opened strictly READ-ONLY (or borrowed from the daemon's
 *   live handle for the 5-minute-sync path — reads only). Nothing here marks
 *   rows or touches collector.config.json.
 * - Idempotency comes from deterministic session ids, not local state: the
 *   cloud upserts by id with a grow-only guard, so re-sending the same
 *   snapshots updates rows in place with identical values — run twice over
 *   the same --until, nothing changes (the cloud reports what it did:
 *   inserted/updated/skippedStale). No resume watermark: the whole walk is
 *   cheap (thousands of sessions, not hundreds of thousands of events).
 * - Privacy parity: only canonical linkage hashes, privacy-safe actor aliases
 *   and typed counters cross. Raw non-UUID session ids are deterministically
 *   replaced and never leave the machine. The shared outbound sealer runs
 *   both while rows are built and immediately before batch construction.
 * - Honest numbers: costUsd sums PRICED events only; pricedEvents says how
 *   many. An unpriced session renders "unpriced" in the audit, never $0.00.
 */

/**
 * Postgres accepts any RFC-shaped hex UUID for a uuid column regardless of
 * version bits (the upload-history event-id rule). Session ids must follow
 * the SAME passthrough or the session row id stops matching what the event
 * lane stored: claude session ids (UUIDv4) live in events.session_id, codex
 * session ids (UUIDv7) live as their UUID value — both pass through verbatim.
 * Lowercased because Postgres normalizes uuid text output to
 * lowercase, keeping text-level joins exact.
 */
const POSTGRES_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Deterministic UUID for ledger session ids the cloud's uuid column would
 * reject. Same ledger id → same UUID on every run, so cloud upserts dedupe
 * re-sends without exporting the original. The namespace part
 * ("session-sync") is deliberately distinct from the event
 * lane's "workspace-backfill", so a session id that happens to equal some
 * event's raw id can never collide into the same derived UUID.
 */
export function ensureUuidSessionId(rawId: string): { id: string; derived: boolean } {
  if (POSTGRES_UUID_RE.test(rawId)) {
    return { id: rawId.toLowerCase(), derived: false };
  }
  return { id: deterministicEventId(["session-sync", rawId]), derived: true };
}

export type SessionSnapshot = {
  sessionId: string;
  source: string;
  startedAt: string;
  endedAt: string;
  events: number;
  inputTokens: number;
  outputTokens: number;
  cacheReadTokens: number;
  cacheCreationTokens: number;
  pricedEvents: number;
  costUsd: number;
  repoHash: string | null;
  branchHash: string | null;
  accountHash: string | null;
};

/**
 * One aggregate row per session, recomputed from the ledger. Scope is
 * created_at <= until (the upload-history watermark semantics) so two runs
 * over the same --until see the IDENTICAL snapshot set — that is what makes
 * the idempotent re-run proof exact while the daemon keeps appending.
 *
 * Attribution within a session: projectKey/branchHash come from the session's
 * DOMINANT (repo, branch) pair by event count (ties broken lexically for
 * determinism); the account hash likewise. Sessions are single-source in
 * practice (one tool per session id) — max(source) only breaks impossible
 * ties deterministically.
 */
export function collectSessionSnapshots(
  ledger: Database.Database,
  options: { until: string; sessionIds?: string[] },
): SessionSnapshot[] {
  // SQLite caps bind variables (999 on conservative builds). A daemon cycle
  // can touch up to 5×500 events; chunk the id filter well under the cap.
  if (options.sessionIds && options.sessionIds.length > 400) {
    const out: SessionSnapshot[] = [];
    for (let start = 0; start < options.sessionIds.length; start += 400) {
      out.push(
        ...collectSessionSnapshots(ledger, {
          until: options.until,
          sessionIds: options.sessionIds.slice(start, start + 400),
        }),
      );
    }
    return out;
  }

  const filters: string[] = ["e.session_id is not null", "e.created_at <= @until"];
  const params: Record<string, unknown> = { until: options.until };
  if (options.sessionIds && options.sessionIds.length > 0) {
    const names = options.sessionIds.map((_, index) => `@sid${index}`);
    filters.push(`e.session_id in (${names.join(", ")})`);
    options.sessionIds.forEach((value, index) => {
      params[`sid${index}`] = value;
    });
  }

  const rows = ledger
    .prepare(
      `select
         e.session_id as sessionId,
         max(e.source) as source,
         min(e.observed_at) as startedAt,
         max(e.observed_at) as endedAt,
         count(*) as events,
         coalesce(sum(e.input_tokens), 0) as inputTokens,
         coalesce(sum(e.output_tokens), 0) as outputTokens,
         coalesce(sum(e.cache_read_tokens), 0) as cacheReadTokens,
         coalesce(sum(e.cache_creation_tokens), 0) as cacheCreationTokens,
         sum(case when e.cost_usd is not null then 1 else 0 end) as pricedEvents,
         coalesce(sum(e.cost_usd), 0) as costUsd,
         (select r.repo_hash || '|' || coalesce(r.branch_hash, '')
            from buffered_events r
            where r.session_id = e.session_id and r.repo_hash is not null
              and r.created_at <= @until
            group by r.repo_hash, r.branch_hash
            order by count(*) desc, r.repo_hash asc, r.branch_hash asc
            limit 1) as dominantRepoPair,
         (select a.account_hash
            from buffered_events a
            where a.session_id = e.session_id and a.account_hash is not null
              and a.created_at <= @until
            group by a.account_hash
            order by count(*) desc, a.account_hash asc
            limit 1) as accountHash
       from buffered_events e
       where ${filters.join(" and ")}
       group by e.session_id
       order by min(e.observed_at) asc`,
    )
    .all(params) as Array<
    Omit<SessionSnapshot, "repoHash" | "branchHash"> & { dominantRepoPair: string | null }
  >;

  return rows.map((row) => {
    const { dominantRepoPair, ...rest } = row;
    // "sha256:…|sha256:…" — "|" never appears inside the hash alphabet.
    const [repoHash, branchHash] = dominantRepoPair
      ? dominantRepoPair.split("|", 2)
      : [null, null];
    return {
      ...rest,
      repoHash: repoHash || null,
      branchHash: branchHash || null,
    };
  });
}

export type SessionSkipReason = "source_invalid" | "schema_invalid" | "forbidden_content";

export type NormalizedSessionRow =
  | { ok: true; row: AiWorkSessionSyncRow; bytes: number; idDerived: boolean }
  | { ok: false; reason: SessionSkipReason; detail: string };

/** Snapshot → wire row. Anything that fails the strict schema or carries a
 * forbidden metadata field is skipped with a reason — never silently. */
export function buildSessionSyncRow(snapshot: SessionSnapshot): NormalizedSessionRow {
  if (hasUnsafeOutboundString(snapshot.sessionId)) {
    return { ok: false, reason: "forbidden_content", detail: "unsafe session id" };
  }
  const ensured = ensureUuidSessionId(snapshot.sessionId);
  const metadata: Record<string, unknown> = {};
  if (snapshot.branchHash) metadata.branchHash = snapshot.branchHash;
  if (snapshot.accountHash) metadata.externalActorId = snapshot.accountHash;

  const candidate = {
    session: {
      id: ensured.id,
      source: snapshot.source,
      dataMode: "metadata",
      startedAt: snapshot.startedAt,
      endedAt: snapshot.endedAt,
      ...(snapshot.repoHash ? { projectKey: snapshot.repoHash } : {}),
      intent: "unknown",
      metadata,
    },
    totals: {
      events: snapshot.events,
      inputTokens: snapshot.inputTokens ?? 0,
      outputTokens: snapshot.outputTokens ?? 0,
      cacheReadTokens: snapshot.cacheReadTokens ?? 0,
      cacheCreationTokens: snapshot.cacheCreationTokens ?? 0,
      pricedEvents: snapshot.pricedEvents ?? 0,
      // Sums of float costs can pick up 1e-18-scale negative dust; clamp,
      // never invent: a session with no priced events keeps costUsd 0 AND
      // pricedEvents 0 — the audit renders that as "unpriced".
      costUsd: Math.max(0, snapshot.costUsd ?? 0),
    },
  };

  const sealed = sealOutboundSessionRow(candidate);
  if (!sealed.ok) {
    const sourceValid = [
      "anthropic_admin", "anthropic_usage", "claude_code", "codex",
      "github", "openai_usage", "manual", "unknown",
    ].includes(snapshot.source);
    const reason: SessionSkipReason = !sourceValid
      ? "source_invalid"
      : sealed.reason === "privacy"
        ? "forbidden_content"
        : "schema_invalid";
    return { ok: false, reason, detail: `shared outbound sealer: ${sealed.reason}` };
  }

  return {
    ok: true,
    row: sealed.row,
    bytes: Buffer.byteLength(JSON.stringify(sealed.row)),
    idDerived: ensured.derived,
  };
}

/** Distinct non-null session ids across just-uploaded event batches — the
 * daemon's "sessions touched since the last sync" set (no extra state). */
export function sessionIdsFromBatches(batches: Array<AiWorkIngestBatch | null>): string[] {
  const ids = new Set<string>();
  for (const batch of batches) {
    for (const entry of batch?.events ?? []) {
      const sessionId = entry.event.sessionId;
      if (typeof sessionId === "string" && sessionId.trim()) ids.add(sessionId);
    }
  }
  return [...ids];
}

export type SessionAuditCell = {
  sessions: number;
  events: number;
  inputTokens: number;
  outputTokens: number;
  pricedEvents: number;
  costUsd: number;
  sentSessions: number;
  acceptedSessions: number;
};

export type SessionAudit = {
  cells: Record<string, SessionAuditCell>;
  skipped: Record<string, number>;
  derivedIds: number;
};

export function createSessionAudit(): SessionAudit {
  return { cells: {}, skipped: {}, derivedIds: 0 };
}

function sessionAuditCell(audit: SessionAudit, key: string): SessionAuditCell {
  audit.cells[key] = audit.cells[key] ?? {
    sessions: 0,
    events: 0,
    inputTokens: 0,
    outputTokens: 0,
    pricedEvents: 0,
    costUsd: 0,
    sentSessions: 0,
    acceptedSessions: 0,
  };
  return audit.cells[key];
}

/** Audit cells key by source × UTC month of startedAt ("codex|2026-05") —
 * the upload-history reconciliation shape, at session grain. */
export function sessionAuditKey(row: AiWorkSessionSyncRow): string {
  return `${row.session.source}|${row.session.startedAt.slice(0, 7)}`;
}

export function recordSessionEligible(audit: SessionAudit, row: AiWorkSessionSyncRow): void {
  const cell = sessionAuditCell(audit, sessionAuditKey(row));
  cell.sessions += 1;
  cell.events += row.totals.events;
  cell.inputTokens += row.totals.inputTokens;
  cell.outputTokens += row.totals.outputTokens;
  cell.pricedEvents += row.totals.pricedEvents;
  cell.costUsd += row.totals.costUsd;
}

export function recordSessionOutcome(
  audit: SessionAudit,
  rows: AiWorkSessionSyncRow[],
  accepted: boolean,
): void {
  for (const row of rows) {
    const cell = sessionAuditCell(audit, sessionAuditKey(row));
    cell.sentSessions += 1;
    if (accepted) cell.acceptedSessions += 1;
  }
}

export function renderSessionAudit(audit: SessionAudit): string {
  const header = [
    "source",
    "month",
    "sessions",
    "events",
    "input tok",
    "output tok",
    "cost (priced rows)",
    "sent",
    "accepted",
  ];
  const renderCost = (cell: SessionAuditCell) =>
    cell.pricedEvents === 0
      ? "unpriced"
      : `$${cell.costUsd.toFixed(2)} (${cell.pricedEvents}/${cell.events} priced)`;
  const keys = Object.keys(audit.cells).sort((a, b) => {
    const [sourceA, monthA] = a.split("|");
    const [sourceB, monthB] = b.split("|");
    return monthA === monthB ? sourceA.localeCompare(sourceB) : monthA.localeCompare(monthB);
  });
  const totals = {
    sessions: 0,
    events: 0,
    inputTokens: 0,
    outputTokens: 0,
    pricedEvents: 0,
    costUsd: 0,
    sentSessions: 0,
    acceptedSessions: 0,
  };
  const rows: string[][] = keys.map((key) => {
    const [source, month] = key.split("|");
    const cell = audit.cells[key];
    totals.sessions += cell.sessions;
    totals.events += cell.events;
    totals.inputTokens += cell.inputTokens;
    totals.outputTokens += cell.outputTokens;
    totals.pricedEvents += cell.pricedEvents;
    totals.costUsd += cell.costUsd;
    totals.sentSessions += cell.sentSessions;
    totals.acceptedSessions += cell.acceptedSessions;
    return [
      source,
      month,
      String(cell.sessions),
      String(cell.events),
      String(cell.inputTokens),
      String(cell.outputTokens),
      renderCost(cell),
      String(cell.sentSessions),
      String(cell.acceptedSessions),
    ];
  });
  rows.push([
    "TOTAL",
    "",
    String(totals.sessions),
    String(totals.events),
    String(totals.inputTokens),
    String(totals.outputTokens),
    renderCost(totals),
    String(totals.sentSessions),
    String(totals.acceptedSessions),
  ]);

  const widths = header.map((_, column) =>
    Math.max(header[column].length, ...rows.map((row) => row[column].length)),
  );
  const renderRow = (row: string[]) =>
    row.map((value, column) => value.padEnd(widths[column])).join("  ").trimEnd();
  const lines = [renderRow(header), widths.map((width) => "-".repeat(width)).join("  ")];
  for (const row of rows) lines.push(renderRow(row));

  const skippedTotal = Object.values(audit.skipped).reduce((sum, count) => sum + count, 0);
  lines.push("");
  lines.push(`sessions skipped (NOT uploaded): ${skippedTotal}`);
  for (const [reason, count] of Object.entries(audit.skipped).sort((a, b) =>
    a[0].localeCompare(b[0]),
  )) {
    lines.push(`  - ${reason}: ${count}`);
  }
  if (audit.derivedIds > 0) {
    lines.push(`session ids deterministically re-derived to UUID: ${audit.derivedIds}`);
  }
  return lines.join("\n");
}

export type SessionSyncOptions = {
  /** Only ledger rows with created_at <= until count toward snapshots.
   * Default: now at start. Re-use run 1's value to prove idempotency over an
   * identical snapshot set. */
  until?: string;
  /** Restrict to these ledger session ids (the daemon's touched-set path).
   * Omit for the full walk (the backfill path). */
  sessionIds?: string[];
  batchSize?: number;
  concurrency?: number;
  delayMs?: number;
  /** Audit only: walk + normalize + reconcile, zero network. */
  dryRun?: boolean;
  url?: string;
  appVersion?: string;
  ledgerPath?: string;
  /** Borrow an already-open handle (the daemon's live buffer) instead of
   * opening the ledger file read-only. Reads only; never closed here. */
  ledgerDb?: Database.Database;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  log?: (line: string) => void;
  maxAttemptsPerBatch?: number;
};

export type SessionSyncResult = {
  ok: boolean;
  reason: string | null;
  until: string;
  ledgerSessions: number;
  eligibleSessions: number;
  skippedSessions: number;
  sentSessions: number;
  acceptedSessions: number;
  /** Server-reported genuinely-new rows (null when the server never reported the field). */
  insertedSessions: number | null;
  /** Server-reported in-place snapshot updates (null when unreported). */
  updatedSessions: number | null;
  batches: number;
  derivedIds: number;
  durationMs: number;
  dryRun: boolean;
  audit: SessionAudit;
  auditTable: string;
};

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * The session push. Walks the ledger's stitched sessions (full walk, or just
 * the daemon's touched set), normalizes snapshots to wire rows, ships
 * ≤500-session signed batches with bounded concurrency over the SAME
 * transport as upload-history, and prints a source × month reconciliation
 * audit. Read-only, stateless, idempotent — re-running over the same --until
 * sends identical snapshots the cloud upserts in place.
 */
export async function runSessionSync(
  config: CollectorConfig,
  options: SessionSyncOptions = {},
): Promise<SessionSyncResult> {
  const log = options.log ?? ((line: string) => console.log(line));
  const sleep = options.sleep ?? defaultSleep;
  const fetchImpl = options.fetchImpl ?? fetch;

  const url = options.url ?? config.uploadUrl;
  if (!url) {
    throw new Error(
      "This machine has not joined a workspace (no uploadUrl in collector.config.json). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry upload-history --sessions.',
    );
  }
  if ((!config.installKey || config.installKey === "local-dev") && !config.ingestKey) {
    throw new Error(
      "No workspace install credentials found (installKey is missing/local-dev and there is no ingestKey). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry upload-history --sessions.',
    );
  }

  let ledger = options.ledgerDb ?? null;
  let ownsLedger = false;
  if (!ledger) {
    const ledgerPath = options.ledgerPath ?? collectorBufferPath();
    try {
      ledger = new Database(ledgerPath, { readonly: true, fileMustExist: true });
      ownsLedger = true;
    } catch (error) {
      throw new Error(
        `No readable local ledger at ${ledgerPath} (${error instanceof Error ? error.message : String(error)}) — nothing to sync.`,
      );
    }
  }

  const startedAt = Date.now();
  const until = options.until ?? new Date().toISOString();
  if (Number.isNaN(Date.parse(until))) {
    if (ownsLedger) ledger.close();
    throw new Error(`--until must be an ISO timestamp, got: ${until}`);
  }
  const batchSize = Math.max(1, Math.min(options.batchSize ?? 500, 500));
  const concurrency = Math.max(1, Math.min(options.concurrency ?? 2, 8));
  const delayMs = Math.max(0, options.delayMs ?? 100);
  const maxAttempts = Math.max(1, Math.min(options.maxAttemptsPerBatch ?? 5, 10));
  const appVersion = options.appVersion ?? "0.1.0";

  const audit = createSessionAudit();
  let snapshots: SessionSnapshot[];
  try {
    snapshots = collectSessionSnapshots(ledger, { until, sessionIds: options.sessionIds });
  } finally {
    if (ownsLedger) ledger.close();
  }

  log(
    JSON.stringify({
      status: "session_sync_start",
      until,
      ledgerSessions: snapshots.length,
      scopedToTouched: Boolean(options.sessionIds),
      batchSize,
      concurrency,
      dryRun: Boolean(options.dryRun),
    }),
  );

  const eligible: Array<{ row: AiWorkSessionSyncRow; bytes: number }> = [];
  let derivedIds = 0;
  for (const snapshot of snapshots) {
    const normalized = buildSessionSyncRow(snapshot);
    if (!normalized.ok) {
      audit.skipped[normalized.reason] = (audit.skipped[normalized.reason] ?? 0) + 1;
      continue;
    }
    if (normalized.idDerived) derivedIds += 1;
    recordSessionEligible(audit, normalized.row);
    eligible.push({ row: normalized.row, bytes: normalized.bytes });
  }
  audit.derivedIds = derivedIds;

  const chunks = chunkHistoryEnvelopes(eligible, { maxEvents: batchSize });

  let sentSessions = 0;
  let acceptedSessions = 0;
  let insertedSessions: number | null = null;
  let updatedSessions: number | null = null;
  let batches = 0;
  let abortReason: string | null = null;

  const inFlight = new Set<Promise<void>>();
  const dispatch = async (chunk: Array<{ row: AiWorkSessionSyncRow }>) => {
    // Batch-level reseal prevents a future alternate caller from bypassing
    // buildSessionSyncRow and placing raw identifiers into a signed request.
    const sealedRows = chunk.map((item) => sealOutboundSessionRow(item.row));
    if (sealedRows.some((item) => !item.ok)) {
      abortReason = abortReason ?? "Session batch failed the shared outbound sealer.";
      return;
    }
    const rows = sealedRows.flatMap((item) => item.ok ? [item.row] : []);
    const body = JSON.stringify(
      aiWorkSessionSyncBatchSchema.parse({
        kind: "session_sync",
        tenantId: config.tenantId,
        installKey: config.installKey,
        appVersion,
        sessions: rows,
      }),
    );
    const task = (async () => {
      try {
        const result = await postHistoryBatch({
          url,
          body,
          installKey: config.installKey,
          ingestKey: config.ingestKey,
          signingSecret: config.uploadSigningSecret,
          fetchImpl,
          sleep,
          maxAttempts,
          log,
        });
        batches += 1;
        sentSessions += rows.length;
        acceptedSessions += result.accepted;
        if (result.inserted !== null) insertedSessions = (insertedSessions ?? 0) + result.inserted;
        if (result.updated !== null) updatedSessions = (updatedSessions ?? 0) + result.updated;
        recordSessionOutcome(audit, rows, true);
        log(
          JSON.stringify({
            status: "session_sync_progress",
            batches,
            sentSessions,
            acceptedSessions,
            insertedSessions,
            updatedSessions,
          }),
        );
      } catch (error) {
        abortReason = abortReason ?? (error instanceof Error ? error.message : String(error));
      }
    })();
    const tracked: Promise<void> = task.finally(() => {
      inFlight.delete(tracked);
    });
    inFlight.add(tracked);
    if (inFlight.size >= concurrency) {
      await Promise.race([...inFlight]);
    }
    if (delayMs > 0) await sleep(delayMs);
  };

  for (const chunk of chunks) {
    if (abortReason !== null) break;
    if (options.dryRun) {
      batches += 1;
      sentSessions += chunk.length;
      continue;
    }
    await dispatch(chunk);
  }

  await Promise.allSettled([...inFlight]);

  const durationMs = Date.now() - startedAt;
  const skippedSessions = Object.values(audit.skipped).reduce((sum, count) => sum + count, 0);
  const result: SessionSyncResult = {
    ok: abortReason === null,
    reason: abortReason,
    until,
    ledgerSessions: snapshots.length,
    eligibleSessions: eligible.length,
    skippedSessions,
    sentSessions,
    acceptedSessions,
    insertedSessions,
    updatedSessions,
    batches,
    derivedIds,
    durationMs,
    dryRun: Boolean(options.dryRun),
    audit,
    auditTable: renderSessionAudit(audit),
  };

  log(
    JSON.stringify({
      status: abortReason ? "session_sync_failed" : "session_sync_done",
      reason: abortReason,
      until,
      ledgerSessions: result.ledgerSessions,
      eligibleSessions: result.eligibleSessions,
      skippedSessions,
      sentSessions,
      acceptedSessions,
      insertedSessions,
      updatedSessions,
      batches,
      durationMs,
      dryRun: result.dryRun,
    }),
  );
  log("");
  log(result.auditTable);

  return result;
}
