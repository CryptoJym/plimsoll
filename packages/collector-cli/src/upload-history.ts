import crypto from "node:crypto";
import fs from "node:fs";

import Database from "better-sqlite3";

import type { CollectorConfig } from "./config";
import { collectorBufferPath, collectorLogPath } from "./config";
import { deterministicEventId } from "./normalizer";
import {
  aiWorkAttributionRepairBatchSchema,
  aiWorkIngestBatchSchema,
  aiInteractionEventSchema,
  findForbiddenRawContentFields,
  type AiInteractionEvent,
  type AiWorkAttributionRepairRow,
} from "../../shared/src/index";

/**
 * Workspace backfill (issue 0035): push the ENTIRE local ledger history to the
 * hosted workspace ingest, idempotently, with a reconciliation audit.
 *
 * Direction note — in this repo "backfill" means INTO the ledger
 * (scan-rollouts, backfill-v1-archive); the cloud direction is "upload".
 * Hence this ships as `upload-history`, a sibling of `upload`, not a mode of
 * the local archive backfill.
 *
 * Invariants:
 * - The ledger is opened strictly READ-ONLY. The live daemon keeps writing it
 *   (WAL) and keeps draining its own 5-minute sync; nothing here marks rows
 *   uploaded or touches collector.config.json.
 * - Idempotency comes from event ids, not from local state: the cloud dedupes
 *   by id (bulk createMany(skipDuplicates) since cloud PR #19; per-event
 *   upserts before that), so re-sending the same history can never create new
 *   rows. The resume watermark is a fast-forward optimization, never a
 *   correctness mechanism.
 * - Privacy parity: the wire envelope is exactly the recent-buffer upload
 *   shape (upload.ts) — payload + suppressedFields — re-validated against the
 *   strict event schema and the forbidden-raw-content gate before send.
 * - Honest numbers: unpriced events stay unpriced in the audit; cost is only
 *   summed over events that carry a real costUsd.
 */

/**
 * Postgres accepts any RFC-shaped hex UUID for a uuid column regardless of
 * version bits, and the daemon uploads ledger ids verbatim — so passthrough
 * must accept exactly what Postgres accepts. Re-deriving an id the daemon
 * could upload as-is would split one ledger row into two cloud rows.
 */
const POSTGRES_UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/**
 * Deterministic UUID for ledger ids the cloud's uuid column would reject
 * (e.g. pre-normalizer hook ids). Same ledger id → same UUID on every run, so
 * the cloud ingest dedupes re-sends; the original id is preserved in
 * metadata.externalEventId. Reuses the repo's deterministicEventId (sha256 →
 * version-5/variant-9 shape) with a fixed namespace part.
 */
export function ensureUuidEventId(rawId: string): { id: string; derived: boolean } {
  if (POSTGRES_UUID_RE.test(rawId)) {
    return { id: rawId, derived: false };
  }
  return { id: deterministicEventId(["workspace-backfill", rawId]), derived: true };
}

export type LedgerHistoryRow = {
  rowid: number;
  id: string;
  createdAt: string;
  payloadJson: string;
  suppressedFieldsJson: string;
  /** Per-event repo linkage columns (issue 0008 stitching) — forwarded as
   * event.projectKey / metadata.branchHash (issue 0036). */
  repoHash?: string | null;
  branchHash?: string | null;
};

export type HistorySkipReason = "payload_unparseable" | "schema_invalid" | "forbidden_content";

export type HistoryEnvelope = {
  event: AiInteractionEvent;
  suppressedFields: string[];
};

export type NormalizedHistoryEvent =
  | { ok: true; envelope: HistoryEnvelope; bytes: number; idDerived: boolean }
  | { ok: false; reason: HistorySkipReason; detail: string };

/**
 * Row → wire envelope. The payload is the captured truth and crosses the wire
 * unchanged, with two repairs the strict schema demands:
 * - top-level nulls are dropped (reconcileCodexUsage's json_set writes
 *   `sessionId: null` when no stitch neighbor exists; the schema wants the
 *   key absent — these rows otherwise wedge any oldest-first drain);
 * - non-UUID ids are deterministically re-derived (see ensureUuidEventId).
 * Everything else that fails the schema or carries forbidden raw-content
 * metadata is skipped with a reason — never silently.
 */
export function normalizeHistoryEvent(row: {
  payloadJson: string;
  suppressedFieldsJson: string;
  repoHash?: string | null;
  branchHash?: string | null;
}): NormalizedHistoryEvent {
  let payload: unknown;
  try {
    payload = JSON.parse(row.payloadJson);
  } catch (error) {
    return {
      ok: false,
      reason: "payload_unparseable",
      detail: error instanceof Error ? error.message : String(error),
    };
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return { ok: false, reason: "schema_invalid", detail: "payload is not an object" };
  }

  const candidate: Record<string, unknown> = { ...(payload as Record<string, unknown>) };
  for (const key of Object.keys(candidate)) {
    if (candidate[key] === null) delete candidate[key];
  }

  let idDerived = false;
  if (typeof candidate.id === "string" && candidate.id.trim()) {
    const ensured = ensureUuidEventId(candidate.id.trim());
    if (ensured.derived) {
      idDerived = true;
      const metadata =
        candidate.metadata && typeof candidate.metadata === "object" && !Array.isArray(candidate.metadata)
          ? (candidate.metadata as Record<string, unknown>)
          : {};
      candidate.metadata = { ...metadata, externalEventId: candidate.id };
      candidate.id = ensured.id;
    }
  }

  // Project attribution parity (issue 0036): forward the ledger's repo
  // linkage as projectKey, exactly like the live sync path (upload.ts
  // attachRepoLinkage). Never overwrites a payload-supplied projectKey.
  if (row.repoHash && !candidate.projectKey) {
    candidate.projectKey = row.repoHash;
    if (row.branchHash) {
      const metadata =
        candidate.metadata && typeof candidate.metadata === "object" && !Array.isArray(candidate.metadata)
          ? (candidate.metadata as Record<string, unknown>)
          : {};
      candidate.metadata = { ...metadata, branchHash: row.branchHash };
    }
  }

  const parsed = aiInteractionEventSchema.safeParse(candidate);
  if (!parsed.success) {
    const issue = parsed.error.issues[0];
    return {
      ok: false,
      reason: "schema_invalid",
      detail: issue ? `${issue.path.join(".") || "(root)"}: ${issue.code}` : "unknown issue",
    };
  }

  // Defense in depth before anything crosses the wire: the cloud rejects the
  // whole batch on forbidden fields; skip the row locally instead.
  const forbidden = findForbiddenRawContentFields(parsed.data.metadata);
  if (forbidden.length > 0) {
    return { ok: false, reason: "forbidden_content", detail: forbidden.join(",") };
  }

  let suppressedFields: string[] = [];
  try {
    const parsedSuppressed = JSON.parse(row.suppressedFieldsJson) as unknown;
    if (Array.isArray(parsedSuppressed)) {
      suppressedFields = parsedSuppressed.filter(
        (field): field is string => typeof field === "string" && field.trim().length > 0,
      );
    }
  } catch {
    suppressedFields = [];
  }

  const envelope: HistoryEnvelope = { event: parsed.data, suppressedFields };
  return { ok: true, envelope, bytes: JSON.stringify(envelope).length, idDerived };
}

export const HISTORY_MAX_BATCH_EVENTS = 500;
export const HISTORY_MAX_BATCH_BYTES = 1_500_000;

/**
 * Order-preserving chunking under the ingest contract: ≤500 events per batch
 * (aiWorkIngestBatchSchema max) and a byte budget mirroring listUnuploaded's
 * spool cap. A single oversized envelope still ships alone — the cap splits
 * batches, it never drops events.
 */
export function chunkHistoryEnvelopes<T extends { bytes: number }>(
  items: T[],
  limits: { maxEvents?: number; maxBytes?: number } = {},
): T[][] {
  const maxEvents = Math.max(1, Math.min(limits.maxEvents ?? HISTORY_MAX_BATCH_EVENTS, HISTORY_MAX_BATCH_EVENTS));
  const maxBytes = Math.max(1, limits.maxBytes ?? HISTORY_MAX_BATCH_BYTES);
  const batches: T[][] = [];
  let current: T[] = [];
  let currentBytes = 0;
  for (const item of items) {
    const wouldOverflow =
      current.length >= maxEvents || (current.length > 0 && currentBytes + item.bytes > maxBytes);
    if (wouldOverflow) {
      batches.push(current);
      current = [];
      currentBytes = 0;
    }
    current.push(item);
    currentBytes += item.bytes;
  }
  if (current.length > 0) batches.push(current);
  return batches;
}

export type HistoryAuditCell = {
  localEvents: number;
  inputTokens: number;
  outputTokens: number;
  pricedEvents: number;
  costUsd: number;
  sentEvents: number;
  acceptedEvents: number;
};

export type HistoryAudit = {
  cells: Record<string, HistoryAuditCell>;
  skipped: Record<string, number>;
  derivedIds: number;
  /** Sum of the server's additive `inserted` field (cloud PR #19 fast lane):
   * rows that were genuinely NEW server-side. Re-sends of already-present ids
   * report 0 — the run-2 idempotency proof reads `inserted: 0` here. */
  insertedEvents?: number;
  /** Completed batches that actually reported `inserted` — distinguishes a
   * real zero from a server that predates the field. */
  insertedBatchesReported?: number;
};

export function createHistoryAudit(): HistoryAudit {
  return { cells: {}, skipped: {}, derivedIds: 0 };
}

function emptyCell(): HistoryAuditCell {
  return {
    localEvents: 0,
    inputTokens: 0,
    outputTokens: 0,
    pricedEvents: 0,
    costUsd: 0,
    sentEvents: 0,
    acceptedEvents: 0,
  };
}

/** Audit cells key by source × UTC month of observedAt ("codex|2026-05"). */
export function historyAuditKey(event: Pick<AiInteractionEvent, "source" | "observedAt">): string {
  return `${event.source}|${event.observedAt.slice(0, 7)}`;
}

function auditCell(audit: HistoryAudit, key: string): HistoryAuditCell {
  audit.cells[key] = audit.cells[key] ?? emptyCell();
  return audit.cells[key];
}

export function recordHistoryEligible(audit: HistoryAudit, event: AiInteractionEvent): void {
  const cell = auditCell(audit, historyAuditKey(event));
  cell.localEvents += 1;
  cell.inputTokens += event.inputTokens ?? 0;
  cell.outputTokens += event.outputTokens ?? 0;
  if (typeof event.costUsd === "number") {
    cell.pricedEvents += 1;
    cell.costUsd += event.costUsd;
  }
}

export function recordHistoryOutcome(
  audit: HistoryAudit,
  events: Array<Pick<AiInteractionEvent, "source" | "observedAt">>,
  accepted: boolean,
): void {
  for (const event of events) {
    const cell = auditCell(audit, historyAuditKey(event));
    cell.sentEvents += 1;
    if (accepted) cell.acceptedEvents += 1;
  }
}

export function recordHistorySkip(audit: HistoryAudit, reason: HistorySkipReason, count = 1): void {
  if (count <= 0) return;
  audit.skipped[reason] = (audit.skipped[reason] ?? 0) + count;
}

/** Fold a per-segment audit delta into a cumulative audit (resume bookkeeping). */
export function mergeHistoryAudit(target: HistoryAudit, delta: HistoryAudit): void {
  for (const [key, cell] of Object.entries(delta.cells)) {
    const into = auditCell(target, key);
    into.localEvents += cell.localEvents;
    into.inputTokens += cell.inputTokens;
    into.outputTokens += cell.outputTokens;
    into.pricedEvents += cell.pricedEvents;
    into.costUsd += cell.costUsd;
    into.sentEvents += cell.sentEvents;
    into.acceptedEvents += cell.acceptedEvents;
  }
  for (const [reason, count] of Object.entries(delta.skipped)) {
    recordHistorySkip(target, reason as HistorySkipReason, count);
  }
  target.derivedIds += delta.derivedIds;
}

export function historyAuditTotals(audit: HistoryAudit): HistoryAuditCell {
  const totals = emptyCell();
  for (const cell of Object.values(audit.cells)) {
    totals.localEvents += cell.localEvents;
    totals.inputTokens += cell.inputTokens;
    totals.outputTokens += cell.outputTokens;
    totals.pricedEvents += cell.pricedEvents;
    totals.costUsd += cell.costUsd;
    totals.sentEvents += cell.sentEvents;
    totals.acceptedEvents += cell.acceptedEvents;
  }
  return totals;
}

export function historySkippedTotal(audit: HistoryAudit): number {
  return Object.values(audit.skipped).reduce((sum, count) => sum + count, 0);
}

/**
 * The reconciliation table. Cost renders only when priced events exist for the
 * cell — an unpriced month shows "unpriced", never a fabricated $0.00
 * (issue 0025's doctrine).
 */
export function renderHistoryAudit(audit: HistoryAudit): string {
  const header = [
    "source",
    "month",
    "local events",
    "input tok",
    "output tok",
    "cost (priced rows)",
    "sent",
    "accepted",
  ];
  const renderCost = (cell: HistoryAuditCell) =>
    cell.pricedEvents === 0
      ? "unpriced"
      : `$${cell.costUsd.toFixed(2)} (${cell.pricedEvents}/${cell.localEvents} priced)`;
  const keys = Object.keys(audit.cells).sort((a, b) => {
    const [sourceA, monthA] = a.split("|");
    const [sourceB, monthB] = b.split("|");
    return monthA === monthB ? sourceA.localeCompare(sourceB) : monthA.localeCompare(monthB);
  });
  const rows: string[][] = keys.map((key) => {
    const [source, month] = key.split("|");
    const cell = audit.cells[key];
    return [
      source,
      month,
      String(cell.localEvents),
      String(cell.inputTokens),
      String(cell.outputTokens),
      renderCost(cell),
      String(cell.sentEvents),
      String(cell.acceptedEvents),
    ];
  });
  const totals = historyAuditTotals(audit);
  rows.push([
    "TOTAL",
    "",
    String(totals.localEvents),
    String(totals.inputTokens),
    String(totals.outputTokens),
    renderCost(totals),
    String(totals.sentEvents),
    String(totals.acceptedEvents),
  ]);

  const widths = header.map((_, column) =>
    Math.max(header[column].length, ...rows.map((row) => row[column].length)),
  );
  const renderRow = (row: string[]) =>
    row.map((value, column) => value.padEnd(widths[column])).join("  ").trimEnd();
  const lines = [renderRow(header), widths.map((width) => "-".repeat(width)).join("  ")];
  for (const row of rows) lines.push(renderRow(row));

  lines.push("");
  lines.push(`skipped (NOT uploaded): ${historySkippedTotal(audit)}`);
  for (const [reason, count] of Object.entries(audit.skipped).sort((a, b) => a[0].localeCompare(b[0]))) {
    lines.push(`  - ${reason}: ${count}`);
  }
  if (audit.derivedIds > 0) {
    lines.push(`ids deterministically re-derived to UUID: ${audit.derivedIds}`);
  }
  if ((audit.insertedBatchesReported ?? 0) > 0) {
    lines.push(
      `server-reported NEW rows this backfill: ${audit.insertedEvents ?? 0} ` +
        `(accepted-but-already-present re-sends insert nothing — that is the idempotency working)`,
    );
  }
  return lines.join("\n");
}

export type WorkspaceBackfillState = {
  version: 1;
  target: string;
  untilCreatedAt: string;
  watermark: { rowid: number; id: string; createdAt: string } | null;
  startedAt: string;
  updatedAt: string;
  completedAt: string | null;
  audit: HistoryAudit;
};

/** Fingerprint of where uploads go — never the credentials themselves. */
export function backfillTargetFingerprint(uploadUrl: string, tenantId: string): string {
  return `sha256:${crypto.createHash("sha256").update(`${uploadUrl}|${tenantId}`).digest("hex").slice(0, 16)}`;
}

export function defaultBackfillStatePath(homeDir?: string): string {
  return collectorLogPath("workspace-backfill-state.json", homeDir);
}

export function readBackfillState(statePath: string): WorkspaceBackfillState | null {
  if (!fs.existsSync(statePath)) return null;
  try {
    const parsed = JSON.parse(fs.readFileSync(statePath, "utf8")) as WorkspaceBackfillState;
    if (parsed.version !== 1 || typeof parsed.target !== "string") return null;
    return parsed;
  } catch {
    return null;
  }
}

export function writeBackfillState(statePath: string, state: WorkspaceBackfillState): void {
  fs.writeFileSync(statePath, `${JSON.stringify(state, null, 2)}\n`, { mode: 0o600 });
}

export type WorkspaceHistoryUploadOptions = {
  /** Only rows with created_at <= until are in scope. Default: now at start (a resumed run keeps its stored scope). Re-use run 1's value to prove idempotency over an identical set. */
  until?: string;
  batchSize?: number;
  maxBatchBytes?: number;
  /** Parallel in-flight batches, 1–8. Default 1; the live route is ~300ms/event server-side, so large histories want 4–8. */
  concurrency?: number;
  /** Pause between batch dispatches. Default 250ms — be kind to the hosted ingest. */
  delayMs?: number;
  /** Stop after roughly this many dispatched events (smoke runs). The watermark still advances over completed batches. */
  limit?: number;
  /** Ignore any resume watermark and re-walk the full history (the idempotency proof mode). */
  full?: boolean;
  /** Audit only: walk + normalize + reconcile, zero network, zero state writes. */
  dryRun?: boolean;
  url?: string;
  appVersion?: string;
  ledgerPath?: string;
  statePath?: string;
  fetchImpl?: typeof fetch;
  sleep?: (ms: number) => Promise<void>;
  log?: (line: string) => void;
  now?: () => Date;
  maxAttemptsPerBatch?: number;
  pageSize?: number;
};

export type WorkspaceHistoryUploadResult = {
  ok: boolean;
  reason: string | null;
  until: string;
  scannedRows: number;
  eligibleEvents: number;
  skippedEvents: number;
  sentEvents: number;
  acceptedEvents: number;
  /** Server-reported genuinely-new rows (null when the server never reported the field). */
  insertedEvents: number | null;
  batchesSent: number;
  derivedIds: number;
  durationMs: number;
  eventsPerSecond: number;
  completed: boolean;
  resumedFromRowid: number | null;
  watermark: WorkspaceBackfillState["watermark"];
  audit: HistoryAudit;
  auditTable: string;
  statePath: string | null;
  dryRun: boolean;
};

class FatalUploadError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "FatalUploadError";
  }
}

function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * One signed POST of one prepared batch. The signature is computed per attempt
 * (fresh timestamp over the exact body string — the server enforces a 5-minute
 * skew window). 429/5xx/network retry with exponential backoff honoring
 * Retry-After; signature/auth/payload errors (400/401/403) FAIL CLOSED — they
 * mean credentials or schema drift, and retrying cannot fix either.
 */
async function postHistoryBatch(input: {
  url: string;
  body: string;
  installKey: string;
  ingestKey?: string;
  signingSecret?: string;
  fetchImpl: typeof fetch;
  sleep: (ms: number) => Promise<void>;
  maxAttempts: number;
  log: (line: string) => void;
}): Promise<{
  accepted: number;
  inserted: number | null;
  matched: number | null;
  updated: number | null;
  attempts: number;
}> {
  let lastError = "";
  for (let attempt = 1; attempt <= input.maxAttempts; attempt += 1) {
    const headers: Record<string, string> = {
      "content-type": "application/json",
      "x-plimsoll-install-key": input.installKey,
    };
    if (input.ingestKey) headers["x-plimsoll-ingest-key"] = input.ingestKey;
    if (input.signingSecret) {
      const timestamp = new Date().toISOString();
      const digest = crypto
        .createHmac("sha256", input.signingSecret)
        .update(`${timestamp}.${input.body}`)
        .digest("hex");
      headers["x-plimsoll-upload-timestamp"] = timestamp;
      headers["x-plimsoll-upload-signature"] = `sha256=${digest}`;
    }

    let response: Response | null = null;
    try {
      response = await input.fetchImpl(input.url, { method: "POST", headers, body: input.body });
    } catch (error) {
      lastError = `network: ${error instanceof Error ? error.message : String(error)}`;
    }

    if (response?.ok) {
      const body = (await response.json().catch(() => ({}))) as {
        accepted?: unknown;
        inserted?: unknown;
        matched?: unknown;
        updated?: unknown;
      };
      const accepted = typeof body.accepted === "number" ? body.accepted : 0;
      // Additive field from the cloud's bulk-ingest fast lane (cloud PR #19):
      // how many rows were genuinely new. Older servers omit it.
      const inserted = typeof body.inserted === "number" ? body.inserted : null;
      // Attribution-repair lane responses (issue 0036).
      const matched = typeof body.matched === "number" ? body.matched : null;
      const updated = typeof body.updated === "number" ? body.updated : null;
      return { accepted, inserted, matched, updated, attempts: attempt };
    }

    if (response && response.status !== 429 && response.status < 500) {
      // NB: ingest error bodies can echo request fields — surface only the
      // server's error code, never the raw body (it can contain the install key).
      const errorBody = (await response.json().catch(() => ({}))) as { error?: unknown };
      const errorCode = typeof errorBody.error === "string" ? errorBody.error : "unknown_error";
      throw new FatalUploadError(
        `Workspace ingest refused the batch with HTTP ${response.status} (${errorCode}). ` +
          (response.status === 401 || response.status === 403
            ? "This is an auth/signature failure — check that this machine's join credentials (installKey / uploadSigningSecret in collector.config.json) still match the workspace. Failing closed; nothing further was uploaded."
            : "The batch payload was rejected — schema drift between collector and cloud. Failing closed; nothing further was uploaded."),
      );
    }

    if (response) {
      lastError = `HTTP ${response.status}`;
    }
    if (attempt < input.maxAttempts) {
      const retryAfterSeconds = Number(response?.headers.get("retry-after") ?? "");
      const backoffMs =
        Number.isFinite(retryAfterSeconds) && retryAfterSeconds > 0
          ? Math.min(120_000, retryAfterSeconds * 1000)
          : Math.min(30_000, 1_000 * 2 ** (attempt - 1));
      input.log(JSON.stringify({ status: "workspace_backfill_retry", attempt, backoffMs, error: lastError }));
      await input.sleep(backoffMs);
    }
  }
  throw new FatalUploadError(
    `Workspace ingest unreachable after ${input.maxAttempts} attempts (${lastError}). The resume watermark is saved; re-run to continue.`,
  );
}

/**
 * The full history push. Reads the ledger read-only in rowid order (stable,
 * indexed, append-correlated; the repo never VACUUMs — and the watermark id is
 * re-verified on resume anyway), normalizes rows to the wire envelope, ships
 * ≤500-event signed batches with bounded concurrency, advances a local resume
 * watermark only when every batch at-or-before it succeeded, and folds each
 * batch's audit slice into the cumulative reconciliation audit at that same
 * frontier — so a resumed run never double-counts.
 *
 * Interaction with the live 5-minute sync: both push the same event ids; the
 * cloud dedupes by id, so overlap is harmless. The daemon keeps draining its
 * own queue and live events keep flowing while (and after) this runs.
 */
export async function runWorkspaceHistoryUpload(
  config: CollectorConfig,
  options: WorkspaceHistoryUploadOptions = {},
): Promise<WorkspaceHistoryUploadResult> {
  const log = options.log ?? ((line: string) => console.log(line));
  const sleep = options.sleep ?? defaultSleep;
  const fetchImpl = options.fetchImpl ?? fetch;
  const now = options.now ?? (() => new Date());

  const url = options.url ?? config.uploadUrl;
  if (!url) {
    throw new Error(
      "This machine has not joined a workspace (no uploadUrl in collector.config.json). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry upload-history.',
    );
  }
  if ((!config.installKey || config.installKey === "local-dev") && !config.ingestKey) {
    throw new Error(
      "No workspace install credentials found (installKey is missing/local-dev and there is no ingestKey). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry upload-history.',
    );
  }

  const ledgerPath = options.ledgerPath ?? collectorBufferPath();
  let ledger: Database.Database;
  try {
    ledger = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  } catch (error) {
    throw new Error(
      `No readable local ledger at ${ledgerPath} (${error instanceof Error ? error.message : String(error)}) — nothing to backfill.`,
    );
  }

  const startedAt = now();
  const batchSize = Math.max(1, Math.min(options.batchSize ?? HISTORY_MAX_BATCH_EVENTS, HISTORY_MAX_BATCH_EVENTS));
  const maxBatchBytes = options.maxBatchBytes ?? HISTORY_MAX_BATCH_BYTES;
  const concurrency = Math.max(1, Math.min(options.concurrency ?? 1, 8));
  const delayMs = Math.max(0, options.delayMs ?? 250);
  const pageSize = Math.max(batchSize, Math.min(options.pageSize ?? 4_000, 20_000));
  const maxAttempts = Math.max(1, Math.min(options.maxAttemptsPerBatch ?? 5, 10));
  const appVersion = options.appVersion ?? "0.1.0";
  const target = backfillTargetFingerprint(url, config.tenantId);
  const statePath = options.statePath ?? defaultBackfillStatePath();

  let state: WorkspaceBackfillState | null = options.full ? null : readBackfillState(statePath);
  if (state && state.target !== target) state = null;
  let resumedFromRowid: number | null = null;
  if (state?.watermark) {
    const atRowid = ledger
      .prepare(`select id from buffered_events where rowid = ?`)
      .get(state.watermark.rowid) as { id: string } | undefined;
    if (atRowid && atRowid.id !== state.watermark.id) {
      ledger.close();
      throw new Error(
        `Resume watermark mismatch (rowid ${state.watermark.rowid} no longer holds event ${state.watermark.id}). ` +
          "The ledger file changed shape; re-run with --full to walk everything (idempotent ids make that safe).",
      );
    }
    resumedFromRowid = state.watermark.rowid;
  }

  if (options.until && Number.isNaN(Date.parse(options.until))) {
    ledger.close();
    throw new Error(`--until must be an ISO timestamp, got: ${options.until}`);
  }
  // A resumed run keeps its original scope unless --until is given explicitly.
  const scopeUntil = options.until ?? state?.untilCreatedAt ?? startedAt.toISOString();

  // Cumulative audit: what past segments of this backfill committed (resume),
  // plus what this run commits batch-by-batch at the watermark frontier.
  const audit: HistoryAudit = state?.audit ?? createHistoryAudit();
  let watermark = state?.watermark ?? null;
  const watermarkStart = watermark?.rowid ?? 0;

  const pending = ledger
    .prepare(`select count(*) as n from buffered_events where rowid > ? and created_at <= ?`)
    .get(watermarkStart, scopeUntil) as { n: number };

  log(
    JSON.stringify({
      status: "workspace_backfill_start",
      until: scopeUntil,
      pendingRows: pending.n,
      resumedFromRowid,
      batchSize,
      concurrency,
      delayMs,
      dryRun: Boolean(options.dryRun),
      ledgerPath,
    }),
  );

  const page = ledger.prepare(
    `select rowid as rowid, id, created_at as createdAt, payload_json as payloadJson,
       suppressed_fields_json as suppressedFieldsJson,
       repo_hash as repoHash, branch_hash as branchHash
     from buffered_events
     where rowid > ? and created_at <= ?
     order by rowid asc
     limit ?`,
  );

  type CarryItem = {
    envelope: HistoryEnvelope;
    bytes: number;
    rowid: number;
    id: string;
    createdAt: string;
    idDerived: boolean;
  };

  type PreparedBatch = {
    index: number;
    body: string;
    events: Array<Pick<AiInteractionEvent, "source" | "observedAt">>;
    /** This batch's slice of the audit (eligible rows + skips scanned just before it). Folded in only when the frontier passes it. */
    auditDelta: HistoryAudit;
    /** Server-reported newly-inserted count; set on completion, folded at the frontier. */
    inserted: number | null;
    maxRowid: number;
    maxRowidId: string;
    maxRowidCreatedAt: string;
  };

  let scannedRows = 0;
  let eligibleEvents = 0;
  let dispatchedEvents = 0;
  let sentEvents = 0;
  let acceptedEvents = 0;
  let batchesSent = 0;
  let nextBatchIndex = 0;
  let abortReason: string | null = null;
  let limitReached = false;

  // Skipped rows commit to the audit at the watermark frontier exactly like
  // uploaded rows: each skip is queued with its rowid and folds into the
  // first batch whose maxRowid covers it (so a resume that re-scans past the
  // watermark can never double-count a skip). Trailing skips beyond the last
  // batch fold in only when the walk completes.
  const skipQueue: Array<{ rowid: number; reason: HistorySkipReason }> = [];
  const drainSkipsUpTo = (maxRowid: number, into: HistoryAudit) => {
    while (skipQueue.length > 0 && skipQueue[0].rowid <= maxRowid) {
      recordHistorySkip(into, skipQueue.shift()!.reason);
    }
  };

  const completedBatches = new Map<number, PreparedBatch>();
  let frontierBatchIndex = 0;

  const persistState = (completed: boolean) => {
    if (options.dryRun) return;
    const record: WorkspaceBackfillState = {
      version: 1,
      target,
      untilCreatedAt: scopeUntil,
      watermark,
      startedAt: state?.startedAt ?? startedAt.toISOString(),
      updatedAt: new Date().toISOString(),
      completedAt: completed ? new Date().toISOString() : null,
      audit,
    };
    writeBackfillState(statePath, record);
  };

  const advanceFrontier = () => {
    while (completedBatches.has(frontierBatchIndex)) {
      const batch = completedBatches.get(frontierBatchIndex)!;
      completedBatches.delete(frontierBatchIndex);
      mergeHistoryAudit(audit, batch.auditDelta);
      recordHistoryOutcome(audit, batch.events, true);
      if (batch.inserted !== null) {
        audit.insertedEvents = (audit.insertedEvents ?? 0) + batch.inserted;
        audit.insertedBatchesReported = (audit.insertedBatchesReported ?? 0) + 1;
      }
      watermark = { rowid: batch.maxRowid, id: batch.maxRowidId, createdAt: batch.maxRowidCreatedAt };
      frontierBatchIndex += 1;
    }
  };

  const totalBatchesEstimate = Math.max(1, Math.ceil(pending.n / batchSize));
  const logProgress = () => {
    const elapsedSeconds = Math.max(0.001, (Date.now() - startedAt.getTime()) / 1000);
    const rate = sentEvents / elapsedSeconds;
    const remaining = Math.max(0, pending.n - scannedRows) + Math.max(0, eligibleEvents - sentEvents);
    log(
      JSON.stringify({
        status: "workspace_backfill_progress",
        batches: `${batchesSent}/${totalBatchesEstimate}`,
        sentEvents,
        acceptedEvents,
        insertedEvents: audit.insertedEvents ?? null,
        eventsPerSecond: Math.round(rate * 10) / 10,
        etaMinutes: rate > 0 ? Math.round(remaining / rate / 60) : null,
      }),
    );
  };

  const inFlight = new Set<Promise<void>>();
  const dispatch = async (batch: PreparedBatch) => {
    const task = (async () => {
      try {
        const result = await postHistoryBatch({
          url,
          body: batch.body,
          installKey: config.installKey,
          ingestKey: config.ingestKey,
          signingSecret: config.uploadSigningSecret,
          fetchImpl,
          sleep,
          maxAttempts,
          log,
        });
        batchesSent += 1;
        sentEvents += batch.events.length;
        acceptedEvents += result.accepted;
        batch.inserted = result.inserted;
        completedBatches.set(batch.index, batch);
        advanceFrontier();
        persistState(false);
        logProgress();
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

  let cursorRowid = watermarkStart;
  let lastScanned: WorkspaceBackfillState["watermark"] = null;
  let carry: CarryItem[] = [];

  pageLoop: while (abortReason === null) {
    const rows = page.all(cursorRowid, scopeUntil, pageSize) as LedgerHistoryRow[];
    if (rows.length === 0 && carry.length === 0) break;
    if (rows.length > 0) {
      const tail = rows[rows.length - 1];
      cursorRowid = tail.rowid;
      lastScanned = { rowid: tail.rowid, id: tail.id, createdAt: tail.createdAt };
    }
    const lastPage = rows.length < pageSize;

    for (const row of rows) {
      scannedRows += 1;
      const normalized = normalizeHistoryEvent(row);
      if (!normalized.ok) {
        skipQueue.push({ rowid: row.rowid, reason: normalized.reason });
        continue;
      }
      eligibleEvents += 1;
      carry.push({
        envelope: normalized.envelope,
        bytes: normalized.bytes,
        rowid: row.rowid,
        id: row.id,
        createdAt: row.createdAt,
        idDerived: normalized.idDerived,
      });
    }

    const chunks = chunkHistoryEnvelopes(carry, { maxEvents: batchSize, maxBytes: maxBatchBytes });
    carry = [];
    for (let index = 0; index < chunks.length; index += 1) {
      const chunk = chunks[index];
      const isFinalChunk = index === chunks.length - 1;
      if (!lastPage && isFinalChunk && chunk.length < batchSize) {
        // Partial tail chunk mid-history: roll into the next page so batches
        // stay full-size.
        carry = chunk;
        break;
      }

      const auditDelta = createHistoryAudit();
      drainSkipsUpTo(chunk[chunk.length - 1].rowid, auditDelta);
      for (const item of chunk) {
        recordHistoryEligible(auditDelta, item.envelope.event);
        if (item.idDerived) auditDelta.derivedIds += 1;
      }

      if (options.dryRun) {
        // No network, no state: commit the slice straight into the audit.
        mergeHistoryAudit(audit, auditDelta);
        continue;
      }

      const batchPayload = aiWorkIngestBatchSchema.parse({
        tenantId: config.tenantId,
        installKey: config.installKey,
        appVersion,
        events: chunk.map((item) => item.envelope),
      });
      const prepared: PreparedBatch = {
        index: nextBatchIndex,
        body: JSON.stringify(batchPayload),
        events: chunk.map((item) => ({
          source: item.envelope.event.source,
          observedAt: item.envelope.event.observedAt,
        })),
        auditDelta,
        inserted: null,
        maxRowid: chunk[chunk.length - 1].rowid,
        maxRowidId: chunk[chunk.length - 1].id,
        maxRowidCreatedAt: chunk[chunk.length - 1].createdAt,
      };
      nextBatchIndex += 1;
      dispatchedEvents += chunk.length;
      await dispatch(prepared);
      if (abortReason !== null) break pageLoop;
      if (options.limit && dispatchedEvents >= options.limit) {
        limitReached = true;
        break pageLoop;
      }
    }

    if (lastPage) break;
  }

  await Promise.allSettled([...inFlight]);
  ledger.close();

  const drainedEverything = abortReason === null && !limitReached;
  if (drainedEverything && skipQueue.length > 0) {
    // Trailing skipped rows beyond the last batch still belong in the audit.
    drainSkipsUpTo(Number.MAX_SAFE_INTEGER, audit);
  }
  if (drainedEverything && !options.dryRun && lastScanned && lastScanned.rowid > (watermark?.rowid ?? 0)) {
    // A completed walk handled every scanned row (uploaded or audited), so
    // the watermark advances to the last scanned row — trailing skips are
    // covered and a later top-up run never re-counts them.
    watermark = lastScanned;
  }

  const completed = drainedEverything && !options.dryRun;
  if (!options.dryRun) persistState(completed);

  const durationMs = Date.now() - startedAt.getTime();
  const auditTable = renderHistoryAudit(audit);
  const result: WorkspaceHistoryUploadResult = {
    ok: abortReason === null,
    reason: abortReason,
    until: scopeUntil,
    scannedRows,
    eligibleEvents,
    skippedEvents: historySkippedTotal(audit),
    sentEvents,
    acceptedEvents,
    insertedEvents: (audit.insertedBatchesReported ?? 0) > 0 ? audit.insertedEvents ?? 0 : null,
    batchesSent,
    derivedIds: audit.derivedIds,
    durationMs,
    eventsPerSecond: durationMs > 0 ? Math.round((sentEvents / (durationMs / 1000)) * 10) / 10 : 0,
    completed,
    resumedFromRowid,
    watermark,
    audit,
    auditTable,
    statePath: options.dryRun ? null : statePath,
    dryRun: Boolean(options.dryRun),
  };

  log(
    JSON.stringify({
      status: abortReason ? "workspace_backfill_failed" : "workspace_backfill_done",
      reason: abortReason,
      until: result.until,
      scannedRows,
      eligibleEvents,
      skippedEvents: result.skippedEvents,
      sentEvents,
      acceptedEvents,
      insertedEvents: result.insertedEvents,
      batchesSent,
      completed,
      durationMs,
      eventsPerSecond: result.eventsPerSecond,
    }),
  );
  log("");
  log(auditTable);

  return result;
}

/**
 * Attribution repair (issue 0036) — pure mapper from ledger linkage rows to
 * the wire shape. Ids go through the SAME deterministic UUID mapping as
 * uploads, so the pair targets exactly the row the upload created.
 */
export function buildAttributionRepairRows(
  rows: Array<{ id: string; repoHash: string | null }>,
): AiWorkAttributionRepairRow[] {
  const out: AiWorkAttributionRepairRow[] = [];
  for (const row of rows) {
    const repoHash = row.repoHash?.trim();
    if (!repoHash || !row.id?.trim()) continue;
    out.push({ id: ensureUuidEventId(row.id.trim()).id, projectKey: repoHash });
  }
  return out;
}

export type AttributionRepairResult = {
  ok: boolean;
  reason: string | null;
  until: string;
  rowsWithRepoHash: number;
  distinctRepos: number;
  sentRows: number;
  matchedRows: number;
  updatedRows: number;
  batches: number;
  durationMs: number;
  dryRun: boolean;
};

/**
 * Fill projectKey on ALREADY-UPLOADED workspace rows. The bulk ingest lane is
 * first-writer-wins (createMany skipDuplicates), so re-sending events can
 * never back-fill attribution — this lane sends bare {id, projectKey} pairs
 * and the cloud applies ONE set-based, tenant-scoped, FILL-ONLY update per
 * batch. Fill-only twice over: rows with a differing non-null projectKey are
 * left alone, and re-running reports updated: 0 — that is the proof it
 * settled. Walks every ledger row with repo_hash (uploaded or not — unknown
 * ids simply match nothing), read-only, no resume state needed: the whole
 * walk is cheap and idempotent.
 */
export async function runAttributionRepair(
  config: CollectorConfig,
  options: {
    until?: string;
    batchSize?: number;
    concurrency?: number;
    delayMs?: number;
    dryRun?: boolean;
    url?: string;
    appVersion?: string;
    ledgerPath?: string;
    fetchImpl?: typeof fetch;
    sleep?: (ms: number) => Promise<void>;
    log?: (line: string) => void;
    maxAttemptsPerBatch?: number;
    pageSize?: number;
  } = {},
): Promise<AttributionRepairResult> {
  const log = options.log ?? ((line: string) => console.log(line));
  const sleep = options.sleep ?? defaultSleep;
  const fetchImpl = options.fetchImpl ?? fetch;

  const url = options.url ?? config.uploadUrl;
  if (!url) {
    throw new Error(
      "This machine has not joined a workspace (no uploadUrl in collector.config.json). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry upload-history --repair-attribution.',
    );
  }
  if ((!config.installKey || config.installKey === "local-dev") && !config.ingestKey) {
    throw new Error(
      "No workspace install credentials found (installKey is missing/local-dev and there is no ingestKey). " +
        'Run: plimsoll join "<join-url>#<token>" — then retry upload-history --repair-attribution.',
    );
  }

  const ledgerPath = options.ledgerPath ?? collectorBufferPath();
  let ledger: Database.Database;
  try {
    ledger = new Database(ledgerPath, { readonly: true, fileMustExist: true });
  } catch (error) {
    throw new Error(
      `No readable local ledger at ${ledgerPath} (${error instanceof Error ? error.message : String(error)}) — nothing to repair.`,
    );
  }

  const startedAt = Date.now();
  const until = options.until ?? new Date().toISOString();
  if (Number.isNaN(Date.parse(until))) {
    ledger.close();
    throw new Error(`--until must be an ISO timestamp, got: ${until}`);
  }
  const batchSize = Math.max(1, Math.min(options.batchSize ?? HISTORY_MAX_BATCH_EVENTS, HISTORY_MAX_BATCH_EVENTS));
  const concurrency = Math.max(1, Math.min(options.concurrency ?? 2, 8));
  const delayMs = Math.max(0, options.delayMs ?? 100);
  const pageSize = Math.max(batchSize, Math.min(options.pageSize ?? 10_000, 50_000));
  const maxAttempts = Math.max(1, Math.min(options.maxAttemptsPerBatch ?? 5, 10));
  const appVersion = options.appVersion ?? "0.1.0";

  const overview = ledger
    .prepare(
      `select count(*) as n, count(distinct repo_hash) as repos
       from buffered_events where repo_hash is not null and created_at <= ?`,
    )
    .get(until) as { n: number; repos: number };

  log(
    JSON.stringify({
      status: "attribution_repair_start",
      until,
      rowsWithRepoHash: overview.n,
      distinctRepos: overview.repos,
      batchSize,
      concurrency,
      dryRun: Boolean(options.dryRun),
      ledgerPath,
    }),
  );

  const page = ledger.prepare(
    `select rowid as rowid, id, repo_hash as repoHash
     from buffered_events
     where repo_hash is not null and created_at <= ? and rowid > ?
     order by rowid asc
     limit ?`,
  );

  let cursorRowid = 0;
  let sentRows = 0;
  let matchedRows = 0;
  let updatedRows = 0;
  let batches = 0;
  let abortReason: string | null = null;
  const totalBatchesEstimate = Math.max(1, Math.ceil(overview.n / batchSize));

  const inFlight = new Set<Promise<void>>();
  const dispatch = async (rows: AiWorkAttributionRepairRow[]) => {
    const body = JSON.stringify(
      aiWorkAttributionRepairBatchSchema.parse({
        kind: "attribution_repair",
        tenantId: config.tenantId,
        installKey: config.installKey,
        appVersion,
        rows,
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
        sentRows += rows.length;
        matchedRows += result.matched ?? 0;
        updatedRows += result.updated ?? 0;
        log(
          JSON.stringify({
            status: "attribution_repair_progress",
            batches: `${batches}/${totalBatchesEstimate}`,
            sentRows,
            matchedRows,
            updatedRows,
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

  let carry: AiWorkAttributionRepairRow[] = [];
  pageLoop: while (abortReason === null) {
    const rows = page.all(until, cursorRowid, pageSize) as Array<{
      rowid: number;
      id: string;
      repoHash: string;
    }>;
    if (rows.length === 0 && carry.length === 0) break;
    if (rows.length > 0) cursorRowid = rows[rows.length - 1].rowid;
    const lastPage = rows.length < pageSize;

    carry = carry.concat(buildAttributionRepairRows(rows));
    while (carry.length >= batchSize || (lastPage && carry.length > 0)) {
      const chunk = carry.slice(0, batchSize);
      carry = carry.slice(batchSize);
      if (options.dryRun) {
        batches += 1;
        sentRows += chunk.length;
        continue;
      }
      await dispatch(chunk);
      if (abortReason !== null) break pageLoop;
    }
    if (lastPage) break;
  }

  await Promise.allSettled([...inFlight]);
  ledger.close();

  const durationMs = Date.now() - startedAt;
  const result: AttributionRepairResult = {
    ok: abortReason === null,
    reason: abortReason,
    until,
    rowsWithRepoHash: overview.n,
    distinctRepos: overview.repos,
    sentRows,
    matchedRows,
    updatedRows,
    batches,
    durationMs,
    dryRun: Boolean(options.dryRun),
  };

  log(
    JSON.stringify({
      status: abortReason ? "attribution_repair_failed" : "attribution_repair_done",
      ...result,
    }),
  );
  log("");
  log(
    [
      `ledger rows with repo linkage (<= until): ${overview.n} across ${overview.repos} repos`,
      `pairs sent: ${sentRows}${options.dryRun ? " (dry-run: nothing crossed the wire)" : ""}`,
      `matched in workspace: ${options.dryRun ? "n/a" : matchedRows}`,
      `newly filled: ${options.dryRun ? "n/a" : updatedRows}${
        !options.dryRun && updatedRows === 0 && sentRows > 0
          ? " (already attributed — fill-only repair settled)"
          : ""
      }`,
    ].join("\n"),
  );

  return result;
}
