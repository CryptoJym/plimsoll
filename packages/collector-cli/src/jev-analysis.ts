import Database from "better-sqlite3";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

/** Read the existing Jev receipt store. This module never invokes a model,
 * reads task text, or writes to either the receipt store or Plimsoll's ledger. */
const MODES = ["rescue", "reuse", "readiness", "change", "waste", "calibration"] as const;
const RUNTIMES = ["codex", "claude-code", "cursor", "opencode", "grok"];
const ACTIONS = ["used", "dismissed", "accepted", "regressed"];
const SHA = /^[a-f0-9]{64}$/;
const LIMIT = 50;
const MAX_JSON = 64_000;
const MAX_DB_BYTES = 64 * 1024 * 1024;
type Json = Record<string, unknown>;

function object(value: unknown): Json {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid_record");
  return value as Json;
}
function parse(raw: unknown): Json {
  if (typeof raw !== "string" || Buffer.byteLength(raw) > MAX_JSON) throw new Error("invalid_record");
  return object(JSON.parse(raw));
}
function hash(value: unknown): string | null {
  return typeof value === "string" && SHA.test(value) ? value : null;
}
function label(value: unknown): string | null {
  return typeof value === "string" && /^[a-zA-Z0-9_.:-]{1,100}$/.test(value) ? value : null;
}
function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) && value >= 0 ? value : null;
}
function count(value: unknown): number | null {
  return number(value) !== null && Number.isSafeInteger(value) ? value as number : null;
}
function timestamp(value: unknown, now: number): string {
  const seconds = number(value);
  if (seconds === null || seconds * 1000 > now + 30_000) throw new Error("invalid_timestamp");
  return new Date(seconds * 1000).toISOString();
}

export type JevAnalysisDecision = {
  id: string;
  observedAt: string;
  mode: string;
  recommendation: string;
  held: boolean;
  native: { runtime: string; sessionId: string; workspaceHash: string; eventId: string; eventState: string };
  inputHash: string;
  inference: {
    state: string;
    requestHash: string | null;
    model: string | null;
    requestedModel: string | null;
    cacheHit: boolean | null;
    inputTokens: number | null;
    outputTokens: number | null;
    latencyMs: number | null;
    estimatedCostUsd: number | null;
    billedCostUsd: number | null;
  };
  offeredAt: string | null;
  reportedActions: Array<{ action: string; reportedAt: string }>;
  outcomeRecordsRejected: number;
  outcomeRecordsTruncated: boolean;
  workAttribution: "unavailable";
  verifiedAcceptance: null;
  measuredSavingsUsd: null;
};

export type JevAnalysisSnapshot = {
  schema: "plimsoll.jev-analysis.v1";
  generatedAt: string;
  state: "available" | "partial" | "unavailable" | "not_installed";
  reason: string | null;
  scope: "local_machine";
  windowDays: number;
  coverage: { inspected: number; returned: number; rejected: number; rejectedOutcomes: number; truncated: boolean; limit: number };
  overhead: {
    deterministicDecisions: number;
    uniqueRequests: number;
    estimatedCostUsd: number | null;
    requestsWithEstimate: number;
    billedCostUsd: number | null;
    requestsWithBilling: number;
  };
  decisions: JevAnalysisDecision[];
};

export function readJevAnalysis(options: { databasePath?: string; days?: number; nowMs?: number } = {}): JevAnalysisSnapshot {
  const now = options.nowMs ?? Date.now();
  const days = options.days ?? 30;
  const result: JevAnalysisSnapshot = {
    schema: "plimsoll.jev-analysis.v1", generatedAt: new Date(now).toISOString(),
    state: "unavailable", reason: null, scope: "local_machine", windowDays: days,
    coverage: { inspected: 0, returned: 0, rejected: 0, rejectedOutcomes: 0, truncated: false, limit: LIMIT },
    overhead: { deterministicDecisions: 0, uniqueRequests: 0, estimatedCostUsd: null, requestsWithEstimate: 0, billedCostUsd: null, requestsWithBilling: 0 },
    decisions: [],
  };
  if (!Number.isInteger(days) || days < 1 || days > 90) {
    result.reason = "unsupported_window";
    return result;
  }
  if (process.env.PLIMSOLL_JEV_DISABLED === "1") {
    result.reason = "disabled_by_operator";
    return result;
  }
  const databasePath = options.databasePath ?? process.env.PLIMSOLL_JEV_DB ??
    path.join(os.homedir(), ".local", "state", "jev-decisions", "inference.sqlite3");
  let db: Database.Database | undefined;
  try {
    const stat = fs.lstatSync(databasePath);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size > MAX_DB_BYTES) {
      result.reason = "source_outside_read_bounds";
      return result;
    }
    let sourceBytes = stat.size;
    for (const suffix of ["-wal", "-shm", "-journal"]) {
      try {
        const sidecar = fs.lstatSync(databasePath + suffix);
        sourceBytes += sidecar.size;
        if (!sidecar.isFile() || sidecar.isSymbolicLink() || sourceBytes > MAX_DB_BYTES) {
          result.reason = "source_outside_read_bounds";
          return result;
        }
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
      }
    }
    db = new Database(databasePath, { readonly: true, fileMustExist: true, timeout: 100 });
    db.pragma("query_only = ON");
    db.pragma("trusted_schema = OFF");
    db.exec("BEGIN");
    const rows = db.prepare(`SELECT id, session_key, mode, created,
      CASE WHEN length(CAST(result AS BLOB)) <= ? THEN result ELSE NULL END AS result
      FROM auto_decisions WHERE created >= ? ORDER BY created DESC, id LIMIT ?`)
      .all(MAX_JSON, now / 1000 - days * 86400, LIMIT + 1) as Array<Json>;
    result.coverage.truncated = rows.length > LIMIT;
    const eventQuery = db.prepare(`SELECT session_key, created, state,
      CASE WHEN length(CAST(receipt AS BLOB)) <= ? THEN receipt ELSE NULL END AS receipt
      FROM auto_events WHERE id = ?`);
    const offerQuery = db.prepare("SELECT event_id, offered_at FROM auto_offers WHERE decision_id = ?");
    // The source contract does not promise an outcome index. Read this table
    // once, with at most eleven rows per displayed decision, not fifty scans.
    const decisionIds = rows.slice(0, LIMIT).map(row => row.id);
    const outcomesByDecision = new Map<string, Json[]>();
    if (decisionIds.length) {
      const outcomes = db.prepare(`SELECT decision_id, created, record FROM (
        SELECT decision_id, created,
          CASE WHEN length(CAST(record AS BLOB)) <= ? THEN record ELSE NULL END AS record,
          row_number() OVER (PARTITION BY decision_id ORDER BY created DESC, id) AS position
        FROM auto_outcomes WHERE decision_id IN (${decisionIds.map(() => "?").join(",")})
      ) WHERE position <= 11 ORDER BY decision_id, position`).all(MAX_JSON, ...decisionIds) as Json[];
      for (const outcome of outcomes) {
        const id = outcome.decision_id as string;
        outcomesByDecision.set(id, [...(outcomesByDecision.get(id) ?? []), outcome]);
      }
    }
    for (const row of rows.slice(0, LIMIT)) {
      result.coverage.inspected++;
      try {
        const saved = parse(row.result);
        const decision = object(saved.decision);
        const inference = object(saved.inference);
        const id = hash(row.id), inputHash = hash(saved.input_sha256), eventId = hash(saved.event_id);
        const recommendation = label(decision.recommendation), state = label(inference.state);
        if (!id || !inputHash || !eventId || !recommendation || !state ||
          !MODES.includes(row.mode as typeof MODES[number]) || saved.mode !== row.mode ||
          saved.schema !== "jev-decision/v1" || saved.execution_authorized !== false ||
          !Array.isArray(saved.holds)) throw new Error("invalid_decision");
        const event = eventQuery.get(MAX_JSON, eventId) as Json | undefined;
        if (!event || event.session_key !== row.session_key || !hash(row.session_key)) throw new Error("event_identity_mismatch");
        const receipt = parse(event.receipt);
        const runtime = label(receipt.runtime), session = receipt.session, workspace = hash(receipt.workspace_sha256);
        if (!runtime || !RUNTIMES.includes(runtime) || typeof session !== "string" ||
          !/^[A-Za-z0-9_.:-]{6,180}$/.test(session) || !workspace ||
          !["started", "complete"].includes(event.state as string)) throw new Error("invalid_native_identity");
        const observedAt = timestamp(row.created, now);
        timestamp(event.created, now);
        if ((event.created as number) > (row.created as number)) throw new Error("invalid_event_order");
        const usage = inference.usage && typeof inference.usage === "object" ? object(inference.usage) : {};
        const offer = offerQuery.get(id) as Json | undefined;
        let offeredAt: string | null = null;
        if (offer) {
          const offeredEvent = hash(offer.event_id) ? eventQuery.get(MAX_JSON, offer.event_id) as Json | undefined : undefined;
          if (!offeredEvent || offeredEvent.session_key !== row.session_key) throw new Error("offer_identity_mismatch");
          offeredAt = timestamp(offer.offered_at, now);
          if ((offer.offered_at as number) < (row.created as number)) throw new Error("invalid_offer_order");
        }
        const outcomes = outcomesByDecision.get(id) ?? [];
        const actions: JevAnalysisDecision["reportedActions"] = [];
        let outcomeRecordsRejected = 0;
        for (const outcome of outcomes.slice(0, 10)) {
          try {
          const record = parse(outcome.record);
          if (record.decision_id !== id || !ACTIONS.includes(record.action as string) ||
            typeof record.evidence_ref !== "string" || !record.evidence_ref.trim()) throw new Error("invalid_outcome");
          if ((outcome.created as number) < (row.created as number)) throw new Error("invalid_outcome_order");
          // Evidence refs can be arbitrary paths or private URLs. Do not expose
          // them or mistake an owner's report for independently verified work.
          actions.push({ action: record.action as string, reportedAt: timestamp(outcome.created, now) });
          } catch { outcomeRecordsRejected++; }
        }
        result.coverage.rejectedOutcomes += outcomeRecordsRejected;
        result.decisions.push({
          id, observedAt, mode: row.mode as string, recommendation, held: saved.holds.length > 0,
          native: { runtime, sessionId: session, workspaceHash: workspace, eventId, eventState: event.state as string }, inputHash,
          inference: { state, requestHash: hash(inference.request_sha256), model: label(inference.model),
            requestedModel: label(inference.requested_model),
            cacheHit: typeof inference.cache_hit === "boolean" ? inference.cache_hit : null,
            inputTokens: count(usage.input_tokens), outputTokens: count(usage.output_tokens),
            latencyMs: number(inference.latency_ms), estimatedCostUsd: number(inference.estimated_cost_usd),
            billedCostUsd: number(inference.billed_cost_usd) },
          offeredAt, reportedActions: actions, outcomeRecordsRejected, outcomeRecordsTruncated: outcomes.length > 10,
          workAttribution: "unavailable", verifiedAcceptance: null, measuredSavingsUsd: null,
        });
      } catch {
        result.coverage.rejected++;
      }
    }
    db.exec("ROLLBACK");
    result.coverage.returned = result.decisions.length;
    // One request can be referenced by several decisions. Never sum it once
    // per event, offer, outcome or cache hit. Disagreement stays unknown.
    const requests = new Map<string, Array<JevAnalysisDecision["inference"]>>();
    for (const decision of result.decisions) {
      const key = decision.inference.requestHash;
      if (key) requests.set(key, [...(requests.get(key) ?? []), decision.inference]);
    }
    result.overhead.uniqueRequests = requests.size;
    result.overhead.deterministicDecisions = result.decisions.filter(decision => decision.inference.state === "deterministic").length;
    for (const [field, covered] of [["estimatedCostUsd", "requestsWithEstimate"], ["billedCostUsd", "requestsWithBilling"]] as const) {
      let sum = 0;
      for (const entries of requests.values()) {
        const values = new Set(entries.map(item => item[field]).filter(value => value !== null));
        if (values.size === 1) { sum += [...values][0]!; result.overhead[covered]++; }
      }
      result.overhead[field] = requests.size > 0 && result.overhead[covered] === requests.size && Number.isFinite(sum) ? sum : null;
    }
    result.state = result.coverage.rejected > 0 || result.coverage.rejectedOutcomes > 0 || result.coverage.truncated ? "partial" : "available";
    result.reason = result.coverage.rejected > 0 || result.coverage.rejectedOutcomes > 0 ? "invalid_records_excluded" : result.coverage.truncated ? "recent_decision_limit" : null;
  } catch (error) {
    result.decisions = [];
    result.coverage = { inspected: 0, returned: 0, rejected: 0, rejectedOutcomes: 0, truncated: false, limit: LIMIT };
    const code = (error as { code?: string }).code;
    result.state = code === "ENOENT" ? "not_installed" : "unavailable";
    result.reason = code === "ENOENT" ? "receipt_store_missing" : code === "SQLITE_BUSY" || code === "SQLITE_LOCKED" ? "receipt_store_busy" : "receipt_store_unreadable_or_unsupported";
  } finally {
    db?.close();
  }
  return result;
}
