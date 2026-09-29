import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { collectorConfigSchema, type CollectorConfig } from "./config";
import { normalizeForwardedHook } from "./forwarder";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import { classifyEventType, isUuid } from "./normalizer";
import { withRebuildCoordination } from "./rebuild-coordination";
import { acquireRebuildOpenToken, releaseRebuildOpenToken } from "./rebuild-open-gate";
import { HOOK_ACK_LOOKUP_SQL, HOOK_ROW_LOOKUP_SQL, HOOK_ROW_LEGACY_LOOKUP_SQL,
  ledgerAdmissionSequence, originalHookTimestampDigest, removeMaintenanceHookAdmission,
  sameHookIdentityPart } from "./maintenance-hook-admission";
import { hookBodyDigest, hookBodyFromWire, hookReceiptFileName } from "./maintenance-hook-fingerprint";

const MARKER = "maintenance-rebuild-pause.json";
const REFUSALS = "maintenance-rebuild-refusals";
const TERMINAL = "maintenance-rebuild-terminal.jsonl";
type PauseMarker = { version: 1; at: string; pid?: number; endedAt?: string;
  ledgerName?: "work-ledger.sqlite" | "ledger.sqlite"; ledgerHighWater?: number | null;
  ledgerAdmissionSequence?: number | null; ledgerExistedAtPause?: boolean };

type RefusalRoute = "hook" | "otlp" | "live";
type RefusalReceipt = { version: number; route: RefusalRoute; at: string;
  source?: string; eventId?: string; kind?: string; ledgerHighWater?: number | null;
  eventDigest?: string | null; receiveClockFallback?: boolean;
  receiptId?: string; sessionId?: string | null; originalTimestampDigest?: string | null;
  bodyDigest?: string | null;
  tenantId?: string; ledgerAdmissionSequence?: number | null;
  ledgerAbsentAtRefusal?: boolean;
  spoolName?: string; unknownAt?: string };
/** The client writes its retry immediately after the response; the spool's
 * ten-minute stale-pending diagnostic is our conservative missing-retry
 * threshold. A durable receipt remains so later exact acceptance can heal it. */
export const MISSING_HOOK_RETRY_MS = 600_000;
const SPOOL_NAME = /^\d{13,}-\d+-[0-9a-f]{6}\.json$/;
function refusalDirectory(home: string) { return path.join(home, REFUSALS); }
function refusalPath(home: string, route: RefusalRoute, source: string, body: string | Buffer) {
  if (route === "hook") {
    try {
      return path.join(refusalDirectory(home), hookReceiptFileName(source, hookBodyDigest(hookBodyFromWire(body))));
    } catch { /* Invalid JSON still has a terminal receipt keyed by its wire bytes. */ }
  }
  const digest = createHash("sha256").update(`${route}\0${source}\0`).update(body).digest("hex");
  return path.join(refusalDirectory(home), `${digest}.receipt`);
}
function existingRefusalPath(home: string, route: RefusalRoute, source: string, body: string | Buffer) {
  const current = refusalPath(home, route, source, body);
  if (fs.existsSync(current) || route !== "hook") return current;
  const legacyName = createHash("sha256").update(`${route}\0${source}\0`).update(body).digest("hex");
  const legacy = path.join(refusalDirectory(home), `${legacyName}.receipt`);
  return fs.existsSync(legacy) ? legacy : current;
}
function fsyncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}
function sameEventId(left: string, right: string) {
  return isUuid(left) && isUuid(right)
    ? left.toLowerCase() === right.toLowerCase() : left === right;
}
function stableJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stableJson);
  if (!value || typeof value !== "object") return value;
  return Object.fromEntries(Object.entries(value as Record<string, unknown>)
    .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
    .map(([key, entry]) => [key, stableJson(entry)]));
}
/** The payload_json column is JSON.stringify(canonical.event). Canonicalize
 * object order and UUID spelling before hashing, so older drain versions and
 * mixed-case producer UUIDs compare the same normalized event. A hook with no
 * usable time alias gets its observedAt from the receiver clock; that clock
 * is the one field a retry cannot repeat. Its sentinel is part of the digest
 * basis, while every caller-controlled normalized field remains exact. */
function normalizedEventDigest(event: Record<string, unknown>, receiveClockFallback: boolean) {
  const comparable = { ...event,
    id: typeof event.id === "string" && isUuid(event.id) ? event.id.toLowerCase() : event.id,
    ...(receiveClockFallback ? { observedAt: "<receive-clock>" } : {}) };
  return createHash("sha256").update(JSON.stringify(stableJson(comparable))).digest("hex");
}
function hookReceiptIdentity(home: string, source: string, body: string | Buffer,
  eventId: string, config?: CollectorConfig) {
  try {
    const payload: unknown = JSON.parse(String(body));
    const storedConfig = config ?? (() => {
      try {
        return collectorConfigSchema.parse(JSON.parse(fs.readFileSync(path.join(home, "collector.config.json"), "utf8")));
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return collectorConfigSchema.parse({});
        throw error;
      }
    })();
    const options = { config: storedConfig,
      source: source as Parameters<typeof normalizeForwardedHook>[1]["source"],
      producerEventId: eventId };
    const atMs = Date.now();
    const event = normalizeForwardedHook(payload, { ...options, now: () => atMs }).event;
    const next = normalizeForwardedHook(payload, { ...options, now: () => atMs + 1_000 }).event;
    const receiveClockFallback = event.observedAt !== next.observedAt;
    return { kind: event.eventType,
      sessionId: event.sessionId ?? null,
      tenantId: event.tenantId,
      bodyDigest: hookBodyDigest(payload),
      originalTimestampDigest: originalHookTimestampDigest(payload),
      eventDigest: normalizedEventDigest(event as Record<string, unknown>, receiveClockFallback),
      receiveClockFallback };
  } catch {
    // Malformed or unnormalizable requests are still refused. Their receipt
    // can settle only on the exact terminal outcome, never on a ledger guess.
    let tenantId = config?.policy.tenantId;
    if (!tenantId) {
      try {
        tenantId = collectorConfigSchema.parse(JSON.parse(
          fs.readFileSync(path.join(home, "collector.config.json"), "utf8"))).policy.tenantId;
      } catch { tenantId = collectorConfigSchema.parse({}).policy.tenantId; }
    }
    return { kind: hookEventKind(body), sessionId: null, tenantId,
      bodyDigest: null,
      originalTimestampDigest: null, eventDigest: null, receiveClockFallback: false };
  }
}
function ledgerName(home: string): "work-ledger.sqlite" | "ledger.sqlite" {
  return fs.existsSync(path.join(home, "work-ledger.sqlite")) ? "work-ledger.sqlite" : "ledger.sqlite";
}
function selectedLedger(home: string, marker?: PauseMarker) {
  return path.join(home, marker?.ledgerName ?? ledgerName(home));
}
function ledgerHighWater(db: Database.Database) {
  const exists = db.prepare("select 1 from sqlite_master where type='table' and name='buffered_events'").get();
  if (!exists) return 0;
  const row = db.prepare("select coalesce(max(rowid), 0) as highWater from buffered_events")
    .get() as { highWater: number };
  if (!Number.isSafeInteger(row.highWater) || row.highWater < 0) throw new Error("maintenance_ledger_rowid_unsafe");
  return row.highWater;
}
function observedLedgerHighWater(file: string): number | null {
  if (!fs.existsSync(file)) return 0;
  try {
    const db = new Database(file, { readonly: true, fileMustExist: true, timeout: 0 });
    try { return ledgerHighWater(db); } finally { db.close(); }
  } catch { return null; }
}
function observedLedgerAdmissionSequence(file: string): number | null {
  if (!fs.existsSync(file)) return 0;
  try {
    const db = new Database(file, { readonly: true, fileMustExist: true, timeout: 0 });
    try { return ledgerAdmissionSequence(db); } finally { db.close(); }
  } catch { return null; }
}
type HookBoundary = { highWater: number | null; admissionSequence: number | null;
  ledgerAbsentAtRefusal: boolean };
/** Before quiescence, BEGIN IMMEDIATE holds writers until the receipt is
 * durable. During the fenced swap, the post-quiesce marker supplies the exact
 * last admission sequence; an unavailable snapshot remains unknown. */
function withHookHighWater<T>(home: string, marker: PauseMarker, action: (boundary: HookBoundary) => T): T {
  const ledger = selectedLedger(home, marker);
  const fenced = fs.existsSync(`${ledger}.maintenance-rebuild.lock`);
  if (!fs.existsSync(ledger)) return action(fenced
    ? { highWater: marker.ledgerHighWater ?? null,
      admissionSequence: marker.ledgerAdmissionSequence ?? null,
      ledgerAbsentAtRefusal: marker.ledgerExistedAtPause === false }
    : { highWater: marker.ledgerHighWater === 0 ? 0 : null,
      admissionSequence: marker.ledgerAdmissionSequence === 0 ? 0 : null,
      ledgerAbsentAtRefusal: marker.ledgerExistedAtPause === false });
  let token: string | null = null;
  let db: Database.Database | null = null;
  try {
    token = acquireRebuildOpenToken(ledger);
    db = new Database(ledger, { fileMustExist: true, timeout: 500 });
    db.exec("BEGIN IMMEDIATE");
  } catch (error) {
    db?.close();
    releaseRebuildOpenToken(token);
    if (fenced || (error instanceof Error && error.message === "maintenance_rebuild_paused")) {
      return action({ highWater: marker.ledgerHighWater ?? null,
        admissionSequence: marker.ledgerAdmissionSequence ?? null,
        ledgerAbsentAtRefusal: false });
    }
    // A busy or unreadable ledger cannot prove a pre-refusal row boundary.
    return action({ highWater: null, admissionSequence: null, ledgerAbsentAtRefusal: false });
  }
  try {
    const result = action({ highWater: ledgerHighWater(db),
      admissionSequence: ledgerAdmissionSequence(db), ledgerAbsentAtRefusal: false });
    db.exec("COMMIT");
    return result;
  } catch (error) {
    try { db.exec("ROLLBACK"); } catch { /* Preserve the first failure. */ }
    throw error;
  } finally {
    db.close();
    releaseRebuildOpenToken(token);
  }
}
function hookEventId(body: string | Buffer) {
  try {
    const parsed: unknown = JSON.parse(String(body));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    for (const alias of HOOK_AUTHORITY_CONTRACT.eventId.aliases) {
      const value = (parsed as Record<string, unknown>)[alias];
      if (typeof value === "string" && isUuid(value.trim())) return value.trim();
    }
  } catch { /* An invalid body is still a refused request. */ }
  return null;
}
function hookEventKind(body: string | Buffer) {
  try {
    const parsed: unknown = JSON.parse(String(body));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return "unknown";
    const record = parsed as Record<string, unknown>;
    for (const alias of [...HOOK_AUTHORITY_CONTRACT.eventType.aliases, "name", "span_name"]) {
      const value = record[alias];
      if (typeof value === "string") {
        const kind = classifyEventType(value);
        if (kind) return kind;
      }
    }
  } catch { /* A terminal rejection can settle invalid JSON. */ }
  return "unknown";
}
function readReceipt(file: string): RefusalReceipt {
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) throw new Error("maintenance_refusal_unsafe");
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as RefusalReceipt;
  if (!Number.isSafeInteger(value.version) || value.version < 1 ||
    value.version > 1_000_000 || !["hook", "otlp", "live"].includes(value.route) ||
    !Number.isFinite(Date.parse(value.at)) ||
    (value.source !== undefined && !/^[a-z_]{1,32}$/.test(value.source)) ||
    (value.eventId !== undefined && !isUuid(value.eventId)) ||
    (value.version === 3 && (value.route !== "hook" || !value.source || !value.eventId ||
      typeof value.kind !== "string" || !/^[a-z][a-z_]{0,32}$/.test(value.kind) ||
      !(value.ledgerHighWater === null ||
        (Number.isSafeInteger(value.ledgerHighWater) && (value.ledgerHighWater ?? -1) >= 0)))) ||
    (value.version === 4 && (value.route !== "hook" || !value.source || !value.eventId ||
      typeof value.kind !== "string" || !/^[a-z][a-z_]{0,32}$/.test(value.kind) ||
      !(value.eventDigest === null || (typeof value.eventDigest === "string" &&
        /^[a-f0-9]{64}$/.test(value.eventDigest))) ||
      typeof value.receiveClockFallback !== "boolean" ||
      !(value.ledgerHighWater === null ||
        (Number.isSafeInteger(value.ledgerHighWater) && (value.ledgerHighWater ?? -1) >= 0)))) ||
    (value.version === 5 && (value.route !== "hook" || !value.source || !value.eventId ||
      !value.receiptId || !isUuid(value.receiptId) ||
      typeof value.kind !== "string" || !/^[a-z][a-z_]{0,32}$/.test(value.kind) ||
      !(value.sessionId === null ||
        (typeof value.sessionId === "string" && value.sessionId.length <= 256)) ||
      !(value.originalTimestampDigest === null ||
        (typeof value.originalTimestampDigest === "string" &&
          /^[a-f0-9]{64}$/.test(value.originalTimestampDigest))) ||
      !(value.eventDigest === null || (typeof value.eventDigest === "string" &&
        /^[a-f0-9]{64}$/.test(value.eventDigest))) ||
      typeof value.receiveClockFallback !== "boolean" ||
      !(value.ledgerHighWater === null ||
        (Number.isSafeInteger(value.ledgerHighWater) && (value.ledgerHighWater ?? -1) >= 0)))) ||
    (value.version === 6 && (value.route !== "hook" || !value.source || !value.eventId ||
      !value.receiptId || !isUuid(value.receiptId) ||
      typeof value.kind !== "string" || !/^[a-z][a-z_]{0,32}$/.test(value.kind) ||
      !(value.sessionId === null ||
        (typeof value.sessionId === "string" && value.sessionId.length <= 256)) ||
      !(value.bodyDigest === null || (typeof value.bodyDigest === "string" &&
        /^[a-f0-9]{64}$/.test(value.bodyDigest))) ||
      !(value.ledgerHighWater === null ||
        (Number.isSafeInteger(value.ledgerHighWater) && (value.ledgerHighWater ?? -1) >= 0)))) ||
    (value.version === 7 && (value.route !== "hook" || !value.source || !value.eventId ||
      !value.receiptId || !isUuid(value.receiptId) ||
      typeof value.kind !== "string" || !/^[a-z][a-z_]{0,32}$/.test(value.kind) ||
      !(value.sessionId === null ||
        (typeof value.sessionId === "string" && value.sessionId.length <= 256)) ||
      typeof value.tenantId !== "string" || value.tenantId.length < 1 ||
      value.tenantId.length > 256 ||
      !(value.bodyDigest === null || (typeof value.bodyDigest === "string" &&
        /^[a-f0-9]{64}$/.test(value.bodyDigest))) ||
      !(value.ledgerAdmissionSequence === null ||
        (Number.isSafeInteger(value.ledgerAdmissionSequence) &&
          (value.ledgerAdmissionSequence ?? -1) >= 0)) ||
      typeof value.ledgerAbsentAtRefusal !== "boolean")) ||
    (value.unknownAt !== undefined && !Number.isFinite(Date.parse(value.unknownAt))) ||
    (value.spoolName !== undefined && !SPOOL_NAME.test(value.spoolName))) {
    throw new Error("maintenance_refusal_unsafe");
  }
  return value;
}
function writeReceipt(file: string, value: RefusalReceipt) {
  const temporary = `${file}.${randomUUID()}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, file);
  fsyncDirectory(path.dirname(file));
}

/** The 0.7.44 reader sees only the old spool grammar. When a direct caller
 * supplied no ID, bind its compatible retry to the ID durably minted by the
 * pause listener, so even an old drain records the same ledger key. */
export function prepareMaintenanceHookSpoolBody(home: string, source: string, body: string) {
  const original = existingRefusalPath(home, "hook", source, body);
  let receipt: RefusalReceipt | null = null;
  try { receipt = readReceipt(original); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
  const eventId = hookEventId(body) ?? receipt?.eventId ?? randomUUID();
  if (hookEventId(body)) return { body, receiptBody: body, eventId };
  let parsed: unknown;
  try { parsed = JSON.parse(body); } catch { return { body, receiptBody: body, eventId }; }
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return { body, receiptBody: body, eventId };
  const record = parsed as Record<string, unknown>;
  if (!Object.prototype.hasOwnProperty.call(record, "id")) record.id = eventId;
  else if (!Object.prototype.hasOwnProperty.call(record, "eventId")) record.eventId = eventId;
  else record.event_id = eventId;
  const stableBody = JSON.stringify(record);
  return { body: stableBody, receiptBody: receipt ? body : stableBody, eventId };
}

/** An authenticated direct hook may retry the exact body without a client
 * spool or its own ID. Bind only that ID-less body's pending receipt to the
 * normalizer; ordinary posts retain their existing ID selection. */
export function pendingMaintenanceHookEventId(home: string, source: string, body: string) {
  if (hookEventId(body)) return null;
  try {
    const receipt = readReceipt(existingRefusalPath(home, "hook", source, body));
    return receipt.route === "hook" && receipt.source === source ? receipt.eventId ?? null : null;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

/** A 503 has no server spool, so its route and body identity must survive the
 * listener's exit. Repeated refusals of the same payload share one receipt. */
export function recordMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer,
  options: { eventId?: string; spoolName?: string; config?: CollectorConfig } = {}) {
  const marker = readMaintenanceRebuildPause(home);
  if (!marker) throw new Error("maintenance_pause_marker_missing");
  const directory = refusalDirectory(home);
  try {
    fs.mkdirSync(directory, { mode: 0o700 });
    fsyncDirectory(home);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
  }
  const dirStat = fs.lstatSync(directory);
  if (!dirStat.isDirectory() || dirStat.isSymbolicLink()) throw new Error("maintenance_refusals_unsafe");
  const file = existingRefusalPath(home, route, source, body);
  let descriptor: number;
  try { descriptor = fs.openSync(file, "wx", 0o600); }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
    const prior = readReceipt(file);
    if (prior.route !== route) throw new Error("maintenance_refusal_unsafe");
    if (route === "hook") {
      const eventId = options.eventId ?? hookEventId(body) ?? prior.eventId ?? randomUUID();
      if (prior.eventId && !sameEventId(prior.eventId, eventId)) {
        throw new Error("maintenance_refusal_event_id_changed");
      }
      const identity = hookReceiptIdentity(home, source, body, eventId, options.config);
      if ((prior.version === 3 || prior.version === 4 || prior.version === 5 ||
        prior.version === 6 || prior.version === 7) &&
        (prior.source !== source || prior.kind !== identity.kind ||
          ((prior.version === 5 || prior.version === 6 || prior.version === 7) &&
            (!sameHookIdentityPart(prior.sessionId ?? null, identity.sessionId) ||
              prior.originalTimestampDigest !== identity.originalTimestampDigest ||
              ((prior.version === 6 || prior.version === 7) && prior.bodyDigest !== identity.bodyDigest) ||
              (prior.version === 7 && prior.tenantId !== identity.tenantId))))) {
        throw new Error("maintenance_refusal_identity_changed");
      }
      // Keep unknown and older evidence in its original format; guessing a
      // v7 tenant or sequence at this later retry moves the refusal boundary.
      if (prior.version !== 7) {
        if (options.spoolName && prior.spoolName !== options.spoolName)
          writeReceipt(file, { ...prior, spoolName: options.spoolName });
        return;
      }
      if (prior.version !== 7 || prior.source !== source || prior.eventId !== eventId ||
        (options.spoolName && prior.spoolName !== options.spoolName)) {
        withHookHighWater(home, marker, (boundary) => {
          writeReceipt(file, { ...prior, version: 7, source,
            receiptId: prior.receiptId ?? randomUUID(),
            eventId: prior.eventId ?? eventId, ...identity,
            ledgerHighWater: prior.version >= 3 ? prior.ledgerHighWater ?? null : boundary.highWater,
            ledgerAdmissionSequence: prior.version === 7
              ? prior.ledgerAdmissionSequence ?? null : boundary.admissionSequence,
            ledgerAbsentAtRefusal: prior.version === 7
              ? prior.ledgerAbsentAtRefusal ?? false : boundary.ledgerAbsentAtRefusal,
            ...(options.spoolName ? { spoolName: options.spoolName } : {}) });
        });
      }
    }
    return;
  }
  try {
    if (route === "hook") {
      const eventId = options.eventId ?? hookEventId(body) ?? randomUUID();
      const identity = hookReceiptIdentity(home, source, body, eventId, options.config);
      withHookHighWater(home, marker, (boundary) => {
        const value: RefusalReceipt = { version: 7, route, source, receiptId: randomUUID(),
          at: new Date().toISOString(), eventId, ...identity,
          ledgerHighWater: boundary.highWater,
          ledgerAdmissionSequence: boundary.admissionSequence,
          ledgerAbsentAtRefusal: boundary.ledgerAbsentAtRefusal,
          ...(options.spoolName ? { spoolName: options.spoolName } : {}) };
        fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`);
        fs.fsyncSync(descriptor);
      });
    } else {
      const value: RefusalReceipt = { version: 1, route, at: new Date().toISOString() };
      fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`);
      fs.fsyncSync(descriptor);
    }
  } finally { fs.closeSync(descriptor); }
  fsyncDirectory(directory);
}

type AdmissionMatch = "accepted" | "unverified" | "unknown" | "none";
function ledgerAdmissionMatches(db: Database.Database, receipt: RefusalReceipt): AdmissionMatch {
  if (receipt.version !== 7 || !receipt.receiptId || !receipt.eventId ||
    !receipt.source || !receipt.kind || !receipt.tenantId) return "none";
  const hasAdmissionTable = db.prepare(`select 1 from sqlite_master
    where type = 'table' and name = 'maintenance_rebuild_hook_admissions'`).get();
  const admissions = hasAdmissionTable ? db.prepare(HOOK_ACK_LOOKUP_SQL).all(receipt.receiptId) as
    Array<{ admitted_event_id: string; outcome: "accepted" | "mismatch" }> : [];
  if (admissions.some((admission) => admission.outcome === "accepted")) return "accepted";
  const rejectedIds = new Set(admissions.filter((admission) => admission.outcome === "mismatch")
    .map((admission) => admission.admitted_event_id));
  // SQLite's ID primary-key index supports every spelling below. A matching
  // immutable digest proves capture regardless of insertion order. Only the
  // digestless 0.7.44 fallback uses the never-reused admission sequence.
  const variants = [receipt.eventId, receipt.eventId.toLowerCase(), receipt.eventId.toUpperCase()];
  const hasDigest = db.prepare(`select 1 from pragma_table_info('buffered_events')
    where name = 'maintenance_hook_body_digest'`).get();
  const hasOrder = db.prepare(`select 1 from sqlite_master where type = 'table'
    and name = 'maintenance_rebuild_event_order'`).get();
  const rows = db.prepare(hasDigest && hasOrder ? HOOK_ROW_LOOKUP_SQL : HOOK_ROW_LEGACY_LOOKUP_SQL).all(...variants) as
    Array<{ rowid: number; id: string; source: string; event_type: string;
      session_id: string | null; tenant_id: string | null;
      body_digest: string | null; admission_seq: number | null }>;
  let ambiguous = false;
  let digestlessAfter = false;
  let digestedAfter = false;
  for (const row of rows) {
    if (rejectedIds.has(row.id) || !sameEventId(row.id, receipt.eventId) ||
      row.source !== receipt.source || row.event_type !== receipt.kind ||
      row.tenant_id !== receipt.tenantId ||
      !sameHookIdentityPart(row.session_id, receipt.sessionId ?? null)) continue;
    ambiguous = true;
    if (receipt.bodyDigest && row.body_digest === receipt.bodyDigest) return "accepted";
    if (row.body_digest !== null) { digestedAfter = true; continue; }
    if (receipt.ledgerAdmissionSequence !== null &&
      receipt.ledgerAdmissionSequence !== undefined &&
      row.admission_seq !== null &&
      row.admission_seq > receipt.ledgerAdmissionSequence) digestlessAfter = true;
    // A brand-new ledger had no pre-refusal rows. A downgrade may create it
    // without the trigger, so its first digestless row is still later evidence.
    if (receipt.ledgerAbsentAtRefusal && row.admission_seq === null)
      digestlessAfter = true;
  }
  if (digestlessAfter && !digestedAfter) return "unverified";
  return ambiguous ? "unknown" : "none";
}

type TerminalRecord = { version?: number; receipt?: string; at?: string;
  outcome?: string; eventId?: string | null; source?: string; kind?: string; spoolName?: string };
function terminalRecords(home: string): TerminalRecord[] {
  try {
    return fs.readFileSync(path.join(home, TERMINAL), "utf8").split("\n").filter(Boolean)
      .flatMap((line) => {
        try { return [JSON.parse(line) as TerminalRecord]; }
        catch { return []; /* An unfinished last line is not a durable outcome. */ }
      });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
    throw error;
  }
}
function unverifiedCount(records: TerminalRecord[]) {
  return new Set(records.filter((entry) => entry.version === 1 &&
    entry.outcome === "retired_unverified" && typeof entry.receipt === "string" &&
    /^[a-f0-9]{64}\.receipt$/.test(entry.receipt) && typeof entry.at === "string")
    .map((entry) => `${entry.receipt}\0${entry.at}`)).size;
}
function unknownFormatRecords(records: TerminalRecord[]) {
  return records.filter((entry) => entry.version === 1 &&
    entry.outcome === "unknown_receipt_format" && typeof entry.receipt === "string" &&
    /^[a-f0-9]{64}\.receipt$/.test(entry.receipt) && typeof entry.at === "string" &&
    Number.isFinite(Date.parse(entry.at)));
}
function unknownFormatCount(records: TerminalRecord[]) {
  return new Set(unknownFormatRecords(records)
    .map((entry) => `${entry.receipt}\0${entry.at}`)).size;
}
function unknownFormatLosses(records: TerminalRecord[]) {
  const unique = new Map(unknownFormatRecords(records)
    .map((entry) => [`${entry.receipt}\0${entry.at}`, entry]));
  return [...unique.values()].map((entry) => {
    const at = Date.parse(entry.at!);
    return { fromMs: at, toMs: at, count: 1 };
  });
}
export function readUnverifiedHookRetries(home: string): number | null {
  try { return unverifiedCount(terminalRecords(home)); }
  catch { return null; }
}
export function readUnknownHookReceiptFormats(home: string): number | null {
  try { return unknownFormatCount(terminalRecords(home)); }
  catch { return null; }
}

function terminalTailStart(descriptor: number, size: number) {
  const block = Buffer.alloc(4096);
  for (let cursor = size; cursor > 0;) {
    const length = Math.min(block.length, cursor);
    const start = cursor - length;
    fs.readSync(descriptor, block, 0, length, start);
    for (let index = length - 1; index >= 0; index -= 1) {
      if (block[index] === 10) return start + index + 1;
    }
    cursor = start;
  }
  return 0;
}
/** Repair an unterminated final record before appending. The repair and its
 * note are durable before the new terminal outcome can retire a receipt. */
function appendTerminalOutcome(home: string, value: Record<string, unknown>) {
  const file = path.join(home, TERMINAL);
  // A permanent sibling SQLite lock file supplies an OS-released,
  // process-shared exclusive lock, including after SIGKILL. Hold it from tail
  // inspection through repair, append, fsync and directory publication.
  withRebuildCoordination(file, () => {
    const flags = fs.constants.O_RDWR | fs.constants.O_CREAT | fs.constants.O_APPEND |
      (fs.constants.O_NOFOLLOW ?? 0);
    const descriptor = fs.openSync(file, flags, 0o600);
    try {
    const stat = fs.fstatSync(descriptor);
    if (!stat.isFile()) throw new Error("maintenance_terminal_unsafe");
    fs.fchmodSync(descriptor, 0o600);
    if (stat.size > 0) {
      const last = Buffer.alloc(1);
      fs.readSync(descriptor, last, 0, 1, stat.size - 1);
      if (last[0] !== 10) {
        const start = terminalTailStart(descriptor, stat.size);
        const length = stat.size - start;
        let complete = false;
        if (length <= 65_536) {
          const tail = Buffer.alloc(length);
          fs.readSync(descriptor, tail, 0, length, start);
          try { JSON.parse(tail.toString("utf8")); complete = true; } catch { /* Torn last line. */ }
        }
        if (complete) {
          fs.writeFileSync(descriptor, "\n");
        } else {
          fs.ftruncateSync(descriptor, start);
          fs.fsyncSync(descriptor);
          fs.writeFileSync(descriptor, `${JSON.stringify({ version: 1,
            event: "recovered_torn_tail", truncatedBytes: length, at: new Date().toISOString() })}\n`);
          fs.fsyncSync(descriptor);
        }
      }
    }
    fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`);
    fs.fsyncSync(descriptor);
    } finally { fs.closeSync(descriptor); }
    fsyncDirectory(home);
  });
}

/** Only a matching retry whose normal route committed may retire this file. */
export function resolveMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer,
  options: { outcome?: "accepted" | "terminal"; acceptedEventId?: string;
    ledger?: Database.Database; spoolName?: string } = {}) {
  let file = existingRefusalPath(home, route, source, body);
  try {
    let receipt: RefusalReceipt;
    try { receipt = readReceipt(file); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT" || route !== "hook" || !options.spoolName) throw error;
      const entries = fs.readdirSync(refusalDirectory(home)).filter((entry) => entry.endsWith(".receipt"));
      const matched = entries.map((entry) => path.join(refusalDirectory(home), entry))
        .filter((candidate) => {
          const value = readReceipt(candidate);
          return value.route === "hook" && value.source === source &&
            value.spoolName === options.spoolName && !!value.eventId && !!hookEventId(body) &&
            sameEventId(value.eventId, hookEventId(body)!);
        });
      if (matched.length !== 1) return;
      file = matched[0]!;
      receipt = readReceipt(file);
    }
    if (route === "hook" && options.outcome !== "terminal") {
      if (!receipt.eventId || !options.acceptedEventId ||
        !sameEventId(options.acceptedEventId, receipt.eventId) || !options.ledger ||
        ledgerAdmissionMatches(options.ledger, receipt) !== "accepted") return;
    }
    if (options.outcome === "terminal") {
      // Record the exact receipt instance before removing its hold. A crash
      // after this fsync is repaired by reconciliation on the next startup.
      appendTerminalOutcome(home, { version: 1, receipt: path.basename(file),
        at: receipt.at, route, eventId: receipt.eventId ?? null, outcome: "terminal" });
    }
    fs.unlinkSync(file);
    fsyncDirectory(refusalDirectory(home));
    if (receipt.receiptId && options.ledger) removeMaintenanceHookAdmission(options.ledger, receipt.receiptId);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
}

/** null means the receipt inventory is unsafe or unreadable: hold attestation. */
export function countMaintenanceRebuildRefusals(home: string): number | null {
  return reconcileMaintenanceRebuildRefusals(home).count;
}

/** Only a row admitted after this refusal, with its canonical ID, source and
 * kind, may settle an old-version drain. Missing files alone never do. */
export function reconcileMaintenanceRebuildRefusals(home: string,
  ledgerPath?: string, nowMs = Date.now()): { count: number | null;
    unverifiedHookRetries: number | null;
    unknownHookReceiptFormats: number | null;
    lost: Array<{ fromMs: number; toMs: number; count: number }> } {
  const directory = refusalDirectory(home);
  try {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink()) return {
      count: null, unverifiedHookRetries: null, unknownHookReceiptFormats: null, lost: [] };
    const entries = fs.readdirSync(directory, { withFileTypes: true });
    if (entries.some((entry) =>
      !(/^[a-f0-9]{64}\.receipt$/.test(entry.name) ||
        /^[a-f0-9]{64}\.receipt\.[0-9a-f-]{36}\.tmp$/.test(entry.name)) ||
      !entry.isFile() || entry.isSymbolicLink())) return {
        count: null, unverifiedHookRetries: null, unknownHookReceiptFormats: null, lost: [] };
    const receiptFiles = entries.filter((entry) => entry.name.endsWith(".receipt"))
      .map((entry) => path.join(directory, entry.name));
    if (receiptFiles.length === 0) {
      const records = terminalRecords(home);
      return { count: 0, unverifiedHookRetries: unverifiedCount(records),
        unknownHookReceiptFormats: unknownFormatCount(records), lost: unknownFormatLosses(records) };
    }
    const receipts = receiptFiles.map(readReceipt);
    const terminal = new Set<string>();
    let unverifiedHookRetries: number;
    let unknownHookReceiptFormats: number;
    let lost: Array<{ fromMs: number; toMs: number; count: number }>;
    try {
      const records = terminalRecords(home);
      unverifiedHookRetries = unverifiedCount(records);
      unknownHookReceiptFormats = unknownFormatCount(records);
      lost = unknownFormatLosses(records);
      for (const value of records) {
        if (value.version === 1 && (value.outcome === "terminal" ||
          value.outcome === "retired_unverified" || value.outcome === "unknown_receipt_format") &&
          typeof value.receipt === "string" && /^[a-f0-9]{64}\.receipt$/.test(value.receipt) &&
          typeof value.at === "string") terminal.add(`${value.receipt}\0${value.at}`);
      }
    } catch (error) {
      return { count: null, unverifiedHookRetries: null, unknownHookReceiptFormats: null, lost: [] };
    }
    const hookReceipts = receipts.filter((receipt) => receipt.route === "hook");
    const retiredIds: string[] = [];
    let db: Database.Database | null = null;
    const marker = readMaintenanceRebuildPause(home);
    const selected = ledgerPath ?? selectedLedger(home, marker ?? undefined);
    if (hookReceipts.length > 0) {
      if (fs.existsSync(selected) && !fs.existsSync(`${selected}.maintenance-rebuild.lock`)) {
        try { db = new Database(selected, { readonly: true, fileMustExist: true, timeout: 0 }); }
        catch { /* An unavailable ledger holds receipts until a later pass. */ }
      }
    }
    let count = 0;
    try {
      for (let index = 0; index < receipts.length; index += 1) {
        const receipt = receipts[index]!;
        const file = receiptFiles[index]!;
        if (terminal.has(`${path.basename(file)}\0${receipt.at}`)) {
          fs.unlinkSync(file);
          fsyncDirectory(directory);
          if (receipt.receiptId) retiredIds.push(receipt.receiptId);
          continue;
        }
        if (receipt.route !== "hook") { count += 1; continue; }
        let match: AdmissionMatch = "none";
        let checked = false;
        if (db && receipt.eventId) {
          try {
            match = ledgerAdmissionMatches(db, receipt);
            checked = true;
          }
          catch { /* An unreadable inventory never proves acceptance. */ }
        }
        if (match === "accepted") {
          fs.unlinkSync(file);
          fsyncDirectory(directory);
          if (receipt.receiptId) retiredIds.push(receipt.receiptId);
          continue;
        }
        const pendingFile = receipt.spoolName &&
          fs.existsSync(path.join(home, "hook-spool", receipt.spoolName));
        const atMs = Date.parse(receipt.at);
        if (receipt.version !== 7 && !pendingFile && nowMs - atMs >= MISSING_HOOK_RETRY_MS) {
          // Previous unreleased receipt formats lack tenant and/or immutable
          // caller-body evidence. Never guess acceptance, and never hold a
          // capture claim forever after the retry window.
          appendTerminalOutcome(home, { version: 1, receipt: path.basename(file), at: receipt.at,
            route: "hook", eventId: receipt.eventId ?? null, source: receipt.source,
            kind: receipt.kind, spoolName: receipt.spoolName, outcome: "unknown_receipt_format" });
          unknownHookReceiptFormats += 1;
          lost.push({ fromMs: atMs, toMs: atMs, count: 1 });
          fs.unlinkSync(file);
          fsyncDirectory(directory);
          if (receipt.receiptId) retiredIds.push(receipt.receiptId);
          continue;
        }
        if (match === "unverified" && receipt.spoolName && !pendingFile) {
          appendTerminalOutcome(home, { version: 1, receipt: path.basename(file), at: receipt.at,
            route: "hook", eventId: receipt.eventId ?? null, source: receipt.source,
            kind: receipt.kind, spoolName: receipt.spoolName, outcome: "retired_unverified" });
          unverifiedHookRetries += 1;
          console.warn(JSON.stringify({ status: "maintenance_hook_retired_unverified",
            eventId: receipt.eventId, source: receipt.source, kind: receipt.kind,
            spoolName: receipt.spoolName }));
          fs.unlinkSync(file);
          fsyncDirectory(directory);
          if (receipt.receiptId) retiredIds.push(receipt.receiptId);
          continue;
        }
        if (!pendingFile && (checked || !receipt.eventId) && nowMs - atMs >= MISSING_HOOK_RETRY_MS) {
          if (!receipt.unknownAt) writeReceipt(file, { ...receipt, unknownAt: new Date(nowMs).toISOString() });
          if (match === "none") lost.push({ fromMs: atMs, toMs: atMs, count: 1 });
          else count += 1;
        } else count += 1;
      }
    } finally { db?.close(); }
    if (retiredIds.length > 0 && fs.existsSync(selected)) {
      let token: string | null = null;
      let writer: Database.Database | null = null;
      try {
        token = acquireRebuildOpenToken(selected);
        writer = new Database(selected, { fileMustExist: true, timeout: 0 });
        const remove = writer.transaction(() => {
          for (const id of retiredIds) removeMaintenanceHookAdmission(writer!, id);
        });
        remove.immediate();
      } catch { /* Retention also removes orphan acknowledgements after the event expires. */ }
      finally { writer?.close(); releaseRebuildOpenToken(token); }
    }
    return { count, unverifiedHookRetries, unknownHookReceiptFormats, lost };
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT"
      ? { count: 0, unverifiedHookRetries: readUnverifiedHookRetries(home),
        unknownHookReceiptFormats: readUnknownHookReceiptFormats(home), lost: [] }
      : { count: null, unverifiedHookRetries: null, unknownHookReceiptFormats: null, lost: [] };
  }
}

function writeMarker(home: string, marker: PauseMarker) {
  const file = path.join(home, MARKER);
  const temporary = `${file}.${process.pid}.tmp`;
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try { fs.writeFileSync(descriptor, `${JSON.stringify(marker)}\n`); fs.fsyncSync(descriptor); }
  finally { fs.closeSync(descriptor); }
  fs.renameSync(temporary, file);
  const directory = fs.openSync(home, "r");
  try { fs.fsyncSync(directory); } finally { fs.closeSync(directory); }
}

export function markMaintenanceRebuildPause(home: string) {
  const name = ledgerName(home);
  const file = path.join(home, name);
  const selectedExisted = fs.existsSync(file);
  const alternate = path.join(home, name === "ledger.sqlite" ? "work-ledger.sqlite" : "ledger.sqlite");
  const existedAtPause = selectedExisted || fs.existsSync(alternate);
  writeMarker(home, { version: 1, at: new Date().toISOString(), pid: process.pid,
    ledgerName: name, ledgerHighWater: selectedExisted ? observedLedgerHighWater(file)
      : existedAtPause ? null : 0,
    ledgerAdmissionSequence: selectedExisted ? observedLedgerAdmissionSequence(file)
      : existedAtPause ? null : 0,
    ledgerExistedAtPause: existedAtPause });
}

/** Called immediately after every daemon writer has quiesced, before the
 * rebuild publishes its fence. Later 503 receipts can use this exact boundary
 * while the exclusive source lock prevents a live SQLite query. */
export function refreshMaintenanceRebuildPauseHighWater(ledgerPath: string) {
  const home = path.dirname(ledgerPath);
  const marker = readMaintenanceRebuildPause(home);
  if (!marker || marker.endedAt) return;
  const name = path.basename(ledgerPath);
  if (name !== "work-ledger.sqlite" && name !== "ledger.sqlite") return;
  const highWater = observedLedgerHighWater(ledgerPath);
  if (highWater === null) throw new Error("maintenance_ledger_high_water_unavailable");
  const admissionSequence = observedLedgerAdmissionSequence(ledgerPath);
  if (admissionSequence === null) throw new Error("maintenance_admission_sequence_unavailable");
  writeMarker(home, { ...marker, ledgerName: name, ledgerHighWater: highWater,
    ledgerAdmissionSequence: admissionSequence });
}

export function readMaintenanceRebuildPause(home: string): PauseMarker | null {
  try {
    const file = path.join(home, MARKER);
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("maintenance_pause_marker_invalid");
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as PauseMarker;
    if (value.version !== 1 || !Number.isFinite(Date.parse(value.at)) ||
      (value.ledgerName !== undefined && value.ledgerName !== "work-ledger.sqlite" &&
        value.ledgerName !== "ledger.sqlite") ||
      (value.ledgerHighWater !== undefined && value.ledgerHighWater !== null &&
        (!Number.isSafeInteger(value.ledgerHighWater) || value.ledgerHighWater < 0)) ||
      (value.ledgerAdmissionSequence !== undefined && value.ledgerAdmissionSequence !== null &&
        (!Number.isSafeInteger(value.ledgerAdmissionSequence) || value.ledgerAdmissionSequence < 0)) ||
      (value.ledgerExistedAtPause !== undefined && typeof value.ledgerExistedAtPause !== "boolean") ||
      (value.endedAt !== undefined && !Number.isFinite(Date.parse(value.endedAt)))) {
      throw new Error("maintenance_pause_marker_invalid");
    }
    return value;
  }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export function finishMaintenanceRebuildPause(home: string) {
  const marker = readMaintenanceRebuildPause(home);
  if (marker && !marker.endedAt) writeMarker(home, { ...marker, endedAt: new Date().toISOString() });
}

/** A SIGKILL cannot stamp endedAt. A later daemon may settle its dead
 * listener's marker before deciding whether any of its arrivals remain. */
export function settleInterruptedMaintenanceRebuildPause(home: string) {
  const marker = readMaintenanceRebuildPause(home);
  if (!marker || marker.endedAt) return marker;
  // Markers written by the first B13 version have no PID. Only that version
  // wrote this shape, so a current daemon reading it is after its pause.
  if (!marker.pid) {
    finishMaintenanceRebuildPause(home);
    return readMaintenanceRebuildPause(home);
  }
  try { process.kill(marker.pid, 0); return marker; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ESRCH") return marker;
    finishMaintenanceRebuildPause(home);
    return readMaintenanceRebuildPause(home);
  }
}

export function clearMaintenanceRebuildPause(home: string) {
  try { fs.unlinkSync(path.join(home, MARKER)); }
  catch (error) { if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error; }
}

export function maintenanceRebuildPauseSeen(home: string) {
  return readMaintenanceRebuildPause(home) !== null;
}
