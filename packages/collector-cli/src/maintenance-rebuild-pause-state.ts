import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import { classifyEventType, isUuid } from "./normalizer";
import { acquireRebuildOpenToken, releaseRebuildOpenToken } from "./rebuild-open-gate";

const MARKER = "maintenance-rebuild-pause.json";
const REFUSALS = "maintenance-rebuild-refusals";
const TERMINAL = "maintenance-rebuild-terminal.jsonl";
type PauseMarker = { version: 1; at: string; pid?: number; endedAt?: string;
  ledgerName?: "work-ledger.sqlite" | "ledger.sqlite"; ledgerHighWater?: number | null };

type RefusalRoute = "hook" | "otlp" | "live";
type RefusalReceipt = { version: 1 | 2 | 3; route: RefusalRoute; at: string;
  source?: string; eventId?: string; kind?: string; ledgerHighWater?: number | null;
  spoolName?: string; unknownAt?: string };
/** The client writes its retry immediately after the response; the spool's
 * ten-minute stale-pending diagnostic is our conservative missing-retry
 * threshold. A durable receipt remains so later exact acceptance can heal it. */
export const MISSING_HOOK_RETRY_MS = 600_000;
const SPOOL_NAME = /^\d{13,}-\d+-[0-9a-f]{6}\.json$/;
function refusalDirectory(home: string) { return path.join(home, REFUSALS); }
function refusalPath(home: string, route: RefusalRoute, source: string, body: string | Buffer) {
  const digest = createHash("sha256").update(`${route}\0${source}\0`).update(body).digest("hex");
  return path.join(refusalDirectory(home), `${digest}.receipt`);
}
function fsyncDirectory(directory: string) {
  const descriptor = fs.openSync(directory, "r");
  try { fs.fsyncSync(descriptor); } finally { fs.closeSync(descriptor); }
}
function sameEventId(left: string, right: string) {
  return isUuid(left) && isUuid(right)
    ? left.toLowerCase() === right.toLowerCase() : left === right;
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
/** Before quiescence, BEGIN IMMEDIATE holds writers until the receipt is
 * durable. During the fenced swap, the post-quiesce marker supplies the exact
 * last rowid; an unavailable snapshot remains unknown instead of guessing. */
function withHookHighWater<T>(home: string, marker: PauseMarker, action: (highWater: number | null) => T): T {
  const ledger = selectedLedger(home, marker);
  const fenced = fs.existsSync(`${ledger}.maintenance-rebuild.lock`);
  if (!fs.existsSync(ledger)) return action(fenced ? marker.ledgerHighWater ?? null :
    marker.ledgerHighWater === 0 ? 0 : null);
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
      return action(marker.ledgerHighWater ?? null);
    }
    // A busy or unreadable ledger cannot prove a pre-refusal row boundary.
    return action(null);
  }
  try {
    const result = action(ledgerHighWater(db));
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
  if (![1, 2, 3].includes(value.version) || !["hook", "otlp", "live"].includes(value.route) ||
    !Number.isFinite(Date.parse(value.at)) ||
    (value.source !== undefined && !/^[a-z_]{1,32}$/.test(value.source)) ||
    (value.eventId !== undefined && !isUuid(value.eventId)) ||
    (value.version === 3 && (value.route !== "hook" || !value.source || !value.eventId ||
      typeof value.kind !== "string" || !/^[a-z][a-z_]{0,32}$/.test(value.kind) ||
      !(value.ledgerHighWater === null ||
        (Number.isSafeInteger(value.ledgerHighWater) && (value.ledgerHighWater ?? -1) >= 0)))) ||
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
  const original = refusalPath(home, "hook", source, body);
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
    const receipt = readReceipt(refusalPath(home, "hook", source, body));
    return receipt.route === "hook" && receipt.source === source ? receipt.eventId ?? null : null;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

/** A 503 has no server spool, so its route and body identity must survive the
 * listener's exit. Repeated refusals of the same payload share one receipt. */
export function recordMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer, options: { eventId?: string; spoolName?: string } = {}) {
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
  const file = refusalPath(home, route, source, body);
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
      const kind = hookEventKind(body);
      if (prior.version === 3 && (prior.source !== source || prior.kind !== kind)) {
        throw new Error("maintenance_refusal_identity_changed");
      }
      if (prior.version !== 3 || prior.source !== source || prior.eventId !== eventId ||
        (options.spoolName && prior.spoolName !== options.spoolName)) {
        withHookHighWater(home, marker, (highWater) => {
          writeReceipt(file, { ...prior, version: 3, source,
            eventId: prior.eventId ?? eventId, kind,
            ledgerHighWater: prior.version === 3 ? prior.ledgerHighWater ?? null : highWater,
            ...(options.spoolName ? { spoolName: options.spoolName } : {}) });
        });
      }
    }
    return;
  }
  try {
    if (route === "hook") {
      withHookHighWater(home, marker, (highWater) => {
        const value: RefusalReceipt = { version: 3, route, source,
          at: new Date().toISOString(), eventId: options.eventId ?? hookEventId(body) ?? randomUUID(),
          kind: hookEventKind(body), ledgerHighWater: highWater,
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

function ledgerAdmissionMatches(db: Database.Database, receipt: RefusalReceipt) {
  if (receipt.version !== 3 || !receipt.eventId || !receipt.source || !receipt.kind ||
    !Number.isSafeInteger(receipt.ledgerHighWater) || (receipt.ledgerHighWater ?? -1) < 0) return false;
  const idPredicate = isUuid(receipt.eventId) ? "id = ? COLLATE NOCASE" : "id = ?";
  return Boolean(db.prepare(`select 1 from buffered_events where rowid > ? and source = ?
    and event_type = ? and ${idPredicate} limit 1`)
    .get(receipt.ledgerHighWater, receipt.source, receipt.kind, receipt.eventId));
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
}

/** Only a matching retry whose normal route committed may retire this file. */
export function resolveMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer,
  options: { outcome?: "accepted" | "terminal"; acceptedEventId?: string;
    ledger?: Database.Database; spoolName?: string } = {}) {
  let file = refusalPath(home, route, source, body);
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
        !ledgerAdmissionMatches(options.ledger, receipt)) return;
    }
    if (options.outcome === "terminal") {
      // Record the exact receipt instance before removing its hold. A crash
      // after this fsync is repaired by reconciliation on the next startup.
      appendTerminalOutcome(home, { version: 1, receipt: path.basename(file),
        at: receipt.at, route, eventId: receipt.eventId ?? null, outcome: "terminal" });
    }
    fs.unlinkSync(file);
    fsyncDirectory(refusalDirectory(home));
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
  ledgerPath?: string, nowMs = Date.now()): { count: number | null; lost: Array<{ fromMs: number; toMs: number; count: number }> } {
  const directory = refusalDirectory(home);
  try {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink()) return { count: null, lost: [] };
    const entries = fs.readdirSync(directory, { withFileTypes: true });
    if (entries.some((entry) =>
      !(/^[a-f0-9]{64}\.receipt$/.test(entry.name) ||
        /^[a-f0-9]{64}\.receipt\.[0-9a-f-]{36}\.tmp$/.test(entry.name)) ||
      !entry.isFile() || entry.isSymbolicLink())) return { count: null, lost: [] };
    const receiptFiles = entries.filter((entry) => entry.name.endsWith(".receipt"))
      .map((entry) => path.join(directory, entry.name));
    if (receiptFiles.length === 0) return { count: 0, lost: [] };
    const receipts = receiptFiles.map(readReceipt);
    const terminal = new Set<string>();
    try {
      for (const line of fs.readFileSync(path.join(home, TERMINAL), "utf8").split("\n").filter(Boolean)) {
        try {
          const value = JSON.parse(line) as { version?: number; receipt?: string; at?: string;
            outcome?: string };
          if (value.version === 1 && value.outcome === "terminal" &&
            typeof value.receipt === "string" && /^[a-f0-9]{64}\.receipt$/.test(value.receipt) &&
            typeof value.at === "string") terminal.add(`${value.receipt}\0${value.at}`);
        } catch { /* A killed append leaves its receipt for exact retry or a visible unknown. */ }
      }
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "ENOENT") return { count: null, lost: [] };
    }
    const hookReceipts = receipts.filter((receipt) => receipt.route === "hook");
    let db: Database.Database | null = null;
    if (hookReceipts.length > 0) {
      const marker = readMaintenanceRebuildPause(home);
      const selected = ledgerPath ?? selectedLedger(home, marker ?? undefined);
      if (fs.existsSync(selected) && !fs.existsSync(`${selected}.maintenance-rebuild.lock`)) {
        try { db = new Database(selected, { readonly: true, fileMustExist: true, timeout: 0 }); }
        catch { /* An unavailable ledger holds receipts until a later pass. */ }
      }
    }
    const lost: Array<{ fromMs: number; toMs: number; count: number }> = [];
    let count = 0;
    try {
      for (let index = 0; index < receipts.length; index += 1) {
        const receipt = receipts[index]!;
        const file = receiptFiles[index]!;
        if (terminal.has(`${path.basename(file)}\0${receipt.at}`)) {
          fs.unlinkSync(file);
          fsyncDirectory(directory);
          continue;
        }
        if (receipt.route !== "hook") { count += 1; continue; }
        let accepted = false;
        let checked = false;
        if (db && receipt.eventId) {
          try {
            accepted = ledgerAdmissionMatches(db, receipt);
            checked = true;
          }
          catch { /* An unreadable inventory never proves acceptance. */ }
        }
        if (accepted) {
          fs.unlinkSync(file);
          fsyncDirectory(directory);
          continue;
        }
        const pendingFile = receipt.spoolName &&
          fs.existsSync(path.join(home, "hook-spool", receipt.spoolName));
        const atMs = Date.parse(receipt.at);
        if (!pendingFile && (checked || !receipt.eventId) && nowMs - atMs >= MISSING_HOOK_RETRY_MS) {
          if (!receipt.unknownAt) writeReceipt(file, { ...receipt, unknownAt: new Date(nowMs).toISOString() });
          lost.push({ fromMs: atMs, toMs: atMs, count: 1 });
        } else count += 1;
      }
    } finally { db?.close(); }
    return { count, lost };
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "ENOENT"
      ? { count: 0, lost: [] } : { count: null, lost: [] };
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
  writeMarker(home, { version: 1, at: new Date().toISOString(), pid: process.pid,
    ledgerName: name, ledgerHighWater: observedLedgerHighWater(path.join(home, name)) });
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
  writeMarker(home, { ...marker, ledgerName: name, ledgerHighWater: highWater });
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
