import fs from "node:fs";
import path from "node:path";
import { createHash, randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import { isUuid } from "./normalizer";

const MARKER = "maintenance-rebuild-pause.json";
const REFUSALS = "maintenance-rebuild-refusals";
const TERMINAL = "maintenance-rebuild-terminal.jsonl";
type PauseMarker = { version: 1; at: string; pid?: number; endedAt?: string };

type RefusalRoute = "hook" | "otlp" | "live";
type RefusalReceipt = { version: 1 | 2; route: RefusalRoute; at: string;
  source?: string; eventId?: string; spoolName?: string; unknownAt?: string };
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
function hookEventId(body: string | Buffer) {
  try {
    const parsed: unknown = JSON.parse(String(body));
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) return null;
    for (const alias of HOOK_AUTHORITY_CONTRACT.eventId.aliases) {
      const value = (parsed as Record<string, unknown>)[alias];
      if (typeof value === "string" && isUuid(value.trim())) return value.trim().toLowerCase();
    }
  } catch { /* An invalid body is still a refused request. */ }
  return null;
}
function readReceipt(file: string): RefusalReceipt {
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) throw new Error("maintenance_refusal_unsafe");
  const value = JSON.parse(fs.readFileSync(file, "utf8")) as RefusalReceipt;
  if (![1, 2].includes(value.version) || !["hook", "otlp", "live"].includes(value.route) ||
    !Number.isFinite(Date.parse(value.at)) ||
    (value.source !== undefined && !/^[a-z_]{1,32}$/.test(value.source)) ||
    (value.eventId !== undefined && !isUuid(value.eventId)) ||
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

/** A 503 has no server spool, so its route and body identity must survive the
 * listener's exit. Repeated refusals of the same payload share one receipt. */
export function recordMaintenanceRebuildRefusal(home: string, route: RefusalRoute,
  source: string, body: string | Buffer, options: { eventId?: string; spoolName?: string } = {}) {
  if (!readMaintenanceRebuildPause(home)) throw new Error("maintenance_pause_marker_missing");
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
      if (prior.eventId && prior.eventId !== eventId) throw new Error("maintenance_refusal_event_id_changed");
      if (prior.version !== 2 || prior.source !== source || prior.eventId !== eventId ||
        (options.spoolName && prior.spoolName !== options.spoolName)) {
        writeReceipt(file, { ...prior, version: 2, source, eventId,
          ...(options.spoolName ? { spoolName: options.spoolName } : {}) });
      }
    }
    return;
  }
  try {
    const value: RefusalReceipt = route === "hook"
      ? { version: 2, route, source, at: new Date().toISOString(),
        eventId: options.eventId ?? hookEventId(body) ?? randomUUID(),
        ...(options.spoolName ? { spoolName: options.spoolName } : {}) }
      : { version: 1, route, at: new Date().toISOString() };
    fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`);
    fs.fsyncSync(descriptor);
  } finally { fs.closeSync(descriptor); }
  fsyncDirectory(directory);
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
            value.spoolName === options.spoolName && value.eventId === hookEventId(body);
        });
      if (matched.length !== 1) return;
      file = matched[0]!;
      receipt = readReceipt(file);
    }
    if (route === "hook" && options.outcome !== "terminal") {
      if (!receipt.eventId || options.acceptedEventId?.toLowerCase() !== receipt.eventId ||
        !options.ledger?.prepare("select 1 from buffered_events where id = ? limit 1").get(receipt.eventId)) return;
    }
    if (options.outcome === "terminal") {
      // Record the exact receipt instance before removing its hold. A crash
      // after this fsync is repaired by reconciliation on the next startup.
      const descriptor = fs.openSync(path.join(home, TERMINAL), "a", 0o600);
      try {
        fs.writeFileSync(descriptor, `${JSON.stringify({ version: 1,
          receipt: path.basename(file), at: receipt.at, route, eventId: receipt.eventId ?? null,
          outcome: "terminal" })}\n`);
        fs.fsyncSync(descriptor);
      } finally { fs.closeSync(descriptor); }
      fsyncDirectory(home);
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

/** Only the exact ledger key may settle a hook refusal that an older drain
 * accepted. Missing files alone never do. An old/versionless receipt remains
 * an explicit unknown after the missing-retry threshold. */
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
      const canonical = path.join(home, "work-ledger.sqlite");
      const fallback = path.join(home, "ledger.sqlite");
      const selected = ledgerPath ?? (fs.existsSync(canonical) ? canonical : fallback);
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
            accepted = Boolean(db.prepare("select 1 from buffered_events where id = ? limit 1").get(receipt.eventId));
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
  writeMarker(home, { version: 1, at: new Date().toISOString(), pid: process.pid });
}

export function readMaintenanceRebuildPause(home: string): PauseMarker | null {
  try {
    const file = path.join(home, MARKER);
    const stat = fs.lstatSync(file);
    if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("maintenance_pause_marker_invalid");
    const value = JSON.parse(fs.readFileSync(file, "utf8")) as PauseMarker;
    if (value.version !== 1 || !Number.isFinite(Date.parse(value.at)) ||
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
