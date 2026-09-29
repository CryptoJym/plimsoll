import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import type Database from "better-sqlite3";
import type { AiInteractionEvent } from "../../shared/src/index";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import { isUuid } from "./normalizer";

export const HOOK_ACK_LOOKUP_SQL = `select admitted_event_id, outcome
  from maintenance_rebuild_hook_admissions where receipt_id = ?`;
export const HOOK_ROW_LOOKUP_SQL = `select id, source, event_type, session_id
  from buffered_events where id in (?, ?, ?)`;

export function sameHookIdentityPart(left: string | null, right: string | null) {
  if (left === null || right === null) return left === right;
  return isUuid(left) && isUuid(right) ? left.toLowerCase() === right.toLowerCase() : left === right;
}

/** Preserve the caller's time claim before the normalizer can clamp it. */
export function originalHookTimestampDigest(payload: unknown): string | null {
  const aliases = new Set<string>([...HOOK_AUTHORITY_CONTRACT.observedAt.aliases,
    "timeUnixNano", "observedTimeUnixNano", "startTimeUnixNano"]);
  const claims: Array<[string, unknown]> = [];
  const seen = new WeakSet<object>();
  const visit = (value: unknown, location: string, depth: number) => {
    if (!value || typeof value !== "object" || depth > 32 || seen.has(value)) return;
    seen.add(value);
    if (Array.isArray(value)) {
      value.forEach((entry, index) => visit(entry, `${location}[${index}]`, depth + 1));
      return;
    }
    const record = value as Record<string, unknown>;
    // OTLP attributes put the authority name in `key` and its original scalar
    // in `value`; normalizer.ts selects these along with direct aliases.
    if (typeof record.key === "string" && aliases.has(record.key) && "value" in record) {
      claims.push([`${location}.attribute:${record.key}`, record.value]);
    }
    for (const key of Object.keys(record).sort()) {
      if (aliases.has(key)) claims.push([`${location}.${key}`, record[key]]);
      visit(record[key], `${location}.${key}`, depth + 1);
    }
  };
  visit(payload, "$", 0);
  return claims.length === 0 ? null :
    createHash("sha256").update(JSON.stringify(claims)).digest("hex");
}

export function ensureMaintenanceHookAdmissionSchema(db: Database.Database) {
  db.exec(`create table if not exists maintenance_rebuild_hook_admissions (
    receipt_id text not null,
    admitted_event_id text not null,
    admitted_rowid integer not null,
    outcome text not null check (outcome in ('accepted', 'mismatch')),
    primary key (receipt_id, admitted_event_id)
  )`);
}

type PendingHookReceipt = {
  version: 5; route: "hook"; receiptId: string; eventId: string;
  source: string; kind: string; sessionId: string | null;
  originalTimestampDigest: string | null;
};

/** Called inside the same SQLite transaction as the buffered_events insert.
 * The private receipt directory is read only when a rebuild left one behind.
 * A mismatched caller time is recorded too, so the older-binary fallback cannot
 * mistake that particular admission for the refused hook. */
export function recordMaintenanceHookAdmission(db: Database.Database,
  rawPayload: unknown, event: AiInteractionEvent) {
  if (db.name === ":memory:") return;
  const directory = path.join(path.dirname(db.name), "maintenance-rebuild-refusals");
  let entries: string[];
  try {
    const stat = fs.lstatSync(directory);
    if (!stat.isDirectory() || stat.isSymbolicLink()) throw new Error("maintenance_refusals_unsafe");
    entries = fs.readdirSync(directory).filter((entry) => /^[a-f0-9]{64}\.receipt$/.test(entry));
  }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return;
    throw error;
  }
  if (entries.length === 0) return;
  const rawTime = originalHookTimestampDigest(rawPayload);
  const admittedRow = db.prepare("select rowid from buffered_events where id = ?")
    .get(event.id) as { rowid: number } | undefined;
  if (!admittedRow) throw new Error("maintenance_admission_row_missing");
  const insert = db.prepare(`insert into maintenance_rebuild_hook_admissions
    (receipt_id, admitted_event_id, admitted_rowid, outcome) values (?, ?, ?, ?)
    on conflict (receipt_id, admitted_event_id) do update set
      admitted_rowid = excluded.admitted_rowid,
      outcome = case when excluded.outcome = 'accepted' then 'accepted' else outcome end`);
  for (const entry of entries) {
    const file = path.join(directory, entry);
    let receipt: PendingHookReceipt;
    try {
      const stat = fs.lstatSync(file);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) continue;
      receipt = JSON.parse(fs.readFileSync(file, "utf8")) as PendingHookReceipt;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      // A concurrent refusal has published its filename but has not yet
      // fsynced its JSON. Reconciliation retains that unreadable receipt;
      // admission can still commit and the indexed fallback resolves it.
      if (error instanceof SyntaxError) continue;
      throw error;
    }
    if (receipt.version !== 5 || receipt.route !== "hook" || !isUuid(receipt.receiptId) ||
      !isUuid(receipt.eventId) || receipt.source !== event.source ||
      receipt.kind !== event.eventType ||
      !sameHookIdentityPart(receipt.eventId, event.id) ||
      !sameHookIdentityPart(receipt.sessionId, event.sessionId ?? null)) continue;
    insert.run(receipt.receiptId, event.id, admittedRow.rowid,
      receipt.originalTimestampDigest === rawTime ? "accepted" : "mismatch");
  }
}
