import fs from "node:fs";
import path from "node:path";
import { createHash } from "node:crypto";
import type Database from "better-sqlite3";
import type { AiInteractionEvent } from "../../shared/src/index";
import { HOOK_AUTHORITY_CONTRACT } from "./hook-authority";
import { isUuid } from "./normalizer";
import { hookBodyDigestCandidates, hookReceiptFileName } from "./maintenance-hook-fingerprint";

export const HOOK_ACK_LOOKUP_SQL = `select admitted_event_id, outcome
  from maintenance_rebuild_hook_admissions where receipt_id = ?`;
export const HOOK_ROW_LOOKUP_SQL = `select e.rowid, e.id, e.source, e.event_type, e.session_id,
    json_extract(e.payload_json, '$.tenantId') as tenant_id,
    e.maintenance_hook_body_digest as body_digest, o.seq as admission_seq
  from buffered_events e left join maintenance_rebuild_event_order o on o.event_id = e.id
  where e.id in (?, ?, ?)`;
export const HOOK_ROW_LEGACY_LOOKUP_SQL = `select rowid, id, source, event_type, session_id,
    json_extract(payload_json, '$.tenantId') as tenant_id,
    null as body_digest, null as admission_seq
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
  const columns = new Set((db.pragma("table_info(buffered_events)") as Array<{ name: string }>)
    .map((column) => column.name));
  if (!columns.has("maintenance_hook_body_digest")) {
    db.exec(`alter table buffered_events add column maintenance_hook_body_digest text
      check (maintenance_hook_body_digest is null or
        (length(maintenance_hook_body_digest) = 64 and
         maintenance_hook_body_digest not glob '*[^0-9a-f]*'))`);
  }
  db.exec(`create trigger if not exists trg_events_maintenance_hook_body_digest_immutable
    before update of maintenance_hook_body_digest on buffered_events
    when old.maintenance_hook_body_digest is not null and
      new.maintenance_hook_body_digest is not old.maintenance_hook_body_digest
    begin select raise(abort, 'maintenance_hook_body_digest_is_immutable'); end`);
  db.exec(`create table if not exists maintenance_rebuild_hook_admissions (
    receipt_id text not null,
    admitted_event_id text not null,
    admitted_rowid integer not null,
    outcome text not null check (outcome in ('accepted', 'mismatch')),
    primary key (receipt_id, admitted_event_id)
  )`);
  const admissionColumns = new Set((db.pragma("table_info(maintenance_rebuild_hook_admissions)") as
    Array<{ name: string }>).map((column) => column.name));
  if (!admissionColumns.has("receipt_name")) {
    db.exec("alter table maintenance_rebuild_hook_admissions add column receipt_name text");
  }
  db.exec(`create index if not exists idx_maintenance_hook_admission_event
    on maintenance_rebuild_hook_admissions(admitted_event_id)`);
  ensureMaintenanceEventOrderSchema(db);
}

/** Publish the side table and all triggers as one schema transition. A
 * downgraded writer can insert only before BEGIN IMMEDIATE or after COMMIT;
 * it can never see a committed table without its insert trigger. This also
 * completes an interrupted installation made by an earlier build. */
export function ensureMaintenanceEventOrderSchema(db: Database.Database) {
  const installed = db.prepare(`select count(*) as n from sqlite_master
    where (type = 'table' and name = 'maintenance_rebuild_event_order')
       or (type = 'trigger' and name in (
         'trg_maintenance_rebuild_event_order_insert',
         'trg_maintenance_rebuild_event_order_delete',
         'trg_maintenance_rebuild_event_order_rekey'))`).get() as { n: number };
  if (installed.n === 4) return;
  // AUTOINCREMENT stores its last issued sequence in sqlite_sequence even
  // after retention deletes the top event. These triggers also run when
  // 0.7.44 writes through the same ledger after a downgrade.
  db.transaction(() => {
    db.exec(`create table if not exists maintenance_rebuild_event_order (
      seq integer primary key autoincrement,
      event_id text not null unique
    )`);
    db.exec(`create trigger if not exists trg_maintenance_rebuild_event_order_insert
      after insert on buffered_events begin
        insert into maintenance_rebuild_event_order(event_id) values (new.id);
      end`);
    db.exec(`create trigger if not exists trg_maintenance_rebuild_event_order_delete
      after delete on buffered_events begin
        delete from maintenance_rebuild_event_order where event_id = old.id;
      end`);
    db.exec(`create trigger if not exists trg_maintenance_rebuild_event_order_rekey
      after update of id on buffered_events
      when new.id is not old.id begin
        update maintenance_rebuild_event_order set event_id = new.id where event_id = old.id;
      end`);
  }).immediate();
}

/** Never use max(rowid): SQLite can reuse it after the top row is pruned. */
export function ledgerAdmissionSequence(db: Database.Database): number | null {
  const exists = db.prepare(`select 1 from sqlite_master where type = 'table'
    and name = 'maintenance_rebuild_event_order'`).get();
  if (!exists) return null;
  const row = db.prepare(`select seq from sqlite_sequence
    where name = 'maintenance_rebuild_event_order'`).get() as { seq: number } | undefined;
  const sequence = row?.seq ?? 0;
  if (!Number.isSafeInteger(sequence) || sequence < 0) throw new Error("maintenance_admission_sequence_unsafe");
  return sequence;
}

type PendingHookReceipt = {
  version: 7; route: "hook"; receiptId: string; eventId: string;
  source: string; kind: string; sessionId: string | null; tenantId: string;
  bodyDigest: string; ledgerAdmissionSequence: number | null;
};

/** Called inside the same SQLite transaction as the buffered_events insert.
 * The one direct receipt probe stays constant-time regardless of the pending
 * receipt count. The fingerprint is of the original caller body: a retry
 * resends it unchanged, even if admission clamps time or later enriches JSON. */
export function recordMaintenanceHookAdmission(db: Database.Database,
  rawPayload: unknown, event: AiInteractionEvent, inserted = true) {
  if (db.name === ":memory:") return;
  const directory = path.join(path.dirname(db.name), "maintenance-rebuild-refusals");
  const candidates = hookBodyDigestCandidates(rawPayload);
  const admittedRow = db.prepare(`select rowid,
      maintenance_hook_body_digest as body_digest,
      json_extract(payload_json, '$.tenantId') as tenant_id,
      source, event_type, session_id
    from buffered_events where id = ?`).get(event.id) as {
      rowid: number; body_digest: string | null; tenant_id: string | null;
      source: string; event_type: string; session_id: string | null
    } | undefined;
  if (!admittedRow) throw new Error("maintenance_admission_row_missing");
  const insert = db.prepare(`insert into maintenance_rebuild_hook_admissions
    (receipt_id, admitted_event_id, admitted_rowid, outcome, receipt_name) values (?, ?, ?, 'accepted', ?)
    on conflict (receipt_id, admitted_event_id) do update set
      admitted_rowid = excluded.admitted_rowid,
      outcome = 'accepted', receipt_name = excluded.receipt_name`);
  let admittedDigest = candidates[0]!.digest;
  for (const candidate of candidates) {
    const entry = hookReceiptFileName(event.source, candidate.digest);
    const file = path.join(directory, entry);
    let receipt: PendingHookReceipt;
    try {
      const stat = fs.lstatSync(file);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) throw new Error("maintenance_refusal_unsafe");
      receipt = JSON.parse(fs.readFileSync(file, "utf8")) as PendingHookReceipt;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") continue;
      // A concurrent refusal has published its filename but has not yet
      // fsynced its JSON. Reconciliation retains that unreadable receipt;
      // admission can still commit and the indexed fallback resolves it.
      if (error instanceof SyntaxError) continue;
      throw error;
    }
    if (receipt.version !== 7 || receipt.route !== "hook" || !isUuid(receipt.receiptId) ||
      !isUuid(receipt.eventId) || receipt.source !== event.source ||
      receipt.kind !== event.eventType ||
      receipt.bodyDigest !== candidate.digest ||
      receipt.tenantId !== event.tenantId ||
      admittedRow.tenant_id !== receipt.tenantId ||
      admittedRow.source !== receipt.source ||
      admittedRow.event_type !== receipt.kind ||
      !sameHookIdentityPart(receipt.eventId, event.id) ||
      (candidate.injectedId !== null && !sameHookIdentityPart(candidate.injectedId, receipt.eventId)) ||
      !sameHookIdentityPart(receipt.sessionId, event.sessionId ?? null) ||
      !sameHookIdentityPart(receipt.sessionId, admittedRow.session_id) ||
      (!inserted && admittedRow.body_digest !== candidate.digest)) continue;
    // A digest match proves the same caller body, even if this append
    // deduplicated against a row captured before the refusal. Both the
    // acknowledgement and any newly inserted row share this transaction.
    insert.run(receipt.receiptId, event.id, admittedRow.rowid, entry);
    admittedDigest = candidate.digest;
    break;
  }
  if (inserted) db.prepare(`update buffered_events set maintenance_hook_body_digest = ?
    where id = ? and maintenance_hook_body_digest is null`).run(admittedDigest, event.id);
}

export function removeMaintenanceHookAdmission(db: Database.Database, receiptId: string) {
  db.prepare("delete from maintenance_rebuild_hook_admissions where receipt_id = ?")
    .run(receiptId);
}

/** A bounded retention page may delete an event after its refusal retired.
 * Keep an acknowledgement while the private receipt still exists. */
export function pruneRetiredMaintenanceHookAdmissions(db: Database.Database, home: string, eventId: string) {
  const rows = db.prepare(`select receipt_id, receipt_name from maintenance_rebuild_hook_admissions
    indexed by idx_maintenance_hook_admission_event where admitted_event_id = ?`)
    .all(eventId) as Array<{ receipt_id: string; receipt_name: string | null }>;
  const remove = db.prepare("delete from maintenance_rebuild_hook_admissions where receipt_id = ?");
  for (const row of rows) {
    if (row.receipt_name && fs.existsSync(path.join(home, "maintenance-rebuild-refusals", row.receipt_name))) continue;
    if (row.receipt_name) remove.run(row.receipt_id);
  }
}
