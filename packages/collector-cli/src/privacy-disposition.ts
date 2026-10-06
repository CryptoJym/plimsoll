import type Database from "better-sqlite3";

import { registerRetentionDeliveryId } from "./delivery-id";

export type TerminalPrivacyReason =
  | "local_evidence_quarantined"
  | "local_privacy_violation";

const TERMINAL_REASONS_SQL =
  "'local_evidence_quarantined','local_privacy_violation'";

function safeAlias(alias: string) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) {
    throw new Error("Privacy eligibility requires a simple SQL alias.");
  }
  return alias;
}

/** A NULL-lineage receipt is a candidate, never proof of ownership. */
export function legacyNullLineageReceiptMatchSql(rawAlias: string, receiptAlias: string) {
  const raw = safeAlias(rawAlias);
  const receipt = safeAlias(receiptAlias);
  return `(${receipt}.raw_rowid is null and ${receipt}.raw_id is null
    and ${receipt}.raw_created_at is null and ${receipt}.raw_generation is null
    and ${receipt}.delivery_id = retention_delivery_id(${raw}.id))`;
}

function tableExists(db: Database.Database, table: string) {
  return Boolean(
    db.prepare(
      `select 1 as present from sqlite_master
       where type = 'table' and name = ? limit 1`,
    ).get(table),
  );
}

function columns(db: Database.Database, table: string) {
  if (!tableExists(db, table)) return new Set<string>();
  return new Set(
    (db.pragma(`table_info(${table})`) as Array<{ name: string }>).map(
      (column) => column.name,
    ),
  );
}

const eligibilitySqlCache = new WeakMap<Database.Database, {
  schemaVersion: number;
  predicates: Map<string, string>;
}>();

/**
 * One authoritative event-eligibility predicate for every local/read/export
 * lane. Schema checks are O(1) control reads; row evaluation uses indexed
 * receipt and outbox raw lineage where available.
 */
export function terminalPrivacyEligibilitySql(
  db: Database.Database,
  rawAlias = "buffered_events",
  options: { includeUnboundLegacyReceipts?: boolean; includeUsageDuplicates?: boolean } = {},
) {
  const alias = safeAlias(rawAlias);
  const includeUnbound = options.includeUnboundLegacyReceipts !== false;
  // A maintenance turn asks for the same predicate many times. The SQL only
  // depends on schema shape, so retain it until SQLite's schema cookie moves.
  const schemaVersion = db.pragma("schema_version", { simple: true }) as number;
  let cache = eligibilitySqlCache.get(db);
  if (!cache || cache.schemaVersion !== schemaVersion) {
    cache = { schemaVersion, predicates: new Map() };
    eligibilitySqlCache.set(db, cache);
  }
  const cacheKey = `${alias}:${includeUnbound ? 1 : 0}:${options.includeUsageDuplicates ? 1 : 0}`;
  const cached = cache.predicates.get(cacheKey);
  if (cached) return cached;
  const rawColumns = columns(db, "buffered_events");
  const terms: string[] = [];
  if (rawColumns.has("data_mode")) terms.push(`${alias}.data_mode <> 'evidence'`);
  if (rawColumns.has("privacy_disposition")) {
    terms.push(`${alias}.privacy_disposition is null`);
  }
  // A Codex response span retained as evidence for an SSE usage event is not
  // a second accounting event. This one predicate feeds projections, session
  // summaries, local lists, and the legacy upload path. Native contradiction
  // checks explicitly retain duplicates as facts; that never admits them to
  // a second financial delivery.
  if (!options.includeUsageDuplicates && rawColumns.has("usage_duplicate_reason")) {
    terms.push(`${alias}.usage_duplicate_reason is null`);
  }
  if (rawColumns.has("privacy_generation")) {
    // Rows created before the stable-lineage upgrade remain local until the
    // bounded raw migration assigns their one-time generation.
    terms.push(`${alias}.privacy_generation is not null`);
  }

  if (tableExists(db, "upload_receipts")) {
    const receiptColumns = columns(db, "upload_receipts");
    const receiptLineage = ["raw_rowid", "raw_id", "raw_created_at", "raw_generation"]
      .every((column) => receiptColumns.has(column)) && rawColumns.has("privacy_generation");
    if (receiptLineage && includeUnbound) registerRetentionDeliveryId(db);
    const dead = receiptColumns.has("terminal_state")
      ? "and privacy_receipt.terminal_state = 'dead'" : "";
    if (receiptLineage) {
      // Separate indexed probes: an OR would scan every dead receipt for each
      // raw row on a large upgraded ledger.
      terms.push(`not exists (select 1 from upload_receipts privacy_receipt
        where privacy_receipt.raw_rowid = ${alias}.rowid
          and privacy_receipt.raw_id = ${alias}.id
          and privacy_receipt.raw_created_at = ${alias}.created_at
          and privacy_receipt.raw_generation is ${alias}.privacy_generation
          ${dead} and privacy_receipt.reason in (${TERMINAL_REASONS_SQL}))`);
      if (includeUnbound) terms.push(`(case when exists (
        select 1 from upload_receipts unbound_privacy_receipt
        where unbound_privacy_receipt.raw_rowid is null
          and unbound_privacy_receipt.raw_id is null
          and unbound_privacy_receipt.raw_created_at is null
          and unbound_privacy_receipt.raw_generation is null
          ${receiptColumns.has("terminal_state")
            ? "and unbound_privacy_receipt.terminal_state = 'dead'" : ""}
          and unbound_privacy_receipt.reason in (${TERMINAL_REASONS_SQL})
      ) then not exists (select 1 from upload_receipts privacy_receipt
        where ${legacyNullLineageReceiptMatchSql(alias, "privacy_receipt")}
          ${dead} and privacy_receipt.reason in (${TERMINAL_REASONS_SQL}))
        else 1 end)`);
    } else {
      terms.push(`not exists (select 1 from upload_receipts privacy_receipt
        where privacy_receipt.delivery_id = ${alias}.id
          ${dead} and privacy_receipt.reason in (${TERMINAL_REASONS_SQL}))`);
    }
  }

  const predicate = terms.length > 0 ? `(${terms.join(" and ")})` : "1 = 1";
  cache.predicates.set(cacheKey, predicate);
  return predicate;
}

/** First terminal privacy disposition wins and cannot be cleared. */
export function markRawPrivacyDisposition(
  db: Database.Database,
  rawRowid: number,
  reason: TerminalPrivacyReason,
  terminalAt: string,
) {
  return db.prepare(
    `update buffered_events set
       privacy_disposition = coalesce(privacy_disposition, @reason),
       privacy_disposed_at = coalesce(privacy_disposed_at, @terminalAt)
     where rowid = @rawRowid`,
  ).run({ rawRowid, reason, terminalAt }).changes;
}
