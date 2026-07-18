import type Database from "better-sqlite3";

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

/**
 * One authoritative event-eligibility predicate for every local/read/export
 * lane. Schema checks are O(1) control reads; row evaluation uses the raw id,
 * receipt primary key, and the indexed outbox raw_rowid linkage.
 */
export function terminalPrivacyEligibilitySql(
  db: Database.Database,
  rawAlias = "buffered_events",
) {
  const alias = safeAlias(rawAlias);
  const rawColumns = columns(db, "buffered_events");
  const terms: string[] = [];
  if (rawColumns.has("data_mode")) terms.push(`${alias}.data_mode <> 'evidence'`);
  if (rawColumns.has("privacy_disposition")) {
    terms.push(`${alias}.privacy_disposition is null`);
  }
  if (rawColumns.has("privacy_generation")) {
    // Rows created before the stable-lineage upgrade remain local until the
    // bounded raw migration assigns their one-time generation.
    terms.push(`${alias}.privacy_generation is not null`);
  }

  if (tableExists(db, "upload_receipts")) {
    terms.push(
      `not exists (
         select 1 from upload_receipts privacy_receipt
         where privacy_receipt.delivery_id = ${alias}.id
           and privacy_receipt.reason in (${TERMINAL_REASONS_SQL})
       )`,
    );
  }

  const outboxColumns = columns(db, "upload_outbox");
  if (
    rawColumns.has("privacy_generation") &&
    outboxColumns.has("raw_id") &&
    outboxColumns.has("raw_created_at") &&
    outboxColumns.has("raw_generation")
  ) {
    terms.push(
      `not exists (
         select 1 from upload_outbox privacy_outbox
         where privacy_outbox.raw_rowid = ${alias}.rowid
           and (
             privacy_outbox.raw_id is null or
             privacy_outbox.raw_created_at is null or
             privacy_outbox.raw_generation is null or
             ${alias}.privacy_generation is null or
             privacy_outbox.raw_id is not ${alias}.id or
             privacy_outbox.raw_created_at is not ${alias}.created_at or
             privacy_outbox.raw_generation is not ${alias}.privacy_generation
           )
       )`,
    );
  }
  return terms.length > 0 ? `(${terms.join(" and ")})` : "1 = 1";
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
