/** Explain every new admission, fallback and retirement SQL on real schemas. */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { randomUUID } from "node:crypto";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { HOOK_ACK_LOOKUP_SQL, HOOK_ROW_LOOKUP_SQL, HOOK_ROW_LEGACY_LOOKUP_SQL } from
  "../packages/collector-cli/src/maintenance-hook-admission";

async function main() {
  const oldModule = path.resolve(process.env.PR424_0744_CHECKOUT ??
    path.resolve(process.cwd(), "../plimsoll-0744"), "packages/collector-cli/src/buffer.ts");
  const old = await import(pathToFileURL(oldModule).href);
  const root = fs.realpathSync(fs.mkdtempSync(path.join(process.env.TMPDIR ?? os.tmpdir(), "r7-plans-")));
  const current = new LocalEventBuffer(path.join(root, "current.sqlite"));
  const legacy = new old.LocalEventBuffer(path.join(root, "legacy.sqlite"));
  try {
  const id = randomUUID();
  const variants = [id, id.toLowerCase(), id.toUpperCase()];
  const checks = [
    { schema: "current", name: "receipt_ack_lookup", db: current.database,
      sql: HOOK_ACK_LOOKUP_SQL, args: [id] },
    { schema: "current", name: "row_fallback", db: current.database,
      sql: HOOK_ROW_LOOKUP_SQL, args: variants },
    { schema: "legacy_0744", name: "row_fallback", db: legacy.database,
      sql: HOOK_ROW_LEGACY_LOOKUP_SQL, args: variants },
    { schema: "current", name: "admission_row", db: current.database,
      sql: "select rowid from buffered_events where id=?", args: [id] },
    { schema: "current", name: "admission_digest_update", db: current.database,
      sql: `update buffered_events set maintenance_hook_body_digest=?
        where id=? and maintenance_hook_body_digest is null`, args: ["a".repeat(64), id] },
    { schema: "current", name: "ack_retention_index", db: current.database,
      sql: `select receipt_id,receipt_name from maintenance_rebuild_hook_admissions
        indexed by idx_maintenance_hook_admission_event where admitted_event_id=?`, args: [id] },
    { schema: "current", name: "ack_delete", db: current.database,
      sql: "delete from maintenance_rebuild_hook_admissions where receipt_id=?", args: [id] },
    { schema: "current", name: "high_water", db: current.database,
      sql: "select coalesce(max(rowid),0) as highWater from buffered_events", args: [] },
    { schema: "current", name: "admission_sequence_boundary", db: current.database,
      sql: "select seq from sqlite_sequence where name = 'maintenance_rebuild_event_order'", args: [] },
    { schema: "current", name: "admission_sequence_row", db: current.database,
      sql: "select seq from maintenance_rebuild_event_order where event_id = ?", args: [id] },
    { schema: "legacy_0744", name: "high_water", db: legacy.database,
      sql: "select coalesce(max(rowid),0) as highWater from buffered_events", args: [] },
  ];
  for (const check of checks) {
    const details = (check.db.prepare(`explain query plan ${check.sql}`).all(...check.args) as
      Array<{ detail: string }>).map((row) => row.detail);
    console.log(JSON.stringify({ check: "query_plan", schema: check.schema, name: check.name,
      sql: check.sql.replace(/\s+/g, " ").trim(), details }));
    assert.ok(details.length > 0);
    assert.ok(details.every((detail) => !/\bSCAN\s+buffered_events\b/i.test(detail)),
      `${check.name} scans buffered_events on ${check.schema}`);
    if (/buffered_events/i.test(check.sql)) assert.ok(details.some((detail) =>
      /\bSEARCH\s+(?:buffered_events|e)\b/i.test(detail)), `${check.name} lacks indexed search`);
    if (check.name === "admission_sequence_row") assert.ok(details.some((detail) =>
      /\bSEARCH\s+maintenance_rebuild_event_order\b/i.test(detail)),
      "admission sequence lookup is not indexed");
  }
  } finally {
    current.close();
    legacy.close();
    fs.rmSync(root, { recursive: true, force: true });
  }
}
main().catch((error) => { console.error(error); process.exitCode = 1; });
