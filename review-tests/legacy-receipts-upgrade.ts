import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";

import Database from "better-sqlite3";

import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { readLedgerOffThread } from "../packages/collector-cli/src/session-sync";
import { collisionSafeDeliveryId, ensureUuidEventId } from "../packages/collector-cli/src/upload-history";

// Recreate the exact 0.7.44 upload_receipts columns while retaining the
// collector's other 0.7.44-compatible tables. The old table had no raw lineage.
const oldReceiptTable = `create table upload_receipts (
  delivery_id text primary key,
  terminal_state text not null check (terminal_state in ('acknowledged','dead')),
  reason text not null,
  status_class text not null,
  attempt_count integer not null,
  created_at text not null,
  terminal_at text not null
)`;

async function main() {
const root = fs.mkdtempSync(path.join(os.tmpdir(), "pr417-legacy-receipts-"));
const ledgerPath = path.join(root, "ledger.sqlite");
const oldAt = "2025-01-01T00:00:00.000Z";
const terminalAt = "2025-01-02T00:00:00.000Z";
const now = new Date("2026-09-28T00:00:00.000Z");
const workspaceId = "legacy-receipt-upgrade";
const deviceId = "legacy-device";
const ids = {
  unparseable: "00000000-0000-4000-8000-000000002201",
  invalid: "00000000-0000-4000-8000-000000002202",
  oversize: "00000000-0000-4000-8000-000000002203",
  unacknowledged: "00000000-0000-4000-8000-000000002204",
  privacy: "00000000-0000-4000-8000-000000002205",
  pending: "00000000-0000-4000-8000-000000002206",
  collisionLegacy: "legacy-upgrade-receipt-collision",
};
const collisionUuid = ensureUuidEventId(ids.collisionLegacy).id;
const generation = (n: number) => `00000000-0000-4000-8000-${String(n).padStart(12, "0")}`;
const validPayload = (id: string) => JSON.stringify({
  id, sessionId: id, source: "codex", eventType: "assistant_response",
  dataMode: "metadata", observedAt: oldAt, actionClass: "other",
  inputTokens: 1, outputTokens: 1,
});

try {
  const seed = new LocalEventBuffer(ledgerPath, {
    workspaceId, deviceId, delivery: { enabled: false },
  });
  try {
    const insert = seed.database.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id,privacy_generation,uploaded_at)
      values (?,?,?,'metadata',?,?,?,?,?,?,?)`);
    const add = (id: string, n: number, payload = validPayload(id),
      source = "codex", eventType = "assistant_response", uploadedAt: string | null = null) =>
      insert.run(id, source, eventType, oldAt, payload, oldAt,
        workspaceId, deviceId, generation(n), uploadedAt);
    add(ids.unparseable, 2201, "{");
    add(ids.invalid, 2202, "{}", "unsupported_source", "unsupported_kind");
    add(ids.oversize, 2203, validPayload(ids.oversize) + " ".repeat(300_000));
    add(ids.unacknowledged, 2204, validPayload(ids.unacknowledged),
      "codex", "assistant_response", terminalAt);
    add(ids.privacy, 2205);
    add(ids.pending, 2206);
    add(ids.collisionLegacy, 2207);
    add(collisionUuid, 2208, "{}", "unsupported_source", "unsupported_kind");
    for (let n = 0; n < 40; n++) {
      const id = `00000000-0000-4000-8000-${String(2300 + n).padStart(12, "0")}`;
      add(id, 2300 + n, validPayload(id), "codex", "assistant_response", terminalAt);
    }
  } finally {
    seed.close();
  }

  const old = new Database(ledgerPath);
  try {
    old.exec(`drop table upload_receipts; ${oldReceiptTable};
      create index idx_upload_receipts_state on upload_receipts (terminal_state);`);
    const insert = old.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead',?,?,0,?,?)`);
    for (const [id, reason, statusClass] of [
      [ids.unparseable, "local_payload_unparseable", "local"],
      [ids.invalid, "local_schema_invalid", "local"],
      [ids.oversize, "local_item_oversize", "local"],
      [ids.unacknowledged, "remote_validation_rejected", "remote_validation"],
      [ids.privacy, "local_privacy_violation", "local"],
      [collisionUuid, "local_schema_invalid", "local"],
    ] as const) insert.run(ensureUuidEventId(id).id, reason, statusClass, oldAt, terminalAt);
    old.prepare(`update upload_control set privacy_migration_version=1,
      migration_cursor_rowid=(select max(rowid) from buffered_events),migration_complete=1
      where singleton=1`).run();
    const columns = (old.pragma("table_info(upload_receipts)") as Array<{ name: string }>).map((r) => r.name);
    assert.deepEqual(columns, ["delivery_id", "terminal_state", "reason", "status_class",
      "attempt_count", "created_at", "terminal_at"]);
    console.log(JSON.stringify({ phase: "old_0_7_44_shape", receipts: 6,
      privacyMigrationVersion: 1, migrationComplete: 1, rawRows: 48 }));
  } finally {
    old.close();
  }

  const openUpgraded = () => new LocalEventBuffer(ledgerPath, {
    workspaceId, deviceId, delivery: { enabled: true },
  });
  let upgraded = openUpgraded();
  try {
    let db = upgraded.database;
    const rawDeleteTrigger = db.prepare(`select sql from sqlite_master
      where type='trigger' and name='trg_dashboard_raw_delete'`).get() as { sql: string };
    assert.doesNotMatch(rawDeleteTrigger.sql, /retention_delivery_id\(/,
      "a persisted raw-delete trigger must work on independent SQLite connections");
    const independentWriter = new Database(ledgerPath);
    try {
      assert.equal(independentWriter.prepare("delete from buffered_events where id=?")
        .run("missing-upgrade-proof-row").changes, 0);
    } finally {
      independentWriter.close();
    }
    const prebindPrivacySql = terminalPrivacyEligibilitySql(db, "buffered_events");
    assert.equal(Boolean(db.prepare(`select 1 from buffered_events
      where id=? and ${prebindPrivacySql}`).get(ids.privacy)), false,
    "unbound old privacy receipt must exclude reads while repair is pending");
    const workerVisible = await readLedgerOffThread(db, [{
      sql: `select id from buffered_events e where id=@id and
        ${terminalPrivacyEligibilitySql(db, "e")}`,
      params: { id: ids.privacy },
    }]);
    assert.equal(workerVisible.length, 0,
      "session read worker must exclude an unbound old privacy receipt");
    const exportReader = new Database(ledgerPath, { readonly: true });
    try {
      const exportEligible = terminalPrivacyEligibilitySql(exportReader, "buffered_events");
      assert.equal(Boolean(exportReader.prepare(`select 1 from buffered_events
        where id=? and ${exportEligible}`).get(ids.privacy)), false,
      "independent export reader must exclude the present privacy-rejected raw");
    } finally {
      exportReader.close();
    }
    const holdSql = (upgraded as unknown as { rawRetentionUploadHoldSql: () => string })
      .rawRetentionUploadHoldSql();
    assert.equal((db.prepare(`select case when ${holdSql} then 1 else 0 end as held
      from buffered_events e where e.id=?`).get(ids.unacknowledged) as { held: number }).held, 1,
    "unbound old remote rejection must keep uploaded_at raw held");
    const backfillFor = () => upgraded.delivery as typeof upgraded.delivery & {
      backfillLegacyReceiptLineage?: (options: { maxRows: number; maxWriterMs: number }) =>
        { complete: boolean; visited: number; bound: number };
    };
    let slices = 0;
    if (backfillFor().backfillLegacyReceiptLineage) {
      const first = backfillFor().backfillLegacyReceiptLineage!({ maxRows: 2, maxWriterMs: 100 });
      assert.equal(first.complete, false, "the first bounded slice must leave work to resume");
      const cursor = (db.prepare(`select scan_cursor_rowid as n
        from upload_receipt_lineage_backfill where singleton=1`).get() as { n: number }).n;
      upgraded.close();
      upgraded = openUpgraded();
      db = upgraded.database;
      assert.ok((db.prepare(`select scan_cursor_rowid as n from
        upload_receipt_lineage_backfill where singleton=1`).get() as { n: number }).n >= cursor,
        "the receipt cursor must survive a reopen");
      slices = 1;
      for (; slices < 64; slices++) {
        const progress = backfillFor().backfillLegacyReceiptLineage!({ maxRows: 2, maxWriterMs: 100 });
        assert.ok(progress.visited <= 2 && progress.bound <= 2, "lineage repair must be bounded");
        if (progress.complete) break;
      }
      assert.ok(slices < 64, "legacy receipt lineage did not finish bounded repair");
    }
    const before = upgraded.retentionStatus(90, now).states.heldForUpload;
    const collisionOwners = db.prepare(`select generation,delivery_id as deliveryId,
      raw_rowid as rawRowid,raw_id as rawId from upload_receipt_lineage_candidates
      where delivery_id=? order by generation,raw_rowid`).all(collisionUuid);
    const collisionReceipt = db.prepare(`select raw_rowid as rawRowid,raw_id as rawId
      from upload_receipts where delivery_id=?`).get(collisionUuid);
    for (const id of [ids.unparseable, ids.invalid, ids.oversize, ids.unacknowledged, ids.privacy]) {
      const linked = db.prepare(`select raw_id as rawId from upload_receipts
        where delivery_id=?`).get(id) as { rawId: string | null };
      assert.equal(linked.rawId, id, "one-owner old receipt must bind to its raw");
    }
    assert.throws(() => db.prepare(`update upload_receipts set raw_id='rewritten'
      where delivery_id=?`).run(ids.unparseable), /upload_receipt_lineage_is_immutable/);
    const receiptIds = db.prepare(`select delivery_id as id,reason from upload_receipts
      order by delivery_id`).all();
    console.log(JSON.stringify({ phase: "collision_owner_audit", collisionUuid,
      collisionOwners, collisionReceipt, receiptIds }));
    const privacySql = terminalPrivacyEligibilitySql(db, "buffered_events");
    const privacyVisible = Boolean(db.prepare(`select 1 from buffered_events
      where id=? and ${privacySql}`).get(ids.privacy));
    const expired = upgraded.prune(90, { maxRows: 100, now });
    const remains = (id: string) => Boolean(db.prepare("select 1 from buffered_events where id=?").get(id));
    const l1 = [ids.unparseable, ids.invalid, ids.oversize].map((id) => remains(id));
    const l2 = remains(ids.unacknowledged);
    const l3 = remains(ids.privacy);
    const f1 = remains(ids.collisionLegacy);
    const after = upgraded.retentionStatus(90, now).states.heldForUpload;
    const observations = { phase: "after_upgrade", slices, heldBefore: before,
      expired: expired.events, l1Remaining: l1, l2Remaining: l2,
      l3RawRemaining: l3, l3Visible: privacyVisible, f1LegacyRemaining: f1,
      heldAfter: after };
    console.log(JSON.stringify(observations));
    assert.deepEqual(l1, [false, false, false], "L1 local-dead raw must expire");
    assert.equal(before, 4, "L1 must not inflate heldForUpload after repair");
    assert.equal(l2, true, "L2 uploaded-at raw with non-acknowledgment must remain held");
    assert.equal(privacyVisible, false, "L3 terminal privacy receipt must exclude raw");
    assert.equal(f1, true, "F1 unrelated legacy raw must stay held");
    assert.equal(after, 4);
    assert.equal(upgraded.list(20).some((row) => row.id === ids.privacy), false);
    assert.equal(upgraded.listUnuploaded({ maxRows: 20 }).some((row) => row.id === ids.privacy), false);
    const collisionRepair = upgraded.delivery.migrateLegacy({ maxRows: 100 });
    const collisionDelivery = db.prepare(`select delivery_id as id from upload_outbox
      where raw_id=?`).get(ids.collisionLegacy) as { id: string } | undefined;
    assert.equal(collisionDelivery?.id,
      collisionSafeDeliveryId(ids.collisionLegacy, 1),
      "a completed old raw cursor must reopen and choose a collision-safe ID");
    console.log(JSON.stringify({ phase: "collision_safe_repair",
      visited: collisionRepair.visited, deliveryId: collisionDelivery?.id }));

    // A raw appended between the completed scan and the bind can take the
    // same literal UUID as a legacy raw's derived delivery ID. The bind must
    // scan that tail before claiming the old receipt has one owner.
    const lateLegacyId = "late-backfill-collision";
    const lateDeliveryId = ensureUuidEventId(lateLegacyId).id;
    const addLateRaw = db.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id,privacy_generation)
      values (?,'codex','assistant_response','metadata',?,?,?,?,?,?)`);
    addLateRaw.run(lateLegacyId, oldAt, validPayload(lateLegacyId), oldAt,
      workspaceId, deviceId, generation(3001));
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_schema_invalid','local',0,?,?)`)
      .run(lateDeliveryId, oldAt, terminalAt);
    const scanRows = (db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n;
    const scan = backfillFor().backfillLegacyReceiptLineage!({ maxRows: scanRows, maxWriterMs: 500 });
    assert.equal(scan.visited, scanRows);
    assert.equal((db.prepare(`select phase from upload_receipt_lineage_backfill
      where singleton=1`).get() as { phase: string }).phase, "bind");
    addLateRaw.run(lateDeliveryId, oldAt, validPayload(lateDeliveryId), oldAt,
      workspaceId, deviceId, generation(3002));
    const restarted = backfillFor().backfillLegacyReceiptLineage!({ maxRows: 10, maxWriterMs: 500 });
    assert.equal(restarted.complete, false, "a new raw must invalidate bind uniqueness");
    let completed = false;
    for (let attempt = 0; attempt < 10; attempt++) {
      completed = backfillFor().backfillLegacyReceiptLineage!({ maxRows: 10, maxWriterMs: 500 }).complete;
      if (completed) break;
    }
    assert.equal(completed, true);
    const lateReceipt = db.prepare(`select raw_rowid as rawRowid
      from upload_receipts where delivery_id=?`).get(lateDeliveryId) as { rawRowid: number | null };
    assert.equal(lateReceipt.rawRowid, null, "two current owners make the old receipt ambiguous");
    console.log(JSON.stringify({ phase: "late_collision_fence", scanRows,
      receiptBound: lateReceipt.rawRowid !== null, resumed: completed }));

    // Deleting the highest rowid permits SQLite to reuse that rowid. A
    // watermark alone cannot notice the new literal-UUID competitor.
    const reusedLegacyId = "reused-rowid-receipt-collision";
    const reusedDeliveryId = ensureUuidEventId(reusedLegacyId).id;
    addLateRaw.run(reusedLegacyId, oldAt, validPayload(reusedLegacyId), oldAt,
      workspaceId, deviceId, generation(3003));
    const filler = addLateRaw.run("receipt-backfill-tail-filler", oldAt,
      validPayload("receipt-backfill-tail-filler"), oldAt,
      workspaceId, deviceId, generation(3004));
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_schema_invalid','local',0,?,?)`)
      .run(reusedDeliveryId, oldAt, terminalAt);
    const reuseScanRows = (db.prepare("select count(*) as n from buffered_events").get() as
      { n: number }).n;
    assert.equal(backfillFor().backfillLegacyReceiptLineage!({
      maxRows: reuseScanRows, maxWriterMs: 500,
    }).visited, reuseScanRows);
    assert.equal((db.prepare(`select phase from upload_receipt_lineage_backfill
      where singleton=1`).get() as { phase: string }).phase, "bind");
    const tailRowid = Number(filler.lastInsertRowid);
    db.prepare("delete from buffered_events where rowid=?").run(tailRowid);
    const replacement = addLateRaw.run(reusedDeliveryId, oldAt,
      validPayload(reusedDeliveryId), oldAt, workspaceId, deviceId, generation(3005));
    assert.equal(Number(replacement.lastInsertRowid), tailRowid);
    assert.equal(backfillFor().backfillLegacyReceiptLineage!({
      maxRows: 10, maxWriterMs: 500,
    }).complete, true);
    const reusedReceipt = db.prepare(`select raw_rowid as rawRowid from upload_receipts
      where delivery_id=?`).get(reusedDeliveryId) as { rawRowid: number | null };
    assert.equal(reusedReceipt.rawRowid, null,
      "a reused rowid must not hide a new literal-UUID owner");
    console.log(JSON.stringify({ phase: "reused_rowid_collision_fence", tailRowid,
      receiptBound: reusedReceipt.rawRowid !== null }));

    const recycledId = "00000000-0000-4000-8000-000000003006";
    const recycled = addLateRaw.run(recycledId, oldAt, validPayload(recycledId), oldAt,
      workspaceId, deviceId, generation(3006));
    const recycledRowid = Number(recycled.lastInsertRowid);
    db.prepare(`insert into raw_retention_receipts
      (event_id,raw_rowid,raw_created_at,raw_generation,expired_at,reason)
      values (?,?,?,?,?,'retention_window_elapsed')`)
      .run(recycledId, recycledRowid, oldAt, generation(9999), terminalAt);
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_schema_invalid','local',0,?,?)`)
      .run(recycledId, oldAt, terminalAt);
    assert.equal(backfillFor().backfillLegacyReceiptLineage!({
      maxRows: 100, maxWriterMs: 500,
    }).complete, true);
    const recycledReceipt = db.prepare(`select raw_rowid as rawRowid from upload_receipts
      where delivery_id=?`).get(recycledId) as { rawRowid: number | null };
    assert.equal(recycledReceipt.rawRowid, null,
      "same rowid and timestamp with a different generation is not lineage");
    console.log(JSON.stringify({ phase: "recycled_generation_fence", recycledRowid,
      receiptBound: recycledReceipt.rawRowid !== null }));

    // The dashboard's persisted raw-delete trigger runs on arbitrary SQLite
    // connections. Its existing live delivery guard must still exclude an old
    // terminal privacy receipt when the raw id was a non-UUID legacy id.
    const liveId = "legacy-live-privacy-receipt";
    const liveDeliveryId = ensureUuidEventId(liveId).id;
    const liveGeneration = generation(3007);
    const liveRow = db.prepare(`insert into buffered_events
      (id,source,event_type,data_mode,observed_at,payload_json,created_at,
       workspace_id,device_id,privacy_generation)
      values (?,'codex','usage_live','metadata',?,?,?,?,?,?)`)
      .run(liveId, oldAt, validPayload(liveId), oldAt,
        workspaceId, deviceId, liveGeneration);
    const liveRowid = Number(liveRow.lastInsertRowid);
    db.prepare(`insert into upload_receipts
      (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at)
      values (?,'dead','local_privacy_violation','local',0,?,?)`)
      .run(liveDeliveryId, oldAt, terminalAt);
    db.prepare(`insert into raw_retention_receipts
      (event_id,raw_rowid,raw_created_at,raw_generation,expired_at,reason)
      values (?,?,?,?,?,'retention_window_elapsed')`)
      .run(liveId, liveRowid, oldAt, liveGeneration, terminalAt);
    db.prepare(`insert into dashboard_event_facts
      (projection_id,raw_rowid,source,event_type,observed_at,raw_generation,
       workspace_id,installation_epoch_id,observed_at_ms,
       live_usage_json,live_usage_fact_json,live_delivery_id)
      values (?,?,'codex','usage_live',?,?,?,?,?,?,?,?)`)
      .run(liveDeliveryId, liveRowid, oldAt, liveGeneration,
        workspaceId, "legacy-live-epoch", Date.parse(oldAt),
        JSON.stringify({ intervalStart: oldAt }), "{}", liveDeliveryId);
    const independentDelete = new Database(ledgerPath);
    try {
      assert.equal(independentDelete.prepare("delete from buffered_events where rowid=?")
        .run(liveRowid).changes, 1);
    } finally {
      independentDelete.close();
    }
    assert.equal(Boolean(db.prepare(`select 1 from dashboard_live_usage_retained
      where event_id=?`).get(liveId)), false,
    "an old terminal privacy receipt must not become a retained dashboard fact");
    console.log(JSON.stringify({ phase: "portable_raw_delete_privacy_guard",
      liveDeliveryId, retained: false }));
  } finally {
    upgraded.close();
  }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
}

main().catch((error) => { console.error(error); process.exitCode = 1; });
