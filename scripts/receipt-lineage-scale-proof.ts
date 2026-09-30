import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import Database from "better-sqlite3";
import { LocalEventBuffer } from "../packages/collector-cli/src/buffer";
import { terminalPrivacyEligibilitySql } from "../packages/collector-cli/src/privacy-disposition";
import { collisionSafeDeliveryId, ensureUuidEventId } from "../packages/collector-cli/src/upload-history";

const count = Number(process.env.LINEAGE_SCALE_ROWS ?? "5000000");
assert.ok(Number.isInteger(count) && count >= 10000);
const root = fs.mkdtempSync(path.join(os.tmpdir(), "plimsoll-receipt-lineage-scale-"));
const ledger = path.join(root, "ledger.sqlite");
const workspace = "scale-upgrade-workspace";
const device = "scale-upgrade-device";
const oldAt = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
const terminalAt = new Date(Date.parse(oldAt) + 24 * 60 * 60 * 1000).toISOString();
const uuid = (n: number) => "00000000-0000-4000-8000-" + String(n).padStart(12, "0");
const f1 = ensureUuidEventId("scale-f1-legacy").id;
const ambiguous = ensureUuidEventId("scale-ambiguous-legacy").id;
const rawId = (n: number) => n === 1001 ? "scale-f1-legacy" :
  n === 1002 ? f1 : n === 5005 ? "scale-ambiguous-legacy" :
  n === 5006 ? ambiguous : uuid(n);
const backlog = (n: number) => (n >= 3001 && n <= 3100) ||
  (n >= 3501 && n <= 3600);
const deadRaw = (n: number) => (n >= 1001 && n <= 2998 && n !== 1002) ||
  n === 5005 || n === 5009;
const ackId = (n: number) => backlog(n) || deadRaw(n) ||
  n === 1002 || n === 5006 ? uuid(n + 10000000) : rawId(n);
const validPayload = (id: string) => JSON.stringify({
  id, sessionId: id, source: "codex", eventType: "assistant_response",
  dataMode: "metadata", observedAt: oldAt, actionClass: "other",
  inputTokens: 1, outputTokens: 1,
});
const reason = (n: number) => n === 1001 ? "local_schema_invalid" :
  n === 1003 ? "local_payload_unparseable" :
  n === 1004 ? "local_item_oversize" :
  n === 1008 ? "remote_validation_rejected" :
  n === 1009 ? "local_privacy_violation" : "local_schema_invalid";
const diskBytes = () => fs.readdirSync(root).reduce((sum, name) =>
  sum + fs.statSync(path.join(root, name)).size, 0);

try {
  const seed = new LocalEventBuffer(ledger, {
    workspaceId: workspace, deviceId: device, delivery: { enabled: false },
  });
  seed.close();
  const db = new Database(ledger);
  db.pragma("journal_mode = OFF");
  db.pragma("synchronous = OFF");
  db.pragma("cache_size = -262144");
  const rawObjects = db.prepare("select type,name,sql from sqlite_master where tbl_name='buffered_events' and type in ('index','trigger') and sql is not null").all() as
    Array<{ type: "index" | "trigger"; name: string; sql: string }>;
  for (const item of rawObjects) db.exec("drop " + item.type + " " + item.name);
  db.exec("drop table upload_receipts");
  db.exec("create table upload_receipts (delivery_id text primary key,terminal_state text not null check (terminal_state in ('acknowledged','dead')),reason text not null,status_class text not null,attempt_count integer not null,created_at text not null,terminal_at text not null)");
  const insertRaw = db.prepare("insert into buffered_events (id,source,event_type,data_mode,observed_at,payload_json,created_at,workspace_id,device_id,privacy_generation,uploaded_at) values (?,'codex','assistant_response','metadata',?,?,?,?,?,?,?)");
  const insertAck = db.prepare("insert into upload_receipts (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at) values (?,'acknowledged','uploaded','success',1,?,?)");
  const seedStarted = performance.now();
  let timestamp = "";
  let nextTimestampGroup = -1;
  const insertRaws = db.transaction((from: number, to: number) => {
    for (let n = from; n <= to; n++) {
      const group = Math.floor(n / 10);
      if (group !== nextTimestampGroup) {
        timestamp = new Date(Date.parse(oldAt) + group * 1000).toISOString();
        nextTimestampGroup = group;
      }
      const id = rawId(n);
      insertRaw.run(id, timestamp, backlog(n) || deadRaw(n) ?
        validPayload(id) : "{}", timestamp, workspace, device, uuid(n + 20000000),
        backlog(n) || (deadRaw(n) && n !== 1008) ? null : terminalAt);
    }
  });
  const insertAcks = db.transaction((from: number, to: number) => {
    for (let n = from; n <= to; n++) {
      const at = new Date(Date.parse(oldAt) + Math.floor(n / 10) * 1000).toISOString();
      insertAck.run(ackId(n), at, terminalAt);
    }
  });
  for (let from = 1; from <= count; from += 100000) {
    const to = Math.min(count, from + 99999);
    insertRaws(from, to);
    insertAcks(from, to);
    if (to % 1000000 === 0 || to === count)
      console.log(JSON.stringify({ phase: "seed", rows: to, bytes: diskBytes(),
        seconds: (performance.now() - seedStarted) / 1000 }));
  }
  const insertDead = db.prepare("insert into upload_receipts (delivery_id,terminal_state,reason,status_class,attempt_count,created_at,terminal_at) values (?,'dead',?,?,0,?,?)");
  db.transaction(() => {
    for (let n = 1001; n <= 2998; n++) {
      if (n === 1002) continue;
      const at = new Date(Date.parse(oldAt) + Math.floor(n / 10) * 1000).toISOString();
      insertDead.run(ensureUuidEventId(rawId(n)).id, reason(n), n === 1008 ?
        "remote_validation" : "local", at, terminalAt);
    }
    for (const n of [5005, 5009]) {
      const at = new Date(Date.parse(oldAt) + Math.floor(n / 10) * 1000).toISOString();
      insertDead.run(ensureUuidEventId(rawId(n)).id, "local_schema_invalid", "local", at, terminalAt);
    }
    insertDead.run(uuid(80000000), "local_schema_invalid", "local", oldAt, terminalAt);
  })();
  db.exec("create index idx_upload_receipts_state on upload_receipts (terminal_state)");
  for (const item of rawObjects.filter((item) => item.type === "index")) db.exec(item.sql);
  for (const item of rawObjects.filter((item) => item.type === "trigger")) db.exec(item.sql);
  db.prepare("update upload_control set privacy_migration_version=1,migration_cursor_rowid=3000,migration_complete=0 where singleton=1").run();
  assert.equal((db.prepare("select count(*) as n from buffered_events").get() as { n: number }).n, count);
  assert.equal((db.prepare("select count(*) as n from upload_receipts where terminal_state='acknowledged'").get() as { n: number }).n, count);
  assert.equal((db.prepare("select count(*) as n from upload_receipts where terminal_state='dead'").get() as { n: number }).n, 2000);
  db.pragma("journal_mode = WAL");
  db.close();
  const seededBytes = diskBytes();
  assert.ok(seededBytes < 6 * 1024 ** 3, "fixture exceeds 6 GB new-use budget");
  console.log(JSON.stringify({ phase: "seeded_0_7_44", rows: count,
    acknowledged: count, dead: 2000, bytes: seededBytes,
    seconds: (performance.now() - seedStarted) / 1000 }));
  const openSteps: Array<{ step: string; elapsedMs: number }> = [];
  const openStarted = performance.now();
  const buffer = new LocalEventBuffer(ledger, {
    workspaceId: workspace, deviceId: device, delivery: { enabled: true },
    onOpenStep: (step) => openSteps.push({ step: step.step, elapsedMs: step.elapsedMs }),
  });
  try {
    const openMs = performance.now() - openStarted;
    const rawCursorAtOpen = (buffer.database.prepare("select migration_cursor_rowid as n from upload_control where singleton=1").get() as { n: number }).n;
    const sliceEnd = openSteps.findIndex((item) => item.step === "ledger.receipt_lineage_slice");
    const openSliceMs = sliceEnd > 0 ?
      openSteps[sliceEnd]!.elapsedMs - openSteps[sliceEnd - 1]!.elapsedMs : null;
    const remaining = () => (buffer.database.prepare("select count(*) as n from upload_receipts where terminal_state='dead' and raw_rowid is null and raw_id is null and raw_created_at is null and raw_generation is null and rowid>(select cursor_rowid from upload_receipt_lineage_backfill where singleton=1)").get() as { n: number }).n;
    // The daemon starts retention immediately, then follows hasMore every
    // five seconds. This fixture's raws are only 30 days old, so hasMore can
    // only come from receipt repair, not an overdue raw candidate page.
    const firstPruneStarted = performance.now();
    const firstPrune = buffer.prune(90, { maxRows: 128 });
    const firstPruneMs = performance.now() - firstPruneStarted;
    assert.equal(firstPrune.eventRowsVisited, 0);
    assert.ok(firstPrune.hasMore && remaining() > 0,
      "retention cadence must schedule receipt repair before the first upload timer");
    const scheduledFollowupBoundMs = Math.ceil(remaining() / 256) * 5_000;
    assert.ok(scheduledFollowupBoundMs < 60_000,
      "five-second retention followups must be sufficient for this fixture");
    const turns: Array<{ number: number; ms: number; enqueued: number;
      visited: number; cursor: number; paused: string | null; remaining: number }> = [];
    for (let n = 1; n <= 64 && remaining() > 0; n++) {
      const started = performance.now();
      const progress = buffer.delivery.migrateLegacy({ maxRows: 256, maxWriterMs: 100 });
      turns.push({ number: n, ms: performance.now() - started,
        enqueued: progress.enqueued, visited: progress.visited,
        cursor: buffer.delivery.status().migration.cursorRowid,
        paused: progress.paused, remaining: remaining() });
    }
    const elapsedFromOpenMs = performance.now() - openStarted;
    const earlyCollisionDelivery = buffer.database.prepare("select delivery_id as id from upload_outbox where raw_id=?").get("scale-f1-legacy") as { id: string } | undefined;
    const acknowledgementsNull = (buffer.database.prepare("select count(*) as n from upload_receipts where terminal_state='acknowledged' and raw_rowid is null").get() as { n: number }).n;
    const linkedDead = (buffer.database.prepare("select count(*) as n from upload_receipts where terminal_state='dead' and raw_rowid is not null").get() as { n: number }).n;
    const collision = buffer.database.prepare("select raw_rowid as rawRowid from upload_receipts where delivery_id=?").get(f1) as { rawRowid: number | null };
    const ambiguousReceipt = buffer.database.prepare("select raw_rowid as rawRowid from upload_receipts where delivery_id=?").get(ambiguous) as { rawRowid: number | null };
    const expired = buffer.database.prepare("select raw_rowid as rawRowid from upload_receipts where delivery_id=?").get(uuid(80000000)) as { rawRowid: number | null };
    const backlogEnqueued = (buffer.database.prepare("select count(*) as n from upload_outbox where raw_rowid between 3001 and 3600").get() as { n: number }).n;
    const privacyPlan = (buffer.database.prepare("explain query plan select 1 from buffered_events e where e.id=? and " +
      terminalPrivacyEligibilitySql(buffer.database, "e")).all(uuid(1009)) as Array<{ detail: string }>)
      .map((row) => row.detail);
    const maxTurnMs = Math.max(openSliceMs ?? 0, firstPruneMs, ...turns.map((turn) => turn.ms));
    const projection = Math.ceil((count - 16) / 256);
    console.log(JSON.stringify({ phase: "upgrade_measurement", openMs, openSliceMs,
      lineageElapsedMs: elapsedFromOpenMs, remaining: remaining(), linkedDead,
      acknowledgementsNull, collision, ambiguousReceipt, expired, backlogEnqueued,
      rawCursorAtOpen, earlyCollisionDelivery, firstPruneMs,
      firstPruneHasMore: firstPrune.hasMore, scheduledFollowupBoundMs,
      maxWriterSliceMs: maxTurnMs, turns, round12ProjectedUploadTurns: projection,
      finalBytes: diskBytes(), privacyPlan }));
    assert.equal(remaining(), 0, "receipt-side repair must finish");
    assert.ok(elapsedFromOpenMs < 60000, "lineage must complete within 60 seconds of open");
    assert.ok(maxTurnMs < 750, "writer slice must stay under 750 ms");
    assert.equal(acknowledgementsNull, count, "acknowledged receipts must keep NULL lineage");
    assert.equal(linkedDead, 1997, "only uniquely owned dead receipts bind");
    assert.equal(collision.rawRowid, null, "F1 collision must not bind");
    assert.equal(ambiguousReceipt.rawRowid, null, "same-created-at collision must not bind");
    assert.equal(expired.rawRowid, null, "expired raw has no owner to bind");
    assert.equal(rawCursorAtOpen, 3000, "early F1 receipt must not rewind the old raw cursor");
    assert.equal(earlyCollisionDelivery?.id, collisionSafeDeliveryId("scale-f1-legacy", 1),
      "early F1 raw must get a collision-safe delivery without a raw rescan");
    assert.ok(privacyPlan.some((detail) => detail.includes("idx_upload_receipts_raw_lineage")) &&
      privacyPlan.some((detail) => detail.includes("sqlite_autoindex_upload_receipts_1")),
    "privacy reads must use separate linked and delivery-id index probes");
    assert.ok(backlogEnqueued >= 100, "unrelated backlog must enqueue");
    assert.ok(turns.filter((turn) => turn.enqueued > 0 && turn.remaining > 0).length >= 2,
      "backlog must enqueue during multiple incomplete receipt slices");
  } finally {
    buffer.close();
  }
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
